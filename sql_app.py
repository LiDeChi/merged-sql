import streamlit as st
import pandas as pd
import json
from pathlib import Path
import sys
import os
import tempfile
from io import StringIO
import requests
import urllib.parse
import re
import time
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from multiprocessing import Value

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.append(str(project_root))

# 初始化session state
if 'saved_queries' not in st.session_state:
    st.session_state.saved_queries = {}

def save_query(name, query):
    """保存SQL查询"""
    st.session_state.saved_queries[name] = query
    save_queries_to_file()

def delete_query(name):
    """删除SQL查询"""
    if name in st.session_state.saved_queries:
        del st.session_state.saved_queries[name]
        save_queries_to_file()

def save_queries_to_file():
    """将查询保存到文件"""
    queries_file = project_root / 'config' / 'saved_queries.json'
    with open(queries_file, 'w', encoding='utf-8') as f:
        json.dump(st.session_state.saved_queries, f, ensure_ascii=False, indent=2)

def load_saved_queries():
    """加载保存的查询"""
    queries_file = project_root / 'config' / 'saved_queries.json'
    if queries_file.exists():
        with open(queries_file, 'r', encoding='utf-8') as f:
            st.session_state.saved_queries = json.load(f)

def get_table_names(query):
    """从SQL查询中提取所有表名"""
    # 移除注释
    query = re.sub(r'--.*$', '', query, flags=re.MULTILINE)
    query = re.sub(r'/\*.*?\*/', '', query, flags=re.DOTALL)
    
    # 找到FROM和JOIN后面的所有表名
    tables = set()
    
    # 匹配FROM后的表名
    from_matches = re.finditer(r'from\s+([a-zA-Z_][a-zA-Z0-9_]*)', query.lower())
    for match in from_matches:
        tables.add(match.group(1))
    
    # 匹配JOIN后的表名
    join_matches = re.finditer(r'join\s+([a-zA-Z_][a-zA-Z0-9_]*)', query.lower())
    for match in join_matches:
        tables.add(match.group(1))
    
    return list(tables)

def check_table_exists(base_url, session_id, device_id, database_id, table_name):
    """检查表是否存在"""
    check_query = f"SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}' LIMIT 1;"
    
    # 构建查询数据
    query_data = {
        "database": database_id,
        "native": {
            "query": check_query,
            "template-tags": {}
        },
        "type": "native"
    }
    
    # 准备POST数据
    data = {
        'query': json.dumps(query_data)
    }
    
    try:
        with requests.Session() as session:
            session.headers.update({
                'Accept': 'text/csv',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Cookie': f'metabase.DEVICE={device_id}; metabase.SESSION={session_id}'
            })
            
            response = session.post(
                f"{base_url}/api/dataset/csv",
                data=urllib.parse.urlencode(data),
                timeout=5
            )
            
            if response.status_code == 200:
                return len(response.text.strip()) > 0
            return False
            
    except Exception as e:
        print(f"检查表存在性时发生错误: {str(e)}")
        return False

def get_data_as_csv(base_url, session_id, device_id, database_id, query):
    """从Metabase获取CSV格式的数据"""
    print(f"\n正在处理数据库 {database_id}...")
    
    # 构建查询数据
    query_data = {
        "database": database_id,
        "native": {
            "query": query,
            "template-tags": {}
        },
        "type": "native",
        "middleware": {
            "js-int-to-string?": True,
            "add-default-userland-constraints?": True
        }
    }
    
    # 构建最小化的可视化设置
    viz_settings = {
        "table.pivot": False,
        "table.columns": []
    }
    
    # 准备POST数据
    data = {
        'query': json.dumps(query_data),
        'visualization_settings': json.dumps(viz_settings)
    }

    try:
        with requests.Session() as session:
            session.headers.update({
                'Accept': 'text/csv',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Cookie': f'metabase.DEVICE={device_id}; metabase.SESSION={session_id}'
            })

            print(f"发送查询请求到数据库 {database_id}...")
            response = session.post(
                f"{base_url}/api/dataset/csv",
                data=urllib.parse.urlencode(data),
                stream=True,
                timeout=999
            )

            if response.status_code == 200:
                print(f"开始接收数据库 {database_id} 的数据...")
                # 使用流式读取响应内容
                chunks = []
                total_size = 0
                for chunk in response.iter_content(chunk_size=8192, decode_unicode=True):
                    if chunk:
                        chunks.append(chunk)
                        total_size += len(chunk)
                        print(f"\r数据库 {database_id} 已接收: {total_size/1024:.2f} KB", end="")
                print(f"\n数据库 {database_id} 数据接收完成")
                return ''.join(chunks)
            else:
                print(f"数据库 {database_id} 请求失败: {response.status_code}")
                return None

    except Exception as e:
        print(f"数据库 {database_id} 请求发生错误: {str(e)}")
        return None

def process_database(metabase, db_id, query, total_rows, output_lock):
    """处理单个数据库的函数"""
    db_start_time = time.time()
    
    # 首先检查所有需要的表是否存在
    tables = get_table_names(query)
    for table in tables:
        if not check_table_exists(
            metabase["base_url"],
            metabase["session_id"],
            metabase["device_id"],
            db_id,
            table
        ):
            print(f"数据库 {db_id} 中不存在表 {table}，跳过查询")
            return 0
    
    # 获取CSV数据
    csv_content = get_data_as_csv(
        metabase["base_url"],
        metabase["session_id"],
        metabase["device_id"],
        db_id,
        query
    )
    
    if not csv_content:
        return 0
        
    # 解析CSV内容
    print(f"正在处理数据库 {db_id} 的数据...")
    df = pd.read_csv(StringIO(csv_content))
    
    if df.empty:
        print(f"数据库 {db_id} 未获取到数据")
        return 0
    
    # 计算单个数据库处理时间
    db_time = time.time() - db_start_time
    db_rows = len(df)
    print(f"数据库 {db_id} 获取到 {db_rows} 行数据")
    print(f"数据库 {db_id} 处理耗时: {db_time:.2f} 秒")
    
    return df

def execute_query(query):
    """执行SQL查询"""
    try:
        # 记录开始时间
        start_time = time.time()
        
        # 加载配置
        config = load_config()
        metabase = config["metabase"]
        
        print(f"\n=== 开始查询 ===")
        print(f"开始时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"涉及的表: {', '.join(get_table_names(query))}")
        print(f"查询语句: {query}")
        print(f"目标数据库: {config['target_databases']}")
        
        # 创建共享变量和锁
        total_rows = Value('i', 0)
        output_lock = threading.Lock()
        
        # 使用线程池并行处理数据库查询
        all_dfs = []
        with ThreadPoolExecutor(max_workers=min(len(config["target_databases"]), 20)) as executor:
            # 创建future到数据库ID的映射
            future_to_db = {
                executor.submit(
                    process_database,
                    metabase,
                    db_id,
                    query,
                    total_rows,
                    output_lock
                ): db_id for db_id in config["target_databases"]
            }
            
            # 处理完成的任务
            completed_dbs = 0
            successful_dbs = 0
            for future in as_completed(future_to_db):
                db_id = future_to_db[future]
                completed_dbs += 1
                try:
                    df = future.result()
                    if df is not None and not df.empty:
                        all_dfs.append(df)
                        successful_dbs += 1
                except Exception as e:
                    print(f"数据库 {db_id} 处理失败: {str(e)}")
        
        if not all_dfs:
            return None, "未从任何数据库获取到数据"
            
        # 合并所有数据库的结果
        final_df = pd.concat(all_dfs, ignore_index=True)
        
        # 计算总耗时
        total_time = time.time() - start_time
        
        print(f"\n=== 完成 ===")
        print(f"结束时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"总耗时: {total_time:.2f} 秒")
        print(f"成功处理数据库数: {successful_dbs}/{len(config['target_databases'])}")
        
        if successful_dbs > 0:
            print(f"平均每个成功数据库耗时: {(total_time/successful_dbs):.2f} 秒")
            print(f"总行数: {len(final_df)}")
            print(f"平均每千行数据耗时: {(total_time/(len(final_df)/1000)):.2f} 秒")
        
        return final_df, None

    except Exception as e:
        return None, f"查询执行错误: {str(e)}"

def load_config():
    """加载配置文件"""
    config_path = project_root / "config" / "database_config.json"
    with open(config_path, 'r') as f:
        return json.load(f)

def main():
    st.title('SQL查询工具')

    # 加载保存的查询
    load_saved_queries()

    # 侧边栏：保存的查询列表
    with st.sidebar:
        st.header('保存的查询')
        for query_name in st.session_state.saved_queries:
            col1, col2, col3 = st.columns([2, 1, 1])
            with col1:
                if st.button(f'加载 {query_name}', key=f'load_{query_name}'):
                    st.session_state.sql_input = st.session_state.saved_queries[query_name]
            with col2:
                if st.button('编辑', key=f'edit_{query_name}'):
                    st.session_state.sql_input = st.session_state.saved_queries[query_name]
            with col3:
                if st.button('删除', key=f'delete_{query_name}'):
                    delete_query(query_name)
                    st.rerun()

    # 主界面
    # SQL输入区域
    if 'sql_input' not in st.session_state:
        st.session_state.sql_input = ''

    sql_query = st.text_area('输入SQL查询', value=st.session_state.sql_input, height=200)

    # 保存查询
    col1, col2 = st.columns([3, 1])
    with col1:
        query_name = st.text_input('查询名称')
    with col2:
        if st.button('保存查询') and query_name and sql_query:
            save_query(query_name, sql_query)
            st.success(f'查询 "{query_name}" 已保存')

    # 执行查询
    if st.button('执行查询') and sql_query:
        with st.spinner('执行查询中...'):
            # 创建日志显示区域
            log_area = st.empty()
            
            df, error = execute_query(sql_query)
            
            if error:
                st.error(error)
            elif df is not None:
                st.success('查询执行成功！')
                
                # 显示结果统计
                st.info(f"总共获取到 {len(df)} 行数据")
                
                # 显示结果
                st.dataframe(df)
                
                # 下载按钮
                csv = df.to_csv(index=False)
                st.download_button(
                    label="下载CSV文件",
                    data=csv,
                    file_name="query_result.csv",
                    mime="text/csv",
                )

if __name__ == "__main__":
    main()
