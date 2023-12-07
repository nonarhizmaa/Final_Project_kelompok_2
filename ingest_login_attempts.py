from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import glob
from sqlalchemy import create_engine

def get_sqlalchemy_conn():
    engine = create_engine('postgresql://user:password@dataeng-warehouse-postgres:5432/data_warehouse')
    return engine.connect()

def test_conn():
    conn = get_sqlalchemy_conn()
    result = conn.execute("SELECT version();")
    record = result.fetchone()
    print(f"You are connected to - {record}")
    conn.close()
    
def ingest_json_files(folder_path, table_name):
    conn = get_sqlalchemy_conn()
    for file_path in glob.glob(f"{folder_path}/login_attempts_*.json"):
        df = pd.read_json(file_path)
        df.to_sql(table_name, conn, if_exists='append', index=False)
    conn.close()
    
default_args = {
    'owner': 'kelompok 2',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ingest_login_attempts',
    default_args=default_args,
    description='DAG for ingesting data files into PostgreSQL',
    schedule_interval='@once'
)

data_folder_path = 'data/'

test_conn_task = PythonOperator(
    task_id='test_connection',
    python_callable=test_conn,
    dag=dag,
)

ingest_json_task = PythonOperator(
    task_id='ingest_json',
    python_callable=ingest_json_files,
    op_kwargs={'folder_path': data_folder_path, 'table_name': 'login_attempt_history'},
    dag=dag
)