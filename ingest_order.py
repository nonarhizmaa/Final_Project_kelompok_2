from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine  
import pandas as pd
import fastparquet


default_args = {
    'owner': 'kelompok 2',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'ingest_order',
    default_args=default_args,
    schedule_interval='@once', 
)

def run_pandas_postgres_script():
    import pandas as pd
    from sqlalchemy import create_engine  

    file_path = '/opt/airflow/data/order.parquet'
    df = pd.read_parquet(file_path)

    db_username = 'user'
    db_password = 'password'
    db_host = 'dataeng-warehouse-postgres'
    db_port = '5432'
    db_name = 'data_warehouse'

    conn_string = f"postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}"

    engine = create_engine(conn_string)

    table_name = 'orders'

    df.to_sql(table_name, engine, if_exists='replace', index=False)

run_script_task = PythonOperator(
    task_id='run_pandas_postgres_script',
    python_callable=run_pandas_postgres_script,
    dag=dag,
)
