from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine, types
import pandas as pd
import xlrd

default_args = {
    'owner': 'kelompok 2',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ingest_products',
    default_args=default_args,
    schedule_interval='@once',
)

def run_pandas_postgres_script():
    df = pd.read_excel('/opt/airflow/data/product.xls')
    df.drop('Unnamed: 0', axis=1, inplace=True)

    db_username = 'user'
    db_password = 'password'
    db_host = 'dataeng-warehouse-postgres'
    db_port = '5432'
    db_name = 'data_warehouse'

    engine = create_engine(f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')

    table_name = 'products'

    column_types = {
        'id': types.Integer, 
        'name': types.String,
        'price': types.Float,
        'category_id': types.Integer,
        'supplier_id': types.Integer
    }

    df.to_sql(table_name, engine, if_exists='replace', index=False, dtype=column_types)

run_script_task = PythonOperator(
    task_id='run_pandas_postgres_script',
    python_callable=run_pandas_postgres_script,
    dag=dag,
)
