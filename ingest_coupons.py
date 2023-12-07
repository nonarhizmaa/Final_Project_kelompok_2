from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine, types
import pandas as pd

default_args = {
    'owner': 'kelompok 2',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'ingest_coupons',
    default_args=default_args,
    schedule_interval='@once',
)

def run_pandas_postgres_script():
    df = pd.read_json('/opt/airflow/data/coupons.json', orient='column')
    
    db_username = 'user'
    db_password = 'password'
    db_host = 'dataeng-warehouse-postgres'
    db_port = '5432'
    db_name = 'data_warehouse'

    engine = create_engine(f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')

    table_name = 'coupons'

    column_types = {
        'id': types.Integer, 
        'discount_percent': types.Float
    }

    df.to_sql(table_name, engine, if_exists='replace', index=False, dtype=column_types)

run_script_task = PythonOperator(
    task_id='run_pandas_postgres_script',
    python_callable=run_pandas_postgres_script,
    dag=dag,
)


def some_other_function():
    pass

some_other_task = PythonOperator(
    task_id='some_other_task',
    python_callable=some_other_function,
    dag=dag,
)

some_other_task.set_upstream(run_script_task)

if __name__ == "__main__":
    dag.cli() 
