from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import fastavro
import pandas as pd
from sqlalchemy import create_engine

def my_python_function():
    file_path = '/opt/airflow/data/order_item.avro'

    with open(file_path, 'rb') as avro_file:
        avro_reader = fastavro.reader(avro_file)
        df = pd.DataFrame.from_records(avro_reader)

    db_params = {
        'user': 'user',
        'password': 'password',
        'host': 'dataeng-warehouse-postgres',
        'port': '5432',
        'database': 'data_warehouse',
    }

    db_url = f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"
    engine = create_engine(db_url)

    df.to_sql('order_items', engine, if_exists='replace', index=False)

    engine.dispose()

default_args = {
    'owner': 'kelompok 2',
    'start_date': datetime(2023, 12, 5),

}

with DAG('ingest_order_item', default_args=default_args, schedule_interval='@once') as dag:
    task1 = PythonOperator(
        task_id='my_task',
        python_callable=my_python_function)