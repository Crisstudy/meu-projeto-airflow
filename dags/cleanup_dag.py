import pandas as pd
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import os
from airflow.operators.python import PythonOperator

AIRFLOW_HOME = '/workspaces/meu-projeto-airflow'

def read_characters():
    pghook = PostgresHook(postgres_conn_id='postgres_default')
    pghook.copy_expert(
        sql="COPY (SELECT * FROM characters) TO stdout WITH CSV HEADER",
        filename=AIRFLOW_HOME + '/data/characters.csv'
    )

def transform_data():
    data = pd.read_csv(AIRFLOW_HOME + '/data/characters.csv')
    data['characters_level'] = 1
    data['characters_level'] = data['characters_level'].apply(lambda x: x + 10)
    data.to_csv(AIRFLOW_HOME + '/data/characters_modified.csv', index=False)

def cleanup():
    os.remove(AIRFLOW_HOME + '/data/characters.csv')
    os.remove(AIRFLOW_HOME + '/data/characters_modified.csv')

with DAG(
    dag_id= 'cleanup_dag', 
    schedule_interval=None,
    start_date=datetime(2020, 1, 1)
) as dag:

    task1 = PythonOperator(
        task_id='read_data',
        python_callable=read_characters
    )

    task2 = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    task3 = PythonOperator(
        task_id='cleanup_data',
        python_callable=cleanup
    )

    task1 >> task2 >> task3