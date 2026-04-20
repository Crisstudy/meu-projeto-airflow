

from datetime import datetime
from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

AIRFLOW_HOME = '/workspaces/meu-projeto-airflow'

CHARACTERS = Dataset('file://' + AIRFLOW_HOME + '/data/characters.csv')


def read_characters():
    pghook = PostgresHook(postgres_conn_id='postgres_default')
    pghook.copy_expert(
        sql="COPY (SELECT * FROM characters) TO stdout WITH CSV HEADER",
        filename=AIRFLOW_HOME + '/data/characters.csv'
    )


with DAG(
    dag_id='dataset_dag',
    schedule_interval=None,
    start_date=datetime(2020, 1, 1)
) as dag:
    
    task1 = PythonOperator(
        task_id='write_characters',
        python_callable=read_characters,
        outlets=[CHARACTERS]
    )