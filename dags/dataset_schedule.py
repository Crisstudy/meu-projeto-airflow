from airflow import DAG
from airflow.datasets import Dataset
from datetime import datetime
import pandas as pd
from airflow.operators.python import PythonOperator

AIRFLOW_HOME = '/workspaces/meu-projeto-airflow'

CHARACTERS = Dataset('file://' + AIRFLOW_HOME + '/data/characters.csv')

def read_characters():
    df = pd.read_csv(AIRFLOW_HOME + '/data/characters.csv')

with DAG(
    dag_id='dataset_schedule_dag',
    schedule=[CHARACTERS],
    start_date=datetime(2020, 1, 1)
) as dag:

    task = PythonOperator(
        task_id='read_characters',
        python_callable=read_characters
    )

    task