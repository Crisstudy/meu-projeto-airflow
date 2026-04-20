from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

AIRFLOW_HOME = '/workspaces/meu-projeto-airflow'

def read_fs():
    with open(AIRFLOW_HOME + '/data/arquivo.txt', 'r') as f:
        for line in f:
            data = line.strip().split(' ')
            print(f'{data[0]}, {data[1]}, {data[2]}')

def read_pandas():
    df = pd.read_csv(AIRFLOW_HOME + '/data/arquivo.txt', header=None)
    print(df)

def read_psql():
    pghook = PostgresHook(postgres_conn_id='postgres_default')
    pghook.copy_expert(
        sql="COPY (SELECT * FROM players) TO stdout WITH CSV HEADER",
        filename=AIRFLOW_HOME + '/data/players.csv'
    )

with DAG(
    dag_id= 'load_dag', 
    schedule_interval=None,
    start_date=datetime(2020, 1, 1)
) as dag:

    task1 = PythonOperator(
        task_id='read_fs',
        python_callable=read_fs
    )

    task2 = PythonOperator(
        task_id='read_pandas',
        python_callable=read_pandas
    )

    task3 = PythonOperator(
        task_id='read_psql',
        python_callable=read_psql
    )

    task1 >> task2 >> task3