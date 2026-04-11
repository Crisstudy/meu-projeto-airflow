from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

AIRFLOW_HOME = '/workspaces/meu-projeto-airflow'

def read_data():
    pghook = PostgresHook(postgres_conn_id='postgres_default')
    pghook.copy_expert(
        sql="COPY (SELECT * FROM characters) TO stdout WITH CSV HEADER",
        filename=AIRFLOW_HOME + '/data/characters.csv'
    )

def copy_data():
    lines = ''
    with open(AIRFLOW_HOME + '/data/characters.csv', 'r') as f:
        for line in f:
            lines += line.replace(',', '-')
    with open(AIRFLOW_HOME + '/data/characters_copy.csv', 'w') as f:
        f.write(lines)

with DAG(
    dag_id="hook_dag_v2",
    schedule_interval=None,
    start_date=datetime(2020, 1, 1),
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id='read_data',
        python_callable=read_data
    )

    task2 = PythonOperator(
        task_id='copy_data',
        python_callable=copy_data
    )

    task1 >> task2