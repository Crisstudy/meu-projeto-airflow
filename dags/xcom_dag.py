from datetime import datetime
from airflow import DAG
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

AIRFLOW_HOME = '/workspaces/meu-projeto-airflow'

def read_players():
    pghook = PostgresHook(postgres_conn_id='postgres_default')
    pghook.copy_expert(
        sql="COPY (SELECT * FROM players) TO stdout WITH CSV HEADER",
        filename=AIRFLOW_HOME + '/data/players.csv'
    )

def read_currency():
    pghook = PostgresHook(postgres_conn_id='postgres_default')
    pghook.copy_expert(
        sql="COPY (SELECT * FROM currency) TO stdout WITH CSV HEADER",
        filename=AIRFLOW_HOME + '/data/currency.csv'
    )

def read_currency_modified():
    pghook = PostgresHook(postgres_conn_id='postgres_default')
    pghook.copy_expert(
        sql="COPY (SELECT * FROM currency) TO stdout WITH CSV HEADER",
        filename=AIRFLOW_HOME + '/data/currency_modified.csv'
    )

def read_players_ids(ti):
    df = pd.read_csv(AIRFLOW_HOME + '/data/players.csv')
    list_ids = df['id'].tolist()
    ids = str(list_ids).replace('[','(').replace(']',')')
    ti.xcom_push(key='players_ids', value=ids)
    


with DAG(
    dag_id="xcom_dag_v2",
    schedule_interval=None,
    start_date=datetime(2020, 1, 1),
    catchup=False
) as dag:
    
    task1 = PythonOperator(
        task_id='read_players',
        python_callable=read_players
    )

    task2 = PythonOperator(
        task_id='read_currency',
        python_callable=read_currency
    )

    task3 = PythonOperator(
        task_id='read_playerid',
        python_callable=read_players_ids
    )


    task4 = PythonOperator(
        task_id='read_currency_modified',
        python_callable=read_currency_modified
    )

    task5 = PostgresOperator(
        task_id='update_currency',
        postgres_conn_id='postgres_default',
        sql="UPDATE currency SET currency_amount= currency_amount + 500 WHERE player_id in"
            "{{ task_instance.xcom_pull(task_ids='read_playerid', key='players_ids') }}"
    )

    task1 >> [task2, task3] >> task4 >> task5