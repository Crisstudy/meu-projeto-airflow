from datatime import datetime
from airflow import DAG
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

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

def read_players_ids(ti):
    df = pd.read_csv(AIRFLOW_HOME + '/data/players.csv')
    list_ids = df['player_id'].tolist()
    ids = str(list_ids).replace('[','(').replace(']',')')
    ti.xcom_push(key='players_ids', value='ids')
    


with DAG(
    dag_id="xcom_dag_v2",
    schedule_interval=None,
    start_date=datetime(2020, 1, 1),
    catchup=False
) as dag: