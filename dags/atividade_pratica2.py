from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd


def print_hello():
    print("Iniciando atividade pratica 2")

def check_data_exists(ti):
    hook = PostgresHook(postgres_conn_id='postgres_default')
    df = hook.get_pandas_df(sql="SELECT count(*) FROM players")
    count = df.iloc[0,0]

    ti.xcom_push(key='player_count', value=count)

    if count > 0:
        return 'task_com_dados'
    return 'task_sem_dados'

def task_com_dados():
    print("Sucesso! existem jogadores cadastrados.")

def task_sem_dados():
    print("Atenção ! a tabela está vazia.")


with DAG(
    dag_id="atividade_pratica2_cristina",
    schedule_interval=None,
    start_date=datetime(2020, 1, 1),
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id = 'print_hello',
        python_callable=print_hello
    )

    t2 = BashOperator(
        task_id = 'Limpar_arquivos',
        bash_command = 'ls -l /workspaces/meu-projeto-airflow/data/'
    )

    t3_branch = BranchPythonOperator(
        task_id='branch_check_players',
        python_callable=check_data_exists
    )

    t4_com_dados = PythonOperator(
        task_id='task_com_dados',
        python_callable=task_com_dados
    )

    t4_sem_dados = PythonOperator(
        task_id='task_sem_dados',
        python_callable=task_sem_dados
    )

    # Fluxo de execução
    t1 >> t2 >> t3_branch >> [t4_com_dados, t4_sem_dados]