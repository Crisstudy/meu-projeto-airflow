from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

AIRFLOW_HOME = '/workspaces/meu-projeto-airflow'

def gera_arquivo():
    with open(AIRFLOW_HOME + '/data/arquivo.txt', 'w') as f:
        for i in range(100):
            f.write('Arquivo text linha{}\n'.format(i))       

def processa_arquivo():
    arquivo = ''
    with open(AIRFLOW_HOME + '/data/arquivo.txt', 'r') as f:
        for line in f:
            arquivo += line.strip('\n') + ' - Processado\n'

    with open(AIRFLOW_HOME + '/data/arquivo_processado.txt', 'w') as f:
        f.write(arquivo)


with DAG(
    dag_id= 'dag_scheduler', 
    schedule_interval='@weekly',
    start_date=datetime(2020, 1, 1),
    end_date=datetime(2027, 1, 10),
    catchup=False
) as dag:

    tarefa_gerar= PythonOperator(
        task_id= 'gera_arquivo',
        python_callable=gera_arquivo
    )

    tarefa_processar= PythonOperator(
        task_id= 'processa_arquivo',
        python_callable=processa_arquivo
    )

    tarefa_gerar >> tarefa_processar