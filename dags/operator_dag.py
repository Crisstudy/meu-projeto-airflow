from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def funcao_hello():
    print("Hello World!")

with DAG(
    dag_id="operator_dag",
    schedule_interval=None,
    start_date=datetime(2020, 1, 1)
) as dag:
    
    task1 = BashOperator(
        task_id='task1',
        bash_command='echo "Hello World!"'
    )

    task2 = PythonOperator(
        task_id= 'task2',
        python_callable= funcao_hello
    )

    task1 >> task2