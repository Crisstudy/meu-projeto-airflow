from datetime import datetime
from airflow import DAG 
from airflow.operators.python import PythonOperator

def task1():
    print('task1')


def sucess_callback():
    print('sucess')


def failure_callback():
    print('failure')

with DAG(
    dag_id= 'notification_dag', 
    schedule_interval='@once',
    default_args={
        'email': 'teste@test.comp',
        'email_on_failure': True,
        'email_on_success': True,
        'email_on_retry': True
    },
    start_date=datetime(2020, 1, 1),
    on_success_callback=sucess_callback,
    on_failure_callback=failure_callback
) as dag:

    task = PythonOperator(
        task_id = 'task1',
        python_callable = task1
    )

    task

