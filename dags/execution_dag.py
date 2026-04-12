

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time

def task1():
    print('task1')

def task2():
    print('task2')
    time.sleep(6)

def task3():
    print('task3')
    time.sleep(6)

def task4():
    print('task4')
    time.sleep(6)

def task5():
    print('task5')
    time.sleep(6)

with DAG(
    dag_id="execution_dag_v2",
    schedule_interval=None,
    start_date=datetime(2020, 1, 1),
    catchup=False
) as dag:
    
    t1 = PythonOperator(
        task_id='task1',
        python_callable=task1
    )

    t2 = PythonOperator(
        task_id='task2',
        python_callable=task2
    )

    t3 = PythonOperator(
        task_id='task3',
        python_callable=task3
    )

    t4 = PythonOperator(
        task_id='task4',
        python_callable=task4
    )

    t5 = PythonOperator(
        task_id='task5',
        python_callable=task5
    )

    t1 >> [t2, t3, t4] >> t5