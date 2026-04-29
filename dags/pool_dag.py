from airflow import DAG
from datetime import datetime
from airflow.decorators import task


with DAG(
    dag_id= 'pool_dag', 
    schedule_interval=None,
    start_date=datetime(2020, 1, 1)
):
    
    @task 
    def task_default_pool():
        print('Task_default_pool')

    @task(pool='pool_teste1')
    def task_pool1():
        print('Task_pool1')


    @task(pool='pool_teste2')
    def task_pool2():
        print('Task_pool2')

    @task(pool='pool_teste1')
    def task_pool1_2():
        print('Task_pool1_2')

    @task(pool='pool_teste2')
    def task_pool2_2():
        print('Task_pool2_2')

    task_default_pool() >> [task_pool1(), task_pool1_2()] >> task_default_pool() >> [task_pool2(), task_pool2_2()]