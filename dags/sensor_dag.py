

from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from os import listdir


AIRFLOW_HOME = '/workspaces/meu-projeto-airflow'

def read_file_func():
    for path in listdir(AIRFLOW_HOME + '/data/'):
        print(path)

with DAG(
    dag_id= 'sensor_dag', 
    schedule_interval=None,
    start_date=datetime(2020, 1, 1),
) as dag:

    sensor = FileSensor(
        task_id = 'filesensor',
        filepath = AIRFLOW_HOME + '/data/',
        fs_conn_id = 'fs_default',
        poke_interval = 5,
        timeout = 300
    )

    read_file = PythonOperator(
        task_id = 'read_file',
        python_callable = read_file_func
    )

    sensor >> read_file