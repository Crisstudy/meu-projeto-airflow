from datetime import datetime
from airflow import DAG 
from airflow.sensors.time_sensor import TimeSensorAsync 
from airflow.operators.python import PythonOperator

def task():
    print("olá")
    


with DAG(
    dag_id= 'dag_defereable', 
    schedule_interval=None,
    start_date=datetime(2020, 1, 1),
) as dag:

    sensor = TimeSensorAsync(
        task_id = 'sensor',
        target_time = datetime(2026, 4, 14, 00, 0).time()
    )

    task = PythonOperator(
        task_id = 'task',
        python_callable = task
    )

    sensor >> task