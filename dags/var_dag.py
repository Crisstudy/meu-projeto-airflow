from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task

with DAG(
    dag_id= 'dag_variables', 
    schedule_interval=None,
    start_date=datetime(2020, 1, 1)
) as dag:

    @task
    def le_mensagem():
        msg = Variable.get('MSG_TESTE')
        print(msg)

    @task
    def le_json():
        json = Variable.get('JSON_TESTE', deserialize_json=True)
        print(json["campo1"] + " " + json["campo2"])


    le_mensagem() >> le_json()