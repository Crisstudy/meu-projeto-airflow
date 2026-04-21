
from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.generic_transfer import GenericTransfer
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

AIRFLOW_HOME = '/workspaces/meu-projeto-airflow'

def read_data():
    pghook = PostgresHook(postgres_conn_id='postgres_default')
    pghook.copy_expert(
        sql="COPY (SELECT * FROM characters) TO stdout WITH CSV HEADER",
        filename=AIRFLOW_HOME + '/data/characters.csv'
    )

with DAG(
    dag_id='transfer_dag',
    schedule_interval=None,
    start_date=datetime(2020, 1, 1)
) as dag:
    
    task1 = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS fox_characters (
            name VARCHAR(50) NOT NULL,
            class VARCHAR(50) NOT NULL,
            level INT NOT NULL
        );
    """   
)
    
    task2 = GenericTransfer(
        task_id='transfer_tables',
        source_conn_id='postgres_default',
        destination_conn_id='postgres_default',
        destination_table='fox_characters',
        sql="""
            SELECT name, 'Fox' as class, 1 as level
            FROM characters;
        """
    )

    task3 =PythonOperator(
        task_id = 'read_table',
        python_callable = read_data
    )


    task1 >> task2 >> task3
