
from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.transfers.generic_transfer import GenericTransfer

AIRFLOW_HOME = '/workspaces/meu-projeto-airflow'

with DAG(
    dag_id='transfer_dag',
    schedule_interval=None,
    start_date=datetime(2020, 1, 1)
) as dag:
    
    task1 = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS fox characters (
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
            SELECT character_name, character_class, character_level
            FROM fox_character_race;
        """
    )

    task3 =PythonOperator(
        task_id = 'read_table',
        python_callable = 'read_table'
    )


    task1 >> task2 >> task3
