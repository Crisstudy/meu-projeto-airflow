from airflow import DAG
from datetime import datetime
from airflow.decorators import task
from plugins.mask_csv_operator import MaskCSVOperator



AIRFLOW_HOME = '/workspaces/meu-projeto-airflow'


with DAG(
    dag_id= 'dag_custom_op', 
    schedule_interval=None,
    start_date=datetime(2020, 1, 1)
) as dag:

    @task
    def gera_arquivo_csv():
        with open(AIRFLOW_HOME + '/data/arquivo.csv', 'w') as f:
            f.write('Jonh Smith, jonh.smith, hard@password1234,20,true\n')
            f.write('Maria Fox, maria.fox,1234,10,false\n')
            f.write('Jason Matt, jason.matt,125,true\n')
            f.write('Willian Richards, willian.richards,password1234, 10000,false\n')
            f.write('R  ose Williams, rose.williams,Rsajlkds2389012,156,true\n')

    mask_file = MaskCSVOperator(
        task_id='mask_file',
        input_file=AIRFLOW_HOME + '/data/arquivo.csv',
        output_file=AIRFLOW_HOME + '/data/arquivo_masked.csv',
        separator=',',
        columns=[2]
        )

gera_arquivo_csv() >> mask_file
            