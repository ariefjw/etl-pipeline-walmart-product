'''
=================================================

Nama  : Arief Joko Wicaksono

Program ini digunakan untuk proses otomatisasi ectract, transform, load menggunakan airflow. Program ini menjalankan tiga node yang dibuat terpisah. Program mengatur schedule berjalannya proses dan urutan dijalankannya node module.
=================================================
'''

import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

#path for running bash command
path ='/home/ariefjw/airflow/data/dags/'

#configuration default argument
default_args = {
    'owner': 'arief',
    'start_date': dt.datetime(2024, 11, 1) - timedelta(hours=7), #mengatur jadwal dimulainya program ini.
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}

#configuration DAGs 
with DAG(
    "ETL_walmart_product",
    description='Merupakan proses ETL pipeline untuk data walmart product',
    schedule_interval='10-30/10 9 * * 6', #Untuk mengatur jadwal running program, disini disetting untuk berjalan pada hari sabtu jam 9.10 hingga 9.30 dengan interval 10 menit.
    default_args=default_args, 
    ) as dag:
  #mengatur code untuk menjalankan node ketika airflow diaktifkan.
  extract_data = BashOperator(task_id='extract_data',
                               bash_command=f'python {path}/extract.py')
  transform_data = BashOperator(task_id='transform_data',
                               bash_command=f'python {path}/transform.py')
  load_data = BashOperator(task_id='load_data',
                               bash_command=f'python {path}/load.py')

#running DAGs  
extract_data>>transform_data>>load_data