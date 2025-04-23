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
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from airflow.models import Variable
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#path for running bash command
path ='/home/ariefjw/airflow/dags/'

def send_notification(context, status):
    """Send email notification for both success and failure"""
    task_instance = context['task_instance']
    execution_time = (context.get('next_execution_date') - context.get('execution_date')).total_seconds()
    
    if status == 'success':
        subject = f"Airflow Success: Task {task_instance.task_id} Completed"
        html_content = f"""
        <h3>ETL Pipeline Success</h3>
        <p>Task {task_instance.task_id} completed successfully at {context.get('execution_date')}</p>
        <p>Execution Time: {execution_time:.2f} seconds</p>
        <p>Log: {task_instance.log_url}</p>
        """
    else:
        subject = f"Airflow Alert: Failed Task {task_instance.task_id}"
        html_content = f"""
        <h3>ETL Pipeline Alert</h3>
        <p>Task {task_instance.task_id} failed at {context.get('execution_date')}</p>
        <p>Error: {context.get('exception')}</p>
        <p>Log: {task_instance.log_url}</p>
        """
    
    send_email(
        to=Variable.get("email_notification_list", default_var="ariefjoko78@gmail.com"),
        subject=subject,
        html_content=html_content
    )

def notify_on_failure(context):
    """Send notification on task failure"""
    task_instance = context['task_instance']
    error_message = f"Task {task_instance.task_id} failed. Error: {context.get('exception')}"
    logger.error(error_message)
    send_notification(context, 'failure')

def notify_on_success(context):
    """Send notification on task success"""
    task_instance = context['task_instance']
    logger.info(f"Task {task_instance.task_id} completed successfully")
    send_notification(context, 'success')

#configuration default argument
default_args = {
    'owner': 'arief',
    'start_date': dt.datetime(2024, 11, 1) - timedelta(hours=7),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'email_on_failure': True,
    'email_on_retry': True,
    'on_failure_callback': notify_on_failure,
    'on_success_callback': notify_on_success,
}

#configuration DAGs 
with DAG(
    "ETL_walmart_product",
    description='Merupakan proses ETL pipeline untuk data walmart product',
    schedule_interval='10-30/10 9 * * 6',
    default_args=default_args,
    catchup=False,  # Prevent backfilling
    max_active_runs=1,  # Ensure only one run at a time
    ) as dag:

    # Extract task with error handling
    extract_data = BashOperator(
        task_id='extract_data',
        bash_command=f'python3 {path}/extract.py',
        retries=2,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=3),
    )

    # Transform task with error handling
    transform_data = BashOperator(
        task_id='transform_data',
        bash_command=f'python3 {path}/transform.py',
        retries=2,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=3),
    )

    # Load task with error handling
    load_data = BashOperator(
        task_id='load_data',
        bash_command=f'python3 {path}/load.py',
        retries=2,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=3),
    )

    # Set task dependencies
    extract_data >> transform_data >> load_data