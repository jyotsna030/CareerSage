from airflow import DAG
from airflow.operators.python import PythonOperator
import configparser
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from extract.extract_indeed_jobs import scrape_indeed_jobs
from validate.validation import validate_and_process_jobs
from embed.embed_and_upsert import storing_pinecone
from load.loading import load_to_snowflake 

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'start_date': days_ago(0),
    'dagrun_timeout': timedelta(minutes=60),
}

dag= DAG(
    dag_id = "trigger_indeed_pipeline",
    default_args=default_args,
    description="DAG scheduled at 11 pm everyday",
    # schedule_interval='07 23 * * *',  # Scheduled to run at 11 PM daily
    schedule=None,  # Scheduled to run at 11 PM daily
)

# Define the task operators
extract_jobs_task = PythonOperator(
    task_id='extract_jobs',
    python_callable=scrape_indeed_jobs,
    dag=dag,
)

validate_jobs_task = PythonOperator(
    task_id='validate_jobs',
    python_callable=validate_and_process_jobs,
    op_args=["indeed_jobs.csv"],
    dag=dag,
)

embed_jobs_task = PythonOperator(
    task_id='embed_jobs',
    python_callable=storing_pinecone,
    op_args=["indeed_jobs.csv"],
    dag=dag,
)

load_jobs_task = PythonOperator(
    task_id='load_jobs',
    python_callable=load_to_snowflake,
    op_args=["indeed_jobs.csv"],
    dag=dag,
)

# task dependencies
extract_jobs_task >> validate_jobs_task >> embed_jobs_task >> load_jobs_task