
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from scripts.aws_analytics import awsAnalytics
import pandas as pd
from io import StringIO
import subprocess

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
}

# Define the DAG
dag = DAG(
    'upload_to_s3_and_redshift',
    default_args=default_args,
    description='Fetch data from PostgreSQL, clean, and upload to S3 and Redshift',
    schedule_interval='10 8 * * *',  # Run every day at 6 AM
)

def fetch_data_from_postgres(**kwargs):
    destination_config = {
        'dbname': 'destination_db',
        'user': 'postgres',
        'password': 'secret',
        'host': 'destination_postgres'
    }
    query = "SELECT * FROM telecom_customer_churn;"
    fetch_command = [
        'psql',
        '-h', destination_config['host'],
        '-U', destination_config['user'],
        '-d', destination_config['dbname'],
        '-c', query,
        '-A', '-F', ','
    ]
    subprocess_env = dict(PGPASSWORD=destination_config['password'])
    result = subprocess.run(fetch_command, env=subprocess_env, capture_output=True, text=True, check=True)
    df = pd.read_csv(StringIO(result.stdout))
    return df

def upload_data(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='fetch_data')
    bucket_name = 'telecom-customer-churn'
    redshift_config = {
        'dbname': 'redshift_db',
        'user': 'redshift_user',
        'password': '18blc1089!SENSE',
        'host': 'customer-churn.339713084201.us-east-2.redshift-serverless.amazonaws.com',
        'port': 5439,
        'iam_role': 'arn:aws:iam::339713084201:role/service-role/AmazonRedshift-CommandsAccessRole-20240521T203408'
    }
    table_name = 'telecom_customer_churn'
    aws = awsAnalytics(bucket_name, df, redshift_config, table_name)
    aws.upload_to_s3()
    aws.upload_to_redshift()

fetch_data_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data_from_postgres,
    provide_context=True,
    dag=dag,
)

upload_data_task = PythonOperator(
    task_id='upload_data',
    python_callable=upload_data,
    provide_context=True,
    dag=dag,
)

fetch_data_task >> upload_data_task

