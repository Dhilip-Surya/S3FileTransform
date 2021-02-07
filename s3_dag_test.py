import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.amazon.aws.operators.s3_file_transform import S3FileTransformOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

input=sys.argv[1]
output=sys.argv[2]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2016, 11, 1),
    'email': ['dhilipsurya8@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily'
}

dag = DAG('file_transform', default_args=default_args, schedule_interval= '@once')

def loader():
    s3uploader = S3Hook(
    aws_conn_id="aws_conn",
    )

    s3uploader.load_file(
        filename=input,
        key = 'asset.json',
        bucket_name = 'dagtest/asset'
    )


t1 = PythonOperator(
    task_id="s3_loader", python_callable=loader, dag=dag
)

t2 = S3FileTransformOperator(
        task_id='json_to_csv',
        description='Convert JSON file to CSV with headers',
        source_s3_key='s3://dagtest/asset/asset.json',
        dest_s3_key='s3://dagtest/asset_transformed/asset.csv',
        replace=True,
        transform_script='json_to_csv.py'
    )


