from datetime import datetime, timedelta

import os
from airflow import DAG
from airflow.operators import StageToRedshiftOperator
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2020, 12, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=300),
    'catchup': False
}

dag = DAG('test_stage_events',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow'
        #   schedule_interval='0 * * * *'
        )

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events_to_redshift',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_path="s3://udacity-dend/log_json_path.json"
)