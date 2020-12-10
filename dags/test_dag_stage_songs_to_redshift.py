from datetime import datetime, timedelta

import os
from airflow import DAG
from airflow.operators import StageToRedshiftOperator
from helpers import SqlQueries

default_args = {
    'owner': 'johnrick',
    'start_date': datetime(2020, 12, 9),
    'depends_on_past': False,
    'retries': 0, # change back to 3 when working
    'retry_delay': timedelta(seconds=300),
    'catchup': False
}

dag = DAG('test_dag_stage_songs_to_redshift',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          max_active_runs=1
        #   schedule_interval='0 * * * *'
        )

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs_to_redshift_task',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    file_format = 'JSON \'auto\'' 
)