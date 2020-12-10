from datetime import datetime, timedelta

import os
from airflow import DAG
from airflow.operators import StageToRedshiftOperator
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'johnrick',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'retries': 0, # change back to 3 when working
    'retry_delay': timedelta(seconds=300),
    'catchup': False
}

dag = DAG('test_dag_stage_events_to_redshift',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          max_active_runs=1
        #   schedule_interval='0 * * * *'
        )

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket='udacity-dend',
    s3_key = "log-data",    
    table="staging_events",
    file_format='JSON \'s3://udacity-dend/log_json_path.json\''
)