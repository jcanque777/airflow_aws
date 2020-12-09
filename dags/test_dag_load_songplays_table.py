from datetime import datetime, timedelta

import os
from airflow import DAG
from airflow.operators import LoadFactOperator
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'johnrick',
    'start_date': datetime(2020, 12, 8),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=300),
    'catchup': False
}


dag = DAG('test_dag_load_songplays_table',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow'#,
          #schedule_interval='0 * * * *'
        )

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table_task',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    select_query=SqlQueries.songplay_table_insert
)