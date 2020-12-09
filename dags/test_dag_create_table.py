from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'johnrick',
    'start_date': datetime(2020, 12, 8),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=300),
    'catchup': False
}


dag = DAG('test_dag_create_table',
          default_args=default_args,
          description='Create tables on Redshift'#,
        #   schedule_interval='0 * * * *'
        )

table_creation = PostgresOperator(
    task_id='tables_creation',  
    dag=dag,
    postgres_conn_id='redshift',
    sql = '/create_tables.sql'
)