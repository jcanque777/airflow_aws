# airflow_aws

## Introduction

Airflow is one of the best tools for workflow automation and an easy way to manage data pipelines. Sparkify is looking to automate their ETL process and wants their data team to use Apache Airflow. Airflow uses DAGs (directed acyclic graphs) for their workflows. The illustration is an example of a DAG and shows the full process for our created pipeline. 


![Graph View of Data Pipeline](https://user-images.githubusercontent.com/53429726/101825085-25c80580-3afb-11eb-9210-b34b68b69f8d.png)

## ETL Pipeline
- Create Tables From SQL File
- Get Data From S3 and Load Into Staging Tables
- Load Data To Fact and Dimension Tables
- Run Final Check To Ensure Data Is Received Correctly

## Apache Airflow On Docker
One of the most difficult things when working with different technologies is that they rarely work well together out of the box. Airflow is not an exeption, for this project Airflow has to work with Python, Posgresql, AWS Redshift and AWS S3. The silver lining is that I discovered Docker and Docker Hub. The easiest way to implement this project is pulling from puckel/docker-airflow and installing awscli and boto3 on the Dockerfile. 

## DAGs Folder
The DAG folder contains the udac_example_dag.py and is where the DAG, tasks, and task dependecies can be found. Once Airflow connections and variables are established. Simply turning on the DAG from the Airflow UI will run the pipeline as scheduled on the file.

## Plugins/Helpers Folder
The file sql_queries.py contains the SQL queries that are used to copy data from S3 bucket and eventually to our tables on our Redshift cluster.

## Plugins/Operators Folder
The four program files in this folder are stage_redshift, load_fact, load_dimension, and data_quality operators that our DAG uses to complete their tasks. 
- StageToReshiftOperator gets the data from an S3 bucket. Finds the correct table on Redshift and copies the JSON files from the S3 bucket to our Redshift table. 
- LoadFactorOperator gets the information our staging tables, transforms then loads data to our songplays table
- LoadDimensionOperator gets the information our staging tables, transforms then loads data to our dimension tables
- DataQualityOperator is our quality check to make sure the data that is coming from JSON files in an S3 bucket has safely arrived in our Redshift cluster