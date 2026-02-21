import json
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.postgre.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago

## Define the DAG
with DAG(
    dag_id='nasa_apod_postgres',
    start_date=days_ago(1),
    schedule='@daily',
    catchup=False
) as dag:
    
    ## Step 1: Create the table if doesn't exist


    ## Step 2: Extract the NASA API Data (Astronomy Picture of the Day)


    ## Step 3: Transform the data (Pick the required information)


    ## Step 4: Load the data into PostgreSQL


    ## Step 5: Verify the data


    ## Step 6: Define the task dependencies