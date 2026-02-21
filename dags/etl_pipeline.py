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
    @task
    def create_table():
        # Initialize the Postgres hook
        postgres_hook = PostgresHook(postgres_conn_id='postgres_connection')
        
        # Define the SQL command to create the table
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS nasa_apod (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255),
                explanation TEXT,
                url TEXT,
                date DATE,
                media_type VARCHAR(50)

            );
        """
        
        # Execute the SQL command
        postgres_hook.run(create_table_sql)

    ## Step 2: Extract the NASA API Data (Astronomy Picture of the Day)
    ## https://api.nasa.gov/planetary/apod?api_key=your_api_key
    extract_apod = SimpleHttpOperator(
        task_id='extract_apod',
        http_conn_id='nasa_api', # Connection ID defined in Airflow Connections
        endpoint="planetary/apod", # NASA API endpoint for APOD
        method='GET',
        data={"api_key": "{{ connections.nasa_api.extra_dejson.api_key }}"}, # your API key from Airflow Connections
        response_filter=lambda response: response.json() # Parse the JSON response
    )

    ## Step 3: Transform the data (Pick the required information)
    @task
    def transform_data(response):
        # Extract the relevant fields from the API response
        apod_data = {
            'title': response.get('title', ''),
            'explanation': response.get('explanation', ''),
            'url': response.get('url', ''),
            'date': response.get('date', ''),
            'media_type': response.get('media_type', '')
        }
        return apod_data


    ## Step 4: Load the data into PostgreSQL
    @task
    def load_data(apod_data):
        # Initialize the Postgres hook
        postgres_hook = PostgresHook(postgres_conn_id='postgres_connection')
        
        # Define the SQL command to insert data into the table
        insert_sql = """
            INSERT INTO nasa_apod (title, explanation, url, date, media_type)
            VALUES (%s, %s, %s, %s, %s);
        """
        
        # Execute the SQL command with the transformed data
        postgres_hook.run(insert_sql, parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']
        ))


    ## Step 5: Verify the data


    ## Step 6: Define the task dependencies