from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from datetime import datetime, timedelta
import requests
import pandas as pd
import json
import os

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'weather_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for Weather data to Snowflake via S3',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# 1. Extract Data
def extract_weather_data(**kwargs):
    # Fetch API Key from Airflow Variables
    api_key = Variable.get("openweather_api_key")
    city = "London"
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"
    
    # Make API request
    response = requests.get(url)
    response.raise_for_status()
    weather_data = response.json()
    
    # Extract specific fields to match our Snowflake table schema
    data = {
        # Convert epoch time to ISO format
        "dt": datetime.fromtimestamp(weather_data.get('dt')).isoformat(),
        "city_name": weather_data.get('name', city),
        "temp": weather_data['main']['temp'],
        "feels_like": weather_data['main']['feels_like'],
        "conditions": weather_data['weather'][0]['main'],
        "humidity": weather_data['main']['humidity'],
        "wind_speed": weather_data['wind']['speed']
    }
    
    # Save to local CSV temporarily
    df = pd.DataFrame([data])
    file_path = '/opt/airflow/dags/weather_data.csv'
    df.to_csv(file_path, index=False)
    return file_path

extract_task = PythonOperator(
    task_id='extract_weather_data',
    python_callable=extract_weather_data,
    dag=dag,
)

# 2. Upload to S3
upload_to_s3_task = LocalFilesystemToS3Operator(
    task_id='upload_to_s3',
    filename='/opt/airflow/dags/weather_data.csv',
    dest_key='weather_data/{{ ds }}/weather.csv',
    dest_bucket='airflow-snowflake-etl-data',
    aws_conn_id='aws_default',
    replace=True,
    dag=dag,
)

# 3. Load to Snowflake
# Using COPY INTO command
copy_into_snowflake_task = SnowflakeOperator(
    task_id='copy_into_snowflake',
    snowflake_conn_id='snowflake_default',
    sql="""
        COPY INTO airflow_etl_db.weather_data.weather_log (dt, city_name, temp, feels_like, conditions, humidity, wind_speed)
        FROM @airflow_etl_db.weather_data.weather_s3_stage/weather_data/{{ ds }}/weather.csv
        FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1);
    """,
    dag=dag,
)

# Task Dependencies
extract_task >> upload_to_s3_task >> copy_into_snowflake_task
