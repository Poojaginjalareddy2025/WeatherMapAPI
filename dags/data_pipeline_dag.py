from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import sqlite3
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['Poojaginjalareddy@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG defintion
dag = DAG(
    dag_id='data_pipeline_dag',
    default_args=default_args,
    description='An Airflow DAG to process city data with OpenWeatherMap API',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

# Paths declarations
csv_file_path = '../data/cities.csv'
database_path = '/path/to/local_database.db'
api_key = 'openweathermap_api_key'

# Task 1: Load Data
def load_csv_data():
    df = pd.read_csv(csv_file_path)
    return df.to_dict(orient='records')

# Task 2: Call API
def fetch_weather_data(cities):
    weather_data = []
    base_url = "https://api.openweathermap.org/data/2.5/weather"
    
    for city in cities:
        params = {
            'q': city['city'],
            'appid': api_key,
        }
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()
            weather_data.append({
                'city': city['city'],
                'country': city['country'],
                'population': city['population'],
                'temperature': data['main']['temp'],
                'weather_description': data['weather'][0]['description'],
            })
        except requests.exceptions.RequestException as e:
            print(f"Error fetching weather for {city['city']}: {e}")
    return weather_data

# Task 3: Merge Data
def merge_data(ti):
    city_data = ti.xcom_pull(task_ids='load_csv_data')
    weather_data = ti.xcom_pull(task_ids='fetch_weather_data')
    df = pd.DataFrame(weather_data)
    print("Merged Data result:")
    print(df)
    return df.to_dict(orient='records')

# Task 4: Load to Database
def load_to_database(merged_data):
    conn = sqlite3.connect(database_path)
    df = pd.DataFrame(merged_data)
    df.to_sql('weather_data', conn, if_exists='replace', index=False)
    conn.close()

# Task 5: Cleanup
def cleanup():
    if os.path.exists(database_path):
        print("Database connections closed.")
    else:
        print("No cleanup required.")

# Define tasks
load_data_task = PythonOperator(
    task_id='load_csv_data',
    python_callable=load_csv_data,
    dag=dag,
)

fetch_weather_task = PythonOperator(
    task_id='fetch_weather_data',
    python_callable=fetch_weather_data,
    op_args=[{{ ti.xcom_pull(task_ids='load_csv_data') }}],
    dag=dag,
)

merge_data_task = PythonOperator(
    task_id='merge_data',
    python_callable=merge_data,
    dag=dag,
)

load_database_task = PythonOperator(
    task_id='load_to_database',
    python_callable=load_to_database,
    op_args=[{{ ti.xcom_pull(task_ids='merge_data') }}],
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id='cleanup',
    python_callable=cleanup,
    dag=dag,
)

email_task = EmailOperator(
    task_id='send_email',
    to='Poojaginjalareddy@gmail.com',
    subject='Data Pipeline Status',
    html_content='The data pipeline has completed successfully.',
    dag=dag,
)

# Invoking task dependencies
load_data_task >> fetch_weather_task >> merge_data_task >> load_database_task >> cleanup_task >> email_task
