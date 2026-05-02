from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import requests
import json
default_args = {
    "owner": "abdullah",
    "retries": 2
}

#https://jsonplaceholder.typicode.com/posts

def fetch_data(**context):
    url = 'https://jsonplaceholder.typicode.com/posts'
    try:
        response = requests.get(url) 
        data = response.json()

        #Push to XCOM 
        print(f"Fetched {len(data)} records")
        file_path = "/tmp/raw_data.json"
        with open(file_path,'w') as f:
            json.dump(data,f)
            return file_path
    
    except Exception as e: 
        print('Error Raised while fetching data: ', e)
    
     

def transform_data(**context):
    file_path = context['ti'].xcom_pull(task_ids='fetch_data')
    with open(file_path) as f:
        data = json.load(f)
        print(data)
    transformed = [
        {
            "id": row["id"],
            "title": row["title"].upper()
        }
        for row in data
    ]
    output_path = "/tmp/transformed.json"
    with open(output_path, "w") as f:
        json.dump(transformed, f)
    return output_path
    
def validate_schema(**context):
    path = context['ti'].xcom_pull(task_ids='transform_data')
    with open(path) as f:
        data = json.load(f)
    for row in data:
        if "id" not in row or "title" not in row:
            raise Exception("Schema validation failed")

def validate_nulls(**context):
    path = context['ti'].xcom_pull(task_ids='transform_data')
    with open(path) as f:
        data = json.load(f)
    for row in data:
        if row["title"] is None:
            raise Exception("Null values found")

with DAG(
    dag_id='5-download_and_transforming_data',
    default_args=default_args,
    description = '',
    start_date = datetime(2025,1,1,1),
    schedule = '@hourly'
) as dag:
    task1 = PythonOperator(
        task_id = 'fetch_data',
        python_callable = fetch_data
    )
    task2 = PythonOperator(
        task_id = 'transform_data',
        python_callable = transform_data
    )

    with TaskGroup("validations") as validations:
        schema_check = PythonOperator(
            task_id="schema_check",
            python_callable=validate_schema
        )
        null_check = PythonOperator(
            task_id="null_check",
            python_callable=validate_nulls
        )
        schema_check >> null_check

    task1 >> task2 >> validations
