from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import requests 
import json 
import os 
from datetime import datetime,timedelta

# Path to store file (inside container)
FILE_PATH = "/opt/airflow/dags/data.json"

default_args = {
    'owner' : 'AZ',
    'retries': 1,
    'retry_delay': timedelta(seconds = 30)
}
# ----------------------------
# Task 1: Call API
# ----------------------------

def fetch_data(ti):
    url = "https://jsonplaceholder.typicode.com/users"
    response = requests.get(url) 

    if response.status_code != 200:
        raise Exception("API FAILED")
    
    data = response.json()

    #Push to XCOM 
    ti.xcom_push(key='api_data',value=data)

def save_json(ti):
    try:
        data = ti.xcom_pull(task_ids='get_data_from_api',key = 'api_data')

        if not data:
            raise Exception("No data found in Json")
        
        with open(FILE_PATH,'w') as f:
            json.dump(data,f)
        
        print(f"Saved {len(data)} records to {FILE_PATH}")
    except Exception as e:
        print(e)

def load_to_redshift():
    if not os.path.exists(FILE_PATH):
        print('File Not Found')
    
    with open(FILE_PATH,'r') as f:
        data = json.load(f)
        print(f"Simulating load of {len(data)} records to Redshift...")


with DAG(
    dag_id='4-api_download_operator',
    default_args=default_args,
    description = 'Python Dag Time',
    start_date = datetime(2025,1,1,1),
    schedule = '@hourly'
) as dag:
    task1 = PythonOperator(
        task_id='get_data_from_api',
        python_callable=fetch_data 
    )
    task2 = PythonOperator(
        task_id='save_data_as_json',
        python_callable=save_json 
    )
    task3 = PythonOperator(
        task_id='load_to_redshift',
        python_callable=load_to_redshift 
    )
    task4 = BashOperator(
        task_id='check_if_file_in_container',
        bash_command=f'''
        if [ -f {FILE_PATH} ]; then
            echo "File_Exists"
        else
            echo "File missing"
            exit 1
        fi
    '''
    )

    trigger_next = TriggerDagRunOperator(
        task_id="trigger_downstream",
        trigger_dag_id="5-download_and_transforming_data"
    )

    task1 >> task2 >> task3 >> task4 >> trigger_next

