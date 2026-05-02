from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta


default_args = {
    'owner' : 'AZ',
    'retries': 1,
    'retry_delay': timedelta(seconds = 30)
}

def push_data(**context):
    data = {'Name' : 'Abdullah', 'JD' : 'Senior Data Engineer'}
    return data

def pull_data(**context):
    data = context['ti'].xcom_pull(task_ids='Push_data_x.com')
    print(data)

def say_hello():
    print("Hello from PythonOperator")

with DAG(
    dag_id = '3_Python_Operator',
    default_args = default_args,
    description = 'Python Dag Time',
    start_date = datetime(2025,1,1,1),
    schedule = '@hourly'
) as dag:
    task1 = PythonOperator(
        task_id='prompt',
        python_callable = say_hello
    )

    task2 = PythonOperator(
        task_id = 'Push_data_x.com',
        python_callable = push_data
    )

    task3 = PythonOperator(
        task_id = 'Pull_data_x.com',
        python_callable = pull_data
    )

    trigger_next = TriggerDagRunOperator(
        task_id="trigger_downstream",
        trigger_dag_id="4-api_download_operator"
    )


    task1 >> task2 >> task3 >> trigger_next
