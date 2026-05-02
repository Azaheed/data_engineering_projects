from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner' : 'AZ',
    'retries': 1,
    'retry_delay': timedelta(seconds = 30)
}

with DAG(
    dag_id = '2_bash_operator_with_commands',
    default_args = default_args,
    description = 'This is actually my very first dag',
    start_date = datetime(2026,1,1,12),
    schedule = '@daily'
) as dag:
    task1 = BashOperator(
        task_id = 'first_task',
        bash_command = 'echo Hello Boysas'
    )

    task2 = BashOperator(
        task_id = 'second_task',
        bash_command = 'sleep 10'
    )

    task3 = BashOperator(
        task_id = 'third_task',
        bash_command = """

    echo "Current dir:"

    pwd

    echo "Temp dir contents:"

    ls -l

    echo "DAGs folder:"

    ls -l /opt/airflow/dags

    """
    )

    trigger_next = TriggerDagRunOperator(
        task_id="trigger_downstream",
        trigger_dag_id="3_Python_Operator"
    )


    task1 >> task2 >> task3  >> trigger_next

