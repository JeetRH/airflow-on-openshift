from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator

def helloWorld():
    print("Hello World")

# This is a demo

default_args = {
    'owner': 'jeet'
}

with DAG(
    'hello_world_dag_PythonOperator',
    default_args=default_args,
    start_date=datetime(2023,9,1),
    schedule_interval="@hourly",
    catchup=False
) as dag:

    start = DummyOperator(task_id='run_this_first')

    python_task = PythonOperator(
            task_id="hello_world",
            python_callable=helloWorld)

start >> python_task