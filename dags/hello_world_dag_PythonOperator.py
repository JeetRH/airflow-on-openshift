from airflow import DAG
from datetime import datetime, timedelta
# from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator

def helloWorld():
    print("Hello World")

default_args = {
    'owner': 'airflow_python'
}

with DAG(
    'hello_world_dag_PythonOperator',
    start_date=datetime(2023,9,1),
    schedule_interval="@hourly",
    catchup=False
) as dag:

    start = DummyOperator(task_id='run_this_first')

    python_task = PythonOperator(
            task_id="hello_world",
            python_callable=helloWorld)

start >> python_task