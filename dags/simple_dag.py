from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}
dag = DAG(
    'hello_world_dag_PythonOperator',
    default_args=default_args,
    schedule_interval=None
    # schedule_interval=timedelta(minutes=10)
)

start = DummyOperator(task_id='run_this_first', dag=dag)

passing = KubernetesPodOperator(
    namespace='airflow',
    image="python:3.8",
    cmds=["python","-c"],
    arguments=["print('hello world')"],
    labels={"app": "airflow"},
    name="second-test",
    task_id="second-test",
    get_logs=True,
    dag=dag
)

#passing.set_upstream(start)
start >> passing