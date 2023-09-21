from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'jeet'
    # 'depends_on_past': False,
    # 'start_date': datetime.utcnow(),
    # 'email': ['airflow@example.com'],
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5)
}
with DAG(
    'kubernetes_pod_operator',
    default_args=default_args,
    start_date=datetime(2023,8,31),
    schedule_interval=None
    # schedule_interval=timedelta(minutes=10)
) as dag:

    start = DummyOperator(task_id='run_this_first', dag=dag)

    passing = KubernetesPodOperator(
        # namespace='airflow',
        image="python:3.8",
        cmds=["python","-c"],
        arguments=["print('hello world')"],
        labels={"app": "airflow"},
        name="passing-test",
        task_id="passing-task",
        get_logs=True,
        dag=dag
    )

start >> passing