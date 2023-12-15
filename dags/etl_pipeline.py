import pandas as pd
import numpy as np

from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


default_args = {
   'owner': 'jeet'
}

def remove_nulls(obj):
    if isinstance(obj, (list, tuple)):
        return [remove_nulls(item) for item in obj if item is not None]
    elif isinstance(obj, dict):
        return {key: remove_nulls(value) for key, value in obj.items() if value is not None}
    else:
        return obj


def read_csv_file():
    key = 'insurance.csv'
    s3_hook = S3Hook(aws_conn_id='minio')
    # df = s3_hook.read_key(
    #     key,
    #     bucket_name='airflow'
    # )

    df2 = s3_hook.select_key(
        key,
        bucket_name='airflow',
        input_serialization = {'CSV': {'FileHeaderInfo': 'USE'}},
        output_serialization = {'JSON': {}}
    )

    # print(df[0:50])

    print(type(df2))

    # return df.to_json()
    return df2


def remove_null_values(ti):
    json_data = ti.xcom_pull(task_ids='read_csv_file')

    cleaned_data = remove_nulls(json_data)

    return cleaned_data


def groupby_smoker(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data, lines=True)

    # Lets clean out our dataframe more
    # Replace "null" with NaN
    df.replace('null', np.nan, inplace=True)

    # Remove rows with NaN values
    df_cleaned = df.dropna()

    smoker_df = df_cleaned.groupby('smoker').agg({
        'age': ['min', 'max'], 
        'bmi': ['min', 'max'],
        'children': ['min', 'max']
    })

    print(smoker_df.head(50))
    
    s3_hook = S3Hook(aws_conn_id='minio')
    s3_hook.load_file(smoker_df.to_csv, 'smoker_df.csv', bucket_name='airflow', replace=True, encrypt=False)
    # smoker_df.to_csv(
    #     './output/grouped_by_smoker.csv', index=False)


def groupby_region(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data, lines=True)

    # Lets clean out our dataframe more
    # Replace "null" with NaN
    df.replace('null', np.nan, inplace=True)

    # Remove rows with NaN values
    df_cleaned = df.dropna()

    region_df = df_cleaned.groupby('region').agg({
        'age': ['min', 'max'], 
        'bmi': ['min', 'max'], 
        'children': ['min', 'max']
    })
    
    print(region_df.head(50))

    s3_hook = S3Hook(aws_conn_id='minio')
    s3_hook.load_file(region_df.to_csv, 'region_df.csv', bucket_name='airflow', replace=True, encrypt=False)
    # region_df.to_csv(
    #     './output/grouped_by_region.csv', index=False)


with DAG(
    dag_id = 'python_pipeline',
    description = 'Running a Python pipeline',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['python', 'transform', 'pipeline','xcom']
) as dag:
    
    read_csv_file = PythonOperator(
        task_id='read_csv_file',
        python_callable=read_csv_file
    )

    remove_null_values = PythonOperator(
        task_id='remove_null_values',
        python_callable=remove_null_values
    )
    
    groupby_smoker = PythonOperator(
        task_id='groupby_smoker',
        python_callable=groupby_smoker
    )
    
    groupby_region = PythonOperator(
        task_id='groupby_region',
        python_callable=groupby_region
    )

read_csv_file >> remove_null_values >> [groupby_smoker, groupby_region]


