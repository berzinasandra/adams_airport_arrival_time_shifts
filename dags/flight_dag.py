import datetime
import os
import pathlib

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

from src.test import say_hello

with DAG(
    dag_id="flight-pipeline",
    start_date=datetime.datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    start_job = DummyOperator(
        task_id = "start"
    )
    extract_job = PythonOperator(
        task_id="extract_zip_file",
        python_callable= say_hello
    )

    start_job >> extract_job
