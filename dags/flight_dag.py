import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.db import provide_session
from airflow.models import XCom

from tasks.extract import extract_data_from_api
from tasks.mongo_db import upload_to_mongo
from tasks.transform import save_arrival_time_shifts

args = {
    'start_date': datetime.datetime(2022, 9, 1),
    'end_date': datetime.datetime(2022, 9, 7),
    'depends_on_past': True,
    'wait_for_downstream': True,
}

@provide_session
def clean_xcom(session=None):
    session.query(XCom).filter(XCom.key == "flights_raw").delete()

with DAG(
    dag_id="flight-pipeline",
    schedule_interval="@daily",
    default_args=args,
    max_active_runs=1,
    max_active_tasks=1,
    concurrency= 1,
    catchup=True
) as dag:

    start_job = DummyOperator(
        task_id = "start"
    )
    extract_job = PythonOperator(
        task_id="extract_data",
        python_callable= extract_data_from_api
    )

    upload_to_db_job = PythonOperator(
        task_id="upload_to_db",
        python_callable= upload_to_mongo
    )

    save_arrival_time_shifts_job = PythonOperator(
        task_id="save_arrival_time_shifts",
        python_callable= save_arrival_time_shifts
    )

    clean_xcom_job = PythonOperator(
        task_id="clean_xcom",
        python_callable= clean_xcom
    )

    end_job = DummyOperator(
        task_id = "end"
    )

    start_job >> extract_job >> upload_to_db_job >> save_arrival_time_shifts_job >> clean_xcom_job >> end_job
 