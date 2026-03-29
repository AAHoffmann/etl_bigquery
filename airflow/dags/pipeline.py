from airflow import DAG
from airflow.operators.python import PythonOperator  # type: ignore
from datetime import datetime

from src.extract.pipeline_api_extract import main as extract_main_api

default_args = {
    "owner" : "amandio",
    "start_date" : datetime(2026,3,29),
}

def run_extract():
    extract_main_api()


with DAG(
    dag_id="pipeline",
    default_args=default_args,
    schedule = None,
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id = "extract_api",
        python_callable = run_extract
    )
