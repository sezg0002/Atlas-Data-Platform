
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from etl.run_etl import run_pipeline
from validation.run_ge_checks import run_ge_validation
from dbt_project.run_dbt import run_dbt_models

default_args = {
    "owner": "harun",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="gdi_full_pipeline",
    default_args=default_args,
    description="Global Data Intelligence: ETL + GE + dbt",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gdi", "etl", "quality", "dbt"],
) as dag:

    etl_task = PythonOperator(
        task_id="run_etl",
        python_callable=run_pipeline,
    )

    ge_task = PythonOperator(
        task_id="run_ge_validation",
        python_callable=run_ge_validation,
    )

    dbt_task = PythonOperator(
        task_id="run_dbt_models",
        python_callable=run_dbt_models,
    )

    etl_task >> ge_task >> dbt_task
