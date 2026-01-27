from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook


default_args = {
    "owner": "sahibjotsingh", 
    "depends_on_past": False,
    "retries": 3,
}

with DAG(
    dag_id="job_market_pipeline",
    description="End-to-end job market ETL pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["etl", "postgres"],
) as dag:

    clean_data = BashOperator(
        task_id="clean_data",
        bash_command="python -u /opt/airflow/scripts/clean.py",
    )

    load_to_staging = BashOperator(
        task_id="load_to_staging",
        bash_command="python -u /opt/airflow/scripts/load_to_staging_db.py",
        env={
            
             "JOB_MARKET_DB_URL": BaseHook.get_connection(
            "job_market_postgres"
        ).get_uri()
        },
    )

    transform_data = BashOperator(
        task_id="transform_data",
        bash_command="python -u /opt/airflow/scripts/transform.py",
    )

    clean_data >> load_to_staging >> transform_data
