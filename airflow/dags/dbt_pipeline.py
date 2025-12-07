from __future__ import annotations
import os
from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)

DBT_PROJECT_DIR = "/opt/airflow/dbt"

with DAG(
    dag_id="dbt_weather_pipeline",
    schedule_interval="0 */3 * * *",  # Каждые 3 часа
    start_date=days_ago(1),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["dbt", "weather", "analytics"],
) as dag:
    
    t_dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps",
    )
    
    t_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run",
    )
    
    t_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test",
    )
    
    t_dbt_docs = BashOperator(
        task_id="dbt_docs",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt docs generate",
    )
    
    t_dbt_deps >> t_dbt_run >> t_dbt_test >> t_dbt_docs