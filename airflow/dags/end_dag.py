from airflow import DAG, Dataset
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

gold_dataset = Dataset("nyc-taxi-pipeline/gold_done")

DBT_PROJECT_DIR  = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"
DBT_ENV          = "DBT_HOST=postgres"

with DAG(
    dag_id="dag_end",
    start_date=datetime(2024, 1, 1),
    schedule=[gold_dataset],
    catchup=False,
    tags=["nyc_taxi_pipeline"],
) as dag:

    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end")

    start >> end