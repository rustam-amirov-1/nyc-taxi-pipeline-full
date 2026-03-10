from airflow import DAG, Dataset
from airflow.operators.empty import EmptyOperator
from datetime import datetime

start_dataset = Dataset("nyc-taxi-pipeline/start_done")

with DAG(
    dag_id="dag_start",
    start_date=datetime(2024, 1, 1),
    schedule="0 0 * * 2",
    catchup=False,
    tags=["nyc_taxi_pipeline"],
) as dag:

    start = EmptyOperator(task_id="start")

    end = EmptyOperator(
        task_id="end",
        outlets=[start_dataset],
    )

    start >> end