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

    dbt_mart_monthly = BashOperator(
        task_id="dbt_mart_monthly_summary",
        bash_command=(
            f"{DBT_ENV} dbt run "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--select mart_monthly_summary"
        ),
    )

    dbt_mart_payment = BashOperator(
        task_id="dbt_mart_payment_breakdown",
        bash_command=(
            f"{DBT_ENV} dbt run "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--select mart_payment_breakdown"
        ),
    )

    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=(
            f"{DBT_ENV} dbt test "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--select mart_monthly_summary mart_payment_breakdown"
        ),
    )

    end = EmptyOperator(task_id="end")

    start >> [dbt_mart_monthly, dbt_mart_payment] >> dbt_test_marts >> end