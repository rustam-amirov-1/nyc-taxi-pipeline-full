from airflow import DAG, Dataset
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

bronze_dataset = Dataset("nyc-taxi-pipeline/bronze_done")
silver_dataset = Dataset("nyc-taxi-pipeline/silver_done")

DBT_PROJECT_DIR  = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"
DBT_ENV          = "DBT_HOST=postgres"

with DAG(
    dag_id="dag_silver",
    start_date=datetime(2024, 1, 1),
    schedule=[bronze_dataset],
    catchup=False,
    tags=["nyc_taxi_pipeline"],
) as dag:

    start = EmptyOperator(task_id="start")

    dbt_stg_trips = BashOperator(
        task_id="dbt_stg_trips",
        bash_command=(
            f"{DBT_ENV} dbt run "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--select stg_trips"
        ),
    )

    dbt_stg_payments = BashOperator(
        task_id="dbt_stg_payments",
        bash_command=(
            f"{DBT_ENV} dbt run "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--select stg_payments"
        ),
    )

    dbt_test_silver = BashOperator(
        task_id="dbt_test_silver",
        bash_command=(
            f"{DBT_ENV} dbt test "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--select stg_trips stg_payments"
        ),
        trigger_rule=TriggerRule.ALL_DONE,
    )

    end = EmptyOperator(
        task_id="end",
        outlets=[silver_dataset],
        trigger_rule=TriggerRule.ALL_DONE,
    )

    start >> [dbt_stg_trips, dbt_stg_payments] >> dbt_test_silver >> end