from airflow import DAG, Dataset
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
from dotenv import load_dotenv
import urllib.request

load_dotenv()

start_dataset  = Dataset("nyc-taxi-pipeline/start_done")
bronze_dataset = Dataset("nyc-taxi-pipeline/bronze_done")

CSV_FILE_PATH = "/opt/airflow/data/raw/yellow_tripdata_sample_1000.csv"
JDBC_JAR_PATH = "/opt/airflow/drivers/postgresql-42.7.1.jar"
TARGET_TABLE  = "raw.taxi_trips"

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

JDBC_JAR_URL = "https://jdbc.postgresql.org/download/postgresql-42.7.1.jar"

def download_jar():
    os.makedirs(os.path.dirname(JDBC_JAR_PATH), exist_ok=True)
    if not os.path.exists(JDBC_JAR_PATH):
        print("Downloading JDBC jar...")
        urllib.request.urlretrieve(JDBC_JAR_URL, JDBC_JAR_PATH)
        print("Download complete.")
    else:
        print("JDBC jar already exists, skipping download.")


def load_to_bronze():
    download_jar()

    spark = SparkSession.builder \
        .appName("NYC Taxi Bronze Loader") \
        .master("local[*]") \
        .config("spark.jars", JDBC_JAR_PATH) \
        .getOrCreate()

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .csv(CSV_FILE_PATH)

    df = df.select([col(c).cast("string").alias(c.lower()) for c in df.columns])
    print(f"Rows read: {df.count()}")

    df.write \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", TARGET_TABLE) \
        .option("user", DB_USER) \
        .option("password", DB_PASS) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .option("truncate","true") \
        .save()

    print(f"Bronze load complete: {df.count()} rows loaded into {TARGET_TABLE}")
    spark.stop()

with DAG(
    dag_id="dag_bronze",
    start_date=datetime(2024, 1, 1),
    schedule=[start_dataset],
    catchup=False,
    tags=["nyc_taxi_pipeline"],
) as dag:

    start = EmptyOperator(task_id="start")

    bronze_task = PythonOperator(
        task_id="load_to_bronze",
        python_callable=load_to_bronze,
    )

    end = EmptyOperator(
        task_id="end",
        outlets=[bronze_dataset],
    )

    start >> bronze_task >> end