import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, TimestampType, DoubleType
from dotenv import load_dotenv

JDBC_JAR_PATH = "/opt/airflow/drivers/postgresql-42.7.1.jar"
RAW_CSV = "/opt/airflow/data/raw/yellow_tripdata_sample_1000.csv"

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("TaxiTripsTransform")
        .config("spark.driver.memory", "4g")
        .config("spark.jars", JDBC_JAR_PATH)
        .getOrCreate()
    )

def transform(spark):
    print("[INFO] Reading raw CSV ...")
    df = spark.read.option("header", "true").csv(RAW_CSV)
    df = df.toDF(*[c.lower() for c in df.columns])
    cleaned = (
        df
        .withColumn("vendor_id",           F.col("vendorid").cast(IntegerType()))
        .withColumn("ratecode_id",          F.col("ratecodeid").cast(IntegerType()))
        .withColumn("pickup_datetime",      F.col("tpep_pickup_datetime").cast(TimestampType()))
        .withColumn("dropoff_datetime",     F.col("tpep_dropoff_datetime").cast(TimestampType()))
        .withColumn("passenger_count",      F.col("passenger_count").cast(IntegerType()))
        .withColumn("trip_distance",        F.col("trip_distance").cast(DoubleType()))
        .withColumn("pickup_longitude",     F.col("pickup_longitude").cast(DoubleType()))
        .withColumn("pickup_latitude",      F.col("pickup_latitude").cast(DoubleType()))
        .withColumn("dropoff_longitude",    F.col("dropoff_longitude").cast(DoubleType()))
        .withColumn("dropoff_latitude",     F.col("dropoff_latitude").cast(DoubleType()))
        .withColumn("fare_amount",          F.col("fare_amount").cast(DoubleType()))
        .withColumn("tip_amount",           F.col("tip_amount").cast(DoubleType()))
        .withColumn("tolls_amount",         F.col("tolls_amount").cast(DoubleType()))
        .withColumn("total_amount",         F.col("total_amount").cast(DoubleType()))
        .withColumn("payment_type",         F.col("payment_type").cast(IntegerType()))
        .withColumn("trip_duration_minutes",
            (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60.0)
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("total_amount") > 0)
        .filter(F.col("dropoff_datetime") > F.col("pickup_datetime"))
        .filter(F.col("payment_type").isin(1, 2, 3, 4))
        .filter(F.col("pickup_latitude").between(40.5, 40.9))
        .filter(F.col("pickup_longitude").between(-74.3, -73.7))
        .select(
            "vendor_id", "ratecode_id",
            "pickup_datetime", "dropoff_datetime",
            "passenger_count", "trip_distance", "trip_duration_minutes",
            "pickup_longitude", "pickup_latitude",
            "dropoff_longitude", "dropoff_latitude",
            "fare_amount", "tip_amount", "tolls_amount", "total_amount",
            "payment_type"
        )
    )
    print(f"[INFO] Transformed row count: {cleaned.count():,}")
    return cleaned

def save_postgres(df, jdbc_url, user, password) -> None:
    print("[INFO] Writing to PostgreSQL staging.stg_trips_spark ...")
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "staging.stg_trips_spark") \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .option("truncate", "true") \
        .save()
    print("[SUCCESS] Written to staging.stg_trips_spark")

def main():
    load_dotenv("/opt/airflow/.env")

    host     = os.getenv("DB_HOST")
    port     = os.getenv("DB_PORT")
    db       = os.getenv("DB_NAME")
    user     = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{db}"

    spark    = build_spark()
    df_clean = transform(spark)
    save_postgres(df_clean, jdbc_url, user, password)
    spark.stop()

if __name__ == "__main__":
    main()