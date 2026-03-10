# NYC Taxi Data Engineering Pipeline

## What is this project?

This project is a data engineering pipeline built on the NYC Taxi dataset. I built it during my first week of learning data engineering.I had no background in this field before starting.

## What I did not know before

Before this project, I had no idea what data engineers actually do or which tools they use. I did not know what a DAG was, I had never heard of dbt, and I did not understand why someone would use Spark instead of just Python. After going through this project step by step, all of these things started to make sense to me.

## How the pipeline works

First, the NYC Taxi CSV file is loaded into PostgreSQL using pandas inside the Airflow ingestion task. This became my raw layer — also called the Bronze layer. The data at this stage is untouched, exactly as it came from the source.

After that, I used dbt to clean and transform the raw data. I filtered out bad rows like negative fares, zero distances and invalid timestamps. I also added a trip_type column that categorizes each trip as short, medium or long, a trip_duration_minutes column calculated from the pickup and dropoff times, and a time_of_day column that labels each trip as morning, afternoon, evening or night. This became the Silver layer. After the model runs, dbt tests check the data before moving on.

Then I created dimension tables and a fact table — this is the Gold layer. The dim_vendor table holds vendor names, dim_rate holds rate code names, and fct_trips is the main fact table that connects everything together.

Finally I built the mart layer which contains two aggregation tables. mart_monthly_summary shows the number of rides, average fare and average distance grouped by month, vendor and rate type. mart_payment_breakdown breaks down trips by payment method.The hardest part of the whole project was automating all of these steps with Airflow. Getting the tasks to run in the right order and making the DAG actually work inside Docker took the most time and effort.

## Tools used

- **PySpark** — to read the CSV and load data into PostgreSQL
- **PostgreSQL** — the database where all layers are stored
- **dbt** — to transform and model the data with SQL
- **Apache Airflow** — to automate and orchestrate the pipeline
- **Docker** — to run Airflow and PostgreSQL in containers
- **Python / Jupyter Notebook** — for the initial Spark work


---

## Project structure

```
nyc-taxi-pipeline/
├── airflow/
│   └── dags/
│       └── yellow_taxi_pipeline.py
├── data/
│   └── raw/
│       └── yellow_tripdata_sample_1000.csv
├── dbt/
│   └── taxi_pipeline/
│       ├── models/
│       │   ├── silver/     ← stg_trips.sql
│       │   ├── gold/       ← fct_trips.sql
│       │   └── marts/      ← mart_monthly_summary.sql, mart_payment_breakdown.sql
│       ├── seeds/
│       │   ├── dim_vendor.csv
│       │   └── dim_rate.csv
│       └── dbt_project.yml
├── load_data_via_spark/
│   ├── load_raw.py
│   └── init_db.sql
├── drivers/
└── docker-compose.yml
```

---


---

## How to run

**Requirements:** Docker, Docker Compose, Python, dbt, PySpark

```bash
# 1. Start containers
docker-compose up -d

# 2. Place the CSV file in data/raw/

# 3. Open Airflow UI at http://localhost:8080
#    (default credentials: airflow / airflow)

# 4. Trigger the yellow_taxi_pipeline DAG
```

The DAG handles everything from ingestion through to the mart layer automatically.

---

## Dataset

NYC Taxi 2015 dataset was used for this project.
You can download it from Kaggle:
https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data