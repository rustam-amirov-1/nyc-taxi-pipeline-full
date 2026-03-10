-- init_db.sql
-- Runs inside the 'airflow' database (created by Docker automatically)

-- BRONZE LAYER
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.taxi_trips (
    vendor_id                VARCHAR(10),
    tpep_pickup_datetime     VARCHAR(30),
    tpep_dropoff_datetime    VARCHAR(30),
    passenger_count          VARCHAR(10),
    trip_distance            VARCHAR(20),
    pickup_longitude         VARCHAR(20),
    pickup_latitude          VARCHAR(20),
    ratecode_id              VARCHAR(10),
    store_and_fwd_flag       VARCHAR(5),
    dropoff_longitude        VARCHAR(20),
    dropoff_latitude         VARCHAR(20),
    payment_type             VARCHAR(10),
    fare_amount              VARCHAR(20),
    extra                    VARCHAR(20),
    mta_tax                  VARCHAR(20),
    tip_amount               VARCHAR(20),
    tolls_amount             VARCHAR(20),
    improvement_surcharge    VARCHAR(20),
    total_amount             VARCHAR(20)
);

-- SILVER LAYER
CREATE SCHEMA IF NOT EXISTS staging;

-- GOLD LAYER
CREATE SCHEMA IF NOT EXISTS marts;