{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'taxi_trips') }}
),
cleaned AS (
    SELECT
        -- IDs
        vendorid::INTEGER                              AS vendor_id,
        ratecodeid::INTEGER                            AS ratecode_id,

        -- Timestamps
        tpep_pickup_datetime::TIMESTAMP                AS pickup_datetime,
        tpep_dropoff_datetime::TIMESTAMP               AS dropoff_datetime,

        -- Coordinates
        pickup_longitude::NUMERIC(10, 6)               AS pickup_longitude,
        pickup_latitude::NUMERIC(10, 6)                AS pickup_latitude,
        dropoff_longitude::NUMERIC(10, 6)              AS dropoff_longitude,
        dropoff_latitude::NUMERIC(10, 6)               AS dropoff_latitude,

        -- Numeric measures
        passenger_count::INTEGER                       AS passenger_count,
        trip_distance::NUMERIC(10, 2)                  AS trip_distance,
        fare_amount::NUMERIC(10, 2)                    AS fare_amount,
        extra::NUMERIC(10, 2)                          AS extra,
        mta_tax::NUMERIC(10, 2)                        AS mta_tax,
        tip_amount::NUMERIC(10, 2)                     AS tip_amount,
        tolls_amount::NUMERIC(10, 2)                   AS tolls_amount,
        improvement_surcharge::NUMERIC(10, 2)          AS improvement_surcharge,
        total_amount::NUMERIC(10, 2)                   AS total_amount,

        -- Categoricals
        payment_type::INTEGER                          AS payment_type,
        store_and_fwd_flag                             AS store_and_fwd_flag,

        -- Derived: trip type
        CASE
            WHEN trip_distance::NUMERIC <= 3 THEN 'short_trip'
            WHEN trip_distance::NUMERIC <= 8 THEN 'medium_trip'
            WHEN trip_distance::NUMERIC > 8  THEN 'long_trip'
        END                                            AS trip_type,

        -- Derived: trip duration in minutes
        EXTRACT(EPOCH FROM (
            tpep_dropoff_datetime::TIMESTAMP - tpep_pickup_datetime::TIMESTAMP
        )) / 60                                        AS trip_duration_minutes,

        -- Derived: time of day
        CASE
            WHEN EXTRACT(HOUR FROM tpep_pickup_datetime::TIMESTAMP) BETWEEN 6 AND 11  THEN 'morning'
            WHEN EXTRACT(HOUR FROM tpep_pickup_datetime::TIMESTAMP) BETWEEN 12 AND 17 THEN 'afternoon'
            WHEN EXTRACT(HOUR FROM tpep_pickup_datetime::TIMESTAMP) BETWEEN 18 AND 22 THEN 'evening'
            ELSE 'night'
        END                                            AS time_of_day,

        -- Derived: pickup location zone
        CASE
            WHEN pickup_latitude::NUMERIC BETWEEN 40.50 AND 40.65 AND pickup_longitude::NUMERIC BETWEEN -74.30 AND -74.00 THEN 1
            WHEN pickup_latitude::NUMERIC BETWEEN 40.50 AND 40.65 AND pickup_longitude::NUMERIC BETWEEN -74.00 AND -73.85 THEN 2
            WHEN pickup_latitude::NUMERIC BETWEEN 40.50 AND 40.65 AND pickup_longitude::NUMERIC BETWEEN -73.85 AND -73.70 THEN 3
            WHEN pickup_latitude::NUMERIC BETWEEN 40.65 AND 40.75 AND pickup_longitude::NUMERIC BETWEEN -74.30 AND -74.00 THEN 4
            WHEN pickup_latitude::NUMERIC BETWEEN 40.65 AND 40.75 AND pickup_longitude::NUMERIC BETWEEN -74.00 AND -73.85 THEN 5
            WHEN pickup_latitude::NUMERIC BETWEEN 40.65 AND 40.75 AND pickup_longitude::NUMERIC BETWEEN -73.85 AND -73.70 THEN 6
            WHEN pickup_latitude::NUMERIC BETWEEN 40.75 AND 40.90 AND pickup_longitude::NUMERIC BETWEEN -74.30 AND -74.00 THEN 7
            WHEN pickup_latitude::NUMERIC BETWEEN 40.75 AND 40.90 AND pickup_longitude::NUMERIC BETWEEN -74.00 AND -73.85 THEN 8
            WHEN pickup_latitude::NUMERIC BETWEEN 40.75 AND 40.90 AND pickup_longitude::NUMERIC BETWEEN -73.85 AND -73.70 THEN 9
            ELSE 0
        END                                            AS pickup_location_id

    FROM source
    WHERE
        tpep_pickup_datetime IS NOT NULL
        AND tpep_dropoff_datetime IS NOT NULL
        AND trip_distance <> 'nan'
        AND total_amount <> 'nan'
        AND fare_amount <> 'nan'
        AND total_amount::NUMERIC > 0
        AND trip_distance::NUMERIC > 0
        AND passenger_count::INTEGER > 0
        AND ratecodeid::INTEGER != 99
        AND tpep_pickup_datetime::TIMESTAMP < tpep_dropoff_datetime::TIMESTAMP
        AND payment_type::INTEGER in (1,2,3,4)
        AND pickup_latitude::NUMERIC BETWEEN 40.5 AND 40.9
        AND pickup_longitude::NUMERIC BETWEEN -74.3 AND -73.7
)
SELECT * FROM cleaned