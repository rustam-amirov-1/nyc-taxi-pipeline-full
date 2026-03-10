{{
  config(
    materialized='table'
  )
}}

SELECT
    t.vendor_id,
    t.ratecode_id,
    t.pickup_datetime,
    t.dropoff_datetime,
    t.passenger_count,
    t.trip_distance,
    t.total_amount,
    t.tip_amount,
    t.payment_type,
    t.trip_duration_minutes,
    t.time_of_day,
    t.trip_type
FROM {{ ref('stg_trips') }} t