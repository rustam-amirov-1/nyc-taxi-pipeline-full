{{
  config(
    materialized='table'
  )
}}

SELECT
    DATE_TRUNC('month', t.pickup_datetime)  AS ride_month,
    v.vendor_name,
    r.rate_description,
    COUNT(*)                                AS ride_count,
    ROUND(AVG(t.total_amount), 2)           AS avg_total_amount,
    ROUND(AVG(t.trip_distance), 2)          AS avg_trip_distance
FROM {{ ref('fct_trips') }} t
LEFT JOIN {{ ref('dim_vendor') }} v ON t.vendor_id = v.vendor_id
LEFT JOIN {{ ref('dim_rate') }} r ON t.ratecode_id = r.ratecode_id
GROUP BY 1, 2, 3
ORDER BY 1