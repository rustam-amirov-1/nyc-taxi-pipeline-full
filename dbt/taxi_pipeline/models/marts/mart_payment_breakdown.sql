-- models/marts/mart_payment_breakdown.sql

{{ config(materialized='table') }}

SELECT
    payment_type,
    CASE payment_type
        WHEN 1 THEN 'Credit Card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No Charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        WHEN 6 THEN 'Voided Trip'
        ELSE 'Other'
    END                             AS payment_label,
    COUNT(*)                        AS total_trips,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_trips,
    SUM(total_amount)               AS total_revenue,
    AVG(tip_amount)                 AS avg_tip

FROM {{ ref('fct_trips') }}
GROUP BY payment_type
ORDER BY total_trips DESC