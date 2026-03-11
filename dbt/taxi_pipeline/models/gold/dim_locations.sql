{{
  config(
    materialized='table',
    schema='marts'
  )
}}

select * from {{ ref('taxi_zones') }}