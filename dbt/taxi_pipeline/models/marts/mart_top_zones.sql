{{
  config(
    materialized='table',
    schema='marts'
  )
}}

select
    l.location_id,
    l.zone_name,
    l.lat_min,
    l.lat_max,
    l.lon_min,
    l.lon_max,
    count(*) as pickup_count,
    round(avg(t.total_amount), 2) as avg_total_amount,
    round(avg(t.trip_distance), 2) as avg_trip_distance
from {{ ref('fct_trips') }} t
left join {{ ref('dim_locations') }} l
    on t.pickup_location_id = l.location_id
group by 1, 2, 3, 4, 5, 6
order by pickup_count desc
limit 5