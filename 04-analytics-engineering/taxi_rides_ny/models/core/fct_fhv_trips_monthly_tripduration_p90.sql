{{ config(materialized='table') }}

with valid_trips AS (
select 
    *,
    timestamp_diff(dropOff_datetime, pickup_datetime, second) as trip_duration
from {{ ref('dim_fhv_trips') }}
)

SELECT DISTINCT
    *,
    PERCENTILE_CONT(trip_duration, 0.90) OVER (PARTITION BY year, month, PUlocationID, DOlocationID) AS p90

FROM valid_trips
