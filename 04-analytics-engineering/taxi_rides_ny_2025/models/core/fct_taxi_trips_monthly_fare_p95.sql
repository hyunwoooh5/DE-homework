{{ config(materialized='table') }}

with valid_trips AS (
select 
    service_type,
    EXTRACT(YEAR FROM pickup_datetime) as year,
    EXTRACT(MONTH FROM pickup_datetime) as month,
    fare_amount

from {{ ref('fact_trips') }}
WHERE fare_amount>0 AND trip_distance >0 and payment_type_description in ('Cash', 'Credit card')
)

SELECT DISTINCT
    service_type,
    year,
    month,
    PERCENTILE_CONT(fare_amount, 0.97) OVER (PARTITION BY service_type, year, month) AS p97,
    PERCENTILE_CONT(fare_amount, 0.95) OVER (PARTITION BY service_type, year, month) AS p95,
    PERCENTILE_CONT(fare_amount, 0.90) OVER (PARTITION BY service_type, year, month) AS p90,
FROM valid_trips