{{
    config(
        materialized='table'
    )
}}

with tripdata as (
    select * from {{ ref('stg_fhv_tripdata') }}
), 

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

SELECT
    tripdata.dispatching_base_num,
    tripdata.pickup_datetime,
    tripdata.dropOff_datetime,
    EXTRACT(YEAR FROM tripdata.pickup_datetime) AS year,
    EXTRACT(MONTH FROM tripdata.pickup_datetime) AS month,
    tripdata.PUlocationID,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    tripdata.DOlocationID,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    tripdata.SR_Flag,
    tripdata.Affiliated_base_number
FROM tripdata
INNER JOIN dim_zones as pickup_zone
ON tripdata.PUlocationID = pickup_zone.locationid
INNER JOIN dim_zones as dropoff_zone
ON tripdata.DOlocationID = dropoff_zone.locationid
