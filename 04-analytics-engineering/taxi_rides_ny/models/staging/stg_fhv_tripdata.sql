with source as (
    select * from {{ source('raw', 'fhv_tripdata') }}
),

renamed as (
    select
        cast(dispatching_base_num as string) as dispatching_base_num,

        -- timestamps
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropOff_datetime as timestamp) as dropoff_datetime,

        -- identifiers
        cast(PUlocationID as numeric) as pickup_location_id,
        cast(DOlocationID as numeric) as dropoff_location_id,


        cast(SR_Flag as numeric) as SR_Flag,
        cast(Affiliated_base_number as string) as Affiliated_base_number


       
    from source
    -- Filter out records with null vendor_id (data quality requirement)
    where dispatching_base_num IS NOT NULL
)

select * from renamed

-- Sample records for dev environment using deterministic date filter
{% if target.name == 'dev' %}
where pickup_datetime >= '2019-01-01' and pickup_datetime < '2019-02-01'
{% endif %}
