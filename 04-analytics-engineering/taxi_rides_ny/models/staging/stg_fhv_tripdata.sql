-- Transform and materialize data from a source table

{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select *
  from {{ source('staging','fhv_tripdata_2019') }}
  WHERE dispatching_base_num IS NOT NULL
)
select
    cast(dispatching_base_num as string) as dispatching_base_num,
    timestamp_micros(cast(pickup_datetime / 1000 as int64)) as pickup_datetime,
    timestamp_micros(cast(dropOff_datetime / 1000 as int64)) as dropOff_datetime,
    cast(PUlocationID as numeric) as PUlocationID,
    cast(DOlocationID as numeric) as DOlocationID,
    cast(SR_Flag as numeric) as SR_Flag,
    cast(Affiliated_base_number as string) as Affiliated_base_number

from tripdata

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}