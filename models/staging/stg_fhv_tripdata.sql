{{ config(materialized='view') }}
 

select
   -- identifiers
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    cast(dispatching_base_num as string) as dispatching_base_num,
    cast(pulocationid as integer) as  pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    cast(SR_Flag as float64) as sr_flag,
    cast(Affiliated_base_number as string) as affiliated_base_number
from {{ source('staging','fhv_tripdata') }}
where date(pickup_datetime) >= Date(2019,01,01) and date(pickup_datetime) <= Date(2019, 12, 31)

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}