{{ config(materialized='table') }}


select 
    fhv_tripdata.tripid, 
    'FHV' as service_type,
    fhv_tripdata.pickup_locationid, 
    fhv_tripdata.dropoff_locationid,
    fhv_tripdata.pickup_datetime, 
    fhv_tripdata.dropoff_datetime, 
    fhv_tripdata.sr_flag,
    fhv_tripdata.affiliated_base_number
from {{ ref('stg_fhv_tripdata') }} as fhv_tripdata