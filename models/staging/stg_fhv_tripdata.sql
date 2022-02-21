{{ config(materialized='view') }}

with tripdata as 
(
  select *,
    row_number() over(partition by dispatching_base_num, pickup_datetime) as rn
  from {{ source('staging','fhv_tripdata_external_table') }}
  where dispatching_base_num is not null 
)

-- dispatching_base_num	STRING	NULLABLE	
-- pickup_datetime	TIMESTAMP	NULLABLE	
-- dropoff_datetime	TIMESTAMP	NULLABLE	
-- PULocationID	INTEGER	NULLABLE	
-- DOLocationID	INTEGER	NULLABLE	
-- SR_Flag	INTEGER	NULLABLE	

select
    -- identifiers
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    cast(SUBSTRING (dispatching_base_num, 2) as integer) as vendorid,
    0 as ratecodeid,
    cast(PULocationID as integer) as  pickup_locationid,
    cast(DOLocationID as integer) as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    0 as store_and_fwd_flag,
    0 as passenger_count,
    0 as trip_distance,
    COALESCE(SR_Flag, 0 ) as trip_type,
    
    -- payment info
    0 as fare_amount,
    0 as extra,
    0 as mta_tax,
    0 as tip_amount,
    0 as tolls_amount,
    0 as ehail_fee,
    0 as improvement_surcharge,
    0 as total_amount,
    0 as payment_type,
    0 as payment_type_description, 
    0 as congestion_surcharge

from tripdata
where rn = 1

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}

