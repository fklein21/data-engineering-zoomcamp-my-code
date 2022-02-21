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
    PULocationID as pickup_locationid,
    cast(DOLocationID as integer) as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    'N' as store_and_fwd_flag,
    0 as passenger_count,
    cast(0. as numeric) as trip_distance,
    COALESCE(SR_Flag, 0 ) as trip_type,
    
    -- payment info
    cast(0. as numeric) as fare_amount,
    cast(0. as numeric) as extra,
    cast(0. as numeric) as mta_tax,
    cast(0. as numeric) as tip_amount,
    cast(0. as numeric) as tolls_amount,
    cast(0. as numeric) as ehail_fee,
    cast(0. as numeric) as improvement_surcharge,
    cast(0. as numeric) as total_amount,
    0 as payment_type,
    'Unknown' as payment_type_description, 
    cast(0. as numeric) as congestion_surcharge

from tripdata
where rn = 1
and DOLocationID is not null
and PULocationID is not null

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}

