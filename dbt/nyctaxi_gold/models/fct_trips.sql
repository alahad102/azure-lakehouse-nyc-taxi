with trips as (

    select * from {{ source('silver', 'silver_trips') }}

)

select
    -- foreign keys
    cast(date_format(t.pickup_datetime, 'yyyyMMdd') as int) as pickup_date_key,
    coalesce(t.pu_location_id, -1)                          as pu_location_key,
    coalesce(t.do_location_id, -1)                          as do_location_key,
    t.vendor_id                                             as vendor_key,
    v.vendor_sk                                             as vendor_version_key,

    -- degenerate dimensions
    t.service_type,
    t.payment_type,
    t.ratecode_id,

    -- timestamps
    t.pickup_datetime,
    t.dropoff_datetime,

    -- measures
    t.trip_distance,
    t.passenger_count,
    round((unix_timestamp(t.dropoff_datetime)
         - unix_timestamp(t.pickup_datetime)) / 60.0, 2)    as trip_duration_minutes,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.improvement_surcharge,
    t.congestion_surcharge,
    t.total_amount,

    -- quality flags carried through from silver
    t.passenger_count_missing,
    t.is_reversal

from trips t
left join {{ ref('dim_vendor_scd2') }} v
  on  t.vendor_id = v.vendor_id
  and t.pickup_datetime >= v.effective_start_date
  and t.pickup_datetime <  v.effective_end_date