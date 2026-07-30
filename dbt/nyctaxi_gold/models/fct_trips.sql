select
    -- foreign keys
    cast(date_format(pickup_datetime, 'yyyyMMdd') as int) as pickup_date_key,
    coalesce(pu_location_id, -1)                          as pu_location_key,
    coalesce(do_location_id, -1)                          as do_location_key,
    vendor_id                                             as vendor_key,

    -- degenerate dimensions
    service_type,
    payment_type,
    ratecode_id,

    -- timestamps
    pickup_datetime,
    dropoff_datetime,

    -- measures
    trip_distance,
    passenger_count,
    round((unix_timestamp(dropoff_datetime)
         - unix_timestamp(pickup_datetime)) / 60.0, 2) as trip_duration_minutes,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    total_amount,

    -- quality flags carried through from silver
    passenger_count_missing,
    is_reversal
from {{ source('silver', 'silver_trips') }}