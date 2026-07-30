select
    cast(locationid as int) as location_key,
    borough,
    zone                    as zone_name,
    service_zone
from {{ source('bronze', 'bronze_taxi_zones') }}

union all

select
    -1        as location_key,
    'Unknown' as borough,
    'Unknown' as zone_name,
    'Unknown' as service_zone