with vendors as (
    select distinct vendor_id as vendor_key
    from {{ source('silver', 'silver_trips') }}
    where vendor_id is not null
)

select
    vendor_key,
    case vendor_key
        when 1 then 'Creative Mobile Technologies, LLC'
        when 2 then 'VeriFone Inc.'
        else 'Unknown vendor'
    end as vendor_name
from vendors