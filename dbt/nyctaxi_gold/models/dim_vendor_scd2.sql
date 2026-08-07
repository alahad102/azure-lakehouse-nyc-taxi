with snap as (

    select * from {{ ref('snap_vendor_master') }}

)

select
    dbt_scd_id as vendor_sk,
    vendor_id,
    vendor_name,
    contract_tier,
    cast(dbt_valid_from as timestamp_ntz) as effective_start_date,
    cast(coalesce(dbt_valid_to, timestamp'9999-12-31') as timestamp_ntz) as effective_end_date,
    dbt_valid_to is null as is_current

from snap