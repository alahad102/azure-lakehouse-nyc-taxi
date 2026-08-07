-- SCD2 invariant: for any vendor, no two versions may be valid simultaneously.
-- An overlap would cause a point-in-time join to match multiple dimension rows
-- for a single fact row, silently double-counting revenue.
-- Returns offending pairs; the test passes only when zero rows come back.

select
    a.vendor_id,
    a.vendor_sk              as version_a,
    b.vendor_sk              as version_b,
    a.effective_start_date   as a_start,
    a.effective_end_date     as a_end,
    b.effective_start_date   as b_start,
    b.effective_end_date     as b_end

from {{ ref('dim_vendor_scd2') }} a
join {{ ref('dim_vendor_scd2') }} b
  on  a.vendor_id = b.vendor_id
  and a.vendor_sk < b.vendor_sk
  and a.effective_start_date < b.effective_end_date
  and b.effective_start_date < a.effective_end_date