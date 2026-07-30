with bounds as (
    select
        date(min(pickup_datetime)) as min_date,
        date(max(pickup_datetime)) as max_date
    from {{ source('silver', 'silver_trips') }}
),

date_spine as (
    select explode(sequence(min_date, max_date, interval 1 day)) as date_day
    from bounds
)

select
    cast(date_format(date_day, 'yyyyMMdd') as int) as date_key,
    date_day                                        as full_date,
    year(date_day)                                  as year,
    quarter(date_day)                               as quarter,
    month(date_day)                                 as month,
    date_format(date_day, 'MMMM')                   as month_name,
    day(date_day)                                   as day_of_month,
    dayofweek(date_day)                             as day_of_week,
    date_format(date_day, 'EEEE')                   as day_name,
    weekofyear(date_day)                            as week_of_year,
    dayofweek(date_day) in (1, 7)                   as is_weekend
from date_spine