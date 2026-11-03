-- One row per calendar day for 1000 days from var('calendar_start').
-- Mirrors recon.business_days: business days are Monday to Friday, with no holiday calendar.
-- business_day_index counts business days up to and including date_day, so for any date d:
--   settlement deadline = the business day whose index is index(d) + window   (T+2, inclusive)
--   age in business days = index(as_of) - index(d)
-- A weekend day shares the index of the Friday before it, which is what makes Sat/Sun + 2 = Tuesday.

with digits as (
    select 0 as n union all select 1 union all select 2 union all select 3 union all select 4
    union all select 5 union all select 6 union all select 7 union all select 8 union all select 9
),

day_offsets as (
    select ones.n + 10 * tens.n + 100 * hundreds.n as day_offset
    from digits as ones
    cross join digits as tens
    cross join digits as hundreds
),

days as (
    select
        cast({{ dbt.dateadd('day', 'day_offset', "cast('" ~ var('calendar_start') ~ "' as date)") }} as date)
            as date_day
    from day_offsets
),

flagged as (
    select
        date_day,
        {{ iso_dow('date_day') }} as iso_dow
    from days
)

select
    date_day,
    iso_dow,
    iso_dow <= 5 as is_business_day,
    sum(case when iso_dow <= 5 then 1 else 0 end)
        over (order by date_day rows between unbounded preceding and current row) as business_day_index
from flagged
