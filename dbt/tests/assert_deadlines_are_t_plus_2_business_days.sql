-- Deadlines fall on a business day, at least 2 calendar days after the charge, and with exactly
-- 2 business days in (charge_date, deadline]. Mirrors recon.business_days.settlement_deadline.

with calendar as (
    select * from {{ ref('int_business_calendar') }}
)

select charges.charge_id
from {{ ref('int_processor_charges') }} as charges
inner join calendar as charge_day
    on charges.charge_date_utc = charge_day.date_day
inner join calendar as deadline_day
    on charges.settlement_deadline = deadline_day.date_day
where not deadline_day.is_business_day
    or charges.settlement_deadline < {{ dbt.dateadd('day', 2, 'charges.charge_date_utc') }}
    or deadline_day.business_day_index - charge_day.business_day_index <> {{ var('settlement_window_business_days') }}
