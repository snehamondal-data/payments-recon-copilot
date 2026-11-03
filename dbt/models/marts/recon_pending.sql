-- Charges not settled yet but still inside their T+2 business-day window. Not exceptions.
-- Re-checked as windows close (Slice 6 schedules this daily).

with calendar as (
    select * from {{ ref('int_business_calendar') }}
)

select
    recon.charge_id,
    recon.currency,
    recon.expected_net_minor,
    recon.charge_date_utc,
    recon.settlement_deadline,
    deadline_day.business_day_index - as_of_day.business_day_index as business_days_left,
    recon.age_business_days,
    recon.window_status,
    {{ as_of_date() }} as as_of_date
from {{ ref('int_charge_recon') }} as recon
inner join calendar as deadline_day
    on recon.settlement_deadline = deadline_day.date_day
inner join calendar as as_of_day
    on as_of_day.date_day = {{ as_of_date() }}
where recon.window_status = 'pending_in_window'
