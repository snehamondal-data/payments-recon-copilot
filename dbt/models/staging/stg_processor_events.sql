-- One row per processor webhook delivery. A duplicate webhook is two rows with the same object_id.
select
    event_id,
    event_type,
    object_id,
    charge_id,
    amount_minor,
    currency,
    fx_rate,
    settlement_amount_minor,
    occurred_at,
    {{ utc_date('occurred_at') }} as occurred_date_utc
from {{ source('raw', 'processor_events') }}
