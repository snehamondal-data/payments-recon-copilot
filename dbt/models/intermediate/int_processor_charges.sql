-- One row per charge, as the processor sees it, with its T+2 business-day settlement deadline.

with events as (
    select * from {{ ref('stg_processor_events') }}
),

charge_deliveries as (
    select
        charge_id,
        count(*) as charge_delivery_count,  -- more than 1 means a duplicate webhook
        max(currency) as currency,
        max(amount_minor) as amount_minor,
        max(settlement_amount_minor) as gross_usd_minor,
        min(occurred_at) as charged_at,
        min(occurred_date_utc) as charge_date_utc
    from events
    where event_type = 'charge'
    group by charge_id
),

-- Fees, refunds and disputes, one row per business object, so a redelivered object counts once.
objects as (
    select
        charge_id,
        event_type,
        object_id,
        max(settlement_amount_minor) as settlement_amount_minor
    from events
    where event_type <> 'charge'
    group by charge_id, event_type, object_id
),

object_totals as (
    select
        charge_id,
        sum(case when event_type = 'fee' then settlement_amount_minor else 0 end) as fee_usd_minor,
        sum(case when event_type = 'refund' then settlement_amount_minor else 0 end) as refunds_usd_minor,
        sum(case when event_type = 'dispute' then settlement_amount_minor else 0 end) as disputes_usd_minor
    from objects
    group by charge_id
),

calendar as (
    select * from {{ ref('int_business_calendar') }}
)

select
    charges.charge_id,
    charges.charge_delivery_count,
    charges.currency,
    charges.amount_minor,
    charges.gross_usd_minor,
    coalesce(totals.fee_usd_minor, 0) as fee_usd_minor,
    charges.gross_usd_minor - coalesce(totals.fee_usd_minor, 0) as expected_net_minor,
    coalesce(totals.refunds_usd_minor, 0) as refunds_usd_minor,
    coalesce(totals.disputes_usd_minor, 0) as disputes_usd_minor,
    charges.charged_at,
    charges.charge_date_utc,
    deadline.date_day as settlement_deadline
from charge_deliveries as charges
left join object_totals as totals
    on charges.charge_id = totals.charge_id
inner join calendar as charge_day
    on charges.charge_date_utc = charge_day.date_day
inner join calendar as deadline
    on deadline.is_business_day
    and deadline.business_day_index = charge_day.business_day_index + {{ var('settlement_window_business_days') }}
