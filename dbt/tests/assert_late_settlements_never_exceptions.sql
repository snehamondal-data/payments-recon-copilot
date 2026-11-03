-- A charge that is unsettled but still inside its T+2 business-day window (deadline >= as_of,
-- inclusive) must never be flagged for its settlement, and must be waiting in recon_pending.

with recon as (
    select * from {{ ref('int_charge_recon') }}
),

in_window as (
    select * from recon
    where not is_settled
        and settlement_deadline >= {{ as_of_date() }}
),

flagged_early as (
    select exceptions.exception_id as failing_item
    from {{ ref('recon_exceptions') }} as exceptions
    inner join in_window
        on exceptions.charge_id = in_window.charge_id
    where exceptions.reason_code = 'missing_settlement'
),

not_pending as (
    select in_window.charge_id as failing_item
    from in_window
    left join {{ ref('recon_pending') }} as pending
        on in_window.charge_id = pending.charge_id
    where in_window.reason_code is null
        and pending.charge_id is null
)

select * from flagged_early
union all
select * from not_pending
