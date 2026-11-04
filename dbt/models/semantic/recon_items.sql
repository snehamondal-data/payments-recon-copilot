-- One row per reconciliation item at the as-of date: every charge, plus every bank line that is an
-- exception in its own right. This is the grain the semantic layer (MetricFlow) measures over.
-- A charge absorbed into an ambiguous bank line keeps its own row (for match rate and pending), but
-- only the bank line is in the exceptions queue, so only the line counts as a queued exception.

with queue as (
    select * from {{ ref('recon_exceptions') }}
),

charges as (
    select
        {{ dbt.concat(["'charge:'", 'recon.charge_id']) }} as item_id,
        'charge' as item_type,
        recon.currency,
        recon.window_status,
        recon.reason_code,
        case when queue.exception_id is not null then 1 else 0 end as is_queued_exception,
        coalesce(queue.amount_minor, 0) as amount_at_stake_minor
    from {{ ref('int_charge_recon') }} as recon
    left join queue
        on queue.exception_id = {{ dbt.concat(["'charge:'", 'recon.charge_id']) }}
),

bank_lines as (
    select
        queue.exception_id as item_id,
        'bank_line' as item_type,
        batches.currency,
        queue.window_status,
        queue.reason_code,
        1 as is_queued_exception,
        queue.amount_minor as amount_at_stake_minor
    from queue
    inner join {{ ref('stg_bank_settlement_lines') }} as lines
        on lines.line_id = queue.bank_line_id
    inner join {{ source('raw', 'bank_settlement_batches') }} as batches
        on batches.batch_id = lines.batch_id
    where queue.item_type = 'bank_line'
)

select *, {{ as_of_date() }} as as_of_date from charges
union all
select *, {{ as_of_date() }} as as_of_date from bank_lines
