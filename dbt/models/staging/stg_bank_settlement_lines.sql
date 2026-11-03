-- One row per bank statement line, with its batch's value date.
select
    lines.line_id,
    lines.batch_id,
    batches.value_date,
    lines.reference,
    lines.direction,
    lines.description,
    lines.net_amount_minor,
    case when lines.direction = 'credit' then lines.net_amount_minor else -lines.net_amount_minor end
        as signed_amount_minor
from {{ source('raw', 'bank_settlement_lines') }} as lines
inner join {{ source('raw', 'bank_settlement_batches') }} as batches
    on lines.event_id = batches.event_id
