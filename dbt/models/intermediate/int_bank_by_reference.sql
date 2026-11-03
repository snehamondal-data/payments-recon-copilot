-- Bank lines that carry a reference, rolled up per reference (normally a processor charge_id).

select
    reference,
    count(*) as line_count,
    sum(case when description = 'SETTLEMENT' and direction = 'credit' then 1 else 0 end) as settlement_line_count,
    sum(case when description = 'SETTLEMENT' and direction = 'credit' then net_amount_minor else 0 end)
        as settled_net_minor,
    min(case when description = 'SETTLEMENT' and direction = 'credit' then value_date end) as first_settlement_date,
    sum(case when description = 'REFUND' and direction = 'debit' then net_amount_minor else 0 end)
        as refund_debits_minor,
    sum(case when description = 'CHARGEBACK' and direction = 'debit' then net_amount_minor else 0 end)
        as chargeback_debits_minor
from {{ ref('stg_bank_settlement_lines') }}
where reference is not null
group by reference
