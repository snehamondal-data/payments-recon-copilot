-- Ledger journals rolled up per charge.

select
    charge_id,
    sum(case when entry_type = 'sale' then 1 else 0 end) as sale_journal_count,
    sum(case when entry_type = 'sale' then receivable_net_minor else 0 end) as ledger_sale_net_minor,
    sum(case when entry_type = 'sale' then revenue_minor else 0 end) as ledger_revenue_minor,
    sum(case when entry_type = 'refund' then refunds_minor else 0 end) as ledger_refunds_minor,
    sum(case when entry_type = 'chargeback' then chargebacks_minor else 0 end) as ledger_chargebacks_minor,
    min(case when entry_type = 'sale' then posting_date end) as first_posting_date
from {{ ref('stg_ledger_journals') }}
group by charge_id
