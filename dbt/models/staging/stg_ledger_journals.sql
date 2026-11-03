-- One row per ledger journal, with its lines pivoted into per-account amounts (USD cents).
select
    journals.journal_id,
    journals.charge_id,
    journals.entry_type,
    journals.posted_at,
    journals.posting_date,
    sum(case when lines.account = 'processor_receivable' then lines.debit_minor - lines.credit_minor else 0 end)
        as receivable_net_minor,
    sum(case when lines.account = 'revenue' then lines.credit_minor - lines.debit_minor else 0 end)
        as revenue_minor,
    sum(case when lines.account = 'processing_fees' then lines.debit_minor else 0 end) as fees_minor,
    sum(case when lines.account = 'refunds' then lines.debit_minor else 0 end) as refunds_minor,
    sum(case when lines.account in ('chargebacks', 'dispute_fees') then lines.debit_minor else 0 end)
        as chargebacks_minor
from {{ source('raw', 'ledger_journals') }} as journals
inner join {{ source('raw', 'ledger_lines') }} as lines
    on journals.event_id = lines.event_id
group by journals.journal_id, journals.charge_id, journals.entry_type, journals.posted_at, journals.posting_date
