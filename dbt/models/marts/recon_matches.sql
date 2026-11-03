-- Charges that tie out across processor, bank and ledger.

select
    charge_id,
    currency,
    charge_amount_minor,
    gross_usd_minor,
    fee_usd_minor,
    expected_net_minor,
    bank_settled_net_minor,
    ledger_sale_net_minor,
    refunds_usd_minor,
    bank_refund_debits_minor,
    ledger_refunds_minor,
    charge_date_utc,
    settlement_date,
    settlement_deadline,
    settlement_match_method,
    age_business_days,
    window_status,
    {{ as_of_date() }} as as_of_date
from {{ ref('int_charge_recon') }}
where window_status = 'matched'
