-- Every matched charge ties out row by row, and so do the totals:
-- processor gross - fee = bank settled net = ledger receivable, and refunds agree on all three sides.

with row_breaks as (
    select
        charge_id as failing_item,
        'row' as level
    from {{ ref('recon_matches') }}
    where expected_net_minor <> bank_settled_net_minor
        or ledger_sale_net_minor <> bank_settled_net_minor
        or refunds_usd_minor <> bank_refund_debits_minor
        or refunds_usd_minor <> ledger_refunds_minor
),

totals as (
    select
        sum(expected_net_minor) as processor_net,
        sum(bank_settled_net_minor) as bank_net,
        sum(ledger_sale_net_minor) as ledger_net,
        sum(refunds_usd_minor) as processor_refunds,
        sum(bank_refund_debits_minor) as bank_refunds,
        sum(ledger_refunds_minor) as ledger_refunds
    from {{ ref('recon_matches') }}
),

total_breaks as (
    select
        'totals' as failing_item,
        'total' as level
    from totals
    where processor_net <> bank_net
        or ledger_net <> bank_net
        or processor_refunds <> bank_refunds
        or processor_refunds <> ledger_refunds
)

select * from row_breaks
union all
select * from total_breaks
