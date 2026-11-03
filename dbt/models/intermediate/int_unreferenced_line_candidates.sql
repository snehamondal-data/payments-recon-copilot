-- For each bank settlement credit with no reference: every charge it could be paying.
-- A candidate has no referenced settlement of its own, the same expected net amount, and the
-- line's value date falls inside the charge's settlement window.

with lines as (
    select *
    from {{ ref('stg_bank_settlement_lines') }}
    where reference is null
        and direction = 'credit'
        and description = 'SETTLEMENT'
),

unsettled_charges as (
    select charges.*
    from {{ ref('int_processor_charges') }} as charges
    left join {{ ref('int_bank_by_reference') }} as bank
        on charges.charge_id = bank.reference
    where coalesce(bank.settlement_line_count, 0) = 0
)

select
    lines.line_id,
    unsettled_charges.charge_id
from lines
inner join unsettled_charges
    on lines.net_amount_minor = unsettled_charges.expected_net_minor
    and lines.value_date > unsettled_charges.charge_date_utc
    and lines.value_date <= unsettled_charges.settlement_deadline
