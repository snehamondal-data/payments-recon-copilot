-- One row per charge: processor vs bank vs ledger, with a reason_code and a window_status.
--
-- Reason codes are checked in priority order, and the first one that applies wins:
--   ambiguous                     two explanations fit equally well, or none does (escalate, don't guess)
--   duplicate_webhook             the processor delivered the same charge more than once
--   missing_ledger_entry          no sale journal
--   missing_settlement            unsettled and the T+2 business-day window has closed
--   (pending_in_window)           unsettled but still inside the window: not an exception
--   chargeback                    disputed, with no ledger reversal
--   partial_refund_not_in_ledger  processor refunds exceed ledger refunds
--   fee_mismatch                  bank net differs from gross minus the processor fee
--   currency_rounding             ledger differs from bank by a cent or two on a non-USD charge
--   (ambiguous again)             ledger differs from bank and no rule above explains it
--   date_mismatch                 amounts tie, but the ledger dates it on a different day (timing only)
--
-- More than one problem on one charge: every problem is also detected on its own (the has_* flags). If a
-- charge has two or more, reason_code is ambiguous (so it is escalated) and detected_problems lists them
-- all, so the reviewer sees each one. The priority order above still picks the primary problem, which
-- sets amount_minor. Timing-only date differences don't count as a second problem.

with charges as (
    select * from {{ ref('int_processor_charges') }}
),

bank as (
    select * from {{ ref('int_bank_by_reference') }}
),

ledger as (
    select * from {{ ref('int_ledger_by_charge') }}
),

-- Unreferenced deposits with exactly one candidate settle that charge on amount.
amount_matches as (
    select
        matched_charge_id as charge_id,
        count(*) as amount_match_count,
        sum(net_amount_minor) as amount_matched_net_minor,
        min(value_date) as amount_match_date
    from {{ ref('int_unreferenced_lines') }}
    where matched_charge_id is not null
    group by matched_charge_id
),

-- Charges that are one of several candidates for the same unreferenced deposit.
ambiguous_candidates as (
    select
        candidates.charge_id,
        min(candidates.line_id) as ambiguous_line_id
    from {{ ref('int_unreferenced_line_candidates') }} as candidates
    inner join {{ ref('int_unreferenced_lines') }} as lines
        on candidates.line_id = lines.line_id
    where lines.candidate_count > 1
    group by candidates.charge_id
),

calendar as (
    select * from {{ ref('int_business_calendar') }}
),

joined as (
    select
        charges.charge_id,
        charges.charge_delivery_count,
        charges.currency,
        charges.amount_minor as charge_amount_minor,
        charges.gross_usd_minor,
        charges.fee_usd_minor,
        charges.expected_net_minor,
        charges.refunds_usd_minor,
        charges.disputes_usd_minor,
        charges.charged_at,
        charges.charge_date_utc,
        charges.settlement_deadline,
        coalesce(bank.settlement_line_count, 0) + coalesce(amount_matches.amount_match_count, 0)
            as settlement_line_count,
        case
            when bank.settlement_line_count > 0 then 'reference'
            when amount_matches.amount_match_count > 0 then 'amount'
        end as settlement_match_method,
        coalesce(bank.settled_net_minor, 0) + coalesce(amount_matches.amount_matched_net_minor, 0)
            as bank_settled_net_minor,
        coalesce(bank.first_settlement_date, amount_matches.amount_match_date) as settlement_date,
        coalesce(bank.refund_debits_minor, 0) as bank_refund_debits_minor,
        coalesce(bank.chargeback_debits_minor, 0) as bank_chargeback_debits_minor,
        coalesce(ledger.sale_journal_count, 0) as sale_journal_count,
        coalesce(ledger.ledger_sale_net_minor, 0) as ledger_sale_net_minor,
        coalesce(ledger.ledger_refunds_minor, 0) as ledger_refunds_minor,
        coalesce(ledger.ledger_chargebacks_minor, 0) as ledger_chargebacks_minor,
        ledger.first_posting_date,
        ambiguous_candidates.ambiguous_line_id,
        as_of_day.business_day_index - charge_day.business_day_index as age_business_days
    from charges
    left join bank
        on charges.charge_id = bank.reference
    left join ledger
        on charges.charge_id = ledger.charge_id
    left join amount_matches
        on charges.charge_id = amount_matches.charge_id
    left join ambiguous_candidates
        on charges.charge_id = ambiguous_candidates.charge_id
    inner join calendar as charge_day
        on charges.charge_date_utc = charge_day.date_day
    inner join calendar as as_of_day
        on as_of_day.date_day = {{ as_of_date() }}
),

measured as (
    select
        joined.*,
        settlement_line_count > 0 as is_settled,
        settlement_deadline < {{ as_of_date() }} as window_closed,
        expected_net_minor - bank_settled_net_minor as settlement_shortfall_minor,
        refunds_usd_minor - ledger_refunds_minor as unbooked_refund_minor,
        refunds_usd_minor - bank_refund_debits_minor as refund_not_on_bank_minor,
        ledger_sale_net_minor - bank_settled_net_minor as ledger_vs_bank_minor
    from joined
),

flagged as (
    select
        measured.*,
        charge_delivery_count > 1 as has_duplicate_webhook,
        sale_journal_count = 0 as has_missing_ledger_entry,
        not is_settled and window_closed as has_missing_settlement,
        disputes_usd_minor > 0 and ledger_chargebacks_minor = 0 as has_chargeback,
        unbooked_refund_minor > 0 as has_partial_refund_not_in_ledger,
        is_settled and bank_settled_net_minor <> expected_net_minor as has_fee_mismatch,
        -- Rounding only means something on a single, booked, correctly settled sale.
        is_settled and sale_journal_count = 1 and charge_delivery_count = 1
            and bank_settled_net_minor = expected_net_minor
            and currency <> 'USD' and ledger_vs_bank_minor <> 0
            and abs(ledger_vs_bank_minor) <= {{ var('fx_rounding_tolerance_minor') }} as has_currency_rounding
    from measured
),

counted as (
    select
        flagged.*,
        case when has_duplicate_webhook then 1 else 0 end
            + case when has_missing_ledger_entry then 1 else 0 end
            + case when has_missing_settlement then 1 else 0 end
            + case when has_chargeback then 1 else 0 end
            + case when has_partial_refund_not_in_ledger then 1 else 0 end
            + case when has_fee_mismatch then 1 else 0 end
            + case when has_currency_rounding then 1 else 0 end as problem_count,
        rtrim({{ dbt.concat([
            "case when has_duplicate_webhook then 'duplicate_webhook,' else '' end",
            "case when has_missing_ledger_entry then 'missing_ledger_entry,' else '' end",
            "case when has_missing_settlement then 'missing_settlement,' else '' end",
            "case when has_chargeback then 'chargeback,' else '' end",
            "case when has_partial_refund_not_in_ledger then 'partial_refund_not_in_ledger,' else '' end",
            "case when has_fee_mismatch then 'fee_mismatch,' else '' end",
            "case when has_currency_rounding then 'currency_rounding,' else '' end",
        ]) }}, ',') as problem_list
    from flagged
),

prioritised as (
    select
        counted.*,
        case
            when ambiguous_line_id is not null then 'ambiguous'
            -- The deposit is short by exactly a refund that is neither in the ledger nor debited
            -- separately: it was either netted into the settlement or the fee is wrong.
            when is_settled
                and settlement_shortfall_minor > 0
                and unbooked_refund_minor > 0
                and settlement_shortfall_minor = refund_not_on_bank_minor then 'ambiguous'
            when charge_delivery_count > 1 then 'duplicate_webhook'
            when sale_journal_count = 0 then 'missing_ledger_entry'
            when not is_settled and window_closed then 'missing_settlement'
            when not is_settled then null
            when disputes_usd_minor > 0 and ledger_chargebacks_minor = 0 then 'chargeback'
            when unbooked_refund_minor > 0 then 'partial_refund_not_in_ledger'
            when bank_settled_net_minor <> expected_net_minor then 'fee_mismatch'
            when ledger_vs_bank_minor <> 0
                and currency <> 'USD'
                and abs(ledger_vs_bank_minor) <= {{ var('fx_rounding_tolerance_minor') }} then 'currency_rounding'
            -- A difference no rule explains is for a human, not a guess.
            when ledger_vs_bank_minor <> 0 then 'ambiguous'
            when first_posting_date <> charge_date_utc then 'date_mismatch'
        end as primary_reason_code
    from counted
),

reasoned as (
    select
        prioritised.*,
        case
            when primary_reason_code is not null and problem_count > 1 then 'ambiguous'
            else primary_reason_code
        end as reason_code,
        case
            when primary_reason_code is null then null
            when problem_list <> '' then problem_list
            else primary_reason_code
        end as detected_problems
    from prioritised
)

select
    reasoned.*,
    case
        when reason_code is not null then 'exception'
        when not is_settled then 'pending_in_window'
        else 'matched'
    end as window_status,
    -- The amount at stake for the primary problem, in USD cents (0 when only the timing differs).
    case primary_reason_code
        when 'ambiguous' then
            case
                when not is_settled then expected_net_minor
                when settlement_shortfall_minor <> 0 then abs(settlement_shortfall_minor)
                else abs(ledger_vs_bank_minor)
            end
        when 'duplicate_webhook' then abs(ledger_sale_net_minor - expected_net_minor)
        when 'missing_ledger_entry' then case when is_settled then bank_settled_net_minor else expected_net_minor end
        when 'missing_settlement' then expected_net_minor
        when 'chargeback' then bank_chargeback_debits_minor
        when 'partial_refund_not_in_ledger' then unbooked_refund_minor
        when 'fee_mismatch' then abs(settlement_shortfall_minor)
        when 'currency_rounding' then abs(ledger_vs_bank_minor)
        when 'date_mismatch' then 0
    end as amount_minor
from reasoned
