"""Planted problems, one function per exception type in spec section 3.

Each planter creates the records for one case and returns its ground truth. The
ground truth goes to data/ground_truth.duckdb only, never into the warehouse.
"""

from __future__ import annotations

import random
from collections.abc import Callable
from datetime import UTC, datetime, time, timedelta
from decimal import ROUND_HALF_UP, Decimal

from recon.business_days import add_business_days, is_business_day, settlement_deadline
from recon.generator.scenario import LOCAL_TZ, PlantedCase, ScenarioBuilder
from recon.money import CHARGEBACK_FEE_MINOR, to_minor
from recon.taxonomy import ExceptionType as T

CASES_PER_TYPE = 3
HELD_OUT_CASES = 6
LEGACY_LEDGER_RATE_QUANTUM = Decimal("0.0001")  # the ledger rounds FX rates to 4 dp


def _case(b: ScenarioBuilder, exception_type: T, charges: list, extra_ids: list[str] | None = None, *,
          required: list[str], description: str, expected_status: str | None = "exception",
          should_escalate: bool = False) -> PlantedCase:
    """`required` are the records that prove the cause: what a correct diagnosis must cite (evidence recall)."""
    return PlantedCase(
        case_id="",  # assigned by plant_all
        exception_type=exception_type,
        expected_status=expected_status,
        should_escalate=should_escalate,
        charge_ids=[c.charge_id for c in charges],
        record_ids=[rid for c in charges for rid in c.record_ids] + (extra_ids or []),
        description=description,
        required_record_ids=required,
    )


def late_settlement(b: ScenarioBuilder, i: int) -> PlantedCase:
    # Case 2 sits exactly on the edge: its deadline is as_of itself, and the window is inclusive.
    edge_days = [d for d in (b.as_of - timedelta(days=k) for k in range(1, 8)) if settlement_deadline(d) == b.as_of]
    day = max(edge_days) if (i == 1 and edge_days) else b.as_of - timedelta(days=1)
    c = b.charge(b.local_time(day), b.amount(), settle_lag=None)
    deadline = settlement_deadline(day)
    assert deadline >= b.as_of
    edge = " (edge of window: deadline is the as-of date)" if deadline == b.as_of else ""
    return _case(b, T.LATE_SETTLEMENT, [c], required=[c.event.event_id], expected_status="pending_in_window",
                 description=f"Charged {day}, not settled yet. Deadline {deadline} (T+2 business days, "
                             f"inclusive) has not passed as of {b.as_of}{edge}. Must not be flagged.")


def missing_settlement(b: ScenarioBuilder, i: int) -> PlantedCase:
    day = b.baseline_day()
    c = b.charge(b.local_time(day), b.amount(), settle_lag=None)
    deadline = settlement_deadline(day)
    assert deadline < b.as_of
    return _case(b, T.MISSING_SETTLEMENT, [c], required=[c.event.event_id],
                 description=f"Charged {day}, deadline {deadline} passed, never settled by the bank.")


def duplicate_webhook(b: ScenarioBuilder, i: int) -> PlantedCase:
    day = b.baseline_day()
    at = b.local_time(day)
    c = b.charge(at, b.amount(), settle_lag=b.rng.choice((1, 2)))
    # Same business object, new delivery ID: the loader keeps both, and the ledger naively posted twice.
    dup = c.event.model_copy(update={"event_id": b.new_id("evt")})
    b.processor.append(dup)
    c.event_ids.append(dup.event_id)
    b.sale_journal(c, c.gross_usd, at + timedelta(minutes=b.rng.randint(11, 20)))
    return _case(b, T.DUPLICATE_WEBHOOK, [c], required=[c.event.event_id, dup.event_id],
                 description=f"Processor delivered charge {c.charge_id} twice under different event_ids; "
                             "ledger posted two sale journals; bank settled once.")


def fee_mismatch(b: ScenarioBuilder, i: int) -> PlantedCase:
    extra = b.rng.randint(25, 75)
    c = b.charge(b.local_time(b.baseline_day()), b.amount(), settle_lag=b.rng.choice((1, 2)), bank_net_delta=-extra)
    return _case(b, T.FEE_MISMATCH, [c], required=[c.event_ids[1], c.bank_line_ids[0]],  # fee event, settlement
                 description=f"Bank net is {extra} cents below gross minus the expected fee "
                             f"(expected net {c.net_usd}, settled {c.net_usd - extra}).")


def partial_refund_not_in_ledger(b: ScenarioBuilder, i: int) -> PlantedCase:
    day = b.baseline_day(last_offset=3)
    amount = b.amount(2_000, 50_000)
    c = b.charge(b.local_time(day), amount, settle_lag=1)
    refund_amount = amount * b.rng.randint(20, 60) // 100
    refund = b.refund(c, b.local_time(day + timedelta(days=b.rng.randint(1, 3))), refund_amount, ledger=False)
    return _case(b, T.PARTIAL_REFUND_NOT_IN_LEDGER, [c], required=[refund.event_id, c.bank_line_ids[-1]],
                 description=f"Partial refund of {refund_amount} of {amount} cents at the processor and bank; "
                             "no refund journal in the ledger.")


def currency_rounding(b: ScenarioBuilder, i: int) -> PlantedCase:
    # Search for an amount where the ledger's 4 dp rate gives a total 1 or 2 cents off the processor's.
    for _ in range(10_000):
        day = b.baseline_day()
        currency = b.rng.choice(("EUR", "GBP"))
        amount = b.amount(20_000, 40_000)
        rate = b.fx_rate(day, currency)
        legacy_rate = rate.quantize(LEGACY_LEDGER_RATE_QUANTUM, rounding=ROUND_HALF_UP)
        diff = to_minor(amount, legacy_rate) - to_minor(amount, rate)
        if abs(diff) in (1, 2):
            break
    else:
        raise RuntimeError("could not plant a currency rounding case")
    c = b.charge(b.local_time(day), amount, currency, settle_lag=b.rng.choice((1, 2)), ledger_rate=legacy_rate)
    return _case(b, T.CURRENCY_ROUNDING, [c], required=[c.journal_ids[0], c.bank_line_ids[0]],
                 description=f"{amount} {currency} minor units on {day}. Processor and bank used ref.fx_rates "
                             f"rate {rate}; ledger used it rounded to 4 dp ({legacy_rate}), so the ledger is "
                             f"{diff:+d} cents off.")


def missing_ledger_entry(b: ScenarioBuilder, i: int) -> PlantedCase:
    c = b.charge(b.local_time(b.baseline_day()), b.amount(), settle_lag=b.rng.choice((1, 2)), ledger=False)
    return _case(b, T.MISSING_LEDGER_ENTRY, [c], required=[c.event.event_id, c.bank_line_ids[0]],
                 description="Processor and bank agree; the ledger has no journal for this charge.")


def orphan_bank_deposit(b: ScenarioBuilder, i: int) -> PlantedCase:
    value_date = b.baseline_day()
    while not is_business_day(value_date):
        value_date += timedelta(days=1)
    reference = f"TRF-{b.rng.getrandbits(32):08X}"
    line = b.emit_bank_line(value_date, reference, "credit", "DEPOSIT", b.amount(1_000, 30_000))
    return _case(b, T.ORPHAN_BANK_DEPOSIT, [], [line.line_id], required=[line.line_id],
                 description=f"Bank credit {line.net_amount_minor} cents on {value_date}, reference "
                             f"{reference}, matches no processor charge.")


def chargeback(b: ScenarioBuilder, i: int) -> PlantedCase:
    day = b.first_day + timedelta(days=b.rng.randrange(5))  # early, so the dispute lands before as_of
    amount = b.amount(2_000, 50_000)
    c = b.charge(b.local_time(day), amount, settle_lag=1)
    dispute_day = add_business_days(day, 1) + timedelta(days=b.rng.randint(2, 4))
    dispute = b.emit_processor("dispute", c.charge_id, amount, "USD", b.local_time(dispute_day))
    c.event_ids.append(dispute.event_id)
    line = b.emit_bank_line(add_business_days(dispute_day, 1), c.charge_id, "debit", "CHARGEBACK",
                            amount + CHARGEBACK_FEE_MINOR)
    c.bank_line_ids.append(line.line_id)
    return _case(b, T.CHARGEBACK, [c], required=[dispute.event_id, line.line_id],
                 description=f"Settled charge disputed on {dispute_day}; bank debited {amount} plus a "
                             f"{CHARGEBACK_FEE_MINOR} cent fee. No reversal in the ledger.")


def date_boundary(b: ScenarioBuilder, i: int) -> PlantedCase:
    day = b.baseline_day()
    # 23:10 to 23:44 New York time is the next day in UTC; the ledger posts within 10 minutes, still local `day`.
    local = datetime.combine(day, time(23, b.rng.randint(10, 44), b.rng.randrange(60)), tzinfo=LOCAL_TZ)
    c = b.charge(local.astimezone(UTC), b.amount(), settle_lag=1)
    return _case(b, T.DATE_BOUNDARY, [c], required=[c.event.event_id, c.journal_ids[0]], expected_status=None,
                 description=f"Charged {local:%Y-%m-%d %H:%M} New York time = {c.event.occurred_at.date()} UTC. "
                             f"Processor date is {c.event.occurred_at.date()}, ledger posting_date is {day}. "
                             "Amounts agree. Expected status is decided in Slice 2.")


def ambiguous(b: ScenarioBuilder, i: int) -> PlantedCase:
    if i % 2 == 0:
        # Variant A: one unreferenced deposit, two equally good candidate charges.
        day = b.baseline_day()
        amount = b.amount()
        c1 = b.charge(b.local_time(day), amount, settle_lag=None)
        c2 = b.charge(b.local_time(day), amount, settle_lag=None)
        line = b.emit_bank_line(add_business_days(day, 1), None, "credit", "SETTLEMENT", c1.net_usd)
        return _case(b, T.AMBIGUOUS, [c1, c2], [line.line_id], should_escalate=True,
                     required=[line.line_id, c1.event.event_id, c2.event.event_id],
                     description=f"Bank credit {c1.net_usd} cents with no reference equals the expected net of "
                                 f"both {c1.charge_id} and {c2.charge_id}. Cannot tell which one settled.")
    # Variant B: a shortfall explained equally well by an unledgered partial refund or a fee error.
    day = b.baseline_day()
    at = b.local_time(day, 9, 12)
    shortfall = b.rng.randint(50, 150)
    c = b.charge(at, b.amount(), settle_lag=b.rng.choice((1, 2)), bank_net_delta=-shortfall)
    refund = b.refund(c, at + timedelta(hours=b.rng.randint(1, 5)), shortfall, ledger=False, bank=False)
    return _case(b, T.AMBIGUOUS, [c], should_escalate=True, required=[refund.event_id, c.bank_line_ids[0]],
                 description=f"Bank net is {shortfall} cents short. A same-day partial refund of exactly "
                             f"{shortfall} cents (not in the ledger) could have been netted, or the fee could be wrong.")


PLANTERS: dict[T, tuple[int, Callable[[ScenarioBuilder, int], PlantedCase]]] = {
    T.LATE_SETTLEMENT: (1, late_settlement),
    T.MISSING_SETTLEMENT: (2, missing_settlement),
    T.DUPLICATE_WEBHOOK: (3, duplicate_webhook),
    T.FEE_MISMATCH: (4, fee_mismatch),
    T.PARTIAL_REFUND_NOT_IN_LEDGER: (5, partial_refund_not_in_ledger),
    T.CURRENCY_ROUNDING: (6, currency_rounding),
    T.MISSING_LEDGER_ENTRY: (7, missing_ledger_entry),
    T.ORPHAN_BANK_DEPOSIT: (8, orphan_bank_deposit),
    T.CHARGEBACK: (9, chargeback),
    T.DATE_BOUNDARY: (10, date_boundary),
    T.AMBIGUOUS: (11, ambiguous),
}


def plant_all(b: ScenarioBuilder) -> None:
    for exception_type, (number, plant) in PLANTERS.items():
        for i in range(CASES_PER_TYPE):
            case = plant(b, i)
            case.case_id = f"{number:02d}-{exception_type.value}-{i + 1}"
            b.cases.append(case)
    # Hold out the last case of a few types for honest evals. A separate RNG keeps the data itself unchanged.
    holdout_rng = random.Random(f"{b.seed}:holdout")
    for exception_type in holdout_rng.sample(sorted(PLANTERS), HELD_OUT_CASES):
        [c for c in b.cases if c.exception_type == exception_type][-1].held_out = True
