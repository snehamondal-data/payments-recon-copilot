"""Harder cases, planted only with --scenario hard (seed 42: the original two) or --scenario multi (new seed
2027: four fresh "two problems on one charge" cases). The standard scenarios never include them.

Each case's expected outcome is written to ground truth (table hard_expectations) when the scenario is
generated, before dbt or the copilot run on it. They were defined before any run, and not changed after.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum

from decimal import ROUND_HALF_UP

from recon.business_days import add_business_days, is_business_day, settlement_deadline
from recon.generator.planted import LEGACY_LEDGER_RATE_QUANTUM, _case
from recon.generator.scenario import PlantedCase, ScenarioBuilder
from recon.money import CHARGEBACK_FEE_MINOR, to_minor


class HardCaseType(StrEnum):
    SPLIT_SETTLEMENT = "split_settlement"
    FEE_AND_REFUND_ON_ONE_CHARGE = "fee_and_refund_on_one_charge"
    FEE_AND_CHARGEBACK = "fee_and_chargeback"
    DUPLICATE_AND_MISSING_LEDGER = "duplicate_and_missing_ledger"
    REFUND_AND_ROUNDING = "refund_and_rounding"
    UNSETTLED_AND_MISSING_LEDGER = "unsettled_and_missing_ledger"


TWO_PROBLEMS = ("Two independent problems on one charge need two different fixes. No single reason code or "
                "action covers both, so it must reach a human: reason code ambiguous, and the copilot must "
                "escalate. Naming only one of the two problems is a miss.")


def _two_problems(b: ScenarioBuilder, kind: HardCaseType, c, required: list[str], description: str
                  ) -> tuple[PlantedCase, HardExpectation]:
    case = _case(b, kind, [c], required=required, should_escalate=True, description=description)
    return case, HardExpectation("", "exception", "ambiguous", True, TWO_PROBLEMS)


def fee_and_chargeback(b: ScenarioBuilder) -> tuple[PlantedCase, HardExpectation]:
    day = b.first_day + timedelta(days=b.rng.randrange(5))  # early, so the dispute lands before as_of
    amount = b.amount(2_000, 50_000)
    extra = b.rng.randint(25, 75)
    c = b.charge(b.local_time(day), amount, settle_lag=1, bank_net_delta=-extra)
    dispute_day = add_business_days(day, 1) + timedelta(days=b.rng.randint(2, 4))
    dispute = b.emit_processor("dispute", c.charge_id, amount, "USD", b.local_time(dispute_day))
    c.event_ids.append(dispute.event_id)
    line = b.emit_bank_line(add_business_days(dispute_day, 1), c.charge_id, "debit", "CHARGEBACK",
                            amount + CHARGEBACK_FEE_MINOR)
    c.bank_line_ids.append(line.line_id)
    return _two_problems(b, HardCaseType.FEE_AND_CHARGEBACK, c,
                         [c.event_ids[1], c.bank_line_ids[0], dispute.event_id, line.line_id],
                         f"Two problems on {c.charge_id}: the settlement is {extra} cents below gross minus the fee "
                         f"(a fee mismatch), and the charge was disputed on {dispute_day} and debited by the bank "
                         f"with no chargeback reversal in the ledger.")


def duplicate_and_missing_ledger(b: ScenarioBuilder) -> tuple[PlantedCase, HardExpectation]:
    at = b.local_time(b.baseline_day())
    c = b.charge(at, b.amount(), settle_lag=b.rng.choice((1, 2)), ledger=False)
    dup = c.event.model_copy(update={"event_id": b.new_id("evt")})
    b.processor.append(dup)
    c.event_ids.append(dup.event_id)
    return _two_problems(b, HardCaseType.DUPLICATE_AND_MISSING_LEDGER, c,
                         [c.event.event_id, dup.event_id, c.bank_line_ids[0]],
                         f"Two problems on {c.charge_id}: the processor delivered the charge twice under different "
                         f"event_ids (a duplicate webhook), and the ledger has no sale journal for it at all. "
                         f"The bank settled it once.")


def refund_and_rounding(b: ScenarioBuilder) -> tuple[PlantedCase, HardExpectation]:
    for _ in range(10_000):  # an amount whose 4 dp ledger rate is 1 or 2 cents off, as in currency_rounding
        day = b.baseline_day(last_offset=3)
        currency = b.rng.choice(("EUR", "GBP"))
        amount = b.amount(20_000, 40_000)
        rate = b.fx_rate(day, currency)
        legacy_rate = rate.quantize(LEGACY_LEDGER_RATE_QUANTUM, rounding=ROUND_HALF_UP)
        diff = to_minor(amount, legacy_rate) - to_minor(amount, rate)
        if abs(diff) in (1, 2):
            break
    else:
        raise RuntimeError("could not plant a refund and rounding case")
    c = b.charge(b.local_time(day), amount, currency, settle_lag=1, ledger_rate=legacy_rate)
    refund_amount = amount * b.rng.randint(20, 60) // 100
    refund = b.refund(c, b.local_time(day + timedelta(days=b.rng.randint(1, 3))), refund_amount, ledger=False)
    return _two_problems(b, HardCaseType.REFUND_AND_ROUNDING, c,
                         [refund.event_id, c.bank_line_ids[-1], c.journal_ids[0], c.bank_line_ids[0]],
                         f"Two problems on {c.charge_id} ({amount} {currency} minor units): a partial refund of "
                         f"{refund_amount} is at the processor and the bank but not in the ledger, and the ledger "
                         f"booked the sale at the FX rate rounded to 4 dp ({legacy_rate} instead of {rate}), "
                         f"{diff:+d} cents off.")


def unsettled_and_missing_ledger(b: ScenarioBuilder) -> tuple[PlantedCase, HardExpectation]:
    day = b.baseline_day()
    c = b.charge(b.local_time(day), b.amount(), settle_lag=None, ledger=False)
    deadline = settlement_deadline(day)
    assert deadline < b.as_of
    return _two_problems(b, HardCaseType.UNSETTLED_AND_MISSING_LEDGER, c, [c.event.event_id],
                         f"Two problems on {c.charge_id}: charged {day}, its settlement deadline {deadline} has "
                         f"passed with no bank settlement, and the ledger has no sale journal for it either.")


@dataclass(frozen=True)
class HardExpectation:
    case_id: str
    expected_status: str  # what reconciliation should call it: matched | exception
    expected_reason_code: str | None  # None when it must not be an exception at all
    should_escalate: bool  # what the copilot must do if it is in the queue
    rationale: str


def split_settlement(b: ScenarioBuilder) -> tuple[PlantedCase, HardExpectation]:
    """One charge settled in two bank lines, on two different value dates (so two batches), summing to the net."""
    while True:
        day = b.baseline_day()
        first, second = add_business_days(day, 1), add_business_days(day, 2)
        if is_business_day(first) and second <= settlement_deadline(day) and second <= b.as_of:
            break
    c = b.charge(b.local_time(day), b.amount(5_000, 50_000), settle_lag=None)
    part = c.net_usd * b.rng.randint(30, 70) // 100
    line1 = b.emit_bank_line(first, c.charge_id, "credit", "SETTLEMENT", part)
    line2 = b.emit_bank_line(second, c.charge_id, "credit", "SETTLEMENT", c.net_usd - part)
    c.bank_line_ids += [line1.line_id, line2.line_id]
    case = _case(b, HardCaseType.SPLIT_SETTLEMENT, [c], required=[c.event.event_id, line1.line_id, line2.line_id],
                 expected_status="matched",
                 description=f"Charge {c.charge_id} (net {c.net_usd} cents) settled in two bank lines: {part} cents "
                             f"on {first} and {c.net_usd - part} cents on {second}, in different batches. Both are "
                             f"inside the T+2 window and they sum to the expected net.")
    return case, HardExpectation(
        "", "matched", None, False,
        "The two lines sum exactly to the expected net, both inside the window: it ties out. Flagging it would "
        "be a false alarm. If reconciliation queues it anyway there is no correct copilot action, so its "
        "proposal is reported, not scored.")


def fee_and_refund_on_one_charge(b: ScenarioBuilder) -> tuple[PlantedCase, HardExpectation]:
    """Two independent problems on one charge: the bank net is short (fee) and a partial refund is unbooked."""
    day = b.baseline_day(last_offset=3)
    amount = b.amount(2_000, 50_000)
    extra = b.rng.randint(25, 75)
    c = b.charge(b.local_time(day), amount, settle_lag=1, bank_net_delta=-extra)
    refund_amount = amount * b.rng.randint(20, 60) // 100
    refund = b.refund(c, b.local_time(day + timedelta(days=b.rng.randint(1, 3))), refund_amount, ledger=False)
    case = _case(b, HardCaseType.FEE_AND_REFUND_ON_ONE_CHARGE, [c],
                 required=[c.event_ids[1], c.bank_line_ids[0], refund.event_id], should_escalate=True,
                 description=f"Two problems on {c.charge_id}: the settlement is {extra} cents below gross minus the "
                             f"fee (a fee mismatch), and a partial refund of {refund_amount} of {amount} cents is at "
                             f"the processor and the bank but not in the ledger.")
    return case, HardExpectation(
        "", "exception", "ambiguous", True,
        "Two independent problems need two different fixes (a fee claim and a refund journal). No single reason "
        "code or action covers both, so it must reach a human: reason code ambiguous, and the copilot must "
        "escalate. Naming only one of the two problems is a miss.")


HARD_PLANTERS = ((12, HardCaseType.SPLIT_SETTLEMENT, split_settlement),
                 (13, HardCaseType.FEE_AND_REFUND_ON_ONE_CHARGE, fee_and_refund_on_one_charge))


MULTI_PLANTERS = ((14, HardCaseType.FEE_AND_CHARGEBACK, fee_and_chargeback),
                  (15, HardCaseType.DUPLICATE_AND_MISSING_LEDGER, duplicate_and_missing_ledger),
                  (16, HardCaseType.REFUND_AND_ROUNDING, refund_and_rounding),
                  (17, HardCaseType.UNSETTLED_AND_MISSING_LEDGER, unsettled_and_missing_ledger))


def plant_hard(b: ScenarioBuilder, planters=HARD_PLANTERS) -> list[HardExpectation]:
    expectations = []
    for number, kind, plant in planters:
        case, expected = plant(b)
        case.case_id = f"{number:02d}-{kind.value}-1"
        b.cases.append(case)
        expectations.append(HardExpectation(case.case_id, expected.expected_status, expected.expected_reason_code,
                                            expected.should_escalate, expected.rationale))
    return expectations
