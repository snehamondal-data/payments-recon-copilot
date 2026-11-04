"""Two harder cases, planted only with --scenario hard. The standard seed-42 scenario never includes them.

Each case's expected outcome is written to ground truth (table hard_expectations) when the scenario is
generated, before dbt or the copilot run on it. They were defined before any run, and not changed after.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum

from recon.business_days import add_business_days, is_business_day, settlement_deadline
from recon.generator.planted import _case
from recon.generator.scenario import PlantedCase, ScenarioBuilder


class HardCaseType(StrEnum):
    SPLIT_SETTLEMENT = "split_settlement"
    FEE_AND_REFUND_ON_ONE_CHARGE = "fee_and_refund_on_one_charge"


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


def plant_hard(b: ScenarioBuilder) -> list[HardExpectation]:
    expectations = []
    for number, kind, plant in HARD_PLANTERS:
        case, expected = plant(b)
        case.case_id = f"{number:02d}-{kind.value}-1"
        b.cases.append(case)
        expectations.append(HardExpectation(case.case_id, expected.expected_status, expected.expected_reason_code,
                                            expected.should_escalate, expected.rationale))
    return expectations
