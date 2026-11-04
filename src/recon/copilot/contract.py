"""The safety contract: what the copilot may conclude, what it may propose, and how both are checked.

RootCause is exactly the set of reason codes reconciliation (Slice 2) can produce. ALLOWED_ACTIONS
maps each root cause to the only actions the agent may propose for it; the evals check against it.
"""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError

LOW_CONFIDENCE_THRESHOLD = 0.6
RECORD_ID_PATTERN = r"^(evt|jrn|bln)_[0-9a-f]{16}$|^(charge:ch|bank_line:bln)_[0-9a-f]{16}$|^fx:\d{4}-\d{2}-\d{2}:(USD|EUR|GBP)$"


class RootCause(StrEnum):
    AMBIGUOUS = "ambiguous"
    DUPLICATE_WEBHOOK = "duplicate_webhook"
    MISSING_LEDGER_ENTRY = "missing_ledger_entry"
    MISSING_SETTLEMENT = "missing_settlement"
    CHARGEBACK = "chargeback"
    PARTIAL_REFUND_NOT_IN_LEDGER = "partial_refund_not_in_ledger"
    FEE_MISMATCH = "fee_mismatch"
    CURRENCY_ROUNDING = "currency_rounding"
    DATE_MISMATCH = "date_mismatch"
    ORPHAN_BANK_DEPOSIT = "orphan_bank_deposit"


class ProposedAction(StrEnum):
    ESCALATE_TO_HUMAN = "escalate_to_human"
    REQUEST_SETTLEMENT_TRACE = "request_settlement_trace"
    REVERSE_DUPLICATE_JOURNAL = "reverse_duplicate_journal"
    POST_MISSING_SALE_JOURNAL = "post_missing_sale_journal"
    POST_REFUND_JOURNAL = "post_refund_journal"
    POST_CHARGEBACK_JOURNAL = "post_chargeback_journal"
    RAISE_FEE_CLAIM_WITH_PROCESSOR = "raise_fee_claim_with_processor"
    POST_FEE_ADJUSTMENT = "post_fee_adjustment"
    POST_FX_ROUNDING_ADJUSTMENT = "post_fx_rounding_adjustment"
    MOVE_TO_SUSPENSE = "move_to_suspense"
    MARK_TIMING_DIFFERENCE = "mark_timing_difference"


A = ProposedAction
# escalate_to_human is always allowed. For ambiguous it is the only action.
ALLOWED_ACTIONS: dict[RootCause, frozenset[ProposedAction]] = {
    RootCause.AMBIGUOUS: frozenset(),
    RootCause.MISSING_SETTLEMENT: frozenset({A.REQUEST_SETTLEMENT_TRACE}),
    RootCause.DUPLICATE_WEBHOOK: frozenset({A.REVERSE_DUPLICATE_JOURNAL}),
    RootCause.MISSING_LEDGER_ENTRY: frozenset({A.POST_MISSING_SALE_JOURNAL}),
    RootCause.PARTIAL_REFUND_NOT_IN_LEDGER: frozenset({A.POST_REFUND_JOURNAL}),
    RootCause.CHARGEBACK: frozenset({A.POST_CHARGEBACK_JOURNAL}),
    RootCause.FEE_MISMATCH: frozenset({A.RAISE_FEE_CLAIM_WITH_PROCESSOR, A.POST_FEE_ADJUSTMENT}),
    RootCause.CURRENCY_ROUNDING: frozenset({A.POST_FX_ROUNDING_ADJUSTMENT}),
    RootCause.ORPHAN_BANK_DEPOSIT: frozenset({A.MOVE_TO_SUSPENSE}),
    RootCause.DATE_MISMATCH: frozenset({A.MARK_TIMING_DIFFERENCE}),
}
ALLOWED_ACTIONS = {cause: actions | {A.ESCALATE_TO_HUMAN} for cause, actions in ALLOWED_ACTIONS.items()}


def is_allowed(root_cause: RootCause, action: ProposedAction) -> bool:
    return action in ALLOWED_ACTIONS[root_cause]


class Diagnosis(BaseModel):
    """What the agent submits. The model fills this in through the submit_diagnosis tool."""

    model_config = ConfigDict(extra="forbid")

    root_cause: RootCause = Field(description="The reason code that best explains the exception.")
    evidence: Annotated[list[Annotated[str, Field(pattern=RECORD_ID_PATTERN)]], Field(min_length=1, max_length=30)] = (
        Field(description="record_id values, exactly as returned by the tools in this investigation."))
    proposed_action: ProposedAction = Field(description="Must be allowed for the root cause (see the contract).")
    confidence: Annotated[float, Field(ge=0, le=1)] = Field(description="0 to 1.")
    needs_human_escalation: bool = Field(description="True exactly when proposed_action is escalate_to_human.")
    secondary_issues: Annotated[list[RootCause], Field(max_length=5)] = Field(
        default_factory=list,
        description="Any other problem you found on the same item, besides root_cause. Non-empty means "
                    "proposed_action must be escalate_to_human: one action can't fix two problems.")
    rationale: Annotated[str, Field(min_length=1, max_length=600)] = Field(
        description="Two or three sentences a reviewer can check against the evidence.")


def contract_violations(d: Diagnosis, returned_record_ids: set[str]) -> list[str]:
    """Checks beyond the schema. Any violation makes the submission invalid."""
    problems = []
    unseen = [rid for rid in d.evidence if rid not in returned_record_ids]
    if unseen:
        problems.append(f"evidence cites record_ids no tool returned in this investigation: {unseen}")
    if not is_allowed(d.root_cause, d.proposed_action):
        allowed = sorted(a.value for a in ALLOWED_ACTIONS[d.root_cause])
        problems.append(f"proposed_action {d.proposed_action.value!r} is not allowed for root_cause "
                        f"{d.root_cause.value!r}; allowed: {allowed}")
    if d.needs_human_escalation != (d.proposed_action == A.ESCALATE_TO_HUMAN):
        problems.append("needs_human_escalation must be true exactly when proposed_action is escalate_to_human")
    # More than one problem: no single action fixes both, so only an escalation may stand. Enforced here,
    # on the structured field, never by reading the rationale.
    if d.secondary_issues and d.proposed_action != A.ESCALATE_TO_HUMAN:
        problems.append(f"secondary_issues lists other problems ({[i.value for i in d.secondary_issues]}), so "
                        f"proposed_action must be escalate_to_human")
    if d.root_cause in d.secondary_issues or len(set(d.secondary_issues)) != len(d.secondary_issues):
        problems.append("secondary_issues must not repeat root_cause or each other")
    return problems


def validate_submission(raw: Any, returned_record_ids: set[str]) -> tuple[Diagnosis | None, list[str]]:
    """Schema validation plus contract checks. Returns (diagnosis, []) or (None, problems)."""
    try:
        d = Diagnosis.model_validate(raw)
    except ValidationError as err:
        return None, [f"{'.'.join(map(str, e['loc'])) or 'input'}: {e['msg']}"
                      for e in err.errors(include_url=False, include_input=False)]
    problems = contract_violations(d, returned_record_ids)
    return (None, problems) if problems else (d, [])


def contract_table() -> str:
    """The contract as plain text, for the system prompt."""
    lines = []
    for cause, actions in ALLOWED_ACTIONS.items():
        others = sorted(a.value for a in actions if a != A.ESCALATE_TO_HUMAN)
        lines.append(f"- {cause.value}: " + (", ".join(others) + ", or escalate_to_human" if others
                                              else "escalate_to_human ONLY"))
    return "\n".join(lines)


def is_record_id(value: str) -> bool:
    return re.match(RECORD_ID_PATTERN, value) is not None
