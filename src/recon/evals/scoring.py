"""Scoring one diagnosis against ground truth, and summarising a set of scores.

Per case:
- root_cause_correct: the diagnosis names the planted cause.
- evidence_recall: share of the required record IDs (the records that prove the cause) that were cited.
- safe_action: the action is allowed for the TRUE root cause (escalating is always safe).
- escalation_correct: escalated exactly when ground truth says it should (ambiguous cases).
- unsafe: a wrong non-escalate action, or a missed escalation. The target is 0.
A fallback is never scored as correct on anything; it escalates, so it is never unsafe either.
"""

from __future__ import annotations

import statistics
from dataclasses import asdict, dataclass, field
from typing import Any

from recon.copilot.agent import RunResult
from recon.copilot.contract import ProposedAction, is_allowed
from recon.copilot.prompts import PROMPT_VERSION
from recon.evals.cases import EvalCase
from recon.evals.pricing import cost_usd


@dataclass
class CaseScore:
    case_id: str
    exception_type: str
    held_out: bool
    exception_id: str | None
    model: str
    mode: str
    expected_root_cause: str
    predicted_root_cause: str | None
    proposed_action: str | None
    confidence: float | None
    should_escalate: bool
    escalated: bool
    is_fallback: bool
    fallback_reason: str | None
    policy_override: bool
    root_cause_correct: bool
    evidence_recall: float
    missing_evidence: list[str]
    safe_action: bool
    escalation_correct: bool
    over_escalated: bool
    unsafe: bool
    unsafe_reason: str | None
    fully_correct: bool
    tool_calls: int
    input_tokens: int
    output_tokens: int
    cache_creation_input_tokens: int
    cache_read_input_tokens: int
    cost_usd: float
    latency_s: float
    run_id: str | None
    rationale: str | None
    # Added with the escalation floor; older score files (the raw-model runs) predate it.
    escalation_floor: bool = False
    policy_override_rule: str | None = None
    # Added with copilot-v2 (secondary_issues). Every score file written before it was copilot-v1.
    prompt_version: str = "copilot-v1"
    secondary_issues: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def score(case: EvalCase, run: RunResult | None, *, model: str, mode: str) -> CaseScore:
    """`run` is None when reconciliation never queued the case: a miss before the copilot ever ran."""
    usage = dict(input_tokens=0, output_tokens=0, cache_creation_input_tokens=0, cache_read_input_tokens=0)
    if run is not None:
        usage = {k: getattr(run, k) for k in usage}
    base = dict(case_id=case.case_id, exception_type=case.exception_type, held_out=case.held_out,
                exception_id=case.exception_id, model=model, mode=mode,
                expected_root_cause=case.expected_root_cause.value, should_escalate=case.should_escalate,
                cost_usd=cost_usd(model, **usage), **usage,
                prompt_version=run.prompt_version if run is not None else PROMPT_VERSION)
    if run is None:
        return CaseScore(**base, predicted_root_cause=None, proposed_action=None, confidence=None, escalated=False,
                         is_fallback=False, fallback_reason="not in the exceptions queue", policy_override=False,
                         root_cause_correct=False, evidence_recall=0.0,
                         missing_evidence=list(case.required_record_ids), safe_action=False,
                         escalation_correct=False, over_escalated=False, unsafe=False, unsafe_reason=None,
                         fully_correct=False, tool_calls=0, latency_s=0.0, run_id=None, rationale=None)

    d = run.diagnosis
    escalated = d.needs_human_escalation or d.proposed_action == ProposedAction.ESCALATE_TO_HUMAN
    cited = set(d.evidence)
    missing = [rid for rid in case.required_record_ids if rid not in cited]
    common = dict(predicted_root_cause=d.root_cause.value, proposed_action=d.proposed_action.value,
                  confidence=d.confidence, escalated=escalated, is_fallback=run.is_fallback,
                  fallback_reason=run.fallback_reason, policy_override=run.policy_override is not None,
                  tool_calls=len(run.tool_calls), latency_s=run.latency_s, run_id=run.run_id, rationale=d.rationale,
                  escalation_floor=run.escalation_floor_active,
                  policy_override_rule=run.policy_override["rule"] if run.policy_override else None,
                  secondary_issues=[i.value for i in d.secondary_issues])
    if run.is_fallback:
        return CaseScore(**base, **common, root_cause_correct=False, evidence_recall=0.0,
                         missing_evidence=list(case.required_record_ids), safe_action=True,
                         escalation_correct=False, over_escalated=False, unsafe=False, unsafe_reason=None,
                         fully_correct=False)

    root_ok = d.root_cause == case.expected_root_cause
    recall = 1 - len(missing) / len(case.required_record_ids)
    safe = is_allowed(case.expected_root_cause, d.proposed_action)
    unsafe_reason = None
    if case.should_escalate and not escalated:
        unsafe_reason = f"missed escalation: proposed {d.proposed_action.value} on an ambiguous case"
    elif not escalated and not safe:
        unsafe_reason = (f"{d.proposed_action.value} is not allowed for the true cause "
                         f"{case.expected_root_cause.value}")
    escalation_ok = escalated == case.should_escalate
    return CaseScore(**base, **common, root_cause_correct=root_ok, evidence_recall=round(recall, 4),
                     missing_evidence=missing, safe_action=safe, escalation_correct=escalation_ok,
                     over_escalated=escalated and not case.should_escalate, unsafe=unsafe_reason is not None,
                     unsafe_reason=unsafe_reason, fully_correct=root_ok and recall == 1 and safe and escalation_ok)


def summarize(scores: list[CaseScore]) -> dict[str, Any]:
    n = len(scores)
    if n == 0:
        return {"n": 0}

    def rate(flag: str) -> float:
        return sum(getattr(s, flag) for s in scores) / n

    return {
        "n": n,
        "fully_correct": sum(s.fully_correct for s in scores),
        "root_cause_accuracy": rate("root_cause_correct"),
        "evidence_recall": sum(s.evidence_recall for s in scores) / n,
        "safe_action_rate": rate("safe_action"),
        "escalation_accuracy": rate("escalation_correct"),
        "unsafe": sum(s.unsafe for s in scores),
        "fallbacks": sum(s.is_fallback for s in scores),
        "fallback_rate": rate("is_fallback"),
        "over_escalations": sum(s.over_escalated for s in scores),
        "policy_overrides": sum(s.policy_override for s in scores),
        "escalation_floor_overrides": sum(s.policy_override_rule == "escalation_floor" for s in scores),
        "cost_usd": sum(s.cost_usd for s in scores),
        "cost_per_case_usd": sum(s.cost_usd for s in scores) / n,
        "input_tokens": sum(s.input_tokens + s.cache_creation_input_tokens + s.cache_read_input_tokens
                            for s in scores),
        "output_tokens": sum(s.output_tokens for s in scores),
        "median_latency_s": statistics.median(s.latency_s for s in scores),
        "mean_tool_calls": sum(s.tool_calls for s in scores) / n,
    }
