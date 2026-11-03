"""A single demo review session, and the operational metrics read back from the audit log.

The demo reviewer is scripted and knows the ground truth, so its decisions measure agreement with
the planted answers, and its review time is near zero. These metrics show the pipeline working end to
end; they are not a measurement of a real operations team.
"""

from __future__ import annotations

import json
import statistics
from typing import Any

from recon.copilot.contract import ALLOWED_ACTIONS, ProposedAction, RootCause, is_allowed
from recon.copilot.store import CopilotStore
from recon.copilot.tools import ReadOnlyWarehouse
from recon.evals.cases import EvalCase

DEMO_REVIEWER = "demo-reviewer"


def human_action(cause: RootCause) -> ProposedAction | None:
    """What the demo reviewer does by hand for an escalated case. None for ambiguous: it stays open."""
    actions = sorted(a for a in ALLOWED_ACTIONS[cause] if a != ProposedAction.ESCALATE_TO_HUMAN)
    return actions[0] if actions else None


def demo_review(store: CopilotStore, warehouse: ReadOnlyWarehouse, cases: list[EvalCase]) -> dict[str, int]:
    counts = {"approved": 0, "rejected": 0, "resolved_by_hand": 0, "left_open": 0}
    for case in cases:
        if case.exception_id is None:
            continue
        p = store.latest_proposal(case.exception_id)
        if p is None or p.status != "proposed":
            continue
        amount = warehouse.exception_row(case.exception_id)["amount_minor"]
        truth = case.expected_root_cause
        d = p.diagnosis
        escalated = p.is_fallback or d["proposed_action"] == ProposedAction.ESCALATE_TO_HUMAN
        by_hand = human_action(truth)
        if escalated:
            if by_hand is None:
                counts["left_open"] += 1  # ambiguous: waiting on the bank or processor to say which it was
                continue
            store.resolve(case.exception_id, action=by_hand.value, root_cause=truth.value, reviewer=DEMO_REVIEWER,
                          reason=f"Escalated; investigated by hand: {truth.value}", amount_minor=amount)
            counts["resolved_by_hand"] += 1
        elif d["root_cause"] == truth.value and is_allowed(truth, ProposedAction(d["proposed_action"])):
            store.approve(case.exception_id, reviewer=DEMO_REVIEWER, reason="Evidence checks out", amount_minor=amount)
            counts["approved"] += 1
        else:
            store.reject(case.exception_id, reviewer=DEMO_REVIEWER,
                         reason=f"Wrong diagnosis: this is {truth.value}, not {d['root_cause']}")
            counts["rejected"] += 1
            if by_hand is not None:
                store.resolve(case.exception_id, action=by_hand.value, root_cause=truth.value,
                              reviewer=DEMO_REVIEWER, reason="Resolved by hand after rejecting the proposal",
                              amount_minor=amount)
    return counts


def review_metrics(store: CopilotStore, warehouse: ReadOnlyWarehouse) -> dict[str, Any]:
    """Backlog, acceptance rate and time to resolve, computed only from the audit log."""
    queue = {r["exception_id"]: r for r in warehouse.queue()}
    log = store.con.execute(
        "SELECT exception_id, run_id, event_type, payload, created_at FROM audit_log ORDER BY audit_id").fetchall()
    first_seen: dict[str, Any] = {}
    resolved_at: dict[str, Any] = {}
    first_decision: dict[str, str] = {}
    reviewed_runs: set[str] = set()
    for exception_id, run_id, event_type, payload, created_at in log:
        payload = json.loads(payload)
        if event_type == "run_started":
            first_seen.setdefault(exception_id, created_at)
        elif event_type == "review":
            first_decision.setdefault(exception_id, payload["decision"])
            if payload.get("reviewer"):
                reviewed_runs.add(run_id)
        elif event_type == "resolution_applied":
            resolved_at.setdefault(exception_id, created_at)
    resolutions = store.resolutions()
    open_ids = [eid for eid in queue if eid not in resolved_at]
    open_ages = [queue[eid]["age_business_days"] for eid in open_ids]
    decided = list(first_decision.values())
    seconds = [(resolved_at[eid] - first_seen[eid]).total_seconds() for eid in resolved_at if eid in first_seen]
    return {
        "backlog_before": len(queue),
        "backlog_after": len(open_ids),
        "backlog_open_ids": open_ids,
        "backlog_max_age_business_days": max(open_ages) if open_ages else 0,
        "reviewed": len(decided),
        "approved": decided.count("approved"),
        "rejected": decided.count("rejected"),
        "resolved_by_hand": decided.count("resolved"),
        # Spec section 2: % of exceptions where the copilot's diagnosis is accepted by a human.
        "acceptance_rate": decided.count("approved") / len(decided) if decided else 0.0,
        "median_time_to_resolve_s": statistics.median(seconds) if seconds else None,
        "resolutions": len(resolutions),
        "resolutions_without_human_review": sum(r["run_id"] not in reviewed_runs for r in resolutions),
    }
