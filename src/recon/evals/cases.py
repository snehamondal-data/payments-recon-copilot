"""The eval set: every planted case in ground truth, matched to the exception it shows up as."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb

from recon.copilot.contract import RootCause
from recon.copilot.tools import ReadOnlyWarehouse

# Planted problem type -> the root cause a correct diagnosis names. Late settlements are not
# exceptions at all, so they never reach the copilot (Slice 2 checks they stay out of the queue).
ROOT_CAUSE_FOR = {"date_boundary": RootCause.DATE_MISMATCH}


@dataclass(frozen=True)
class EvalCase:
    case_id: str
    exception_type: str
    expected_root_cause: RootCause
    should_escalate: bool
    held_out: bool
    exception_id: str | None  # None if reconciliation never queued it (a Slice 2 miss)
    required_record_ids: tuple[str, ...]
    description: str


def load_cases(ground_truth_path: Path, warehouse: ReadOnlyWarehouse) -> tuple[list[EvalCase], list[dict]]:
    """Returns (eval cases, late-settlement cases). Each eval case names its queued exception_id."""
    gt = duckdb.connect(str(ground_truth_path), read_only=True)
    try:
        cur = gt.execute("SELECT * FROM planted_cases ORDER BY case_id")
        names = [d[0] for d in cur.description]
        rows = [dict(zip(names, r)) for r in cur.fetchall()]
    finally:
        gt.close()
    queued = {r["exception_id"] for r in warehouse.queue()}
    cases, late = [], []
    for r in rows:
        if r["exception_type"] == "late_settlement":
            late.append(r | {"in_queue": any(f"charge:{c}" in queued for c in r["charge_ids"])})
            continue
        candidates = [f"charge:{c}" for c in r["charge_ids"]] + [
            f"bank_line:{rid}" for rid in r["record_ids"] if rid.startswith("bln_")]
        cases.append(EvalCase(
            case_id=r["case_id"],
            exception_type=r["exception_type"],
            expected_root_cause=ROOT_CAUSE_FOR.get(r["exception_type"]) or RootCause(r["exception_type"]),
            should_escalate=r["should_escalate"],
            held_out=r["held_out"],
            exception_id=next((c for c in candidates if c in queued), None),
            required_record_ids=tuple(r["required_record_ids"]),
            description=r["description"],
        ))
    return cases, late
