"""The two harder cases (--scenario hard), kept apart from the standard seed-42 data and results.

  python -m recon.generator --scenario hard     # data/hard/: warehouse, ground truth with hard_expectations
  make recon-hard                               # dbt build on data/hard/warehouse.duckdb
  python -m recon.evals hard --model claude-sonnet-5-5 --mode standard

The expected outcomes are written to ground truth when the scenario is generated, before dbt or the
copilot runs. This module only reads them; nothing here changes a prompt or the SQL based on a result.
"""

from __future__ import annotations

import json
import shutil
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any

import duckdb

from recon.copilot.contract import RootCause
from recon.evals.cases import EvalCase

HARD_DIR = Path("data/hard")
WAREHOUSE = HARD_DIR / "warehouse.duckdb"
GROUND_TRUTH = HARD_DIR / "ground_truth.duckdb"
COPILOT_DB = HARD_DIR / "copilot.duckdb"
RESULTS_DIR = Path("evals/results/hard")


def build_hard(*, seed: int = 42, as_of: date = date(2026, 10, 15), log=print) -> None:
    """Generate the hard scenario into data/hard/ and print its pre-registered expectations. No dbt, no model."""
    from recon.fx import write_fx_csv
    from recon.generator import build_scenario
    from recon.generator.ground_truth import write_ground_truth
    from recon.orchestration import load_without_kafka

    scenario = build_scenario(seed, as_of, scenario="hard")
    shutil.rmtree(HARD_DIR, ignore_errors=True)
    HARD_DIR.mkdir(parents=True)
    write_ground_truth(GROUND_TRUTH, scenario)
    write_fx_csv(HARD_DIR / "fx_rates.csv", scenario.fx_rates)
    load_without_kafka(WAREHOUSE, scenario)
    log(f"hard scenario: seed={seed} as_of={as_of} sha256={scenario.content_hash()[:12]} cases={len(scenario.cases)}")
    log(f"wrote {WAREHOUSE} and {GROUND_TRUTH}; expected outcomes, fixed before any run:")
    for row in expectations(GROUND_TRUTH):
        log(f"  {row['case_id']}: status={row['expected_status']} reason_code={row['expected_reason_code']} "
            f"escalate={row['should_escalate']}")
        log(f"    {row['description']}")
        log(f"    why: {row['rationale']}")


def expectations(ground_truth_path: Path) -> list[dict[str, Any]]:
    con = duckdb.connect(str(ground_truth_path), read_only=True)
    try:
        cur = con.execute("""
            SELECT e.*, p.exception_type, p.charge_ids, p.record_ids, p.required_record_ids, p.description
            FROM hard_expectations AS e JOIN planted_cases AS p USING (case_id) ORDER BY case_id""")
        names = [d[0] for d in cur.description]
        return [dict(zip(names, r)) for r in cur.fetchall()]
    finally:
        con.close()


def sql_classification(con, case: dict[str, Any]) -> dict[str, Any]:
    """What reconciliation did with this case, read from the marts."""
    charges = list(case["charge_ids"])
    lines = [r for r in case["record_ids"] if r.startswith("bln_")]
    status = dict(con.execute("SELECT charge_id, window_status FROM intermediate.int_charge_recon "
                              "WHERE charge_id IN (SELECT unnest(?))", [charges]).fetchall())
    queued = [{"exception_id": e, "reason_code": rc, "amount_minor": amt} for e, rc, amt in con.execute(
        "SELECT exception_id, reason_code, amount_minor FROM marts.recon_exceptions "
        "WHERE charge_id IN (SELECT unnest(?)) OR bank_line_id IN (SELECT unnest(?)) "
        "OR list_has_any(string_split(coalesce(related_charge_ids, ''), ','), ?) ORDER BY exception_id",
        [charges, lines, charges]).fetchall()]
    statuses = sorted(set(status.values()))
    got_status = "exception" if queued else (statuses[0] if len(statuses) == 1 else ",".join(statuses))
    reasons = sorted({q["reason_code"] for q in queued})
    if case["expected_status"] == "matched":
        correct = not queued and got_status == "matched"
    else:
        correct = bool(queued) and reasons == [case["expected_reason_code"]]
    return {"window_status": got_status, "queued": queued, "reason_codes": reasons, "correct": correct}


def run_hard(model: str, mode: str, *, client=None, log=print) -> list[dict[str, Any]]:
    from recon.copilot.agent import AgentConfig, Copilot
    from recon.copilot.model_client import AnthropicModelClient
    from recon.copilot.store import CopilotStore
    from recon.copilot.tools import ReadOnlyWarehouse, open_read_only
    from recon.evals.scoring import score

    out_path = RESULTS_DIR / f"{model}__{mode}.jsonl"
    if out_path.exists():
        raise FileExistsError(f"{out_path} exists: these are measured results; move it aside to run again")
    cases = expectations(GROUND_TRUTH)
    con = open_read_only(WAREHOUSE)
    try:
        sql = {c["case_id"]: sql_classification(con, c) for c in cases}
    finally:
        con.close()
    warehouse = ReadOnlyWarehouse(WAREHOUSE, forbidden_paths=(GROUND_TRUTH, COPILOT_DB),
                                  hide_reason_code=mode == "blind")
    store = CopilotStore(COPILOT_DB)
    client = client or AnthropicModelClient(model)
    results = []
    try:
        for c in cases:
            s = sql[c["case_id"]]
            record: dict[str, Any] = {"case_id": c["case_id"], "model": model, "mode": mode,
                                      "expected": {k: c[k] for k in ("expected_status", "expected_reason_code",
                                                                     "should_escalate")},
                                      "sql": s, "copilot": None, "score": None}
            log(f"{c['case_id']}: SQL {s['window_status']} {s['reason_codes'] or ''} -> "
                f"{'correct' if s['correct'] else 'MISS'}")
            for q in s["queued"]:  # the copilot only ever sees what reconciliation queued
                run = Copilot(client, warehouse, AgentConfig(escalation_floor=mode == "standard")).diagnose(
                    q["exception_id"])
                store.record_run(run)
                d = run.diagnosis
                record["copilot"] = {"exception_id": q["exception_id"], "run_id": run.run_id,
                                     "diagnosis": d.model_dump(mode="json"), "is_fallback": run.is_fallback,
                                     "policy_override": run.policy_override, "tool_calls": len(run.tool_calls)}
                if c["expected_reason_code"] is not None:  # a false alarm has no right answer to score against
                    case = EvalCase(c["case_id"], c["exception_type"], RootCause(c["expected_reason_code"]),
                                    c["should_escalate"], False, q["exception_id"], tuple(c["required_record_ids"]),
                                    c["description"])
                    record["score"] = asdict(score(case, run, model=model, mode=mode))
                log(f"  copilot on {q['exception_id']}: {d.root_cause.value} -> {d.proposed_action.value} "
                    f"(escalate={d.needs_human_escalation}, confidence {d.confidence:.2f})")
            results.append(record)
    finally:
        store.close()
        warehouse.close()
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path.write_text("".join(json.dumps(r, default=str) + "\n" for r in results))
    return results
