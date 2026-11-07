"""Runs the copilot over every eval case for one (model, mode) and scores it.

Results are appended one JSON line per case, so a run can be stopped and resumed without paying twice,
and a budget cap (summed across every run so far) stops a run before it can overspend.
"""

from __future__ import annotations

import dataclasses
import json
from collections.abc import Callable
from pathlib import Path

from recon.config import Settings
from recon.copilot.agent import AgentConfig, Copilot
from recon.copilot.backend import make_backend
from recon.copilot.model_client import AnthropicModelClient, ModelClient
from recon.copilot.store import CopilotStore
from recon.copilot.tools import ReadOnlyWarehouse
from recon.evals.cases import load_cases
from recon.evals.scoring import CaseScore, score

MODES = ("standard", "blind")


def label(model: str, mode: str, prompt_version: str = "copilot-v1", backend: str = "inprocess") -> str:
    """copilot-v1 runs keep their original file names; later prompt versions get their own files, and runs
    through the MCP server (Slice 9e) their own again, so an in-process result is never overwritten."""
    name = f"{model}__{mode}" if prompt_version == "copilot-v1" else f"{model}__{mode}__{prompt_version}"
    return name + ("__mcp" if backend == "mcp" else "")


def load_scores(path: Path) -> list[CaseScore]:
    if not path.exists():
        return []
    return [CaseScore(**json.loads(line)) for line in path.read_text().splitlines() if line.strip()]


def spent_usd(results_dir: Path) -> float:
    return sum(s.cost_usd for path in results_dir.glob("*.jsonl") for s in load_scores(path))


def run_config(settings: Settings, *, model: str, mode: str, results_dir: Path, audit_dir: Path,
               budget_usd: float, client: ModelClient | None = None, limit: int | None = None,
               log: Callable[[str], None] = print) -> list[CaseScore]:
    if mode not in MODES:
        raise ValueError(f"mode must be one of {MODES}")
    results_dir.mkdir(parents=True, exist_ok=True)
    audit_dir.mkdir(parents=True, exist_ok=True)
    from recon.copilot.prompts import PROMPT_VERSION

    backend_kind = settings.copilot_tool_backend
    run_label = label(model, mode, PROMPT_VERSION, backend_kind)
    out_path = results_dir / f"{run_label}.jsonl"
    done = {s.case_id: s for s in load_scores(out_path)}

    # The harness keeps its own read-only connection to map cases to exceptions; with mcp, the agent
    # itself reads only through the server.
    warehouse = ReadOnlyWarehouse(settings.duckdb_path, forbidden_paths=settings.agent_forbidden_paths(),
                                  hide_reason_code=mode == "blind")
    backend = make_backend(dataclasses.replace(settings, copilot_hide_reason_code=mode == "blind",
                                               mcp_access_log=audit_dir / f"{run_label}.access.jsonl"),
                           warehouse)
    store = CopilotStore(audit_dir / f"{run_label}.duckdb")
    try:
        backend.start()
        cases, _ = load_cases(settings.ground_truth_path, warehouse)
        copilot = Copilot(client or AnthropicModelClient(model, effort=settings.copilot_effort), None,
                          AgentConfig(max_tool_calls=settings.copilot_max_tool_calls,
                                      timeout_s=settings.copilot_timeout_s), backend=backend)
        for i, case in enumerate(cases[:limit] if limit else cases, start=1):
            if case.case_id in done:
                continue
            spent = spent_usd(results_dir)
            if spent >= budget_usd:
                log(f"STOP: estimated spend ${spent:.2f} reached the ${budget_usd:.2f} budget")
                break
            if case.exception_id is None:
                s = score(case, None, model=model, mode=mode)
            else:
                run = copilot.diagnose(case.exception_id)
                store.record_run(run)
                s = score(case, run, model=model, mode=mode)
            s.tool_backend = backend_kind
            with out_path.open("a") as f:
                f.write(json.dumps(s.to_dict()) + "\n")
            done[case.case_id] = s
            verdict = "OK " if s.fully_correct else ("UNSAFE" if s.unsafe else "miss")
            log(f"[{label(model, mode)}] {i:2d}/{len(cases)} {verdict:<6} {case.case_id:<36} "
                f"-> {s.predicted_root_cause} / {s.proposed_action}{'  FALLBACK' if s.is_fallback else ''}  "
                f"${s.cost_usd:.3f} {s.latency_s:.0f}s")
    finally:
        backend.close()
        store.close()
        warehouse.close()
    return [done[c.case_id] for c in cases if c.case_id in done]
