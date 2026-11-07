"""In-process vs MCP parity (Slice 9c). No API calls: a scripted investigator stands in for the model.

  make parity-mcp     # python -m recon.mcp_parity, on DUCKDB_PATH (data/warehouse.duckdb)

Two checks, like the DuckDB vs Snowflake parity:
  1. Every tool output the agent could ask for, for every exception, byte for byte through both backends.
  2. The full `diagnose --all` flow through both backends, into two fresh copilot stores: every audit
     event (and so every diagnosis) identical, apart from run IDs, timestamps and durations.

The investigator decides each step from what the tools returned (which tools to call from the exception's
IDs, which records to cite, what to propose), so a difference in any tool output changes the transcript.
"""

from __future__ import annotations

import dataclasses
import json
import re
import sys
import tempfile
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import duckdb

from recon.config import Settings, load_settings
from recon.copilot.contract import ALLOWED_ACTIONS, ProposedAction, RootCause
from recon.copilot.tools import ReadOnlyWarehouse

# Differ on every run by design.
VOLATILE = {"run_id", "created_at", "started_at", "finished_at", "duration_ms", "latency_s"}


@dataclass
class _Block:
    type: str
    id: str | None = None
    name: str | None = None
    input: Any = None


@dataclass
class _Usage:
    input_tokens: int = 0
    output_tokens: int = 0


@dataclass
class _Response:
    content: list[_Block]
    stop_reason: str = "tool_use"
    model: str = "scripted-investigator"
    usage: _Usage = field(default_factory=_Usage)

    def to_dict(self) -> dict[str, Any]:
        return {"content": [asdict(b) for b in self.content], "stop_reason": self.stop_reason, "model": self.model}


class ScriptedInvestigator:
    """A deterministic stand-in for the model: get_exception, then the source tools for its IDs, then a
    diagnosis citing what came back. For an ambiguous item it claims a fix, so the escalation floor acts."""

    model = "scripted-investigator"

    def create(self, *, system, messages, tools, max_tokens, timeout) -> _Response:
        turn = sum(1 for m in messages if m["role"] == "assistant")
        uses: list[_Block] = []

        def use(name: str, **arguments: Any) -> None:
            uses.append(_Block("tool_use", id=f"toolu_{turn}_{len(uses)}", name=name, input=arguments))

        results = _tool_results(messages)
        if turn == 0:
            use("get_exception", exception_id=re.search(r"(charge|bank_line):\S+_[0-9a-f]{16}",
                                                        messages[0]["content"]).group(0))
        elif turn == 1 and results and results[0][1] is not None:
            row = results[0][1][0]
            if row.get("charge_id"):
                for name in ("get_processor_events", "get_bank_settlement", "get_ledger_entries",
                             "get_related_refunds"):
                    use(name, txn_id=row["charge_id"])
            if row.get("bank_line_id"):
                use("get_bank_settlement", txn_id=row["bank_line_id"])
            use("get_fx_rate", date=row["item_date"], currency="EUR")
        else:
            row = results[0][1][0] if results and results[0][1] else {}
            seen = [r["record_id"] for _, records in results if records for r in records]
            cause = RootCause(row.get("reason_code") or RootCause.AMBIGUOUS)
            if cause == RootCause.AMBIGUOUS:
                cause = RootCause.MISSING_SETTLEMENT  # a confident wrong answer the floor must stop
            action = min(a for a in ALLOWED_ACTIONS[cause] if a != ProposedAction.ESCALATE_TO_HUMAN)
            errors = sum(1 for _, records in results if records is None)
            use("submit_diagnosis", root_cause=cause.value, evidence=sorted(set(seen))[:8],
                proposed_action=action.value, confidence=0.8, needs_human_escalation=False,
                rationale=f"Scripted: {len(seen)} records from {len(results)} tool calls, {errors} errors.")
        return _Response(uses)


def _tool_results(messages: list[dict[str, Any]]) -> list[tuple[str, list[dict[str, Any]] | None]]:
    """(content, records or None for an error) for every tool result so far, in order."""
    out = []
    for m in messages:
        if m["role"] != "user" or not isinstance(m["content"], list):
            continue
        for block in m["content"]:
            if isinstance(block, dict) and block.get("type") == "tool_result":
                records = None if block.get("is_error") else json.loads(block["content"])["records"]
                out.append((block["content"], records))
    return out


def _strip(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _strip(v) for k, v in value.items() if k not in VOLATILE}
    if isinstance(value, list):
        return [_strip(v) for v in value]
    return value


def audit_events(copilot_db: Path) -> list[dict[str, Any]]:
    con = duckdb.connect(str(copilot_db), read_only=True)
    try:
        cur = con.execute("SELECT * FROM audit_log ORDER BY audit_id")
        names = [d[0] for d in cur.description]
        rows = [dict(zip(names, r)) for r in cur.fetchall()]
    finally:
        con.close()
    for row in rows:
        row["payload"] = json.loads(row["payload"])
    return [_strip(row) for row in rows]


def diagnose_all(settings: Settings, backend: str, workdir: Path) -> tuple[list[str], list[dict[str, Any]]]:
    """Run `diagnose --all` through one backend into a fresh store; return the CLI output and the audit."""
    from recon.copilot.__main__ import main

    run = dataclasses.replace(settings, copilot_tool_backend=backend,
                              copilot_db_path=workdir / f"copilot_{backend}.duckdb",
                              mcp_access_log=workdir / f"mcp_access_{backend}.jsonl")
    out: list[str] = []
    code = main(["diagnose", "--all"], settings=run, client_factory=lambda s: ScriptedInvestigator(), out=out.append)
    if code != 0:
        raise RuntimeError(f"diagnose --all through {backend} exited {code}: {out}")
    return out, audit_events(run.copilot_db_path)


def compare(settings: Settings, workdir: Path, log=print) -> dict[str, Any]:
    from recon.copilot.mcp_backend import McpBackend
    from recon.parity import tool_outputs

    wh = ReadOnlyWarehouse(settings.duckdb_path, forbidden_paths=settings.agent_forbidden_paths(),
                           hide_reason_code=settings.copilot_hide_reason_code)
    mcp = McpBackend(dataclasses.replace(settings, mcp_access_log=workdir / "mcp_access_outputs.jsonl"))
    try:
        exceptions = len(wh.queue())
        mcp.start()
        inprocess, over_mcp = tool_outputs(wh), tool_outputs(wh, tools=mcp)
    finally:
        mcp.close()
        wh.close()
    output_diffs = sorted(k for k in inprocess.keys() | over_mcp.keys() if inprocess.get(k) != over_mcp.get(k))

    lines_in, events_in = diagnose_all(settings, "inprocess", workdir)
    lines_mcp, events_mcp = diagnose_all(settings, "mcp", workdir)
    event_diffs = [i for i, (a, b) in enumerate(zip(events_in, events_mcp)) if a != b]
    if len(events_in) != len(events_mcp):
        event_diffs.append(min(len(events_in), len(events_mcp)))
    diagnoses = [e["payload"] for e in events_in if e["event_type"] == "diagnosis"]
    report = {"exceptions": exceptions, "tool_outputs": len(inprocess), "tool_output_diffs": output_diffs,
              "diagnosed": len(diagnoses), "fallbacks": sum(d["is_fallback"] for d in diagnoses),
              "policy_overrides": sum(1 for e in events_in if e["event_type"] == "policy_override"),
              "audit_events": len(events_in), "audit_event_diffs": event_diffs,
              "cli_output_identical": lines_in == lines_mcp}
    report["identical"] = not output_diffs and not event_diffs and report["cli_output_identical"]
    log(f"exceptions: {exceptions}")
    log(f"tool outputs: {len(inprocess)} compared, {len(output_diffs)} different")
    log(f"diagnose --all: {report['diagnosed']} diagnoses ({report['fallbacks']} fallbacks, "
        f"{report['policy_overrides']} escalation-floor overrides)")
    log(f"audit events: {len(events_in)} in-process, {len(events_mcp)} over MCP, {len(event_diffs)} different")
    log(f"CLI output identical: {report['cli_output_identical']}")
    log("PARITY: identical" if report["identical"] else "PARITY: DIFFERENT")
    return report


def main() -> int:
    from dotenv import load_dotenv

    load_dotenv()
    settings = load_settings()
    if settings.warehouse_engine != "duckdb":
        print("make parity-mcp runs on DuckDB (WAREHOUSE_ENGINE=duckdb)", file=sys.stderr)
        return 2
    with tempfile.TemporaryDirectory(prefix="parity_mcp_") as workdir:
        return 0 if compare(settings, Path(workdir))["identical"] else 1


if __name__ == "__main__":
    sys.exit(main())
