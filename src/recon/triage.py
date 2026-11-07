"""Scheduled triage (Slice 9d): the copilot proposes a diagnosis for each new exception of an as-of date.
It never approves or applies anything: every proposal lands in the queue as proposed, marked
proposed_by = scheduled_triage with the DAG run ID, and a human approves, rejects or resolves it.

What the recon_triage DAG (airflow/dags/recon_triage.py) runs for each as-of date recon_daily snapshotted:
1. List the date's exceptions from its snapshot (marts.recon_exceptions_daily), not the live marts, which
   may already hold a later date: new since the previous date first, then the oldest.
2. Start one MCP server pinned to that date (COPILOT_AS_OF). The agent reads only through it, as the agent
   identity; this process reads the snapshot and writes the copilot store as the loader.
3. Skip every exception that already has a diagnosis for the same content hash, prompt version and model,
   so re-running a date proposes nothing. A changed exception (say, a second problem detected later) has a
   new hash and is triaged again. Fallbacks don't count: an infrastructure failure is retried next run.
4. Diagnose the rest, up to TRIAGE_MAX_EXCEPTIONS and TRIAGE_MAX_COST_USD per run, shared by every date
   in the run. The ones left over wait for the next run.
5. Record a date as finished for the model (COPILOT triage_dates) once nothing is left over. Every run
   also catches up on unfinished dates, oldest first: Airflow can drop an asset event that arrives
   while a triage run is already queued, so a run per event can't be relied on.

TRIAGE_MODEL_MODE=fake (the default) uses the scripted model: no API calls, no cost. live uses RECON_MODEL.

  python -m recon.triage --as-of 2026-10-16     # one date, outside Airflow
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys
from datetime import date
from typing import Any

from recon.config import Settings, load_settings
from recon.copilot.agent import AgentConfig, Copilot, content_hash
from recon.copilot.backend import BackendUnavailable, ToolBackend
from recon.copilot.contract import ProposedAction
from recon.copilot.prompts import PROMPT_VERSION
from recon.copilot.store import CopilotStore
from recon.warehouse import WarehouseClient

PROPOSED_BY = "scheduled_triage"
TRIAGE_DATES = "triage_dates"  # in the copilot store (RECON.COPILOT on Snowflake), written by the loader
EXCEPTIONS_TABLE = "marts.recon_exceptions_daily"  # written by recon.orchestration.snapshot_marts
SNAPSHOT_TABLE = "marts.recon_daily_snapshot"


def ordered_exceptions(reader: WarehouseClient, as_of: date) -> list[str]:
    """The date's exception IDs: those new since the previous snapshot first, then the rest, oldest first."""
    if not reader.execute(f"SELECT count(*) FROM {SNAPSHOT_TABLE} WHERE as_of_date = ? AND kind = 'build'",
                          (as_of,))[0][0]:
        raise RuntimeError(f"no snapshot for as-of {as_of}: run recon_daily for that date first")
    rows = [json.loads(r) for (r,) in reader.execute(
        f"SELECT row_json FROM {EXCEPTIONS_TABLE} WHERE as_of_date = ?", (as_of,))]
    previous = reader.execute(f"SELECT max(as_of_date) FROM {EXCEPTIONS_TABLE} WHERE as_of_date < ?", (as_of,))[0][0]
    before = set() if previous is None else {e for (e,) in reader.execute(
        f"SELECT exception_id FROM {EXCEPTIONS_TABLE} WHERE as_of_date = ?", (previous,))}
    rows.sort(key=lambda r: (r["exception_id"] in before, r["item_date"], r["exception_id"]))
    return [r["exception_id"] for r in rows]


def make_client(settings: Settings):
    if settings.triage_model_mode == "fake":
        from recon.copilot.scripted import ScriptedInvestigator

        return ScriptedInvestigator()
    if settings.triage_model_mode == "live":
        from recon.copilot.model_client import AnthropicModelClient

        return AnthropicModelClient(settings.recon_model, effort=settings.copilot_effort)
    raise ValueError(f"TRIAGE_MODEL_MODE must be fake or live, not {settings.triage_model_mode!r}")


def run_cost(run, live: bool) -> float:
    if not live:
        return 0.0
    from recon.evals.pricing import cost_usd

    return cost_usd(run.model, input_tokens=run.input_tokens, output_tokens=run.output_tokens,
                    cache_creation_input_tokens=run.cache_creation_input_tokens,
                    cache_read_input_tokens=run.cache_read_input_tokens)


def new_budget(settings: Settings) -> dict[str, float]:
    """What one run may still spend, shared by all its dates."""
    return {"exceptions": settings.triage_max_exceptions, "cost_usd": settings.triage_max_cost_usd}


def ensure_triage_dates(store: CopilotStore) -> None:
    ts = "TIMESTAMP_TZ" if store.db.dialect == "snowflake" else "TIMESTAMPTZ"
    store.db.execute(f"""CREATE TABLE IF NOT EXISTS {TRIAGE_DATES} (
        as_of_date DATE NOT NULL,
        model VARCHAR NOT NULL,
        prompt_version VARCHAR NOT NULL,
        dag_run_id VARCHAR NOT NULL,
        summary VARCHAR NOT NULL,
        finished_at {ts} NOT NULL DEFAULT current_timestamp,
        PRIMARY KEY (as_of_date, model, prompt_version))""")


def unfinished_dates(reader: WarehouseClient, store: CopilotStore, model: str) -> set[date]:
    """Snapshot dates with exceptions that this model and prompt version hasn't finished triaging."""
    snapshots = {d for (d,) in reader.execute(f"SELECT DISTINCT as_of_date FROM {EXCEPTIONS_TABLE}")}
    finished = {d for (d,) in store.db.execute(
        f"SELECT as_of_date FROM {TRIAGE_DATES} WHERE model = ? AND prompt_version = ?", (model, PROMPT_VERSION))}
    return {date.fromisoformat(str(d)) for d in snapshots} - {date.fromisoformat(str(d)) for d in finished}


def mark_finished(store: CopilotStore, result: dict[str, Any]) -> None:
    with store.db.transaction():
        store.db.execute(f"DELETE FROM {TRIAGE_DATES} WHERE as_of_date = ? AND model = ? AND prompt_version = ?",
                         (result["as_of"], result["model"], PROMPT_VERSION))
        store.db.execute(f"INSERT INTO {TRIAGE_DATES} (as_of_date, model, prompt_version, dag_run_id, summary) "
                         "VALUES (?, ?, ?, ?, ?)", (result["as_of"], result["model"], PROMPT_VERSION,
                                                    result["dag_run_id"], json.dumps(result)))


def triage_date(as_of: date, *, settings: Settings, reader: WarehouseClient, store: CopilotStore, client,
                dag_run_id: str, backend: ToolBackend | None = None, budget: dict[str, float] | None = None,
                log=print) -> dict[str, Any]:
    """Triage one as-of date within the run's budget (spent in place). Returns the counts the summary logs."""
    from recon.copilot.mcp_backend import McpBackend

    live = settings.triage_model_mode == "live"
    budget = new_budget(settings) if budget is None else budget
    ids = ordered_exceptions(reader, as_of)
    backend = backend or McpBackend(dataclasses.replace(settings, copilot_as_of=as_of))
    out: dict[str, Any] = {"as_of": as_of.isoformat(), "dag_run_id": dag_run_id, "model": client.model,
                           "exceptions": len(ids), "skipped": 0, "triaged": 0, "fixes_proposed": 0,
                           "escalations": 0, "fallbacks": 0, "left_over": 0, "stopped_by": None, "cost_usd": 0.0,
                           "proposals": []}
    backend.start()
    try:
        copilot = Copilot(client, None, AgentConfig(max_tool_calls=settings.copilot_max_tool_calls,
                                                    timeout_s=settings.copilot_timeout_s), backend=backend)
        done = store.proposal_keys(prompt_version=PROMPT_VERSION, model=client.model)
        todo: list[tuple[str, dict[str, Any] | None]] = []
        for eid in ids:
            try:
                row = backend.exception_row(eid)
            except BackendUnavailable:
                row = None  # diagnose fetches it again and ends in the fallback, with the reason logged
            if row is not None and (eid, content_hash(row)) in done:
                out["skipped"] += 1
            else:
                todo.append((eid, row))
        for eid, row in todo:
            if budget["exceptions"] <= 0:
                out["stopped_by"] = f"TRIAGE_MAX_EXCEPTIONS={settings.triage_max_exceptions}"
                break
            if budget["cost_usd"] <= 0:
                out["stopped_by"] = f"TRIAGE_MAX_COST_USD={settings.triage_max_cost_usd:.2f}"
                break
            run = copilot.diagnose(eid, row=row, proposed_by=PROPOSED_BY, dag_run_id=dag_run_id)
            store.record_run(run)  # as proposed; nothing here approves, rejects or resolves
            d = run.diagnosis
            escalated = d.proposed_action == ProposedAction.ESCALATE_TO_HUMAN
            out["triaged"] += 1
            out["escalations" if escalated else "fixes_proposed"] += 1
            out["fallbacks"] += run.is_fallback
            cost = run_cost(run, live)
            out["cost_usd"] += cost
            budget["exceptions"] -= 1
            budget["cost_usd"] -= cost
            out["proposals"].append({"exception_id": eid, "root_cause": d.root_cause.value,
                                     "proposed_action": d.proposed_action.value, "fallback": run.is_fallback})
            log(f"{as_of} {eid}: {d.root_cause.value} -> {d.proposed_action.value}"
                + (f"  FALLBACK: {run.fallback_reason}" if run.is_fallback else ""))
        out["left_over"] = len(todo) - out["triaged"]
        if out["left_over"]:
            log(f"{as_of}: stopped at {out['stopped_by']}; {out['left_over']} left for the next run")
    finally:
        backend.close()
    out["cost_usd"] = round(out["cost_usd"], 4)
    return out


def summary_lines(results: list[dict[str, Any]]) -> list[str]:
    lines = []
    for r in results:
        lines.append(f"{r['as_of']} ({r['model']}): {r['exceptions']} exceptions; triaged {r['triaged']} "
                     f"({r['fixes_proposed']} fixes proposed, {r['escalations']} escalations, {r['fallbacks']} "
                     f"fallbacks); skipped {r['skipped']} already triaged; left over {r['left_over']}"
                     + (f" ({r['stopped_by']})" if r["stopped_by"] else "") + f"; cost ${r['cost_usd']:.4f}")
    lines.append("Every one is a proposal for a human to approve or reject: python -m recon.copilot queue")
    return lines


def open_reader_and_store(settings: Settings) -> tuple[WarehouseClient, CopilotStore]:
    """The loader's side: reads the snapshot, writes the copilot store. (The agent reads through MCP only.)"""
    if settings.warehouse_engine == "snowflake":
        from recon.warehouse.snowflake_client import SnowflakeClient

        return SnowflakeClient(settings.snowflake, settings.snowflake.loader), CopilotStore.snowflake(settings.snowflake)
    from recon.warehouse import DuckDBClient

    # Read-only, so the MCP server's own read-only connection to the same file can open it.
    return DuckDBClient(settings.duckdb_path, read_only=True), CopilotStore(settings.copilot_db_path)


def run(as_of_dates: list[date], *, dag_run_id: str, settings: Settings | None = None, catch_up: bool = True,
        log=print) -> list[dict]:
    """Triage the given dates and (catch_up) every unfinished one, oldest first, within one run's budget."""
    settings = settings or load_settings()
    reader, store = open_reader_and_store(settings)
    try:
        ensure_triage_dates(store)
        client = make_client(settings)
        dates = set(as_of_dates)
        if catch_up:
            caught = unfinished_dates(reader, store, client.model) - dates
            if caught:
                log(f"catching up on unfinished dates: {sorted(d.isoformat() for d in caught)}")
            dates |= caught
        budget, results = new_budget(settings), []
        for day in sorted(dates):
            result = triage_date(day, settings=settings, reader=reader, store=store, client=client,
                                 dag_run_id=dag_run_id, budget=budget, log=log)
            if result["left_over"] == 0:
                mark_finished(store, result)
            results.append(result)
        return results
    finally:
        store.close()
        reader.close()


def main(argv: list[str] | None = None) -> int:
    from dotenv import load_dotenv

    load_dotenv()
    parser = argparse.ArgumentParser(prog="python -m recon.triage", description=__doc__.split("\n\n")[0])
    parser.add_argument("--as-of", type=date.fromisoformat, action="append", default=[])
    parser.add_argument("--dag-run-id", default="manual")
    parser.add_argument("--no-catch-up", action="store_true", help="only the --as-of dates")
    args = parser.parse_args(argv)
    results = run(args.as_of, dag_run_id=args.dag_run_id, catch_up=not args.no_catch_up)
    for line in summary_lines(results):
        print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
