"""What the Airflow DAGs run (airflow/dags/). Plain functions, so they are unit-testable without Airflow.

recon_daily, for one logical date (the as-of date):
  load_new_messages -> dbt build + snapshot_marts -> recheck_pending -> summarize

The marts hold one as-of date at a time: the last one dbt built. So the dbt task snapshots the pending
settlements and the exceptions for its date straight after building, and everything downstream reads
only that date's snapshot, never the live marts. That makes every step safe to re-run for its date at
any time, in any order, even after later dates have been built:
- the load is idempotent by event_id and only moves the consumer group forward;
- dbt rebuilds the marts for the date, and the snapshot replaces that date's rows;
- recheck_pending replaces that date's rows in the runs and window-history tables;
- summarize only reads.
Writes replace a date's rows with delete then insert in one transaction.

The copilot is deliberately absent: it proposes only when a human runs it (python -m recon.copilot).
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

from recon.config import Settings
from recon.contracts import DLQRecord
from recon.warehouse import WarehouseClient

DAILY_CONSUMER_GROUP = "recon-airflow-loader"
SNAPSHOT_TABLE = "marts.recon_daily_snapshot"
RUNS_TABLE = "marts.recon_daily_runs"
HISTORY_TABLE = "marts.recon_window_history"


def _ddl(dialect: str) -> list[str]:
    # Written by these tasks, not by dbt. dbt rebuilds the marts for one as-of date; these keep every date.
    ts = "TIMESTAMP_TZ" if dialect == "snowflake" else "TIMESTAMPTZ"
    return [
        # What the marts said for each as-of date. kind = build (one marker row per date) | pending | exception.
        f"""CREATE TABLE IF NOT EXISTS {SNAPSHOT_TABLE} (
            as_of_date DATE NOT NULL,
            kind VARCHAR NOT NULL,
            item_id VARCHAR NOT NULL,         -- charge_id for pending, exception_id for exception
            charge_id VARCHAR,
            related_charge_ids VARCHAR,
            reason_code VARCHAR,
            amount_minor BIGINT,
            age_business_days INTEGER,
            settlement_deadline DATE,
            business_days_left INTEGER,
            recorded_at {ts} NOT NULL DEFAULT current_timestamp,
            PRIMARY KEY (as_of_date, kind, item_id))""",
        # One row per as-of date, even when nothing was pending.
        f"""CREATE TABLE IF NOT EXISTS {RUNS_TABLE} (
            as_of_date DATE NOT NULL PRIMARY KEY,
            previous_as_of_date DATE,
            pending INTEGER NOT NULL,
            newly_closed INTEGER NOT NULL,
            settled INTEGER NOT NULL,
            backlog INTEGER NOT NULL,
            recorded_at {ts} NOT NULL DEFAULT current_timestamp)""",
        # One row per (as_of_date, charge_id) pending then, or pending on the previous date.
        f"""CREATE TABLE IF NOT EXISTS {HISTORY_TABLE} (
            as_of_date DATE NOT NULL,
            charge_id VARCHAR NOT NULL,
            status VARCHAR NOT NULL,          -- pending | exception | settled
            reason_code VARCHAR,              -- for status = exception
            exception_id VARCHAR,
            settlement_deadline DATE,
            business_days_left INTEGER,
            previous_as_of_date DATE,         -- the earlier date this was compared with
            recorded_at {ts} NOT NULL DEFAULT current_timestamp,
            PRIMARY KEY (as_of_date, charge_id))""",
    ]


def ensure_tables(wh: WarehouseClient) -> None:
    for statement in _ddl(wh.dialect):
        wh.execute(statement)


def _rows(wh: WarehouseClient, sql: str, columns: tuple[str, ...], params: tuple = ()) -> list[dict[str, Any]]:
    return [dict(zip(columns, r)) for r in wh.execute(sql, params)]


def _replace(wh: WarehouseClient, table: str, columns: tuple[str, ...], as_of: date, records: list[tuple]) -> None:
    """Replace one date's rows. The caller holds the transaction."""
    wh.execute(f"DELETE FROM {table} WHERE as_of_date = ?", (as_of,))
    if records:
        row = "(" + ", ".join(["?"] * len(columns)) + ")"
        wh.execute(f"INSERT INTO {table} ({', '.join(columns)}) VALUES {', '.join([row] * len(records))}",
                   [v for r in records for v in r])


# --- load ----------------------------------------------------------------------

def load_new_messages(wh: WarehouseClient, settings: Settings, *, group_id: str = DAILY_CONSUMER_GROUP,
                      log: Callable[[str], None] = print) -> dict[str, int]:
    """Read whatever is new on the source topics up to the current end of each partition, then return."""
    from recon.loader import run_loader

    stats = run_loader(wh, bootstrap=settings.kafka_bootstrap, group_id=group_id,
                       topic_prefix=settings.topic_prefix, fx_rates_path=settings.fx_rates_path,
                       stop_at_eof=True, log=log)
    return {"received": stats.received, "rows_inserted": stats.rows_inserted, "rejected": stats.rejected,
            "dead_lettered": stats.dead_lettered}


# --- snapshot (right after dbt build, in the same task) -----------------------------

SNAPSHOT_COLUMNS = ("as_of_date", "kind", "item_id", "charge_id", "related_charge_ids", "reason_code", "amount_minor",
                    "age_business_days", "settlement_deadline", "business_days_left")


def marts_as_of(wh: WarehouseClient) -> date | None:
    return wh.execute("SELECT max(as_of_date) FROM marts.recon_exceptions")[0][0]


def snapshot_marts(wh: WarehouseClient, as_of: date) -> dict[str, int]:
    built = marts_as_of(wh)
    if built != as_of:
        raise RuntimeError(f"the marts were built for as-of {built}, not {as_of}")
    ensure_tables(wh)
    pending = wh.execute("SELECT charge_id, settlement_deadline, business_days_left FROM marts.recon_pending")
    exceptions = wh.execute("SELECT exception_id, charge_id, related_charge_ids, reason_code, amount_minor, "
                            "age_business_days FROM marts.recon_exceptions")
    records = [(as_of, "build", as_of.isoformat(), None, None, None, None, None, None, None)]
    records += [(as_of, "pending", c, c, None, None, None, None, deadline, left) for c, deadline, left in pending]
    records += [(as_of, "exception", eid, c, related, code, amount, age, None, None)
                for eid, c, related, code, amount, age in exceptions]
    with wh.transaction():
        _replace(wh, SNAPSHOT_TABLE, SNAPSHOT_COLUMNS, as_of, records)
    return {"pending": len(pending), "exceptions": len(exceptions)}


def _snapshot(wh: WarehouseClient, as_of: date, kind: str) -> list[dict[str, Any]]:
    return _rows(wh, f"SELECT {', '.join(SNAPSHOT_COLUMNS)} FROM {SNAPSHOT_TABLE} WHERE as_of_date = ? AND kind = ? "
                     f"ORDER BY item_id", SNAPSHOT_COLUMNS, (as_of, kind))


def _require_snapshot(wh: WarehouseClient, as_of: date) -> None:
    ensure_tables(wh)
    if not _snapshot(wh, as_of, "build"):
        raise RuntimeError(f"no snapshot for as-of {as_of}: run dbt_build for this date first")


# --- re-check pending settlements ------------------------------------------------

def recheck_pending(wh: WarehouseClient, as_of: date) -> dict[str, Any]:
    """Record what became of every charge that was pending on the previous as-of date, and what is pending now.

    A charge pending last time and an exception now is a settlement window that closed without the money
    arriving. Pending last time and gone now means it settled (and matched). Reads snapshots only.
    """
    _require_snapshot(wh, as_of)
    previous = wh.execute(f"SELECT max(as_of_date) FROM {SNAPSHOT_TABLE} WHERE kind = 'build' AND as_of_date < ?",
                          (as_of,))[0][0]
    pending = _snapshot(wh, as_of, "pending")
    was_pending = {} if previous is None else {r["item_id"]: r for r in _snapshot(wh, previous, "pending")}
    exceptions = _snapshot(wh, as_of, "exception")
    exception_for: dict[str, dict[str, Any]] = {}
    for e in exceptions:
        # A charge can surface on its own row, or inside an ambiguous bank line's related_charge_ids.
        for charge in ([e["charge_id"]] if e["charge_id"] else (e["related_charge_ids"] or "").split(",")):
            if charge:
                exception_for.setdefault(charge, e)

    pending_ids = {p["item_id"] for p in pending}
    records = [(as_of, p["item_id"], "pending", None, None, p["settlement_deadline"], p["business_days_left"],
                previous) for p in pending]
    newly_closed, settled = [], []
    for charge_id, before in sorted(was_pending.items()):
        if charge_id in pending_ids:
            continue
        e = exception_for.get(charge_id)
        if e is not None:
            newly_closed.append({"charge_id": charge_id, "settlement_deadline": before["settlement_deadline"],
                                 "reason_code": e["reason_code"], "exception_id": e["item_id"]})
            records.append((as_of, charge_id, "exception", e["reason_code"], e["item_id"],
                            before["settlement_deadline"], None, previous))
        else:
            settled.append({"charge_id": charge_id, "settlement_deadline": before["settlement_deadline"]})
            records.append((as_of, charge_id, "settled", None, None, before["settlement_deadline"], None, previous))

    with wh.transaction():
        _replace(wh, RUNS_TABLE, ("as_of_date", "previous_as_of_date", "pending", "newly_closed", "settled", "backlog"),
                 as_of, [(as_of, previous, len(pending), len(newly_closed), len(settled), len(exceptions))])
        _replace(wh, HISTORY_TABLE, ("as_of_date", "charge_id", "status", "reason_code", "exception_id",
                                     "settlement_deadline", "business_days_left", "previous_as_of_date"),
                 as_of, records)
    return {"as_of": as_of.isoformat(), "previous_as_of": previous.isoformat() if previous else None,
            "pending": [{"charge_id": p["item_id"], "settlement_deadline": p["settlement_deadline"].isoformat(),
                         "business_days_left": p["business_days_left"]} for p in pending],
            "newly_closed": [c | {"settlement_deadline": c["settlement_deadline"].isoformat()} for c in newly_closed],
            "settled": [c | {"settlement_deadline": c["settlement_deadline"].isoformat()} for c in settled]}


# --- summary --------------------------------------------------------------------

def summarize(wh: WarehouseClient, as_of: date, recheck: dict[str, Any]) -> dict[str, Any]:
    """Backlog and newly closed windows for one as-of date, from its snapshot. Read-only."""
    _require_snapshot(wh, as_of)
    by_reason: dict[str, dict[str, int]] = {}
    exceptions = _snapshot(wh, as_of, "exception")
    for e in exceptions:
        v = by_reason.setdefault(e["reason_code"], {"count": 0, "amount_minor": 0})
        v["count"] += 1
        v["amount_minor"] += int(e["amount_minor"])
    return {"as_of": as_of.isoformat(), "backlog": len(exceptions),
            "backlog_amount_minor": sum(v["amount_minor"] for v in by_reason.values()),
            "oldest_age_business_days": max((e["age_business_days"] for e in exceptions), default=0),
            "by_reason_code": dict(sorted(by_reason.items())),
            "pending": len(recheck["pending"]), "newly_closed": recheck["newly_closed"], "settled": recheck["settled"]}


def summary_lines(s: dict[str, Any]) -> list[str]:
    lines = [f"as-of {s['as_of']}: backlog {s['backlog']} exceptions, {s['backlog_amount_minor'] / 100:,.2f} USD at "
             f"stake, oldest {s['oldest_age_business_days']} business days; {s['pending']} still pending in window",
             *[f"  {code}: {v['count']} ({v['amount_minor'] / 100:,.2f} USD)" for code, v in s["by_reason_code"].items()]]
    for c in s["newly_closed"]:
        lines.append(f"  window closed: {c['charge_id']} (deadline {c['settlement_deadline']}) is now "
                     f"{c['exception_id']} [{c['reason_code']}]")
    for c in s["settled"]:
        lines.append(f"  settled in window: {c['charge_id']} (deadline {c['settlement_deadline']})")
    if not s["newly_closed"] and not s["settled"]:
        lines.append("  no settlement windows closed since the previous run")
    return lines


# --- backfill report (python -m recon.orchestration report) ---------------------------

def backfill_report(wh: WarehouseClient, start: date, end: date) -> str:
    """Markdown: per as-of date, what was pending and which windows closed, from the tables the DAG wrote."""
    runs = wh.execute(f"SELECT as_of_date, previous_as_of_date, pending, newly_closed, settled, backlog FROM {RUNS_TABLE} "
                      f"WHERE as_of_date BETWEEN ? AND ? ORDER BY as_of_date", (start, end))
    history = _rows(wh, f"SELECT as_of_date, charge_id, status, reason_code, exception_id, settlement_deadline, "
                        f"business_days_left FROM {HISTORY_TABLE} WHERE as_of_date BETWEEN ? AND ? "
                        f"ORDER BY as_of_date, charge_id",
                    ("as_of_date", "charge_id", "status", "reason_code", "exception_id", "settlement_deadline",
                     "business_days_left"), (start, end))
    lines = ["| as-of (logical date) | day | pending | windows closed | backlog | what changed |",
             "|---|---|---|---|---|---|"]
    for as_of, _previous, n_pending, n_closed, _n_settled, backlog in runs:
        changes = [f"`{h['charge_id']}` (deadline {h['settlement_deadline']}) -> `{h['exception_id']}` {h['reason_code']}"
                   for h in history if h["as_of_date"] == as_of and h["status"] == "exception"]
        changes += [f"`{h['charge_id']}` settled" for h in history if h["as_of_date"] == as_of and h["status"] == "settled"]
        lines.append(f"| {as_of} | {as_of:%a} | {n_pending} | {n_closed} | {backlog} | {'; '.join(changes) or 'none'} |")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> None:
    import argparse

    from dotenv import load_dotenv

    from recon.config import load_settings
    from recon.warehouse.snowflake_client import SnowflakeClient

    load_dotenv(".env")
    parser = argparse.ArgumentParser(prog="python -m recon.orchestration")
    sub = parser.add_subparsers(dest="command", required=True)
    r = sub.add_parser("report", help="what the recon_daily runs recorded on Snowflake, per as-of date")
    r.add_argument("--from", dest="start", type=date.fromisoformat, required=True)
    r.add_argument("--to", dest="end", type=date.fromisoformat, required=True)
    args = parser.parse_args(argv)
    sf = load_settings().snowflake
    wh = SnowflakeClient(sf, sf.loader)
    try:
        print(backfill_report(wh, args.start, args.end))
    finally:
        wh.close()


# --- nightly evals (a private DuckDB copy, never the host's file) -------------------

class _DiscardDLQ:
    """The eval warehouse is built from the generator directly, without Kafka; rejects are still logged in raw.dlq_log."""

    def publish(self, record: DLQRecord) -> None:
        pass

    def flush(self) -> None:
        pass


def build_eval_warehouse(work: Path, *, seed: int, as_of: date) -> dict[str, str]:
    """Generate the scenario into `work`: a fresh DuckDB warehouse built by dbt, plus the ground-truth file."""
    from recon.generator import build_scenario
    from recon.generator.ground_truth import write_ground_truth
    from recon.loader.schema import ensure_schema
    from recon.loader.sink import RawMessage, Sink, load_fx_rates
    from recon.parity import dbt_build
    from recon.warehouse import DuckDBClient

    work.mkdir(parents=True, exist_ok=True)
    scenario = build_scenario(seed=seed, as_of=as_of)
    paths = {"DUCKDB_PATH": str(work / "warehouse.duckdb"), "GROUND_TRUTH_PATH": str(work / "ground_truth.duckdb"),
             "COPILOT_DB_PATH": str(work / "copilot.duckdb")}
    for p in paths.values():
        Path(p).unlink(missing_ok=True)
    write_ground_truth(Path(paths["GROUND_TRUTH_PATH"]), scenario)
    wh = DuckDBClient(paths["DUCKDB_PATH"])
    try:
        ensure_schema(wh)
        load_fx_rates(wh, scenario.fx_rates)
        offsets: dict[tuple[str, int], int] = {}
        raws = []
        for m in scenario.messages:
            partition = sum(m.key or b"") % 3
            offset = offsets.get((m.topic, partition), 0)
            offsets[(m.topic, partition)] = offset + 1
            raws.append(RawMessage(m.topic, partition, offset, m.key, m.value))
        Sink(wh, _DiscardDLQ()).process(raws)
    finally:
        wh.close()
    dbt_build("duckdb", scenario.as_of, work / "dbt", {"DUCKDB_PATH": paths["DUCKDB_PATH"]})
    return paths


if __name__ == "__main__":
    main()
