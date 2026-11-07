"""python -m recon.parity: run the whole pipeline on DuckDB and on Snowflake, then compare the results.

For each engine, from the same freshly published Kafka topics: Kafka -> loader -> raw tables -> dbt build.
Then compare recon_exceptions row by row (same exception IDs, reason codes and amounts, and every other
column), recon_matches and recon_pending by charge ID, and the six agent tools' output for every exception
(read through the agent's own connection: RECON_AGENT_ROLE on Snowflake). No model is called.

Needs Kafka (make up) and the SNOWFLAKE_* settings in .env. Rebuilds RECON's RAW and REF tables from scratch.
Project-scale: one trial account, one XSMALL warehouse.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from recon.config import Settings, SnowflakeSettings, load_settings
from recon.copilot.tools import ReadOnlyWarehouse, ToolError, ToolSession
from recon.fx import write_fx_csv
from recon.generator import build_scenario
from recon.generator.publish import publish
from recon.loader import run_loader
from recon.loader.kafka_io import scratch_topics
from recon.loader.schema import SOURCE_TABLES
from recon.warehouse import DuckDBClient, WarehouseClient

ROOT = Path(__file__).resolve().parents[2]
DBT_DIR = ROOT / "dbt"
# The dbt next to this Python, or DBT_BIN (in the Airflow image dbt lives in its own venv).
DBT = Path(os.environ.get("DBT_BIN", Path(sys.executable).parent / "dbt"))
WORK_DIR = ROOT / "data" / "parity"
RESULTS = ROOT / "snowflake" / "parity_results.md"
RAW_TABLES = (*(f"raw.{t}" for t in SOURCE_TABLES), "raw.dlq_log", "ref.fx_rates")


def reset_snowflake(wh: WarehouseClient) -> None:
    """Drop the loader's RAW and REF tables (it owns them), so the run starts from an empty warehouse."""
    for table in RAW_TABLES:
        wh.execute(f"DROP TABLE IF EXISTS {table}")


def table_counts(wh: WarehouseClient) -> dict[str, int]:
    return {t: wh.execute(f"SELECT count(*) FROM {t}")[0][0] for t in RAW_TABLES}


def charge_ids(wh: ReadOnlyWarehouse, mart: str) -> set[str]:
    return {r["charge_id"] for r in wh._query(f"SELECT charge_id FROM marts.{mart}")}


def dbt_build(target: str, as_of: date, work: Path, env: dict[str, str] | None = None) -> dict[str, Any]:
    """`dbt build` in a subprocess (dbt-duckdb holds its connection in-process). Returns run_results.json."""
    proc = subprocess.run([
        str(DBT), "build", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR), "--target", target,
        "--target-path", str(work / "target"), "--log-path", str(work / "logs"), "--vars", f"{{as_of: '{as_of}'}}",
    ], cwd=ROOT, env=os.environ | (env or {}), capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"dbt build --target {target} failed:\n{proc.stdout[-3000:]}")
    return json.loads((work / "target" / "run_results.json").read_text())


def dbt_summary(results: dict[str, Any]) -> dict[str, int]:
    out: dict[str, int] = {}
    for r in results["results"]:
        out[r["status"]] = out.get(r["status"], 0) + 1
    return out


def tool_outputs(wh: ReadOnlyWarehouse, tools=None) -> dict[str, str]:
    """Every tool the agent could call about every exception, keyed by call. Values are the JSON the model sees.
    tools: a ToolBackend to call them through instead of the warehouse (the MCP parity, Slice 9c)."""
    out: dict[str, str] = {}
    session = ToolSession(wh if tools is None else tools)
    calls: list[tuple[str, dict[str, Any]]] = []
    fx_days: set[str] = set()
    for row in wh.queue():
        calls.append(("get_exception", {"exception_id": row["exception_id"]}))
        if row["charge_id"]:
            for tool in ("get_processor_events", "get_bank_settlement", "get_ledger_entries", "get_related_refunds"):
                calls.append((tool, {"txn_id": row["charge_id"]}))
        if row["bank_line_id"]:
            calls.append(("get_bank_settlement", {"txn_id": row["bank_line_id"]}))
        fx_days.add(row["item_date"])
    calls += [("get_fx_rate", {"date": day, "currency": c}) for day in sorted(fx_days) for c in ("EUR", "GBP")]
    for name, args in calls:
        key = f"{name}({json.dumps(args, sort_keys=True)})"
        try:
            out[key] = session.call(name, args)
        except ToolError as err:
            out[key] = f"ToolError: {err}"
    return out


def compare(duck: dict[str, Any], snow: dict[str, Any]) -> list[str]:
    """Every difference between the two engines, as readable lines. Empty means parity."""
    problems: list[str] = []
    if duck["raw_counts"] != snow["raw_counts"]:
        problems.append(f"raw row counts differ: duckdb={duck['raw_counts']} snowflake={snow['raw_counts']}")
    d_exc, s_exc = duck["exceptions"], snow["exceptions"]
    for eid in sorted(d_exc.keys() - s_exc.keys()):
        problems.append(f"only on duckdb: {eid}")
    for eid in sorted(s_exc.keys() - d_exc.keys()):
        problems.append(f"only on snowflake: {eid}")
    for eid in sorted(d_exc.keys() & s_exc.keys()):
        if d_exc[eid].keys() != s_exc[eid].keys():
            problems.append(f"{eid}: columns differ: {sorted(d_exc[eid].keys() ^ s_exc[eid].keys())}")
        for col in sorted(d_exc[eid].keys() & s_exc[eid].keys()):
            if d_exc[eid][col] != s_exc[eid][col]:
                problems.append(f"{eid}.{col}: duckdb={d_exc[eid][col]!r} snowflake={s_exc[eid][col]!r}")
    for mart in ("recon_matches", "recon_pending"):
        if duck[mart] != snow[mart]:
            problems.append(f"{mart} differs: {len(duck[mart] ^ snow[mart])} charge IDs on one engine only")
    for call in sorted(duck["tools"].keys() | snow["tools"].keys()):
        if duck["tools"].get(call) != snow["tools"].get(call):
            problems.append(f"tool output differs: {call}")
    return problems


def collect(engine: str, raw_counts: dict[str, int], agent_wh: ReadOnlyWarehouse) -> dict[str, Any]:
    """Everything is read through the agent's own connection, so this is exactly what the agent sees."""
    return {
        "engine": engine,
        "raw_counts": raw_counts,
        "exceptions": {r["exception_id"]: r for r in agent_wh.queue()},
        "recon_matches": charge_ids(agent_wh, "recon_matches"),
        "recon_pending": charge_ids(agent_wh, "recon_pending"),
        "tools": tool_outputs(agent_wh),
    }


def run_pipeline(settings: Settings, sf: SnowflakeSettings, *, seed: int, log: Callable[[str], None] = print
                 ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    scenario = build_scenario(seed=seed)
    shutil.rmtree(WORK_DIR, ignore_errors=True)
    WORK_DIR.mkdir(parents=True)
    fx_path = WORK_DIR / "fx_rates.csv"
    write_fx_csv(fx_path, scenario.fx_rates)
    timings: dict[str, Any] = {"seed": seed, "messages": len(scenario.messages)}

    with scratch_topics(settings.kafka_bootstrap, "parity") as prefix:
        sent = publish(scenario, settings.kafka_bootstrap, prefix)
        log(f"published {sent} messages to {prefix}*")
        kwargs = dict(bootstrap=settings.kafka_bootstrap, topic_prefix=prefix, fx_rates_path=fx_path,
                      log=lambda _: None)

        # DuckDB: a fresh file, so nothing from the dev warehouse leaks in.
        duck_path = WORK_DIR / "warehouse.duckdb"
        started = time.monotonic()
        wh = DuckDBClient(duck_path)
        try:
            stats = run_loader(wh, group_id=f"{prefix}duckdb", **kwargs)
            duck_counts = table_counts(wh)
        finally:
            wh.close()
        timings["duckdb_load_s"] = round(time.monotonic() - started, 1)
        log(f"duckdb: loaded {stats.rows_inserted} rows, {stats.dead_lettered} dead-lettered")
        started = time.monotonic()
        duck_dbt = dbt_build("duckdb", scenario.as_of, WORK_DIR / "duckdb", {"DUCKDB_PATH": str(duck_path)})
        timings["duckdb_dbt_s"] = round(time.monotonic() - started, 1)

        # Snowflake: the loader user, from empty RAW and REF tables.
        from recon.warehouse.snowflake_client import SnowflakeClient

        snow_wh = SnowflakeClient(sf, sf.loader)
        try:
            reset_snowflake(snow_wh)
            started = time.monotonic()
            stats = run_loader(snow_wh, group_id=f"{prefix}snowflake", **kwargs)
            timings["snowflake_load_s"] = round(time.monotonic() - started, 1)
            log(f"snowflake: loaded {stats.rows_inserted} rows, {stats.dead_lettered} dead-lettered")
            started = time.monotonic()
            snow_dbt = dbt_build("snowflake", scenario.as_of, WORK_DIR / "snowflake",
                                 {"SNOWFLAKE_LOADER_KEY_PATH": str(sf.loader.key_path.resolve())})
            timings["snowflake_dbt_s"] = round(time.monotonic() - started, 1)
            snow_counts = table_counts(snow_wh)
        finally:
            snow_wh.close()

    duck_agent = ReadOnlyWarehouse(duck_path)
    snow_agent = ReadOnlyWarehouse.snowflake(sf)
    try:
        duck = collect("duckdb", duck_counts, duck_agent) | {"dbt": dbt_summary(duck_dbt)}
        snow = collect("snowflake", snow_counts, snow_agent) | {"dbt": dbt_summary(snow_dbt)}
    finally:
        duck_agent.close()
        snow_agent.close()
    return duck, snow, timings


def report(duck: dict[str, Any], snow: dict[str, Any], timings: dict[str, Any], problems: list[str]) -> str:
    def by_reason(side):
        out: dict[str, tuple[int, int]] = {}
        for e in side["exceptions"].values():
            n, amount = out.get(e["reason_code"], (0, 0))
            out[e["reason_code"]] = (n + 1, amount + int(e["amount_minor"]))
        return out

    d_reason, s_reason = by_reason(duck), by_reason(snow)
    lines = [
        "# DuckDB vs Snowflake parity",
        "",
        f"Synthetic data, seed {timings['seed']}, {timings['messages']} Kafka messages. Both engines were loaded "
        "from the same freshly published topics by the same loader, then built by the same dbt project "
        "(no model changes). Snowflake is a project-scale trial account (one XSMALL warehouse). "
        "Generated by `python -m recon.parity`.",
        "",
        "| | DuckDB | Snowflake |",
        "|---|---|---|",
        *[f"| {t} rows | {duck['raw_counts'][t]} | {snow['raw_counts'][t]} |" for t in RAW_TABLES],
        f"| dbt build | {duck['dbt']} | {snow['dbt']} |",
        f"| recon_exceptions rows | {len(duck['exceptions'])} | {len(snow['exceptions'])} |",
        f"| recon_matches rows | {len(duck['recon_matches'])} | {len(snow['recon_matches'])} |",
        f"| recon_pending rows | {len(duck['recon_pending'])} | {len(snow['recon_pending'])} |",
        f"| agent tool calls compared | {len(duck['tools'])} | {len(snow['tools'])} |",
        f"| load time (s) | {timings['duckdb_load_s']} | {timings['snowflake_load_s']} |",
        f"| dbt build time (s) | {timings['duckdb_dbt_s']} | {timings['snowflake_dbt_s']} |",
        "",
        "recon_exceptions by reason code (count, total amount at stake in USD cents):",
        "",
        "| reason_code | DuckDB | Snowflake |",
        "|---|---|---|",
        *[f"| {code} | {d_reason.get(code, (0, 0))[0]}, {d_reason.get(code, (0, 0))[1]} | "
          f"{s_reason.get(code, (0, 0))[0]}, {s_reason.get(code, (0, 0))[1]} |"
          for code in sorted(d_reason.keys() | s_reason.keys())],
        "",
    ]
    if problems:
        lines += [f"**{len(problems)} differences:**", "", *[f"- {p}" for p in problems]]
    else:
        same = len(duck["exceptions"])
        lines.append(f"**Parity: no differences.** All {same} exception IDs, reason codes, amounts and every "
                     f"other recon_exceptions column match; recon_matches and recon_pending hold the same "
                     f"charge IDs; all {len(duck['tools'])} agent tool outputs are byte-identical.")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    load_dotenv(ROOT / ".env")
    settings = load_settings()
    parser = argparse.ArgumentParser(prog="python -m recon.parity", description=__doc__.splitlines()[0])
    parser.add_argument("--seed", type=int, default=settings.seed)
    parser.add_argument("--no-write", action="store_true", help=f"print only; don't write {RESULTS.name}")
    args = parser.parse_args(argv)

    duck, snow, timings = run_pipeline(settings, settings.snowflake, seed=args.seed)
    problems = compare(duck, snow)
    text = report(duck, snow, timings, problems)
    print(text)
    if not args.no_write:
        RESULTS.write_text(text)
        print(f"wrote {RESULTS.relative_to(ROOT)}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
