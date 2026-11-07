"""Scheduled triage (Slice 9d), in fake mode on DuckDB: no API calls. The agent reads through a real MCP server."""

import json
import shutil
import sys
from datetime import date
from pathlib import Path

import duckdb
import pytest

import recon.triage as triage
from recon.config import load_settings
from recon.copilot.mcp_backend import McpBackend
from recon.copilot.store import CopilotStore
from recon.orchestration import snapshot_marts
from recon.warehouse import DuckDBClient

DAY, NEXT = date(2026, 10, 15), date(2026, 10, 16)


@pytest.fixture
def env(recon_db, tmp_path):
    """A writable copy of the seed-42 warehouse with the 15 Oct snapshot, and a fresh copilot store."""
    warehouse = tmp_path / "warehouse.duckdb"
    shutil.copy(recon_db[0], warehouse)
    wh = DuckDBClient(warehouse)
    snapshot_marts(wh, DAY)
    wh.close()

    def settings(**extra):
        return load_settings({"DUCKDB_PATH": str(warehouse), "GROUND_TRUTH_PATH": str(recon_db[1]),
                              "COPILOT_DB_PATH": str(tmp_path / "copilot.duckdb"),
                              "MCP_ACCESS_LOG": str(tmp_path / "mcp_access.jsonl"), "TRIAGE_MODEL_MODE": "fake"}
                             | {k: str(v) for k, v in extra.items()})
    return settings


def run(settings, *days, run_id="dag_run_1", **kw):
    return triage.run(list(days) or [DAY], dag_run_id=run_id, settings=settings, log=lambda line: None, **kw)


def events(settings):
    con = duckdb.connect(str(settings.copilot_db_path), read_only=True)
    try:
        return [(t, json.loads(p)) for t, p in con.execute(
            "SELECT event_type, payload FROM audit_log ORDER BY audit_id").fetchall()]
    finally:
        con.close()


def test_triages_up_to_the_cap_and_leaves_the_rest_for_the_next_run(env):
    s = env(TRIAGE_MAX_EXCEPTIONS=10)
    first = run(s)[0]
    assert (first["exceptions"], first["triaged"], first["skipped"], first["left_over"]) == (30, 10, 0, 20)
    assert first["stopped_by"] == "TRIAGE_MAX_EXCEPTIONS=10" and first["cost_usd"] == 0.0
    second = run(s, run_id="dag_run_2")[0]
    assert (second["triaged"], second["skipped"], second["left_over"]) == (10, 10, 10)
    assert {p["exception_id"] for p in first["proposals"]}.isdisjoint(p["exception_id"] for p in second["proposals"])


def test_re_running_a_date_creates_no_proposals(env):
    s = env(TRIAGE_MAX_EXCEPTIONS=50)
    first = run(s)[0]
    assert first["triaged"] == 30 and first["fallbacks"] == 0 and first["left_over"] == 0
    before = [e for e in events(s) if e[0] == "diagnosis"]
    again = run(s, run_id="dag_run_2")[0]
    assert (again["triaged"], again["skipped"]) == (0, 30)
    assert [e for e in events(s) if e[0] == "diagnosis"] == before


def test_triage_never_approves_or_applies_anything(env):
    s = env(TRIAGE_MAX_EXCEPTIONS=50)
    run(s, run_id="scheduled__2026-10-15")
    types = [t for t, _ in events(s)]
    assert "review" not in types and "resolution_applied" not in types
    store = CopilotStore(s.copilot_db_path)
    try:
        assert store.resolutions() == []
        con = duckdb.connect(str(s.duckdb_path), read_only=True)
        ids = [r[0] for r in con.execute("SELECT exception_id FROM marts.recon_exceptions").fetchall()]
        con.close()
        proposals = [store.latest_proposal(eid) for eid in ids]
    finally:
        store.close()
    assert all(p.status == "proposed" and p.proposed_by == "scheduled_triage"
               and p.dag_run_id == "scheduled__2026-10-15" for p in proposals)
    # And nothing in the module could: it never calls a review method.
    source = Path(triage.__file__).read_text()
    assert not any(f".{verb}(" in source for verb in ("approve", "reject", "resolve"))


def add_next_day(settings, *, new_id: str, changed: str | None = None):
    """A 16 Oct snapshot: the same exceptions (one with a second problem detected), plus one new exception."""
    con = duckdb.connect(str(settings.duckdb_path))
    rows = [json.loads(r) for (r,) in con.execute(
        "SELECT row_json FROM marts.recon_exceptions_daily WHERE as_of_date = ?", (DAY,)).fetchall()]
    for r in rows:
        r["as_of_date"], r["age_business_days"] = NEXT.isoformat(), r["age_business_days"] + 1
        if r["exception_id"] == changed:
            r["detected_problems"] += ",fee_mismatch"
    template = next(r for r in rows if r["item_type"] == "charge")
    rows.append(template | {"exception_id": new_id, "reason_code": "missing_settlement",
                            "detected_problems": "missing_settlement", "age_business_days": 0})
    con.executemany("INSERT INTO marts.recon_exceptions_daily (as_of_date, exception_id, row_json) VALUES (?, ?, ?)",
                    [(NEXT, r["exception_id"], json.dumps(r)) for r in rows])
    con.execute("INSERT INTO marts.recon_daily_snapshot (as_of_date, kind, item_id) VALUES (?, 'build', ?)",
                (NEXT, NEXT.isoformat()))
    con.close()


def test_a_changed_or_new_exception_is_triaged_again_and_new_ones_go_first(env):
    s = env(TRIAGE_MAX_EXCEPTIONS=50)
    run(s)
    changed = run(s)[0]["proposals"] or None  # nothing left on 15 Oct
    assert changed is None
    con = duckdb.connect(str(s.duckdb_path), read_only=True)
    target = con.execute("SELECT exception_id FROM marts.recon_exceptions ORDER BY exception_id LIMIT 1").fetchone()[0]
    con.close()
    new_id = "charge:ch_ffffffffffffffff"
    add_next_day(s, changed=target, new_id=new_id)
    nxt = run(s, NEXT, run_id="dag_run_16")[0]
    assert (nxt["exceptions"], nxt["triaged"], nxt["skipped"]) == (31, 2, 29)  # the aged rows hash the same
    assert [p["exception_id"] for p in nxt["proposals"]] == [new_id, target]  # new since 15 Oct first
    assert triage.ordered_exceptions(DuckDBClient(s.duckdb_path, read_only=True), NEXT)[0] == new_id


def test_the_cost_cap_stops_the_run(env, monkeypatch):
    monkeypatch.setattr(triage, "run_cost", lambda run, live: 0.02)
    first = run(env(TRIAGE_MAX_EXCEPTIONS=50, TRIAGE_MAX_COST_USD=0.05))[0]
    assert first["triaged"] == 3 and first["left_over"] == 27 and first["stopped_by"] == "TRIAGE_MAX_COST_USD=0.05"
    assert first["cost_usd"] == pytest.approx(0.06)


def test_both_audit_trails_are_written(env):
    s = env(TRIAGE_MAX_EXCEPTIONS=2)
    result = run(s)[0]
    kinds = [t for t, _ in events(s)]
    assert kinds.count("context_fetch") == 2 and kinds.count("diagnosis") == 2 and "tool_call" in kinds
    diagnosis = next(p for t, p in events(s) if t == "diagnosis")
    assert diagnosis["proposed_by"] == "scheduled_triage" and diagnosis["content_hash"]
    access = [json.loads(line) for line in s.mcp_access_log.read_text().splitlines()]
    # The server saw the orchestrator's row reads (all 30, to check for changes) and the model's calls.
    assert sum(1 for a in access if a["tool"] == "get_exception") >= 30 and len(access) > 30
    assert result["triaged"] == 2


def test_a_dead_server_is_a_fallback_and_is_retried_next_run(env):
    s = env(TRIAGE_MAX_EXCEPTIONS=3)
    reader = DuckDBClient(s.duckdb_path, read_only=True)
    store = CopilotStore(s.copilot_db_path)
    try:
        dead = McpBackend(s, command=[sys.executable, "-c", "import sys; sys.exit(1)"])
        down = triage.triage_date(DAY, settings=s, reader=reader, store=store, client=triage.make_client(s),
                                  dag_run_id="dag_run_1", backend=dead, log=lambda line: None)
    finally:
        store.close()
        reader.close()
    assert down["triaged"] == down["fallbacks"] == down["escalations"] == 3
    retried = run(s, run_id="dag_run_2")[0]
    assert retried["skipped"] == 0 and retried["triaged"] == 3 and retried["fallbacks"] == 0


def test_the_default_mode_is_fake_and_unknown_modes_are_refused():
    assert load_settings({}).triage_model_mode == "fake"
    assert triage.make_client(load_settings({})).model == "scripted-investigator"
    with pytest.raises(ValueError, match="fake or live"):
        triage.make_client(load_settings({"TRIAGE_MODEL_MODE": "maybe"}))


def test_the_cap_is_per_run_across_all_its_dates(env):
    s = env(TRIAGE_MAX_EXCEPTIONS=12)
    add_next_day(s, new_id="charge:ch_ffffffffffffffff")
    first, second = run(s, DAY, NEXT, catch_up=False)
    assert (first["triaged"], second["triaged"]) == (12, 0)  # one budget for the run, not one per date
    assert second["stopped_by"] == "TRIAGE_MAX_EXCEPTIONS=12" and second["left_over"] == 31 - 12


def test_a_date_whose_event_was_lost_is_caught_up_and_finished_dates_are_per_model(env):
    s = env(TRIAGE_MAX_EXCEPTIONS=50)
    run(s)  # finishes 15 Oct for the scripted model
    add_next_day(s, new_id="charge:ch_ffffffffffffffff")
    # A run given no dates at all (its asset event was dropped) still triages 16 Oct.
    [caught] = triage.run([], dag_run_id="manual_catch_up", settings=s, log=lambda line: None)
    assert caught["as_of"] == NEXT.isoformat() and caught["triaged"] == 1 and caught["skipped"] == 30
    assert triage.run([], dag_run_id="again", settings=s, log=lambda line: None) == []  # nothing unfinished
    con = duckdb.connect(str(s.copilot_db_path), read_only=True)
    finished = con.execute("SELECT as_of_date, model, dag_run_id FROM triage_dates ORDER BY as_of_date").fetchall()
    con.close()
    assert finished == [(DAY, "scripted-investigator", "dag_run_1"), (NEXT, "scripted-investigator", "manual_catch_up")]


def test_a_date_with_exceptions_left_over_is_not_finished(env):
    s = env(TRIAGE_MAX_EXCEPTIONS=5)
    run(s)
    reader, store = triage.open_reader_and_store(s)
    try:
        assert triage.unfinished_dates(reader, store, "scripted-investigator") == {DAY}
        assert triage.unfinished_dates(reader, store, "claude-sonnet-5-5") == {DAY}
    finally:
        store.close()
        reader.close()
