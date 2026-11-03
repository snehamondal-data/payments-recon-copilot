"""The recon_daily task logic, on DuckDB: as the as-of date moves forward, pending settlements whose T+2
window has closed become exceptions, exactly once, on the right day. Re-running a date changes nothing,
even after later dates have been built.

This test may read ground truth (to know which charges are the planted late settlements).
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from helpers import build_recon_warehouse
from recon.orchestration import (
    HISTORY_TABLE,
    backfill_report,
    RUNS_TABLE,
    recheck_pending,
    snapshot_marts,
    summarize,
    summary_lines,
)
from recon.parity import dbt_build
from recon.taxonomy import ExceptionType as T
from recon.warehouse import DuckDBClient

DAYS = [date(2026, 10, 15) + timedelta(days=i) for i in range(6)]  # Thu 15 .. Tue 20, weekend included


@pytest.fixture(scope="module")
def backfill(scenario, tmp_path_factory):
    tmp = tmp_path_factory.mktemp("backfill")
    path, _, _ = build_recon_warehouse(scenario, tmp)
    runs = {}
    for day in DAYS:
        dbt_build("duckdb", day, tmp / f"dbt-{day}", {"DUCKDB_PATH": str(path)})
        wh = DuckDBClient(path)
        try:
            snapshot_marts(wh, day)
            recheck = recheck_pending(wh, day)
            again = recheck_pending(wh, day)  # a re-run of the same logical date
            rows = wh.execute(f"SELECT count(*) FROM {HISTORY_TABLE} WHERE as_of_date = ?", (day,))[0][0]
            run_rows = wh.execute(f"SELECT count(*) FROM {RUNS_TABLE} WHERE as_of_date = ?", (day,))[0][0]
            runs[day] = {"recheck": recheck, "again": again, "rows": rows, "run_rows": run_rows,
                         "summary": summarize(wh, day, recheck)}
        finally:
            wh.close()
    return path, runs


def late_settlements(scenario):
    return [c for c in scenario.cases if c.exception_type == T.LATE_SETTLEMENT]


def test_each_late_settlement_flips_once_on_the_day_after_its_deadline(scenario, backfill):
    _, runs = backfill
    flips = {c["charge_id"]: day for day, r in runs.items() for c in r["recheck"]["newly_closed"]}
    late = late_settlements(scenario)
    assert len(late) == 3
    for case in late:
        [charge] = case.charge_ids
        deadline = next(date.fromisoformat(p["settlement_deadline"]) for p in runs[DAYS[0]]["recheck"]["pending"]
                        if p["charge_id"] == charge)
        assert flips[charge] == min(d for d in DAYS if d > deadline), (charge, deadline)
    assert set(flips) == {c.charge_ids[0] for c in late}


def test_flipped_items_are_missing_settlement_exceptions(backfill):
    _, runs = backfill
    closed = [c for r in runs.values() for c in r["recheck"]["newly_closed"]]
    assert closed and all(c["reason_code"] == "missing_settlement" for c in closed)
    assert all(c["exception_id"] == f"charge:{c['charge_id']}" for c in closed)


def test_pending_shrinks_and_backlog_grows_by_the_flips(backfill):
    _, runs = backfill
    first = runs[DAYS[0]]
    assert first["recheck"]["previous_as_of"] is None and first["recheck"]["newly_closed"] == []
    assert len(first["recheck"]["pending"]) == 3 and first["summary"]["backlog"] == 30
    for before, after in zip(DAYS, DAYS[1:]):
        flipped = len(runs[after]["recheck"]["newly_closed"])
        assert runs[after]["recheck"]["previous_as_of"] == before.isoformat()
        assert runs[after]["summary"]["backlog"] == runs[before]["summary"]["backlog"] + flipped
        assert runs[after]["summary"]["pending"] == runs[before]["summary"]["pending"] - flipped
    assert runs[DAYS[-1]]["summary"]["pending"] == 0


def test_rerunning_a_date_is_idempotent(backfill):
    _, runs = backfill
    for r in runs.values():
        assert r["again"] == r["recheck"]
        assert r["run_rows"] == 1
        assert r["rows"] == len(r["recheck"]["pending"]) + len(r["recheck"]["newly_closed"]) + len(r["recheck"]["settled"])


def test_rerunning_old_dates_after_later_ones_were_built_gives_the_same_answer(backfill):
    """The marts now hold 2026-10-20; re-running recheck and summary for every earlier date still works."""
    path, runs = backfill
    wh = DuckDBClient(path)
    try:
        for day in reversed(DAYS):
            again = recheck_pending(wh, day)
            assert again == runs[day]["recheck"]
            assert summarize(wh, day, again) == runs[day]["summary"]
    finally:
        wh.close()


def test_snapshot_refuses_marts_built_for_another_date_and_recheck_needs_a_snapshot(backfill):
    path, _ = backfill
    wh = DuckDBClient(path)
    try:
        with pytest.raises(RuntimeError, match="built for as-of 2026-10-20"):
            snapshot_marts(wh, date(2026, 10, 16))
        with pytest.raises(RuntimeError, match="no snapshot for as-of 2026-10-21"):
            recheck_pending(wh, date(2026, 10, 21))
    finally:
        wh.close()


def test_summary_lines_name_the_closed_windows(backfill):
    _, runs = backfill
    text = "\n".join(line for r in runs.values() for line in summary_lines(r["summary"]))
    assert text.count("window closed:") == 3 and "no settlement windows closed" in text


def test_backfill_report_lists_each_day_and_each_closed_window(backfill):
    path, _ = backfill
    wh = DuckDBClient(path)
    try:
        report = backfill_report(wh, DAYS[0], DAYS[-1])
    finally:
        wh.close()
    assert report.count("\n") == len(DAYS) + 1 and report.count("missing_settlement") == 3
