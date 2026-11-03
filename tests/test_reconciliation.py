"""Runs the real dbt project on a freshly loaded warehouse and scores recon_exceptions against ground truth.

This test may read ground truth. The dbt models must not (see test_ground_truth_isolation.py).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections import Counter
from datetime import timedelta
from pathlib import Path

import duckdb
import pytest

from conftest import FakeDLQ, as_raw_messages, load_all
from recon.business_days import settlement_deadline
from recon.fx import read_fx_csv, write_fx_csv
from recon.generator.ground_truth import write_ground_truth
from recon.loader.schema import ensure_schema
from recon.loader.sink import Sink, load_fx_rates
from recon.taxonomy import ExceptionType as T
from recon.warehouse import DuckDBClient

DBT_DIR = Path(__file__).resolve().parents[1] / "dbt"
DBT = Path(sys.executable).parent / "dbt"

# What reconciliation should call each planted problem. None = must not appear at all.
EXPECTED_REASON = {t: t.value for t in T} | {
    T.LATE_SETTLEMENT: None,
    T.DATE_BOUNDARY: "date_mismatch",
}


@pytest.fixture(scope="module")
def built(scenario, tmp_path_factory):
    """Load the scenario (no Kafka needed), run `dbt build`, and hand back both DuckDB files."""
    tmp = tmp_path_factory.mktemp("recon")
    warehouse_path, ground_truth_path = tmp / "warehouse.duckdb", tmp / "ground_truth.duckdb"
    write_ground_truth(ground_truth_path, scenario)
    write_fx_csv(tmp / "fx.csv", scenario.fx_rates)
    wh = DuckDBClient(warehouse_path)
    ensure_schema(wh)
    load_fx_rates(wh, read_fx_csv(tmp / "fx.csv"))
    load_all(Sink(wh, FakeDLQ()), as_raw_messages(scenario))
    wh.close()

    # A subprocess, like `make recon`: dbt-duckdb keeps its connection open in-process otherwise.
    proc = subprocess.run([
        str(DBT), "build", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR),
        "--target-path", str(tmp / "target"), "--log-path", str(tmp / "logs"),
        "--vars", f"{{as_of: '{scenario.as_of}'}}",
    ], env=os.environ | {"DUCKDB_PATH": str(warehouse_path)}, capture_output=True, text=True)
    assert proc.returncode == 0, f"dbt build failed:\n{proc.stdout[-3000:]}"
    result = json.loads((tmp / "target" / "run_results.json").read_text())

    con = duckdb.connect(str(warehouse_path), read_only=True)
    gt = duckdb.connect(str(ground_truth_path), read_only=True)
    yield con, gt, result
    con.close()
    gt.close()


def rows(con, sql, params=()):
    cur = con.execute(sql, params)
    names = [d[0] for d in cur.description]
    return [dict(zip(names, r)) for r in cur.fetchall()]


@pytest.fixture(scope="module")
def exceptions(built):
    return rows(built[0], "SELECT * FROM marts.recon_exceptions")


@pytest.fixture(scope="module")
def planted(built):
    return rows(built[1], "SELECT * FROM planted_cases ORDER BY case_id")


def rows_for_case(case, exceptions):
    """Exception rows about any of the case's charges, or about one of its bank lines."""
    charges, records = set(case["charge_ids"]), set(case["record_ids"])
    return [e for e in exceptions if e["charge_id"] in charges or e["bank_line_id"] in records]


def test_dbt_build_ran_every_test(built):
    statuses = Counter(r["status"] for r in built[2]["results"])
    assert statuses["fail"] == statuses["error"] == 0
    assert statuses["pass"] >= 40


def test_every_planted_case_appears_with_the_right_reason_code(planted, exceptions):
    misses = []
    for case in planted:
        expected = EXPECTED_REASON[T(case["exception_type"])]
        if expected is None:
            continue
        found = rows_for_case(case, exceptions)
        if [e["reason_code"] for e in found] != [expected]:
            misses.append((case["case_id"], expected, [e["reason_code"] for e in found]))
    assert not misses, f"planted cases with the wrong or missing reason_code: {misses}"


def test_late_settlements_do_not_appear_and_are_pending(built, planted, exceptions):
    late = [c for c in planted if c["exception_type"] == T.LATE_SETTLEMENT]
    assert len(late) == 3
    pending = {r["charge_id"] for r in rows(built[0], "SELECT charge_id FROM marts.recon_pending")}
    for case in late:
        assert rows_for_case(case, exceptions) == [], case["case_id"]
        assert set(case["charge_ids"]) <= pending, case["case_id"]


def test_no_false_positives_every_exception_is_a_planted_case(planted, exceptions):
    claimed = set()
    for case in planted:
        claimed |= {e["exception_id"] for e in rows_for_case(case, exceptions)}
    unexplained = [e["exception_id"] for e in exceptions if e["exception_id"] not in claimed]
    assert unexplained == []


def test_baseline_traffic_all_matches(scenario, built):
    matched = {r["charge_id"] for r in rows(built[0], "SELECT charge_id FROM marts.recon_matches")}
    assert matched == set(scenario.baseline_charge_ids)


def test_ambiguous_deposit_lists_both_candidates(planted, exceptions):
    for case in planted:
        if case["exception_type"] == T.AMBIGUOUS and len(case["charge_ids"]) == 2:
            [row] = rows_for_case(case, exceptions)
            assert row["item_type"] == "bank_line"
            assert sorted(row["related_charge_ids"].split(",")) == sorted(case["charge_ids"])


def test_exception_amounts_and_ages(scenario, exceptions):
    by_reason = {}
    for e in exceptions:
        by_reason.setdefault(e["reason_code"], []).append(e)
    assert all(e["amount_minor"] == 0 for e in by_reason["date_mismatch"])
    assert all(1 <= e["amount_minor"] <= 2 for e in by_reason["currency_rounding"])
    assert all(25 <= e["amount_minor"] <= 75 for e in by_reason["fee_mismatch"])
    for e in exceptions:
        assert e["age_business_days"] >= 0
        assert e["as_of_date"] == scenario.as_of


def test_sql_deadline_mirrors_python_for_every_calendar_day(built):
    pairs = built[0].execute("""
        SELECT d.date_day, deadline.date_day
        FROM intermediate.int_business_calendar AS d
        JOIN intermediate.int_business_calendar AS deadline
          ON deadline.is_business_day AND deadline.business_day_index = d.business_day_index + 2
    """).fetchall()
    assert len(pairs) >= 990
    assert [(d, settlement_deadline(d)) for d, _ in pairs] == pairs


def test_sql_age_mirrors_python(built, scenario):
    for charge_date, age in built[0].execute(
            "SELECT charge_date_utc, age_business_days FROM intermediate.int_charge_recon").fetchall():
        day, expected = charge_date, 0
        while day < scenario.as_of:
            day += timedelta(days=1)
            expected += day.weekday() < 5
        assert age == expected


def test_no_duplicate_column_names_in_any_model(built):
    # DuckDB silently renames a duplicate column to name_1; Snowflake rejects it. Catch it here.
    renamed = built[0].execute(r"""
        SELECT table_schema || '.' || table_name || '.' || column_name
        FROM information_schema.columns
        WHERE table_schema IN ('staging', 'intermediate', 'marts')
          AND regexp_matches(column_name, '_[0-9]+$')
    """).fetchall()
    assert renamed == []
