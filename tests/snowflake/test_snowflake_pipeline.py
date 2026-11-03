"""Replay idempotency and the reconciliation pytest, on Snowflake. Same assertions as on DuckDB.

This test may read ground truth (a local DuckDB file). Nothing on Snowflake ever does.
"""

from __future__ import annotations

import duckdb
import pytest

# The reconciliation tests themselves, unchanged; only the `built` fixture below differs.
from test_reconciliation import (  # noqa: F401
    exceptions,
    planted,
    test_ambiguous_deposit_lists_both_candidates,
    test_baseline_traffic_all_matches,
    test_dbt_build_ran_every_test,
    test_every_planted_case_appears_with_the_right_reason_code,
    test_exception_amounts_and_ages,
    test_late_settlements_do_not_appear_and_are_pending,
    test_no_false_positives_every_exception_is_a_planted_case,
    test_sql_age_mirrors_python,
    test_sql_deadline_mirrors_python_for_every_calendar_day,
)
# Not imported: test_no_duplicate_column_names_in_any_model guards against DuckDB silently renaming a
# duplicate column; Snowflake rejects one at build time, so the dbt build itself is that check here.

from recon.warehouse.snowflake_client import connect, to_pyformat

pytestmark = pytest.mark.snowflake


def test_replay_from_the_earliest_offset_inserts_zero_new_rows(sf_pipeline, scenario):
    first, replay = sf_pipeline["first"], sf_pipeline["replay"]
    expected = scenario.expected_counts() | {"dlq_log": len(scenario.poison), "fx_rates": len(scenario.fx_rates)}
    assert first.received == len(scenario.messages)
    assert first.rejected == first.dead_lettered == len(scenario.poison)
    assert {t.split(".")[1]: n for t, n in sf_pipeline["after_first"].items()} == expected

    assert replay.received == len(scenario.messages)  # everything re-read from Kafka...
    assert replay.rows_inserted == 0  # ...and nothing re-inserted
    assert replay.dead_lettered == 0
    assert sf_pipeline["after_replay"] == sf_pipeline["after_first"]


class _Result:
    """Just enough of a DuckDB result for the reconciliation tests: lower-case column names, fetchall()."""

    def __init__(self, cur):
        self.description = [(d[0].lower(), *d[1:]) for d in cur.description]
        self._rows = cur.fetchall()

    def fetchall(self):
        return self._rows


class _SnowflakeCon:
    def __init__(self, con):
        self.con = con

    def execute(self, sql, params=()):
        cur = self.con.cursor()
        if params:
            cur.execute(to_pyformat(sql), list(params))
        else:
            cur.execute(sql)
        return _Result(cur)


@pytest.fixture(scope="module")
def built(sf, sf_pipeline):
    # The loader role: the SQL-mirror tests read INTERMEDIATE, which the agent role cannot see.
    con = connect(sf, sf.loader)
    gt = duckdb.connect(str(sf_pipeline["ground_truth_path"]), read_only=True)
    yield _SnowflakeCon(con), gt, sf_pipeline["dbt"]
    con.close()
    gt.close()
