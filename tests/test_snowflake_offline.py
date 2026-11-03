"""The Snowflake code paths that don't need an account: generated SQL, settings, and the parity comparison."""

from __future__ import annotations

import pytest

from recon.config import SnowflakeLogin, SnowflakeSettings, load_settings
from recon.copilot.store import SNOWFLAKE_DDL
from recon.loader.schema import ddl
from recon.loader.sink import DLQ_LOG, INSERTS
from recon.parity import compare
from recon.warehouse.snowflake_client import to_pyformat


def test_snowflake_insert_is_a_merge_on_the_primary_key():
    sql = INSERTS["bank_settlement_lines"].statement(2, "snowflake")
    assert sql.startswith("MERGE INTO raw.bank_settlement_lines AS t USING (SELECT column1 AS event_id")
    assert "ON t.event_id = s.event_id AND t.line_no = s.line_no WHEN NOT MATCHED THEN INSERT" in sql
    assert sql.count("?") == 2 * len(INSERTS["bank_settlement_lines"].columns)
    assert "ON CONFLICT" not in sql


def test_snowflake_merge_casts_timestamps_and_quotes_reserved_words():
    assert "TO_TIMESTAMP_TZ(column10) AS occurred_at" in INSERTS["processor_events"].statement(1, "snowflake")
    dlq = DLQ_LOG.statement(1, "snowflake")
    assert 'ON t.topic = s.topic AND t."partition" = s."partition" AND t."offset" = s."offset"' in dlq


def test_duckdb_insert_is_unchanged():
    sql = INSERTS["processor_events"].statement(1, "duckdb")
    assert sql.endswith("ON CONFLICT DO NOTHING") and "CAST(? AS TIMESTAMPTZ)" in sql


def test_unknown_dialect_is_refused():
    with pytest.raises(ValueError):
        INSERTS["ledger_lines"].statement(1, "postgres")


def test_snowflake_ddl_uses_snowflake_types_and_creates_no_schemas():
    statements = ddl("snowflake")
    text = " ".join(statements)
    assert "TIMESTAMPTZ" not in text and "BLOB" not in text and "CREATE SCHEMA" not in text
    assert "TIMESTAMP_TZ" in text and "BINARY" in text
    assert len(statements) == len(ddl("duckdb")) - 2


def test_copilot_store_ddl_on_snowflake_keeps_audit_order():
    assert SNOWFLAKE_DDL[0].endswith(" ORDER")  # NOORDER is the new-account default; the log is read in id order
    assert "DEFAULT audit_id_seq.NEXTVAL" in SNOWFLAKE_DDL[1] and "VARIANT" in SNOWFLAKE_DDL[1]
    assert "JSON" not in " ".join(SNOWFLAKE_DDL)


def test_qmark_placeholders_become_pyformat():
    assert to_pyformat("SELECT ? WHERE a LIKE '5%' AND b = ?") == "SELECT %s WHERE a LIKE '5%%' AND b = %s"


def test_engine_must_be_known():
    with pytest.raises(ValueError):
        load_settings({"WAREHOUSE_ENGINE": "postgres"})
    assert load_settings({}).warehouse_engine == "duckdb"


def test_the_agent_and_the_loader_must_be_different_users_and_roles():
    loader = SnowflakeLogin("RECON_LOADER", "RECON_LOADER_ROLE", "a.p8")
    with pytest.raises(ValueError):
        SnowflakeSettings("acct", "wh", "db", loader=loader, agent=SnowflakeLogin("RECON_AGENT", "recon_loader_role", "b.p8"))
    with pytest.raises(ValueError):
        SnowflakeSettings("acct", "wh", "db", loader=loader, agent=SnowflakeLogin("recon_loader", "RECON_AGENT_ROLE", "b.p8"))


def test_evals_refuse_to_run_on_snowflake(monkeypatch, capsys):
    from recon.evals.__main__ import main

    monkeypatch.setenv("WAREHOUSE_ENGINE", "snowflake")
    assert main(["estimate"]) == 2
    assert "DuckDB only" in capsys.readouterr().out


def _side(**overrides):
    side = {"raw_counts": {"raw.ledger_lines": 3}, "recon_matches": {"ch_1"}, "recon_pending": set(),
            "exceptions": {"charge:ch_2": {"exception_id": "charge:ch_2", "reason_code": "fee_mismatch",
                                           "amount_minor": 40}},
            "tools": {"get_exception(...)": "{}"}}
    return side | overrides


def test_parity_comparison_is_empty_when_the_engines_agree():
    assert compare(_side(), _side()) == []


def test_parity_comparison_reports_every_kind_of_difference():
    snow = _side(raw_counts={"raw.ledger_lines": 4}, recon_matches=set(), tools={"get_exception(...)": "{ }"},
                 exceptions={"charge:ch_2": {"exception_id": "charge:ch_2", "reason_code": "fee_mismatch",
                                             "amount_minor": 41},
                             "charge:ch_3": {"exception_id": "charge:ch_3", "reason_code": "ambiguous",
                                             "amount_minor": 1}})
    problems = compare(_side(), snow)
    assert any(p.startswith("raw row counts differ") for p in problems)
    assert "only on snowflake: charge:ch_3" in problems
    assert "charge:ch_2.amount_minor: duckdb=40 snowflake=41" in problems
    assert any(p.startswith("recon_matches differs") for p in problems)
    assert "tool output differs: get_exception(...)" in problems
