"""Each metric in the semantic layer matches a hand-written SQL check on the existing marts.

The SQL here deliberately doesn't read recon_items (the semantic model's table), so it checks that
model as well as the metric definitions.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError

from recon.copilot.tools import open_read_only
from recon.semantic.layer import METRICS, MetricQuery, SemanticLayer


@pytest.fixture(scope="module")
def layer(recon_db):
    warehouse = recon_db[0]
    s = SemanticLayer(warehouse.parent / "target" / "semantic_manifest.json", warehouse)
    yield s
    s.close()


@pytest.fixture(scope="module")
def sql(recon_db):
    con = open_read_only(recon_db[0])
    yield lambda q: con.execute(q).fetchall()
    con.close()


def q(layer, metric, dimensions=(), filters=()):
    rows = layer.query(MetricQuery(metric=metric, dimensions=list(dimensions), filters=list(filters)))["rows"]
    return {tuple(r[d] for d in dimensions): r["value"] for r in rows}


def usd(cents) -> str:
    return str((Decimal(int(cents)) / 100).quantize(Decimal("0.01")))


def nonzero(d):
    return {k: v for k, v in d.items() if v not in (0, "0.00")}


def test_catalog_is_the_five_metrics_each_described_in_plain_english(layer):
    assert set(layer.catalog) == set(METRICS)
    for m in layer.catalog.values():
        assert len(m.description) >= 60 and m.description[0].isupper(), m.name
    assert {m.name: m.unit for m in layer.catalog.values()} == {
        "exception_count": "count", "amount_at_risk": "USD", "exceptions_with_money_at_stake": "count", "pending_count": "count",
        "match_rate": "fraction"}


def test_exception_count(layer, sql):
    assert q(layer, "exception_count") == {(): sql("SELECT count(*) FROM marts.recon_exceptions")[0][0]} == {(): 30}
    by_reason = dict(sql("SELECT reason_code, count(*) FROM marts.recon_exceptions GROUP BY 1"))
    assert nonzero(q(layer, "exception_count", ["reason_code"])) == {(k,): v for k, v in by_reason.items()}
    assert nonzero(q(layer, "exception_count", ["window_status"])) == {("exception",): 30}


def test_amount_at_risk(layer, sql):
    total = sql("SELECT sum(amount_minor) FROM marts.recon_exceptions")[0][0]
    assert q(layer, "amount_at_risk") == {(): usd(total)}
    by_reason = sql("SELECT reason_code, sum(amount_minor) FROM marts.recon_exceptions GROUP BY 1")
    assert nonzero(q(layer, "amount_at_risk", ["reason_code"])) == {(k,): usd(v) for k, v in by_reason if v}


def test_exceptions_with_money_at_stake(layer, sql):
    assert q(layer, "exceptions_with_money_at_stake") == {(): sql("SELECT count(*) FROM marts.recon_exceptions WHERE amount_minor > 0")[0][0]}
    by_reason = sql("SELECT reason_code, count(*) FROM marts.recon_exceptions WHERE amount_minor > 0 GROUP BY 1")
    assert nonzero(q(layer, "exceptions_with_money_at_stake", ["reason_code"])) == {(k,): v for k, v in by_reason}
    assert "date_mismatch" not in {k[0] for k in nonzero(q(layer, "exceptions_with_money_at_stake", ["reason_code"]))}


def test_pending_count(layer, sql):
    assert q(layer, "pending_count") == {(): sql("SELECT count(*) FROM marts.recon_pending")[0][0]} == {(): 3}
    by_currency = sql("SELECT currency, count(*) FROM marts.recon_pending GROUP BY 1")
    assert nonzero(q(layer, "pending_count", ["currency"])) == {(k,): v for k, v in by_currency}


def test_match_rate(layer, sql):
    matched, charges = sql("SELECT (SELECT count(*) FROM marts.recon_matches), "
                           "(SELECT count(*) FROM intermediate.int_processor_charges)")[0]
    assert q(layer, "match_rate") == {(): round(matched / charges, 4)}
    by_currency = sql("""
        SELECT c.currency, count(m.charge_id) * 1.0 / count(*)
        FROM intermediate.int_processor_charges AS c
        LEFT JOIN marts.recon_matches AS m ON m.charge_id = c.charge_id GROUP BY 1""")
    assert q(layer, "match_rate", ["currency"]) == {(k,): round(float(v), 4) for k, v in by_currency}


def test_as_of_date_dimension(layer, sql):
    as_of = sql("SELECT max(as_of_date) FROM marts.recon_exceptions")[0][0]
    assert q(layer, "exception_count", ["as_of_date"]) == {(as_of.isoformat(),): 30}
    assert q(layer, "exception_count", [], [{"dimension": "as_of_date", "op": "=", "values": [as_of.isoformat()]}]) \
        == {(): 30}


def test_filters_match_sql(layer, sql):
    fee_and_chargeback = sql("SELECT sum(amount_minor) FROM marts.recon_exceptions "
                             "WHERE reason_code IN ('fee_mismatch', 'chargeback')")[0][0]
    assert q(layer, "amount_at_risk", [], [{"dimension": "reason_code", "op": "in",
                                            "values": ["fee_mismatch", "chargeback"]}]) == {(): usd(fee_and_chargeback)}
    # Exceptions on non-USD items: charges by their currency, bank lines by their batch's currency.
    non_usd = sql("""
        SELECT count(*) FROM marts.recon_exceptions AS e
        LEFT JOIN intermediate.int_processor_charges AS c ON c.charge_id = e.charge_id
        LEFT JOIN staging.stg_bank_settlement_lines AS l ON l.line_id = e.bank_line_id
        LEFT JOIN raw.bank_settlement_batches AS b ON b.batch_id = l.batch_id
        WHERE coalesce(c.currency, b.currency) <> 'USD'""")[0][0]
    assert q(layer, "exception_count", [], [{"dimension": "currency", "op": "!=", "values": ["USD"]}]) == {(): non_usd}


@pytest.mark.parametrize("bad", [
    {"metric": "charges"},                                   # an internal building block, not offered
    {"metric": "revenue"},
    {"metric": "exception_count", "dimensions": ["merchant"]},
    {"metric": "exception_count", "dimensions": ["currency", "currency"]},
    {"metric": "exception_count", "sql": "select 1"},
    {"metric": "exception_count", "filters": [{"dimension": "currency", "op": "=", "values": ["USD' OR 1=1 --"]}]},
    {"metric": "exception_count", "filters": [{"dimension": "reason_code", "op": "=", "values": ["a", "b"]}]},
    {"metric": "exception_count", "filters": [{"dimension": "as_of_date", "op": "=", "values": ["yesterday"]}]},
    {"metric": "exception_count", "filters": [{"dimension": "currency", "op": "like", "values": ["U%"]}]},
])
def test_invalid_queries_are_rejected_before_any_sql(bad):
    with pytest.raises(ValidationError):
        MetricQuery.model_validate(bad)


def test_the_connection_is_read_only(layer):
    import duckdb

    with pytest.raises(duckdb.Error):
        layer.con.execute("CREATE TABLE marts.hack (a INTEGER)")


def test_refuses_a_warehouse_the_manifest_was_not_built_for(recon_db, tmp_path):
    with pytest.raises(ValueError, match="built for 'warehouse'"):
        SemanticLayer(recon_db[0].parent / "target" / "semantic_manifest.json", tmp_path / "other.duckdb")
    with pytest.raises(ValueError, match="not the warehouse"):
        SemanticLayer(recon_db[0].parent / "target" / "semantic_manifest.json", recon_db[1],
                      forbidden_paths=(recon_db[1],))


def test_compiled_sql_reads_only_the_semantic_model(layer):
    sql_text = layer.compile(MetricQuery(metric="amount_at_risk", dimensions=["reason_code"])).upper()
    assert '"MARTS"."RECON_ITEMS"' in sql_text
    for word in ("INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ATTACH", "COPY"):
        assert f" {word} " not in f" {sql_text} "
