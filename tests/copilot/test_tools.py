import json
import re
from pathlib import Path

import duckdb
import pytest

from recon.copilot.tools import ReadOnlyWarehouse, ToolError, ToolSession, open_read_only, tool_definitions

COPILOT_SRC = Path(__file__).resolve().parents[2] / "src" / "recon" / "copilot"


def records(session: ToolSession, name: str, **args) -> list[dict]:
    return json.loads(session.call(name, args))["records"]


# --- read-only and isolation --------------------------------------------------

@pytest.mark.parametrize("sql", [
    "CREATE TABLE raw.hack (a INTEGER)",
    "INSERT INTO ref.fx_rates VALUES ('2026-01-01', 'USD', 1)",
    "DELETE FROM raw.processor_events",
    "SET enable_external_access = true",
    "SELECT * FROM read_csv('data/ref/fx_rates.csv')",
])
def test_the_connection_cannot_write_or_reach_outside_the_warehouse(wh, sql):
    with pytest.raises(duckdb.Error):
        wh.con.execute(sql)


def test_the_connection_cannot_attach_the_ground_truth_file(wh, recon_db):
    with pytest.raises(duckdb.Error):
        wh.con.execute(f"ATTACH '{recon_db[1]}' AS gt (READ_ONLY)")


def test_refuses_to_open_a_forbidden_file(recon_db):
    with pytest.raises(ValueError):
        ReadOnlyWarehouse(recon_db[1], forbidden_paths=(recon_db[1],))


def test_tool_sql_is_select_only_and_copilot_code_never_mutates():
    tools_sql = " ".join(re.findall(r'"""(.*?)"""', (COPILOT_SRC / "tools.py").read_text(), flags=re.S)).upper()
    for word in ("INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "ATTACH", "COPY"):
        assert not re.search(rf"\b{word}\b", tools_sql), word
    for path in COPILOT_SRC.rglob("*.py"):
        text = path.read_text().upper()
        assert not re.search(r"\bUPDATE\s+\w", text) and "DELETE FROM" not in text, path


# --- argument validation --------------------------------------------------------

@pytest.mark.parametrize(("name", "args"), [
    ("get_processor_events", {"txn_id": "ch_x' OR 1=1 --"}),
    ("get_processor_events", {"txn_id": "bln_0123456789abcdef"}),  # charge tools take charge IDs only
    ("get_bank_settlement", {"txn_id": "evt_0123456789abcdef"}),
    ("get_exception", {"exception_id": "charge:ch_1; DROP TABLE x"}),
    ("get_fx_rate", {"date": "not-a-date", "currency": "EUR"}),
    ("get_fx_rate", {"date": "2026-10-01", "currency": "JPY"}),
    ("get_ledger_entries", {"txn_id": "ch_0123456789abcdef", "sql": "select 1"}),
    ("run_sql", {"query": "select * from raw.processor_events"}),
])
def test_bad_arguments_are_rejected_before_any_query(wh, name, args):
    session = ToolSession(wh)
    with pytest.raises(ToolError):
        session.call(name, args)
    assert session.returned_record_ids == set()
    assert session.calls[-1].error


def test_tool_definitions_are_the_six_from_the_spec():
    assert [t["name"] for t in tool_definitions()] == [
        "get_exception", "get_processor_events", "get_bank_settlement", "get_ledger_entries",
        "get_related_refunds", "get_fx_rate"]
    for t in tool_definitions():
        assert t["input_schema"]["additionalProperties"] is False


# --- what each tool returns -----------------------------------------------------

def test_get_exception(wh, cases):
    case = cases["fee_mismatch"]
    [row] = records(ToolSession(wh), "get_exception", exception_id=case.exception_id)
    assert row["record_id"] == case.exception_id
    assert row["reason_code"] == "fee_mismatch"


def test_blind_mode_hides_reason_code_from_the_tool_but_not_internally(recon_db, cases):
    blind = ReadOnlyWarehouse(recon_db[0], hide_reason_code=True)
    try:
        [row] = records(ToolSession(blind), "get_exception", exception_id=cases["fee_mismatch"].exception_id)
        assert "reason_code" not in row
        assert blind.exception_row(cases["fee_mismatch"].exception_id)["reason_code"] == "fee_mismatch"
    finally:
        blind.close()


def test_get_processor_events_shows_both_deliveries_of_a_duplicate_webhook(wh, cases):
    events = records(ToolSession(wh), "get_processor_events", txn_id=cases["duplicate_webhook"].charge_ids[0])
    charges = [e for e in events if e["event_type"] == "charge"]
    assert len(charges) == 2 and charges[0]["object_id"] == charges[1]["object_id"]
    assert {e["record_id"] for e in events} <= set(cases["duplicate_webhook"].record_ids)


def test_timestamps_are_shown_in_utc_whatever_the_machine_time_zone(wh, cases):
    # The date-boundary charge is 23:xx in New York, so in UTC it falls on the next day.
    session = ToolSession(wh)
    charge = cases["date_boundary"].charge_ids[0]
    [event] = [e for e in records(session, "get_processor_events", txn_id=charge) if e["event_type"] == "charge"]
    [journal] = records(session, "get_ledger_entries", txn_id=charge)
    assert event["occurred_at"].endswith("+00:00") and journal["posted_at"].endswith("+00:00")
    assert event["occurred_at"][:10] != journal["posting_date"]  # the mismatch the agent has to spot


def test_get_bank_settlement_by_charge_and_by_line(wh, cases):
    session = ToolSession(wh)
    by_charge = records(session, "get_bank_settlement", txn_id=cases["fee_mismatch"].charge_ids[0])
    assert [r["description"] for r in by_charge] == ["SETTLEMENT"]
    orphan_line = cases["orphan_bank_deposit"].exception_id.removeprefix("bank_line:")
    [line] = records(session, "get_bank_settlement", txn_id=orphan_line)
    assert line["record_id"] == orphan_line and line["reference"].startswith("TRF-")


def test_get_ledger_entries_includes_balanced_lines(wh, cases):
    journals = records(ToolSession(wh), "get_ledger_entries", txn_id=cases["date_boundary"].charge_ids[0])
    assert journals and all(sum(l["debit_minor"] for l in j["lines"]) == sum(l["credit_minor"] for l in j["lines"])
                            for j in journals)


def test_get_related_refunds_shows_which_sources_have_the_refund(wh, cases):
    rows = records(ToolSession(wh), "get_related_refunds", txn_id=cases["partial_refund_not_in_ledger"].charge_ids[0])
    assert sorted(r["source"] for r in rows) == ["bank", "processor"]  # no ledger refund: that's the problem


def test_get_fx_rate(wh):
    [rate] = records(ToolSession(wh), "get_fx_rate", date="2026-10-01", currency="EUR")
    assert rate["record_id"] == "fx:2026-10-01:EUR" and float(rate["rate_to_usd"]) > 1


def test_session_tracks_every_record_id_it_returned(wh, cases):
    session = ToolSession(wh)
    records(session, "get_exception", exception_id=cases["chargeback"].exception_id)
    records(session, "get_bank_settlement", txn_id=cases["chargeback"].charge_ids[0])
    assert cases["chargeback"].exception_id in session.returned_record_ids
    assert len(session.calls) == 2 and all(c.record_ids for c in session.calls)


def test_lookup_record_finds_each_kind_of_id(wh, cases):
    case = cases["chargeback"]
    for rid in [case.exception_id, *case.record_ids, "fx:2026-10-01:GBP"]:
        assert wh.lookup_record(rid) is not None, rid


def test_open_read_only_is_what_the_warehouse_uses(recon_db):
    con = open_read_only(recon_db[0])
    try:
        assert con.execute("SELECT count(*) FROM marts.recon_exceptions").fetchone()[0] == 30
    finally:
        con.close()
