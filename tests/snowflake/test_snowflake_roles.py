"""RECON_AGENT_ROLE can SELECT RAW, REF and MARTS and do nothing else. Audit goes to COPILOT via the loader.

Every write probe is a statement that would change nothing even if it ran (WHERE 1 = 0, or a new object
name), so a failure here can't damage the warehouse. Snowflake checks privileges before executing.
"""

from __future__ import annotations

import json

import pytest
from snowflake.connector.errors import ProgrammingError

from fakes import FakeModelClient, call, submit
from recon.config import SnowflakeLogin, SnowflakeSettings
from recon.copilot.agent import Copilot
from recon.copilot.store import CopilotStore
from recon.copilot.tools import ReadOnlyWarehouse, ToolSession
from recon.warehouse.snowflake_client import SnowflakeClient, connect

pytestmark = pytest.mark.snowflake

DENIED = ("insufficient privileges", "does not exist or not authorized")

WRITES = [
    "INSERT INTO raw.processor_events (event_id) SELECT 'evt_probe' WHERE 1 = 0",
    "INSERT INTO ref.fx_rates (rate_date, currency, rate_to_usd) SELECT current_date, 'USD', 1 WHERE 1 = 0",
    "UPDATE marts.recon_exceptions SET amount_minor = 0 WHERE 1 = 0",
    "UPDATE raw.bank_settlement_lines SET reference = NULL WHERE 1 = 0",
    "DELETE FROM ref.fx_rates WHERE 1 = 0",
    "MERGE INTO raw.ledger_journals AS t USING (SELECT 'x' AS event_id) AS s ON t.event_id = s.event_id "
    "WHEN MATCHED AND 1 = 0 THEN DELETE",
    "CREATE TABLE marts.agent_probe (a INTEGER)",
    "CREATE TABLE raw.agent_probe (a INTEGER)",
    "CREATE VIEW marts.agent_probe AS SELECT 1 AS a",
    "CREATE SCHEMA RECON.AGENT_PROBE",
    "CREATE TABLE copilot.agent_probe (a INTEGER)",
    "INSERT INTO copilot.audit_log (exception_id) SELECT 'x' WHERE 1 = 0",
    "GRANT SELECT ON TABLE marts.recon_exceptions TO ROLE PUBLIC",
]
NOT_VISIBLE = [
    "SELECT count(*) FROM copilot.audit_log",
    "SELECT count(*) FROM copilot.resolutions",
    "SELECT count(*) FROM staging.stg_processor_events",
    "SELECT count(*) FROM intermediate.int_charge_recon",
]
READABLE = [
    "SELECT count(*) FROM marts.recon_exceptions",
    "SELECT count(*) FROM raw.processor_events",
    "SELECT count(*) FROM ref.fx_rates",
]


@pytest.fixture(scope="module")
def agent(sf, sf_pipeline):
    con = connect(sf, sf.agent)
    yield con
    con.close()


def run(con, sql):
    return con.cursor().execute(sql).fetchall()


@pytest.mark.parametrize("sql", WRITES)
def test_the_agent_role_cannot_insert_update_delete_create_or_grant(agent, sql):
    with pytest.raises(ProgrammingError) as err:
        run(agent, sql)
    assert any(d in str(err.value).lower() for d in DENIED), err.value  # refused for privileges, not syntax


@pytest.mark.parametrize("sql", NOT_VISIBLE)
def test_the_agent_role_cannot_read_copilot_staging_or_intermediate(agent, sql):
    with pytest.raises(ProgrammingError) as err:
        run(agent, sql)
    assert any(d in str(err.value).lower() for d in DENIED), err.value


@pytest.mark.parametrize("sql", READABLE)
def test_the_agent_role_can_select_raw_ref_and_marts(agent, sql):
    assert run(agent, sql)[0][0] > 0


def test_the_agent_session_is_the_agent_role_with_no_secondary_roles(agent, sf):
    assert run(agent, "SELECT CURRENT_ROLE()")[0][0] == sf.agent.role.upper()
    # connect() turned DEFAULT_SECONDARY_ROLES = ('ALL') off for the session
    assert '"value":""' in run(agent, "SELECT CURRENT_SECONDARY_ROLES()")[0][0].replace(" ", "")


def test_the_agent_user_cannot_assume_the_loader_role(sf):
    with pytest.raises(Exception):  # Snowflake refuses the login: the role is not granted to this user
        connect(sf, SnowflakeLogin(sf.agent.user, sf.loader.role, sf.agent.key_path))


def test_settings_refuse_to_make_the_agent_the_loader(sf):
    with pytest.raises(ValueError):
        SnowflakeSettings(sf.account, sf.warehouse, sf.database, loader=sf.loader, agent=sf.loader)


def test_audit_and_resolutions_go_to_copilot_through_the_loader(sf, sf_pipeline):
    """A full diagnose -> record -> approve cycle on Snowflake, with the fake model. Written to session
    TEMPORARY copies of the COPILOT tables, so the real audit log is untouched."""
    client = SnowflakeClient(sf, sf.loader, schema="COPILOT")
    warehouse = ReadOnlyWarehouse.snowflake(sf)
    try:
        CopilotStore(client)  # the permanent tables exist (idempotent DDL)
        before = client.execute("SELECT count(*) FROM audit_log")[0][0]
        for table in ("audit_log", "resolutions"):
            client.execute(f"CREATE TEMPORARY TABLE {table} LIKE {table}")
        store = CopilotStore(client)

        fee = next(r for r in warehouse.queue() if r["reason_code"] == "fee_mismatch")
        charge = fee["charge_id"]
        tools = ToolSession(warehouse)
        events = json.loads(tools.call("get_processor_events", {"txn_id": charge}))["records"]
        line = json.loads(tools.call("get_bank_settlement", {"txn_id": charge}))["records"][0]["record_id"]
        fee_event = next(e["record_id"] for e in events if e["event_type"] == "fee")
        script = [call("get_exception", exception_id=fee["exception_id"]), call("get_processor_events", txn_id=charge),
                  call("get_bank_settlement", txn_id=charge),
                  submit(root_cause="fee_mismatch", evidence=[fee_event, line],
                         proposed_action="raise_fee_claim_with_processor", confidence=0.9,
                         needs_human_escalation=False, rationale="Short by more than the fee.")]
        run_result = Copilot(FakeModelClient(script), warehouse).diagnose(fee["exception_id"])
        assert not run_result.is_fallback
        store.record_run(run_result)
        rid = store.approve(fee["exception_id"], reviewer="pytest", reason="test", amount_minor=fee["amount_minor"])

        kinds = [e["event_type"] for e in store.events(run_id=run_result.run_id)]
        assert kinds[0] == "run_started" and kinds[-1] == "resolution_applied" and kinds.count("tool_call") == 3
        audit_ids = [e["audit_id"] for e in store.events(run_id=run_result.run_id)]
        assert audit_ids == sorted(audit_ids)  # the ORDER sequence keeps the log in write order
        [res] = store.resolutions()
        assert res["resolution_id"] == rid and json.loads(res["record_ids"]) == [fee_event, line]
        assert store.latest_proposal(fee["exception_id"]).status == "approved"
    finally:
        warehouse.close()
        client.close()
    check = SnowflakeClient(sf, sf.loader, schema="COPILOT")  # a new session: the temporary tables are gone
    try:
        assert check.execute("SELECT count(*) FROM audit_log")[0][0] == before
    finally:
        check.close()
