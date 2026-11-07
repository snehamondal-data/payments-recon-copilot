"""The MCP server (Slice 9b), through a real MCP client session: in memory, and once over stdio."""

import json
import os
import subprocess
import sys

import anyio
import pytest
from mcp import Client, StdioServerParameters

from recon.config import load_settings
from recon.copilot.backend import BackendUnavailable, InProcessBackend
from recon.copilot.tools import ReadOnlyWarehouse, tool_definitions
from recon.mcp_server import (
    BACKEND_UNAVAILABLE,
    ERROR_KIND,
    TOOL_ERROR,
    AccessLog,
    RefuseToStart,
    check_agent_identity,
    create_server,
    open_agent_warehouse,
)

SIX = ["get_exception", "get_processor_events", "get_bank_settlement", "get_ledger_entries", "get_related_refunds",
       "get_fx_rate"]


def over_mcp(server, fn):
    """Run fn(client) against the server through a connected MCP client session."""
    async def go():
        async with Client(server) as client:
            return await fn(client)
    return anyio.run(go)


def list_tools(server):
    async def fn(client):
        return (await client.list_tools()).tools
    return over_mcp(server, fn)


def call_all(server, calls):
    async def fn(client):
        return [await client.call_tool(name, args) for name, args in calls]
    return over_mcp(server, fn)


@pytest.fixture
def log(tmp_path):
    return AccessLog(tmp_path / "mcp_access.jsonl")


@pytest.fixture
def server(wh, log):
    return create_server(InProcessBackend(wh), log)


def real_calls(wh):
    calls = []
    for row in wh.queue():
        calls.append(("get_exception", {"exception_id": row["exception_id"]}))
        if row["charge_id"]:
            calls += [(t, {"txn_id": row["charge_id"]}) for t in SIX[1:5]]
        if row["bank_line_id"]:
            calls.append(("get_bank_settlement", {"txn_id": row["bank_line_id"]}))
        calls += [("get_fx_rate", {"date": row["item_date"], "currency": c}) for c in ("EUR", "GBP")]
    return calls


def test_exactly_the_six_tools_with_the_agents_schemas(server):
    tools = list_tools(server)
    assert [t.name for t in tools] == SIX
    assert [{"name": t.name, "description": t.description, "input_schema": t.input_schema} for t in tools] \
        == tool_definitions()
    banned = ("write", "insert", "update", "delete", "sql", "query", "submit")
    assert not [t.name for t in tools if any(word in t.name for word in banned)]


def test_every_tool_returns_the_in_process_json_for_every_exception(wh, server):
    backend, calls = InProcessBackend(wh), real_calls(wh)
    results = call_all(server, calls)
    assert len(results) == len(calls) > 100
    for (name, args), result in zip(calls, results):
        assert not result.is_error, (name, args)
        [content] = result.content
        assert content.text == backend.call_tool(name, args).to_json()


@pytest.mark.parametrize("name, args", [
    ("run_sql", {"query": "select * from raw.processor_events"}),
    ("get_exception", {"exception_id": "charge:ch_1'; DROP TABLE marts.recon_exceptions; --"}),
    ("get_processor_events", {"txn_id": "ch_0123456789abcdef' OR '1'='1"}),
    ("get_bank_settlement", {"txn_id": "evt_0123456789abcdef"}),
    ("get_ledger_entries", {"txn_id": "ch_0123456789abcdef", "sql": "select 1"}),
    ("get_fx_rate", {"date": "not-a-date", "currency": "EUR"}),
    ("get_fx_rate", {"date": "2026-10-01", "currency": "JPY"}),
    ("get_exception", {"exception_id": "charge:ch_0000000000000000"}),
])
def test_bad_calls_are_tool_errors_with_the_in_process_message(wh, server, name, args):
    [result] = call_all(server, [(name, args)])
    assert result.is_error and result.meta[ERROR_KIND] == TOOL_ERROR
    assert result.content[0].text == InProcessBackend(wh).call_tool(name, args).error


class CrashingBackend(InProcessBackend):
    def __init__(self, wh, err):
        super().__init__(wh)
        self.err = err

    def call_tool(self, name, arguments):
        raise self.err


@pytest.mark.parametrize("err, text", [(BackendUnavailable("get_fx_rate failed: OperationalError: gone"),
                                        "get_fx_rate failed: OperationalError: gone"),
                                       (RuntimeError("unexpected"), "RuntimeError: unexpected")])
def test_a_backend_failure_is_marked_unavailable_not_an_internal_error(wh, log, err, text):
    [result] = call_all(create_server(CrashingBackend(wh, err), log),
                        [("get_fx_rate", {"date": "2026-10-01", "currency": "EUR"})])
    assert result.is_error and result.meta[ERROR_KIND] == BACKEND_UNAVAILABLE and result.content[0].text == text


def test_blind_mode_hides_the_reason_code(recon_db, wh, log):
    blind = open_agent_warehouse(load_settings({"DUCKDB_PATH": str(recon_db[0]), "GROUND_TRUTH_PATH": str(recon_db[1]),
                                                "COPILOT_HIDE_REASON_CODE": "true"}))
    try:
        eid = wh.queue()[0]["exception_id"]
        calls = [("get_exception", {"exception_id": eid})]
        [hidden] = call_all(create_server(InProcessBackend(blind), log), calls)
        [seen] = call_all(create_server(InProcessBackend(wh), log), calls)
    finally:
        blind.close()
    hidden, seen = (json.loads(r.content[0].text)["records"][0] for r in (hidden, seen))
    assert "reason_code" in seen and "detected_problems" in seen
    assert "reason_code" not in hidden and "detected_problems" not in hidden


def test_every_call_is_in_the_access_log(wh, server, log):
    eid = wh.queue()[0]["exception_id"]
    call_all(server, [("get_exception", {"exception_id": eid}), ("run_sql", {"query": "select 1"})])
    ok, bad = (json.loads(line) for line in log.path.read_text().splitlines())
    assert set(ok) == {"timestamp", "tool", "arguments", "record_ids", "duration_ms", "error", "error_kind"}
    assert ok["tool"] == "get_exception" and ok["arguments"] == {"exception_id": eid} and ok["record_ids"] == [eid]
    assert ok["error"] is None and ok["duration_ms"] >= 0
    assert bad["tool"] == "run_sql" and bad["error"] == "unknown tool 'run_sql'" and bad["error_kind"] == TOOL_ERROR
    assert bad["record_ids"] == []


SNOWFLAKE = {"WAREHOUSE_ENGINE": "snowflake", "SNOWFLAKE_ACCOUNT": "example"}


@pytest.mark.parametrize("env, message", [
    ({"SNOWFLAKE_AGENT_USER": "RECON_LOADER", "SNOWFLAKE_LOADER_USER": "LOADER2"}, "loader's"),
    ({"SNOWFLAKE_AGENT_ROLE": "RECON_LOADER_ROLE", "SNOWFLAKE_LOADER_ROLE": "LOADER2_ROLE"}, "loader's"),
    ({"SNOWFLAKE_AGENT_KEY_PATH": "secrets/recon_loader_rsa_key.p8"}, "private key"),
    ({"SNOWFLAKE_AGENT_ROLE": "SYSADMIN"}, "only runs as RECON_AGENT_ROLE"),
])
def test_it_refuses_loader_credentials_and_other_roles(env, message):
    with pytest.raises(RefuseToStart, match=message):
        check_agent_identity(load_settings(SNOWFLAKE | env))  # refused before any connection is attempted


def test_the_agent_login_is_accepted():
    check_agent_identity(load_settings(SNOWFLAKE))


def test_it_refuses_the_ground_truth_file(recon_db, tmp_path):
    with pytest.raises(RefuseToStart, match="ground-truth tables"):  # passed as the warehouse
        check_agent_identity(load_settings({"DUCKDB_PATH": str(recon_db[1]),
                                            "GROUND_TRUTH_PATH": str(tmp_path / "elsewhere.duckdb")}))
    link = tmp_path / "warehouse.duckdb"
    link.symlink_to(recon_db[1])
    with pytest.raises(ValueError, match="different files"):  # a path that resolves to it
        load_settings({"DUCKDB_PATH": str(link), "GROUND_TRUTH_PATH": str(recon_db[1])})


def server_env(recon_db, tmp_path, **overrides):
    return os.environ | {"WAREHOUSE_ENGINE": "duckdb", "DUCKDB_PATH": str(recon_db[0]),
                         "GROUND_TRUTH_PATH": str(recon_db[1]), "COPILOT_DB_PATH": str(tmp_path / "copilot.duckdb"),
                         "COPILOT_HIDE_REASON_CODE": "false", "MCP_ACCESS_LOG": str(tmp_path / "access.jsonl")
                         } | overrides


def test_over_stdio_as_a_subprocess(recon_db, wh, tmp_path):
    params = StdioServerParameters(command=sys.executable, args=["-m", "recon.mcp_server"],
                                   env=server_env(recon_db, tmp_path))
    eid = wh.queue()[0]["exception_id"]

    async def fn(client):
        tools = (await client.list_tools()).tools
        return tools, await client.call_tool("get_exception", {"exception_id": eid})

    tools, result = over_mcp(params, fn)
    assert [t.name for t in tools] == SIX
    assert result.content[0].text == InProcessBackend(wh).call_tool("get_exception", {"exception_id": eid}).to_json()
    assert json.loads((tmp_path / "access.jsonl").read_text())["record_ids"] == [eid]


def test_the_process_exits_without_serving_when_refused(recon_db, tmp_path):
    env = server_env(recon_db, tmp_path, DUCKDB_PATH=str(recon_db[1]),
                     GROUND_TRUTH_PATH=str(tmp_path / "elsewhere.duckdb"))
    done = subprocess.run([sys.executable, "-m", "recon.mcp_server"], env=env, input="", capture_output=True,
                          text=True, timeout=60)
    assert done.returncode == 2 and "refusing to start" in done.stderr and done.stdout == ""
