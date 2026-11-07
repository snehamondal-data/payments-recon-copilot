"""The ToolBackend interface (Slice 9a): the agent loop gets its tools from a backend, not the warehouse."""

import json

import duckdb
import pytest

from fakes import FakeModelClient, call
from recon.config import load_settings
from recon.copilot.agent import Copilot
from recon.copilot.backend import BackendUnavailable, InProcessBackend, ToolResult, make_backend
from recon.copilot.contract import ProposedAction
from recon.copilot.mcp_backend import McpBackend
from recon.copilot.store import CopilotStore
from recon.copilot.tools import TOOL_SPECS, ToolSession, tool_definitions

SIX = ["get_exception", "get_processor_events", "get_bank_settlement", "get_ledger_entries", "get_related_refunds",
       "get_fx_rate"]


class CrashingBackend(InProcessBackend):
    """Lists the real tools, then fails every call the way a dead MCP server would."""

    def call_tool(self, name, arguments):
        raise BackendUnavailable("server process exited")


def test_list_tools_is_exactly_the_six(wh):
    tools = InProcessBackend(wh).list_tools()
    assert [t["name"] for t in tools] == SIX
    assert all(set(t) == {"name", "description", "input_schema"} for t in tools)
    assert tools == tool_definitions()


def test_the_agent_offers_the_backend_tools_plus_submit(wh):
    assert [t["name"] for t in Copilot(FakeModelClient([]), wh).tools] == [*SIX, "submit_diagnosis"]


def test_an_unknown_tool_is_an_error_result_not_an_exception(wh):
    result = InProcessBackend(wh).call_tool("run_sql", {"query": "select 1"})
    assert result == ToolResult(error="unknown tool 'run_sql'")


@pytest.mark.parametrize("name, args", [
    ("get_processor_events", {"txn_id": "ch_1; DROP TABLE x"}),
    ("get_fx_rate", {"date": "2026-10-01", "currency": "JPY"}),
    ("get_ledger_entries", {"txn_id": "ch_0123456789abcdef", "sql": "select 1"}),
    ("get_exception", "not a dict"),
])
def test_bad_arguments_are_an_error_result(wh, name, args):
    result = InProcessBackend(wh).call_tool(name, args)
    assert result.error.startswith("invalid arguments: ") and result.records == []


def test_a_missing_record_is_a_tool_error_not_a_backend_failure(wh):
    result = InProcessBackend(wh).call_tool("get_exception", {"exception_id": "charge:ch_0000000000000000"})
    assert result.error == "no exception charge:ch_0000000000000000"


def test_backend_records_equal_the_session_and_the_warehouse_for_real_ids(wh):
    backend, session = InProcessBackend(wh), ToolSession(wh)
    specs = {s.name: s.args for s in TOOL_SPECS}
    calls = []
    for row in wh.queue()[:6]:
        calls.append(("get_exception", {"exception_id": row["exception_id"]}))
        if row["charge_id"]:
            calls += [(t, {"txn_id": row["charge_id"]}) for t in SIX[1:5]]
        if row["bank_line_id"]:
            calls.append(("get_bank_settlement", {"txn_id": row["bank_line_id"]}))
        calls.append(("get_fx_rate", {"date": row["item_date"], "currency": "EUR"}))
    for name, args in calls:
        result = backend.call_tool(name, args)
        assert result.error is None
        assert json.loads(session.call(name, args))["records"] == result.records
        assert result.records == getattr(wh, name)(specs[name].model_validate(args))
        assert session.call(name, args) == result.to_json()  # the exact string the model sees


def test_a_database_error_is_a_backend_failure(wh):
    backend = InProcessBackend(wh)
    backend._handlers["get_fx_rate"] = (backend._handlers["get_fx_rate"][0], lambda a: 1 / 0)
    with pytest.raises(BackendUnavailable, match="get_fx_rate failed: ZeroDivisionError"):
        backend.call_tool("get_fx_rate", {"date": "2026-10-01", "currency": "EUR"})


def test_a_dead_backend_falls_back_and_the_call_is_logged_with_its_error(wh, cases, tmp_path):
    case = cases["fee_mismatch"]
    client = FakeModelClient([call("get_exception", exception_id=case.exception_id)])
    run = Copilot(client, wh, backend=CrashingBackend(wh)).diagnose(case.exception_id)

    assert run.is_fallback and run.fallback_reason == "tool backend unavailable: server process exited"
    assert run.diagnosis.proposed_action == ProposedAction.ESCALATE_TO_HUMAN and run.diagnosis.confidence == 0.0
    assert len(client.requests) == 1  # the model isn't asked to retry an infrastructure failure
    [failed] = run.tool_calls
    assert failed.name == "get_exception" and failed.error == "backend unavailable: server process exited"

    store = CopilotStore(tmp_path / "copilot.duckdb")
    store.record_run(run)
    store.close()
    con = duckdb.connect(str(tmp_path / "copilot.duckdb"), read_only=True)
    payloads = [json.loads(p) for (p,) in con.execute(
        "SELECT payload FROM audit_log WHERE event_type = 'tool_call' ORDER BY audit_id").fetchall()]
    diagnosis = json.loads(con.execute("SELECT payload FROM audit_log WHERE event_type = 'diagnosis'").fetchone()[0])
    con.close()
    assert payloads == [{"seq": 1, "tool": "get_exception", "arguments": {"exception_id": case.exception_id},
                         "returned_record_ids": [], "error": "backend unavailable: server process exited",
                         "duration_ms": failed.duration_ms}]
    assert diagnosis["is_fallback"] is True


def test_make_backend_reads_copilot_tool_backend(wh):
    assert load_settings({}).copilot_tool_backend == "inprocess"
    assert isinstance(make_backend(load_settings({}), wh), InProcessBackend)
    assert isinstance(make_backend(load_settings({"COPILOT_TOOL_BACKEND": "mcp"})), McpBackend)  # 9c; not started
    with pytest.raises(ValueError, match="COPILOT_TOOL_BACKEND"):
        make_backend(load_settings({"COPILOT_TOOL_BACKEND": "grpc"}), wh)
