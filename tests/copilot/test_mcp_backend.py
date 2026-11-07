"""The agent over MCP (Slice 9c): McpBackend, its failure modes, and parity with the in-process backend."""

import json
import sys
from pathlib import Path

import duckdb
import pytest

import recon.copilot.mcp_backend as mcp_backend_module
from fakes import FakeModelClient, call, last_tool_results, submit
from recon.config import load_settings
from recon.copilot import tools as tools_module
from recon.copilot.__main__ import main
from recon.copilot.agent import AgentConfig, Copilot
from recon.copilot.contract import ProposedAction, RootCause
from recon.copilot.mcp_backend import McpBackend
from recon.copilot.store import CopilotStore
from recon.copilot.tools import ToolSession
from recon.mcp_parity import compare

STUB = Path(__file__).with_name("mcp_stub_server.py")


@pytest.fixture
def settings(recon_db, tmp_path):
    def make(**env):
        return load_settings({"DUCKDB_PATH": str(recon_db[0]), "GROUND_TRUTH_PATH": str(recon_db[1]),
                              "COPILOT_DB_PATH": str(tmp_path / "copilot.duckdb"),
                              "MCP_ACCESS_LOG": str(tmp_path / "mcp_access.jsonl"), "COPILOT_TOOL_BACKEND": "mcp",
                              "COPILOT_MCP_TIMEOUT_S": "10"} | env)
    return make


@pytest.fixture
def fee(wh, cases):
    case = cases["fee_mismatch"]
    charge = case.charge_ids[0]
    session = ToolSession(wh)
    events = json.loads(session.call("get_processor_events", {"txn_id": charge}))["records"]
    line = json.loads(session.call("get_bank_settlement", {"txn_id": charge}))["records"][0]["record_id"]
    evidence = [next(e["record_id"] for e in events if e["event_type"] == "fee"), line]
    script = [call("get_exception", exception_id=case.exception_id),
              call("get_processor_events", txn_id=charge),
              call("get_bank_settlement", txn_id=charge),
              submit(root_cause="fee_mismatch", evidence=evidence, proposed_action="raise_fee_claim_with_processor",
                     confidence=0.85, needs_human_escalation=False, rationale="Bank net is short by the fee.")]
    return case, script


def diagnose(backend, script, exception_id, tmp_path, **config):
    backend.start()
    try:
        run = Copilot(FakeModelClient(script), None, AgentConfig(**config), backend=backend).diagnose(exception_id)
    finally:
        backend.close()
    store = CopilotStore(tmp_path / "audit.duckdb")
    store.record_run(run)
    store.close()
    con = duckdb.connect(str(tmp_path / "audit.duckdb"), read_only=True)
    events = [(t, json.loads(p)) for t, p in con.execute(
        "SELECT event_type, payload FROM audit_log ORDER BY audit_id").fetchall()]
    con.close()
    return run, events


def test_in_mcp_mode_the_agent_never_opens_a_warehouse(settings, fee, tmp_path, monkeypatch):
    case, script = fee
    backend = McpBackend(settings())

    def refuse(*args, **kwargs):
        raise AssertionError("the agent process opened a warehouse connection")

    # Every way this process could reach a warehouse; the server is a separate process and unaffected.
    monkeypatch.setattr(duckdb, "connect", refuse)
    monkeypatch.setattr(tools_module, "open_read_only", refuse)
    monkeypatch.setattr(tools_module.ReadOnlyWarehouse, "__init__", refuse)
    monkeypatch.setattr(tools_module.ReadOnlyWarehouse, "snowflake", refuse)
    backend.start()
    try:
        run = Copilot(FakeModelClient(script), None, backend=backend).diagnose(case.exception_id)
    finally:
        backend.close()
    assert not run.is_fallback, run.fallback_reason
    assert run.diagnosis.proposed_action == ProposedAction.RAISE_FEE_CLAIM_WITH_PROCESSOR
    assert [c.name for c in run.tool_calls] == ["get_exception", "get_processor_events", "get_bank_settlement"]


def test_parity_with_the_in_process_backend(recon_db, tmp_path):
    settings = load_settings({"DUCKDB_PATH": str(recon_db[0]), "GROUND_TRUTH_PATH": str(recon_db[1]),
                              "COPILOT_DB_PATH": str(tmp_path / "unused.duckdb")})
    report = compare(settings, tmp_path, log=lambda line: None)
    assert report["identical"], report
    assert report["exceptions"] == report["diagnosed"] == 30 and report["fallbacks"] == 0
    assert report["tool_outputs"] > 150 and report["audit_events"] > 300 and report["policy_overrides"] > 0


def test_a_tool_error_over_mcp_lets_the_model_retry(settings, fee, tmp_path):
    case, script = fee
    seen, look = [], script[1]

    def bad_call_then_look(messages):
        seen.append(last_tool_results(messages))
        return look

    script = [script[0], call("get_processor_events", txn_id="ch_1; DROP TABLE x"), bad_call_then_look, *script[2:]]
    run, events = diagnose(McpBackend(settings()), script, case.exception_id, tmp_path)
    assert not run.is_fallback
    [bad] = seen[0]
    assert bad["is_error"] and bad["content"].startswith("invalid arguments: txn_id")
    calls = [p for t, p in events if t == "tool_call"]
    assert calls[1]["error"].startswith("invalid arguments") and calls[2]["error"] is None


@pytest.mark.parametrize("mode, reason", [
    ("crash", "the MCP server stopped answering"),
    ("hang", "no answer from the MCP server within 1s"),
    ("malformed", "malformed result from get_processor_events"),
    ("unavailable", "the MCP server reported: warehouse connection lost"),
    ("unmarked", "the MCP server reported: something broke"),
])
def test_a_failing_server_ends_in_the_fallback_with_the_reason_logged(settings, fee, tmp_path, mode, reason):
    case, script = fee
    backend = McpBackend(settings(COPILOT_MCP_TIMEOUT_S="1"), command=[sys.executable, str(STUB), mode])
    # The stub misbehaves from its second call: the orchestrator's get_exception is the first.
    run, events = diagnose(backend, script[1:], case.exception_id, tmp_path)

    assert run.is_fallback and run.fallback_reason.startswith(f"tool backend unavailable: {reason}")
    assert run.diagnosis.proposed_action == ProposedAction.ESCALATE_TO_HUMAN
    assert run.diagnosis.root_cause == RootCause.FEE_MISMATCH  # the reason code from the row
    diagnosis = next(p for t, p in events if t == "diagnosis")
    assert diagnosis["is_fallback"] and diagnosis["fallback_reason"] == run.fallback_reason
    failed = [p for t, p in events if t == "tool_call"][-1]
    assert failed["tool"] == "get_processor_events" and failed["error"].startswith("backend unavailable:")
    if mode == "crash":
        assert "crashing on purpose" in run.fallback_reason  # the server's last stderr line


def test_a_server_that_refuses_to_start_ends_in_the_fallback(settings, recon_db, tmp_path, fee):
    case, script = fee
    refused = settings(DUCKDB_PATH=str(recon_db[1]), GROUND_TRUTH_PATH=str(tmp_path / "elsewhere.duckdb"))
    run, events = diagnose(McpBackend(refused), script, case.exception_id, tmp_path)
    assert run.is_fallback and "the MCP server did not start: refusing to start" in run.fallback_reason
    assert "ground-truth tables" in run.fallback_reason
    assert run.diagnosis.root_cause == RootCause.AMBIGUOUS  # the row never arrived
    assert [t for t, _ in events] == ["run_started", "diagnosis"]  # no model call was made
    assert next(p for t, p in events if t == "diagnosis")["fallback_reason"] == run.fallback_reason


def test_diagnose_all_starts_one_server_and_survives_one_that_wont_start(settings, tmp_path, monkeypatch):
    started = []
    real = mcp_backend_module.stdio_client
    monkeypatch.setattr(mcp_backend_module, "stdio_client", lambda *a, **k: started.append(1) or real(*a, **k))
    out = []
    code = main(["diagnose", "--all"], settings=settings(MCP_ACCESS_LOG="/dev/null/access.jsonl"),
                client_factory=lambda s: FakeModelClient([]), out=out.append)
    assert code == 0 and len(out) == 30 and len(started) == 1
    assert all("FALLBACK: tool backend unavailable: the MCP server did not start" in line for line in out)


def test_blind_mode_is_enforced_by_the_server(settings, wh, tmp_path):
    row = next(r for r in wh.queue() if r["reason_code"] == "ambiguous")
    run, events = diagnose(McpBackend(settings(COPILOT_HIDE_REASON_CODE="true")),
                           [call("get_exception", exception_id=row["exception_id"]), RuntimeError("API down")],
                           row["exception_id"], tmp_path)
    assert run.hide_reason_code and not run.escalation_floor_active
    assert next(p for t, p in events if t == "run_started")["hide_reason_code"] is True
    # The orchestrator's view lacks the reason code too, so the fallback can only say ambiguous.
    assert run.is_fallback and run.diagnosis.root_cause == RootCause.AMBIGUOUS
    backend = McpBackend(settings(COPILOT_HIDE_REASON_CODE="true"))
    backend.start()
    try:
        orchestrator_row = backend.exception_row(row["exception_id"])
        model_view = json.loads(backend.call_tool("get_exception", {"exception_id": row["exception_id"]}).to_json())
    finally:
        backend.close()
    assert orchestrator_row["exception_id"] == row["exception_id"]
    assert "reason_code" not in orchestrator_row and "detected_problems" not in orchestrator_row
    assert "reason_code" not in model_view["records"][0]
