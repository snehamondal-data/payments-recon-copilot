"""The MCP server on Snowflake (Slice 9e, opt-in: make test-snowflake). Its session, started as a real
subprocess, holds exactly RECON_AGENT_ROLE with secondary roles off, and it serves the tools from there."""

from __future__ import annotations

import dataclasses
import json

import pytest

from recon.copilot.mcp_backend import McpBackend

pytestmark = pytest.mark.snowflake


def test_the_mcp_server_session_is_exactly_the_agent_role(settings, tmp_path):
    log = tmp_path / "mcp_access.jsonl"
    backend = McpBackend(dataclasses.replace(settings, warehouse_engine="snowflake", mcp_access_log=log,
                                             copilot_mcp_timeout_s=90))
    backend.start()
    try:
        assert [t["name"] for t in backend.list_tools()][0] == "get_exception"
        result = backend.call_tool("get_fx_rate", {"date": "2026-10-01", "currency": "EUR"})
        denied = backend.call_tool("get_exception", {"exception_id": "charge:ch_0000000000000000"})
    finally:
        backend.close()
    assert result.error is None and result.records[0]["record_id"] == "fx:2026-10-01:EUR"
    assert denied.error == "no exception charge:ch_0000000000000000"
    started, *calls = (json.loads(line) for line in log.read_text().splitlines())
    session = started["session"]
    assert started["event"] == "server_started"
    assert session["engine"] == "snowflake" and session["user"] == "RECON_AGENT"
    assert session["role"] == "RECON_AGENT_ROLE"
    assert session["secondary_roles"]["value"] == ""  # USE SECONDARY ROLES NONE held
    assert [c["tool"] for c in calls] == ["get_fx_rate", "get_exception"]
