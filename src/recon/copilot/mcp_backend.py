"""The agent's tools over MCP (Slice 9c): McpBackend starts `python -m recon.mcp_server` over stdio.

The agent loop stays synchronous. The async MCP client runs in an anyio blocking portal, an event loop in
one background thread: start() enters one client session through it, every call is a portal.call, and
close() leaves the session, which stops the server. One server per command, so diagnose --all reuses it.

In this mode the agent's process opens no warehouse. The queue row comes from get_exception through MCP
(an orchestrator call, outside the model's tool budget and its audit trail; the server's access log has
it), and the as-of date from the recon://as-of resource, which is not a tool, so the model never sees it.
Blind mode is enforced by the server alone: in it, get_exception has no reason_code for anyone.

Error mapping:
  _meta recon/error_kind == tool_error      an error result: the model sees it and can retry
  backend_unavailable, a server that can't start or has exited, a call past COPILOT_MCP_TIMEOUT_S,
  or a result that isn't the agreed JSON    BackendUnavailable: the run escalates, with the reason logged
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from collections.abc import Sequence
from typing import Any

import anyio
from anyio.from_thread import start_blocking_portal
from mcp import Client, StdioServerParameters
from mcp.client.stdio import stdio_client

from recon.config import Settings
from recon.copilot.backend import BackendUnavailable, ToolResult

# The same strings as recon.mcp_server, repeated so the agent doesn't import the server's module.
ERROR_KIND, TOOL_ERROR = "recon/error_kind", "tool_error"
AS_OF_URI = "recon://as-of"


def server_env(settings: Settings) -> dict[str, str]:
    """The server reads its settings from the environment: hand it this process's, with absolute paths (the
    server's working directory may differ). Not the answers' location: the copilot package never handles it.
    The server takes that from its own environment, and refuses any file that holds ground-truth tables."""
    env = {"WAREHOUSE_ENGINE": settings.warehouse_engine, "DUCKDB_PATH": str(settings.duckdb_path.resolve()),
           "COPILOT_DB_PATH": str(settings.copilot_db_path.resolve()),
           "COPILOT_HIDE_REASON_CODE": "true" if settings.copilot_hide_reason_code else "false",
           "MCP_ACCESS_LOG": str(settings.mcp_access_log.resolve()),
           "COPILOT_AS_OF": settings.copilot_as_of.isoformat() if settings.copilot_as_of else ""}
    if (sf := settings.snowflake) is not None:
        env |= {"SNOWFLAKE_ACCOUNT": sf.account, "SNOWFLAKE_WAREHOUSE": sf.warehouse,
                "SNOWFLAKE_DATABASE": sf.database,
                "SNOWFLAKE_LOADER_USER": sf.loader.user, "SNOWFLAKE_LOADER_ROLE": sf.loader.role,
                "SNOWFLAKE_LOADER_KEY_PATH": str(sf.loader.key_path.resolve()),
                "SNOWFLAKE_AGENT_USER": sf.agent.user, "SNOWFLAKE_AGENT_ROLE": sf.agent.role,
                "SNOWFLAKE_AGENT_KEY_PATH": str(sf.agent.key_path.resolve())}
    return os.environ | env


class McpBackend:
    def __init__(self, settings: Settings, *, command: Sequence[str] | None = None):
        """command replaces `python -m recon.mcp_server` (the failure tests use small stand-in servers)."""
        self.timeout_s = settings.copilot_mcp_timeout_s
        self.hide_reason_code = settings.copilot_hide_reason_code  # enforced by the server; recorded here
        command = list(command or [sys.executable, "-m", "recon.mcp_server"])
        self.params = StdioServerParameters(command=command[0], args=command[1:], env=server_env(settings))
        self._portal_cm = self._client_cm = None
        self._portal = self._client = None
        self._tools: list[dict[str, Any]] = []
        self._as_of: str | None = None
        self._dead: str | None = None  # why the server can't be used; set once, then every call fails fast
        self._stderr = None

    # --- lifecycle -----------------------------------------------------------

    def start(self) -> None:
        """Start the server and connect. Never raises: a failure is kept, and every later call reports it."""
        if self._client is not None or self._dead is not None:
            return
        try:
            self._portal_cm = start_blocking_portal()
            self._portal = self._portal_cm.__enter__()
            # The session's task group must enter and exit in one task, so the portal hosts it in its own.
            # Its read timeout bounds the handshake; each call below has its own deadline as well.
            # The server's stderr goes to a file, so a refusal to start can be reported with its reason.
            self._stderr = tempfile.TemporaryFile("w+", encoding="utf-8")
            self._client_cm = self._portal.wrap_async_context_manager(
                Client(stdio_client(self.params, errlog=self._stderr), read_timeout_seconds=self.timeout_s))
            self._client = self._client_cm.__enter__()
            listed = self._portal.call(self._timed, self._client.list_tools)
            self._tools = [{"name": t.name, "description": t.description, "input_schema": t.input_schema}
                           for t in listed.tools]
        except BaseException as err:  # noqa: BLE001 - includes anyio's cancellation on timeout
            said = self._server_said()
            self._fail(f"the MCP server did not start: {said or _describe(err)}")

    def _server_said(self) -> str:
        """The last line the server wrote to stderr, if any."""
        if self._stderr is None:
            return ""
        try:
            self._stderr.seek(0)
            lines = [line.strip() for line in self._stderr.read().splitlines() if line.strip()]
        except (OSError, ValueError):
            return ""
        return lines[-1] if lines else ""

    def close(self) -> None:
        client_cm, portal_cm = self._client_cm, self._portal_cm
        self._client = self._client_cm = self._portal = self._portal_cm = None
        self._dead = self._dead or "closed"
        for cm in (client_cm, portal_cm):
            if cm is not None:
                try:
                    cm.__exit__(None, None, None)
                except BaseException:  # noqa: BLE001, S110 - a server that already died can't be shut down cleanly
                    pass
        if self._stderr is not None:
            self._stderr.close()
            self._stderr = None

    def _fail(self, reason: str) -> None:
        self.close()
        self._dead = reason

    async def _timed(self, fn, *args, **kwargs):
        with anyio.fail_after(self.timeout_s):
            return await fn(*args, **kwargs)

    def _request(self, method: str, *args, **kwargs):
        if self._client is None and self._dead is None:
            self.start()
        if self._dead is not None:
            raise BackendUnavailable(self._dead)
        try:
            return self._portal.call(self._timed, getattr(self._client, method), *args, **kwargs)
        except TimeoutError:
            # The session may still be waiting on that answer; don't reuse it.
            self._fail(f"no answer from the MCP server within {self.timeout_s:g}s")
        except BaseException as err:  # noqa: BLE001 - a dead subprocess shows up as many exception types
            said = self._server_said()
            self._fail(f"the MCP server stopped answering: {_describe(err)}" + (f" ({said})" if said else ""))
        raise BackendUnavailable(self._dead)

    # --- ToolBackend ---------------------------------------------------------

    def list_tools(self) -> list[dict[str, Any]]:
        if self._client is None and self._dead is None:
            self.start()
        if self._dead is not None:
            # Still a usable tool list, so the run can start and then fall back with the real reason.
            from recon.copilot.tools import tool_definitions

            return tool_definitions()
        return self._tools

    def call_tool(self, name: str, arguments: Any) -> ToolResult:
        if not isinstance(arguments, dict):
            # MCP only carries an object; say so the way in-process validation would.
            return ToolResult(error="invalid arguments: : Input should be a valid dictionary")
        result = self._request("call_tool", name, arguments)
        text = "".join(getattr(block, "text", "") for block in result.content)
        if result.is_error:
            if (result.meta or {}).get(ERROR_KIND) == TOOL_ERROR:
                return ToolResult(error=text)
            raise BackendUnavailable(f"the MCP server reported: {text}")
        try:
            return _parse(text)
        except ValueError as err:
            raise BackendUnavailable(f"malformed result from {name}: {err}") from None

    def exception_row(self, exception_id: str) -> dict[str, Any] | None:
        result = self.call_tool("get_exception", {"exception_id": exception_id})
        if result.error is not None:
            return None
        row = dict(result.records[0])
        row.pop("record_id")
        return row

    def as_of(self) -> str:
        if self._as_of is None:
            result = self._request("read_resource", AS_OF_URI)
            try:
                self._as_of = result.contents[0].text
            except (AttributeError, IndexError):
                raise BackendUnavailable(f"malformed {AS_OF_URI} resource") from None
        return self._as_of


def _parse(text: str) -> ToolResult:
    """The server's JSON, checked: an object with a records list of objects carrying a record_id."""
    data = json.loads(text)  # a JSONDecodeError is a ValueError
    if not isinstance(data, dict) or set(data) != {"records", "truncated"}:
        raise ValueError("expected an object with records and truncated")
    records, truncated = data["records"], data["truncated"]
    if not isinstance(records, list) or not isinstance(truncated, bool) or not all(
            isinstance(r, dict) and isinstance(r.get("record_id"), str) for r in records):
        raise ValueError("records must be objects with a record_id")
    return ToolResult(records=records, truncated=truncated)


def _describe(err: BaseException) -> str:
    if isinstance(err, BaseExceptionGroup):
        return "; ".join(_describe(e) for e in err.exceptions)
    return f"{type(err).__name__}: {err}" if str(err) else type(err).__name__
