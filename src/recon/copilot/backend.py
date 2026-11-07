"""Where the copilot's tools run (Slice 9a).

The agent loop asks a ToolBackend for the tool list and for each call's result, and doesn't care
whether the tools run in this process or behind an MCP server (9c). The backend is only transport:
validation of the diagnosis, the evidence check, the escalation floor, the budget, the deadline,
fallbacks and the audit log all stay in the agent loop, so every backend gets the same guarantees.

Two kinds of failure, handled differently:
  a tool error (bad arguments, unknown ID)  returned as data; the model sees it and can try again
  a backend failure (crash, database error) raises BackendUnavailable; the run escalates
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

from pydantic import ValidationError

from recon.copilot.tools import MAX_ROWS, TOOL_SPECS, ReadOnlyWarehouse, ToolError, tool_definitions

if TYPE_CHECKING:
    from recon.config import Settings

BACKENDS = ("inprocess", "mcp")


class BackendUnavailable(Exception):
    """The tools can't be reached or crashed. Not the model's fault, so the run falls back to escalation."""


@dataclass(frozen=True)
class ToolResult:
    """Plain, JSON-friendly data: the same shape whichever backend produced it."""

    records: list[dict[str, Any]] = field(default_factory=list)
    truncated: bool = False
    error: str | None = None

    def to_json(self) -> str:
        """The tool result the model sees."""
        return json.dumps({"records": self.records, "truncated": self.truncated}, default=str)


class ToolBackend(Protocol):
    def list_tools(self) -> list[dict[str, Any]]:
        """Each tool's name, description and input_schema."""

    def call_tool(self, name: str, arguments: Any) -> ToolResult:
        """Tool errors come back in ToolResult.error; raises BackendUnavailable for anything else."""

    def start(self) -> None: ...

    def close(self) -> None: ...

    # For the agent loop itself, never offered to the model.
    hide_reason_code: bool

    def exception_row(self, exception_id: str) -> dict[str, Any] | None:
        """The queue row being investigated, or None if there is no such exception."""

    def as_of(self) -> str:
        """The reconciliation as-of date, for the system prompt."""


class InProcessBackend:
    """The six tools in this process, on the read-only warehouse. Owns argument validation."""

    def __init__(self, warehouse: ReadOnlyWarehouse):
        self.warehouse = warehouse
        self.hide_reason_code = warehouse.hide_reason_code
        self._handlers = {s.name: (s.args, getattr(warehouse, s.name)) for s in TOOL_SPECS}

    def list_tools(self) -> list[dict[str, Any]]:
        return tool_definitions()

    def call_tool(self, name: str, arguments: Any) -> ToolResult:
        if name not in self._handlers:
            return ToolResult(error=f"unknown tool {name!r}")
        args_model, handler = self._handlers[name]
        try:
            args = args_model.model_validate(arguments)
        except ValidationError as err:
            return ToolResult(error="invalid arguments: " + "; ".join(
                f"{'.'.join(map(str, e['loc']))}: {e['msg']}"
                for e in err.errors(include_url=False, include_input=False)))
        try:
            records = handler(args)  # already JSON-friendly: UTC ISO timestamps, decimals as strings
        except ToolError as err:
            return ToolResult(error=str(err))
        except Exception as err:  # noqa: BLE001 - a database error is infrastructure, not the model's mistake
            raise BackendUnavailable(f"{name} failed: {type(err).__name__}: {err}") from err
        return ToolResult(records=records, truncated=len(records) >= MAX_ROWS)

    def start(self) -> None:
        """Nothing to start: the warehouse is opened, and closed, by its owner."""

    def close(self) -> None:
        """Nothing to stop."""

    def exception_row(self, exception_id: str) -> dict[str, Any] | None:
        return self.warehouse.exception_row(exception_id)

    def as_of(self) -> str:
        return self.warehouse.as_of()


def make_backend(settings: Settings, warehouse: ReadOnlyWarehouse | None = None) -> ToolBackend:
    """COPILOT_TOOL_BACKEND picks where the tools run: inprocess (default, on the given warehouse) or mcp
    (the server in a subprocess; this process opens no warehouse for the agent)."""
    kind = settings.copilot_tool_backend
    if kind == "inprocess":
        if warehouse is None:
            raise ValueError("the inprocess tool backend needs a warehouse")
        return InProcessBackend(warehouse)
    if kind == "mcp":
        from recon.copilot.mcp_backend import McpBackend

        return McpBackend(settings)
    raise ValueError(f"COPILOT_TOOL_BACKEND must be one of {BACKENDS}, not {kind!r}")
