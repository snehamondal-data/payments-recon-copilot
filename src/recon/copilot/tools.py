"""The six typed, read-only tools from spec section 4.

The agent never writes SQL. Each tool validates its arguments with pydantic, runs one fixed
parameterised SELECT on a read-only connection, and returns records that each carry a record_id.
The same SELECTs run on DuckDB (a read-only, locked-down file connection) or on Snowflake (as
RECON_AGENT_ROLE, which has SELECT on RAW, REF and MARTS and nothing else).
Record text (bank references, descriptions) is returned as data; the system prompt tells the model
never to treat it as instructions, and the safety contract is enforced in code either way.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Literal

import duckdb
from pydantic import BaseModel, ConfigDict, Field

from recon.config import SnowflakeSettings
from recon.warehouse.values import json_value

if TYPE_CHECKING:
    from recon.copilot.backend import ToolBackend

MAX_ROWS = 50
ChargeId = Annotated[str, Field(pattern=r"^ch_[0-9a-f]{16}$", description="A processor charge ID, e.g. ch_0123456789abcdef.")]


class ToolError(Exception):
    """Returned to the model as an is_error tool result. Never crashes the run."""


def open_read_only(path: Path) -> duckdb.DuckDBPyConnection:
    """Read-only, no file or network access beyond this database, and the settings can't be changed back."""
    con = duckdb.connect(str(path), read_only=True)
    # These settings live on the database instance, which DuckDB shares between connections to the same
    # file in one process. If an earlier connection already locked it, check the lockdown instead of redoing it.
    external, locked = con.execute("SELECT current_setting('enable_external_access'), "
                                   "current_setting('lock_configuration')").fetchone()
    if not locked:
        con.execute("SET enable_external_access = false")
        con.execute("SET lock_configuration = true")
    elif external:
        con.close()
        raise RuntimeError(f"{path} is already open in this process without the read-only lockdown")
    return con


# --- tool arguments ----------------------------------------------------------

class _Args(BaseModel):
    model_config = ConfigDict(extra="forbid")


class GetExceptionArgs(_Args):
    exception_id: Annotated[str, Field(pattern=r"^(charge:ch|bank_line:bln)_[0-9a-f]{16}$",
                                       description="e.g. charge:ch_0123456789abcdef or bank_line:bln_0123456789abcdef")]


class ChargeArgs(_Args):
    txn_id: ChargeId


class BankArgs(_Args):
    txn_id: Annotated[str, Field(pattern=r"^(ch|bln)_[0-9a-f]{16}$",
                                 description="A charge ID (returns bank lines referencing it) or a bank line ID.")]


class FxArgs(_Args):
    date: date
    currency: Literal["USD", "EUR", "GBP"]


@dataclass(frozen=True)
class ToolSpec:
    name: str
    description: str
    args: type[_Args]


TOOL_SPECS = (
    ToolSpec("get_exception", "The exception from the reconciliation queue: item, amount at stake (USD cents), "
             "dates, age in business days, and related charge IDs for an ambiguous deposit.", GetExceptionArgs),
    ToolSpec("get_processor_events", "Every processor webhook delivery for a charge: charge, fee, refund and "
             "dispute events, with amounts in minor units, FX rate and USD settlement amount.", ChargeArgs),
    ToolSpec("get_bank_settlement", "Bank statement lines for a charge (by reference) or one bank line by its ID, "
             "with value date, direction and net amount in USD cents.", BankArgs),
    ToolSpec("get_ledger_entries", "Ledger journals for a charge, each with its double-entry lines in USD cents "
             "and its local posting date.", ChargeArgs),
    ToolSpec("get_related_refunds", "Refunds for a charge as seen by all three sources: processor refund events, "
             "bank REFUND debits and ledger refund journals.", ChargeArgs),
    ToolSpec("get_fx_rate", "The reference rate to USD for a currency on a date (ref.fx_rates).", FxArgs),
)


def tool_definitions() -> list[dict[str, Any]]:
    return [{"name": s.name, "description": s.description, "input_schema": s.args.model_json_schema()}
            for s in TOOL_SPECS]


# --- the warehouse -------------------------------------------------------------

_jsonable = json_value  # UTC ISO timestamps, ISO dates, decimals as strings
EXCEPTIONS_DAILY = "marts.recon_exceptions_daily"  # each as-of date's exception rows (recon.orchestration)


@dataclass
class ToolCall:
    name: str
    arguments: dict[str, Any]
    record_ids: list[str] = field(default_factory=list)
    error: str | None = None
    duration_ms: float = 0.0


class ReadOnlyWarehouse:
    """The only way the copilot reads data. Opens the warehouse read-only and nothing else."""

    def __init__(self, path: Path, *, forbidden_paths: tuple[Path, ...] = (), hide_reason_code: bool = False,
                 as_of: date | None = None):
        """as_of pins the exception queue to one date's snapshot (marts.recon_exceptions_daily) instead of the
        live marts, which hold only the last date built. Scheduled triage uses it; the tools over raw data
        are the same either way."""
        path = Path(path)
        for forbidden in forbidden_paths:
            if path.resolve() == Path(forbidden).resolve():
                raise ValueError(f"refusing to open {path}: it is not the warehouse")
        self.con = open_read_only(path)
        self.hide_reason_code = hide_reason_code
        self.engine = "duckdb"
        self.pinned_as_of = None if as_of is None else date.fromisoformat(str(as_of))

    @classmethod
    def snowflake(cls, sf: SnowflakeSettings, *, hide_reason_code: bool = False,
                  as_of: date | None = None) -> ReadOnlyWarehouse:
        """Logs in as the agent user, never the loader: connect() refuses a session without RECON_AGENT_ROLE."""
        from recon.warehouse.snowflake_client import connect

        self = cls.__new__(cls)
        self.con = connect(sf, sf.agent)
        self.hide_reason_code = hide_reason_code
        self.engine = "snowflake"
        self.pinned_as_of = None if as_of is None else date.fromisoformat(str(as_of))
        return self

    def close(self) -> None:
        self.con.close()

    def session_identity(self) -> dict[str, Any]:
        """Who this connection is: for the MCP server's access log, so it records what it ran as."""
        if self.engine == "snowflake":
            user, role, secondary = self._query(
                "SELECT CURRENT_USER() AS u, CURRENT_ROLE() AS r, CURRENT_SECONDARY_ROLES() AS s")[0].values()
            return {"engine": "snowflake", "user": user, "role": role, "secondary_roles": json.loads(secondary)}
        read_only = self._query("SELECT current_setting('access_mode') AS m")[0]["m"]
        return {"engine": "duckdb", "access_mode": read_only, "pinned_as_of": self.as_of() if self.pinned_as_of else None}

    def _query(self, sql: str, params: tuple = (), limit: int | None = None) -> list[dict[str, Any]]:
        if self.engine == "duckdb":
            cur = self.con.execute(sql, params)
        else:
            from recon.warehouse.snowflake_client import to_pyformat

            cur = self.con.cursor()
            if params:
                cur.execute(to_pyformat(sql), list(params))
            else:
                cur.execute(sql)
        names = [d[0].lower() for d in cur.description]  # Snowflake returns unquoted names in upper case
        rows = cur.fetchmany(limit) if limit else cur.fetchall()
        return [{n: _jsonable(v) for n, v in zip(names, row)} for row in rows]

    def _rows(self, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
        return self._query(sql, params, MAX_ROWS)

    # Internal lookups for the agent loop and the CLI. Never exposed to the model.

    def exception_row(self, exception_id: str) -> dict[str, Any] | None:
        if self.pinned_as_of is not None:
            rows = self._rows(f"SELECT row_json FROM {EXCEPTIONS_DAILY} WHERE as_of_date = ? AND exception_id = ?",
                              (self.pinned_as_of, exception_id))
            return json.loads(rows[0]["row_json"]) if rows else None
        rows = self._rows("SELECT * FROM marts.recon_exceptions WHERE exception_id = ?", (exception_id,))
        return rows[0] if rows else None

    def queue(self) -> list[dict[str, Any]]:
        if self.pinned_as_of is not None:
            rows = [json.loads(r["row_json"]) for r in self._query(
                f"SELECT row_json FROM {EXCEPTIONS_DAILY} WHERE as_of_date = ?", (self.pinned_as_of,))]
            return sorted(rows, key=lambda r: (r["item_date"], r["exception_id"]))
        return self._query("SELECT * FROM marts.recon_exceptions ORDER BY item_date, exception_id")

    def as_of(self) -> str:
        if self.pinned_as_of is not None:
            return self.pinned_as_of.isoformat()
        return self._query("SELECT max(as_of_date) AS as_of FROM marts.recon_exceptions")[0]["as_of"]

    # The six tools.

    def get_exception(self, a: GetExceptionArgs) -> list[dict[str, Any]]:
        row = self.exception_row(a.exception_id)
        if row is None:
            raise ToolError(f"no exception {a.exception_id}")
        row = {"record_id": row["exception_id"], **row}
        if self.hide_reason_code:
            row.pop("reason_code")
            row.pop("detected_problems", None)  # it names the reason code too
        return [row]

    def get_processor_events(self, a: ChargeArgs) -> list[dict[str, Any]]:
        return self._rows("""
            SELECT event_id AS record_id, event_type, object_id, charge_id, amount_minor, currency, fx_rate,
                   settlement_amount_minor, occurred_at
            FROM raw.processor_events WHERE charge_id = ? ORDER BY occurred_at, event_id""", (a.txn_id,))

    def get_bank_settlement(self, a: BankArgs) -> list[dict[str, Any]]:
        column = "lines.reference" if a.txn_id.startswith("ch_") else "lines.line_id"
        return self._rows(f"""
            SELECT lines.line_id AS record_id, lines.batch_id, batches.value_date, lines.reference, lines.direction,
                   lines.description, lines.net_amount_minor
            FROM raw.bank_settlement_lines AS lines
            JOIN raw.bank_settlement_batches AS batches ON lines.event_id = batches.event_id
            WHERE {column} = ? ORDER BY batches.value_date, lines.line_id""", (a.txn_id,))

    def get_ledger_entries(self, a: ChargeArgs) -> list[dict[str, Any]]:
        journals = self._rows("""
            SELECT journal_id AS record_id, event_id, charge_id, entry_type, posted_at, posting_date
            FROM raw.ledger_journals WHERE charge_id = ? ORDER BY posted_at, journal_id""", (a.txn_id,))
        for j in journals:
            j["lines"] = self._rows("""
                SELECT line_no, account, debit_minor, credit_minor
                FROM raw.ledger_lines WHERE event_id = ? ORDER BY line_no""", (j.pop("event_id"),))
        return journals

    def get_related_refunds(self, a: ChargeArgs) -> list[dict[str, Any]]:
        processor = self._rows("""
            SELECT event_id AS record_id, 'processor' AS source, object_id AS refund_id, amount_minor, currency,
                   settlement_amount_minor, occurred_at
            FROM raw.processor_events WHERE charge_id = ? AND event_type = 'refund' ORDER BY occurred_at""",
                               (a.txn_id,))
        bank = self._rows("""
            SELECT lines.line_id AS record_id, 'bank' AS source, batches.value_date, lines.net_amount_minor
            FROM raw.bank_settlement_lines AS lines
            JOIN raw.bank_settlement_batches AS batches ON lines.event_id = batches.event_id
            WHERE lines.reference = ? AND lines.description = 'REFUND' ORDER BY batches.value_date""", (a.txn_id,))
        ledger = [j for j in self.get_ledger_entries(a) if j["entry_type"] == "refund"]
        for j in ledger:
            j["source"] = "ledger"
        return processor + bank + ledger

    def get_fx_rate(self, a: FxArgs) -> list[dict[str, Any]]:
        rows = self._rows("SELECT rate_date, currency, rate_to_usd FROM ref.fx_rates WHERE rate_date = ? AND currency = ?",
                          (a.date, a.currency))
        return [{"record_id": f"fx:{r['rate_date']}:{r['currency']}", **r} for r in rows]

    def lookup_record(self, record_id: str) -> dict[str, Any] | None:
        """Fetch a cited record for display in `show`, by the shape of its ID."""
        if record_id.startswith(("charge:", "bank_line:")):
            return self.exception_row(record_id)
        table = {"evt": ("raw.processor_events", "event_id"), "jrn": ("raw.ledger_journals", "journal_id"),
                 "bln": ("raw.bank_settlement_lines", "line_id")}.get(record_id[:3])
        if table is not None:
            rows = self._rows(f"SELECT * FROM {table[0]} WHERE {table[1]} = ?", (record_id,))
            if rows:
                return rows[0]
        if record_id.startswith("fx:"):
            _, day, currency = record_id.split(":")
            rows = self._rows("SELECT * FROM ref.fx_rates WHERE rate_date = ? AND currency = ?", (day, currency))
            return rows[0] if rows else None
        return None


# --- one investigation's view of the tools ------------------------------------

class ToolSession:
    """Runs tool calls for one investigation and remembers every record_id it handed out.
    The tools themselves run in a ToolBackend (recon.copilot.backend); a warehouse is wrapped in the
    in-process one. The evidence check reads returned_record_ids, so it is the same for every backend."""

    def __init__(self, tools: ToolBackend | ReadOnlyWarehouse):
        if isinstance(tools, ReadOnlyWarehouse):
            from recon.copilot.backend import InProcessBackend  # backend.py imports this module

            tools = InProcessBackend(tools)
        self.backend = tools
        self.returned_record_ids: set[str] = set()
        self.calls: list[ToolCall] = []

    def call(self, name: str, raw_args: Any) -> str:
        """Returns the JSON tool result. Raises ToolError for anything the model should be told about;
        a backend failure is recorded on the call and raised as it is, for the agent loop to fall back on."""
        call = ToolCall(name, raw_args if isinstance(raw_args, dict) else {"_raw": raw_args})
        self.calls.append(call)
        started = time.monotonic()
        try:
            result = self.backend.call_tool(name, raw_args)
        except Exception as err:
            call.error = f"backend unavailable: {err}"
            raise
        finally:
            call.duration_ms = round((time.monotonic() - started) * 1000, 1)
        if result.error is not None:
            call.error = result.error
            raise ToolError(result.error)
        call.record_ids = [r["record_id"] for r in result.records]
        self.returned_record_ids.update(call.record_ids)
        return result.to_json()
