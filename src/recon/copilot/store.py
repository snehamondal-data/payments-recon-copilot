"""The append-only audit log and the resolution records: data/copilot.duckdb, or RECON.COPILOT on Snowflake.

On Snowflake the store writes as the loader user (RECON_LOADER_ROLE). The agent role has no grant on
COPILOT at all, so the agent can neither read its own audit trail nor write a resolution.

Nothing here ever updates or deletes a row. Current state (the latest proposal for an exception and
whether it was reviewed) is derived from the log. Source data lives in the warehouse and is never
touched: approving a proposal writes a resolution record, nothing else.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from recon.config import SnowflakeSettings
from recon.copilot.agent import RunResult
from recon.copilot.contract import ALLOWED_ACTIONS, ProposedAction, RootCause
from recon.warehouse import DuckDBClient, WarehouseClient

DDL = [
    "CREATE SEQUENCE IF NOT EXISTS audit_id_seq",
    """CREATE TABLE IF NOT EXISTS audit_log (
        audit_id BIGINT PRIMARY KEY DEFAULT nextval('audit_id_seq'),
        exception_id VARCHAR NOT NULL,
        run_id VARCHAR,
        event_type VARCHAR NOT NULL,
        model VARCHAR,
        prompt_version VARCHAR,
        payload JSON NOT NULL,
        created_at TIMESTAMPTZ NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS resolutions (
        resolution_id VARCHAR PRIMARY KEY,
        exception_id VARCHAR NOT NULL,
        run_id VARCHAR,
        source VARCHAR NOT NULL,           -- approved_proposal | human_resolution
        root_cause VARCHAR NOT NULL,
        action VARCHAR NOT NULL,
        amount_minor BIGINT,
        record_ids JSON NOT NULL,
        decided_by VARCHAR NOT NULL,
        reason VARCHAR NOT NULL,
        applied_at TIMESTAMPTZ NOT NULL)""",
]
# Snowflake: an ORDER sequence (new accounts default to NOORDER, and the log is read in audit_id order),
# VARIANT for JSON, and its own timestamp type name.
SNOWFLAKE_DDL = [
    DDL[0] + " ORDER",
    DDL[1].replace("DEFAULT nextval('audit_id_seq')", "DEFAULT audit_id_seq.NEXTVAL")
          .replace("JSON", "VARIANT").replace("TIMESTAMPTZ", "TIMESTAMP_TZ"),
    DDL[2].replace("JSON", "VARIANT").replace("TIMESTAMPTZ", "TIMESTAMP_TZ"),
]
AUDIT_COLUMNS = ("audit_id", "exception_id", "run_id", "event_type", "model", "prompt_version", "payload", "created_at")
RESOLUTION_COLUMNS = ("resolution_id", "exception_id", "run_id", "source", "root_cause", "action", "amount_minor",
                      "record_ids", "decided_by", "reason", "applied_at")

EVENT_TYPES = ("run_started", "context_fetch", "tool_call", "model_output", "validation_failed", "policy_override", "diagnosis",
               "review", "resolution_applied")


class ReviewError(Exception):
    """A review action that isn't allowed in the exception's current state."""


@dataclass
class Proposal:
    run_id: str
    exception_id: str
    model: str
    diagnosis: dict[str, Any]
    is_fallback: bool
    fallback_reason: str | None
    policy_override: dict[str, Any] | None
    created_at: datetime
    status: str  # proposed | approved | rejected | resolved
    review: dict[str, Any] | None
    proposed_by: str = "manual"  # or scheduled_triage (Slice 9d); older proposals predate the field
    dag_run_id: str | None = None


def _now() -> datetime:
    return datetime.now(timezone.utc)


class CopilotStore:
    def __init__(self, path: Path | str | WarehouseClient = ":memory:"):
        self.db: WarehouseClient = path if hasattr(path, "dialect") else DuckDBClient(path)
        # A JSON parameter: DuckDB casts the string itself; Snowflake needs PARSE_JSON, which only works in
        # a SELECT (not in VALUES), so every insert below is INSERT ... SELECT.
        self._json = "PARSE_JSON(?)" if self.db.dialect == "snowflake" else "?"
        for statement in SNOWFLAKE_DDL if self.db.dialect == "snowflake" else DDL:
            self.db.execute(statement)

    @classmethod
    def snowflake(cls, sf: SnowflakeSettings) -> CopilotStore:
        from recon.warehouse.snowflake_client import SnowflakeClient

        return cls(SnowflakeClient(sf, sf.loader, schema="COPILOT"))

    def close(self) -> None:
        self.db.close()

    # --- writing (insert only) ----------------------------------------------

    def _append(self, exception_id: str, event_type: str, payload: dict[str, Any], *, run_id: str | None = None,
                model: str | None = None, prompt_version: str | None = None, at: datetime | None = None) -> None:
        assert event_type in EVENT_TYPES, event_type
        self.db.execute(
            "INSERT INTO audit_log (exception_id, run_id, event_type, model, prompt_version, payload, created_at) "
            f"SELECT ?, ?, ?, ?, ?, {self._json}, ?",
            (exception_id, run_id, event_type, model, prompt_version, json.dumps(payload, default=str), at or _now()))

    def record_run(self, run: RunResult) -> None:
        """Write one investigation to the log, in the order it happened."""
        meta = dict(run_id=run.run_id, model=run.model, prompt_version=run.prompt_version)
        eid = run.exception_id
        with self.db.transaction():
            self._append(eid, "run_started", {"hide_reason_code": run.hide_reason_code,
                                              "escalation_floor": run.escalation_floor_active,
                                              "started_at": run.started_at}, at=run.started_at, **meta)
            if run.context_fetch is not None:
                self._append(eid, "context_fetch", run.context_fetch, **meta)
            for seq, call in enumerate(run.tool_calls, start=1):
                self._append(eid, "tool_call", {"seq": seq, "tool": call.name, "arguments": call.arguments,
                                                "returned_record_ids": call.record_ids, "error": call.error,
                                                "duration_ms": call.duration_ms}, **meta)
            for attempt, (output, served) in enumerate(zip(run.model_outputs, run.served_models), start=1):
                self._append(eid, "model_output", {"turn": attempt, "served_by": served, "raw": output}, **meta)
            for problems in run.validation_failures:
                self._append(eid, "validation_failed", {"problems": problems}, **meta)
            if run.policy_override:
                self._append(eid, "policy_override", run.policy_override, **meta)
            self._append(eid, "diagnosis", {
                "diagnosis": run.diagnosis.model_dump(mode="json"),
                "is_fallback": run.is_fallback,
                "fallback_reason": run.fallback_reason,
                "policy_override": run.policy_override,
                "tool_calls": len(run.tool_calls),
                "input_tokens": run.input_tokens,
                "output_tokens": run.output_tokens,
                "cache_creation_input_tokens": run.cache_creation_input_tokens,
                "cache_read_input_tokens": run.cache_read_input_tokens,
                "latency_s": run.latency_s,
                "finished_at": run.finished_at,
                "content_hash": run.content_hash,
                "proposed_by": run.proposed_by,
                "dag_run_id": run.dag_run_id,
            }, at=run.finished_at, **meta)

    def approve(self, exception_id: str, *, reviewer: str, reason: str, amount_minor: int | None) -> str:
        p = self._reviewable(exception_id)
        d = p.diagnosis
        if p.is_fallback or d["proposed_action"] == ProposedAction.ESCALATE_TO_HUMAN:
            raise ReviewError("this proposal escalates to a human: use `resolve --action ...` instead of approve")
        return self._resolve(p, source="approved_proposal", root_cause=d["root_cause"],
                             action=d["proposed_action"], record_ids=d["evidence"], reviewer=reviewer,
                             reason=reason, amount_minor=amount_minor, decision="approved")

    def reject(self, exception_id: str, *, reviewer: str, reason: str) -> None:
        p = self._reviewable(exception_id)
        self._append(exception_id, "review", {"decision": "rejected", "reviewer": reviewer, "reason": reason},
                     run_id=p.run_id, model=p.model)

    def resolve(self, exception_id: str, *, action: str, root_cause: str | None, reviewer: str, reason: str,
                amount_minor: int | None) -> str:
        """A human chooses the resolution, typically for an escalated or rejected proposal."""
        p = self.latest_proposal(exception_id)
        if p is None:
            raise ReviewError(f"{exception_id} has no proposal yet; run diagnose first")
        if p.status in ("approved", "resolved"):
            raise ReviewError(f"{exception_id} is already {p.status}")
        cause = RootCause(root_cause or p.diagnosis["root_cause"])
        act = ProposedAction(action)
        if act == ProposedAction.ESCALATE_TO_HUMAN:
            raise ReviewError("a resolution must be an action, not another escalation")
        if act not in ALLOWED_ACTIONS[cause]:
            allowed = sorted(a.value for a in ALLOWED_ACTIONS[cause] if a != ProposedAction.ESCALATE_TO_HUMAN)
            hint = " (give the real cause with --root-cause)" if not allowed else ""
            raise ReviewError(f"{act.value} is not allowed for root cause {cause.value}; allowed: {allowed}{hint}")
        return self._resolve(p, source="human_resolution", root_cause=cause.value, action=act.value,
                             record_ids=p.diagnosis["evidence"], reviewer=reviewer, reason=reason,
                             amount_minor=amount_minor, decision="resolved")

    def _resolve(self, p: Proposal, *, source, root_cause, action, record_ids, reviewer, reason, amount_minor,
                 decision) -> str:
        resolution_id = f"res_{uuid.uuid4().hex[:16]}"
        at = _now()
        with self.db.transaction():
            self._append(p.exception_id, "review", {"decision": decision, "reviewer": reviewer, "reason": reason,
                                                    "action": action, "root_cause": root_cause},
                         run_id=p.run_id, model=p.model, at=at)
            self.db.execute(
                f"INSERT INTO resolutions ({', '.join(RESOLUTION_COLUMNS)}) "
                f"SELECT ?, ?, ?, ?, ?, ?, ?, {self._json}, ?, ?, ?",
                (resolution_id, p.exception_id, p.run_id, source, root_cause, action, amount_minor,
                 json.dumps(record_ids), reviewer, reason, at))
            self._append(p.exception_id, "resolution_applied", {"resolution_id": resolution_id, "source": source,
                                                                "action": action},
                         run_id=p.run_id, model=p.model, at=at)
        return resolution_id

    def _reviewable(self, exception_id: str) -> Proposal:
        p = self.latest_proposal(exception_id)
        if p is None:
            raise ReviewError(f"{exception_id} has no proposal yet; run diagnose first")
        if p.status != "proposed":
            raise ReviewError(f"{exception_id}'s latest proposal is already {p.status}")
        return p

    # --- reading --------------------------------------------------------------

    def latest_proposal(self, exception_id: str) -> Proposal | None:
        rows = self.db.execute(
            "SELECT run_id, model, payload, created_at FROM audit_log WHERE exception_id = ? "
            "AND event_type = 'diagnosis' ORDER BY audit_id DESC LIMIT 1", (exception_id,))
        if not rows:
            return None
        run_id, model, payload, created_at = rows[0]
        payload = json.loads(payload)
        review_rows = self.db.execute(
            "SELECT payload FROM audit_log WHERE run_id = ? AND event_type = 'review' ORDER BY audit_id DESC LIMIT 1",
            (run_id,))
        review = json.loads(review_rows[0][0]) if review_rows else None
        status = "proposed" if review is None else review["decision"]
        return Proposal(run_id, exception_id, model, payload["diagnosis"], payload["is_fallback"],
                        payload["fallback_reason"], payload["policy_override"], created_at, status, review,
                        payload.get("proposed_by") or "manual", payload.get("dag_run_id"))

    def proposal_keys(self, *, prompt_version: str, model: str) -> set[tuple[str, str]]:
        """(exception_id, content_hash) of every diagnosis this prompt version and model already made,
        whoever ran it. Scheduled triage skips those: re-running a date proposes nothing new. Fallbacks
        are left out: they record an infrastructure failure, not a diagnosis, so they are tried again."""
        rows = self.db.execute("SELECT exception_id, payload FROM audit_log WHERE event_type = 'diagnosis' "
                               "AND prompt_version = ? AND model = ?", (prompt_version, model))
        keys = set()
        for exception_id, payload in rows:
            payload = json.loads(payload)
            content = payload.get("content_hash")
            if content and not payload["is_fallback"]:
                keys.add((exception_id, content))
        return keys

    def log(self) -> list[tuple]:
        """Every event, oldest first: (exception_id, run_id, event_type, payload JSON text, created_at)."""
        return self.db.execute(
            "SELECT exception_id, run_id, event_type, payload, created_at FROM audit_log ORDER BY audit_id")

    def events(self, *, run_id: str | None = None, exception_id: str | None = None) -> list[dict[str, Any]]:
        where, params = ("run_id = ?", (run_id,)) if run_id else ("exception_id = ?", (exception_id,))
        rows = self.db.execute(f"SELECT {', '.join(AUDIT_COLUMNS)} FROM audit_log WHERE {where} ORDER BY audit_id",
                               params)
        return [dict(zip(AUDIT_COLUMNS, r)) | {"payload": json.loads(r[AUDIT_COLUMNS.index("payload")])} for r in rows]

    def resolutions(self) -> list[dict[str, Any]]:
        rows = self.db.execute(f"SELECT {', '.join(RESOLUTION_COLUMNS)} FROM resolutions ORDER BY applied_at")
        return [dict(zip(RESOLUTION_COLUMNS, r)) for r in rows]
