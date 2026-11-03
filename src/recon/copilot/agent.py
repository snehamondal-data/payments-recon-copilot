"""The copilot's tool-use loop: budgeted, time-limited, validated, and with a deterministic fallback.

The loop owns every guarantee the safety contract needs, so a misbehaving model can only ever end in
a valid diagnosis or in the fallback (escalate to a human). It never raises for model or tool trouble.
"""

from __future__ import annotations

import copy
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from recon.copilot.contract import (
    LOW_CONFIDENCE_THRESHOLD,
    Diagnosis,
    ProposedAction,
    RootCause,
    validate_submission,
)
from recon.copilot.model_client import ModelClient
from recon.copilot.prompts import PROMPT_VERSION, first_message, system_prompt
from recon.copilot.tools import ReadOnlyWarehouse, ToolCall, ToolError, ToolSession, tool_definitions

SUBMIT = "submit_diagnosis"
EXTRA_TURNS = 4  # room for one retry and one "budget spent" turn beyond the tool budget


def _inline_refs(schema: dict[str, Any]) -> dict[str, Any]:
    """Inline pydantic's $defs so the tool schema is a single self-contained object."""
    defs = schema.pop("$defs", {})

    def resolve(node: Any) -> Any:
        if isinstance(node, dict):
            if "$ref" in node:
                return resolve(copy.deepcopy(defs[node["$ref"].split("/")[-1]]))
            return {k: resolve(v) for k, v in node.items()}
        if isinstance(node, list):
            return [resolve(v) for v in node]
        return node

    return resolve(schema)


def submit_tool_definition() -> dict[str, Any]:
    return {
        "name": SUBMIT,
        "description": "Submit your final diagnosis. Call it exactly once, at the end of the investigation.",
        "input_schema": _inline_refs(Diagnosis.model_json_schema()),
    }


@dataclass
class AgentConfig:
    max_tool_calls: int = 8
    timeout_s: float = 90.0
    max_tokens: int = 16000


@dataclass
class RunResult:
    run_id: str
    exception_id: str
    model: str
    diagnosis: Diagnosis
    is_fallback: bool
    fallback_reason: str | None
    policy_override: dict[str, Any] | None
    hide_reason_code: bool
    prompt_version: str
    started_at: datetime
    finished_at: datetime
    tool_calls: list[ToolCall] = field(default_factory=list)
    model_outputs: list[dict[str, Any]] = field(default_factory=list)
    validation_failures: list[list[str]] = field(default_factory=list)
    served_models: list[str] = field(default_factory=list)
    input_tokens: int = 0            # uncached input only
    output_tokens: int = 0           # includes thinking
    cache_creation_input_tokens: int = 0
    cache_read_input_tokens: int = 0

    @property
    def latency_s(self) -> float:
        return (self.finished_at - self.started_at).total_seconds()


class Copilot:
    def __init__(self, client: ModelClient, warehouse: ReadOnlyWarehouse, config: AgentConfig | None = None,
                 *, clock: Callable[[], float] = time.monotonic):
        self.client = client
        self.warehouse = warehouse
        self.config = config or AgentConfig()
        self.clock = clock
        self.tools = [*tool_definitions(), submit_tool_definition()]

    def diagnose(self, exception_id: str) -> RunResult:
        row = self.warehouse.exception_row(exception_id)
        if row is None:
            raise KeyError(f"no exception {exception_id} in the queue")
        run = RunResult(
            run_id=f"run_{uuid.uuid4().hex[:16]}", exception_id=exception_id, model=self.client.model,
            diagnosis=None, is_fallback=False, fallback_reason=None, policy_override=None,  # type: ignore[arg-type]
            hide_reason_code=self.warehouse.hide_reason_code, prompt_version=PROMPT_VERSION,
            started_at=datetime.now(timezone.utc), finished_at=datetime.now(timezone.utc))
        session = ToolSession(self.warehouse)
        run.tool_calls = session.calls
        try:
            self._loop(run, row, session)
        except Exception as err:  # noqa: BLE001 - nothing the model or a tool does may crash a run
            self._fallback(run, row, f"unexpected error: {type(err).__name__}: {err}")
        run.finished_at = datetime.now(timezone.utc)
        return run

    def _loop(self, run: RunResult, row: dict[str, Any], session: ToolSession) -> None:
        cfg = self.config
        deadline = self.clock() + cfg.timeout_s
        system = system_prompt(as_of=self.warehouse.as_of(), max_tool_calls=cfg.max_tool_calls)
        messages: list[dict[str, Any]] = [{"role": "user", "content": first_message(run.exception_id)}]
        tool_calls_used = invalid = over_budget_turns = 0

        for _ in range(cfg.max_tool_calls + EXTRA_TURNS):
            remaining = deadline - self.clock()
            if remaining <= 0:
                return self._fallback(run, row, f"timeout after {cfg.timeout_s}s")
            try:
                response = self.client.create(system=system, messages=messages, tools=self.tools,
                                              max_tokens=cfg.max_tokens, timeout=remaining)
            except Exception as err:  # noqa: BLE001 - API errors, timeouts, connection errors
                return self._fallback(run, row, f"model call failed: {type(err).__name__}: {err}")
            self._record_output(run, response)
            if self.clock() > deadline:
                return self._fallback(run, row, f"timeout after {cfg.timeout_s}s")
            if response.stop_reason == "refusal":
                return self._fallback(run, row, "the model declined the request (refusal)")
            if response.stop_reason == "max_tokens":
                return self._fallback(run, row, "the response hit max_tokens")

            # Append the whole content (thinking blocks included) unchanged, as the API requires.
            messages.append({"role": "assistant", "content": response.content})
            if response.stop_reason == "pause_turn":
                continue

            tool_uses = [b for b in response.content if getattr(b, "type", None) == "tool_use"]
            if not tool_uses:
                invalid += 1
                run.validation_failures.append(["ended the turn without calling submit_diagnosis"])
                if invalid > 1:
                    return self._fallback(run, row, "no valid diagnosis after one retry")
                messages.append({"role": "user", "content": f"Finish by calling {SUBMIT}."})
                continue

            # Data tools first, so a submit in the same turn can cite what they just returned.
            tool_uses.sort(key=lambda b: b.name == SUBMIT)
            results, accepted, over_budget = [], None, False
            for block in tool_uses:
                if block.name == SUBMIT:
                    diagnosis, problems = validate_submission(block.input, session.returned_record_ids)
                    if problems:
                        invalid += 1
                        run.validation_failures.append(problems)
                        results.append(_result(block.id, "Invalid diagnosis, fix these and submit again: "
                                               + " | ".join(problems), error=True))
                    else:
                        accepted = diagnosis
                        results.append(_result(block.id, "Diagnosis received."))
                elif tool_calls_used >= cfg.max_tool_calls:
                    over_budget = True
                    results.append(_result(block.id, f"Tool budget of {cfg.max_tool_calls} calls is spent. "
                                           f"Call {SUBMIT} now with the evidence you have.", error=True))
                else:
                    tool_calls_used += 1
                    try:
                        results.append(_result(block.id, session.call(block.name, block.input)))
                    except ToolError as err:
                        results.append(_result(block.id, str(err), error=True))

            if accepted is not None:
                return self._accept(run, accepted)
            if invalid > 1:
                return self._fallback(run, row, "invalid diagnosis after one retry")
            if over_budget:
                over_budget_turns += 1
                if over_budget_turns > 1:
                    return self._fallback(run, row, f"kept calling tools after the budget of {cfg.max_tool_calls}")
            messages.append({"role": "user", "content": results})
        return self._fallback(run, row, "turn limit reached without a diagnosis")

    def _record_output(self, run: RunResult, response: Any) -> None:
        run.model_outputs.append(response.to_dict() if hasattr(response, "to_dict") else {"repr": repr(response)})
        run.served_models.append(getattr(response, "model", self.client.model))
        usage = getattr(response, "usage", None)
        for name in ("input_tokens", "output_tokens", "cache_creation_input_tokens", "cache_read_input_tokens"):
            setattr(run, name, getattr(run, name) + (getattr(usage, name, 0) or 0))

    def _accept(self, run: RunResult, d: Diagnosis) -> None:
        if d.confidence < LOW_CONFIDENCE_THRESHOLD and d.proposed_action != ProposedAction.ESCALATE_TO_HUMAN:
            run.policy_override = {"rule": "low_confidence", "threshold": LOW_CONFIDENCE_THRESHOLD,
                                   "confidence": d.confidence, "original_action": d.proposed_action.value}
            d = d.model_copy(update={"proposed_action": ProposedAction.ESCALATE_TO_HUMAN,
                                     "needs_human_escalation": True})
        run.diagnosis = d

    def _fallback(self, run: RunResult, row: dict[str, Any], reason: str) -> None:
        """Deterministic: the reconciliation reason code, escalated, with zero confidence."""
        run.is_fallback = True
        run.fallback_reason = reason
        run.diagnosis = Diagnosis(
            root_cause=RootCause(row["reason_code"]),
            evidence=[run.exception_id],
            proposed_action=ProposedAction.ESCALATE_TO_HUMAN,
            confidence=0.0,
            needs_human_escalation=True,
            rationale=f"Deterministic fallback ({reason}). A human must investigate.",
        )


def _result(tool_use_id: str, content: str, *, error: bool = False) -> dict[str, Any]:
    block = {"type": "tool_result", "tool_use_id": tool_use_id, "content": content}
    if error:
        block["is_error"] = True
    return block
