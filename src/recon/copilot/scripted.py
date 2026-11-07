"""A scripted stand-in for the model: deterministic, no API calls, no cost.

Used by the MCP parity check (recon.mcp_parity) and by scheduled triage in fake mode (recon.triage). It
decides each step from what the tools returned, so a difference in any tool output changes its run.
"""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from typing import Any

from recon.copilot.contract import ALLOWED_ACTIONS, ProposedAction, RootCause


@dataclass
class _Block:
    type: str
    id: str | None = None
    name: str | None = None
    input: Any = None


@dataclass
class _Usage:
    input_tokens: int = 0
    output_tokens: int = 0


@dataclass
class _Response:
    content: list[_Block]
    stop_reason: str = "tool_use"
    model: str = "scripted-investigator"
    usage: _Usage = field(default_factory=_Usage)

    def to_dict(self) -> dict[str, Any]:
        return {"content": [asdict(b) for b in self.content], "stop_reason": self.stop_reason, "model": self.model}


class ScriptedInvestigator:
    """A deterministic stand-in for the model: get_exception, then the source tools for its IDs, then a
    diagnosis citing what came back. For an ambiguous item it claims a fix, so the escalation floor acts."""

    model = "scripted-investigator"

    def create(self, *, system, messages, tools, max_tokens, timeout) -> _Response:
        turn = sum(1 for m in messages if m["role"] == "assistant")
        uses: list[_Block] = []

        def use(name: str, **arguments: Any) -> None:
            uses.append(_Block("tool_use", id=f"toolu_{turn}_{len(uses)}", name=name, input=arguments))

        results = _tool_results(messages)
        if turn == 0:
            use("get_exception", exception_id=re.search(r"(charge|bank_line):\S+_[0-9a-f]{16}",
                                                        messages[0]["content"]).group(0))
        elif turn == 1 and results and results[0][1] is not None:
            row = results[0][1][0]
            if row.get("charge_id"):
                for name in ("get_processor_events", "get_bank_settlement", "get_ledger_entries",
                             "get_related_refunds"):
                    use(name, txn_id=row["charge_id"])
            if row.get("bank_line_id"):
                use("get_bank_settlement", txn_id=row["bank_line_id"])
            use("get_fx_rate", date=row["item_date"], currency="EUR")
        else:
            row = results[0][1][0] if results and results[0][1] else {}
            seen = [r["record_id"] for _, records in results if records for r in records]
            cause = RootCause(row.get("reason_code") or RootCause.AMBIGUOUS)
            if cause == RootCause.AMBIGUOUS:
                cause = RootCause.MISSING_SETTLEMENT  # a confident wrong answer the floor must stop
            action = min(a for a in ALLOWED_ACTIONS[cause] if a != ProposedAction.ESCALATE_TO_HUMAN)
            errors = sum(1 for _, records in results if records is None)
            use("submit_diagnosis", root_cause=cause.value, evidence=sorted(set(seen))[:8],
                proposed_action=action.value, confidence=0.8, needs_human_escalation=False,
                rationale=f"Scripted: {len(seen)} records from {len(results)} tool calls, {errors} errors.")
        return _Response(uses)


def _tool_results(messages: list[dict[str, Any]]) -> list[tuple[str, list[dict[str, Any]] | None]]:
    """(content, records or None for an error) for every tool result so far, in order."""
    out = []
    for m in messages:
        if m["role"] != "user" or not isinstance(m["content"], list):
            continue
        for block in m["content"]:
            if isinstance(block, dict) and block.get("type") == "tool_result":
                records = None if block.get("is_error") else json.loads(block["content"])["records"]
                out.append((block["content"], records))
    return out
