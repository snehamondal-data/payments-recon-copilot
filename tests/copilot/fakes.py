"""A scripted stand-in for the Anthropic client: no network, no cost, fully deterministic."""

from __future__ import annotations

import itertools
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from typing import Any

_ids = itertools.count(1)


@dataclass
class Block:
    type: str
    id: str | None = None
    name: str | None = None
    input: Any = None
    text: str | None = None


@dataclass
class Usage:
    input_tokens: int = 100
    output_tokens: int = 40


@dataclass
class FakeResponse:
    content: list[Block]
    stop_reason: str = "tool_use"
    model: str = "fake-model"
    usage: Usage = field(default_factory=Usage)

    def to_dict(self) -> dict[str, Any]:
        return {"content": [asdict(b) for b in self.content], "stop_reason": self.stop_reason, "model": self.model}


def tool_use(name: str, **arguments: Any) -> Block:
    return Block(type="tool_use", id=f"toolu_{next(_ids):04d}", name=name, input=arguments)


def calls(*blocks: Block) -> FakeResponse:
    """One assistant turn with one or more (parallel) tool calls."""
    return FakeResponse(list(blocks))


def call(name: str, **arguments: Any) -> FakeResponse:
    return calls(tool_use(name, **arguments))


def submit(**diagnosis: Any) -> FakeResponse:
    return call("submit_diagnosis", **diagnosis)


def text(message: str, stop_reason: str = "end_turn") -> FakeResponse:
    return FakeResponse([Block(type="text", text=message)], stop_reason=stop_reason)


Step = FakeResponse | Exception | Callable[[list[dict[str, Any]]], FakeResponse]


class FakeModelClient:
    """Returns the scripted steps in order. A step may be a response, an exception to raise, or a function
    of the conversation so far. Once the script runs out it answers in plain text, like a model that stops."""

    def __init__(self, script: list[Step], model: str = "fake-model"):
        self.model = model
        self.script = list(script)
        self.requests: list[dict[str, Any]] = []

    def create(self, *, system, messages, tools, max_tokens, timeout):
        self.requests.append({"system": system, "messages": list(messages), "tools": tools, "timeout": timeout})
        if not self.script:
            return text("I have nothing more to add.")
        step = self.script.pop(0)
        if isinstance(step, Exception):
            raise step
        return step(messages) if callable(step) else step


def last_tool_results(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The tool_result blocks the agent sent back in its most recent user turn."""
    return [b for b in messages[-1]["content"] if isinstance(b, dict) and b.get("type") == "tool_result"]
