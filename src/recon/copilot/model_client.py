"""The model behind the copilot. The agent depends on the ModelClient protocol, so tests use a fake."""

from __future__ import annotations

from typing import Any, Protocol

import anthropic

SERVER_FALLBACK_BETA = "server-side-fallback-2026-07-01"


class ModelClient(Protocol):
    model: str

    def create(self, *, system: str, messages: list[dict[str, Any]], tools: list[dict[str, Any]],
               max_tokens: int, timeout: float) -> Any:
        """Return an object shaped like anthropic's Message: .content, .stop_reason, .model, .usage, .to_dict()."""
        ...


class AnthropicModelClient:
    """Claude via the Anthropic SDK. The API key comes from ANTHROPIC_API_KEY (loaded from .env)."""

    def __init__(self, model: str, *, effort: str = "medium", server_fallback: bool = False,
                 api_key: str | None = None):
        # server_fallback is off: a refusal comes back as stop_reason "refusal" and the agent loop turns it
        # into the deterministic escalate fallback, so every run (and every eval) measures exactly one model.
        self.model = model
        self.effort = effort
        self.server_fallback = server_fallback
        # The SDK retries 408/409/429/5xx and connection errors itself (max_retries=2).
        self._client = anthropic.Anthropic(api_key=api_key, max_retries=2)

    def create(self, *, system, messages, tools, max_tokens, timeout):
        client = self._client.with_options(timeout=timeout)
        # Cache breakpoint at the end of the system prompt: tools + system are identical for every turn and
        # every exception, so they are written once and read back at a fraction of the input price.
        cached_system = [{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}]
        kwargs = dict(model=self.model, max_tokens=max_tokens, system=cached_system, messages=messages, tools=tools,
                      output_config={"effort": self.effort})
        if self.server_fallback:
            # If a safety classifier declines, the API re-runs the request on Anthropic's recommended
            # fallback model inside the same call. The model that actually answered is in response.model.
            return client.beta.messages.create(betas=[SERVER_FALLBACK_BETA], fallbacks="default", **kwargs)
        return client.messages.create(**kwargs)
