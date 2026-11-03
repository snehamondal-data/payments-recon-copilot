"""Estimated API cost. USD per million tokens, Anthropic list prices as of October 2026.

These are estimates for the report, not a bill: check the Anthropic console for actual spend.
"""

from __future__ import annotations

from dataclasses import dataclass

CACHE_WRITE_MULTIPLIER = 1.25  # 5-minute TTL


@dataclass(frozen=True)
class Price:
    input: float
    output: float
    cache_read: float

    @property
    def cache_write(self) -> float:
        return self.input * CACHE_WRITE_MULTIPLIER


PRICES = {
    "claude-sonnet-5-5": Price(input=2.00, output=10.00, cache_read=0.20),
    "claude-opus-5-5": Price(input=4.00, output=20.00, cache_read=0.20),
}


def cost_usd(model: str, *, input_tokens: int, output_tokens: int, cache_creation_input_tokens: int = 0,
             cache_read_input_tokens: int = 0) -> float:
    p = PRICES.get(model)
    if p is None:
        return 0.0
    return (input_tokens * p.input + output_tokens * p.output + cache_creation_input_tokens * p.cache_write
            + cache_read_input_tokens * p.cache_read) / 1_000_000
