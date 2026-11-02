"""Runtime settings, read from environment variables (see .env.example)."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path

SOURCE_TOPICS = ("processor.events", "bank.settlements", "ledger.entries")
DLQ_TOPIC = "dlq"


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str
    topic_prefix: str
    duckdb_path: Path
    ground_truth_path: Path
    fx_rates_path: Path
    seed: int
    as_of: date

    def __post_init__(self) -> None:
        # The warehouse must never be the ground-truth file, or the agent could read the answers.
        if self.duckdb_path.resolve() == self.ground_truth_path.resolve():
            raise ValueError("DUCKDB_PATH and GROUND_TRUTH_PATH must be different files")

    def topic(self, logical_name: str) -> str:
        return f"{self.topic_prefix}{logical_name}"


def load_settings(env: Mapping[str, str] = os.environ) -> Settings:
    return Settings(
        kafka_bootstrap=env.get("KAFKA_BOOTSTRAP", "localhost:9092"),
        topic_prefix=env.get("TOPIC_PREFIX", ""),
        duckdb_path=Path(env.get("DUCKDB_PATH", "data/warehouse.duckdb")),
        ground_truth_path=Path(env.get("GROUND_TRUTH_PATH", "data/ground_truth.duckdb")),
        fx_rates_path=Path(env.get("FX_RATES_PATH", "data/ref/fx_rates.csv")),
        seed=int(env.get("SEED", "42")),
        as_of=date.fromisoformat(env.get("AS_OF", "2026-10-15")),
    )
