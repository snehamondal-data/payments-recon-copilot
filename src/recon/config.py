"""Runtime settings, read from environment variables (see .env.example)."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path

SOURCE_TOPICS = ("processor.events", "bank.settlements", "ledger.entries")
DLQ_TOPIC = "dlq"


def _flag(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class SnowflakeLogin:
    user: str
    role: str
    key_path: Path  # unencrypted PKCS8 private key, in the git-ignored secrets/ folder


@dataclass(frozen=True)
class SnowflakeSettings:
    """Two logins: the loader (writes RAW, REF, dbt schemas and COPILOT) and the agent (SELECT only)."""

    account: str
    warehouse: str
    database: str
    loader: SnowflakeLogin
    agent: SnowflakeLogin

    def __post_init__(self) -> None:
        if self.loader.user.upper() == self.agent.user.upper() or self.loader.role.upper() == self.agent.role.upper():
            raise ValueError("the loader and the agent must be different Snowflake users with different roles")


WAREHOUSE_ENGINES = ("duckdb", "snowflake")


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str
    topic_prefix: str
    duckdb_path: Path
    ground_truth_path: Path
    fx_rates_path: Path
    seed: int
    as_of: date
    # Copilot (Slice 3)
    copilot_db_path: Path = Path("data/copilot.duckdb")
    recon_model: str = "claude-sonnet-5-5"
    copilot_max_tool_calls: int = 8
    copilot_timeout_s: float = 90.0
    copilot_hide_reason_code: bool = False
    copilot_effort: str = "medium"
    copilot_tool_backend: str = "inprocess"  # Slice 9: inprocess or mcp
    mcp_access_log: Path = Path("data/mcp_access.jsonl")  # the MCP server's own record of every call
    copilot_mcp_timeout_s: float = 30.0  # per MCP call, and for the server to start
    # Pin the exception queue to one as-of date's snapshot (scheduled triage); None reads the live marts.
    copilot_as_of: date | None = None
    # Scheduled triage (Slice 9d)
    triage_max_exceptions: int = 10
    triage_max_cost_usd: float = 0.50
    triage_model_mode: str = "fake"  # fake (scripted, no API calls) | live
    # Semantic layer (Slice 8): written by dbt build (make recon).
    semantic_manifest_path: Path = Path("dbt/target/semantic_manifest.json")
    # Snowflake (Slice 5). The engine picks where the loader writes and where the copilot reads.
    warehouse_engine: str = "duckdb"
    snowflake: SnowflakeSettings | None = None

    def __post_init__(self) -> None:
        if self.warehouse_engine not in WAREHOUSE_ENGINES:
            raise ValueError(f"WAREHOUSE_ENGINE must be one of {WAREHOUSE_ENGINES}, not {self.warehouse_engine!r}")
        # The warehouse must never be the ground-truth file, or the agent could read the answers.
        files = {"DUCKDB_PATH": self.duckdb_path, "GROUND_TRUTH_PATH": self.ground_truth_path,
                 "COPILOT_DB_PATH": self.copilot_db_path}
        resolved = {name: path.resolve() for name, path in files.items()}
        if len(set(resolved.values())) != len(resolved):
            raise ValueError(f"{', '.join(files)} must be three different files")

    def agent_forbidden_paths(self) -> tuple[Path, ...]:
        """Files the copilot's warehouse connection must refuse to open: the answers, and its own audit store."""
        return (self.ground_truth_path, self.copilot_db_path)

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
        copilot_db_path=Path(env.get("COPILOT_DB_PATH", "data/copilot.duckdb")),
        recon_model=env.get("RECON_MODEL", "claude-sonnet-5-5"),
        copilot_max_tool_calls=int(env.get("COPILOT_MAX_TOOL_CALLS", "8")),
        copilot_timeout_s=float(env.get("COPILOT_TIMEOUT_S", "90")),
        copilot_hide_reason_code=_flag(env.get("COPILOT_HIDE_REASON_CODE", "false")),
        copilot_effort=env.get("COPILOT_EFFORT", "medium"),
        copilot_tool_backend=env.get("COPILOT_TOOL_BACKEND", "inprocess"),
        mcp_access_log=Path(env.get("MCP_ACCESS_LOG", "data/mcp_access.jsonl")),
        copilot_mcp_timeout_s=float(env.get("COPILOT_MCP_TIMEOUT_S", "30")),
        copilot_as_of=date.fromisoformat(env["COPILOT_AS_OF"]) if env.get("COPILOT_AS_OF") else None,
        triage_max_exceptions=int(env.get("TRIAGE_MAX_EXCEPTIONS", "10")),
        triage_max_cost_usd=float(env.get("TRIAGE_MAX_COST_USD", "0.50")),
        triage_model_mode=env.get("TRIAGE_MODEL_MODE", "fake"),
        semantic_manifest_path=Path(env.get("SEMANTIC_MANIFEST_PATH", "dbt/target/semantic_manifest.json")),
        warehouse_engine=env.get("WAREHOUSE_ENGINE", "duckdb"),
        snowflake=SnowflakeSettings(
            account=env.get("SNOWFLAKE_ACCOUNT", ""),
            warehouse=env.get("SNOWFLAKE_WAREHOUSE", "RECON_WH"),
            database=env.get("SNOWFLAKE_DATABASE", "RECON"),
            loader=SnowflakeLogin(env.get("SNOWFLAKE_LOADER_USER", "RECON_LOADER"),
                                  env.get("SNOWFLAKE_LOADER_ROLE", "RECON_LOADER_ROLE"),
                                  Path(env.get("SNOWFLAKE_LOADER_KEY_PATH", "secrets/recon_loader_rsa_key.p8"))),
            agent=SnowflakeLogin(env.get("SNOWFLAKE_AGENT_USER", "RECON_AGENT"),
                                 env.get("SNOWFLAKE_AGENT_ROLE", "RECON_AGENT_ROLE"),
                                 Path(env.get("SNOWFLAKE_AGENT_KEY_PATH", "secrets/recon_agent_rsa_key.p8"))),
        ),
    )
