"""The one small interface the Python side uses to talk to a warehouse.

DuckDBClient and SnowflakeClient implement it. SQL passed to execute() uses ? placeholders on both;
the few statements that differ between engines (idempotent inserts, DDL types) branch on `dialect`.
"""

from __future__ import annotations

from collections.abc import Sequence
from contextlib import AbstractContextManager
from typing import Any, Protocol


class WarehouseClient(Protocol):
    dialect: str  # "duckdb" | "snowflake"

    def execute(self, sql: str, params: Sequence[Any] = ()) -> list[tuple]: ...

    def executemany(self, sql: str, rows: Sequence[Sequence[Any]]) -> None: ...

    def transaction(self) -> AbstractContextManager[None]: ...

    def close(self) -> None: ...
