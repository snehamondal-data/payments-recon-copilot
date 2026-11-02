"""The one small interface the Python side uses to talk to a warehouse.

DuckDB implements it now; Snowflake gets a second implementation in Slice 5.
"""

from __future__ import annotations

from collections.abc import Sequence
from contextlib import AbstractContextManager
from typing import Any, Protocol


class WarehouseClient(Protocol):
    def execute(self, sql: str, params: Sequence[Any] = ()) -> list[tuple]: ...

    def executemany(self, sql: str, rows: Sequence[Sequence[Any]]) -> None: ...

    def transaction(self) -> AbstractContextManager[None]: ...

    def close(self) -> None: ...
