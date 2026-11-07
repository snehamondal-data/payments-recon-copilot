from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import duckdb


class DuckDBClient:
    dialect = "duckdb"

    def __init__(self, path: str | Path = ":memory:", *, read_only: bool = False):
        if str(path) != ":memory:" and not read_only:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._con = duckdb.connect(str(path), read_only=read_only)

    def execute(self, sql: str, params: Sequence[Any] = ()) -> list[tuple]:
        result = self._con.execute(sql, params)
        return result.fetchall() if result.description else []

    def executemany(self, sql: str, rows: Sequence[Sequence[Any]]) -> None:
        self._con.executemany(sql, rows)

    @contextmanager
    def transaction(self) -> Iterator[None]:
        self._con.begin()
        try:
            yield
        except BaseException:
            self._con.rollback()
            raise
        self._con.commit()

    def close(self) -> None:
        self._con.close()
