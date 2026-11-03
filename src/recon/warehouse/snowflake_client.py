"""Snowflake implementation of WarehouseClient (project-scale: one trial account, one XSMALL warehouse).

Key-pair auth only. Each connection logs in as one service user with one role, switches secondary roles
off, and checks that the session holds exactly that role before returning, so the agent can never end up
holding the loader's grants (or any other role's).
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any

import snowflake.connector

from recon.config import SnowflakeLogin, SnowflakeSettings


def to_pyformat(sql: str) -> str:
    """The connector binds client-side with %s. Our SQL uses ? (DuckDB's style) and contains no literal % or ?."""
    return sql.replace("%", "%%").replace("?", "%s")


def connect(sf: SnowflakeSettings, login: SnowflakeLogin, *, schema: str | None = None):
    if not sf.account:
        raise ValueError("SNOWFLAKE_ACCOUNT is not set (see .env.example)")
    con = snowflake.connector.connect(
        account=sf.account, user=login.user, role=login.role, authenticator="SNOWFLAKE_JWT",
        private_key_file=str(login.key_path), warehouse=sf.warehouse, database=sf.database, schema=schema,
        # UTC, so naive timestamps and current_timestamp never pick up the account's default zone.
        session_parameters={"TIMEZONE": "UTC", "QUERY_TAG": "payments-recon-copilot"},
        login_timeout=30, network_timeout=120)
    cur = con.cursor()
    # Users default to DEFAULT_SECONDARY_ROLES = ('ALL'): the session would also hold every other role granted
    # to the user. Turn that off, so the session's privileges are exactly the one role's.
    cur.execute("USE SECONDARY ROLES NONE")
    role, secondary = cur.execute("SELECT CURRENT_ROLE(), CURRENT_SECONDARY_ROLES()").fetchone()
    if role.upper() != login.role.upper() or '"value":""' not in secondary.replace(" ", ""):
        con.close()
        raise RuntimeError(f"connected as {login.user} with role {role} (secondary {secondary}), "
                           f"expected only {login.role}")
    return con


class SnowflakeClient:
    dialect = "snowflake"

    def __init__(self, sf: SnowflakeSettings, login: SnowflakeLogin | None = None, *, schema: str | None = None):
        self._con = connect(sf, login or sf.loader, schema=schema)

    def execute(self, sql: str, params: Sequence[Any] = ()) -> list[tuple]:
        cur = self._con.cursor()
        try:
            if params:
                cur.execute(to_pyformat(sql), list(params))
            else:
                cur.execute(sql)
            return cur.fetchall() if cur.description else []
        finally:
            cur.close()

    def executemany(self, sql: str, rows: Sequence[Sequence[Any]]) -> None:
        cur = self._con.cursor()
        try:
            cur.executemany(to_pyformat(sql), [list(r) for r in rows])
        finally:
            cur.close()

    @contextmanager
    def transaction(self) -> Iterator[None]:
        self.execute("BEGIN")
        try:
            yield
        except BaseException:
            self.execute("ROLLBACK")
            raise
        self.execute("COMMIT")

    def close(self) -> None:
        self._con.close()
