"""The dbt models must run unchanged on Snowflake (Slice 5).

Dialect-specific SQL is allowed only inside the dispatch macros in dbt/macros/portability.sql.
"""

import re
from pathlib import Path

import pytest

MODELS = Path(__file__).resolve().parents[1] / "dbt" / "models"
TESTS = Path(__file__).resolve().parents[1] / "dbt" / "tests"

# DuckDB-only (or DuckDB-spelled) syntax that has no identical Snowflake form.
DUCKDB_ONLY = {
    "postgres-style cast": r"::",
    "isodow": r"\bisodow\s*\(",
    "timezone()": r"\btimezone\s*\(",
    "list functions": r"\blist_\w+\s*\(",
    "string_agg": r"\bstring_agg\s*\(",
    "strftime": r"\bstrftime\s*\(",
    "date_diff/date_add": r"\bdate_(diff|add|sub)\s*\(",
    "epoch": r"\bepoch\s*\(",
    "struct/unnest": r"\b(struct_pack|unnest)\s*\(",
    "regexp_matches": r"\bregexp_matches\s*\(",
    "generate_series/range": r"\b(generate_series|range)\s*\(",
    "exclude/replace star": r"\*\s*(exclude|replace)\b",
    "positional/group by all": r"\bgroup\s+by\s+all\b",
    "double-quoted string literal": r"=\s*\"",
}

SQL_FILES = sorted([*MODELS.rglob("*.sql"), *TESTS.glob("*.sql")])


def strip_comments(sql: str) -> str:
    sql = re.sub(r"\{#.*?#\}", "", sql, flags=re.S)
    return re.sub(r"--[^\n]*", "", sql)


@pytest.mark.parametrize("path", SQL_FILES, ids=lambda p: p.name)
def test_model_sql_is_portable(path):
    sql = strip_comments(path.read_text()).lower()
    found = [name for name, pattern in DUCKDB_ONLY.items() if re.search(pattern, sql)]
    assert not found, f"{path.name} uses {found}; put it behind a macro in dbt/macros/portability.sql"


def test_every_portability_macro_has_duckdb_and_snowflake_versions():
    text = (MODELS.parent / "macros" / "portability.sql").read_text()
    dispatched = re.findall(r"adapter\.dispatch\('(\w+)'", text)
    assert dispatched
    for name in dispatched:
        assert f"macro duckdb__{name}(" in text and f"macro snowflake__{name}(" in text, name
