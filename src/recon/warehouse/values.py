"""How warehouse values become JSON: shared by the copilot's tools and the per-date exception snapshot,
so a row reads the same whether it comes from the live marts or from a snapshot."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any


def json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        # Always UTC, never the machine's local zone: the T+2 rule runs on the charge's UTC date.
        # (The ledger's local calendar date is still there, as posting_date.)
        return (value.astimezone(timezone.utc) if value.tzinfo else value).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value
