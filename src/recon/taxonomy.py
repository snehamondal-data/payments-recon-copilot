"""The exception types from spec section 3. Slice 3 reuses these as the agent's root_cause enum."""

from __future__ import annotations

from enum import StrEnum


class ExceptionType(StrEnum):
    LATE_SETTLEMENT = "late_settlement"
    MISSING_SETTLEMENT = "missing_settlement"
    DUPLICATE_WEBHOOK = "duplicate_webhook"
    FEE_MISMATCH = "fee_mismatch"
    PARTIAL_REFUND_NOT_IN_LEDGER = "partial_refund_not_in_ledger"
    CURRENCY_ROUNDING = "currency_rounding"
    MISSING_LEDGER_ENTRY = "missing_ledger_entry"
    ORPHAN_BANK_DEPOSIT = "orphan_bank_deposit"
    CHARGEBACK = "chargeback"
    DATE_BOUNDARY = "date_boundary"
    AMBIGUOUS = "ambiguous"
