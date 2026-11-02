"""Money arithmetic. Amounts are integer minor units (cents); rates are Decimals."""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal

FEE_RATE = Decimal("0.029")
FEE_FIXED_MINOR = 30
CHARGEBACK_FEE_MINOR = 1500


def to_minor(amount_minor: int, rate: Decimal) -> int:
    """Convert an amount at `rate`, rounding half up to a whole minor unit."""
    return int((Decimal(amount_minor) * rate).quantize(Decimal(1), rounding=ROUND_HALF_UP))


def processing_fee(gross_usd_minor: int) -> int:
    """Tallyline's (fictional) processor fee schedule: 2.9% + 30 cents."""
    return to_minor(gross_usd_minor, FEE_RATE) + FEE_FIXED_MINOR
