"""Reference FX rates (rate to USD per currency per day), loaded into ref.fx_rates.

The processor converts every non-USD event at the rate for its UTC date, so each
processor fx_rate can be looked up in this table.
"""

from __future__ import annotations

import csv
import random
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

RATE_QUANTUM = Decimal("0.000001")
STARTING_RATES = {"EUR": Decimal("1.085000"), "GBP": Decimal("1.270000")}
CSV_COLUMNS = ("rate_date", "currency", "rate_to_usd")


@dataclass(frozen=True)
class FxRate:
    rate_date: date
    currency: str
    rate_to_usd: Decimal


def generate_fx_rates(rng: random.Random, first_day: date, last_day: date) -> list[FxRate]:
    """A seeded random walk, one row per currency for every calendar day (weekends included)."""
    level = dict(STARTING_RATES)
    rates = []
    day = first_day
    while day <= last_day:
        rates.append(FxRate(day, "USD", Decimal("1.000000")))
        for currency in ("EUR", "GBP"):
            # Integer steps keep the walk exact and platform-independent (+/- 0.4% a day).
            step = Decimal(rng.randint(-40, 40)) / Decimal(10_000)
            level[currency] = (level[currency] * (1 + step)).quantize(RATE_QUANTUM, rounding=ROUND_HALF_UP)
            rates.append(FxRate(day, currency, level[currency]))
        day += timedelta(days=1)
    return rates


def write_fx_csv(path: Path, rates: list[FxRate]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_COLUMNS)
        for r in rates:
            writer.writerow((r.rate_date.isoformat(), r.currency, str(r.rate_to_usd)))


def read_fx_csv(path: Path) -> list[FxRate]:
    with path.open(newline="") as f:
        return [
            FxRate(date.fromisoformat(row["rate_date"]), row["currency"], Decimal(row["rate_to_usd"]))
            for row in csv.DictReader(f)
        ]
