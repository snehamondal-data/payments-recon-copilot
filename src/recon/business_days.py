"""Settlement calendar.

T+2 means two business days after the charge's UTC date, inclusive: a charge on
Tuesday may settle as late as Thursday and still be on time. Business days are
Monday to Friday. There is no holiday calendar yet.
"""

from __future__ import annotations

from datetime import date, timedelta

SETTLEMENT_WINDOW_BUSINESS_DAYS = 2


def is_business_day(day: date) -> bool:
    return day.weekday() < 5


def add_business_days(day: date, n: int) -> date:
    while n > 0:
        day += timedelta(days=1)
        if is_business_day(day):
            n -= 1
    return day


def settlement_deadline(charge_date: date) -> date:
    """Last day (inclusive) a charge on `charge_date` can settle and still be on time."""
    return add_business_days(charge_date, SETTLEMENT_WINDOW_BUSINESS_DAYS)


def in_settlement_window(charge_date: date, as_of: date) -> bool:
    return as_of <= settlement_deadline(charge_date)
