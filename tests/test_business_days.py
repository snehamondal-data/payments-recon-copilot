from datetime import date

import pytest

from recon.business_days import add_business_days, in_settlement_window, settlement_deadline


@pytest.mark.parametrize(("charged", "deadline"), [
    (date(2026, 10, 13), date(2026, 10, 15)),  # Tue -> Thu
    (date(2026, 10, 15), date(2026, 10, 19)),  # Thu -> Mon (skips the weekend)
    (date(2026, 10, 16), date(2026, 10, 20)),  # Fri -> Tue
    (date(2026, 10, 17), date(2026, 10, 20)),  # Sat -> Tue
    (date(2026, 10, 18), date(2026, 10, 20)),  # Sun -> Tue
])
def test_deadline_is_two_business_days(charged, deadline):
    assert settlement_deadline(charged) == deadline


def test_window_is_inclusive_of_the_deadline():
    assert in_settlement_window(date(2026, 10, 13), as_of=date(2026, 10, 15))
    assert not in_settlement_window(date(2026, 10, 13), as_of=date(2026, 10, 16))


def test_add_zero_business_days_is_identity():
    assert add_business_days(date(2026, 10, 17), 0) == date(2026, 10, 17)
