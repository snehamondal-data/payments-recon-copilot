"""Synthetic data generator for the fictional Tallyline scenario."""

from __future__ import annotations

from datetime import date

from recon.generator.planted import plant_all
from recon.generator.scenario import Scenario, ScenarioBuilder

DEFAULT_TRANSACTIONS = 500
SCENARIOS = ("standard", "hard", "multi")


def build_scenario(seed: int = 42, as_of: date = date(2026, 10, 15),
                   n_transactions: int = DEFAULT_TRANSACTIONS, scenario: str = "standard") -> Scenario:
    """`hard` is the standard scenario plus the two original harder cases planted after it; `multi` is the
    standard scenario plus four "two problems on one charge" cases (used with seed 2027). The standard
    scenario (its data and content hash) is exactly what it was: extra cases only exist when asked for."""
    if scenario not in SCENARIOS:
        raise ValueError(f"scenario must be one of {SCENARIOS}")
    builder = ScenarioBuilder(seed, as_of)
    builder.add_baseline(n_transactions)
    plant_all(builder)
    expectations = []
    if scenario in ("hard", "multi"):
        from recon.generator.hard import HARD_PLANTERS, MULTI_PLANTERS, plant_hard

        expectations = plant_hard(builder, HARD_PLANTERS if scenario == "hard" else MULTI_PLANTERS)
    built = builder.build()
    built.hard_expectations, built.name = expectations, scenario
    return built
