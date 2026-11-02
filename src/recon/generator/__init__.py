"""Synthetic data generator for the fictional Tallyline scenario."""

from __future__ import annotations

from datetime import date

from recon.generator.planted import plant_all
from recon.generator.scenario import Scenario, ScenarioBuilder

DEFAULT_TRANSACTIONS = 500


def build_scenario(seed: int = 42, as_of: date = date(2026, 10, 15),
                   n_transactions: int = DEFAULT_TRANSACTIONS) -> Scenario:
    builder = ScenarioBuilder(seed, as_of)
    builder.add_baseline(n_transactions)
    plant_all(builder)
    return builder.build()
