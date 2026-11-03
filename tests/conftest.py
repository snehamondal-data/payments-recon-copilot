from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from helpers import build_recon_warehouse
from recon.generator import build_scenario
from recon.generator.scenario import Scenario
from recon.loader.schema import ensure_schema
from recon.warehouse import DuckDBClient


@pytest.fixture(scope="session")
def scenario() -> Scenario:
    return build_scenario()


@pytest.fixture
def warehouse() -> Iterator[DuckDBClient]:
    wh = DuckDBClient()
    ensure_schema(wh)
    yield wh
    wh.close()


@pytest.fixture(scope="session")
def recon_db(scenario, tmp_path_factory) -> tuple[Path, Path]:
    """A dbt-built seed-42 warehouse and its ground-truth file, shared by the copilot tests."""
    warehouse_path, ground_truth_path, _ = build_recon_warehouse(scenario, tmp_path_factory.mktemp("recon42"))
    return warehouse_path, ground_truth_path
