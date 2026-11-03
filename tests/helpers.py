"""Shared test helpers (plain functions and fakes; fixtures live in conftest.py)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

from recon.contracts import DLQRecord
from recon.fx import read_fx_csv, write_fx_csv
from recon.generator.ground_truth import write_ground_truth
from recon.generator.scenario import Scenario
from recon.loader.schema import ensure_schema
from recon.loader.sink import RawMessage, Sink, load_fx_rates
from recon.warehouse import DuckDBClient

DBT_DIR = Path(__file__).resolve().parents[1] / "dbt"
DBT = Path(sys.executable).parent / "dbt"


class FakeDLQ:
    def __init__(self, events: list[str] | None = None):
        self.sent: list[DLQRecord] = []
        self.events = events if events is not None else []

    def publish(self, record: DLQRecord) -> None:
        self.sent.append(record)

    def flush(self) -> None:
        self.events.append("dlq.flush")


def as_raw_messages(scenario: Scenario, partitions: int = 3) -> list[RawMessage]:
    """Mimic Kafka: messages spread over partitions by key, offsets increasing per partition."""
    next_offset: dict[tuple[str, int], int] = defaultdict(int)
    raws = []
    for m in scenario.messages:
        partition = sum(m.key or b"") % partitions
        offset = next_offset[(m.topic, partition)]
        next_offset[(m.topic, partition)] += 1
        raws.append(RawMessage(m.topic, partition, offset, m.key, m.value))
    return raws


def load_all(sink: Sink, raws: list[RawMessage], batch_size: int = 500):
    return [sink.process(raws[i:i + batch_size]) for i in range(0, len(raws), batch_size)]


def build_recon_warehouse(scenario: Scenario, tmp: Path) -> tuple[Path, Path, dict]:
    """Load the scenario (no Kafka needed) and run `dbt build`. Returns warehouse, ground truth, run results."""
    warehouse_path, ground_truth_path = tmp / "warehouse.duckdb", tmp / "ground_truth.duckdb"
    write_ground_truth(ground_truth_path, scenario)
    write_fx_csv(tmp / "fx.csv", scenario.fx_rates)
    wh = DuckDBClient(warehouse_path)
    ensure_schema(wh)
    load_fx_rates(wh, read_fx_csv(tmp / "fx.csv"))
    load_all(Sink(wh, FakeDLQ()), as_raw_messages(scenario))
    wh.close()
    # A subprocess, like `make recon`: dbt-duckdb keeps its connection open in-process otherwise.
    proc = subprocess.run([
        str(DBT), "build", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR),
        "--target-path", str(tmp / "target"), "--log-path", str(tmp / "logs"),
        "--vars", f"{{as_of: '{scenario.as_of}'}}",
    ], env=os.environ | {"DUCKDB_PATH": str(warehouse_path)}, capture_output=True, text=True)
    assert proc.returncode == 0, f"dbt build failed:\n{proc.stdout[-3000:]}"
    return warehouse_path, ground_truth_path, json.loads((tmp / "target" / "run_results.json").read_text())
