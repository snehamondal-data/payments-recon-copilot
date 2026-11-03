from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass

import duckdb
import pytest

from recon.copilot.tools import ReadOnlyWarehouse


@dataclass
class Case:
    case_id: str
    exception_type: str
    exception_id: str
    charge_ids: list[str]
    record_ids: list[str]


@pytest.fixture
def wh(recon_db) -> Iterator[ReadOnlyWarehouse]:
    warehouse = ReadOnlyWarehouse(recon_db[0], forbidden_paths=(recon_db[1],))
    yield warehouse
    warehouse.close()


@pytest.fixture(scope="session")
def cases(recon_db) -> dict[str, Case]:
    """Planted cases (dev set only) keyed by exception type, each with the exception_id it shows up as.
    Tests may read ground truth; the copilot code may not."""
    gt = duckdb.connect(str(recon_db[1]), read_only=True)
    rows = gt.execute("SELECT case_id, exception_type, charge_ids, record_ids FROM planted_cases "
                      "WHERE NOT held_out AND exception_type <> 'late_settlement' ORDER BY case_id").fetchall()
    gt.close()
    warehouse = ReadOnlyWarehouse(recon_db[0])
    queue = {r["exception_id"] for r in warehouse.queue()}
    warehouse.close()
    out: dict[str, Case] = {}
    for case_id, exception_type, charge_ids, record_ids in rows:
        candidates = [f"charge:{c}" for c in charge_ids] + [f"bank_line:{r}" for r in record_ids if r.startswith("bln_")]
        exception_id = next(c for c in candidates if c in queue)
        out.setdefault(exception_type, Case(case_id, exception_type, exception_id, list(charge_ids), list(record_ids)))
    return out
