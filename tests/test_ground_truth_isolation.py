"""Ground truth lives in its own file. The warehouse, the loader and (later) the agent tools never see it."""

from pathlib import Path

import duckdb
import pytest

from conftest import FakeDLQ, as_raw_messages, load_all
from recon.config import load_settings
from recon.fx import read_fx_csv, write_fx_csv
from recon.generator.ground_truth import write_ground_truth
from recon.loader.schema import ensure_schema
from recon.loader.sink import Sink, load_fx_rates
from recon.warehouse import DuckDBClient

SRC = Path(__file__).resolve().parents[1] / "src" / "recon"
GROUND_TRUTH_TABLES = {"planted_cases", "expected_dlq", "generator_run"}


def test_ground_truth_is_written_to_its_own_file(scenario, tmp_path):
    path = tmp_path / "ground_truth.duckdb"
    write_ground_truth(path, scenario)
    con = duckdb.connect(str(path), read_only=True)
    try:
        tables = {r[0] for r in con.execute("SELECT table_name FROM information_schema.tables").fetchall()}
        assert tables == GROUND_TRUTH_TABLES
        assert con.execute("SELECT count(*) FROM planted_cases").fetchone()[0] == len(scenario.cases)
        assert con.execute("SELECT count(*) FROM expected_dlq").fetchone()[0] == len(scenario.poison)
        assert con.execute("SELECT content_sha256 FROM generator_run").fetchone()[0] == scenario.content_hash()
    finally:
        con.close()


def test_main_warehouse_contains_no_ground_truth_tables(scenario, tmp_path):
    write_ground_truth(tmp_path / "ground_truth.duckdb", scenario)
    write_fx_csv(tmp_path / "fx.csv", scenario.fx_rates)
    wh = DuckDBClient(tmp_path / "warehouse.duckdb")
    try:
        ensure_schema(wh)
        load_fx_rates(wh, read_fx_csv(tmp_path / "fx.csv"))
        load_all(Sink(wh, FakeDLQ()), as_raw_messages(scenario))
        rows = wh.execute("SELECT table_schema, table_name FROM information_schema.tables")
        names = {name for _, name in rows}
        schemas = {schema for schema, _ in rows}
        assert not names & GROUND_TRUTH_TABLES
        assert schemas == {"raw", "ref"}
        # No attached databases either (only the warehouse itself plus DuckDB's built-ins).
        attached = {r[0] for r in wh.execute("SELECT database_name FROM duckdb_databases() WHERE NOT internal")}
        assert attached == {"warehouse"}
    finally:
        wh.close()


@pytest.mark.parametrize("package", ["loader", "warehouse"])
def test_loader_and_warehouse_code_never_reference_ground_truth(package):
    for path in (SRC / package).rglob("*.py"):
        text = path.read_text()
        assert "ground_truth" not in text, path
        assert "recon.generator" not in text, path


def test_dbt_project_never_reads_ground_truth():
    dbt_dir = SRC.parents[1] / "dbt"
    files = [p for p in dbt_dir.rglob("*") if p.suffix in {".sql", ".yml"}
             and not {"target", "logs", "dbt_packages"} & set(p.relative_to(dbt_dir).parts)]
    assert files
    for path in files:
        text = path.read_text().lower()
        for forbidden in ("ground_truth", *GROUND_TRUTH_TABLES, "attach"):
            assert forbidden not in text, f"{path} mentions {forbidden!r}"


def test_settings_refuse_to_use_the_ground_truth_file_as_the_warehouse():
    with pytest.raises(ValueError):
        load_settings({"DUCKDB_PATH": "data/x.duckdb", "GROUND_TRUTH_PATH": "data/x.duckdb"})
