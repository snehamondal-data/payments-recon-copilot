"""Writes the answers (planted cases, expected DLQ contents) to their own DuckDB file.

This file is for tests and evals only. The loader and the agent tools must never open it.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from recon.business_days import SETTLEMENT_WINDOW_BUSINESS_DAYS
from recon.generator.scenario import Scenario


def write_ground_truth(path: Path, scenario: Scenario) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.unlink(missing_ok=True)
    Path(f"{path}.wal").unlink(missing_ok=True)
    con = duckdb.connect(str(path))
    try:
        con.execute("""
            CREATE TABLE planted_cases (
                case_id VARCHAR PRIMARY KEY,
                exception_type VARCHAR NOT NULL,
                expected_status VARCHAR,          -- NULL: decided in Slice 2
                should_escalate BOOLEAN NOT NULL,
                held_out BOOLEAN NOT NULL,
                charge_ids VARCHAR[] NOT NULL,
                record_ids VARCHAR[] NOT NULL,
                description VARCHAR NOT NULL
            )""")
        con.executemany(
            "INSERT INTO planted_cases VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [(c.case_id, c.exception_type.value, c.expected_status, c.should_escalate, c.held_out,
              c.charge_ids, c.record_ids, c.description) for c in scenario.cases],
        )
        con.execute("CREATE TABLE expected_dlq (seq INTEGER PRIMARY KEY, topic VARCHAR NOT NULL, kind VARCHAR NOT NULL)")
        con.executemany("INSERT INTO expected_dlq VALUES (?, ?, ?)",
                        [(i, p.topic, p.kind) for i, p in enumerate(scenario.poison, start=1)])
        con.execute("""
            CREATE TABLE generator_run (
                seed INTEGER, as_of DATE, settlement_window_business_days INTEGER,
                content_sha256 VARCHAR, messages INTEGER, redelivered INTEGER, expected_counts JSON
            )""")
        con.execute("INSERT INTO generator_run VALUES (?, ?, ?, ?, ?, ?, ?)", (
            scenario.seed, scenario.as_of, SETTLEMENT_WINDOW_BUSINESS_DAYS, scenario.content_hash(),
            len(scenario.messages), scenario.redelivered, json.dumps(scenario.expected_counts()),
        ))
    finally:
        con.close()
