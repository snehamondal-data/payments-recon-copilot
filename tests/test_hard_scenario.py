"""--scenario hard: the two harder cases exist only when asked for, and are built as described.

These tests check the data and the pre-registered expectations. They deliberately don't assert what
reconciliation or the copilot does with the cases: that is the measurement (README, "Harder cases").
"""

from __future__ import annotations

import duckdb
import pytest

from recon.generator import build_scenario
from recon.generator.ground_truth import write_ground_truth


@pytest.fixture(scope="module")
def hard():
    return build_scenario(scenario="hard")


def test_the_standard_scenario_is_unchanged(scenario):
    assert scenario.content_hash()[:12] == "e462c3badf2b"
    assert len(scenario.cases) == 33 and scenario.hard_expectations == [] and scenario.name == "standard"


def test_hard_is_the_standard_cases_plus_two(scenario, hard):
    assert [c.case_id for c in hard.cases[:33]] == [c.case_id for c in scenario.cases]
    assert [c.case_id for c in hard.cases[33:]] == ["12-split_settlement-1", "13-fee_and_refund_on_one_charge-1"]
    assert [e.case_id for e in hard.hard_expectations] == [c.case_id for c in hard.cases[33:]]


def lines_for(scenario, charge_id):
    return [(b.batch_id, b.value_date, line) for b in scenario.bank_settlements for line in b.lines
            if line.reference == charge_id]


def test_split_settlement_is_two_lines_in_two_batches_summing_to_the_net(hard):
    case = hard.cases[33]
    [charge_id] = case.charge_ids
    c = hard.charges[charge_id]
    settled = [(batch, day, line) for batch, day, line in lines_for(hard, charge_id) if line.description == "SETTLEMENT"]
    assert len(settled) == 2 and len({batch for batch, _, _ in settled}) == 2
    assert sum(line.net_amount_minor for _, _, line in settled) == c.net_usd
    assert hard.hard_expectations[0].expected_status == "matched" and hard.hard_expectations[0].expected_reason_code is None


def test_two_problems_is_a_short_settlement_plus_an_unbooked_refund(hard):
    case = hard.cases[34]
    [charge_id] = case.charge_ids
    c = hard.charges[charge_id]
    by_kind = {line.description: line for _, _, line in lines_for(hard, charge_id)}
    assert 0 < c.net_usd - by_kind["SETTLEMENT"].net_amount_minor <= 75  # the fee mismatch
    refunds = [e for e in hard.processor_events if e.charge_id == charge_id and e.event_type == "refund"]
    assert len(refunds) == 1 and by_kind["REFUND"].net_amount_minor == refunds[0].settlement_amount_minor
    assert not [e for e in hard.ledger_entries if e.charge_id == charge_id and e.entry_type == "refund"]
    e = hard.hard_expectations[1]
    assert (e.expected_status, e.expected_reason_code, e.should_escalate) == ("exception", "ambiguous", True)


def test_expectations_are_in_the_hard_ground_truth_only(scenario, hard, tmp_path):
    write_ground_truth(tmp_path / "standard.duckdb", scenario)
    write_ground_truth(tmp_path / "hard.duckdb", hard)
    tables = lambda p: {r[0] for r in duckdb.connect(str(p), read_only=True).execute(  # noqa: E731
        "SELECT table_name FROM information_schema.tables").fetchall()}
    assert "hard_expectations" not in tables(tmp_path / "standard.duckdb")
    con = duckdb.connect(str(tmp_path / "hard.duckdb"), read_only=True)
    assert con.execute("SELECT count(*) FROM hard_expectations").fetchone()[0] == 2
