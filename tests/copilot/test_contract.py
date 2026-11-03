from pathlib import Path

import pytest
import yaml

from recon.copilot.contract import (
    ALLOWED_ACTIONS,
    Diagnosis,
    ProposedAction,
    RootCause,
    contract_table,
    contract_violations,
    validate_submission,
)
from recon.taxonomy import ExceptionType

MARTS_YML = Path(__file__).resolve().parents[2] / "dbt" / "models" / "marts" / "_marts.yml"
A = ProposedAction


def reconciliation_reason_codes() -> set[str]:
    spec = yaml.safe_load(MARTS_YML.read_text())
    exceptions = next(m for m in spec["models"] if m["name"] == "recon_exceptions")
    column = next(c for c in exceptions["columns"] if c["name"] == "reason_code")
    accepted = next(t["accepted_values"] for t in column["data_tests"] if isinstance(t, dict))
    return set(accepted["arguments"]["values"])


def test_root_cause_enum_is_exactly_what_reconciliation_can_produce():
    assert {c.value for c in RootCause} == reconciliation_reason_codes()
    assert "ledger_amount_mismatch" not in {c.value for c in RootCause}


def test_every_planted_type_except_late_settlement_maps_to_a_root_cause():
    mapped = {"date_boundary": "date_mismatch"}
    for t in ExceptionType:
        if t != ExceptionType.LATE_SETTLEMENT:
            assert RootCause(mapped.get(t.value, t.value))


def test_escalation_is_always_allowed_and_is_the_only_option_for_ambiguous():
    assert set(ALLOWED_ACTIONS) == set(RootCause)
    assert all(A.ESCALATE_TO_HUMAN in actions for actions in ALLOWED_ACTIONS.values())
    assert ALLOWED_ACTIONS[RootCause.AMBIGUOUS] == {A.ESCALATE_TO_HUMAN}


def test_every_action_belongs_to_exactly_one_root_cause():
    for action in ProposedAction:
        if action != A.ESCALATE_TO_HUMAN:
            owners = [c for c, actions in ALLOWED_ACTIONS.items() if action in actions]
            assert len(owners) == 1, (action, owners)


def test_contract_table_names_every_root_cause():
    text = contract_table()
    assert all(c.value in text for c in RootCause)
    assert "ambiguous: escalate_to_human ONLY" in text


def make(**overrides) -> Diagnosis:
    base = dict(root_cause="fee_mismatch", evidence=["evt_0123456789abcdef"],
                proposed_action="raise_fee_claim_with_processor", confidence=0.8,
                needs_human_escalation=False, rationale="Bank net is 40 cents short of gross minus fee.")
    return Diagnosis.model_validate(base | overrides)


def test_a_consistent_diagnosis_has_no_violations():
    assert contract_violations(make(), {"evt_0123456789abcdef"}) == []


@pytest.mark.parametrize(("overrides", "needle"), [
    ({"evidence": ["evt_ffffffffffffffff"]}, "no tool returned"),
    ({"proposed_action": "move_to_suspense"}, "not allowed"),
    ({"root_cause": "ambiguous"}, "not allowed"),
    ({"needs_human_escalation": True}, "needs_human_escalation"),
    ({"proposed_action": "escalate_to_human"}, "needs_human_escalation"),
])
def test_contract_violations(overrides, needle):
    problems = contract_violations(make(**overrides), {"evt_0123456789abcdef"})
    assert any(needle in p for p in problems), problems


@pytest.mark.parametrize("bad", [
    {"confidence": 1.5},
    {"root_cause": "ledger_amount_mismatch"},
    {"proposed_action": "delete_everything"},
    {"evidence": []},
    {"evidence": ["'; DROP TABLE raw.processor_events; --"]},
    {"rationale": "x" * 601},
    {"extra_field": 1},
])
def test_schema_rejects_bad_submissions(bad):
    base = make().model_dump(mode="json")
    diagnosis, problems = validate_submission(base | bad, {"evt_0123456789abcdef"})
    assert diagnosis is None and problems
