"""More than one problem on one item: secondary_issues forces an escalation, enforced in code."""

from __future__ import annotations

import json

import pytest

from fakes import FakeModelClient, call, submit
from recon.copilot.agent import Copilot
from recon.copilot.contract import Diagnosis, ProposedAction, contract_violations
from recon.copilot.tools import ReadOnlyWarehouse, ToolSession


def diagnosis(**overrides):
    return Diagnosis.model_validate({"root_cause": "partial_refund_not_in_ledger", "evidence": ["evt_0123456789abcdef"],
                                     "proposed_action": "post_refund_journal", "confidence": 0.8,
                                     "needs_human_escalation": False, "rationale": "x"} | overrides)


def test_secondary_issues_with_a_fix_is_a_contract_violation():
    d = diagnosis(secondary_issues=["fee_mismatch"])
    assert any("must be escalate_to_human" in p for p in contract_violations(d, {"evt_0123456789abcdef"}))


def test_secondary_issues_with_an_escalation_is_valid():
    d = diagnosis(secondary_issues=["fee_mismatch"], proposed_action="escalate_to_human", needs_human_escalation=True)
    assert contract_violations(d, {"evt_0123456789abcdef"}) == []


def test_no_secondary_issues_is_unchanged_behaviour():
    assert contract_violations(diagnosis(), {"evt_0123456789abcdef"}) == []
    assert diagnosis().secondary_issues == []


@pytest.mark.parametrize("secondary", [["partial_refund_not_in_ledger"], ["fee_mismatch", "fee_mismatch"]])
def test_secondary_issues_cannot_repeat(secondary):
    d = diagnosis(secondary_issues=secondary, proposed_action="escalate_to_human", needs_human_escalation=True)
    assert any("must not repeat" in p for p in contract_violations(d, {"evt_0123456789abcdef"}))


def test_the_rule_reads_the_field_not_the_rationale():
    """A rationale that mentions a second problem changes nothing; only the structured field counts."""
    d = diagnosis(rationale="Also the bank is 59 cents short, possibly a fee issue.")
    assert contract_violations(d, {"evt_0123456789abcdef"}) == []


def test_the_agent_is_made_to_escalate_and_falls_back_if_it_wont(wh):
    row = next(r for r in wh.queue() if r["reason_code"] == "partial_refund_not_in_ledger")
    charge = row["charge_id"]
    refund = json.loads(ToolSession(wh).call("get_related_refunds", {"txn_id": charge}))["records"][0]["record_id"]
    stubborn = submit(root_cause="partial_refund_not_in_ledger", evidence=[refund], proposed_action="post_refund_journal",
                      confidence=0.8, needs_human_escalation=False, secondary_issues=["fee_mismatch"],
                      rationale="Refund unbooked, and a fee shortfall too.")
    run = Copilot(FakeModelClient([call("get_related_refunds", txn_id=charge), stubborn, stubborn]), wh).diagnose(
        row["exception_id"])
    assert run.is_fallback and run.diagnosis.proposed_action == ProposedAction.ESCALATE_TO_HUMAN
    assert any("must be escalate_to_human" in p for p in run.validation_failures[0])

    complies = submit(root_cause="partial_refund_not_in_ledger", evidence=[refund], proposed_action="escalate_to_human",
                      confidence=0.8, needs_human_escalation=True, secondary_issues=["fee_mismatch"],
                      rationale="Two problems: escalate.")
    run = Copilot(FakeModelClient([call("get_related_refunds", txn_id=charge), stubborn, complies]), wh).diagnose(
        row["exception_id"])
    assert not run.is_fallback and run.diagnosis.secondary_issues == ["fee_mismatch"]


def test_exceptions_list_their_detected_problems_and_blind_mode_hides_them(recon_db):
    standard = ReadOnlyWarehouse(recon_db[0])
    blind = ReadOnlyWarehouse(recon_db[0], hide_reason_code=True)
    try:
        eid = standard.queue()[0]["exception_id"]
        seen = json.loads(ToolSession(standard).call("get_exception", {"exception_id": eid}))["records"][0]
        hidden = json.loads(ToolSession(blind).call("get_exception", {"exception_id": eid}))["records"][0]
        assert seen["detected_problems"] and "reason_code" in seen
        assert "detected_problems" not in hidden and "reason_code" not in hidden
    finally:
        standard.close()
        blind.close()
