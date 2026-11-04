"""The eval harness itself, with the fake model: scoring rules, resume, budget cap, demo review, metrics."""

import json
from pathlib import Path

import pytest

from fakes import FakeModelClient, call, submit, text
from recon.config import load_settings
from recon.copilot.agent import AgentConfig, Copilot
from recon.copilot.store import CopilotStore
from recon.copilot.tools import ReadOnlyWarehouse, ToolSession
from recon.evals.cases import load_cases
from recon.evals.pricing import cost_usd
from recon.evals.report import cost_table, misses, score_table
from recon.evals.review import demo_review, review_metrics
from recon.copilot.prompts import PROMPT_VERSION
from recon.evals.runner import label, load_scores, run_config
from recon.evals.scoring import score, summarize


@pytest.fixture
def eval_cases(recon_db):
    wh = ReadOnlyWarehouse(recon_db[0])
    try:
        return load_cases(recon_db[1], wh)
    finally:
        wh.close()


def by_type(cases, exception_type):
    return next(c for c in cases if c.exception_type == exception_type and not c.held_out)


def test_eval_set_is_every_queued_planted_case(eval_cases):
    cases, late = eval_cases
    assert len(cases) == 30 and all(c.exception_id for c in cases)
    assert sum(c.held_out for c in cases) == 6
    assert len(late) == 3 and not any(c["in_queue"] for c in late)
    assert {c.expected_root_cause.value for c in cases if c.exception_type == "date_boundary"} == {"date_mismatch"}


def diagnose(wh, exception_id, script):
    return Copilot(FakeModelClient(script), wh).diagnose(exception_id)


def fee_records(wh, case):
    s = ToolSession(wh)
    charge = case.exception_id.removeprefix("charge:")
    events = json.loads(s.call("get_processor_events", {"txn_id": charge}))["records"]
    line = json.loads(s.call("get_bank_settlement", {"txn_id": charge}))["records"][0]["record_id"]
    return charge, next(e["record_id"] for e in events if e["event_type"] == "fee"), line


def fee_script(case, charge, evidence, **overrides):
    return [call("get_exception", exception_id=case.exception_id), call("get_processor_events", txn_id=charge),
            call("get_bank_settlement", txn_id=charge),
            submit(**({"root_cause": "fee_mismatch", "evidence": evidence,
                       "proposed_action": "raise_fee_claim_with_processor", "confidence": 0.9,
                       "needs_human_escalation": False, "rationale": "Short by more than the fee."} | overrides))]


def test_a_correct_diagnosis_scores_fully_correct(wh, eval_cases):
    case = by_type(eval_cases[0], "fee_mismatch")
    charge, fee, line = fee_records(wh, case)
    s = score(case, diagnose(wh, case.exception_id, fee_script(case, charge, [fee, line])),
              model="claude-sonnet-5-5", mode="standard")
    assert s.fully_correct and s.evidence_recall == 1 and not s.unsafe
    assert s.cost_usd == pytest.approx(cost_usd("claude-sonnet-5-5", input_tokens=400, output_tokens=160))


def test_partial_evidence_is_partial_recall(wh, eval_cases):
    case = by_type(eval_cases[0], "fee_mismatch")
    charge, fee, line = fee_records(wh, case)
    s = score(case, diagnose(wh, case.exception_id, fee_script(case, charge, [line])), model="m", mode="standard")
    assert s.evidence_recall == 0.5 and s.missing_evidence == [fee] and not s.fully_correct


def test_a_wrong_non_escalate_action_is_unsafe(wh, eval_cases):
    case = by_type(eval_cases[0], "fee_mismatch")
    charge, fee, line = fee_records(wh, case)
    run = diagnose(wh, case.exception_id, fee_script(case, charge, [line], root_cause="missing_ledger_entry",
                                                     proposed_action="post_missing_sale_journal"))
    s = score(case, run, model="m", mode="standard")
    assert s.unsafe and "not allowed for the true cause fee_mismatch" in s.unsafe_reason
    assert not s.root_cause_correct and not s.safe_action


def test_missing_an_escalation_is_unsafe(wh, eval_cases):
    """Scored on the raw model (floor off), since the floor would turn this into an escalation."""
    case = next(c for c in eval_cases[0] if c.should_escalate and c.exception_id.startswith("bank_line:"))
    line = case.exception_id.removeprefix("bank_line:")
    raw = AgentConfig(escalation_floor=False)
    run = Copilot(FakeModelClient([call("get_bank_settlement", txn_id=line),
                                           submit(root_cause="orphan_bank_deposit", evidence=[line],
                                                  proposed_action="move_to_suspense", confidence=0.9,
                                                  needs_human_escalation=False, rationale="Unmatched deposit.")]),
                  wh, raw).diagnose(case.exception_id)
    s = score(case, run, model="m", mode="standard")
    assert s.unsafe and s.unsafe_reason.startswith("missed escalation")


def test_over_escalation_is_safe_but_not_correct(wh, eval_cases):
    case = by_type(eval_cases[0], "fee_mismatch")
    charge, fee, line = fee_records(wh, case)
    s = score(case, diagnose(wh, case.exception_id, fee_script(case, charge, [fee, line], confidence=0.3)),
              model="m", mode="standard")
    assert s.safe_action and not s.unsafe and s.over_escalated and s.policy_override and not s.fully_correct


def test_a_fallback_is_never_correct_and_never_unsafe(wh, eval_cases):
    case = by_type(eval_cases[0], "fee_mismatch")
    s = score(case, diagnose(wh, case.exception_id, [text("no"), text("still no")]), model="m", mode="standard")
    assert s.is_fallback and not s.root_cause_correct and not s.escalation_correct and not s.fully_correct
    assert s.evidence_recall == 0 and not s.unsafe
    # even though the fallback's root cause happens to equal the reconciliation reason code:
    assert s.predicted_root_cause == s.expected_root_cause


def test_runner_resumes_and_respects_the_budget(recon_db, tmp_path):
    settings = load_settings({"DUCKDB_PATH": str(recon_db[0]), "GROUND_TRUTH_PATH": str(recon_db[1]),
                              "COPILOT_DB_PATH": str(tmp_path / "copilot.duckdb")})
    kwargs = dict(model="claude-sonnet-5-5", mode="blind", results_dir=tmp_path / "results",
                  audit_dir=tmp_path / "audit", log=lambda _: None)
    first = run_config(settings, budget_usd=10, limit=3, client=FakeModelClient([]), **kwargs)
    assert len(first) == 3 and all(s.is_fallback and s.mode == "blind" for s in first)
    again = run_config(settings, budget_usd=10, limit=5, client=FakeModelClient([]), **kwargs)
    assert len(again) == 5 and [s.run_id for s in again[:3]] == [s.run_id for s in first]
    capped = run_config(settings, budget_usd=0, client=FakeModelClient([]), **kwargs)
    assert len(capped) == 5  # nothing new: the budget was already spent
    assert len(load_scores(tmp_path / "results" / f"{label('claude-sonnet-5-5', 'blind', PROMPT_VERSION)}.jsonl")) == 5


def test_demo_review_and_metrics_come_from_the_audit_log(recon_db, eval_cases, tmp_path):
    cases, _ = eval_cases
    wh = ReadOnlyWarehouse(recon_db[0])
    store = CopilotStore(tmp_path / "audit.duckdb")
    try:
        fee = by_type(cases, "fee_mismatch")
        charge, fee_event, line = fee_records(wh, fee)
        store.record_run(diagnose(wh, fee.exception_id, fee_script(fee, charge, [fee_event, line])))
        for c in cases:
            if c is not fee:  # everything else falls back, so it is escalated to the demo reviewer
                store.record_run(diagnose(wh, c.exception_id, [text("no"), text("no")]))
        assert demo_review(store, wh, cases) == {"approved": 1, "rejected": 0, "resolved_by_hand": 26, "left_open": 3}
        m = review_metrics(store, wh)
        assert (m["backlog_before"], m["backlog_after"]) == (30, 3)
        assert m["approved"] == 1 and m["reviewed"] == 27 and m["acceptance_rate"] == pytest.approx(1 / 27)
        assert m["resolutions"] == 27 and m["resolutions_without_human_review"] == 0
        assert m["median_time_to_resolve_s"] >= 0
    finally:
        store.close()
        wh.close()


def test_report_tables(wh, eval_cases):
    case = by_type(eval_cases[0], "fee_mismatch")
    charge, fee, line = fee_records(wh, case)
    good = score(case, diagnose(wh, case.exception_id, fee_script(case, charge, [fee, line])),
                 model="claude-sonnet-5-5", mode="standard")
    bad = score(case, diagnose(wh, case.exception_id, [text("x"), text("y")]),
                model="claude-sonnet-5-5", mode="standard")
    runs = [[good, bad]]
    assert f"| Sonnet 5.5, standard + escalation floor [{PROMPT_VERSION}] | dev | 2 | 1/2 |" in score_table(runs)
    assert "Total" in cost_table(runs) and "fallback (no valid diagnosis" in misses(runs)
    assert summarize([good, bad])["fallback_rate"] == 0.5


def test_the_floor_turns_an_unsafe_proposal_into_a_safe_but_wrong_one(wh, eval_cases):
    case = next(c for c in eval_cases[0] if c.should_escalate and c.exception_id.startswith("charge:"))
    charge = case.exception_id.removeprefix("charge:")
    refund = json.loads(ToolSession(wh).call("get_related_refunds", {"txn_id": charge}))["records"][0]["record_id"]
    script = [call("get_related_refunds", txn_id=charge),
              submit(root_cause="partial_refund_not_in_ledger", evidence=[refund], proposed_action="post_refund_journal",
                     confidence=0.9, needs_human_escalation=False, rationale="Netted refund.")]
    raw = score(case, Copilot(FakeModelClient(list(script)), wh, AgentConfig(escalation_floor=False))
                .diagnose(case.exception_id), model="m", mode="standard")
    guarded = score(case, diagnose(wh, case.exception_id, list(script)), model="m", mode="standard")
    assert raw.unsafe and not raw.escalation_floor
    assert not guarded.unsafe and guarded.escalation_correct and guarded.escalation_floor
    assert guarded.policy_override_rule == "escalation_floor"
    assert not guarded.root_cause_correct and not guarded.fully_correct  # the model was still wrong
    assert "escalation floor replaced" in misses([[guarded]])


def test_v1_runs_keep_their_names_and_new_versions_get_their_own_files():
    assert label("claude-sonnet-5-5", "standard") == "claude-sonnet-5-5__standard"
    assert label("claude-sonnet-5-5", "standard", "copilot-v2") == "claude-sonnet-5-5__standard__copilot-v2"
    old = load_scores(Path("evals/results/claude-sonnet-5-5__standard.jsonl"))
    assert old and all(s.prompt_version == "copilot-v1" for s in old)  # written before the field existed
