"""The review CLI end to end, with the fake model: queue -> diagnose -> show -> approve/reject/resolve."""

import hashlib
import json

import duckdb
import pytest

from fakes import FakeModelClient, call, submit
from recon.config import load_settings
from recon.copilot.__main__ import main
from recon.copilot.tools import ReadOnlyWarehouse, ToolSession, open_read_only


def source_fingerprint(path) -> str:
    """A hash of every row in every warehouse table, to prove the copilot never changes source data."""
    con = open_read_only(path)
    try:
        tables = con.execute("SELECT table_schema, table_name FROM information_schema.tables "
                             "WHERE table_type = 'BASE TABLE' ORDER BY 1, 2").fetchall()
        h = hashlib.sha256()
        for schema, table in tables:
            rows = con.execute(f'SELECT * FROM "{schema}"."{table}"').fetchall()
            h.update(f"{schema}.{table}:{sorted(map(repr, rows))}".encode())
        return h.hexdigest()
    finally:
        con.close()


@pytest.fixture
def cli(recon_db, tmp_path):
    """Run the CLI against the seed-42 warehouse with a fresh copilot DB and a scripted model."""
    state = {"script": [], "output": []}

    def run(*argv, blind=False):
        settings = load_settings({"DUCKDB_PATH": str(recon_db[0]), "GROUND_TRUTH_PATH": str(recon_db[1]),
                                  "COPILOT_DB_PATH": str(tmp_path / "copilot.duckdb"),
                                  "COPILOT_HIDE_REASON_CODE": "true" if blind else "false"})
        state["output"].clear()
        code = main(list(argv), settings=settings, client_factory=lambda s: FakeModelClient(state["script"]),
                    out=state["output"].append)
        return code, "\n".join(state["output"])

    run.state = state
    run.copilot_db = tmp_path / "copilot.duckdb"
    return run


@pytest.fixture
def fee_script(recon_db, cases):
    case = cases["fee_mismatch"]
    charge = case.charge_ids[0]
    wh = ReadOnlyWarehouse(recon_db[0])
    session = ToolSession(wh)
    events = json.loads(session.call("get_processor_events", {"txn_id": charge}))["records"]
    line = json.loads(session.call("get_bank_settlement", {"txn_id": charge}))["records"][0]["record_id"]
    wh.close()
    fee_event = next(e["record_id"] for e in events if e["event_type"] == "fee")

    def script(**overrides):
        return [call("get_exception", exception_id=case.exception_id),
                call("get_processor_events", txn_id=charge),
                call("get_bank_settlement", txn_id=charge),
                submit(**({"root_cause": "fee_mismatch", "evidence": [fee_event, line],
                           "proposed_action": "raise_fee_claim_with_processor", "confidence": 0.85,
                           "needs_human_escalation": False,
                           "rationale": "Bank net is short of gross minus the fee event's amount."} | overrides))]
    return case, script, [fee_event, line]


def audit(path):
    con = duckdb.connect(str(path), read_only=True)
    try:
        cur = con.execute("SELECT event_type, run_id, model, prompt_version, payload, created_at FROM audit_log "
                          "ORDER BY audit_id")
        return [dict(zip([d[0] for d in cur.description], r)) for r in cur.fetchall()]
    finally:
        con.close()


def test_queue_lists_every_exception_as_open(cli):
    code, out = cli("queue")
    assert code == 0
    lines = out.splitlines()[1:]
    assert len(lines) == 30 and all(" open " in line for line in lines)


def test_diagnose_show_approve_writes_a_resolution_and_never_touches_source_data(cli, recon_db, fee_script):
    case, script, evidence = fee_script
    before = source_fingerprint(recon_db[0])

    cli.state["script"] = script()
    code, out = cli("diagnose", case.exception_id)
    assert code == 0 and "fee_mismatch -> raise_fee_claim_with_processor" in out

    code, out = cli("show", case.exception_id)
    assert "reason_code (reconciliation): fee_mismatch" in out
    assert "status: PROPOSED" in out and "1. get_exception" in out and "3. get_bank_settlement" in out
    assert all(f"{rid}: {{" in out for rid in evidence)  # evidence records are fetched and printed

    code, out = cli("approve", case.exception_id, "--reason", "Matches the fee schedule", "--reviewer", "sneha")
    assert code == 0 and "resolution res_" in out
    code, out = cli("approve", case.exception_id, "--reason", "again", "--reviewer", "sneha")
    assert code == 1 and "already approved" in out

    assert source_fingerprint(recon_db[0]) == before
    con = duckdb.connect(str(cli.copilot_db), read_only=True)
    [res] = con.execute("SELECT exception_id, source, action, decided_by, record_ids FROM resolutions").fetchall()
    con.close()
    assert res[:4] == (case.exception_id, "approved_proposal", "raise_fee_claim_with_processor", "sneha")
    assert json.loads(res[4]) == evidence


def test_the_audit_log_has_every_step(cli, fee_script):
    case, script, _ = fee_script
    cli.state["script"] = script()
    cli("diagnose", case.exception_id)
    cli("approve", case.exception_id, "--reason", "ok", "--reviewer", "sneha")
    events = audit(cli.copilot_db)
    types = [e["event_type"] for e in events]
    assert types == ["run_started", "tool_call", "tool_call", "tool_call", "model_output", "model_output",
                     "model_output", "model_output", "diagnosis", "review", "resolution_applied"]
    tool_calls = [json.loads(e["payload"]) for e in events if e["event_type"] == "tool_call"]
    assert tool_calls[0]["arguments"] == {"exception_id": case.exception_id}
    assert all(c["returned_record_ids"] for c in tool_calls)
    diagnosis = json.loads(next(e["payload"] for e in events if e["event_type"] == "diagnosis"))
    assert diagnosis["diagnosis"]["root_cause"] == "fee_mismatch" and diagnosis["is_fallback"] is False
    assert json.loads(events[4]["payload"])["raw"]["content"]  # the raw model output is kept
    review = json.loads(next(e["payload"] for e in events if e["event_type"] == "review"))
    assert review == {"decision": "approved", "reviewer": "sneha", "reason": "ok",
                      "action": "raise_fee_claim_with_processor", "root_cause": "fee_mismatch"}
    assert all(e["created_at"] is not None and e["model"] == "fake-model" for e in events)
    assert all(e["prompt_version"] == "copilot-v1" for e in events if e["event_type"] != "review"
               and e["event_type"] != "resolution_applied")


def test_reject_then_rediagnose(cli, fee_script):
    case, script, _ = fee_script
    cli.state["script"] = script()
    cli("diagnose", case.exception_id)
    code, out = cli("reject", case.exception_id, "--reason", "Fee schedule changed that day", "--reviewer", "sneha")
    assert code == 0
    assert " rejected " in cli("queue", "--status", "rejected")[1]
    cli.state["script"] = script()
    cli("diagnose", case.exception_id)
    assert case.exception_id in cli("queue", "--status", "proposed")[1]


def test_escalations_need_resolve_with_an_allowed_action(cli, fee_script):
    case, script, _ = fee_script
    cli.state["script"] = script(confidence=0.3)  # low confidence: downgraded to escalate_to_human
    code, out = cli("diagnose", case.exception_id)
    assert "escalate_to_human" in out and "low confidence" in out
    assert "POLICY OVERRIDE" in cli("show", case.exception_id)[1]

    code, out = cli("approve", case.exception_id, "--reason", "x")
    assert code == 1 and "use `resolve" in out
    code, out = cli("resolve", case.exception_id, "--action", "move_to_suspense", "--reason", "x")
    assert code == 1 and "not allowed" in out
    code, out = cli("resolve", case.exception_id, "--action", "post_fee_adjustment", "--reason",
                    "Checked the processor statement", "--reviewer", "sneha")
    assert code == 0 and "resolved" in out
    assert " resolved " in cli("queue", "--status", "resolved")[1]


def test_ambiguous_resolutions_need_the_real_root_cause(cli, cases):
    case = cases["ambiguous"]
    cli.state["script"] = [call("get_exception", exception_id=case.exception_id),
                           submit(root_cause="ambiguous", evidence=[case.exception_id], confidence=0.9,
                                  proposed_action="escalate_to_human", needs_human_escalation=True,
                                  rationale="Two candidates.")]
    cli("diagnose", case.exception_id)
    code, out = cli("resolve", case.exception_id, "--action", "request_settlement_trace", "--reason", "x")
    assert code == 1 and "--root-cause" in out
    code, out = cli("resolve", case.exception_id, "--action", "request_settlement_trace",
                    "--root-cause", "missing_settlement", "--reason", "Bank confirmed which charge it paid")
    assert code == 0


def test_fallbacks_are_flagged_everywhere(cli, fee_script):
    case, _, _ = fee_script
    cli.state["script"] = [RuntimeError("API down")]
    code, out = cli("diagnose", case.exception_id)
    assert "FALLBACK" in out
    assert "FALLBACK: model call failed" in cli("show", case.exception_id)[1]
    assert "(fallback)" in cli("queue")[1]
    diagnosis = json.loads(next(e["payload"] for e in audit(cli.copilot_db) if e["event_type"] == "diagnosis"))
    assert diagnosis["is_fallback"] is True


def test_blind_mode_still_shows_the_reason_code_to_the_reviewer(cli, fee_script):
    case, script, _ = fee_script
    cli.state["script"] = script()
    cli("diagnose", case.exception_id, blind=True)
    out = cli("show", case.exception_id, blind=True)[1]
    assert "reason_code (reconciliation): fee_mismatch" in out and "mode: blind" in out
    started = json.loads(next(e["payload"] for e in audit(cli.copilot_db) if e["event_type"] == "run_started"))
    assert started["hide_reason_code"] is True


def test_diagnose_all_skips_exceptions_that_already_have_a_proposal(cli, fee_script):
    case, script, _ = fee_script
    cli.state["script"] = script()
    cli("diagnose", case.exception_id)
    cli.state["script"] = []  # the fake answers in plain text: every run falls back, which is fine here
    code, out = cli("diagnose", "--all")
    assert code == 0 and len(out.splitlines()) == 29 and case.exception_id not in out


def test_show_explains_an_escalation_floor_override(cli, recon_db):
    wh = ReadOnlyWarehouse(recon_db[0])
    row = next(r for r in wh.queue() if r["reason_code"] == "ambiguous" and r["item_type"] == "charge")
    refund = json.loads(ToolSession(wh).call("get_related_refunds", {"txn_id": row["charge_id"]}))["records"][0]
    wh.close()
    cli.state["script"] = [call("get_related_refunds", txn_id=row["charge_id"]),
                           submit(root_cause="partial_refund_not_in_ledger", evidence=[refund["record_id"]],
                                  proposed_action="post_refund_journal", confidence=0.9,
                                  needs_human_escalation=False, rationale="Netted refund.")]
    code, out = cli("diagnose", row["exception_id"])
    assert "escalate_to_human" in out and "escalation floor: escalated" in out
    shown = cli("show", row["exception_id"])[1]
    assert "POLICY OVERRIDE (escalation floor)" in shown and "post_refund_journal" in shown
    events = audit(cli.copilot_db)
    override = json.loads(next(e["payload"] for e in events if e["event_type"] == "policy_override"))
    assert override["rule"] == "escalation_floor"
    assert json.loads(next(e["payload"] for e in events if e["event_type"] == "run_started"))["escalation_floor"]
