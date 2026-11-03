"""Review CLI: python -m recon.copilot {queue,diagnose,show,approve,reject,resolve} ..."""

from __future__ import annotations

import argparse
import getpass
import json
import sys
from collections.abc import Callable
from typing import Any

from dotenv import load_dotenv

from recon.config import Settings, load_settings
from recon.copilot.agent import AgentConfig, Copilot
from recon.copilot.contract import ProposedAction
from recon.copilot.model_client import AnthropicModelClient, ModelClient
from recon.copilot.store import CopilotStore, ReviewError
from recon.copilot.tools import ReadOnlyWarehouse


def _cents(value: Any) -> str:
    return "" if value is None else f"{int(value) / 100:,.2f}"


def render_queue(warehouse: ReadOnlyWarehouse, store: CopilotStore, status: str | None) -> str:
    lines = [f"{'exception_id':<34} {'reason_code':<29} {'amount':>10} {'age':>3}  {'status':<9} "
             f"{'proposed_action':<30} conf"]
    for row in warehouse.queue():
        p = store.latest_proposal(row["exception_id"])
        state = p.status if p else "open"
        if status and state != status:
            continue
        action = conf = ""
        if p:
            action = p.diagnosis["proposed_action"] + (" (fallback)" if p.is_fallback else "")
            conf = f"{p.diagnosis['confidence']:.2f}"
        lines.append(f"{row['exception_id']:<34} {row['reason_code']:<29} {_cents(row['amount_minor']):>10} "
                     f"{row['age_business_days']:>3}  {state:<9} {action:<30} {conf}")
    return "\n".join(lines)


def render_show(warehouse: ReadOnlyWarehouse, store: CopilotStore, exception_id: str) -> str:
    row = warehouse.exception_row(exception_id)
    if row is None:
        raise ReviewError(f"no exception {exception_id}")
    out = [f"Exception   {exception_id}",
           # The reviewer always sees the reconciliation reason code, even when the agent ran blind.
           f"  reason_code (reconciliation): {row['reason_code']}",
           f"  amount at stake: {_cents(row['amount_minor'])} USD   item date: {row['item_date']}   "
           f"age: {row['age_business_days']} business days   as-of: {row['as_of_date']}"]
    if row.get("related_charge_ids"):
        out.append(f"  related charges: {row['related_charge_ids']}")
    p = store.latest_proposal(exception_id)
    if p is None:
        return "\n".join(out + ["", "No diagnosis yet. Run: python -m recon.copilot diagnose " + exception_id])
    d = p.diagnosis
    events = store.events(run_id=p.run_id)
    started = next(e for e in events if e["event_type"] == "run_started")
    finished = next(e for e in events if e["event_type"] == "diagnosis")
    out += ["", f"Diagnosis   run {p.run_id}   model {p.model}   status: {p.status.upper()}",
            f"  mode: {'blind (reason_code hidden from the agent)' if started['payload']['hide_reason_code'] else 'standard'}"
            f"   tool calls: {finished['payload']['tool_calls']}"
            f"   tokens in/out: {finished['payload']['input_tokens']}/{finished['payload']['output_tokens']}",
            f"  root_cause:             {d['root_cause']}"
            + ("" if d["root_cause"] == row["reason_code"] else "   <-- differs from reconciliation's reason_code"),
            f"  proposed_action:        {d['proposed_action']}",
            f"  confidence:             {d['confidence']:.2f}",
            f"  needs_human_escalation: {d['needs_human_escalation']}"]
    if p.is_fallback:
        out.append(f"  FALLBACK: {p.fallback_reason}")
    if p.policy_override:
        o = p.policy_override
        if o["rule"] == "escalation_floor":
            out.append(f"  POLICY OVERRIDE (escalation floor): reconciliation says ambiguous, so the agent's "
                       f"{o['original_action']} (as {o['original_root_cause']}) became escalate_to_human")
        else:
            out.append(f"  POLICY OVERRIDE: confidence {o['confidence']:.2f} < {o['threshold']}, "
                       f"so {o['original_action']} became escalate_to_human")
    out += ["  rationale:", *[f"    {line}" for line in d["rationale"].splitlines()], "", "Evidence"]
    for rid in d["evidence"]:
        record = warehouse.lookup_record(rid)
        shown = {k: v for k, v in (record or {}).items() if not k.startswith("_")}
        out.append(f"  {rid}: {json.dumps(shown, default=str) if record else '(not found)'}")
    out += ["", "Tool trace"]
    for e in events:
        if e["event_type"] == "tool_call":
            c = e["payload"]
            result = f"error: {c['error']}" if c["error"] else f"{len(c['returned_record_ids'])} records"
            out.append(f"  {c['seq']}. {c['tool']}({json.dumps(c['arguments'])}) -> {result}")
        elif e["event_type"] == "validation_failed":
            out.append(f"  ! invalid submission: {' | '.join(e['payload']['problems'])}")
    if p.review:
        out += ["", f"Review: {p.review['decision']} by {p.review['reviewer']}: {p.review['reason']}"]
    return "\n".join(out)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m recon.copilot", description="Reconciliation copilot review CLI.")
    sub = parser.add_subparsers(dest="command", required=True)
    q = sub.add_parser("queue", help="list exceptions with their proposal and review state")
    q.add_argument("--status", choices=["open", "proposed", "approved", "rejected", "resolved"])
    d = sub.add_parser("diagnose", help="run the copilot on one exception, or all open ones")
    target = d.add_mutually_exclusive_group(required=True)
    target.add_argument("exception_id", nargs="?")
    target.add_argument("--all", action="store_true", help="every exception without a proposal")
    s = sub.add_parser("show", help="diagnosis, evidence and tool trace")
    s.add_argument("exception_id")
    for name, help_text in (("approve", "accept the proposal and write a resolution record"),
                            ("reject", "reject the proposal")):
        c = sub.add_parser(name, help=help_text)
        c.add_argument("exception_id")
        c.add_argument("--reason", required=True)
        c.add_argument("--reviewer", default=getpass.getuser())
    r = sub.add_parser("resolve", help="a human chooses the action (for escalations and rejections)")
    r.add_argument("exception_id")
    r.add_argument("--action", required=True, choices=[a.value for a in ProposedAction])
    r.add_argument("--root-cause", help="override the proposal's root cause (required when it was ambiguous)")
    r.add_argument("--reason", required=True)
    r.add_argument("--reviewer", default=getpass.getuser())
    return parser


def open_warehouse(settings: Settings) -> ReadOnlyWarehouse:
    """WAREHOUSE_ENGINE=snowflake reads as RECON_AGENT_ROLE; otherwise the read-only DuckDB file."""
    if settings.warehouse_engine == "snowflake":
        return ReadOnlyWarehouse.snowflake(settings.snowflake, hide_reason_code=settings.copilot_hide_reason_code)
    return ReadOnlyWarehouse(settings.duckdb_path, forbidden_paths=settings.agent_forbidden_paths(),
                             hide_reason_code=settings.copilot_hide_reason_code)


def open_store(settings: Settings) -> CopilotStore:
    """WAREHOUSE_ENGINE=snowflake writes RECON.COPILOT as the loader, never the agent; otherwise copilot.duckdb."""
    if settings.warehouse_engine == "snowflake":
        return CopilotStore.snowflake(settings.snowflake)
    return CopilotStore(settings.copilot_db_path)


def main(argv: list[str] | None = None, *, settings: Settings | None = None,
         client_factory: Callable[[Settings], ModelClient] | None = None, out: Callable[[str], None] = print) -> int:
    load_dotenv()  # ANTHROPIC_API_KEY and RECON_MODEL live in .env; never in code
    settings = settings or load_settings()
    args = build_parser().parse_args(argv)
    warehouse = open_warehouse(settings)
    store = open_store(settings)
    try:
        if args.command == "queue":
            out(render_queue(warehouse, store, args.status))
        elif args.command == "diagnose":
            factory = client_factory or (lambda s: AnthropicModelClient(s.recon_model, effort=s.copilot_effort))
            copilot = Copilot(factory(settings), warehouse, AgentConfig(max_tool_calls=settings.copilot_max_tool_calls,
                                                                        timeout_s=settings.copilot_timeout_s))
            ids = ([r["exception_id"] for r in warehouse.queue() if store.latest_proposal(r["exception_id"]) is None]
                   if args.all else [args.exception_id])
            for eid in ids:
                run = copilot.diagnose(eid)
                store.record_run(run)
                d = run.diagnosis
                flag = f"  FALLBACK: {run.fallback_reason}" if run.is_fallback else (
                    f"  ({run.policy_override['rule'].replace('_', ' ')}: escalated)" if run.policy_override else "")
                out(f"{eid}: {d.root_cause.value} -> {d.proposed_action.value} "
                    f"(confidence {d.confidence:.2f}, {len(run.tool_calls)} tool calls){flag}")
        elif args.command == "show":
            out(render_show(warehouse, store, args.exception_id))
        elif args.command == "approve":
            amount = warehouse.exception_row(args.exception_id)["amount_minor"]
            rid = store.approve(args.exception_id, reviewer=args.reviewer, reason=args.reason, amount_minor=amount)
            out(f"approved {args.exception_id}; resolution {rid} recorded (source data unchanged)")
        elif args.command == "reject":
            store.reject(args.exception_id, reviewer=args.reviewer, reason=args.reason)
            out(f"rejected {args.exception_id}")
        elif args.command == "resolve":
            amount = warehouse.exception_row(args.exception_id)["amount_minor"]
            rid = store.resolve(args.exception_id, action=args.action, root_cause=args.root_cause,
                                reviewer=args.reviewer, reason=args.reason, amount_minor=amount)
            out(f"resolved {args.exception_id} as {args.action}; resolution {rid} recorded (source data unchanged)")
    except (ReviewError, KeyError) as err:
        out(f"error: {err}")
        return 1
    finally:
        store.close()
        warehouse.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
