"""python -m recon.evals {estimate,run,review-demo,metrics,report,semantic}"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from dotenv import load_dotenv

from recon.config import load_settings
from recon.copilot.store import CopilotStore
from recon.copilot.tools import ReadOnlyWarehouse
from recon.evals.cases import load_cases
from recon.evals.pricing import cost_usd
from recon.evals.report import cost_table, misses, score_table
from recon.evals.review import demo_review, review_metrics
from recon.evals.runner import MODES, label, load_scores, run_config, spent_usd
from recon.evals.scoring import summarize

RESULTS_DIR = Path("evals/results")   # per-case scores: committed, no raw model output
SEMANTIC_RESULTS_DIR = RESULTS_DIR / "semantic"  # the ask eval (Slice 8); its own folder, its own format
AUDIT_DIR = Path("data/evals")        # full audit logs per run: git-ignored
MODELS = ("claude-sonnet-5-5", "claude-opus-5-5")

# Prior for the estimate, before any eval spend: the Slice 3 live run (fee_mismatch, Sonnet 5.5, 2 turns,
# no prompt cache) used 8,213 input and 623 output tokens. Harder cases take more turns, and Opus is
# assumed to write more (thinking included). No caching is assumed, so this errs high.
PRIOR = {"input_tokens": 8213, "output_tokens": 623}
HARDER_CASE_FACTOR = 1.5
OPUS_OUTPUT_FACTOR = 1.5


def estimate(n_cases: int) -> dict[str, float]:
    out = {}
    for model in MODELS:
        out_factor = OPUS_OUTPUT_FACTOR if "opus" in model else 1.0
        per_case = cost_usd(model, input_tokens=int(PRIOR["input_tokens"] * HARDER_CASE_FACTOR),
                            output_tokens=int(PRIOR["output_tokens"] * HARDER_CASE_FACTOR * out_factor))
        for mode in MODES:
            measured = load_scores(RESULTS_DIR / f"{label(model, mode)}.jsonl")
            if measured:
                per_case = summarize(measured)["cost_per_case_usd"]
            out[label(model, mode)] = per_case * n_cases
    return out


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    settings = load_settings()
    parser = argparse.ArgumentParser(prog="python -m recon.evals")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("estimate", help="estimated cost of the full matrix")
    r = sub.add_parser("run", help="run one (model, mode) over every eval case")
    r.add_argument("--model", required=True, choices=MODELS)
    r.add_argument("--mode", required=True, choices=MODES)
    r.add_argument("--budget", type=float, default=6.0, help="stop when estimated spend across all runs reaches this")
    r.add_argument("--limit", type=int)
    d = sub.add_parser("review-demo", help="a scripted demo review session over one run's proposals")
    d.add_argument("--run", required=True, help="e.g. claude-sonnet-5-5__standard")
    m = sub.add_parser("metrics", help="backlog, acceptance rate and time to resolve from a run's audit log")
    m.add_argument("--run", required=True)
    sub.add_parser("report", help="markdown tables for the README")
    se = sub.add_parser("semantic", help="the 5-question eval for `ask` (exact numbers from plain SQL)")
    se.add_argument("--model", required=True, choices=MODELS)
    se.add_argument("--rescore", action="store_true",
                    help="score the saved answers again with the current scorer (no model calls)")
    args = parser.parse_args(argv)
    if settings.warehouse_engine != "duckdb":
        # Evals spend API budget per run; they are measured once, on DuckDB. Snowflake parity is checked
        # without the model (python -m recon.parity).
        print("evals run on DuckDB only; unset WAREHOUSE_ENGINE")
        return 2

    warehouse = ReadOnlyWarehouse(settings.duckdb_path, forbidden_paths=settings.agent_forbidden_paths())
    try:
        cases, late = load_cases(settings.ground_truth_path, warehouse)
        if args.command == "estimate":
            est = estimate(len(cases))
            for name, usd in est.items():
                print(f"{name:<32} ${usd:.2f}")
            print(f"{'full matrix':<32} ${sum(est.values()):.2f}   (spent so far: ${spent_usd(RESULTS_DIR):.2f})")
        elif args.command == "run":
            warehouse.close()
            scores = run_config(settings, model=args.model, mode=args.mode, results_dir=RESULTS_DIR,
                                audit_dir=AUDIT_DIR, budget_usd=args.budget, limit=args.limit)
            print(json.dumps(summarize(scores), indent=2))
            return 0
        elif args.command in ("review-demo", "metrics"):
            store = CopilotStore(AUDIT_DIR / f"{args.run}.duckdb")
            try:
                if args.command == "review-demo":
                    print(json.dumps(demo_review(store, warehouse, cases)))
                print(json.dumps(review_metrics(store, warehouse), indent=2, default=str))
            finally:
                store.close()
        elif args.command == "semantic":
            warehouse.close()
            from recon.copilot.model_client import AnthropicModelClient
            from recon.copilot.tools import open_read_only
            from recon.evals.semantic import report, run_semantic_eval
            from recon.semantic.ask import Asker
            from recon.semantic.layer import SemanticLayer

            layer = SemanticLayer(settings.semantic_manifest_path, settings.duckdb_path,
                                  forbidden_paths=settings.agent_forbidden_paths())
            truth_con = open_read_only(settings.duckdb_path)
            try:
                if args.rescore:
                    from recon.evals.semantic import rescore

                    print(report(rescore(truth_con, SEMANTIC_RESULTS_DIR / f"{args.model}.jsonl")))
                    return 0
                asker = Asker(AnthropicModelClient(args.model, effort=settings.copilot_effort), layer)
                scores = run_semantic_eval(asker, truth_con, SEMANTIC_RESULTS_DIR / f"{args.model}.jsonl")
                print(report(scores))
            finally:
                truth_con.close()
                layer.close()
            return 0
        elif args.command == "report":
            runs = [s for s in (load_scores(p) for p in sorted(RESULTS_DIR.glob("*.jsonl"))) if s]
            print(score_table(runs), "", cost_table(runs), "", misses(runs), sep="\n")
            print(f"Late settlements kept out of the queue (Slice 2): "
                  f"{sum(not c['in_queue'] for c in late)}/{len(late)}")
    finally:
        try:
            warehouse.close()
        except Exception:  # noqa: BLE001 - already closed before a run
            pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
