"""Markdown tables for the README, built from the per-case score files."""

from __future__ import annotations

from recon.evals.scoring import CaseScore, summarize

MODEL_NAMES = {"claude-sonnet-5-5": "Sonnet 5.5", "claude-opus-5-5": "Opus 5.5"}


def _pct(x: float) -> str:
    return f"{100 * x:.0f}%"


def config_name(scores: list[CaseScore]) -> str:
    s = scores[0]
    return f"{MODEL_NAMES.get(s.model, s.model)}, {s.mode}"


def score_table(runs: list[list[CaseScore]]) -> str:
    lines = ["| Config | Split | Cases | Fully correct | Root cause | Evidence recall | Safe action | "
             "Escalation | **Unsafe** | Fallbacks | Over-escalated |",
             "|---|---|---|---|---|---|---|---|---|---|---|"]
    for scores in runs:
        for split, subset in (("dev", [s for s in scores if not s.held_out]),
                              ("held-out", [s for s in scores if s.held_out])):
            m = summarize(subset)
            if not m["n"]:
                continue
            lines.append(f"| {config_name(scores)} | {split} | {m['n']} | {m['fully_correct']}/{m['n']} | "
                         f"{_pct(m['root_cause_accuracy'])} | {_pct(m['evidence_recall'])} | "
                         f"{_pct(m['safe_action_rate'])} | {_pct(m['escalation_accuracy'])} | **{m['unsafe']}** | "
                         f"{m['fallbacks']} ({_pct(m['fallback_rate'])}) | {m['over_escalations']} |")
    return "\n".join(lines)


def cost_table(runs: list[list[CaseScore]]) -> str:
    lines = ["| Config | Cases | Input tokens (incl. cached) | Output tokens | Est. cost | Cost per case | "
             "Median latency | Mean tool calls |", "|---|---|---|---|---|---|---|---|"]
    total = 0.0
    for scores in runs:
        m = summarize(scores)
        total += m["cost_usd"]
        lines.append(f"| {config_name(scores)} | {m['n']} | {m['input_tokens']:,} | {m['output_tokens']:,} | "
                     f"${m['cost_usd']:.2f} | ${m['cost_per_case_usd']:.3f} | {m['median_latency_s']:.1f}s | "
                     f"{m['mean_tool_calls']:.1f} |")
    lines.append(f"| **Total** | | | | **${total:.2f}** | | | |")
    return "\n".join(lines)


def describe_miss(s: CaseScore) -> str:
    """What went wrong, mechanically. The README adds why."""
    if s.is_fallback:
        return f"fallback ({s.fallback_reason})"
    parts = []
    if s.unsafe:
        parts.append(f"UNSAFE: {s.unsafe_reason}")
    if not s.root_cause_correct:
        parts.append(f"said {s.predicted_root_cause}, truth {s.expected_root_cause}")
    if s.evidence_recall < 1:
        parts.append(f"evidence recall {s.evidence_recall:.2f} (missed {', '.join(s.missing_evidence)})")
    if s.over_escalated:
        parts.append("escalated a case that didn't need it"
                     + (f" (confidence {s.confidence:.2f} < 0.6 downgrade)" if s.policy_override else ""))
    if not s.safe_action and not s.unsafe:
        parts.append(f"action {s.proposed_action} not allowed for {s.expected_root_cause}")
    return "; ".join(parts)


def misses(runs: list[list[CaseScore]]) -> str:
    out = []
    for scores in runs:
        bad = [s for s in scores if not s.fully_correct]
        out.append(f"**{config_name(scores)}**: {len(bad)} miss{'es' if len(bad) != 1 else ''}")
        out += [f"- `{s.case_id}`{' (held-out)' if s.held_out else ''}: {describe_miss(s)}" for s in bad]
        out.append("")
    return "\n".join(out)
