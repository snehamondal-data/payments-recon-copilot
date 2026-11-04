"""Eval for `ask`: five questions, answers computed by plain SQL on the marts, scored on exact numbers.

The expected numbers never go through the semantic layer: they come from recon_exceptions,
recon_pending, recon_matches and the charges table directly. A question is correct when every
expected non-zero figure appears with exactly the right value (to the cent, to 4 decimals for a rate,
exact for a count), and no stated figure contradicts the SQL. Every figure the agent states, including
extra ones the question didn't ask for, is checked against an independent plain-SQL value for its
metric and dimension values (figure_truth); a figure that can't be checked counts as wrong.

python -m recon.evals semantic --model claude-sonnet-5-5
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any

from recon.evals.pricing import cost_usd
from recon.semantic.ask import ASK_PROMPT_VERSION, AskResult, _number

# Metrics renamed after a run was recorded. Saved answers keep the name the model used; scoring maps it.
LEGACY_METRIC_NAMES = {"open_backlog": "exceptions_with_money_at_stake"}


@dataclass(frozen=True)
class Question:
    qid: str
    text: str
    key: str | None  # the dimension the answer is broken down by, or None for unkeyed totals
    unit: str  # "USD" | "count" | "fraction"
    truth_sql: str  # returns (key, value) rows, or (value,) rows when key is None


QUESTIONS = (
    Question("q1-risk-by-reason", "How much money is at risk by reason code?", "reason_code", "USD",
             "SELECT reason_code, sum(amount_minor) FROM marts.recon_exceptions GROUP BY 1"),
    Question("q2-queue-and-money", "How many exceptions are in the queue, and how many of them still have money "
             "at stake?", None, "count",
             "SELECT count(*) FROM marts.recon_exceptions UNION ALL "
             "SELECT count(*) FROM marts.recon_exceptions WHERE amount_minor > 0"),
    Question("q3-match-rate-by-currency", "What is our match rate for each currency?", "currency", "fraction",
             "SELECT c.currency, count(m.charge_id) * 1.0 / count(*) FROM intermediate.int_processor_charges AS c "
             "LEFT JOIN marts.recon_matches AS m ON m.charge_id = c.charge_id GROUP BY 1"),
    Question("q4-pending-by-currency", "How many charges are still waiting to settle inside their window, and in "
             "which currencies?", "currency", "count",
             "SELECT c.currency, count(p.charge_id) FROM (SELECT DISTINCT currency FROM intermediate.int_processor_charges) AS c "
             "LEFT JOIN marts.recon_pending AS p ON p.currency = c.currency GROUP BY 1"),
    Question("q5-fee-and-chargeback-combined", "How much money is at risk from fee mismatches and chargebacks "
             "combined?", None, "USD",
             "SELECT sum(amount_minor) FROM marts.recon_exceptions WHERE reason_code IN ('fee_mismatch', 'chargeback')"),
)


def _value(raw: Any, unit: str) -> str:
    if unit == "USD":
        return _number((Decimal(int(raw)) / 100).quantize(Decimal("0.01")))
    if unit == "fraction":
        return _number(round(float(raw), 4))
    return _number(int(raw))


def truth(con, question: Question) -> dict[str | None, list[str]]:
    """{key: [value]} for keyed questions; {None: [values...]} for totals."""
    rows = con.execute(question.truth_sql).fetchall()
    if question.key is None:
        return {None: [_value(r[0], question.unit) for r in rows]}
    return {r[0]: [_value(r[1], question.unit)] for r in rows}


@dataclass
class SemanticScore:
    qid: str
    question: str
    model: str
    correct: bool
    prompt_version: str = "ask-v1"  # the first run predates this field; it was ask-v1
    missing: list[Any] = field(default_factory=list)
    wrong: list[Any] = field(default_factory=list)
    answerable: bool = True
    is_fallback: bool = False
    fallback_reason: str | None = None
    answer: str = ""
    figures: list[dict[str, Any]] = field(default_factory=list)
    expected: dict[str, list[str]] = field(default_factory=dict)
    queries: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    latency_s: float = 0.0


# --- an independent SQL value for any figure (metric + dimension values) -------------------------

_ITEM_CURRENCY = """coalesce(c.currency, b.currency)"""
_QUEUE_FROM = """FROM marts.recon_exceptions AS e
    LEFT JOIN intermediate.int_processor_charges AS c ON c.charge_id = e.charge_id
    LEFT JOIN staging.stg_bank_settlement_lines AS l ON l.line_id = e.bank_line_id
    LEFT JOIN raw.bank_settlement_batches AS b ON b.batch_id = l.batch_id"""
_QUEUE = {"exception_count": "count(*)", "amount_at_risk": "coalesce(sum(e.amount_minor), 0)",
          "exceptions_with_money_at_stake": "count(*) FILTER (WHERE e.amount_minor > 0)"}
_UNIT = {"exception_count": "count", "amount_at_risk": "USD", "exceptions_with_money_at_stake": "count", "pending_count": "count",
         "match_rate": "fraction"}


def _condition(column: str, op: str, values: list[str]) -> tuple[str, list[str]]:
    if op in ("=", "!="):
        return f"{column} {op} ?", [values[0]]
    return f"{column} {op.upper()} ({', '.join('?' * len(values))})", list(values)


def _holds(value: str, op: str, values: list[str]) -> bool:
    return (value in values) == (op in ("=", "in"))


def figure_truth(con, metric: str, group: dict[str, Any], filters: list[dict[str, Any]] = ()) -> str | None:
    """The plain-SQL value of `metric` for items with these dimension values and filters, or None if it can't
    be checked. Filters follow SQL: a filter on reason_code never matches an item that has none."""
    conds = [(d, "=", [v]) for d, v in group.items()] + [(f["dimension"], f["op"], f["values"]) for f in filters]
    zero = _value(0, _UNIT.get(metric, "count")) if metric in _UNIT else None
    where, params = [], []
    if metric in _QUEUE:
        for dim, op, values in conds:
            if dim == "reason_code" and values == [None]:
                return zero  # items with no reason code are matched or pending: nothing queued, nothing at stake
            if dim == "window_status":
                if not _holds("exception", op, values):
                    return zero
                continue
            column = {"reason_code": "e.reason_code", "currency": _ITEM_CURRENCY,
                      "as_of_date": "CAST(e.as_of_date AS VARCHAR)"}[dim]
            c, p = _condition(column, op, values)
            where.append(c), params.extend(p)
        sql = f"SELECT {_QUEUE[metric]} {_QUEUE_FROM} {'WHERE ' + ' AND '.join(where) if where else ''}"
    elif metric == "pending_count":
        for dim, op, values in conds:
            if dim == "reason_code":
                if values == [None] and op == "=":
                    continue  # every pending item has no reason code
                return zero
            if dim == "window_status":
                if not _holds("pending_in_window", op, values):
                    return zero
                continue
            column = {"currency": "p.currency", "as_of_date": "CAST(p.as_of_date AS VARCHAR)"}[dim]
            c, p = _condition(column, op, values)
            where.append(c), params.extend(p)
        sql = f"SELECT count(*) FROM marts.recon_pending AS p {'WHERE ' + ' AND '.join(where) if where else ''}"
    elif metric == "match_rate":
        for dim, op, values in conds:
            if dim != "currency":
                return None
            c, p = _condition("c.currency", op, values)
            where.append(c), params.extend(p)
        sql = ("SELECT count(m.charge_id) * 1.0 / nullif(count(*), 0) FROM intermediate.int_processor_charges AS c "
               "LEFT JOIN marts.recon_matches AS m ON m.charge_id = c.charge_id "
               f"{'WHERE ' + ' AND '.join(where) if where else ''}")
    else:
        return None
    value = con.execute(sql, params).fetchone()[0]
    return None if value is None else _value(value, _UNIT[metric])


def score_figures(question: Question, expected: dict[str | None, list[str]], figures: list[dict[str, Any]],
                  con) -> tuple[list[Any], list[Any]]:
    """(missing, wrong). Missing: an expected non-zero figure not stated. Wrong: a stated figure SQL disagrees with."""
    zero = _value(0, question.unit)
    wrong = []
    for f in figures:
        metric = LEGACY_METRIC_NAMES.get(f["metric"], f["metric"])
        actual = figure_truth(con, metric, f["group"], f.get("filters", []))
        if actual is None or _number(f["value"]) != actual:
            wrong.append(f | {"sql_says": actual})
    if question.key is None:
        stated = [_number(f["value"]) for f in figures if not f["group"]]  # totals, filtered or not
        missing = [v for v in expected[None] if v != zero and v not in stated]
    else:
        stated = {f["group"][question.key]: _number(f["value"]) for f in figures
                  if question.key in f["group"] and f["group"][question.key] is not None}
        missing = [(k, v[0]) for k, v in expected.items() if v[0] != zero and stated.get(k) != v[0]]
    return missing, wrong


def score(question: Question, expected: dict[str | None, list[str]], run: AskResult, con) -> SemanticScore:
    a = run.answer
    figures = [f.model_dump() for f in a.figures]
    missing, wrong = score_figures(question, expected, figures, con)
    s = SemanticScore(
        question.qid, question.text, run.model, correct=a.answerable and not run.is_fallback and not missing and not wrong,
        missing=missing, wrong=wrong, answerable=a.answerable, is_fallback=run.is_fallback,
        fallback_reason=run.fallback_reason, answer=a.answer, figures=figures,
        expected={str(k): v for k, v in expected.items()}, queries=len(run.queries), input_tokens=run.input_tokens,
        output_tokens=run.output_tokens, latency_s=run.latency_s, prompt_version=ASK_PROMPT_VERSION)
    s.cost_usd = cost_usd(run.model, input_tokens=run.input_tokens, output_tokens=run.output_tokens,
                          cache_creation_input_tokens=run.cache_creation_input_tokens,
                          cache_read_input_tokens=run.cache_read_input_tokens)
    return s


def rescore(con, path: Path) -> list[SemanticScore]:
    """Score saved answers again with the current scorer. No model call: the answers are what was recorded."""
    by_id = {q.qid: q for q in QUESTIONS}
    out = []
    for line in path.read_text().splitlines():
        saved = SemanticScore(**json.loads(line))
        question = by_id[saved.qid]
        missing, wrong = score_figures(question, truth(con, question), saved.figures, con)
        saved.missing, saved.wrong = missing, wrong
        saved.correct = saved.answerable and not saved.is_fallback and not missing and not wrong
        out.append(saved)
    return out


def run_semantic_eval(asker, con, out_path: Path, log=print) -> list[SemanticScore]:
    if out_path.exists():
        raise FileExistsError(f"{out_path} exists: these are measured results; move it aside to run again")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    scores = []
    for question in QUESTIONS:
        s = score(question, truth(con, question), asker.ask(question.text), con)
        scores.append(s)
        with out_path.open("a") as fh:
            fh.write(json.dumps(asdict(s), default=str) + "\n")
        log(f"{s.qid}: {'correct' if s.correct else 'WRONG'} ({s.queries} queries, ${s.cost_usd:.4f})")
    return scores


def report(scores: list[SemanticScore]) -> str:
    lines = ["| Question | Correct | Metric queries | Cost | Latency |", "|---|---|---|---|---|"]
    for s in scores:
        verdict = "yes" if s.correct else ("fallback" if s.is_fallback else "no")
        lines.append(f"| {s.question} | {verdict} | {s.queries} | ${s.cost_usd:.4f} | {s.latency_s:.1f}s |")
    n = sum(s.correct for s in scores)
    lines.append(f"| **Total** | **{n}/{len(scores)}** | | **${sum(s.cost_usd for s in scores):.3f}** | |")
    return "\n".join(lines)
