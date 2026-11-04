"""The ask eval's scoring, with the fake model: exact numbers or nothing."""

from __future__ import annotations

import pytest

from fakes import FakeModelClient, call
from recon.copilot.tools import open_read_only
from recon.evals.semantic import QUESTIONS, figure_truth, rescore, run_semantic_eval, score, truth
from recon.semantic.ask import QUERY, SUBMIT, Asker
from recon.semantic.layer import MetricQuery, SemanticLayer

# What a correct agent would query for each question.
QUERIES = {
    "q1-risk-by-reason": [{"metric": "amount_at_risk", "dimensions": ["reason_code"]}],
    "q2-queue-and-money": [{"metric": "exception_count"}, {"metric": "exceptions_with_money_at_stake"}],
    "q3-match-rate-by-currency": [{"metric": "match_rate", "dimensions": ["currency"]}],
    "q4-pending-by-currency": [{"metric": "pending_count", "dimensions": ["currency"]}],
    "q5-fee-and-chargeback-combined": [{"metric": "amount_at_risk", "filters": [
        {"dimension": "reason_code", "op": "in", "values": ["fee_mismatch", "chargeback"]}]}],
}


@pytest.fixture(scope="module")
def layer(recon_db):
    s = SemanticLayer(recon_db[0].parent / "target" / "semantic_manifest.json", recon_db[0])
    yield s
    s.close()


@pytest.fixture(scope="module")
def con(recon_db):
    c = open_read_only(recon_db[0])
    yield c
    c.close()


def perfect_script(layer, qid, tamper=None):
    """Query what's needed, then submit every returned non-empty row as a figure."""
    script, figures = [], []
    for args in QUERIES[qid]:
        script.append(call(QUERY, **args))
        q = MetricQuery.model_validate(args)
        for row in layer.query(q)["rows"]:
            group = {d: row[d] for d in q.dimensions}
            if any(v is None for v in group.values()):
                continue
            figures.append({"metric": q.metric, "group": group, "filters": [f.model_dump() for f in q.filters],
                            "value": row["value"]})
    if tamper:
        figures = tamper(figures)
    return script + [call(SUBMIT, answerable=True, answer="Here you go.", figures=figures)]


@pytest.mark.parametrize("question", QUESTIONS, ids=lambda q: q.qid)
def test_a_perfect_answer_scores_correct(layer, con, question):
    run = Asker(FakeModelClient(perfect_script(layer, question.qid)), layer).ask(question.text)
    s = score(question, truth(con, question), run, con)
    assert s.correct, (s.missing, s.wrong)


def test_truths_are_the_numbers_we_expect(con):
    t = {q.qid: truth(con, q) for q in QUESTIONS}
    assert sorted(t["q2-queue-and-money"][None]) == ["27", "30"]
    assert t["q4-pending-by-currency"] == {"EUR": ["0"], "GBP": ["0"], "USD": ["3"]}
    assert t["q1-risk-by-reason"]["missing_settlement"] == ["1154.4"]


def test_a_wrong_number_is_not_correct(layer, con):
    q1 = QUESTIONS[0]
    # A figure off by a cent fails grounding first; if it ever got through, scoring would still fail it.
    run = Asker(FakeModelClient(perfect_script(layer, q1.qid)), layer).ask(q1.text)
    run.answer.figures[0].value = "0.01"
    assert not score(q1, truth(con, q1), run, con).correct


def test_a_missing_figure_is_not_correct(layer, con):
    q1 = QUESTIONS[0]
    run = Asker(FakeModelClient(perfect_script(layer, q1.qid, tamper=lambda f: f[1:])), layer).ask(q1.text)
    s = score(q1, truth(con, q1), run, con)
    assert not s.correct and s.missing


def test_an_honest_refusal_on_an_answerable_question_is_not_correct(layer, con):
    q = QUESTIONS[4]
    run = Asker(FakeModelClient([call(SUBMIT, answerable=False, answer="Can't.")]), layer).ask(q.text)
    assert not score(q, truth(con, q), run, con).correct


def test_results_are_never_overwritten(layer, con, tmp_path):
    out = tmp_path / "r.jsonl"
    out.write_text("{}\n")
    with pytest.raises(FileExistsError):
        run_semantic_eval(Asker(FakeModelClient([]), layer), con, out)


def test_extra_figures_that_sql_confirms_are_fine_and_ones_it_contradicts_are_not(layer, con):
    """The cases from the first live run: a true total, a true breakdown, the true 0.00 for no reason code."""
    q4, q5 = QUESTIONS[3], QUESTIONS[4]
    extra_total = [call(QUERY, metric="pending_count"), call(QUERY, metric="pending_count", dimensions=["currency"]),
                   call(SUBMIT, answerable=True, answer="3, all USD.", figures=[
                       {"metric": "pending_count", "group": {}, "value": 3},
                       {"metric": "pending_count", "group": {"currency": "USD"}, "value": 3}])]
    assert score(q4, truth(con, q4), Asker(FakeModelClient(extra_total), layer).ask(q4.text), con).correct
    combined = perfect_script(layer, q5.qid)
    run = Asker(FakeModelClient(combined), layer).ask(q5.text)
    run.answer.figures.append(run.answer.figures[0].model_copy(update={"group": {"reason_code": "chargeback"},
                                                                       "filters": [], "value": "1.00"}))
    s = score(q5, truth(con, q5), run, con)
    assert not s.correct and s.wrong[0]["sql_says"] == "728.4"


def test_figure_truth_is_plain_sql(con):
    assert figure_truth(con, "amount_at_risk", {"reason_code": None}) == "0"
    assert figure_truth(con, "exception_count", {}) == "30"
    assert figure_truth(con, "pending_count", {"currency": "EUR"}) == "0"
    assert figure_truth(con, "match_rate", {"reason_code": "fee_mismatch"}) is None  # can't be checked: wrong


def test_rescore_reads_saved_answers_without_calling_a_model(layer, con, tmp_path):
    out = tmp_path / "r.jsonl"
    run_semantic_eval(Asker(FakeModelClient(perfect_script(layer, QUESTIONS[0].qid)), layer), con, out, log=lambda _: None)
    first = out.read_text().splitlines()[0]
    out.write_text(first + "\n")
    [again] = rescore(con, out)
    assert again.qid == QUESTIONS[0].qid and again.correct


def test_a_filtered_total_is_checked_with_its_filter(con):
    both = [{"dimension": "reason_code", "op": "in", "values": ["fee_mismatch", "chargeback"]}]
    assert figure_truth(con, "amount_at_risk", {}, both) == "729.36"
    assert figure_truth(con, "amount_at_risk", {}) == "4208.88"  # the same number with no filter is not this
    assert figure_truth(con, "exception_count", {}, [{"dimension": "window_status", "op": "=", "values": ["matched"]}]) == "0"
