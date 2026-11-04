"""The ask agent, with the scripted fake model: one data tool, grounded figures, honest refusals."""

from __future__ import annotations

import pytest

from fakes import FakeModelClient, call, calls, text, tool_use
from recon.config import load_settings
from recon.copilot.__main__ import main
from recon.semantic.ask import QUERY, SUBMIT, AskConfig, Asker, render, tool_definitions
from recon.semantic.layer import MetricQuery, SemanticLayer


@pytest.fixture(scope="module")
def layer(recon_db):
    s = SemanticLayer(recon_db[0].parent / "target" / "semantic_manifest.json", recon_db[0])
    yield s
    s.close()


def by_reason(layer):
    return [r for r in layer.query(MetricQuery(metric="amount_at_risk", dimensions=["reason_code"]))["rows"]
            if r["reason_code"]]


def query(**args):
    return call(QUERY, **args)


def answer(**fields):
    return call(SUBMIT, **fields)


def test_only_two_tools_and_no_free_text_sql(layer):
    tools = tool_definitions()
    assert [t["name"] for t in tools] == [QUERY, SUBMIT]
    props = tools[0]["input_schema"]["properties"]
    assert set(props) == {"metric", "dimensions", "filters"}
    assert tools[0]["input_schema"].get("additionalProperties") is False


def test_a_grounded_answer_is_accepted(layer):
    rows = by_reason(layer)
    figures = [{"metric": "amount_at_risk", "group": {"reason_code": r["reason_code"]}, "value": r["value"]} for r in rows]
    run = Asker(FakeModelClient([query(metric="amount_at_risk", dimensions=["reason_code"]),
                                 answer(answerable=True, answer="Money at risk by reason code.", figures=figures)]),
                layer).ask("how much money is at risk by reason code?")
    assert not run.is_fallback and run.answer.answerable and len(run.answer.figures) == len(rows)
    out = render(run, layer)
    assert "amount_at_risk [reason_code=missing_settlement]: $1154.40" in out


def test_the_same_number_spelled_differently_is_accepted(layer):
    r = next(r for r in by_reason(layer) if r["value"].endswith("0"))  # e.g. "728.40"
    figure = {"metric": "amount_at_risk", "group": {"reason_code": r["reason_code"]}, "value": float(r["value"])}
    run = Asker(FakeModelClient([query(metric="amount_at_risk", dimensions=["reason_code"]),
                                 answer(answerable=True, answer="x", figures=[figure])]), layer).ask("q")
    assert not run.is_fallback


def test_a_number_no_query_returned_is_rejected_then_falls_back(layer):
    made_up = {"metric": "amount_at_risk", "group": {}, "value": "123456.78"}
    run = Asker(FakeModelClient([query(metric="amount_at_risk"),
                                 answer(answerable=True, answer="x", figures=[made_up]),
                                 answer(answerable=True, answer="x", figures=[made_up])]), layer).ask("q")
    assert run.is_fallback and "invalid answer" in run.fallback_reason
    assert "not returned by query_metric" in run.validation_failures[0][0]
    assert not run.answer.answerable and run.answer.figures == []


def test_its_own_arithmetic_is_not_a_grounded_figure(layer):
    rows = {r["reason_code"]: r["value"] for r in by_reason(layer)}
    summed = str(sum(float(rows[c]) for c in ("fee_mismatch", "chargeback")))
    script = [query(metric="amount_at_risk", dimensions=["reason_code"]),
              answer(answerable=True, answer="x", figures=[{"metric": "amount_at_risk", "group": {}, "value": summed}])]
    run = Asker(FakeModelClient(script), layer).ask("fee mismatches and chargebacks combined?")
    assert run.validation_failures  # it has to query the combined number with a filter instead


def test_an_unanswerable_question_says_so_and_lists_what_it_can_answer(layer):
    run = Asker(FakeModelClient([answer(answerable=False, answer="I can't tell time to resolve from these metrics.")]),
                layer).ask("what was the median time to resolve last week?")
    assert not run.is_fallback and not run.answer.answerable
    out = render(run, layer)
    assert "I can answer questions about these metrics" in out
    for metric in ("exception_count", "amount_at_risk", "open_backlog", "pending_count", "match_rate"):
        assert metric in out


def test_unanswerable_with_figures_is_invalid(layer):
    run = Asker(FakeModelClient([answer(answerable=False, answer="x",
                                        figures=[{"metric": "pending_count", "group": {}, "value": 3}])] * 2),
                layer).ask("q")
    assert run.is_fallback


@pytest.mark.parametrize("args", [
    {"metric": "exception_count", "sql": "select * from raw.processor_events"},
    {"metric": "exception_count", "filters": [{"dimension": "currency", "op": "=", "values": ["x' OR '1'='1"]}]},
    {"metric": "time_to_resolve"},
])
def test_bad_query_arguments_are_tool_errors(layer, args):
    run = Asker(FakeModelClient([query(**args), answer(answerable=False, answer="Can't answer.")]), layer).ask("q")
    assert run.queries[0].get("error") and "rows" not in run.queries[0]
    assert not run.is_fallback  # the model was told, and answered honestly


def test_any_other_tool_is_refused(layer):
    run = Asker(FakeModelClient([call("run_sql", query="select 1"), answer(answerable=False, answer="No.")]),
                layer).ask("q")
    assert run.queries == [] and not run.answer.answerable


def test_the_query_budget_is_enforced(layer):
    many = calls(*[tool_use(QUERY, metric="pending_count") for _ in range(4)])
    run = Asker(FakeModelClient([many, many, answer(answerable=False, answer="x")]), layer,
                AskConfig(max_tool_calls=5)).ask("q")
    assert sum("rows" in qr for qr in run.queries) == 5


def test_api_errors_and_silence_fall_back(layer):
    assert Asker(FakeModelClient([RuntimeError("boom")]), layer).ask("q").is_fallback
    assert Asker(FakeModelClient([text("hmm"), text("hmm")]), layer).ask("q").is_fallback


def test_cli_ask(recon_db, tmp_path):
    settings = load_settings({"DUCKDB_PATH": str(recon_db[0]), "GROUND_TRUTH_PATH": str(recon_db[1]),
                              "COPILOT_DB_PATH": str(tmp_path / "copilot.duckdb"),
                              "SEMANTIC_MANIFEST_PATH": str(recon_db[0].parent / "target" / "semantic_manifest.json")})
    out: list[str] = []
    script = [query(metric="pending_count"),
              answer(answerable=True, answer="3 charges are pending.",
                     figures=[{"metric": "pending_count", "group": {}, "value": 3}])]
    assert main(["ask", "how many settlements are pending?"], settings=settings,
                client_factory=lambda s: FakeModelClient(script), out=out.append) == 0
    assert "3 charges are pending." in out[0] and "pending_count [total]: 3" in out[0]
    assert not (tmp_path / "copilot.duckdb").exists()  # ask never opens the copilot's store


def test_a_filtered_figure_must_carry_its_filter(layer):
    both = [{"dimension": "reason_code", "op": "in", "values": ["fee_mismatch", "chargeback"]}]
    value = layer.query(MetricQuery(metric="amount_at_risk", filters=both))["rows"][0]["value"]
    query_call = query(metric="amount_at_risk", filters=both)
    without = {"metric": "amount_at_risk", "group": {}, "value": value}
    run = Asker(FakeModelClient([query_call, answer(answerable=True, answer="x", figures=[without]),
                                 answer(answerable=True, answer="x", figures=[without | {"filters": both}])]),
                layer).ask("fee mismatches and chargebacks combined?")
    assert run.validation_failures and not run.is_fallback  # rejected without the filter, accepted with it
    assert "reason_code in fee_mismatch,chargeback" in render(run, layer)
