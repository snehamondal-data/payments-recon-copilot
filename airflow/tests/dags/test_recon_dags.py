"""DAG integrity for recon_daily and recon_nightly_evals. Run with `astro dev pytest` (no Snowflake, no API)."""

from pathlib import Path

import pytest
from airflow.models import DagBag

DAGS_DIR = Path(__file__).resolve().parents[2] / "dags"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}
    assert set(dagbag.dag_ids) == {"recon_daily", "recon_nightly_evals", "recon_triage"}


def test_recon_daily_order(dagbag):
    dag = dagbag.dags["recon_daily"]
    order = {t.task_id: sorted(t.downstream_task_ids) for t in dag.tasks}
    assert order == {"load_new_messages": ["dbt_build"], "dbt_build": ["recheck_pending"],
                     "recheck_pending": ["summary"], "summary": []}


def test_recon_daily_runs_one_as_of_date_at_a_time(dagbag):
    dag = dagbag.dags["recon_daily"]
    assert dag.max_active_runs == 1 and dag.catchup is False
    assert all(t.retries >= 1 for t in dag.tasks)
    writers = {t.task_id for t in dag.tasks if t.pool == "recon_snowflake_writes"}
    assert writers == {"load_new_messages", "dbt_build", "recheck_pending"}



def test_the_copilot_is_not_in_the_daily_dag():
    source = (DAGS_DIR / "recon_daily.py").read_text()
    code = source.split('"""', 2)[2]  # skip the docstring, which explains why
    assert "recon.copilot" not in code and "anthropic" not in code.lower()


def test_nightly_evals_are_paused_by_default(dagbag):
    dag = dagbag.dags["recon_nightly_evals"]
    assert dag.is_paused_upon_creation is True
    assert dag.max_active_runs == 1 and dag.catchup is False
    assert dag.params["budget_usd"] <= 6


def test_dag_files_import_nothing_from_recon_at_parse_time():
    """Only the scheduler has the recon code mounted; parsing must not need it."""
    for path in DAGS_DIR.glob("*.py"):
        top_level = [line for line in path.read_text().splitlines() if line.startswith(("import ", "from "))]
        assert not any("recon" in line for line in top_level), path


def test_recon_triage_runs_on_the_daily_snapshot_asset(dagbag):
    dag = dagbag.dags["recon_triage"]
    daily = dagbag.dags["recon_daily"]
    [outlet] = daily.get_task("dbt_build").outlets
    assert outlet.name == "recon_daily_snapshot"
    assert [a.name for a in dag.timetable.asset_condition.objects] == ["recon_daily_snapshot"]
    order = {t.task_id: sorted(t.downstream_task_ids) for t in dag.tasks}
    assert order == {"triage": ["summary"], "summary": []}
    assert dag.max_active_runs == 1 and dag.catchup is False


def test_recon_triage_defaults_to_the_scripted_model_and_never_reviews():
    source = (DAGS_DIR / "recon_triage.py").read_text()
    code = source.split('"""', 2)[2]
    assert "approve" not in code and "resolve" not in code
    assert "TRIAGE_MODEL_MODE=fake` (the default)" in source
