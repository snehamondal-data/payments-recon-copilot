"""
### recon_triage

Scheduled triage (Slice 9d): the copilot **proposes** a diagnosis for each new exception. It never approves
or applies anything. Every proposal lands in the review queue as proposed, marked `proposed_by =
scheduled_triage` with this DAG run's ID, and a human approves, rejects or resolves it
(`python -m recon.copilot queue`).

Runs when `recon_daily`'s `dbt_build` updates the `recon_daily_snapshot` asset, for the as-of date in the
asset event (several queued events: each date, in order). Every run also catches up on snapshot dates
not yet finished, oldest first, because Airflow can drop an asset event that arrives while a triage run
is already queued. Trigger it by hand with `{"as_of": "2026-10-16"}` as the run conf to re-run one date,
which proposes nothing already proposed, or with no conf to only catch up.

1. `triage`: for each date, read its exceptions from the snapshot, start one MCP server pinned to that
   date (as `RECON_AGENT`, read-only), skip what already has a proposal for the same content, prompt
   version and model, and diagnose the rest up to `TRIAGE_MAX_EXCEPTIONS` (10) and `TRIAGE_MAX_COST_USD`
   (0.50) for the whole run. Proposals and audit events go to `RECON.COPILOT` as `RECON_LOADER`.
2. `summary`: log what was triaged, skipped and left over, and the cost.

`TRIAGE_MODEL_MODE=fake` (the default) uses a scripted model: no API calls, no cost. `live` uses `RECON_MODEL`.
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.sdk import Asset, dag, get_current_context, task

log = logging.getLogger(__name__)
SNAPSHOT = Asset("recon_daily_snapshot")  # emitted by recon_daily.dbt_build


def as_of_dates() -> list[str]:
    """The dates to triage: from the triggering asset events, or {"as_of": ...} in a manual run's conf."""
    ctx = get_current_context()
    conf = ctx["dag_run"].conf or {}
    if conf.get("as_of"):
        return [conf["as_of"]]
    events = ctx["triggering_asset_events"][SNAPSHOT] if ctx.get("triggering_asset_events") else []
    # A manual run without conf has none: it only catches up on unfinished dates.
    return sorted({e.extra["as_of"] for e in events if (e.extra or {}).get("as_of")})


@dag(
    dag_id="recon_triage",
    schedule=[SNAPSHOT],
    start_date=pendulum.datetime(2026, 10, 15, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # one triage at a time, so two runs never race to propose for the same exception
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["recon", "snowflake", "copilot"],
    doc_md=__doc__,
)
def recon_triage():
    @task
    def triage() -> list[dict]:
        from datetime import date

        from recon.config import load_settings
        from recon.triage import run

        settings = load_settings()
        if settings.warehouse_engine != "snowflake":
            raise RuntimeError("recon_triage runs on Snowflake only (WAREHOUSE_ENGINE=snowflake)")
        dates = [date.fromisoformat(d) for d in as_of_dates()]
        log.info("triaging %s in %s mode", [d.isoformat() for d in dates], settings.triage_model_mode)
        return run(dates, dag_run_id=get_current_context()["run_id"], settings=settings, log=log.info)

    @task
    def summary(results: list[dict]) -> list[dict]:
        from recon.triage import summary_lines

        for line in summary_lines(results):
            log.info(line)
        return [{k: v for k, v in r.items() if k != "proposals"} for r in results]

    summary(triage())


recon_triage()
