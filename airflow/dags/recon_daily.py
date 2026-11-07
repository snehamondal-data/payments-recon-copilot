"""
### recon_daily

One reconciliation day on **Snowflake** (project-scale trial account), with the DAG run's **logical date
as the as-of date**. A backfill over a range of dates replays the reconciliation day by day, so pending
settlements turn into exceptions on the day their T+2 business-day window closes.

1. `load_new_messages`: read what is new on the Kafka source topics (internal listener), up to the
   current end of each partition, then exit. Idempotent by `event_id`. Runs as `RECON_LOADER`.
2. `dbt_build`: `dbt build --target snowflake` with `as_of` = the logical date, then snapshot that
   date's pending settlements and exceptions into `MARTS.RECON_DAILY_SNAPSHOT`, in the same task.
3. `recheck_pending`: compare this date's snapshot with the previous date's. Charges that were pending
   and are now exceptions are newly closed windows. Replaces this date's rows in `MARTS.RECON_DAILY_RUNS`
   and `MARTS.RECON_WINDOW_HISTORY`.
4. `summary`: log the backlog and the newly closed windows, from the snapshot. Read-only.

The marts hold one as-of date at a time (the last one built), so nothing after `dbt_build` reads them:
every task is safe to re-run for its logical date at any time, even after later dates were built.
The tasks that write to Snowflake share the one-slot pool `recon_snowflake_writes`, so a scheduled run
and a backfill can't interleave a build and its snapshot.

The copilot is not here on purpose. `dbt_build` emits the `recon_daily_snapshot` asset with its as-of date,
and the separate `recon_triage` DAG (Slice 9d) runs on it: it only proposes, and a human approves.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

import pendulum
from airflow.sdk import Asset, dag, get_current_context, task

log = logging.getLogger(__name__)
POOL = "recon_snowflake_writes"  # one slot (airflow_settings.yaml)
# Updated when a date's snapshot is written; the event's extra carries {"as_of": "YYYY-MM-DD"}.
SNAPSHOT = Asset("recon_daily_snapshot")


def as_of_date() -> date:
    """The logical date (UTC calendar day). A manual run without one falls back to when it was scheduled."""
    ctx = get_current_context()
    when = ctx.get("logical_date") or ctx["dag_run"].run_after
    return pendulum.instance(when).in_timezone("UTC").date()


def snowflake_loader():
    from recon.config import load_settings
    from recon.warehouse.snowflake_client import SnowflakeClient

    settings = load_settings()
    if settings.warehouse_engine != "snowflake":
        raise RuntimeError("recon_daily runs on Snowflake only (WAREHOUSE_ENGINE=snowflake)")
    return settings, SnowflakeClient(settings.snowflake, settings.snowflake.loader)


@dag(
    dag_id="recon_daily",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 10, 15, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # backfills: pass --max-active-runs 1 too (make airflow-backfill does)
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["recon", "snowflake"],
    doc_md=__doc__,
)
def recon_daily():
    @task(pool=POOL)
    def load_new_messages() -> dict:
        from recon.orchestration import load_new_messages as load

        settings, wh = snowflake_loader()
        try:
            stats = load(wh, settings, log=log.info)
        finally:
            wh.close()
        log.info("loaded: %s", stats)
        return stats

    @task(pool=POOL, outlets=[SNAPSHOT])
    def dbt_build() -> dict:
        from pathlib import Path

        from recon.orchestration import snapshot_marts
        from recon.parity import dbt_build as build, dbt_summary

        day = as_of_date()
        summary = dbt_summary(build("snowflake", day, Path(f"/tmp/recon_dbt/{day}")))
        log.info("dbt build --target snowflake --vars as_of=%s: %s", day, summary)
        _, wh = snowflake_loader()
        try:
            snapshot = snapshot_marts(wh, day)
        finally:
            wh.close()
        log.info("snapshot for %s: %s", day, snapshot)
        get_current_context()["outlet_events"][SNAPSHOT].extra = {"as_of": day.isoformat()}
        return summary | {"snapshot": snapshot}

    @task(pool=POOL)
    def recheck_pending() -> dict:
        from recon.orchestration import recheck_pending as recheck

        _, wh = snowflake_loader()
        try:
            return recheck(wh, as_of_date())
        finally:
            wh.close()

    @task
    def summary(recheck: dict) -> dict:
        from recon.orchestration import summarize, summary_lines

        _, wh = snowflake_loader()
        try:
            s = summarize(wh, as_of_date(), recheck)
        finally:
            wh.close()
        for line in summary_lines(s):
            log.info(line)
        return s

    checked = recheck_pending()
    load_new_messages() >> dbt_build() >> checked
    summary(checked)


recon_daily()
