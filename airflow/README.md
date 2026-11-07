# airflow/: orchestration (Slice 6)

An Astro CLI project (Astro Runtime 3.3, Airflow 3), run locally with `astro dev`. **Project-scale**:
one local Airflow, one local Kafka broker, one Snowflake trial account. All data is synthetic.

| DAG | What | Default |
|---|---|---|
| `recon_daily` | Kafka -> Snowflake load, `dbt build` with as-of = logical date, re-check pending settlements, log a summary | paused (Airflow's default); run it by backfill or unpause |
| `recon_triage` | On each `recon_daily_snapshot` asset event: the copilot proposes (never approves) for that date's new exceptions, through the MCP server, up to the caps | paused (Airflow's default); `TRIAGE_MODEL_MODE=fake` (scripted, no API calls) unless set to `live` |
| `recon_nightly_evals` | The Slice 4 evals on a private DuckDB copy | **paused**: each run calls the Claude API and costs money |

The copilot runs only in `recon_triage`, and there it only proposes: approving, rejecting and resolving
stay with a human (`python -m recon.copilot queue`, `show`, `approve`). The MCP server it uses runs in the
scheduler container and logs in as `RECON_AGENT` only; its access log is `/tmp/recon_mcp/mcp_access.jsonl`.

## Run it

From the repo root:

```bash
make up                 # Kafka first: the scheduler joins its network (recon_default)
make generate           # publish the scenario to the dev topics
make airflow-up         # astro dev start
make airflow-test       # DAG integrity tests (astro dev pytest)
make airflow-backfill FROM=2026-10-15 TO=2026-10-20
make airflow-down
```

## What runs where

- **Image** ([Dockerfile](Dockerfile)): Astro Runtime, the task-time Python deps from
  [requirements.txt](requirements.txt), and dbt in its own venv (`DBT_BIN`). It contains no repo code
  and no keys. `.dockerignore` also excludes `secrets/`, `*.p8` and `.env`.
- **Mounted at runtime, read-only, into the scheduler only** ([docker-compose.override.yml](docker-compose.override.yml)).
  Tasks run in the scheduler under the LocalExecutor, so it is the only container that gets:
  - `../src` (the recon package) and `../dbt` (the dbt project);
  - `../data/ref` (FX reference rates). Ground truth is in `data/` but is not mounted;
  - `../secrets` (the Snowflake key pairs) and the settings from `../.env`.

  The DAG files import nothing from recon at parse time, so the other containers need none of it.
- **Kafka**: the scheduler joins Kafka's compose network and uses the internal listener `kafka:19092`.

## Idempotency

- `load_new_messages` uses a fixed consumer group and stops at the end of each partition. Inserts are a
  `MERGE` on `event_id`, so a re-run inserts nothing it already has.
- `dbt_build` rebuilds the marts for the run's as-of date.
- `recheck_pending` replaces that date's rows in `MARTS.RECON_DAILY_RUNS` and
  `MARTS.RECON_WINDOW_HISTORY` in one transaction. It refuses to run if the marts were built for another
  date.
- `summary` only reads.
- `max_active_runs=1` (and `--max-active-runs 1` on backfills), because the marts hold one as-of date
  at a time.
