"""
### recon_nightly_evals

**Paused by default: every run calls the Claude API and costs money** (Sonnet 5.5 standard measured
about $0.34 for the 30 cases). Unpause or trigger it only on purpose.

Runs the Slice 4 eval harness on a private DuckDB copy built inside the task from the generator
(fixed seed): never the host's DuckDB file and never Snowflake. The ground truth it scores against is
generated alongside it in the same scratch folder; the agent's tools never open that file.

1. `build_eval_warehouse`: generate the scenario, load it, `dbt build --target duckdb`.
2. `run_evals`: `python -m recon.evals run` for one model and mode, capped by `budget_usd`.
"""

from __future__ import annotations

import logging
from datetime import date

import pendulum
from airflow.sdk import Param, dag, get_current_context, task

log = logging.getLogger(__name__)
WORK_ROOT = "/tmp/recon_evals"


@dag(
    dag_id="recon_nightly_evals",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 10, 15, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    params={
        "model": Param("claude-sonnet-5-5", enum=["claude-sonnet-5-5", "claude-opus-5-5"]),
        "mode": Param("standard", enum=["standard", "blind"]),
        "budget_usd": Param(1.0, type="number", minimum=0, maximum=6),
        "seed": Param(42, type="integer"),
        "as_of": Param("2026-10-15", type="string", format="date"),
    },
    tags=["recon", "evals", "costs-money"],
    doc_md=__doc__,
)
def recon_nightly_evals():
    @task
    def build_eval_warehouse() -> dict:
        from pathlib import Path

        from recon.orchestration import build_eval_warehouse as build

        ctx = get_current_context()
        p = ctx["params"]
        work = Path(WORK_ROOT) / ctx["run_id"].replace(":", "_").replace("+", "_")
        paths = build(work, seed=int(p["seed"]), as_of=date.fromisoformat(p["as_of"]))
        return paths | {"WORK_DIR": str(work)}

    @task
    def run_evals(paths: dict) -> int:
        import os

        p = get_current_context()["params"]
        os.environ.update({k: v for k, v in paths.items() if k != "WORK_DIR"} | {"WAREHOUSE_ENGINE": "duckdb"})
        os.chdir(paths["WORK_DIR"])  # results and audit logs land in the scratch folder
        from recon.evals.__main__ import main

        code = main(["run", "--model", p["model"], "--mode", p["mode"], "--budget", str(p["budget_usd"])])
        if code:
            raise RuntimeError(f"recon.evals exited with {code}")
        return code

    run_evals(build_eval_warehouse())


recon_nightly_evals()
