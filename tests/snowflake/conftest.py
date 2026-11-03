"""Runs against the Snowflake trial account (project-scale). Opt in with `pytest -m snowflake` (make test-snowflake).

One session-scoped pipeline run, through the real path: fresh Kafka topics -> loader -> RAW, then a replay
from the earliest offset, then `dbt build --target snowflake`. It rebuilds RECON's RAW and REF tables.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from dotenv import load_dotenv

from recon.config import load_settings
from recon.fx import write_fx_csv
from recon.generator.ground_truth import write_ground_truth
from recon.generator.publish import publish
from recon.loader import run_loader
from recon.loader.kafka_io import scratch_topics
from recon.parity import dbt_build, reset_snowflake, table_counts

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tests" / "copilot"))  # for fakes.FakeModelClient


@pytest.fixture(scope="session")
def settings():
    load_dotenv(ROOT / ".env")
    s = load_settings()
    if not s.snowflake.account:
        pytest.fail("SNOWFLAKE_ACCOUNT is not set in .env")
    return s


@pytest.fixture(scope="session")
def sf(settings):
    return settings.snowflake


@pytest.fixture(scope="session")
def sf_pipeline(settings, sf, scenario, tmp_path_factory):
    from recon.warehouse.snowflake_client import SnowflakeClient

    tmp = tmp_path_factory.mktemp("snowflake")
    fx_path, ground_truth_path = tmp / "fx_rates.csv", tmp / "ground_truth.duckdb"
    write_fx_csv(fx_path, scenario.fx_rates)
    write_ground_truth(ground_truth_path, scenario)  # local file for the tests; never uploaded to Snowflake
    wh = SnowflakeClient(sf, sf.loader)
    try:
        reset_snowflake(wh)
        with scratch_topics(settings.kafka_bootstrap, "it-sf") as prefix:
            assert publish(scenario, settings.kafka_bootstrap, prefix) == len(scenario.messages)
            kwargs = dict(bootstrap=settings.kafka_bootstrap, topic_prefix=prefix, fx_rates_path=fx_path,
                          log=lambda _: None)
            first = run_loader(wh, group_id=f"{prefix}loader", **kwargs)
            after_first = table_counts(wh)
            replay = run_loader(wh, group_id=f"{prefix}replay", **kwargs)
            after_replay = table_counts(wh)
    finally:
        wh.close()
    results = dbt_build("snowflake", scenario.as_of, tmp / "dbt",
                        {"SNOWFLAKE_LOADER_KEY_PATH": str(sf.loader.key_path.resolve())})
    return {"first": first, "replay": replay, "after_first": after_first, "after_replay": after_replay,
            "dbt": results, "ground_truth_path": ground_truth_path}
