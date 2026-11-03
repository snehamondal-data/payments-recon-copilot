"""The Astro project's safety properties, checked from the host (no Docker needed)."""

import re
from pathlib import Path

AIRFLOW = Path(__file__).resolve().parents[1] / "airflow"


def test_the_image_never_receives_keys_or_repo_code():
    dockerfile = (AIRFLOW / "Dockerfile").read_text()
    assert not re.search(r"^\s*(COPY|ADD)\b", dockerfile, flags=re.M | re.I)
    ignored = (AIRFLOW / ".dockerignore").read_text().split()
    assert {"secrets/", "*.p8", ".env"} <= set(ignored)


def test_keys_are_mounted_read_only_into_the_scheduler_only():
    override = (AIRFLOW / "docker-compose.override.yml").read_text()
    assert "../secrets:/opt/recon/secrets:ro" in override
    assert override.count("../secrets") == 1 and override.count("env_file") == 1
    assert "KAFKA_BOOTSTRAP: kafka:19092" in override  # the broker's internal listener


def test_ground_truth_is_not_mounted():
    override = (AIRFLOW / "docker-compose.override.yml").read_text()
    assert "../data/ref:" in override and "../data:" not in override and "ground_truth" not in override


def test_nightly_evals_are_paused_and_the_daily_dag_has_no_copilot():
    assert "is_paused_upon_creation=True" in (AIRFLOW / "dags" / "recon_nightly_evals.py").read_text()
    daily = (AIRFLOW / "dags" / "recon_daily.py").read_text().split('"""', 2)[2]
    assert "recon.copilot" not in daily


def test_the_snowflake_write_pool_has_one_slot_and_settings_hold_no_credentials():
    settings = (AIRFLOW / "airflow_settings.yaml").read_text()
    assert "pool_name: recon_snowflake_writes\n      pool_slot: 1" in settings
    assert not re.search(r"conn_(password|extra|login):[ \t]*\S", settings)
