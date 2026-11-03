PY ?= .venv/bin/python

-include .env
export

.PHONY: venv up down generate load replay recon recon-snowflake parity test test-integration test-live test-snowflake eval-estimate eval-report airflow-up airflow-down airflow-test airflow-backfill airflow-report

venv:
	python3 -m venv .venv
	.venv/bin/pip install -q --upgrade pip
	.venv/bin/pip install -q -e '.[dev]'

up:
	docker compose up -d
	docker compose wait kafka-init

down:
	docker compose down -v

generate:
	$(PY) -m recon.generator

load:
	$(PY) -m recon.loader

replay:
	$(PY) -m recon.loader --from-beginning

AS_OF ?= 2026-10-15
recon:
	.venv/bin/dbt build --project-dir dbt --profiles-dir dbt --vars "{as_of: '$(AS_OF)'}"

# Snowflake (project-scale trial). Load with: WAREHOUSE_ENGINE=snowflake make load
recon-snowflake:
	.venv/bin/dbt build --project-dir dbt --profiles-dir dbt --target snowflake --vars "{as_of: '$(AS_OF)'}"

# The whole pipeline on both engines from fresh Kafka topics, then compare. Needs `make up`.
parity:
	$(PY) -m recon.parity

test:
	$(PY) -m pytest

test-integration:
	$(PY) -m pytest -m integration

test-live:
	$(PY) -m pytest -m live -s

test-snowflake:
	$(PY) -m pytest -m snowflake tests/snowflake

eval-estimate:
	$(PY) -m recon.evals estimate

eval-report:
	$(PY) -m recon.evals report

# Airflow (Slice 6, Astro CLI, project-scale). Needs `make up` first: the scheduler joins Kafka's network.
airflow-up:
	cd airflow && astro dev start --no-browser

airflow-down:
	cd airflow && astro dev stop

airflow-test:
	cd airflow && astro dev pytest tests/dags

FROM ?= 2026-10-15
TO ?= 2026-10-20
# One run at a time: the marts hold one as-of date. Re-running a date is safe.
airflow-backfill:
	cd airflow && astro dev run backfill create --dag-id recon_daily --from-date $(FROM) --to-date $(TO) \
		--max-active-runs 1 --reprocess-behavior completed

# What each recon_daily run recorded on Snowflake (pending, closed windows, backlog), per as-of date.
airflow-report:
	$(PY) -m recon.orchestration report --from $(FROM) --to $(TO)
