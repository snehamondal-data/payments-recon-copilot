PY ?= .venv/bin/python

-include .env
export

.PHONY: venv up down generate load replay recon test test-integration test-live eval-estimate eval-report

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

test:
	$(PY) -m pytest

test-integration:
	$(PY) -m pytest -m integration

test-live:
	$(PY) -m pytest -m live -s

eval-estimate:
	$(PY) -m recon.evals estimate

eval-report:
	$(PY) -m recon.evals report
