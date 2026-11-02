PY ?= .venv/bin/python

-include .env
export

.PHONY: venv up down generate load replay test test-integration

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

test:
	$(PY) -m pytest

test-integration:
	$(PY) -m pytest -m integration
