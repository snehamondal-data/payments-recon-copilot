"""Raw-layer tables in the warehouse. Every source row keeps its Kafka coordinates for lineage."""

from __future__ import annotations

from recon.warehouse import WarehouseClient

_KAFKA_COLUMNS = """
    _topic VARCHAR NOT NULL,
    _partition INTEGER NOT NULL,
    _offset BIGINT NOT NULL,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp"""

DDL = [
    "CREATE SCHEMA IF NOT EXISTS raw",
    "CREATE SCHEMA IF NOT EXISTS ref",
    f"""CREATE TABLE IF NOT EXISTS raw.processor_events (
        event_id VARCHAR PRIMARY KEY,
        schema_version INTEGER NOT NULL,
        event_type VARCHAR NOT NULL,
        object_id VARCHAR NOT NULL,
        charge_id VARCHAR NOT NULL,
        amount_minor BIGINT NOT NULL,
        currency VARCHAR NOT NULL,
        fx_rate DECIMAL(12, 6) NOT NULL,
        settlement_amount_minor BIGINT NOT NULL,
        occurred_at TIMESTAMPTZ NOT NULL,{_KAFKA_COLUMNS})""",
    f"""CREATE TABLE IF NOT EXISTS raw.bank_settlement_batches (
        event_id VARCHAR PRIMARY KEY,
        schema_version INTEGER NOT NULL,
        batch_id VARCHAR NOT NULL,
        value_date DATE NOT NULL,
        currency VARCHAR NOT NULL,
        batch_total_minor BIGINT NOT NULL,
        line_count INTEGER NOT NULL,{_KAFKA_COLUMNS})""",
    """CREATE TABLE IF NOT EXISTS raw.bank_settlement_lines (
        event_id VARCHAR NOT NULL,
        line_no INTEGER NOT NULL,
        line_id VARCHAR NOT NULL,
        batch_id VARCHAR NOT NULL,
        reference VARCHAR,
        direction VARCHAR NOT NULL,
        description VARCHAR NOT NULL,
        net_amount_minor BIGINT NOT NULL,
        PRIMARY KEY (event_id, line_no))""",
    f"""CREATE TABLE IF NOT EXISTS raw.ledger_journals (
        event_id VARCHAR PRIMARY KEY,
        schema_version INTEGER NOT NULL,
        journal_id VARCHAR NOT NULL,
        charge_id VARCHAR NOT NULL,
        entry_type VARCHAR NOT NULL,
        currency VARCHAR NOT NULL,
        posted_at TIMESTAMPTZ NOT NULL,
        posting_date DATE NOT NULL,{_KAFKA_COLUMNS})""",
    """CREATE TABLE IF NOT EXISTS raw.ledger_lines (
        event_id VARCHAR NOT NULL,
        line_no INTEGER NOT NULL,
        journal_id VARCHAR NOT NULL,
        account VARCHAR NOT NULL,
        debit_minor BIGINT NOT NULL,
        credit_minor BIGINT NOT NULL,
        PRIMARY KEY (event_id, line_no))""",
    """CREATE TABLE IF NOT EXISTS raw.dlq_log (
        topic VARCHAR NOT NULL,
        "partition" INTEGER NOT NULL,
        "offset" BIGINT NOT NULL,
        key VARCHAR,
        error_type VARCHAR NOT NULL,
        error VARCHAR NOT NULL,
        raw_value BLOB,
        logged_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
        PRIMARY KEY (topic, "partition", "offset"))""",
    """CREATE TABLE IF NOT EXISTS ref.fx_rates (
        rate_date DATE NOT NULL,
        currency VARCHAR NOT NULL,
        rate_to_usd DECIMAL(12, 6) NOT NULL,
        PRIMARY KEY (rate_date, currency))""",
]

SOURCE_TABLES = (
    "processor_events",
    "bank_settlement_batches",
    "bank_settlement_lines",
    "ledger_journals",
    "ledger_lines",
)


def ensure_schema(wh: WarehouseClient) -> None:
    for statement in DDL:
        wh.execute(statement)
