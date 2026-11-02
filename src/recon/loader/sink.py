"""Validates a batch of raw messages and loads it idempotently. No Kafka imports, so it is unit-testable.

Order of operations for one batch (the caller commits Kafka offsets only after process() returns):
  1. validate every message against its topic's contract;
  2. publish rejects not already dead-lettered to the DLQ, and flush;
  3. in one warehouse transaction, insert valid rows and DLQ log rows with ON CONFLICT DO NOTHING.
A crash anywhere before the offset commit means the batch is redelivered and step 3 inserts nothing
new. A crash between steps 2 and 3 can send a DLQ message twice (keyed by source coordinates so it
can be deduped), but it can never lose one.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from recon.contracts import (
    BankSettlement,
    Contract,
    ContractError,
    DLQRecord,
    LedgerEntry,
    ProcessorEvent,
    parse_message,
)
from recon.fx import FxRate
from recon.loader.schema import SOURCE_TABLES
from recon.warehouse import WarehouseClient

INSERT_CHUNK_ROWS = 500


@dataclass(frozen=True)
class InsertSpec:
    """A multi-row INSERT ... ON CONFLICT DO NOTHING. One statement per chunk is ~30x faster than executemany."""

    table: str
    columns: tuple[str, ...]
    timestamptz: frozenset[str] = frozenset()

    def statement(self, n_rows: int) -> str:
        row = "(" + ", ".join("CAST(? AS TIMESTAMPTZ)" if c in self.timestamptz else "?" for c in self.columns) + ")"
        cols = ", ".join(f'"{c}"' for c in self.columns)
        return f"INSERT INTO {self.table} ({cols}) VALUES {', '.join([row] * n_rows)} ON CONFLICT DO NOTHING"


_KAFKA = ("_topic", "_partition", "_offset")
INSERTS = {
    "processor_events": InsertSpec("raw.processor_events", (
        "event_id", "schema_version", "event_type", "object_id", "charge_id", "amount_minor", "currency",
        "fx_rate", "settlement_amount_minor", "occurred_at", *_KAFKA), frozenset({"occurred_at"})),
    "bank_settlement_batches": InsertSpec("raw.bank_settlement_batches", (
        "event_id", "schema_version", "batch_id", "value_date", "currency", "batch_total_minor", "line_count",
        *_KAFKA)),
    "bank_settlement_lines": InsertSpec("raw.bank_settlement_lines", (
        "event_id", "line_no", "line_id", "batch_id", "reference", "direction", "description",
        "net_amount_minor")),
    "ledger_journals": InsertSpec("raw.ledger_journals", (
        "event_id", "schema_version", "journal_id", "charge_id", "entry_type", "currency", "posted_at",
        "posting_date", *_KAFKA), frozenset({"posted_at"})),
    "ledger_lines": InsertSpec("raw.ledger_lines", (
        "event_id", "line_no", "journal_id", "account", "debit_minor", "credit_minor")),
}
DLQ_LOG = InsertSpec("raw.dlq_log", ("topic", "partition", "offset", "key", "error_type", "error", "raw_value"))
PRIMARY_KEY_LENGTH = {"bank_settlement_lines": 2, "ledger_lines": 2}  # the rest: event_id only


def insert_rows(wh: WarehouseClient, spec: InsertSpec, rows: Sequence[tuple[Any, ...]]) -> None:
    for i in range(0, len(rows), INSERT_CHUNK_ROWS):
        chunk = rows[i:i + INSERT_CHUNK_ROWS]
        wh.execute(spec.statement(len(chunk)), [value for row in chunk for value in row])


@dataclass(frozen=True)
class RawMessage:
    topic: str  # physical topic name, including any prefix
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None


class DLQPublisher(Protocol):
    def publish(self, record: DLQRecord) -> None: ...

    def flush(self) -> None: ...


@dataclass
class BatchStats:
    received: int = 0
    valid: int = 0
    rejected: int = 0
    dead_lettered: int = 0  # rejects published to the DLQ for the first time
    inserted: dict[str, int] = field(default_factory=lambda: dict.fromkeys(SOURCE_TABLES, 0))

    def add(self, other: BatchStats) -> None:
        self.received += other.received
        self.valid += other.valid
        self.rejected += other.rejected
        self.dead_lettered += other.dead_lettered
        for table, n in other.inserted.items():
            self.inserted[table] += n

    @property
    def rows_inserted(self) -> int:
        return sum(self.inserted.values())


def to_rows(model: Contract, msg: RawMessage) -> dict[str, list[tuple[Any, ...]]]:
    kafka = (msg.topic, msg.partition, msg.offset)
    match model:
        case ProcessorEvent():
            return {"processor_events": [(
                model.event_id, model.schema_version, model.event_type, model.object_id, model.charge_id,
                model.amount_minor, model.currency, model.fx_rate, model.settlement_amount_minor,
                model.occurred_at.isoformat(), *kafka)]}
        case BankSettlement():
            return {
                "bank_settlement_batches": [(
                    model.event_id, model.schema_version, model.batch_id, model.value_date, model.currency,
                    model.batch_total_minor, len(model.lines), *kafka)],
                "bank_settlement_lines": [(
                    model.event_id, line.line_no, line.line_id, model.batch_id, line.reference, line.direction,
                    line.description, line.net_amount_minor) for line in model.lines],
            }
        case LedgerEntry():
            return {
                "ledger_journals": [(
                    model.event_id, model.schema_version, model.journal_id, model.charge_id, model.entry_type,
                    model.currency, model.posted_at.isoformat(), model.posting_date, *kafka)],
                "ledger_lines": [(
                    model.event_id, line.line_no, model.journal_id, line.account, line.debit_minor,
                    line.credit_minor) for line in model.lines],
            }
    raise TypeError(f"no row mapping for {type(model).__name__}")


class Sink:
    def __init__(self, wh: WarehouseClient, dlq: DLQPublisher, topic_prefix: str = ""):
        self.wh = wh
        self.dlq = dlq
        self.topic_prefix = topic_prefix

    def process(self, messages: Sequence[RawMessage]) -> BatchStats:
        stats = BatchStats(received=len(messages))
        rows: dict[str, dict[tuple, tuple]] = {table: {} for table in SOURCE_TABLES}
        rejects: list[tuple[RawMessage, ContractError]] = []

        for msg in messages:
            try:
                model = parse_message(msg.topic.removeprefix(self.topic_prefix), msg.value)
            except ContractError as err:
                # Drop the traceback: it points back at this frame and would keep the connection alive.
                rejects.append((msg, err.with_traceback(None)))
                continue
            stats.valid += 1
            for table, table_rows in to_rows(model, msg).items():
                for row in table_rows:
                    # Dedupe within the batch too; first occurrence wins, same as ON CONFLICT DO NOTHING.
                    rows[table].setdefault(row[:PRIMARY_KEY_LENGTH.get(table, 1)], row)
        stats.rejected = len(rejects)

        new_rejects = [(m, e) for m, e in rejects if not self._already_dead_lettered(m)]
        for msg, err in new_rejects:
            self.dlq.publish(DLQRecord.build(msg.topic, msg.partition, msg.offset, msg.key, msg.value, err))
        if new_rejects:
            self.dlq.flush()
        stats.dead_lettered = len(new_rejects)

        with self.wh.transaction():
            before = self._counts()
            for table, keyed in rows.items():
                insert_rows(self.wh, INSERTS[table], list(keyed.values()))
            insert_rows(self.wh, DLQ_LOG, [
                (m.topic, m.partition, m.offset, m.key.decode("utf-8", "replace") if m.key else None,
                 e.error_type, e.detail, m.value) for m, e in rejects])
            after = self._counts()
        stats.inserted = {table: after[table] - before[table] for table in SOURCE_TABLES}
        return stats

    def _already_dead_lettered(self, msg: RawMessage) -> bool:
        return bool(self.wh.execute(
            'SELECT 1 FROM raw.dlq_log WHERE topic = ? AND "partition" = ? AND "offset" = ?',
            (msg.topic, msg.partition, msg.offset)))

    def _counts(self) -> dict[str, int]:
        return {table: self.wh.execute(f"SELECT count(*) FROM raw.{table}")[0][0] for table in SOURCE_TABLES}


def load_fx_rates(wh: WarehouseClient, rates: Sequence[FxRate]) -> None:
    with wh.transaction():
        wh.executemany("INSERT INTO ref.fx_rates VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
                       [(r.rate_date, r.currency, r.rate_to_usd) for r in rates])
