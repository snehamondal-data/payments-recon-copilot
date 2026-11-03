"""Kafka -> warehouse (DuckDB or Snowflake) loader: contract validation, DLQ routing, idempotent inserts."""

from __future__ import annotations

from pathlib import Path

from recon.config import DLQ_TOPIC, SOURCE_TOPICS
from recon.fx import read_fx_csv
from recon.loader.consume import run
from recon.loader.kafka_io import KafkaDLQ, make_consumer
from recon.loader.schema import ensure_schema
from recon.loader.sink import BatchStats, Sink, load_fx_rates
from recon.warehouse import WarehouseClient


def run_loader(wh: WarehouseClient, *, bootstrap: str, group_id: str, topic_prefix: str = "",
               fx_rates_path: Path | None = None, stop_at_eof: bool = True, log=print) -> BatchStats:
    ensure_schema(wh)
    if fx_rates_path is not None and fx_rates_path.exists():
        load_fx_rates(wh, read_fx_csv(fx_rates_path))
    consumer = make_consumer(bootstrap, group_id)
    consumer.subscribe([f"{topic_prefix}{t}" for t in SOURCE_TOPICS])
    try:
        sink = Sink(wh, KafkaDLQ(bootstrap, f"{topic_prefix}{DLQ_TOPIC}"), topic_prefix)
        return run(consumer, sink, stop_at_eof=stop_at_eof, log=log)
    finally:
        consumer.close()
