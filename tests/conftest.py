from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterator

import pytest

from recon.contracts import DLQRecord
from recon.generator import build_scenario
from recon.generator.scenario import Scenario
from recon.loader.schema import ensure_schema
from recon.loader.sink import RawMessage, Sink
from recon.warehouse import DuckDBClient


class FakeDLQ:
    def __init__(self, events: list[str] | None = None):
        self.sent: list[DLQRecord] = []
        self.events = events if events is not None else []

    def publish(self, record: DLQRecord) -> None:
        self.sent.append(record)

    def flush(self) -> None:
        self.events.append("dlq.flush")


def as_raw_messages(scenario: Scenario, partitions: int = 3) -> list[RawMessage]:
    """Mimic Kafka: messages spread over partitions by key, offsets increasing per partition."""
    next_offset: dict[tuple[str, int], int] = defaultdict(int)
    raws = []
    for m in scenario.messages:
        partition = sum(m.key or b"") % partitions
        offset = next_offset[(m.topic, partition)]
        next_offset[(m.topic, partition)] += 1
        raws.append(RawMessage(m.topic, partition, offset, m.key, m.value))
    return raws


def load_all(sink: Sink, raws: list[RawMessage], batch_size: int = 500):
    return [sink.process(raws[i:i + batch_size]) for i in range(0, len(raws), batch_size)]


@pytest.fixture(scope="session")
def scenario() -> Scenario:
    return build_scenario()


@pytest.fixture
def warehouse() -> Iterator[DuckDBClient]:
    wh = DuckDBClient()
    ensure_schema(wh)
    yield wh
    wh.close()
