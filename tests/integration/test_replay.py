"""End to end against the real broker from docker compose (make up).

Uses its own uniquely prefixed topics, so it never touches the dev topics or their data.
"""

from __future__ import annotations

import uuid

import pytest
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

from recon.config import DLQ_TOPIC, SOURCE_TOPICS, load_settings
from recon.fx import write_fx_csv
from recon.generator.publish import publish
from recon.loader import run_loader
from recon.loader.schema import SOURCE_TABLES
from recon.warehouse import DuckDBClient

pytestmark = pytest.mark.integration
BOOTSTRAP = load_settings().kafka_bootstrap


def quiet(_: str) -> None:
    pass


@pytest.fixture(scope="module")
def prefix():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    try:
        admin.list_topics(timeout=10)
    except Exception as err:  # noqa: BLE001
        pytest.fail(f"Kafka is not reachable at {BOOTSTRAP}; run `make up` first ({err})")
    prefix = f"it-{uuid.uuid4().hex[:8]}."
    names = [f"{prefix}{t}" for t in SOURCE_TOPICS]
    topics = [NewTopic(n, num_partitions=3, replication_factor=1, config={"retention.ms": "-1"}) for n in names]
    topics.append(NewTopic(f"{prefix}{DLQ_TOPIC}", num_partitions=1, replication_factor=1))
    for future in admin.create_topics(topics).values():
        future.result(30)
    yield prefix
    for future in admin.delete_topics([t.topic for t in topics]).values():
        future.result(30)


def counts(wh):
    out = {t: wh.execute(f"SELECT count(*) FROM raw.{t}")[0][0] for t in SOURCE_TABLES}
    out["dlq_log"] = wh.execute("SELECT count(*) FROM raw.dlq_log")[0][0]
    out["fx_rates"] = wh.execute("SELECT count(*) FROM ref.fx_rates")[0][0]
    return out


def count_dlq_topic(topic: str) -> int:
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"dlq-count-{uuid.uuid4().hex}",
                         "auto.offset.reset": "earliest", "enable.partition.eof": True})
    consumer.subscribe([topic])
    n = 0
    try:
        while True:
            msg = consumer.poll(10)
            assert msg is not None, "timed out reading the DLQ topic"
            if msg.error() and msg.error().code() == KafkaError._PARTITION_EOF:
                return n
            n += 1
    finally:
        consumer.close()


def test_load_then_replay_inserts_zero_duplicates(scenario, prefix, tmp_path):
    fx_path = tmp_path / "fx_rates.csv"
    write_fx_csv(fx_path, scenario.fx_rates)
    assert publish(scenario, BOOTSTRAP, prefix) == len(scenario.messages)

    wh = DuckDBClient(tmp_path / "warehouse.duckdb")
    try:
        kwargs = dict(bootstrap=BOOTSTRAP, topic_prefix=prefix, fx_rates_path=fx_path, log=quiet)
        expected = scenario.expected_counts() | {"dlq_log": len(scenario.poison),
                                                 "fx_rates": len(scenario.fx_rates)}

        first = run_loader(wh, group_id=f"{prefix}loader", **kwargs)
        assert first.received == len(scenario.messages)
        assert first.rejected == first.dead_lettered == len(scenario.poison)
        assert counts(wh) == expected

        # Same group again: offsets were committed, so there is nothing left to read.
        again = run_loader(wh, group_id=f"{prefix}loader", **kwargs)
        assert again.received == 0

        # Replay: fresh group from the earliest offset. Everything is re-read, nothing is re-inserted.
        replay = run_loader(wh, group_id=f"{prefix}replay", **kwargs)
        assert replay.received == len(scenario.messages)
        assert replay.rows_inserted == 0
        assert replay.dead_lettered == 0
        assert counts(wh) == expected
    finally:
        wh.close()

    # The DLQ topic holds each poison message exactly once, even after the replay.
    assert count_dlq_topic(f"{prefix}{DLQ_TOPIC}") == len(scenario.poison)
