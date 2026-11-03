"""Kafka wiring for the loader: the consumer config and the DLQ producer."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager

from confluent_kafka import Consumer, KafkaError, Message, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from recon.config import DLQ_TOPIC, SOURCE_TOPICS
from recon.contracts import DLQRecord


def make_consumer(bootstrap: str, group_id: str) -> Consumer:
    return Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "enable.auto.commit": False,  # offsets are committed by hand, after the load succeeds
        "auto.offset.reset": "earliest",
        "enable.partition.eof": True,
        "isolation.level": "read_committed",
    })


class KafkaDLQ:
    def __init__(self, bootstrap: str, topic: str):
        self.topic = topic
        self._producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True, "acks": "all"})
        self._errors: list[KafkaError] = []

    def publish(self, record: DLQRecord) -> None:
        key = f"{record.source_topic}:{record.source_partition}:{record.source_offset}"
        self._producer.produce(
            self.topic,
            key=key.encode(),
            value=record.model_dump_json().encode(),
            headers=[("error_type", record.error_type.encode()), ("source_topic", record.source_topic.encode())],
            on_delivery=self._on_delivery,
        )

    def flush(self) -> None:
        remaining = self._producer.flush(30)
        if remaining or self._errors:
            errors, self._errors = self._errors, []
            raise RuntimeError(f"DLQ publish failed: {remaining} undelivered, errors={errors[:3]}")

    def _on_delivery(self, err: KafkaError | None, _msg: Message) -> None:
        if err is not None:
            self._errors.append(err)


@contextmanager
def scratch_topics(bootstrap: str, label: str) -> Iterator[str]:
    """Freshly created, uniquely prefixed source and DLQ topics, deleted afterwards. Yields the prefix."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    admin.list_topics(timeout=10)  # fails fast if the broker is not up
    prefix = f"{label}-{uuid.uuid4().hex[:8]}."
    topics = [NewTopic(f"{prefix}{t}", num_partitions=3, replication_factor=1, config={"retention.ms": "-1"})
              for t in SOURCE_TOPICS]
    topics.append(NewTopic(f"{prefix}{DLQ_TOPIC}", num_partitions=1, replication_factor=1))
    for future in admin.create_topics(topics).values():
        future.result(30)
    try:
        yield prefix
    finally:
        for future in admin.delete_topics([t.topic for t in topics]).values():
            future.result(30)
