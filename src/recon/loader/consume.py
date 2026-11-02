"""The consume loop: poll a batch, hand it to the sink, and only then commit offsets."""

from __future__ import annotations

import time
from collections.abc import Callable

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from recon.loader.sink import BatchStats, RawMessage, Sink


def run(consumer: Consumer, sink: Sink, *, batch_size: int = 500, poll_timeout: float = 1.0,
        stop_at_eof: bool = True, assignment_timeout: float = 30.0,
        log: Callable[[str], None] = print) -> BatchStats:
    """Consume until every assigned partition is at its end (or forever if stop_at_eof is False)."""
    total = BatchStats()
    at_eof: set[tuple[str, int]] = set()
    started = time.monotonic()
    while True:
        batch: list[RawMessage] = []
        next_offsets: dict[tuple[str, int], int] = {}
        for msg in consumer.consume(num_messages=batch_size, timeout=poll_timeout):
            err = msg.error()
            if err is not None:
                if err.code() == KafkaError._PARTITION_EOF:
                    at_eof.add((msg.topic(), msg.partition()))
                    continue
                raise KafkaException(err)
            at_eof.discard((msg.topic(), msg.partition()))
            batch.append(RawMessage(msg.topic(), msg.partition(), msg.offset(), msg.key(), msg.value()))
            next_offsets[(msg.topic(), msg.partition())] = msg.offset() + 1

        if batch:
            stats = sink.process(batch)  # raises on failure, so nothing below runs and offsets stay put
            consumer.commit(offsets=[TopicPartition(t, p, o) for (t, p), o in next_offsets.items()],
                            asynchronous=False)
            total.add(stats)
            log(f"batch: received={stats.received} inserted={stats.rows_inserted} "
                f"rejected={stats.rejected} dead_lettered={stats.dead_lettered}")

        assignment = {(tp.topic, tp.partition) for tp in consumer.assignment()}
        if stop_at_eof and assignment and assignment <= at_eof:
            return total
        if not assignment and time.monotonic() - started > assignment_timeout:
            raise TimeoutError("consumer received no partition assignment; is Kafka up and are the topics created?")
