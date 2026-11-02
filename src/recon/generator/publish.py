"""Publishes a scenario's messages to Kafka, in order."""

from __future__ import annotations

from confluent_kafka import KafkaError, Message, Producer

from recon.generator.scenario import Scenario


def publish(scenario: Scenario, bootstrap: str, topic_prefix: str = "") -> int:
    producer = Producer({
        "bootstrap.servers": bootstrap,
        "enable.idempotence": True,  # no broker-side duplicates from producer retries
        "acks": "all",
        "linger.ms": 5,
    })
    errors: list[KafkaError] = []

    def on_delivery(err: KafkaError | None, _msg: Message) -> None:
        if err is not None:
            errors.append(err)

    for m in scenario.messages:
        producer.produce(f"{topic_prefix}{m.topic}", key=m.key, value=m.value, on_delivery=on_delivery)
        producer.poll(0)
    remaining = producer.flush(30)
    if remaining or errors:
        raise RuntimeError(f"publish failed: {remaining} undelivered, errors={errors[:3]}")
    return len(scenario.messages)
