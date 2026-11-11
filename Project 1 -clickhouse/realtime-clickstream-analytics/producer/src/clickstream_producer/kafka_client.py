"""Thin wrapper around confluent-kafka's Producer with delivery accounting."""

from __future__ import annotations

import logging

from confluent_kafka import KafkaError, Message, Producer

logger = logging.getLogger(__name__)


class EventPublisher:
    def __init__(self, bootstrap_servers: str, topic: str, client_id: str = "clickstream-producer") -> None:
        self._topic = topic
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": client_id,
                "acks": "all",
                "enable.idempotence": True,
                "linger.ms": 20,
                "compression.type": "lz4",
                "retries": 10,
                "retry.backoff.ms": 200,
            }
        )
        self.delivered = 0
        self.failed = 0

    def _on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            self.failed += 1
            logger.warning("Delivery failed for key=%s: %s", msg.key(), err)
        else:
            self.delivered += 1

    def publish(self, key: str, value: bytes) -> None:
        while True:
            try:
                self._producer.produce(
                    self._topic, key=key.encode("utf-8"), value=value, on_delivery=self._on_delivery
                )
                break
            except BufferError:
                # Local queue is full: serve delivery callbacks and retry.
                self._producer.poll(0.5)
        self._producer.poll(0)

    def flush(self, timeout: float = 30.0) -> int:
        """Wait for outstanding messages. Returns the number still undelivered."""
        return self._producer.flush(timeout)
