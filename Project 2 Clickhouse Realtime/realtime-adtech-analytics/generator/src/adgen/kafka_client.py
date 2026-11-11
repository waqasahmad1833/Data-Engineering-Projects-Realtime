"""Throughput-tuned Kafka producer wrapper."""

from __future__ import annotations

import logging

from confluent_kafka import KafkaError, Message, Producer

logger = logging.getLogger(__name__)


class EventPublisher:
    def __init__(self, bootstrap_servers: str, topic: str, client_id: str) -> None:
        self._topic = topic
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": client_id,
                "acks": "all",
                "enable.idempotence": True,
                # Batch aggressively: fewer, larger, compressed requests
                "linger.ms": 50,
                "batch.size": 1_000_000,
                "compression.type": "zstd",
                "queue.buffering.max.messages": 500_000,
                "queue.buffering.max.kbytes": 512 * 1024,
            }
        )
        self.delivered = 0
        self.failed = 0

    def _on_delivery(self, err: KafkaError | None, _msg: Message) -> None:
        if err is None:
            self.delivered += 1
        else:
            self.failed += 1
            if self.failed % 1000 == 1:
                logger.warning("delivery failed: %s", err)

    def send(self, key: bytes, value: bytes) -> None:
        while True:
            try:
                self._producer.produce(self._topic, key=key, value=value, on_delivery=self._on_delivery)
                break
            except BufferError:
                self._producer.poll(0.1)
        self._producer.poll(0)

    def flush(self, timeout: float = 30.0) -> int:
        return self._producer.flush(timeout)
