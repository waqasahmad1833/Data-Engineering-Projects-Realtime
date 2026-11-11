"""Entry point: generate events at a fixed rate and publish them to Kafka."""

from __future__ import annotations

import logging
import signal
import threading
import time
from collections import Counter

from .config import ProducerConfig
from .generator import ClickstreamGenerator
from .kafka_client import EventPublisher

logger = logging.getLogger("clickstream_producer")


def run(config: ProducerConfig) -> None:
    generator = ClickstreamGenerator(
        num_users=config.num_users,
        invalid_event_ratio=config.invalid_event_ratio,
        late_event_ratio=config.late_event_ratio,
        max_late_seconds=config.max_late_seconds,
    )
    publisher = EventPublisher(config.bootstrap_servers, config.topic)

    stop = threading.Event()

    def _request_stop(signum, _frame) -> None:
        logger.info("Received signal %s, shutting down", signum)
        stop.set()

    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)

    interval = 1.0 / config.events_per_second
    started = time.monotonic()
    next_send = started
    last_report = started
    stats: Counter[str] = Counter()

    logger.info(
        "Producing ~%.1f events/s to %s on %s (invalid=%.0f%%, late=%.0f%%)",
        config.events_per_second, config.topic, config.bootstrap_servers,
        config.invalid_event_ratio * 100, config.late_event_ratio * 100,
    )

    while not stop.is_set():
        now = time.monotonic()
        if config.run_duration_seconds and now - started >= config.run_duration_seconds:
            logger.info("Configured run duration reached")
            break

        record = generator.next_record()
        publisher.publish(record.key, record.value)
        stats["valid" if record.is_valid else "invalid"] += 1
        stats[record.kind] += 1

        next_send += interval
        sleep_for = next_send - time.monotonic()
        if sleep_for > 0:
            stop.wait(sleep_for)
        elif sleep_for < -1.0:
            next_send = time.monotonic()  # fell far behind; don't burst to catch up

        if now - last_report >= config.stats_interval_seconds:
            elapsed = now - started
            total = stats["valid"] + stats["invalid"]
            logger.info(
                "sent=%d (%.1f/s) valid=%d invalid=%d delivered=%d failed=%d active_sessions=%d",
                total, total / elapsed if elapsed else 0.0, stats["valid"], stats["invalid"],
                publisher.delivered, publisher.failed, generator.active_sessions,
            )
            last_report = now

    remaining = publisher.flush()
    logger.info(
        "Stopped. delivered=%d failed=%d undelivered=%d breakdown=%s",
        publisher.delivered, publisher.failed, remaining,
        {k: v for k, v in stats.items() if k not in ("valid", "invalid")},
    )


def main() -> None:
    config = ProducerConfig.from_env()
    logging.basicConfig(
        level=config.log_level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    run(config)


if __name__ == "__main__":
    main()
