"""One generator worker process: simulate auctions and publish at a target rate."""

from __future__ import annotations

import logging
import threading
import time
from collections import Counter

from .config import GeneratorConfig
from .faults import FaultInjector
from .kafka_client import EventPublisher
from .simulator import AuctionSimulator

logger = logging.getLogger("adgen.worker")


class RateLimiter:
    """Token-bucket style pacing: call :meth:`acquire` with the number of events just produced."""

    def __init__(self, rate_per_second: float) -> None:
        self._interval = 1.0 / rate_per_second
        self._next = time.monotonic()

    def acquire(self, n: int, stop: threading.Event) -> None:
        self._next += n * self._interval
        delay = self._next - time.monotonic()
        if delay > 0:
            stop.wait(delay)
        elif delay < -1.0:  # fell behind by more than a second: don't burst
            self._next = time.monotonic()


def run_worker(worker_id: int, config: GeneratorConfig, stop: threading.Event) -> Counter[str]:
    simulator = AuctionSimulator(
        num_users=config.num_users, win_rate=config.win_rate, ctr=config.ctr, cvr=config.cvr,
    )
    faults = FaultInjector(
        duplicate_ratio=config.duplicate_ratio,
        invalid_ratio=config.invalid_event_ratio,
        late_ratio=config.late_event_ratio,
        max_late_seconds=config.max_late_seconds,
    )
    publisher = EventPublisher(config.bootstrap_servers, config.topic, client_id=f"adgen-{worker_id}")
    limiter = RateLimiter(config.events_per_second_per_worker)

    stats: Counter[str] = Counter()
    started = last_report = time.monotonic()
    while not stop.is_set():
        if config.run_duration_seconds and time.monotonic() - started >= config.run_duration_seconds:
            break
        records = faults.apply(simulator.tick())
        for record in records:
            publisher.send(record.key, record.value)
            stats[record.kind] += 1
        limiter.acquire(len(records), stop)

        now = time.monotonic()
        if now - last_report >= config.stats_interval_seconds:
            total = sum(stats.values())
            logger.info(
                "worker=%d sent=%d (%.0f/s) delivered=%d failed=%d pending_followups=%d",
                worker_id, total, total / (now - started), publisher.delivered, publisher.failed,
                simulator.pending_events,
            )
            last_report = now

    undelivered = publisher.flush()
    logger.info("worker=%d stopped: %s undelivered=%d", worker_id, dict(stats), undelivered)
    return stats
