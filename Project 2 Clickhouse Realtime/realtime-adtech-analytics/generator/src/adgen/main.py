"""Entry point: fan out N worker processes (Python's GIL limits one process to ~20-40k events/s)."""

from __future__ import annotations

import logging
import multiprocessing as mp
import signal
import threading

from .config import GeneratorConfig
from .worker import run_worker

logger = logging.getLogger("adgen")


def _configure_logging(level: str) -> None:
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(processName)s %(name)s - %(message)s")


def _worker_entry(worker_id: int, config: GeneratorConfig) -> None:
    _configure_logging(config.log_level)
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    run_worker(worker_id, config, stop)


def main() -> None:
    config = GeneratorConfig.from_env()
    _configure_logging(config.log_level)
    logger.info(
        "Starting %d worker(s), target %.0f events/s total -> %s/%s",
        config.workers, config.events_per_second, config.bootstrap_servers, config.topic,
    )
    processes = [
        mp.Process(target=_worker_entry, args=(i, config), name=f"adgen-worker-{i}", daemon=False)
        for i in range(config.workers)
    ]
    for p in processes:
        p.start()

    def _shutdown(signum, _frame) -> None:
        logger.info("signal %s received, stopping workers", signum)
        for p in processes:
            if p.is_alive():
                p.terminate()  # delivers SIGTERM -> graceful flush in the worker

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)
    for p in processes:
        p.join()


if __name__ == "__main__":
    main()
