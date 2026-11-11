"""Generator configuration from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping


def _get(env: Mapping[str, str], key: str, default: str) -> str:
    value = env.get(key)
    return default if value is None or not value.strip() else value.strip()


@dataclass(frozen=True)
class GeneratorConfig:
    bootstrap_servers: str = "localhost:29092"
    topic: str = "ad.events"
    events_per_second: float = 2000.0
    workers: int = 2
    num_users: int = 1_000_000
    win_rate: float = 0.35
    ctr: float = 0.015
    cvr: float = 0.06
    duplicate_ratio: float = 0.01
    invalid_event_ratio: float = 0.005
    late_event_ratio: float = 0.02
    max_late_seconds: int = 10
    run_duration_seconds: int = 0  # 0 = forever
    stats_interval_seconds: int = 10
    log_level: str = "INFO"

    def __post_init__(self) -> None:
        if self.events_per_second <= 0:
            raise ValueError("EVENTS_PER_SECOND must be > 0")
        if self.workers < 1:
            raise ValueError("GENERATOR_WORKERS must be >= 1")
        if self.num_users < 1:
            raise ValueError("NUM_USERS must be >= 1")
        for name in ("win_rate", "ctr", "cvr", "duplicate_ratio", "invalid_event_ratio", "late_event_ratio"):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be between 0 and 1, got {value}")
        if self.max_late_seconds < 1:
            raise ValueError("MAX_LATE_SECONDS must be >= 1")

    @property
    def events_per_second_per_worker(self) -> float:
        return self.events_per_second / self.workers

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "GeneratorConfig":
        env = os.environ if env is None else env
        d = cls()
        return cls(
            bootstrap_servers=_get(env, "KAFKA_BOOTSTRAP_SERVERS", d.bootstrap_servers),
            topic=_get(env, "KAFKA_TOPIC", d.topic),
            events_per_second=float(_get(env, "EVENTS_PER_SECOND", str(d.events_per_second))),
            workers=int(_get(env, "GENERATOR_WORKERS", str(d.workers))),
            num_users=int(_get(env, "NUM_USERS", str(d.num_users))),
            win_rate=float(_get(env, "WIN_RATE", str(d.win_rate))),
            ctr=float(_get(env, "CTR", str(d.ctr))),
            cvr=float(_get(env, "CVR", str(d.cvr))),
            duplicate_ratio=float(_get(env, "DUPLICATE_RATIO", str(d.duplicate_ratio))),
            invalid_event_ratio=float(_get(env, "INVALID_EVENT_RATIO", str(d.invalid_event_ratio))),
            late_event_ratio=float(_get(env, "LATE_EVENT_RATIO", str(d.late_event_ratio))),
            max_late_seconds=int(_get(env, "MAX_LATE_SECONDS", str(d.max_late_seconds))),
            run_duration_seconds=int(_get(env, "RUN_DURATION_SECONDS", str(d.run_duration_seconds))),
            stats_interval_seconds=int(_get(env, "STATS_INTERVAL_SECONDS", str(d.stats_interval_seconds))),
            log_level=_get(env, "LOG_LEVEL", d.log_level).upper(),
        )
