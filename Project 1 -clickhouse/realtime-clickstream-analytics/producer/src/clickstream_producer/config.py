"""Runtime configuration, loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping


def _get(env: Mapping[str, str], key: str, default: str) -> str:
    value = env.get(key)
    return default if value is None or value.strip() == "" else value.strip()


@dataclass(frozen=True)
class ProducerConfig:
    bootstrap_servers: str = "localhost:29092"
    topic: str = "clickstream.events"
    events_per_second: float = 50.0
    num_users: int = 2000
    invalid_event_ratio: float = 0.02
    late_event_ratio: float = 0.05
    max_late_seconds: int = 120
    run_duration_seconds: int = 0  # 0 = run forever
    stats_interval_seconds: int = 10
    log_level: str = "INFO"

    def __post_init__(self) -> None:
        if self.events_per_second <= 0:
            raise ValueError("EVENTS_PER_SECOND must be > 0")
        if self.num_users <= 0:
            raise ValueError("NUM_USERS must be > 0")
        for name, ratio in (
            ("INVALID_EVENT_RATIO", self.invalid_event_ratio),
            ("LATE_EVENT_RATIO", self.late_event_ratio),
        ):
            if not 0.0 <= ratio <= 1.0:
                raise ValueError(f"{name} must be between 0 and 1, got {ratio}")
        if self.max_late_seconds < 5:
            raise ValueError("MAX_LATE_SECONDS must be >= 5")
        if self.run_duration_seconds < 0:
            raise ValueError("RUN_DURATION_SECONDS must be >= 0")

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "ProducerConfig":
        env = os.environ if env is None else env
        defaults = cls()
        return cls(
            bootstrap_servers=_get(env, "KAFKA_BOOTSTRAP_SERVERS", defaults.bootstrap_servers),
            topic=_get(env, "KAFKA_TOPIC", defaults.topic),
            events_per_second=float(_get(env, "EVENTS_PER_SECOND", str(defaults.events_per_second))),
            num_users=int(_get(env, "NUM_USERS", str(defaults.num_users))),
            invalid_event_ratio=float(_get(env, "INVALID_EVENT_RATIO", str(defaults.invalid_event_ratio))),
            late_event_ratio=float(_get(env, "LATE_EVENT_RATIO", str(defaults.late_event_ratio))),
            max_late_seconds=int(_get(env, "MAX_LATE_SECONDS", str(defaults.max_late_seconds))),
            run_duration_seconds=int(_get(env, "RUN_DURATION_SECONDS", str(defaults.run_duration_seconds))),
            stats_interval_seconds=int(_get(env, "STATS_INTERVAL_SECONDS", str(defaults.stats_interval_seconds))),
            log_level=_get(env, "LOG_LEVEL", defaults.log_level).upper(),
        )
