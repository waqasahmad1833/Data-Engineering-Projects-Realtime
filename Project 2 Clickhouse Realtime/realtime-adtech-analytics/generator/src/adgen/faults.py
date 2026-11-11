"""Fault injection: duplicates (at-least-once exchanges), late events, invalid payloads."""

from __future__ import annotations

import json
import random
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import Iterable, Optional

from .models import AdEvent

INVALID_KINDS: tuple[str, ...] = (
    "malformed_json",
    "missing_event_id",
    "unknown_event_type",
    "negative_price",
    "impression_without_impression_id",
)


@dataclass(frozen=True)
class Record:
    """A Kafka record ready to send."""

    key: bytes
    value: bytes
    kind: str  # event type, "duplicate", or an invalid kind


class FaultInjector:
    def __init__(
        self,
        duplicate_ratio: float = 0.0,
        invalid_ratio: float = 0.0,
        late_ratio: float = 0.0,
        max_late_seconds: int = 60,
        seed: Optional[int] = None,
    ) -> None:
        self._rng = random.Random(seed)
        self._duplicate_ratio = duplicate_ratio
        self._invalid_ratio = invalid_ratio
        self._late_ratio = late_ratio
        self._max_late_seconds = max_late_seconds

    def apply(self, events: Iterable[AdEvent]) -> list[Record]:
        records: list[Record] = []
        for event in events:
            if self._late_ratio and self._rng.random() < self._late_ratio:
                event = replace(
                    event, event_time=event.event_time - timedelta(seconds=self._rng.uniform(1, self._max_late_seconds))
                )
            key = str(event.context.user_id).encode()
            if self._invalid_ratio and self._rng.random() < self._invalid_ratio:
                records.append(self._corrupt(event, key))
                continue
            value = event.to_json().encode()
            records.append(Record(key, value, event.event_type.value))
            if self._duplicate_ratio and self._rng.random() < self._duplicate_ratio:
                records.append(Record(key, value, "duplicate"))
        return records

    def _corrupt(self, event: AdEvent, key: bytes) -> Record:
        kind = self._rng.choice(INVALID_KINDS)
        if kind == "malformed_json":
            return Record(key, event.to_json()[: self._rng.randint(5, 60)].encode(), kind)
        payload = event.to_dict()
        if kind == "missing_event_id":
            payload.pop("event_id")
        elif kind == "unknown_event_type":
            payload["event_type"] = "teleport"
        elif kind == "negative_price":
            payload["event_type"] = "bid"
            payload["bid_price_micros"] = -100
        elif kind == "impression_without_impression_id":
            payload["event_type"] = "impression"
            payload.pop("impression_id", None)
            payload["win_price_micros"] = 1000
        return Record(key, json.dumps(payload, separators=(",", ":")).encode(), kind)
