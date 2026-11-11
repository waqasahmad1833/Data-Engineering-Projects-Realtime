"""Ad event model and JSON wire format (consumed by the Flink job)."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional


class EventType(str, Enum):
    BID = "bid"
    IMPRESSION = "impression"
    CLICK = "click"
    CONVERSION = "conversion"


def format_timestamp(ts: datetime) -> str:
    if ts.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return ts.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass(frozen=True)
class AdContext:
    """Everything that is fixed for one auction and inherited by its follow-up events."""

    request_id: str
    user_id: int
    advertiser_id: int
    campaign_id: int
    creative_id: int
    publisher_id: int
    site_domain: str
    ad_format: str
    device_type: str
    os: str
    country: str


@dataclass(frozen=True)
class AdEvent:
    event_id: str
    event_type: EventType
    event_time: datetime
    context: AdContext
    impression_id: Optional[str] = None
    click_id: Optional[str] = None
    bid_price_micros: int = 0
    win_price_micros: int = 0
    conversion_value_micros: int = 0
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        c = self.context
        payload: dict[str, Any] = {
            "event_id": self.event_id,
            "event_type": self.event_type.value,
            "event_time": format_timestamp(self.event_time),
            "request_id": c.request_id,
            "user_id": c.user_id,
            "advertiser_id": c.advertiser_id,
            "campaign_id": c.campaign_id,
            "creative_id": c.creative_id,
            "publisher_id": c.publisher_id,
            "site_domain": c.site_domain,
            "ad_format": c.ad_format,
            "device_type": c.device_type,
            "os": c.os,
            "country": c.country,
        }
        if self.impression_id is not None:
            payload["impression_id"] = self.impression_id
        if self.click_id is not None:
            payload["click_id"] = self.click_id
        if self.bid_price_micros:
            payload["bid_price_micros"] = self.bid_price_micros
        if self.win_price_micros:
            payload["win_price_micros"] = self.win_price_micros
        if self.conversion_value_micros:
            payload["conversion_value_micros"] = self.conversion_value_micros
        payload.update(self.extra)
        return payload

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"))
