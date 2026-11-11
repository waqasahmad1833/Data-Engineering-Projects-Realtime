"""Event model and wire format shared with the Flink job."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional


class EventType(str, Enum):
    PAGE_VIEW = "page_view"
    PRODUCT_VIEW = "product_view"
    ADD_TO_CART = "add_to_cart"
    REMOVE_FROM_CART = "remove_from_cart"
    CHECKOUT_START = "checkout_start"
    PURCHASE = "purchase"

    @property
    def requires_product(self) -> bool:
        return self in PRODUCT_EVENTS


PRODUCT_EVENTS = frozenset(
    {EventType.PRODUCT_VIEW, EventType.ADD_TO_CART, EventType.REMOVE_FROM_CART, EventType.PURCHASE}
)


def format_timestamp(ts: datetime) -> str:
    """ISO-8601 UTC with millisecond precision, e.g. 2026-11-10T12:00:00.123Z."""
    if ts.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return ts.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass(frozen=True)
class ClickEvent:
    event_id: str
    event_type: EventType
    user_id: str
    session_id: str
    page_url: str
    referrer: str
    user_agent: str
    country: str
    event_time: datetime
    product_id: Optional[str] = None
    price: Optional[float] = None
    quantity: Optional[int] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type.value,
            "user_id": self.user_id,
            "session_id": self.session_id,
            "page_url": self.page_url,
            "referrer": self.referrer,
            "product_id": self.product_id,
            "price": self.price,
            "quantity": self.quantity,
            "user_agent": self.user_agent,
            "country": self.country,
            "event_time": format_timestamp(self.event_time),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"))
