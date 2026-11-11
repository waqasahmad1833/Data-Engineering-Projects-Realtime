"""Simulates user sessions moving through an e-commerce funnel.

Each active session is a small state machine (page_view -> product_view ->
add_to_cart -> checkout_start -> purchase, with drop-offs). The generator can
also inject late events (event_time in the past) and invalid payloads so the
downstream Flink job's watermarking and dead-letter handling get exercised.
"""

from __future__ import annotations

import json
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional, Sequence, TypeVar

from . import catalog
from .models import ClickEvent, EventType

T = TypeVar("T")

# Next-step probabilities. ``None`` means the session ends.
TRANSITIONS: dict[EventType, tuple[tuple[Optional[EventType], float], ...]] = {
    EventType.PAGE_VIEW: (
        (EventType.PAGE_VIEW, 0.35), (EventType.PRODUCT_VIEW, 0.50), (None, 0.15)),
    EventType.PRODUCT_VIEW: (
        (EventType.PRODUCT_VIEW, 0.30), (EventType.ADD_TO_CART, 0.30),
        (EventType.PAGE_VIEW, 0.25), (None, 0.15)),
    EventType.ADD_TO_CART: (
        (EventType.CHECKOUT_START, 0.45), (EventType.REMOVE_FROM_CART, 0.10),
        (EventType.PRODUCT_VIEW, 0.30), (None, 0.15)),
    EventType.REMOVE_FROM_CART: (
        (EventType.PRODUCT_VIEW, 0.50), (None, 0.50)),
    EventType.CHECKOUT_START: (
        (EventType.PURCHASE, 0.60), (None, 0.40)),
    EventType.PURCHASE: (
        (EventType.PAGE_VIEW, 0.30), (None, 0.70)),
}

INVALID_KINDS: tuple[str, ...] = (
    "malformed_json",
    "missing_event_id",
    "unknown_event_type",
    "bad_timestamp",
    "purchase_without_price",
)


@dataclass(frozen=True)
class GeneratedRecord:
    """A Kafka record ready to publish."""

    key: str
    value: bytes
    is_valid: bool
    kind: str  # event type for valid records, corruption kind for invalid ones


@dataclass
class Session:
    session_id: str
    user_id: str
    country: str
    user_agent: str
    referrer: str
    last_event: Optional[EventType] = None
    current_product: Optional[catalog.Product] = None
    cart: list[catalog.Product] = field(default_factory=list)


class ClickstreamGenerator:
    def __init__(
        self,
        num_users: int,
        invalid_event_ratio: float = 0.0,
        late_event_ratio: float = 0.0,
        max_late_seconds: int = 120,
        max_active_sessions: Optional[int] = None,
        seed: Optional[int] = None,
        clock: Optional[Callable[[], datetime]] = None,
    ) -> None:
        if num_users <= 0:
            raise ValueError("num_users must be > 0")
        for name, ratio in (("invalid_event_ratio", invalid_event_ratio),
                            ("late_event_ratio", late_event_ratio)):
            if not 0.0 <= ratio <= 1.0:
                raise ValueError(f"{name} must be between 0 and 1")
        if max_late_seconds < 5:
            raise ValueError("max_late_seconds must be >= 5")

        self._rng = random.Random(seed)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._num_users = num_users
        self._invalid_ratio = invalid_event_ratio
        self._late_ratio = late_event_ratio
        self._max_late_seconds = max_late_seconds
        self._max_active_sessions = max_active_sessions or max(10, num_users // 20)
        self._sessions: list[Session] = []
        self._profiles: dict[str, tuple[str, str]] = {}

    @property
    def active_sessions(self) -> int:
        return len(self._sessions)

    # ------------------------------------------------------------------ API --
    def next_record(self) -> GeneratedRecord:
        if self._invalid_ratio and self._rng.random() < self._invalid_ratio:
            return self._invalid_record()
        event = self._next_valid_event()
        return GeneratedRecord(
            key=event.session_id,
            value=event.to_json().encode("utf-8"),
            is_valid=True,
            kind=event.event_type.value,
        )

    # -------------------------------------------------------------- helpers --
    def _weighted(self, options: Sequence[tuple[T, float]]) -> T:
        values = [value for value, _ in options]
        weights = [weight for _, weight in options]
        return self._rng.choices(values, weights=weights, k=1)[0]

    def _uuid(self) -> str:
        return str(uuid.UUID(int=self._rng.getrandbits(128), version=4))

    def _profile(self, user_id: str) -> tuple[str, str]:
        """A user keeps the same country and device across sessions."""
        if user_id not in self._profiles:
            self._profiles[user_id] = (
                self._weighted(catalog.COUNTRIES),
                self._weighted(catalog.USER_AGENTS),
            )
        return self._profiles[user_id]

    def _new_session(self) -> Session:
        user_id = f"u_{self._rng.randrange(self._num_users):06d}"
        country, user_agent = self._profile(user_id)
        session = Session(
            session_id=self._uuid(),
            user_id=user_id,
            country=country,
            user_agent=user_agent,
            referrer=self._weighted(catalog.REFERRERS),
        )
        self._sessions.append(session)
        return session

    def _pick_session(self) -> Session:
        room_for_more = len(self._sessions) < self._max_active_sessions
        if not self._sessions or (room_for_more and self._rng.random() < 0.3):
            return self._new_session()
        return self._rng.choice(self._sessions)

    def _next_event_type(self, last: Optional[EventType]) -> Optional[EventType]:
        if last is None:
            return EventType.PAGE_VIEW
        return self._weighted(TRANSITIONS[last])

    def _event_time(self) -> datetime:
        now = self._clock()
        if self._late_ratio and self._rng.random() < self._late_ratio:
            return now - timedelta(seconds=self._rng.uniform(5, self._max_late_seconds))
        return now

    def _next_valid_event(self) -> ClickEvent:
        while True:
            session = self._pick_session()
            event_type = self._next_event_type(session.last_event)
            if event_type is None:
                self._sessions.remove(session)
                continue
            return self._build_event(session, event_type)

    def _build_event(self, session: Session, event_type: EventType) -> ClickEvent:
        rng = self._rng
        page_url = rng.choice(catalog.CONTENT_PAGES)
        product: Optional[catalog.Product] = None
        quantity: Optional[int] = None

        if event_type is EventType.PRODUCT_VIEW:
            product = rng.choice(catalog.PRODUCTS)
            session.current_product = product
            page_url = product.url
        elif event_type is EventType.ADD_TO_CART:
            product = session.current_product or rng.choice(catalog.PRODUCTS)
            session.cart.append(product)
            page_url = product.url
            quantity = 1
        elif event_type is EventType.REMOVE_FROM_CART:
            product = session.cart.pop() if session.cart else (
                session.current_product or rng.choice(catalog.PRODUCTS))
            page_url = "/cart"
            quantity = 1
        elif event_type is EventType.CHECKOUT_START:
            page_url = "/checkout"
        elif event_type is EventType.PURCHASE:
            product = session.cart[-1] if session.cart else (
                session.current_product or rng.choice(catalog.PRODUCTS))
            page_url = "/checkout/confirmation"
            quantity = rng.choices((1, 2, 3), weights=(0.75, 0.2, 0.05), k=1)[0]
            session.cart.clear()

        session.last_event = event_type
        return ClickEvent(
            event_id=self._uuid(),
            event_type=event_type,
            user_id=session.user_id,
            session_id=session.session_id,
            page_url=page_url,
            referrer=session.referrer,
            user_agent=session.user_agent,
            country=session.country,
            event_time=self._event_time(),
            product_id=product.product_id if product else None,
            price=product.price if product else None,
            quantity=quantity,
        )

    def _invalid_record(self) -> GeneratedRecord:
        kind = self._rng.choice(INVALID_KINDS)
        event = self._next_valid_event()

        if kind == "malformed_json":
            value = event.to_json()[: self._rng.randint(5, 40)].encode("utf-8")
        else:
            payload = event.to_dict()
            if kind == "missing_event_id":
                payload.pop("event_id")
            elif kind == "unknown_event_type":
                payload["event_type"] = "teleport"
            elif kind == "bad_timestamp":
                payload["event_time"] = "yesterday at noon"
            elif kind == "purchase_without_price":
                payload.update(
                    event_type=EventType.PURCHASE.value,
                    product_id=payload.get("product_id") or catalog.PRODUCTS[0].product_id,
                    price=None,
                    quantity=1,
                )
            value = json.dumps(payload, separators=(",", ":")).encode("utf-8")

        return GeneratedRecord(key=event.session_id, value=value, is_valid=False, kind=kind)
