"""Real-time bidding simulation.

Each call to :meth:`AuctionSimulator.tick` runs one auction "now":

* a ``bid`` event is always emitted,
* the bid is won with probability ``win_rate`` -> ``impression`` (win price below the bid),
* an impression is clicked with probability ``ctr`` after a realistic delay,
* a click converts with probability ``cvr`` after a longer delay.

Clicks and conversions are *scheduled* in a min-heap and emitted only when
their event time arrives, so the stream has the same temporal structure as
real traffic (which is what the Flink interval join relies on). About 1% of
clicks deliberately arrive after the 30-minute attribution window.
"""

from __future__ import annotations

import heapq
import itertools
import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional, Sequence, TypeVar

from . import catalog, ids
from .models import AdContext, AdEvent, EventType

T = TypeVar("T")

ATTRIBUTION_WINDOW = timedelta(minutes=30)


class AuctionSimulator:
    def __init__(
        self,
        num_users: int = 1_000_000,
        win_rate: float = 0.35,
        ctr: float = 0.015,
        cvr: float = 0.06,
        seed: Optional[int] = None,
        clock: Optional[Callable[[], datetime]] = None,
    ) -> None:
        self._rng = random.Random(seed)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._num_users = num_users
        self._win_rate = win_rate
        self._ctr = ctr
        self._cvr = cvr
        self._pending: list[tuple[datetime, int, AdEvent]] = []
        self._seq = itertools.count()

    @property
    def pending_events(self) -> int:
        return len(self._pending)

    # ------------------------------------------------------------- public --
    def tick(self) -> list[AdEvent]:
        """Run one auction and return it together with any scheduled events now due."""
        now = self._clock()
        events = self._auction(now)
        events.extend(self._due(now))
        return events

    def drain(self) -> list[AdEvent]:
        """Return all scheduled events regardless of due time (used at shutdown and in tests)."""
        events = [event for _, _, event in sorted(self._pending)]
        self._pending.clear()
        return events

    # ------------------------------------------------------------ helpers --
    def _uuid(self) -> str:
        return str(uuid.UUID(int=self._rng.getrandbits(128), version=4))

    def _weighted(self, options: Sequence[tuple[T, float]]) -> T:
        return self._rng.choices([v for v, _ in options], weights=[w for _, w in options], k=1)[0]

    def _schedule(self, event: AdEvent) -> None:
        heapq.heappush(self._pending, (event.event_time, next(self._seq), event))

    def _due(self, now: datetime) -> list[AdEvent]:
        due = []
        while self._pending and self._pending[0][0] <= now:
            due.append(heapq.heappop(self._pending)[2])
        return due

    def _context(self) -> AdContext:
        rng = self._rng
        campaign_id = rng.randint(1, ids.NUM_CAMPAIGNS)
        publisher_id = rng.randint(1, ids.NUM_PUBLISHERS)
        device = self._weighted(catalog.DEVICES)
        return AdContext(
            request_id=self._uuid(),
            user_id=ids.user_id_for(rng.randrange(self._num_users)),
            advertiser_id=ids.advertiser_for(campaign_id),
            campaign_id=campaign_id,
            creative_id=rng.choice(ids.creatives_for(campaign_id)),
            publisher_id=publisher_id,
            site_domain=catalog.PUBLISHERS[publisher_id],
            ad_format=self._weighted(catalog.AD_FORMATS),
            device_type=device,
            os=self._weighted(catalog.OS_BY_DEVICE[device]),
            country=self._weighted(catalog.COUNTRIES),
        )

    def _click_delay(self) -> timedelta:
        if self._rng.random() < 0.01:  # outside the attribution window on purpose
            return ATTRIBUTION_WINDOW + timedelta(seconds=self._rng.uniform(60, 600))
        seconds = min(self._rng.lognormvariate(2.0, 1.0), ATTRIBUTION_WINDOW.total_seconds() - 60)
        return timedelta(seconds=max(0.2, seconds))

    def _auction(self, now: datetime) -> list[AdEvent]:
        rng = self._rng
        ctx = self._context()
        low, high = catalog.CPM_RANGE_USD[ctx.ad_format]
        bid_micros = int(rng.uniform(low, high) * 1000)  # CPM USD -> micros per impression

        bid = AdEvent(self._uuid(), EventType.BID, now, ctx, bid_price_micros=bid_micros)
        events = [bid]
        if rng.random() >= self._win_rate:
            return events

        impression_id = self._uuid()
        impression_time = now + timedelta(milliseconds=rng.randint(20, 400))
        impression = AdEvent(
            self._uuid(), EventType.IMPRESSION, impression_time, ctx,
            impression_id=impression_id,
            bid_price_micros=bid_micros,
            win_price_micros=max(1, int(bid_micros * rng.uniform(0.55, 0.95))),
        )
        self._schedule(impression)

        if rng.random() < self._ctr:
            click_id = self._uuid()
            click = AdEvent(
                self._uuid(), EventType.CLICK, impression_time + self._click_delay(), ctx,
                impression_id=impression_id, click_id=click_id,
            )
            self._schedule(click)
            if rng.random() < self._cvr:
                conversion = AdEvent(
                    self._uuid(), EventType.CONVERSION,
                    click.event_time + timedelta(seconds=rng.uniform(30, 1800)), ctx,
                    impression_id=impression_id, click_id=click_id,
                    conversion_value_micros=int(rng.uniform(2, 40) * 1_000_000),
                )
                self._schedule(conversion)
        return events
