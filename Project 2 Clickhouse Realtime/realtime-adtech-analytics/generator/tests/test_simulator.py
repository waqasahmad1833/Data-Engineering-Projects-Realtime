import json
from datetime import datetime, timedelta, timezone

from adgen.models import EventType
from adgen.simulator import ATTRIBUTION_WINDOW, AuctionSimulator


class FakeClock:
    def __init__(self, start: datetime) -> None:
        self.now = start

    def __call__(self) -> datetime:
        return self.now


def run(sim: AuctionSimulator, clock: FakeClock, auctions: int, step_ms: int = 10):
    events = []
    for _ in range(auctions):
        events.extend(sim.tick())
        clock.now += timedelta(milliseconds=step_ms)
    events.extend(sim.drain())
    return events


def make(**kwargs):
    clock = FakeClock(datetime(2026, 11, 10, 12, 0, tzinfo=timezone.utc))
    params = {"num_users": 10_000, "seed": 1, "clock": clock}
    params.update(kwargs)
    return AuctionSimulator(**params), clock


def test_every_auction_emits_a_bid():
    sim, clock = make()
    events = run(sim, clock, 2_000)
    assert sum(e.event_type is EventType.BID for e in events) == 2_000


def test_win_rate_and_prices():
    sim, clock = make(win_rate=0.35)
    events = run(sim, clock, 20_000)
    impressions = [e for e in events if e.event_type is EventType.IMPRESSION]
    assert 0.32 < len(impressions) / 20_000 < 0.38
    assert all(0 < e.win_price_micros <= e.bid_price_micros for e in impressions)
    assert all(e.impression_id for e in impressions)


def test_clicks_follow_their_impression():
    sim, clock = make(ctr=0.2)
    events = run(sim, clock, 5_000)
    impressions = {e.impression_id: e for e in events if e.event_type is EventType.IMPRESSION}
    clicks = [e for e in events if e.event_type is EventType.CLICK]
    assert clicks
    for click in clicks:
        imp = impressions[click.impression_id]
        assert click.event_time > imp.event_time
        assert click.context == imp.context
    outside = sum(c.event_time - impressions[c.impression_id].event_time > ATTRIBUTION_WINDOW for c in clicks)
    assert outside / len(clicks) < 0.05


def test_conversions_reference_clicks_and_carry_value():
    sim, clock = make(ctr=0.5, cvr=0.5)
    events = run(sim, clock, 3_000)
    click_ids = {e.click_id for e in events if e.event_type is EventType.CLICK}
    conversions = [e for e in events if e.event_type is EventType.CONVERSION]
    assert conversions
    assert all(c.click_id in click_ids and c.conversion_value_micros > 0 for c in conversions)


def test_followups_are_not_emitted_before_they_are_due():
    sim, clock = make(win_rate=1.0, ctr=1.0)
    emitted = sim.tick()
    assert [e.event_type for e in emitted] == [EventType.BID]
    assert sim.pending_events >= 2  # impression (+ click) scheduled for later


def test_wire_format():
    sim, clock = make()
    payload = json.loads(run(sim, clock, 1)[0].to_json())
    assert payload["event_type"] == "bid"
    assert payload["event_time"].endswith("Z")
    assert isinstance(payload["user_id"], int)
    assert "impression_id" not in payload


def test_same_seed_is_deterministic():
    a, ca = make(seed=9)
    b, cb = make(seed=9)
    assert [e.to_json() for e in run(a, ca, 500)] == [e.to_json() for e in run(b, cb, 500)]
