import json
from datetime import datetime, timedelta, timezone

import pytest

from clickstream_producer.generator import ClickstreamGenerator
from clickstream_producer.models import PRODUCT_EVENTS, EventType

FIXED_NOW = datetime(2026, 11, 10, 12, 0, 0, tzinfo=timezone.utc)
REQUIRED_FIELDS = {
    "event_id", "event_type", "user_id", "session_id", "page_url",
    "referrer", "user_agent", "country", "event_time",
}


def make_generator(**kwargs) -> ClickstreamGenerator:
    params = {"num_users": 200, "seed": 42, "clock": lambda: FIXED_NOW}
    params.update(kwargs)
    return ClickstreamGenerator(**params)


def generate(gen: ClickstreamGenerator, n: int):
    return [gen.next_record() for _ in range(n)]


def test_valid_events_contain_required_fields_and_use_session_key():
    for record in generate(make_generator(), 1000):
        assert record.is_valid
        payload = json.loads(record.value)
        assert REQUIRED_FIELDS <= payload.keys()
        assert record.key == payload["session_id"]
        assert payload["event_time"].endswith("Z")


def test_product_events_carry_a_product_and_price():
    for record in generate(make_generator(), 2000):
        payload = json.loads(record.value)
        if EventType(payload["event_type"]) in PRODUCT_EVENTS:
            assert payload["product_id"]
            assert payload["price"] > 0


def test_purchases_have_positive_quantity():
    purchases = [
        json.loads(r.value) for r in generate(make_generator(), 5000)
        if json.loads(r.value)["event_type"] == EventType.PURCHASE.value
    ]
    assert purchases, "expected at least one purchase in 5000 events"
    assert all(p["quantity"] >= 1 for p in purchases)


def test_every_session_starts_with_a_page_view():
    first_seen = {}
    for record in generate(make_generator(), 3000):
        payload = json.loads(record.value)
        first_seen.setdefault(payload["session_id"], payload["event_type"])
    assert set(first_seen.values()) == {EventType.PAGE_VIEW.value}


def test_invalid_ratio_of_one_produces_only_invalid_records():
    records = generate(make_generator(invalid_event_ratio=1.0), 200)
    assert all(not r.is_valid for r in records)
    assert {r.kind for r in records} >= {"malformed_json", "missing_event_id", "unknown_event_type"}


def test_late_events_are_in_the_past():
    for record in generate(make_generator(late_event_ratio=1.0, max_late_seconds=60), 200):
        event_time = datetime.fromisoformat(json.loads(record.value)["event_time"].replace("Z", "+00:00"))
        assert FIXED_NOW - timedelta(seconds=60) <= event_time <= FIXED_NOW - timedelta(seconds=5)


def test_same_seed_produces_identical_streams():
    first = [r.value for r in generate(make_generator(seed=7), 300)]
    second = [r.value for r in generate(make_generator(seed=7), 300)]
    assert first == second


def test_active_sessions_are_bounded():
    gen = make_generator(max_active_sessions=15)
    generate(gen, 2000)
    assert gen.active_sessions <= 15


@pytest.mark.parametrize("kwargs", [
    {"num_users": 0},
    {"invalid_event_ratio": 1.5},
    {"late_event_ratio": -0.1},
    {"max_late_seconds": 1},
])
def test_rejects_invalid_arguments(kwargs):
    with pytest.raises(ValueError):
        make_generator(**kwargs)
