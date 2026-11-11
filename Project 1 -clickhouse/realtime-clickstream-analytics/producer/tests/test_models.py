import json
from datetime import datetime, timedelta, timezone

import pytest

from clickstream_producer.models import ClickEvent, EventType, format_timestamp


def test_format_timestamp_is_utc_with_millis():
    ts = datetime(2026, 11, 10, 13, 0, 0, 123456, tzinfo=timezone(timedelta(hours=1)))
    assert format_timestamp(ts) == "2026-11-10T12:00:00.123Z"


def test_format_timestamp_rejects_naive_datetimes():
    with pytest.raises(ValueError):
        format_timestamp(datetime(2026, 1, 1))


def test_to_json_round_trip():
    event = ClickEvent(
        event_id="7f3c1a52-3c2b-4b7e-9a51-0c9f0e1d2a3b", event_type=EventType.PURCHASE,
        user_id="u_000001", session_id="s1", page_url="/checkout/confirmation",
        referrer="", user_agent="ua", country="DE",
        event_time=datetime(2026, 11, 10, tzinfo=timezone.utc),
        product_id="ele-0001", price=99.5, quantity=2,
    )
    payload = json.loads(event.to_json())
    assert payload["event_type"] == "purchase"
    assert payload["price"] == 99.5
    assert payload["event_time"] == "2026-11-10T00:00:00.000Z"
