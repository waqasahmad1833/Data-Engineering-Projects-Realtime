import json
from datetime import datetime, timedelta, timezone

from adgen.faults import INVALID_KINDS, FaultInjector
from adgen.simulator import AuctionSimulator

NOW = datetime(2026, 11, 10, 12, 0, tzinfo=timezone.utc)


def events(n=500):
    sim = AuctionSimulator(seed=3, clock=lambda: NOW)
    out = []
    for _ in range(n):
        out.extend(sim.tick())
    return out + sim.drain()


def test_no_faults_passes_events_through():
    evs = events()
    records = FaultInjector(seed=1).apply(evs)
    assert len(records) == len(evs)
    assert all(json.loads(r.value)["event_id"] for r in records)


def test_duplicates_are_byte_identical_copies():
    records = FaultInjector(duplicate_ratio=1.0, seed=1).apply(events(50))
    originals, dups = records[0::2], records[1::2]
    assert all(d.kind == "duplicate" and d.value == o.value for o, d in zip(originals, dups))


def test_invalid_records_cover_all_kinds():
    records = FaultInjector(invalid_ratio=1.0, seed=1).apply(events())
    assert {r.kind for r in records} == set(INVALID_KINDS)


def test_late_events_are_shifted_into_the_past():
    late = FaultInjector(late_ratio=1.0, max_late_seconds=30, seed=1).apply(events(50))
    for r in late:
        t = datetime.fromisoformat(json.loads(r.value)["event_time"].replace("Z", "+00:00"))
        assert t <= NOW + timedelta(minutes=31)  # follow-ups are in the future, but all shifted back
    bids = [json.loads(r.value) for r in late if r.kind == "bid"]
    assert all(datetime.fromisoformat(b["event_time"].replace("Z", "+00:00")) < NOW for b in bids)


def test_records_are_keyed_by_user():
    for r in FaultInjector(seed=1).apply(events(20)):
        if r.kind != "malformed_json":
            assert r.key == str(json.loads(r.value)["user_id"]).encode()
