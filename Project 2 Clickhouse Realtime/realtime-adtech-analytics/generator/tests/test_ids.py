from collections import Counter

import pytest

from adgen import ids


def test_advertiser_mapping_matches_clickhouse_seed():
    # ClickHouse seed: advertiser_id = (number % 50) + 1 where number = campaign_id - 1
    assert ids.advertiser_for(1) == 1
    assert ids.advertiser_for(50) == 50
    assert ids.advertiser_for(51) == 1
    assert ids.advertiser_for(500) == 50


def test_advertiser_rejects_unknown_campaign():
    with pytest.raises(ValueError):
        ids.advertiser_for(501)


def test_user_ids_are_non_negative_63_bit():
    for i in range(10_000):
        uid = ids.user_id_for(i)
        assert 0 <= uid < 2**63


def test_user_ids_spread_evenly_across_shards():
    # The cluster shards by user_id % shards, so the distribution must be uniform.
    for shards in (2, 4, 8):
        counts = Counter(ids.user_id_for(i) % shards for i in range(40_000))
        expected = 40_000 / shards
        assert all(abs(c - expected) / expected < 0.05 for c in counts.values())


def test_creatives_are_unique_per_campaign():
    assert len(set(ids.creatives_for(7))) == ids.CREATIVES_PER_CAMPAIGN
    assert not set(ids.creatives_for(7)) & set(ids.creatives_for(8))
