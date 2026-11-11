"""Deterministic identifiers shared with the ClickHouse seed data (06_dimensions.sql)."""

from __future__ import annotations

NUM_ADVERTISERS = 50
NUM_CAMPAIGNS = 500
CREATIVES_PER_CAMPAIGN = 5
NUM_PUBLISHERS = 200

_MASK_64 = (1 << 64) - 1
_MASK_63 = (1 << 63) - 1


def advertiser_for(campaign_id: int) -> int:
    """Campaign n belongs to advertiser ((n - 1) % 50) + 1, as seeded in ClickHouse."""
    if not 1 <= campaign_id <= NUM_CAMPAIGNS:
        raise ValueError(f"campaign_id out of range: {campaign_id}")
    return (campaign_id - 1) % NUM_ADVERTISERS + 1


def creatives_for(campaign_id: int) -> tuple[int, ...]:
    return tuple(campaign_id * 10 + i for i in range(CREATIVES_PER_CAMPAIGN))


def splitmix64(x: int) -> int:
    """Fast, well-mixed 64-bit hash (used to turn user indexes into exchange-style hashed IDs)."""
    x = (x + 0x9E3779B97F4A7C15) & _MASK_64
    x = ((x ^ (x >> 30)) * 0xBF58476D1CE4E5B9) & _MASK_64
    x = ((x ^ (x >> 27)) * 0x94D049BB133111EB) & _MASK_64
    return x ^ (x >> 31)


def user_id_for(index: int) -> int:
    """Uniformly distributed, non-negative 63-bit user id (safe as a Java long and a ClickHouse UInt64).

    Uniformity matters: ClickHouse shards by ``user_id % shards``.
    """
    return splitmix64(index) & _MASK_63
