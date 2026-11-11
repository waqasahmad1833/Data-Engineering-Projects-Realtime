-- ===========================================================================
-- Raw ad events (bids, impressions, clicks, conversions) - the 30+ TB/month table.
--
-- * Sharded by user_id: Flink routes each row to shard (user_id % shards),
--   exactly what the Distributed table's sharding key does, so a user's full
--   funnel is co-located and per-user analysis runs shard-locally.
-- * Plain ReplicatedMergeTree (no FINAL needed): Flink deduplicates by event_id
--   and retried inserts reuse an insert_deduplication_token.
-- * Tiering: 7 days on local NVMe, then S3; bids (the bulk of volume) are
--   deleted after 30 days, everything else after 180 days.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS adtech.ad_events_local ON CLUSTER adtech
(
    event_time              DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    event_type              Enum8('bid' = 1, 'impression' = 2, 'click' = 3, 'conversion' = 4),
    event_id                UUID,
    request_id              UUID,
    impression_id           UUID,
    click_id                UUID,
    user_id                 UInt64,
    advertiser_id           UInt32,
    campaign_id             UInt32,
    creative_id             UInt32,
    publisher_id            UInt32,
    site_domain             LowCardinality(String),
    ad_format               LowCardinality(String),
    device_type             LowCardinality(String),
    os                      LowCardinality(String),
    country                 LowCardinality(String),
    bid_price_micros        UInt64 CODEC(T64, ZSTD(1)),
    win_price_micros        UInt64 CODEC(T64, ZSTD(1)),
    conversion_value_micros UInt64 CODEC(T64, ZSTD(1)),
    ingested_at             DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),

    INDEX idx_request_id request_id TYPE bloom_filter(0.001) GRANULARITY 4,
    INDEX idx_user_id    user_id    TYPE bloom_filter(0.01)  GRANULARITY 4,
    INDEX idx_publisher  publisher_id TYPE minmax GRANULARITY 4,

    -- Pre-aggregated publisher report, maintained automatically inside each part
    PROJECTION p_publisher_hourly
    (
        SELECT
            publisher_id,
            site_domain,
            event_type,
            toStartOfHour(event_time),
            count(),
            sum(win_price_micros)
        GROUP BY publisher_id, site_domain, event_type, toStartOfHour(event_time)
    )
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/ad_events_local', '{replica}')
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (advertiser_id, campaign_id, event_type, toStartOfHour(event_time), user_id)
TTL toDateTime(event_time) + INTERVAL 7 DAY TO VOLUME 'cold',
    toDateTime(event_time) + INTERVAL 30 DAY DELETE WHERE event_type = 'bid',
    toDateTime(event_time) + INTERVAL 180 DAY DELETE
SETTINGS storage_policy = 'hot_cold',
         index_granularity = 8192,
         merge_with_ttl_timeout = 14400;

CREATE TABLE IF NOT EXISTS adtech.ad_events ON CLUSTER adtech
AS adtech.ad_events_local
ENGINE = Distributed(adtech, adtech, ad_events_local, user_id);
