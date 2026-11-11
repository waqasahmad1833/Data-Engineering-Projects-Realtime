-- ===========================================================================
-- Clicks attributed to their impression by a Flink interval join
-- (click within 30 minutes of the impression). Sharded by user_id.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS adtech.attributed_clicks_local ON CLUSTER adtech
(
    click_id               UUID,
    impression_id          UUID,
    request_id             UUID,
    user_id                UInt64,
    advertiser_id          UInt32,
    campaign_id            UInt32,
    creative_id            UInt32,
    publisher_id           UInt32,
    site_domain            LowCardinality(String),
    device_type            LowCardinality(String),
    country                LowCardinality(String),
    impression_time        DateTime64(3, 'UTC'),
    click_time             DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    time_to_click_ms       Int64,
    impression_cost_micros UInt64,
    processed_at           DateTime64(3, 'UTC')
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/attributed_clicks_local', '{replica}', processed_at)
PARTITION BY toYYYYMM(click_time)
ORDER BY (advertiser_id, campaign_id, toStartOfHour(click_time), click_id)
TTL toDateTime(click_time) + INTERVAL 395 DAY;

CREATE TABLE IF NOT EXISTS adtech.attributed_clicks ON CLUSTER adtech
AS adtech.attributed_clicks_local
ENGINE = Distributed(adtech, adtech, attributed_clicks_local, user_id);
