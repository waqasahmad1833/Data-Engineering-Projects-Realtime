-- ===========================================================================
-- Per-minute campaign rollup maintained by a materialized view on every shard.
-- Dashboards read this (thousands of times fewer rows than ad_events).
-- ===========================================================================
CREATE TABLE IF NOT EXISTS adtech.campaign_stats_1m_local ON CLUSTER adtech
(
    minute                  DateTime('UTC'),
    advertiser_id           UInt32,
    campaign_id             UInt32,
    country                 LowCardinality(String),
    device_type             LowCardinality(String),
    bids                    SimpleAggregateFunction(sum, UInt64),
    impressions             SimpleAggregateFunction(sum, UInt64),
    clicks                  SimpleAggregateFunction(sum, UInt64),
    conversions             SimpleAggregateFunction(sum, UInt64),
    bid_value_micros        SimpleAggregateFunction(sum, UInt64),
    spend_micros            SimpleAggregateFunction(sum, UInt64),
    conversion_value_micros SimpleAggregateFunction(sum, UInt64),
    unique_users            AggregateFunction(uniqCombined64, UInt64)
)
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/{database}/campaign_stats_1m_local', '{replica}')
PARTITION BY toYYYYMM(minute)
ORDER BY (advertiser_id, campaign_id, minute, country, device_type)
TTL minute + INTERVAL 395 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS adtech.campaign_stats_1m_mv ON CLUSTER adtech
TO adtech.campaign_stats_1m_local
AS
SELECT
    toStartOfMinute(event_time)                              AS minute,
    advertiser_id,
    campaign_id,
    country,
    device_type,
    countIf(event_type = 'bid')                              AS bids,
    countIf(event_type = 'impression')                       AS impressions,
    countIf(event_type = 'click')                            AS clicks,
    countIf(event_type = 'conversion')                       AS conversions,
    sumIf(bid_price_micros, event_type = 'bid')              AS bid_value_micros,
    sumIf(win_price_micros, event_type = 'impression')       AS spend_micros,
    sumIf(conversion_value_micros, event_type = 'conversion') AS conversion_value_micros,
    uniqCombined64State(user_id)                             AS unique_users
FROM adtech.ad_events_local
GROUP BY minute, advertiser_id, campaign_id, country, device_type;

CREATE TABLE IF NOT EXISTS adtech.campaign_stats_1m ON CLUSTER adtech
AS adtech.campaign_stats_1m_local
ENGINE = Distributed(adtech, adtech, campaign_stats_1m_local);
