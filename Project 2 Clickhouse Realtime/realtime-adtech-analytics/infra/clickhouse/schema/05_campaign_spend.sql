-- ===========================================================================
-- 10-second campaign spend windows from Flink (also published to the
-- compacted Kafka topic campaign.spend for the budget-pacing service).
-- Late impressions re-fire a window with a higher version; the newest wins.
-- Sharded by campaign_id so one campaign's spend lives on one shard.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS adtech.campaign_spend_10s_local ON CLUSTER adtech
(
    window_start  DateTime('UTC'),
    window_end    DateTime('UTC'),
    campaign_id   UInt32,
    advertiser_id UInt32,
    impressions   UInt64,
    spend_micros  UInt64,
    version       UInt64,
    updated_at    DateTime64(3, 'UTC')
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/campaign_spend_10s_local', '{replica}', version)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (campaign_id, window_start)
TTL window_start + INTERVAL 90 DAY;

CREATE TABLE IF NOT EXISTS adtech.campaign_spend_10s ON CLUSTER adtech
AS adtech.campaign_spend_10s_local
ENGINE = Distributed(adtech, adtech, campaign_spend_10s_local, campaign_id);
