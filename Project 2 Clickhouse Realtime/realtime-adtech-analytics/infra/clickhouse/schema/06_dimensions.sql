-- ===========================================================================
-- Campaign dimension: a small table fully replicated to EVERY node (shared
-- Keeper path without {shard}), exposed as an in-memory dictionary for
-- dictGet() lookups in dashboards instead of distributed JOINs.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS adtech.campaigns ON CLUSTER adtech
(
    campaign_id         UInt32,
    advertiser_id       UInt32,
    campaign_name       String,
    advertiser_name     String,
    objective           LowCardinality(String),
    daily_budget_micros UInt64,
    updated_at          DateTime('UTC')
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/global/{database}/campaigns', '{replica}', updated_at)
ORDER BY campaign_id;

-- Seed data. Must match generator/src/adgen/ids.py (500 campaigns, 50 advertisers).
-- A fixed updated_at keeps the block identical, so re-running is deduplicated.
INSERT INTO adtech.campaigns
SELECT
    toUInt32(number + 1)                                                    AS campaign_id,
    toUInt32(number % 50 + 1)                                               AS advertiser_id,
    concat('Campaign ', toString(number + 1))                               AS campaign_name,
    concat('Advertiser ', toString(number % 50 + 1))                        AS advertiser_name,
    ['awareness', 'consideration', 'conversion'][number % 3 + 1]            AS objective,
    toUInt64((1000 + (number % 20) * 500) * 1000000)                        AS daily_budget_micros,
    toDateTime('2026-01-01 00:00:00', 'UTC')                                AS updated_at
FROM numbers(500);

CREATE DICTIONARY IF NOT EXISTS adtech.campaigns_dict ON CLUSTER adtech
(
    campaign_id         UInt64,
    advertiser_id       UInt32,
    campaign_name       String,
    advertiser_name     String,
    objective           String,
    daily_budget_micros UInt64
)
PRIMARY KEY campaign_id
SOURCE(CLICKHOUSE(
    HOST 'localhost' PORT 9000 USER 'dict_reader' PASSWORD ''
    QUERY 'SELECT campaign_id, advertiser_id, campaign_name, advertiser_name, objective, daily_budget_micros FROM adtech.campaigns FINAL'
))
LIFETIME(MIN 60 MAX 300)
LAYOUT(HASHED());
