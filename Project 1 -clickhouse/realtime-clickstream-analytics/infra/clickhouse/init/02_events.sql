-- ---------------------------------------------------------------------------
-- Raw, validated and enriched clickstream events written by Flink.
--
-- The Flink sink is at-least-once: after a failure, rows written since the
-- last checkpoint can be written again. ReplacingMergeTree collapses those
-- duplicates (same sorting key -> same event) during background merges.
-- Use FINAL (or GROUP BY event_id) when exact counts matter.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.events
(
    event_id     UUID,
    event_type   LowCardinality(String),
    user_id      String,
    session_id   String,
    page_url     String,
    referrer     LowCardinality(String),
    product_id   Nullable(String),
    price        Nullable(Decimal(12, 2)),
    quantity     Nullable(UInt16),
    device_type  LowCardinality(String),
    country      LowCardinality(FixedString(2)),
    user_agent   String CODEC(ZSTD(3)),
    event_time   DateTime64(3, 'UTC') CODEC(Delta, ZSTD(1)),
    ingested_at  DateTime64(3, 'UTC') CODEC(Delta, ZSTD(1)),
    processed_at DateTime64(3, 'UTC') CODEC(Delta, ZSTD(1)),

    INDEX idx_session_id session_id TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_user_id    user_id    TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_product_id product_id TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = ReplacingMergeTree(processed_at)
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (event_type, event_time, event_id)
TTL toDateTime(event_time) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;
