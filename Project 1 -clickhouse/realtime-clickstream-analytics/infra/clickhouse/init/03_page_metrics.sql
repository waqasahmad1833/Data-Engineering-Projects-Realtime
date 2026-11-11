-- ---------------------------------------------------------------------------
-- 1-minute tumbling-window aggregates computed by Flink (event time).
--
-- Late events (within the allowed lateness) make Flink re-emit an updated
-- result for the same window. Each emission carries a monotonically increasing
-- `version` (a per-window counter kept in Flink state), and
-- ReplacingMergeTree(version) keeps only the most complete result per
-- (window_start, page_url, event_type) -> query with FINAL.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.page_metrics_1m
(
    window_start    DateTime('UTC'),
    window_end      DateTime('UTC'),
    page_url        String,
    event_type      LowCardinality(String),
    event_count     UInt64,
    unique_users    UInt64,
    unique_sessions UInt64,
    revenue         Float64,
    version         UInt64,
    updated_at      DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (window_start, page_url, event_type)
TTL window_start + INTERVAL 180 DAY;
