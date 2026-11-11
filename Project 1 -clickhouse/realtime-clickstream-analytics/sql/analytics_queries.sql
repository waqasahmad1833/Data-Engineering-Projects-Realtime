-- ===========================================================================
-- Example analytics queries. Run them in the ClickHouse Play UI
-- (http://localhost:8123/play) or with `make ch-client`.
-- ===========================================================================

-- 1. Traffic overview for the last hour --------------------------------------
SELECT
    event_type,
    count()           AS events,
    uniq(user_id)     AS users,
    uniq(session_id)  AS sessions
FROM analytics.events
WHERE event_time >= now() - INTERVAL 1 HOUR
GROUP BY event_type
ORDER BY events DESC;

-- 2. Conversion funnel with windowFunnel (sessions reaching each step) -------
WITH funnel AS
(
    SELECT
        session_id,
        windowFunnel(3600)(
            toDateTime(event_time),
            event_type = 'page_view',
            event_type = 'product_view',
            event_type = 'add_to_cart',
            event_type = 'checkout_start',
            event_type = 'purchase'
        ) AS level
    FROM analytics.events
    WHERE event_time >= now() - INTERVAL 1 DAY
    GROUP BY session_id
)
SELECT
    countIf(level >= 1)                                          AS page_view,
    countIf(level >= 2)                                          AS product_view,
    countIf(level >= 3)                                          AS add_to_cart,
    countIf(level >= 4)                                          AS checkout_start,
    countIf(level >= 5)                                          AS purchase,
    round(countIf(level >= 5) / nullIf(countIf(level >= 1), 0) * 100, 2) AS conversion_pct
FROM funnel;

-- 3. Top products by revenue (FINAL removes at-least-once duplicates) --------
SELECT
    product_id,
    countIf(event_type = 'product_view')                                   AS views,
    countIf(event_type = 'add_to_cart')                                    AS add_to_cart,
    countIf(event_type = 'purchase')                                       AS purchases,
    round(sumIf(toFloat64(price) * quantity, event_type = 'purchase'), 2)  AS revenue,
    round(purchases / nullIf(views, 0) * 100, 2)                           AS view_to_purchase_pct
FROM analytics.events FINAL
WHERE product_id IS NOT NULL
  AND event_time >= now() - INTERVAL 1 DAY
GROUP BY product_id
ORDER BY revenue DESC
LIMIT 10;

-- 4. Revenue and users by country and device from the hourly rollup ----------
--    (merges pre-aggregated states: scans far fewer rows than the raw table)
SELECT
    country,
    device_type,
    countMerge(events)          AS events,
    uniqMerge(unique_users)     AS users,
    round(sumMerge(revenue), 2) AS revenue
FROM analytics.events_hourly
WHERE hour >= now() - INTERVAL 1 DAY
GROUP BY country, device_type
ORDER BY revenue DESC;

-- 5. Session statistics ------------------------------------------------------
SELECT
    count()                          AS sessions,
    round(avg(events), 2)            AS avg_events_per_session,
    quantile(0.95)(events)           AS p95_events_per_session,
    round(avg(duration_s), 1)        AS avg_session_duration_s
FROM
(
    SELECT
        session_id,
        count()                                              AS events,
        dateDiff('second', min(event_time), max(event_time)) AS duration_s
    FROM analytics.events
    WHERE event_time >= now() - INTERVAL 1 HOUR
    GROUP BY session_id
);

-- 6. Pipeline latency: client event time -> Flink ingestion (ms) -------------
SELECT
    quantiles(0.5, 0.9, 0.99)(
        toUnixTimestamp64Milli(ingested_at) - toUnixTimestamp64Milli(event_time)
    ) AS lag_ms_p50_p90_p99
FROM analytics.events
WHERE event_time >= now() - INTERVAL 1 HOUR;

-- 7. Per-minute metrics from Flink windows (latest version of each window) ---
SELECT
    window_start,
    event_type,
    sum(event_count)     AS events,
    sum(unique_sessions) AS sessions,
    round(sum(revenue), 2) AS revenue,
    max(version)         AS max_version
FROM analytics.page_metrics_1m FINAL
WHERE window_start >= now() - INTERVAL 30 MINUTE
GROUP BY window_start, event_type
ORDER BY window_start DESC, events DESC;

-- 8. Windows that were updated by late events --------------------------------
SELECT window_start, page_url, event_type, version AS firings, event_count
FROM analytics.page_metrics_1m FINAL
WHERE version > 1
ORDER BY version DESC
LIMIT 20;

-- 9. Dead letters (ingested from Kafka by the ClickHouse Kafka engine) -------
SELECT
    error_type,
    count()              AS events,
    max(failed_at)       AS last_seen,
    any(error_message)   AS example_message,
    any(raw_payload)     AS example_payload
FROM analytics.dead_letters
GROUP BY error_type
ORDER BY events DESC;

-- 10. Duplicates waiting to be merged away (at-least-once delivery) ----------
SELECT
    count()                          AS total_rows,
    uniqExact(event_id)              AS unique_events,
    total_rows - unique_events       AS pending_duplicates
FROM analytics.events;

-- 11. Storage footprint and compression per table ----------------------------
SELECT
    table,
    sum(rows)                                                           AS total_rows,
    formatReadableSize(sum(data_compressed_bytes))                      AS compressed,
    formatReadableSize(sum(data_uncompressed_bytes))                    AS uncompressed,
    round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) AS ratio
FROM system.parts
WHERE database = 'analytics' AND active
GROUP BY table
ORDER BY sum(data_compressed_bytes) DESC;
