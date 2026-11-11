-- ===========================================================================
-- Query cookbook. Run against any node (http://localhost:8123/play, user adtech).
-- Business queries read the Distributed tables; ops queries use
-- clusterAllReplicas() to inspect every node at once.
-- ===========================================================================

-- [campaign_kpis] Real-time campaign KPIs for the last hour (rollup, not raw) ---
-- Aliases deliberately differ from column names: in ClickHouse an alias like
-- `sum(clicks) AS clicks` would make every later `sum(clicks)` a nested aggregate.
SELECT
    campaign_id,
    dictGet('adtech.campaigns_dict', 'campaign_name', toUInt64(campaign_id))   AS campaign,
    sum(impressions)                                                           AS imps,
    sum(clicks)                                                                AS clks,
    sum(conversions)                                                           AS convs,
    round(sum(spend_micros) / 1e6, 2)                                          AS spend_usd,
    round(clks / nullIf(imps, 0) * 100, 3)                                     AS ctr_pct,
    round(sum(spend_micros) / nullIf(imps, 0) / 1e3, 3)                        AS cpm_usd,
    round(sum(spend_micros) / nullIf(clks, 0) / 1e6, 3)                        AS cpc_usd,
    round(sum(conversion_value_micros) / nullIf(sum(spend_micros), 0), 2)      AS roas,
    uniqCombined64Merge(unique_users)                                          AS reach
FROM adtech.campaign_stats_1m
WHERE minute >= now() - INTERVAL 1 HOUR
GROUP BY campaign_id
ORDER BY spend_usd DESC
LIMIT 20;

-- [budget_pacing] Today's spend vs daily budget, newest version of each 10 s window ---
SELECT
    campaign_id,
    dictGet('adtech.campaigns_dict', 'campaign_name', toUInt64(campaign_id))           AS campaign,
    round(sum(spend) / 1e6, 2)                                                         AS spent_today_usd,
    round(dictGet('adtech.campaigns_dict', 'daily_budget_micros', toUInt64(campaign_id)) / 1e6, 2) AS daily_budget_usd,
    round(spent_today_usd / daily_budget_usd * 100, 2)                                 AS budget_used_pct,
    round(spent_today_usd / greatest(dateDiff('second', toStartOfDay(now()), now()), 1) * 86400, 2) AS projected_eod_usd
FROM
(
    SELECT campaign_id, window_start, argMax(spend_micros, version) AS spend
    FROM adtech.campaign_spend_10s
    WHERE window_start >= toStartOfDay(now())
    GROUP BY campaign_id, window_start
)
GROUP BY campaign_id
ORDER BY budget_used_pct DESC
LIMIT 20;

-- [time_to_click] How quickly users click after seeing an ad ---------------
SELECT
    device_type,
    count()                                                     AS clicks,
    quantiles(0.5, 0.9, 0.99)(time_to_click_ms / 1000)          AS seconds_p50_p90_p99
FROM adtech.attributed_clicks
WHERE click_time >= now() - INTERVAL 1 DAY
GROUP BY device_type
ORDER BY clicks DESC;

-- [frequency] Impressions per user per day (frequency-capping analysis) -----
-- Grouping by the sharding key (user_id) lets each shard finish its own
-- aggregation (optimize_distributed_group_by_sharding_key=1 in the profile).
SELECT
    frequency,
    count() AS users
FROM
(
    SELECT user_id, count() AS frequency
    FROM adtech.ad_events
    WHERE event_type = 'impression' AND event_time >= toStartOfDay(now())
    GROUP BY user_id
)
GROUP BY frequency
ORDER BY frequency
LIMIT 20;

-- [request_funnel] Full funnel per request: co-located on one shard by user_id ---
SELECT
    ad_format,
    count()                                                  AS requests,
    countIf(is_won)                                          AS won_requests,
    countIf(is_clicked)                                      AS clicked_requests,
    countIf(is_converted)                                    AS converted_requests,
    round(won_requests / requests * 100, 2)                  AS win_rate_pct,
    round(clicked_requests / nullIf(won_requests, 0) * 100, 3) AS ctr_pct
FROM
(
    SELECT
        request_id,
        any(ad_format)                       AS ad_format,
        max(event_type = 'impression')       AS is_won,
        max(event_type = 'click')            AS is_clicked,
        max(event_type = 'conversion')       AS is_converted
    FROM adtech.ad_events
    WHERE event_time >= now() - INTERVAL 1 HOUR
    GROUP BY request_id
)
GROUP BY ad_format
ORDER BY requests DESC;

-- [bid_shading] Win rate and clearing price vs bid by format -----------------
SELECT
    ad_format,
    countIf(event_type = 'bid')                                                    AS bids,
    countIf(event_type = 'impression')                                             AS wins,
    round(wins / nullIf(bids, 0) * 100, 2)                                         AS win_rate_pct,
    round(avgIf(win_price_micros / bid_price_micros, event_type = 'impression'), 3) AS avg_clear_to_bid
FROM adtech.ad_events
WHERE event_time >= now() - INTERVAL 1 HOUR
GROUP BY ad_format
ORDER BY bids DESC;

-- [publisher_hourly] Publisher report - answered from the p_publisher_hourly projection ---
-- Filter on toStartOfHour(event_time), not event_time: the projection only stores the
-- hour, so a raw event_time filter would silently fall back to scanning raw rows.
SELECT
    publisher_id,
    site_domain,
    toStartOfHour(event_time)          AS hour,
    count()                            AS events,
    round(sum(win_price_micros) / 1e6, 2) AS revenue_usd
FROM adtech.ad_events
WHERE toStartOfHour(event_time) >= toStartOfHour(now() - INTERVAL 6 HOUR)
GROUP BY publisher_id, site_domain, event_type, hour
ORDER BY revenue_usd DESC
LIMIT 20;

-- [dead_letters] Data-quality issues from the DLQ ---------------------------
SELECT error_type, count() AS events, max(failed_at) AS last_seen, any(error_message) AS example
FROM adtech.dead_letters
WHERE failed_at >= now() - INTERVAL 1 DAY
GROUP BY error_type
ORDER BY events DESC;

-- ===========================================================================
-- Operations
-- ===========================================================================

-- [ops_storage_tiers] Data per node and storage tier (hot NVMe vs S3) -------
SELECT
    hostName()                                          AS host,
    disk_name,
    count()                                             AS parts,
    sum(rows)                                           AS total_rows,
    formatReadableSize(sum(bytes_on_disk))              AS size_on_disk
FROM clusterAllReplicas('adtech', system.parts)
WHERE database = 'adtech' AND table = 'ad_events_local' AND active
GROUP BY host, disk_name
ORDER BY host, disk_name;

-- [ops_compression] Compression per column of the big table (one replica per shard) ---
SELECT
    name                                                                AS column,
    formatReadableSize(sum(data_compressed_bytes))                      AS compressed,
    formatReadableSize(sum(data_uncompressed_bytes))                    AS uncompressed,
    round(sum(data_uncompressed_bytes) / nullIf(sum(data_compressed_bytes), 0), 1) AS ratio
FROM cluster('adtech', system.columns)
WHERE database = 'adtech' AND table = 'ad_events_local'
GROUP BY name
ORDER BY sum(data_compressed_bytes) DESC;

-- [ops_parts_health] Parts per partition - alert well before parts_to_delay_insert (300) ---
SELECT hostName() AS host, table, partition, count() AS active_parts
FROM clusterAllReplicas('adtech', system.parts)
WHERE database = 'adtech' AND active
GROUP BY host, table, partition
ORDER BY active_parts DESC
LIMIT 10;

-- [ops_replication] Replication lag and queue per replica --------------------
SELECT hostName() AS host, table, is_readonly, queue_size, inserts_in_queue, absolute_delay
FROM clusterAllReplicas('adtech', system.replicas)
WHERE database = 'adtech'
ORDER BY absolute_delay DESC, queue_size DESC;

-- [ops_insert_rate] Inserts and rows per minute per table (last 15 min) ------
SELECT
    hostName()                         AS host,
    table,
    toStartOfMinute(event_time)        AS minute,
    count()                            AS new_parts,
    sum(rows)                          AS rows
FROM clusterAllReplicas('adtech', system.part_log)
WHERE database = 'adtech' AND event_type = 'NewPart' AND event_time >= now() - INTERVAL 15 MINUTE
GROUP BY host, table, minute
ORDER BY minute DESC, host, table;
