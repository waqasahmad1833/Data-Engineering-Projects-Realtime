-- ---------------------------------------------------------------------------
-- ClickHouse-native rollup: a materialized view incrementally aggregates
-- every insert into analytics.events into hourly partial aggregate states.
-- Query through analytics.events_hourly_v (which merges the states).
--
-- Note: the MV sees rows at insert time, before ReplacingMergeTree removes
-- duplicates, so after a Flink restart "events" and "revenue" can be slightly
-- over-counted. uniq() based columns are unaffected.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.events_hourly
(
    hour            DateTime('UTC'),
    event_type      LowCardinality(String),
    country         LowCardinality(FixedString(2)),
    device_type     LowCardinality(String),
    events          AggregateFunction(count),
    unique_users    AggregateFunction(uniq, String),
    unique_sessions AggregateFunction(uniq, String),
    revenue         AggregateFunction(sum, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, event_type, country, device_type);

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.events_hourly_mv
TO analytics.events_hourly
AS
SELECT
    toStartOfHour(event_time)                                                   AS hour,
    event_type,
    country,
    device_type,
    countState()                                                                AS events,
    uniqState(user_id)                                                          AS unique_users,
    uniqState(session_id)                                                       AS unique_sessions,
    sumState(if(event_type = 'purchase',
                toFloat64(ifNull(price, 0)) * ifNull(quantity, 1),
                0.))                                                            AS revenue
FROM analytics.events
GROUP BY hour, event_type, country, device_type;

CREATE VIEW IF NOT EXISTS analytics.events_hourly_v
AS
SELECT
    hour,
    event_type,
    country,
    device_type,
    countMerge(events)            AS events,
    uniqMerge(unique_users)       AS unique_users,
    uniqMerge(unique_sessions)    AS unique_sessions,
    round(sumMerge(revenue), 2)   AS revenue
FROM analytics.events_hourly
GROUP BY hour, event_type, country, device_type;
