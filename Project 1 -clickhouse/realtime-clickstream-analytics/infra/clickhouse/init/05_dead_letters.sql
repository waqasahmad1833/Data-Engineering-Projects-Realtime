-- ---------------------------------------------------------------------------
-- Dead-letter observability using the ClickHouse Kafka table engine.
-- Flink publishes invalid events to the clickstream.dlq topic; ClickHouse
-- consumes that topic directly and persists it via a materialized view.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.dead_letters
(
    error_type      LowCardinality(String),
    error_message   String,
    source_topic    LowCardinality(String),
    raw_payload     String CODEC(ZSTD(3)),
    failed_at       DateTime64(3, 'UTC'),
    kafka_partition UInt64,
    kafka_offset    UInt64,
    inserted_at     DateTime('UTC') DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(failed_at)
ORDER BY (error_type, failed_at)
TTL toDateTime(failed_at) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS analytics.dead_letters_queue
(
    error_type    String,
    error_message String,
    source_topic  String,
    raw_payload   String,
    failed_at     DateTime64(3, 'UTC')
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'clickstream.dlq',
    kafka_group_name = 'clickhouse-dlq-consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 100;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.dead_letters_mv
TO analytics.dead_letters
AS
SELECT
    error_type,
    error_message,
    source_topic,
    raw_payload,
    failed_at,
    _partition AS kafka_partition,
    _offset    AS kafka_offset
FROM analytics.dead_letters_queue;
