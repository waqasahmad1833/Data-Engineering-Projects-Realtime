-- ===========================================================================
-- Dead letters: every node runs a Kafka-engine consumer in the same consumer
-- group, so the ad.dlq partitions are spread across the cluster. Rows land in
-- a replicated local table and are queried through a Distributed table.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS adtech.dead_letters_local ON CLUSTER adtech
(
    error_type      LowCardinality(String),
    error_message   String,
    raw_payload     String CODEC(ZSTD(3)),
    failed_at       DateTime64(3, 'UTC'),
    kafka_partition UInt64,
    kafka_offset    UInt64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/dead_letters_local', '{replica}')
PARTITION BY toYYYYMMDD(failed_at)
ORDER BY (error_type, failed_at)
TTL toDateTime(failed_at) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS adtech.dead_letters_queue ON CLUSTER adtech
(
    error_type    String,
    error_message String,
    raw_payload   String,
    failed_at     DateTime64(3, 'UTC')
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'ad.dlq',
         kafka_group_name = 'clickhouse-dlq',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1,
         kafka_skip_broken_messages = 1000;

CREATE MATERIALIZED VIEW IF NOT EXISTS adtech.dead_letters_mv ON CLUSTER adtech
TO adtech.dead_letters_local
AS
SELECT error_type, error_message, raw_payload, failed_at,
       _partition AS kafka_partition, _offset AS kafka_offset
FROM adtech.dead_letters_queue;

CREATE TABLE IF NOT EXISTS adtech.dead_letters ON CLUSTER adtech
AS adtech.dead_letters_local
ENGINE = Distributed(adtech, adtech, dead_letters_local, rand());
