package com.example.adtech.config;

import com.example.adtech.sink.ClickHouseClusterConfig;
import com.example.adtech.sink.ShardedSinkConfig;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.Serializable;
import java.util.Map;

/**
 * Immutable job configuration. Each setting comes from a program argument
 * ({@code --sink.batch-rows 200000}) or an environment variable ({@code SINK_BATCH_ROWS}),
 * arguments taking precedence.
 */
public final class JobConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String jobName;
    private final String kafkaBootstrapServers;
    private final String eventsTopic;
    private final String dlqTopic;
    private final String spendTopic;
    private final String consumerGroupId;

    private final ClickHouseClusterConfig clickHouseCluster;
    private final int sinkBatchRows;
    private final long sinkBatchBytes;
    private final long sinkFlushIntervalMs;
    private final int sinkWritersPerShard;

    private final long checkpointIntervalMs;
    private final long maxOutOfOrdernessMs;
    private final long sourceIdlenessMs;
    private final long dedupTtlMs;
    private final long attributionWindowMs;
    private final long attributionClockSkewMs;
    private final long spendWindowSeconds;
    private final long spendAllowedLatenessSeconds;
    private final long maxFutureSkewMs;
    private final long maxEventAgeMs;

    private JobConfig(Settings s) {
        jobName = s.get("job.name", "JOB_NAME", "adtech-realtime-analytics");
        kafkaBootstrapServers = s.get("kafka.bootstrap-servers", "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092");
        eventsTopic = s.get("kafka.events-topic", "KAFKA_EVENTS_TOPIC", "ad.events");
        dlqTopic = s.get("kafka.dlq-topic", "KAFKA_DLQ_TOPIC", "ad.dlq");
        spendTopic = s.get("kafka.spend-topic", "KAFKA_SPEND_TOPIC", "campaign.spend");
        consumerGroupId = s.get("kafka.group-id", "KAFKA_CONSUMER_GROUP", "flink-adtech");

        clickHouseCluster = ClickHouseClusterConfig.builder()
                .shards(s.get("clickhouse.shards", "CLICKHOUSE_SHARDS", "http://localhost:8123"))
                .user(s.get("clickhouse.user", "CLICKHOUSE_USER", "adtech"))
                .password(s.get("clickhouse.password", "CLICKHOUSE_PASSWORD", ""))
                .database(s.get("clickhouse.database", "CLICKHOUSE_DATABASE", "adtech"))
                .gzip(Boolean.parseBoolean(s.get("clickhouse.gzip", "CLICKHOUSE_GZIP", "true")))
                .maxRetries(s.getInt("clickhouse.max-retries", "CLICKHOUSE_MAX_RETRIES", 6))
                .retryBackoffMs(s.getLong("clickhouse.retry-backoff-ms", "CLICKHOUSE_RETRY_BACKOFF_MS", 500L))
                .requestTimeoutMs(s.getLong("clickhouse.request-timeout-ms", "CLICKHOUSE_REQUEST_TIMEOUT_MS", 120_000L))
                .build();
        sinkBatchRows = s.getInt("sink.batch-rows", "SINK_BATCH_ROWS", 100_000);
        sinkBatchBytes = s.getLong("sink.batch-bytes", "SINK_BATCH_BYTES", 64L * 1024 * 1024);
        sinkFlushIntervalMs = s.getLong("sink.flush-interval-ms", "SINK_FLUSH_INTERVAL_MS", 5_000L);
        sinkWritersPerShard = s.getInt("sink.writers-per-shard", "SINK_WRITERS_PER_SHARD", 1);

        checkpointIntervalMs = s.getLong("checkpoint.interval-ms", "CHECKPOINT_INTERVAL_MS", 30_000L);
        maxOutOfOrdernessMs = s.getLong("watermark.max-out-of-orderness-ms", "MAX_OUT_OF_ORDERNESS_MS", 15_000L);
        sourceIdlenessMs = s.getLong("watermark.idleness-ms", "SOURCE_IDLENESS_MS", 60_000L);
        dedupTtlMs = s.getLong("dedup.ttl-ms", "DEDUP_TTL_MS", 3_600_000L);
        attributionWindowMs = s.getLong("attribution.window-ms", "ATTRIBUTION_WINDOW_MS", 1_800_000L);
        attributionClockSkewMs = s.getLong("attribution.clock-skew-ms", "ATTRIBUTION_CLOCK_SKEW_MS", 10_000L);
        spendWindowSeconds = s.getLong("spend.window-seconds", "SPEND_WINDOW_SECONDS", 10L);
        spendAllowedLatenessSeconds = s.getLong("spend.allowed-lateness-seconds", "SPEND_ALLOWED_LATENESS_SECONDS", 60L);
        maxFutureSkewMs = s.getLong("validation.max-future-skew-ms", "MAX_FUTURE_SKEW_MS", 300_000L);
        maxEventAgeMs = s.getLong("validation.max-event-age-ms", "MAX_EVENT_AGE_MS", 7L * 24 * 3_600_000L);
        validate();
    }

    public static JobConfig fromArgs(String[] args) {
        return new JobConfig(new Settings(ParameterTool.fromArgs(args), System.getenv()));
    }

    /** Visible for tests. */
    static JobConfig from(Map<String, String> args, Map<String, String> env) {
        return new JobConfig(new Settings(ParameterTool.fromMap(args), env));
    }

    private void validate() {
        require(sinkBatchRows > 0 && sinkBatchBytes > 0 && sinkFlushIntervalMs > 0, "sink batch settings must be > 0");
        require(sinkWritersPerShard >= 1, "sink writers per shard must be >= 1");
        require(checkpointIntervalMs >= 1_000, "checkpoint interval must be >= 1s");
        require(dedupTtlMs > 0, "dedup TTL must be > 0");
        require(attributionWindowMs > 0 && attributionClockSkewMs >= 0, "invalid attribution window");
        require(spendWindowSeconds > 0 && spendAllowedLatenessSeconds >= 0, "invalid spend window");
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException("Invalid job configuration: " + message);
        }
    }

    public ShardedSinkConfig sinkConfig(String table) {
        return new ShardedSinkConfig(table, sinkBatchRows, sinkBatchBytes, sinkFlushIntervalMs);
    }

    public String getJobName() { return jobName; }
    public String getKafkaBootstrapServers() { return kafkaBootstrapServers; }
    public String getEventsTopic() { return eventsTopic; }
    public String getDlqTopic() { return dlqTopic; }
    public String getSpendTopic() { return spendTopic; }
    public String getConsumerGroupId() { return consumerGroupId; }
    public ClickHouseClusterConfig getClickHouseCluster() { return clickHouseCluster; }
    public int getSinkWritersPerShard() { return sinkWritersPerShard; }
    public long getCheckpointIntervalMs() { return checkpointIntervalMs; }
    public long getMaxOutOfOrdernessMs() { return maxOutOfOrdernessMs; }
    public long getSourceIdlenessMs() { return sourceIdlenessMs; }
    public long getDedupTtlMs() { return dedupTtlMs; }
    public long getAttributionWindowMs() { return attributionWindowMs; }
    public long getAttributionClockSkewMs() { return attributionClockSkewMs; }
    public long getSpendWindowSeconds() { return spendWindowSeconds; }
    public long getSpendAllowedLatenessSeconds() { return spendAllowedLatenessSeconds; }
    public long getMaxFutureSkewMs() { return maxFutureSkewMs; }
    public long getMaxEventAgeMs() { return maxEventAgeMs; }

    @Override
    public String toString() {
        return "JobConfig{jobName='" + jobName + "', kafka='" + kafkaBootstrapServers + "', eventsTopic='" + eventsTopic
                + "', dlqTopic='" + dlqTopic + "', spendTopic='" + spendTopic + "', group='" + consumerGroupId
                + "', clickhouse=" + clickHouseCluster + ", batchRows=" + sinkBatchRows + ", batchBytes=" + sinkBatchBytes
                + ", flushIntervalMs=" + sinkFlushIntervalMs + ", writersPerShard=" + sinkWritersPerShard
                + ", checkpointIntervalMs=" + checkpointIntervalMs + ", outOfOrdernessMs=" + maxOutOfOrdernessMs
                + ", dedupTtlMs=" + dedupTtlMs + ", attributionWindowMs=" + attributionWindowMs
                + ", spendWindowSeconds=" + spendWindowSeconds + '}';
    }

    private static final class Settings {
        private final ParameterTool args;
        private final Map<String, String> env;

        Settings(ParameterTool args, Map<String, String> env) {
            this.args = args;
            this.env = env;
        }

        String get(String argKey, String envKey, String defaultValue) {
            if (args.has(argKey)) {
                return args.get(argKey).trim();
            }
            String value = env.get(envKey);
            return value == null || value.isBlank() ? defaultValue : value.trim();
        }

        int getInt(String argKey, String envKey, int defaultValue) {
            return Integer.parseInt(get(argKey, envKey, String.valueOf(defaultValue)));
        }

        long getLong(String argKey, String envKey, long defaultValue) {
            return Long.parseLong(get(argKey, envKey, String.valueOf(defaultValue)));
        }
    }
}
