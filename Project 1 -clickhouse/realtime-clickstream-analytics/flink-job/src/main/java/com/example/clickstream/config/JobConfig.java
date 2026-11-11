package com.example.clickstream.config;

import com.example.clickstream.sink.ClickHouseSinkConfig;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.Serializable;
import java.util.Map;

/**
 * Immutable job configuration.
 *
 * <p>Every setting can be provided as a program argument ({@code --kafka.bootstrap-servers ...})
 * or as an environment variable ({@code KAFKA_BOOTSTRAP_SERVERS}); arguments win over env vars.
 */
public final class JobConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String jobName;
    private final String kafkaBootstrapServers;
    private final String sourceTopic;
    private final String dlqTopic;
    private final String consumerGroupId;
    private final String clickhouseUrl;
    private final String clickhouseDatabase;
    private final String clickhouseUser;
    private final String clickhousePassword;
    private final int sinkBatchSize;
    private final long sinkFlushIntervalMs;
    private final int sinkMaxRetries;
    private final long sinkRetryBackoffMs;
    private final long checkpointIntervalMs;
    private final long maxOutOfOrdernessMs;
    private final long sourceIdlenessMs;
    private final long windowSizeSeconds;
    private final long allowedLatenessSeconds;
    private final long maxFutureSkewMs;

    private JobConfig(Settings s) {
        this.jobName = s.get("job.name", "JOB_NAME", "clickstream-analytics");
        this.kafkaBootstrapServers = s.get("kafka.bootstrap-servers", "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092");
        this.sourceTopic = s.get("kafka.source-topic", "KAFKA_SOURCE_TOPIC", "clickstream.events");
        this.dlqTopic = s.get("kafka.dlq-topic", "KAFKA_DLQ_TOPIC", "clickstream.dlq");
        this.consumerGroupId = s.get("kafka.group-id", "KAFKA_CONSUMER_GROUP", "flink-clickstream");
        this.clickhouseUrl = s.get("clickhouse.url", "CLICKHOUSE_URL", "http://localhost:8123");
        this.clickhouseDatabase = s.get("clickhouse.database", "CLICKHOUSE_DATABASE", "analytics");
        this.clickhouseUser = s.get("clickhouse.user", "CLICKHOUSE_USER", "default");
        this.clickhousePassword = s.get("clickhouse.password", "CLICKHOUSE_PASSWORD", "");
        this.sinkBatchSize = s.getInt("sink.batch-size", "SINK_BATCH_SIZE", 5000);
        this.sinkFlushIntervalMs = s.getLong("sink.flush-interval-ms", "SINK_FLUSH_INTERVAL_MS", 2000L);
        this.sinkMaxRetries = s.getInt("sink.max-retries", "SINK_MAX_RETRIES", 5);
        this.sinkRetryBackoffMs = s.getLong("sink.retry-backoff-ms", "SINK_RETRY_BACKOFF_MS", 500L);
        this.checkpointIntervalMs = s.getLong("checkpoint.interval-ms", "CHECKPOINT_INTERVAL_MS", 10_000L);
        this.maxOutOfOrdernessMs = s.getLong("watermark.max-out-of-orderness-ms", "MAX_OUT_OF_ORDERNESS_MS", 5_000L);
        this.sourceIdlenessMs = s.getLong("watermark.idleness-ms", "SOURCE_IDLENESS_MS", 60_000L);
        this.windowSizeSeconds = s.getLong("window.size-seconds", "WINDOW_SIZE_SECONDS", 60L);
        this.allowedLatenessSeconds = s.getLong("window.allowed-lateness-seconds", "ALLOWED_LATENESS_SECONDS", 120L);
        this.maxFutureSkewMs = s.getLong("validation.max-future-skew-ms", "MAX_FUTURE_SKEW_MS", 300_000L);
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
        require(sinkBatchSize > 0, "sink batch size must be > 0");
        require(sinkFlushIntervalMs > 0, "sink flush interval must be > 0");
        require(sinkMaxRetries >= 0, "sink max retries must be >= 0");
        require(checkpointIntervalMs >= 1000, "checkpoint interval must be >= 1000 ms");
        require(maxOutOfOrdernessMs >= 0, "max out-of-orderness must be >= 0");
        require(windowSizeSeconds > 0, "window size must be > 0");
        require(allowedLatenessSeconds >= 0, "allowed lateness must be >= 0");
        require(!kafkaBootstrapServers.isBlank(), "Kafka bootstrap servers must be set");
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException("Invalid job configuration: " + message);
        }
    }

    /** Builds a ClickHouse sink configuration for one target table. */
    public ClickHouseSinkConfig clickHouseSink(String table) {
        return ClickHouseSinkConfig.builder()
                .url(clickhouseUrl)
                .database(clickhouseDatabase)
                .table(table)
                .user(clickhouseUser)
                .password(clickhousePassword)
                .batchSize(sinkBatchSize)
                .flushIntervalMs(sinkFlushIntervalMs)
                .maxRetries(sinkMaxRetries)
                .retryBackoffMs(sinkRetryBackoffMs)
                .build();
    }

    public String getJobName() { return jobName; }
    public String getKafkaBootstrapServers() { return kafkaBootstrapServers; }
    public String getSourceTopic() { return sourceTopic; }
    public String getDlqTopic() { return dlqTopic; }
    public String getConsumerGroupId() { return consumerGroupId; }
    public String getClickhouseUrl() { return clickhouseUrl; }
    public String getClickhouseDatabase() { return clickhouseDatabase; }
    public String getClickhouseUser() { return clickhouseUser; }
    public int getSinkBatchSize() { return sinkBatchSize; }
    public long getSinkFlushIntervalMs() { return sinkFlushIntervalMs; }
    public long getCheckpointIntervalMs() { return checkpointIntervalMs; }
    public long getMaxOutOfOrdernessMs() { return maxOutOfOrdernessMs; }
    public long getSourceIdlenessMs() { return sourceIdlenessMs; }
    public long getWindowSizeSeconds() { return windowSizeSeconds; }
    public long getAllowedLatenessSeconds() { return allowedLatenessSeconds; }
    public long getMaxFutureSkewMs() { return maxFutureSkewMs; }

    @Override
    public String toString() {
        return "JobConfig{"
                + "jobName='" + jobName + '\''
                + ", kafkaBootstrapServers='" + kafkaBootstrapServers + '\''
                + ", sourceTopic='" + sourceTopic + '\''
                + ", dlqTopic='" + dlqTopic + '\''
                + ", consumerGroupId='" + consumerGroupId + '\''
                + ", clickhouseUrl='" + clickhouseUrl + '\''
                + ", clickhouseDatabase='" + clickhouseDatabase + '\''
                + ", clickhouseUser='" + clickhouseUser + '\''
                + ", clickhousePassword='****'"
                + ", sinkBatchSize=" + sinkBatchSize
                + ", sinkFlushIntervalMs=" + sinkFlushIntervalMs
                + ", checkpointIntervalMs=" + checkpointIntervalMs
                + ", maxOutOfOrdernessMs=" + maxOutOfOrdernessMs
                + ", windowSizeSeconds=" + windowSizeSeconds
                + ", allowedLatenessSeconds=" + allowedLatenessSeconds
                + '}';
    }

    /** Resolves a setting from program arguments first, then environment variables, then a default. */
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
