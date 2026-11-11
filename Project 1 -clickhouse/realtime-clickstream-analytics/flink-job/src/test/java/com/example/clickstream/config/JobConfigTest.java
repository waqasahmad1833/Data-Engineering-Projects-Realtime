package com.example.clickstream.config;

import com.example.clickstream.sink.ClickHouseSinkConfig;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JobConfigTest {

    @Test
    void usesDefaults() {
        JobConfig config = JobConfig.from(Map.of(), Map.of());
        assertEquals("clickstream.events", config.getSourceTopic());
        assertEquals(60L, config.getWindowSizeSeconds());
    }

    @Test
    void argumentsOverrideEnvironment() {
        JobConfig config = JobConfig.from(
                Map.of("kafka.source-topic", "from-args"),
                Map.of("KAFKA_SOURCE_TOPIC", "from-env", "KAFKA_DLQ_TOPIC", "dlq-env"));
        assertEquals("from-args", config.getSourceTopic());
        assertEquals("dlq-env", config.getDlqTopic());
    }

    @Test
    void buildsSinkConfigForTable() {
        JobConfig config = JobConfig.from(Map.of(), Map.of(
                "CLICKHOUSE_USER", "analytics", "CLICKHOUSE_PASSWORD", "secret", "SINK_BATCH_SIZE", "100"));
        ClickHouseSinkConfig sink = config.clickHouseSink("page_metrics_1m");
        assertEquals("analytics.page_metrics_1m", sink.qualifiedTable());
        assertEquals("secret", sink.getPassword());
        assertEquals(100, sink.getBatchSize());
    }

    @Test
    void masksPasswordInToString() {
        JobConfig config = JobConfig.from(Map.of(), Map.of("CLICKHOUSE_PASSWORD", "super-secret"));
        assertFalse(config.toString().contains("super-secret"));
    }

    @Test
    void rejectsInvalidValues() {
        assertThrows(IllegalArgumentException.class,
                () -> JobConfig.from(Map.of(), Map.of("SINK_BATCH_SIZE", "0")));
    }
}
