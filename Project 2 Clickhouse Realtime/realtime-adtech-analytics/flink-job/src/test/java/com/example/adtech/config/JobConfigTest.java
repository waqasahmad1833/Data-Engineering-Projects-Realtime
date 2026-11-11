package com.example.adtech.config;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JobConfigTest {

    @Test
    void readsClusterSpecFromEnvironment() {
        JobConfig cfg = JobConfig.from(Map.of(), Map.of(
                "CLICKHOUSE_SHARDS", "http://a:8123,http://b:8123;http://c:8123,http://d:8123",
                "CLICKHOUSE_PASSWORD", "secret",
                "SINK_WRITERS_PER_SHARD", "3"));
        assertEquals(2, cfg.getClickHouseCluster().numShards());
        assertEquals(3, cfg.getSinkWritersPerShard());
        assertEquals("ad_events_local", cfg.sinkConfig("ad_events_local").getTable());
        assertFalse(cfg.toString().contains("secret"));
    }

    @Test
    void argumentsWin() {
        JobConfig cfg = JobConfig.from(Map.of("dedup.ttl-ms", "1000"), Map.of("DEDUP_TTL_MS", "5"));
        assertEquals(1000, cfg.getDedupTtlMs());
    }

    @Test
    void rejectsInvalidSettings() {
        assertThrows(IllegalArgumentException.class,
                () -> JobConfig.from(Map.of(), Map.of("SINK_WRITERS_PER_SHARD", "0")));
        assertThrows(IllegalArgumentException.class,
                () -> JobConfig.from(Map.of(), Map.of("CLICKHOUSE_SHARDS", "not-a-url")));
    }
}
