package com.example.clickstream.sink;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClickHouseSinkConfigTest {

    private static ClickHouseSinkConfig.Builder base() {
        return ClickHouseSinkConfig.builder().url("http://clickhouse:8123").database("analytics").table("events");
    }

    @Test
    void buildsEncodedInsertUri() {
        String uri = base().build().insertUri().toString();
        assertEquals("http://clickhouse:8123/?query=INSERT%20INTO%20analytics.events%20FORMAT%20JSONEachRow"
                + "&date_time_input_format=best_effort", uri);
    }

    @Test
    void rejectsUnsafeIdentifiers() {
        assertThrows(IllegalArgumentException.class, () -> base().table("events; DROP TABLE x").build());
    }

    @Test
    void rejectsInvalidBatchSize() {
        assertThrows(IllegalArgumentException.class, () -> base().batchSize(0).build());
    }

    @Test
    void retryableStatusCodes() {
        assertTrue(new ClickHouseWriteException(503, "").isRetryable());
        assertTrue(new ClickHouseWriteException(429, "").isRetryable());
        assertFalse(new ClickHouseWriteException(400, "").isRetryable());
    }
}
