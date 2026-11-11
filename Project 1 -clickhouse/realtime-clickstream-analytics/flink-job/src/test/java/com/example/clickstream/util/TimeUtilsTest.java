package com.example.clickstream.util;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TimeUtilsTest {

    @Test
    void formatsForClickHouse() {
        long millis = Instant.parse("2026-11-10T09:05:07.042Z").toEpochMilli();
        assertEquals("2026-11-10 09:05:07.042", TimeUtils.toDateTime64(millis));
        assertEquals("2026-11-10 09:05:07", TimeUtils.toDateTime(millis));
    }
}
