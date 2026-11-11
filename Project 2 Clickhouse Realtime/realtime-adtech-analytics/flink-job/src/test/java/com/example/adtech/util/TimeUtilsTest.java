package com.example.adtech.util;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TimeUtilsTest {

    @Test
    void formatsForClickHouse() {
        long t = Instant.parse("2026-11-10T09:05:07.042Z").toEpochMilli();
        assertEquals("2026-11-10 09:05:07.042", TimeUtils.toDateTime64(t));
        assertEquals("2026-11-10 09:05:07", TimeUtils.toDateTime(t));
    }
}
