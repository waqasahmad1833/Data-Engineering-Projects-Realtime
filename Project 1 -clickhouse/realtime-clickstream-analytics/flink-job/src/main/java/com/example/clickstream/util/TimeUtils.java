package com.example.clickstream.util;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/** Formats epoch millis into the text formats ClickHouse parses for DateTime / DateTime64 columns. */
public final class TimeUtils {

    private static final DateTimeFormatter DATE_TIME_64 =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter DATE_TIME =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneOffset.UTC);

    private TimeUtils() {
    }

    /** e.g. {@code 2026-11-10 12:00:00.123} for a {@code DateTime64(3, 'UTC')} column. */
    public static String toDateTime64(long epochMillis) {
        return DATE_TIME_64.format(Instant.ofEpochMilli(epochMillis));
    }

    /** e.g. {@code 2026-11-10 12:00:00} for a {@code DateTime('UTC')} column. */
    public static String toDateTime(long epochMillis) {
        return DATE_TIME.format(Instant.ofEpochMilli(epochMillis));
    }
}
