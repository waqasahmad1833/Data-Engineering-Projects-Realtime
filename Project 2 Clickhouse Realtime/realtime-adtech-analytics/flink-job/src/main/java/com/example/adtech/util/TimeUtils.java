package com.example.adtech.util;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/** Formats epoch millis for ClickHouse DateTime / DateTime64 columns. */
public final class TimeUtils {

    private static final DateTimeFormatter DATE_TIME_64 =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter DATE_TIME =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneOffset.UTC);

    private TimeUtils() {
    }

    public static String toDateTime64(long epochMillis) {
        return DATE_TIME_64.format(Instant.ofEpochMilli(epochMillis));
    }

    public static String toDateTime(long epochMillis) {
        return DATE_TIME.format(Instant.ofEpochMilli(epochMillis));
    }
}
