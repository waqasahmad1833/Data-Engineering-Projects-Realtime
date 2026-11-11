package com.example.adtech.parsing;

import java.util.Locale;
import java.util.Set;

/**
 * Normalises low-cardinality dimensions so ClickHouse LowCardinality dictionaries stay small
 * and dashboards never show "Mobile" and "mobile" as separate values.
 */
public final class FieldNormalizer {

    static final Set<String> DEVICE_TYPES = Set.of("mobile", "desktop", "tablet", "ctv");
    static final Set<String> AD_FORMATS = Set.of("banner", "native", "video");

    private FieldNormalizer() {
    }

    public static String country(String value) {
        if (value == null) {
            return "ZZ";
        }
        String v = value.trim().toUpperCase(Locale.ROOT);
        return v.matches("[A-Z]{2}") ? v : "ZZ";
    }

    public static String deviceType(String value) {
        String v = lower(value);
        return DEVICE_TYPES.contains(v) ? v : "unknown";
    }

    public static String adFormat(String value) {
        String v = lower(value);
        return AD_FORMATS.contains(v) ? v : "other";
    }

    public static String os(String value) {
        String v = lower(value);
        return v.isEmpty() ? "unknown" : v;
    }

    public static String domain(String value) {
        String v = lower(value);
        int scheme = v.indexOf("://");
        if (scheme >= 0) {
            v = v.substring(scheme + 3);
        }
        int slash = v.indexOf('/');
        if (slash >= 0) {
            v = v.substring(0, slash);
        }
        if (v.startsWith("www.")) {
            v = v.substring(4);
        }
        return v.isEmpty() ? "unknown" : v;
    }

    private static String lower(String value) {
        return value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
    }
}
