package com.example.clickstream.processing;

import java.util.Locale;

/** Lightweight user-agent classification (no external dependency). */
public final class DeviceClassifier {

    public static final String DESKTOP = "desktop";
    public static final String MOBILE = "mobile";
    public static final String TABLET = "tablet";
    public static final String BOT = "bot";
    public static final String UNKNOWN = "unknown";

    private DeviceClassifier() {
    }

    public static String classify(String userAgent) {
        if (userAgent == null || userAgent.isBlank()) {
            return UNKNOWN;
        }
        String ua = userAgent.toLowerCase(Locale.ROOT);
        if (containsAny(ua, "bot", "crawler", "spider", "slurp")) {
            return BOT;
        }
        if (containsAny(ua, "ipad", "tablet")) {
            return TABLET;
        }
        if (containsAny(ua, "mobi", "iphone", "android")) {
            return MOBILE;
        }
        return DESKTOP;
    }

    private static boolean containsAny(String value, String... needles) {
        for (String needle : needles) {
            if (value.contains(needle)) {
                return true;
            }
        }
        return false;
    }
}
