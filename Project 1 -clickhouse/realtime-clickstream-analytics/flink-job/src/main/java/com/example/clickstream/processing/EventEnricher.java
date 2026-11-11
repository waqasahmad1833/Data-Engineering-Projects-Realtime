package com.example.clickstream.processing;

import com.example.clickstream.model.ClickEvent;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Locale;

/** Derives the device type and normalises page URL, referrer and country. */
public class EventEnricher implements MapFunction<ClickEvent, ClickEvent> {

    private static final long serialVersionUID = 1L;

    static final String UNKNOWN_COUNTRY = "ZZ";
    static final String DIRECT_REFERRER = "direct";

    @Override
    public ClickEvent map(ClickEvent event) {
        event.deviceType = DeviceClassifier.classify(event.userAgent);
        event.country = normalizeCountry(event.country);
        event.pageUrl = normalizePageUrl(event.pageUrl);
        event.referrer = normalizeReferrer(event.referrer);
        return event;
    }

    /** Two-letter upper-case ISO code, or {@code ZZ} when unknown. */
    static String normalizeCountry(String country) {
        if (country == null) {
            return UNKNOWN_COUNTRY;
        }
        String value = country.trim().toUpperCase(Locale.ROOT);
        return value.matches("[A-Z]{2}") ? value : UNKNOWN_COUNTRY;
    }

    /** Path only, lower case, without query string, fragment or trailing slash. */
    static String normalizePageUrl(String url) {
        if (url == null || url.isBlank()) {
            return "/";
        }
        String value = url.trim();
        int schemeEnd = value.indexOf("://");
        if (schemeEnd >= 0) {
            int pathStart = value.indexOf('/', schemeEnd + 3);
            value = pathStart >= 0 ? value.substring(pathStart) : "/";
        }
        int cut = indexOfAny(value, '?', '#');
        if (cut >= 0) {
            value = value.substring(0, cut);
        }
        value = value.toLowerCase(Locale.ROOT);
        if (!value.startsWith("/")) {
            value = "/" + value;
        }
        if (value.length() > 1 && value.endsWith("/")) {
            value = value.substring(0, value.length() - 1);
        }
        return value;
    }

    /** Referrer host without scheme, port or {@code www.}; {@code direct} when absent. */
    static String normalizeReferrer(String referrer) {
        if (referrer == null || referrer.isBlank()) {
            return DIRECT_REFERRER;
        }
        String value = referrer.trim().toLowerCase(Locale.ROOT);
        int schemeEnd = value.indexOf("://");
        if (schemeEnd >= 0) {
            value = value.substring(schemeEnd + 3);
        }
        int end = indexOfAny(value, '/', '?', '#', ':');
        if (end >= 0) {
            value = value.substring(0, end);
        }
        if (value.startsWith("www.")) {
            value = value.substring(4);
        }
        return value.isEmpty() ? DIRECT_REFERRER : value;
    }

    private static int indexOfAny(String value, char... chars) {
        int best = -1;
        for (char c : chars) {
            int idx = value.indexOf(c);
            if (idx >= 0 && (best < 0 || idx < best)) {
                best = idx;
            }
        }
        return best;
    }
}
