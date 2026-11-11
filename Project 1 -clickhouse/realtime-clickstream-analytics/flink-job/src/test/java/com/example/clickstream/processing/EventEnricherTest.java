package com.example.clickstream.processing;

import com.example.clickstream.model.ClickEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EventEnricherTest {

    @ParameterizedTest
    @CsvSource({
            "/products/ELE-0001?ref=home, /products/ele-0001",
            "/category/books/, /category/books",
            "https://shop.example.com/Deals#top, /deals",
            "https://shop.example.com, /",
            "search, /search",
            "/, /",
    })
    void normalizesPageUrl(String input, String expected) {
        assertEquals(expected, EventEnricher.normalizePageUrl(input));
    }

    @ParameterizedTest
    @CsvSource({
            "https://www.google.com/, google.com",
            "https://newsletter.example.com/campaign?id=42, newsletter.example.com",
            "http://www.bing.com:443/search, bing.com",
            "facebook.com, facebook.com",
    })
    void normalizesReferrer(String input, String expected) {
        assertEquals(expected, EventEnricher.normalizeReferrer(input));
    }

    @Test
    void emptyReferrerIsDirect() {
        assertEquals("direct", EventEnricher.normalizeReferrer(""));
        assertEquals("direct", EventEnricher.normalizeReferrer(null));
    }

    @Test
    void normalizesCountry() {
        assertEquals("DE", EventEnricher.normalizeCountry(" de "));
        assertEquals("ZZ", EventEnricher.normalizeCountry("Germany"));
        assertEquals("ZZ", EventEnricher.normalizeCountry(null));
    }

    @Test
    void enrichesEvent() {
        ClickEvent event = new ClickEvent();
        event.userAgent = "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) Mobile/15E148";
        event.country = "gb";
        event.pageUrl = "/Checkout/";
        event.referrer = "https://www.instagram.com/";

        ClickEvent enriched = new EventEnricher().map(event);

        assertEquals("mobile", enriched.deviceType);
        assertEquals("GB", enriched.country);
        assertEquals("/checkout", enriched.pageUrl);
        assertEquals("instagram.com", enriched.referrer);
    }
}
