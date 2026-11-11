package com.example.adtech.parsing;

import com.example.adtech.model.AdEvent;
import com.example.adtech.util.Json;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AdEventParserTest {

    private static final long NOW = Instant.parse("2026-11-10T12:00:00Z").toEpochMilli();
    private final AdEventParser parser =
            new AdEventParser(Duration.ofMinutes(5).toMillis(), Duration.ofDays(7).toMillis());

    static ObjectNode impression() {
        ObjectNode n = Json.MAPPER.createObjectNode();
        n.put("event_id", "0b8f5f1e-7d3a-4c35-9a8e-1f2b3c4d5e6f");
        n.put("event_type", "impression");
        n.put("event_time", "2026-11-10T11:59:59.250Z");
        n.put("request_id", "6e1c2a10-1111-4a2b-8c3d-9e8f7a6b5c4d");
        n.put("impression_id", "7a1c2a10-2222-4a2b-8c3d-9e8f7a6b5c4d");
        n.put("user_id", 4611686018427387904L);
        n.put("advertiser_id", 7);
        n.put("campaign_id", 57);
        n.put("creative_id", 571);
        n.put("publisher_id", 12);
        n.put("site_domain", "https://www.News12.example.com/article");
        n.put("ad_format", "Video");
        n.put("device_type", "MOBILE");
        n.put("os", "iOS");
        n.put("country", "de");
        n.put("bid_price_micros", 12000);
        n.put("win_price_micros", 9000);
        return n;
    }

    private ParseResult parse(ObjectNode n) {
        return parser.parse(n.toString(), NOW);
    }

    @Test
    void parsesAndNormalisesImpression() {
        ParseResult r = parse(impression());
        assertTrue(r.isValid());
        AdEvent e = r.getEvent();
        assertEquals("impression", e.eventType);
        assertEquals(4611686018427387904L, e.userId);
        assertEquals("news12.example.com", e.siteDomain);
        assertEquals("video", e.adFormat);
        assertEquals("mobile", e.deviceType);
        assertEquals("ios", e.os);
        assertEquals("DE", e.country);
        assertEquals(9000L, e.winPriceMicros);
        assertNull(e.clickId);
        assertEquals(NOW, e.ingestedAt);
    }

    @Test
    void parsesBidWithoutImpressionId() {
        ObjectNode n = impression();
        n.put("event_type", "bid");
        n.remove("impression_id");
        n.remove("win_price_micros");
        assertTrue(parse(n).isValid());
    }

    @Test
    void rejectsMalformedJson() {
        assertEquals(AdEventParser.ERR_MALFORMED_JSON, parser.parse("{\"event_id\":", NOW).getErrorType());
        assertEquals(AdEventParser.ERR_MALFORMED_JSON, parser.parse("[]", NOW).getErrorType());
    }

    @Test
    void rejectsUnknownEventType() {
        ObjectNode n = impression();
        n.put("event_type", "teleport");
        assertEquals(AdEventParser.ERR_INVALID_EVENT_TYPE, parse(n).getErrorType());
    }

    @Test
    void rejectsMissingEventId() {
        ObjectNode n = impression();
        n.remove("event_id");
        assertEquals(AdEventParser.ERR_MISSING_FIELD, parse(n).getErrorType());
    }

    @Test
    void rejectsImpressionWithoutImpressionId() {
        ObjectNode n = impression();
        n.remove("impression_id");
        assertEquals(AdEventParser.ERR_BUSINESS_RULE, parse(n).getErrorType());
    }

    @Test
    void rejectsWinAboveBid() {
        ObjectNode n = impression();
        n.put("win_price_micros", 13000);
        assertEquals(AdEventParser.ERR_BUSINESS_RULE, parse(n).getErrorType());
    }

    @Test
    void rejectsNegativeAndAbsurdPrices() {
        ObjectNode negative = impression();
        negative.put("bid_price_micros", -100);
        assertEquals(AdEventParser.ERR_INVALID_FIELD, parse(negative).getErrorType());

        ObjectNode absurd = impression();
        absurd.put("bid_price_micros", AdEventParser.MAX_PRICE_MICROS + 1);
        assertEquals(AdEventParser.ERR_INVALID_FIELD, parse(absurd).getErrorType());
    }

    @Test
    void rejectsClickWithoutClickId() {
        ObjectNode n = impression();
        n.put("event_type", "click");
        assertEquals(AdEventParser.ERR_BUSINESS_RULE, parse(n).getErrorType());
    }

    @Test
    void rejectsNegativeUserIdAndBadIds() {
        ObjectNode n = impression();
        n.put("user_id", -5);
        assertEquals(AdEventParser.ERR_INVALID_FIELD, parse(n).getErrorType());

        ObjectNode zero = impression();
        zero.put("campaign_id", 0);
        assertEquals(AdEventParser.ERR_INVALID_FIELD, parse(zero).getErrorType());
    }

    @Test
    void rejectsTimestampsOutsideTheAcceptedRange() {
        ObjectNode future = impression();
        future.put("event_time", "2026-11-10T12:30:00Z");
        assertEquals(AdEventParser.ERR_INVALID_TIMESTAMP, parse(future).getErrorType());

        ObjectNode ancient = impression();
        ancient.put("event_time", "2026-10-01T00:00:00Z");
        assertEquals(AdEventParser.ERR_INVALID_TIMESTAMP, parse(ancient).getErrorType());

        ObjectNode garbage = impression();
        garbage.put("event_time", "noon");
        assertEquals(AdEventParser.ERR_INVALID_TIMESTAMP, parse(garbage).getErrorType());
    }

    @Test
    void rejectsInvalidUuid() {
        ObjectNode n = impression();
        n.put("request_id", "abc");
        ParseResult r = parse(n);
        assertFalse(r.isValid());
        assertEquals(AdEventParser.ERR_INVALID_FIELD, r.getErrorType());
    }
}
