package com.example.clickstream.sink;

import com.example.clickstream.model.ClickEvent;
import com.example.clickstream.model.PageMetric;
import com.example.clickstream.util.Json;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClickEventRowMapperTest {

    private static final long T0 = Instant.parse("2026-11-10T12:00:00.123Z").toEpochMilli();

    @Test
    void mapsClickEventToClickHouseRow() throws Exception {
        ClickEvent e = new ClickEvent();
        e.eventId = "7f3c1a52-3c2b-4b7e-9a51-0c9f0e1d2a3b";
        e.eventType = "page_view";
        e.userId = "u1";
        e.sessionId = "s1";
        e.pageUrl = "/";
        e.referrer = "direct";
        e.deviceType = "desktop";
        e.country = "DE";
        e.userAgent = "ua";
        e.eventTime = T0;
        e.ingestedAt = T0 + 5;

        ClickEventRowMapper mapper = new ClickEventRowMapper() {
            @Override
            protected long now() {
                return T0 + 10;
            }
        };
        JsonNode row = Json.MAPPER.readTree(mapper.toJsonRow(e));

        assertEquals("page_view", row.get("event_type").asText());
        assertTrue(row.get("product_id").isNull());
        assertTrue(row.get("price").isNull());
        assertTrue(row.get("quantity").isNull());
        assertEquals("2026-11-10 12:00:00.123", row.get("event_time").asText());
        assertEquals("2026-11-10 12:00:00.128", row.get("ingested_at").asText());
        assertEquals("2026-11-10 12:00:00.133", row.get("processed_at").asText());
    }

    @Test
    void mapsPageMetricToClickHouseRow() throws Exception {
        PageMetric m = new PageMetric();
        m.windowStart = Instant.parse("2026-11-10T12:00:00Z").toEpochMilli();
        m.windowEnd = Instant.parse("2026-11-10T12:01:00Z").toEpochMilli();
        m.pageUrl = "/checkout";
        m.eventType = "purchase";
        m.eventCount = 3;
        m.uniqueUsers = 2;
        m.uniqueSessions = 3;
        m.revenue = 26.5;
        m.version = 3;
        m.updatedAt = T0;

        JsonNode row = Json.MAPPER.readTree(new PageMetricRowMapper().toJsonRow(m));

        assertEquals("2026-11-10 12:00:00", row.get("window_start").asText());
        assertEquals("2026-11-10 12:01:00", row.get("window_end").asText());
        assertEquals(3, row.get("event_count").asLong());
        assertEquals(26.5, row.get("revenue").asDouble());
        assertEquals(3, row.get("version").asLong());
    }
}
