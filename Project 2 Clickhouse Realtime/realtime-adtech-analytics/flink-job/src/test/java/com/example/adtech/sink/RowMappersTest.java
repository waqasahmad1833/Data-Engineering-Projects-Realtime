package com.example.adtech.sink;

import com.example.adtech.model.AdEvent;
import com.example.adtech.model.CampaignSpend;
import com.example.adtech.util.Json;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class RowMappersTest {

    private static final long T = Instant.parse("2026-11-10T12:00:05.123Z").toEpochMilli();

    @Test
    void adEventRowOmitsAbsentIdsAndFormatsTimes() throws Exception {
        AdEvent e = new AdEvent();
        e.eventId = "0b8f5f1e-7d3a-4c35-9a8e-1f2b3c4d5e6f";
        e.eventType = "bid";
        e.eventTime = T;
        e.ingestedAt = T + 7;
        e.requestId = "6e1c2a10-1111-4a2b-8c3d-9e8f7a6b5c4d";
        e.userId = Long.MAX_VALUE;
        e.campaignId = 57;
        e.bidPriceMicros = 12000;

        JsonNode row = Json.MAPPER.readTree(new AdEventRowMapper().toJsonRow(e));
        assertEquals("2026-11-10 12:00:05.123", row.get("event_time").asText());
        assertEquals("2026-11-10 12:00:05.130", row.get("ingested_at").asText());
        assertFalse(row.has("impression_id"));
        assertFalse(row.has("click_id"));
        assertEquals(Long.MAX_VALUE, row.get("user_id").asLong());
        assertEquals(12000, row.get("bid_price_micros").asLong());
    }

    @Test
    void campaignSpendRowAndKafkaKey() throws Exception {
        CampaignSpend s = new CampaignSpend();
        s.windowStart = Instant.parse("2026-11-10T12:00:00Z").toEpochMilli();
        s.windowEnd = s.windowStart + 10_000;
        s.campaignId = 57;
        s.advertiserId = 7;
        s.impressions = 3;
        s.spendMicros = 27000;
        s.version = 2;
        s.updatedAt = T;

        JsonNode row = Json.MAPPER.readTree(new CampaignSpendRowMapper().toJsonRow(s));
        assertEquals("2026-11-10 12:00:00", row.get("window_start").asText());
        assertEquals("2026-11-10 12:00:10", row.get("window_end").asText());
        assertEquals(2, row.get("version").asLong());
        assertEquals("57:" + s.windowStart, new String(new CampaignSpendSerialization.Key().serialize(s)));
    }
}
