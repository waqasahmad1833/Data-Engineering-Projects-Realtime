package com.example.adtech.processing;

import com.example.adtech.model.AdEvent;
import com.example.adtech.model.AttributedClick;
import com.example.adtech.model.CampaignSpend;
import com.example.adtech.model.EventType;
import com.example.adtech.util.Json;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProcessingFunctionsTest {

    private static AdEvent event(EventType type, long time, long winMicros) {
        AdEvent e = new AdEvent();
        e.eventType = type.wireName();
        e.eventTime = time;
        e.impressionId = "imp-1";
        e.clickId = type == EventType.CLICK ? "click-1" : null;
        e.requestId = "req-1";
        e.userId = 42;
        e.advertiserId = 7;
        e.campaignId = 57;
        e.winPriceMicros = winMicros;
        e.siteDomain = "news1.example.com";
        return e;
    }

    @Test
    void attributionComputesTimeToClick() {
        AttributedClick a = ClickAttributionJoinFunction.attribute(
                event(EventType.IMPRESSION, 1_000, 9000), event(EventType.CLICK, 6_500, 0));
        assertEquals("click-1", a.clickId);
        assertEquals("imp-1", a.impressionId);
        assertEquals(5_500, a.timeToClickMs);
        assertEquals(9000, a.impressionCostMicros);
        assertEquals(57, a.campaignId);
    }

    @Test
    void spendAggregatorSumsMicrosExactly() {
        CampaignSpendAggregator agg = new CampaignSpendAggregator();
        SpendAccumulator acc = agg.createAccumulator();
        for (int i = 0; i < 1000; i++) {
            acc = agg.add(event(EventType.IMPRESSION, i, 3333), acc);
        }
        SpendAccumulator other = agg.add(event(EventType.IMPRESSION, 0, 1), agg.createAccumulator());
        CampaignSpend result = agg.getResult(agg.merge(acc, other));
        assertEquals(1001, result.impressions);
        assertEquals(3_333_001, result.spendMicros);
        assertEquals(7, result.advertiserId);
    }

    @Test
    void eventTypeFilter() {
        EventTypeFilter clicks = new EventTypeFilter(EventType.CLICK);
        assertTrue(clicks.filter(event(EventType.CLICK, 0, 0)));
        assertFalse(clicks.filter(event(EventType.IMPRESSION, 0, 0)));
    }

    @Test
    void deadLetterFormat() throws Exception {
        JsonNode n = Json.MAPPER.readTree(DeadLetters.toJson("{bad", "malformed_json", "boom", 0L));
        assertEquals("malformed_json", n.get("error_type").asText());
        assertEquals("{bad", n.get("raw_payload").asText());
        assertEquals("1970-01-01 00:00:00.000", n.get("failed_at").asText());
    }
}
