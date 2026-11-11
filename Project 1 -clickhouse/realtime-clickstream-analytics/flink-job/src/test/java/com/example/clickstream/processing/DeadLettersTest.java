package com.example.clickstream.processing;

import com.example.clickstream.util.Json;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DeadLettersTest {

    @Test
    void buildsDeadLetterJson() throws Exception {
        long failedAt = Instant.parse("2026-11-10T12:00:00.250Z").toEpochMilli();
        String json = DeadLetters.toJson("{bad", "malformed_json", "Unexpected end", "clickstream.events", failedAt);

        JsonNode node = Json.MAPPER.readTree(json);
        assertEquals("malformed_json", node.get("error_type").asText());
        assertEquals("Unexpected end", node.get("error_message").asText());
        assertEquals("clickstream.events", node.get("source_topic").asText());
        assertEquals("{bad", node.get("raw_payload").asText());
        assertEquals("2026-11-10 12:00:00.250", node.get("failed_at").asText());
    }

    @Test
    void truncatesHugePayloads() throws Exception {
        String huge = "x".repeat(DeadLetters.MAX_RAW_PAYLOAD_CHARS + 500);
        JsonNode node = Json.MAPPER.readTree(DeadLetters.toJson(huge, "malformed_json", null, "t", 0L));
        assertEquals(DeadLetters.MAX_RAW_PAYLOAD_CHARS, node.get("raw_payload").asText().length());
        assertEquals("", node.get("error_message").asText());
    }
}
