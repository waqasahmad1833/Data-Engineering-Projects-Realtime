package com.example.clickstream.processing;

import com.example.clickstream.util.Json;
import com.example.clickstream.util.TimeUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Builds dead-letter records. The JSON shape matches analytics.dead_letters_queue in ClickHouse. */
public final class DeadLetters {

    static final int MAX_RAW_PAYLOAD_CHARS = 10_000;

    private DeadLetters() {
    }

    public static String toJson(String rawPayload, String errorType, String errorMessage,
                                String sourceTopic, long failedAtMillis) {
        ObjectNode node = Json.MAPPER.createObjectNode();
        node.put("error_type", errorType);
        node.put("error_message", errorMessage == null ? "" : errorMessage);
        node.put("source_topic", sourceTopic);
        node.put("raw_payload", truncate(rawPayload));
        node.put("failed_at", TimeUtils.toDateTime64(failedAtMillis));
        try {
            return Json.MAPPER.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not serialise dead letter", e);
        }
    }

    private static String truncate(String value) {
        if (value == null) {
            return "";
        }
        return value.length() <= MAX_RAW_PAYLOAD_CHARS ? value : value.substring(0, MAX_RAW_PAYLOAD_CHARS);
    }
}
