package com.example.adtech.processing;

import com.example.adtech.util.Json;
import com.example.adtech.util.TimeUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Dead-letter record format; matches adtech.dead_letters_queue in ClickHouse. */
public final class DeadLetters {

    static final int MAX_RAW_PAYLOAD_CHARS = 10_000;

    private DeadLetters() {
    }

    public static String toJson(String rawPayload, String errorType, String errorMessage, long failedAtMillis) {
        ObjectNode node = Json.MAPPER.createObjectNode();
        node.put("error_type", errorType);
        node.put("error_message", errorMessage == null ? "" : errorMessage);
        String raw = rawPayload == null ? "" : rawPayload;
        node.put("raw_payload", raw.length() <= MAX_RAW_PAYLOAD_CHARS ? raw : raw.substring(0, MAX_RAW_PAYLOAD_CHARS));
        node.put("failed_at", TimeUtils.toDateTime64(failedAtMillis));
        try {
            return Json.MAPPER.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not serialise dead letter", e);
        }
    }
}
