package com.example.clickstream.parsing;

import com.example.clickstream.model.ClickEvent;
import com.example.clickstream.model.EventType;
import com.example.clickstream.util.Json;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.UUID;

/**
 * Parses and validates raw JSON events. Pure and side-effect free so it can be unit tested
 * without a Flink runtime.
 */
public class ClickEventParser implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String ERR_MALFORMED_JSON = "malformed_json";
    public static final String ERR_MISSING_FIELD = "missing_field";
    public static final String ERR_INVALID_FIELD = "invalid_field";
    public static final String ERR_INVALID_EVENT_TYPE = "invalid_event_type";
    public static final String ERR_INVALID_TIMESTAMP = "invalid_timestamp";
    public static final String ERR_BUSINESS_RULE = "business_rule_violation";

    private final long maxFutureSkewMs;

    public ClickEventParser(long maxFutureSkewMs) {
        this.maxFutureSkewMs = maxFutureSkewMs;
    }

    public ParseResult parse(String raw, long nowMillis) {
        if (raw == null || raw.isBlank()) {
            return ParseResult.failure(ERR_MALFORMED_JSON, "Empty payload");
        }
        final JsonNode root;
        try {
            root = Json.MAPPER.readTree(raw);
        } catch (JsonProcessingException e) {
            return ParseResult.failure(ERR_MALFORMED_JSON, e.getOriginalMessage());
        }
        if (root == null || !root.isObject()) {
            return ParseResult.failure(ERR_MALFORMED_JSON, "Payload is not a JSON object");
        }
        try {
            return ParseResult.success(toEvent(root, nowMillis));
        } catch (ValidationException e) {
            return ParseResult.failure(e.getErrorType(), e.getMessage());
        }
    }

    private ClickEvent toEvent(JsonNode root, long nowMillis) throws ValidationException {
        ClickEvent event = new ClickEvent();
        event.eventId = parseUuid(requiredText(root, "event_id"));

        String rawType = requiredText(root, "event_type");
        EventType type = EventType.fromWireName(rawType).orElseThrow(
                () -> new ValidationException(ERR_INVALID_EVENT_TYPE, "Unknown event_type: " + rawType));
        event.eventType = type.wireName();

        event.userId = requiredText(root, "user_id");
        event.sessionId = requiredText(root, "session_id");
        event.pageUrl = requiredText(root, "page_url");
        event.referrer = optionalText(root, "referrer", "");
        event.userAgent = optionalText(root, "user_agent", "");
        event.country = optionalText(root, "country", "");
        event.productId = optionalText(root, "product_id", null);
        event.price = optionalDouble(root, "price");
        event.quantity = optionalInt(root, "quantity");
        event.eventTime = parseEventTime(requiredText(root, "event_time"), nowMillis);
        event.ingestedAt = nowMillis;

        applyBusinessRules(type, event);
        return event;
    }

    private static void applyBusinessRules(EventType type, ClickEvent event) throws ValidationException {
        if (event.price != null && event.price < 0) {
            throw new ValidationException(ERR_INVALID_FIELD, "price must be >= 0, got " + event.price);
        }
        if (event.quantity != null && event.quantity <= 0) {
            throw new ValidationException(ERR_INVALID_FIELD, "quantity must be > 0, got " + event.quantity);
        }
        if (type.requiresProduct() && event.productId == null) {
            throw new ValidationException(ERR_BUSINESS_RULE, type.wireName() + " requires product_id");
        }
        if (type == EventType.PURCHASE) {
            if (event.price == null) {
                throw new ValidationException(ERR_BUSINESS_RULE, "purchase requires price");
            }
            if (event.quantity == null) {
                event.quantity = 1;
            }
        }
    }

    private long parseEventTime(String value, long nowMillis) throws ValidationException {
        final long millis;
        try {
            millis = Instant.parse(value).toEpochMilli();
        } catch (DateTimeParseException e) {
            throw new ValidationException(ERR_INVALID_TIMESTAMP, "event_time is not ISO-8601: " + value);
        }
        if (millis > nowMillis + maxFutureSkewMs) {
            throw new ValidationException(ERR_INVALID_TIMESTAMP, "event_time is too far in the future: " + value);
        }
        return millis;
    }

    private static String parseUuid(String value) throws ValidationException {
        try {
            if (value.length() != 36) {
                throw new IllegalArgumentException();
            }
            return UUID.fromString(value).toString();
        } catch (IllegalArgumentException e) {
            throw new ValidationException(ERR_INVALID_FIELD, "event_id is not a UUID: " + value);
        }
    }

    private static String requiredText(JsonNode root, String field) throws ValidationException {
        JsonNode node = root.get(field);
        if (node == null || node.isNull()) {
            throw new ValidationException(ERR_MISSING_FIELD, field + " is required");
        }
        if (!node.isTextual()) {
            throw new ValidationException(ERR_INVALID_FIELD, field + " must be a string");
        }
        String value = node.asText().trim();
        if (value.isEmpty()) {
            throw new ValidationException(ERR_MISSING_FIELD, field + " must not be empty");
        }
        return value;
    }

    private static String optionalText(JsonNode root, String field, String defaultValue) {
        JsonNode node = root.get(field);
        if (node == null || node.isNull()) {
            return defaultValue;
        }
        String value = node.asText().trim();
        return value.isEmpty() ? defaultValue : value;
    }

    private static Double optionalDouble(JsonNode root, String field) throws ValidationException {
        JsonNode node = root.get(field);
        if (node == null || node.isNull()) {
            return null;
        }
        if (!node.isNumber()) {
            throw new ValidationException(ERR_INVALID_FIELD, field + " must be a number");
        }
        double value = node.asDouble();
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            throw new ValidationException(ERR_INVALID_FIELD, field + " must be finite");
        }
        return value;
    }

    private static Integer optionalInt(JsonNode root, String field) throws ValidationException {
        JsonNode node = root.get(field);
        if (node == null || node.isNull()) {
            return null;
        }
        if (!node.isIntegralNumber() || !node.canConvertToInt()) {
            throw new ValidationException(ERR_INVALID_FIELD, field + " must be an integer");
        }
        return node.asInt();
    }
}
