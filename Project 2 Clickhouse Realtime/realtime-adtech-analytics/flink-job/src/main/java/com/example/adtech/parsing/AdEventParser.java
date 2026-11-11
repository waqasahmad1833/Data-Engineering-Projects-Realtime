package com.example.adtech.parsing;

import com.example.adtech.model.AdEvent;
import com.example.adtech.model.EventType;
import com.example.adtech.util.Json;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.UUID;

/** Parses and validates raw JSON ad events. Pure function of (payload, now): easy to unit test. */
public class AdEventParser implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String ERR_MALFORMED_JSON = "malformed_json";
    public static final String ERR_MISSING_FIELD = "missing_field";
    public static final String ERR_INVALID_FIELD = "invalid_field";
    public static final String ERR_INVALID_EVENT_TYPE = "invalid_event_type";
    public static final String ERR_INVALID_TIMESTAMP = "invalid_timestamp";
    public static final String ERR_BUSINESS_RULE = "business_rule_violation";

    /** Sanity cap: $1,000 for a single impression. */
    static final long MAX_PRICE_MICROS = 1_000_000_000L;

    private final long maxFutureSkewMs;
    private final long maxEventAgeMs;

    public AdEventParser(long maxFutureSkewMs, long maxEventAgeMs) {
        this.maxFutureSkewMs = maxFutureSkewMs;
        this.maxEventAgeMs = maxEventAgeMs;
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

    private AdEvent toEvent(JsonNode root, long now) throws ValidationException {
        AdEvent e = new AdEvent();
        String rawType = requiredText(root, "event_type");
        EventType type = EventType.fromWireName(rawType).orElseThrow(
                () -> new ValidationException(ERR_INVALID_EVENT_TYPE, "Unknown event_type: " + rawType));
        e.eventType = type.wireName();
        e.eventId = uuid(requiredText(root, "event_id"), "event_id");
        e.eventTime = eventTime(requiredText(root, "event_time"), now);
        e.requestId = uuid(requiredText(root, "request_id"), "request_id");
        e.impressionId = optionalUuid(root, "impression_id");
        e.clickId = optionalUuid(root, "click_id");
        e.userId = requiredLong(root, "user_id");
        e.advertiserId = requiredId(root, "advertiser_id");
        e.campaignId = requiredId(root, "campaign_id");
        e.creativeId = requiredId(root, "creative_id");
        e.publisherId = requiredId(root, "publisher_id");
        e.siteDomain = FieldNormalizer.domain(optionalText(root, "site_domain"));
        e.adFormat = FieldNormalizer.adFormat(optionalText(root, "ad_format"));
        e.deviceType = FieldNormalizer.deviceType(optionalText(root, "device_type"));
        e.os = FieldNormalizer.os(optionalText(root, "os"));
        e.country = FieldNormalizer.country(optionalText(root, "country"));
        e.bidPriceMicros = optionalMicros(root, "bid_price_micros");
        e.winPriceMicros = optionalMicros(root, "win_price_micros");
        e.conversionValueMicros = optionalMicros(root, "conversion_value_micros");
        e.ingestedAt = now;
        applyBusinessRules(type, e);
        return e;
    }

    private static void applyBusinessRules(EventType type, AdEvent e) throws ValidationException {
        switch (type) {
            case BID:
                if (e.bidPriceMicros <= 0) {
                    throw new ValidationException(ERR_BUSINESS_RULE, "bid requires bid_price_micros > 0");
                }
                break;
            case IMPRESSION:
                if (e.impressionId == null) {
                    throw new ValidationException(ERR_BUSINESS_RULE, "impression requires impression_id");
                }
                if (e.winPriceMicros <= 0) {
                    throw new ValidationException(ERR_BUSINESS_RULE, "impression requires win_price_micros > 0");
                }
                if (e.bidPriceMicros > 0 && e.winPriceMicros > e.bidPriceMicros) {
                    throw new ValidationException(ERR_BUSINESS_RULE, "win price exceeds bid price");
                }
                break;
            case CLICK:
                if (e.impressionId == null || e.clickId == null) {
                    throw new ValidationException(ERR_BUSINESS_RULE, "click requires impression_id and click_id");
                }
                break;
            case CONVERSION:
                if (e.clickId == null) {
                    throw new ValidationException(ERR_BUSINESS_RULE, "conversion requires click_id");
                }
                break;
            default:
                throw new IllegalStateException("Unhandled event type " + type);
        }
    }

    private long eventTime(String value, long now) throws ValidationException {
        final long millis;
        try {
            millis = Instant.parse(value).toEpochMilli();
        } catch (DateTimeParseException e) {
            throw new ValidationException(ERR_INVALID_TIMESTAMP, "event_time is not ISO-8601: " + value);
        }
        if (millis > now + maxFutureSkewMs) {
            throw new ValidationException(ERR_INVALID_TIMESTAMP, "event_time too far in the future: " + value);
        }
        if (millis < now - maxEventAgeMs) {
            throw new ValidationException(ERR_INVALID_TIMESTAMP, "event_time older than retention window: " + value);
        }
        return millis;
    }

    private static String uuid(String value, String field) throws ValidationException {
        try {
            if (value.length() != 36) {
                throw new IllegalArgumentException();
            }
            return UUID.fromString(value).toString();
        } catch (IllegalArgumentException e) {
            throw new ValidationException(ERR_INVALID_FIELD, field + " is not a UUID: " + value);
        }
    }

    private static String optionalUuid(JsonNode root, String field) throws ValidationException {
        String value = optionalText(root, field);
        return value == null ? null : uuid(value, field);
    }

    private static String requiredText(JsonNode root, String field) throws ValidationException {
        JsonNode node = root.get(field);
        if (node == null || node.isNull()) {
            throw new ValidationException(ERR_MISSING_FIELD, field + " is required");
        }
        if (!node.isTextual() || node.asText().isBlank()) {
            throw new ValidationException(ERR_INVALID_FIELD, field + " must be a non-empty string");
        }
        return node.asText().trim();
    }

    private static String optionalText(JsonNode root, String field) {
        JsonNode node = root.get(field);
        if (node == null || node.isNull()) {
            return null;
        }
        String value = node.asText().trim();
        return value.isEmpty() ? null : value;
    }

    private static long requiredLong(JsonNode root, String field) throws ValidationException {
        JsonNode node = root.get(field);
        if (node == null || node.isNull()) {
            throw new ValidationException(ERR_MISSING_FIELD, field + " is required");
        }
        if (!node.isIntegralNumber() || !node.canConvertToLong() || node.asLong() < 0) {
            throw new ValidationException(ERR_INVALID_FIELD, field + " must be a non-negative 63-bit integer");
        }
        return node.asLong();
    }

    private static int requiredId(JsonNode root, String field) throws ValidationException {
        JsonNode node = root.get(field);
        if (node == null || node.isNull()) {
            throw new ValidationException(ERR_MISSING_FIELD, field + " is required");
        }
        if (!node.isIntegralNumber() || !node.canConvertToInt() || node.asInt() <= 0) {
            throw new ValidationException(ERR_INVALID_FIELD, field + " must be a positive integer");
        }
        return node.asInt();
    }

    private static long optionalMicros(JsonNode root, String field) throws ValidationException {
        JsonNode node = root.get(field);
        if (node == null || node.isNull()) {
            return 0L;
        }
        if (!node.isIntegralNumber() || !node.canConvertToLong()) {
            throw new ValidationException(ERR_INVALID_FIELD, field + " must be an integer amount in micros");
        }
        long value = node.asLong();
        if (value < 0 || value > MAX_PRICE_MICROS) {
            throw new ValidationException(ERR_INVALID_FIELD, field + " out of range: " + value);
        }
        return value;
    }
}
