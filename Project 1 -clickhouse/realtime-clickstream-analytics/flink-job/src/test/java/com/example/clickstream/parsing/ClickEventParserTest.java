package com.example.clickstream.parsing;

import com.example.clickstream.model.ClickEvent;
import com.example.clickstream.util.Json;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClickEventParserTest {

    private static final long NOW = Instant.parse("2026-11-10T12:00:00Z").toEpochMilli();
    private final ClickEventParser parser = new ClickEventParser(Duration.ofMinutes(5).toMillis());

    private static ObjectNode purchase() {
        ObjectNode node = Json.MAPPER.createObjectNode();
        node.put("event_id", "7f3c1a52-3c2b-4b7e-9a51-0c9f0e1d2a3b");
        node.put("event_type", "purchase");
        node.put("user_id", "u_000001");
        node.put("session_id", "a6cecc1b-78e5-4061-b311-d8a3c2ce6f44");
        node.put("page_url", "/checkout/confirmation");
        node.put("referrer", "https://www.google.com/");
        node.put("product_id", "ele-0001");
        node.put("price", 99.99);
        node.put("quantity", 2);
        node.put("user_agent", "Mozilla/5.0 (iPhone)");
        node.put("country", "DE");
        node.put("event_time", "2026-11-10T11:59:58.123Z");
        return node;
    }

    private ParseResult parse(ObjectNode node) {
        return parser.parse(node.toString(), NOW);
    }

    @Test
    void parsesValidPurchase() {
        ParseResult result = parse(purchase());

        assertTrue(result.isValid());
        ClickEvent event = result.getEvent();
        assertEquals("purchase", event.eventType);
        assertEquals("ele-0001", event.productId);
        assertEquals(99.99, event.price);
        assertEquals(2, event.quantity);
        assertEquals(Instant.parse("2026-11-10T11:59:58.123Z").toEpochMilli(), event.eventTime);
        assertEquals(NOW, event.ingestedAt);
        assertEquals(199.98, event.revenue(), 1e-9);
    }

    @Test
    void defaultsPurchaseQuantityToOne() {
        ObjectNode node = purchase();
        node.remove("quantity");
        assertEquals(1, parse(node).getEvent().quantity);
    }

    @Test
    void acceptsPageViewWithoutOptionalFields() {
        ObjectNode node = purchase();
        node.put("event_type", "page_view");
        node.remove("product_id");
        node.remove("price");
        node.remove("quantity");
        node.remove("referrer");
        node.remove("user_agent");

        ParseResult result = parse(node);
        assertTrue(result.isValid());
        assertNull(result.getEvent().productId);
        assertEquals("", result.getEvent().referrer);
    }

    @Test
    void eventTypeIsCaseInsensitive() {
        ObjectNode node = purchase();
        node.put("event_type", " PURCHASE ");
        assertEquals("purchase", parse(node).getEvent().eventType);
    }

    @Test
    void rejectsMalformedJson() {
        ParseResult result = parser.parse("{\"event_id\": \"abc", NOW);
        assertFalse(result.isValid());
        assertEquals(ClickEventParser.ERR_MALFORMED_JSON, result.getErrorType());
    }

    @Test
    void rejectsNonObjectJson() {
        assertEquals(ClickEventParser.ERR_MALFORMED_JSON, parser.parse("[1,2,3]", NOW).getErrorType());
        assertEquals(ClickEventParser.ERR_MALFORMED_JSON, parser.parse("   ", NOW).getErrorType());
    }

    @Test
    void rejectsMissingEventId() {
        ObjectNode node = purchase();
        node.remove("event_id");
        assertEquals(ClickEventParser.ERR_MISSING_FIELD, parse(node).getErrorType());
    }

    @Test
    void rejectsInvalidUuid() {
        ObjectNode node = purchase();
        node.put("event_id", "not-a-uuid");
        assertEquals(ClickEventParser.ERR_INVALID_FIELD, parse(node).getErrorType());
    }

    @Test
    void rejectsUnknownEventType() {
        ObjectNode node = purchase();
        node.put("event_type", "teleport");
        assertEquals(ClickEventParser.ERR_INVALID_EVENT_TYPE, parse(node).getErrorType());
    }

    @Test
    void rejectsUnparseableTimestamp() {
        ObjectNode node = purchase();
        node.put("event_time", "yesterday at noon");
        assertEquals(ClickEventParser.ERR_INVALID_TIMESTAMP, parse(node).getErrorType());
    }

    @Test
    void rejectsEventsTooFarInTheFuture() {
        ObjectNode node = purchase();
        node.put("event_time", "2026-11-10T12:30:00Z");
        assertEquals(ClickEventParser.ERR_INVALID_TIMESTAMP, parse(node).getErrorType());
    }

    @Test
    void rejectsPurchaseWithoutPrice() {
        ObjectNode node = purchase();
        node.putNull("price");
        assertEquals(ClickEventParser.ERR_BUSINESS_RULE, parse(node).getErrorType());
    }

    @Test
    void rejectsProductEventWithoutProductId() {
        ObjectNode node = purchase();
        node.put("event_type", "add_to_cart");
        node.remove("product_id");
        assertEquals(ClickEventParser.ERR_BUSINESS_RULE, parse(node).getErrorType());
    }

    @Test
    void rejectsNegativePriceAndNonIntegerQuantity() {
        ObjectNode negative = purchase();
        negative.put("price", -1.0);
        assertEquals(ClickEventParser.ERR_INVALID_FIELD, parse(negative).getErrorType());

        ObjectNode fractional = purchase();
        fractional.put("quantity", 1.5);
        assertEquals(ClickEventParser.ERR_INVALID_FIELD, parse(fractional).getErrorType());
    }

    @Test
    void rejectsNonStringRequiredField() {
        ObjectNode node = purchase();
        node.put("user_id", 42);
        assertEquals(ClickEventParser.ERR_INVALID_FIELD, parse(node).getErrorType());
    }
}
