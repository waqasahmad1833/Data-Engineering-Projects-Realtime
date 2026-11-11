package com.example.clickstream.sink;

import com.example.clickstream.model.ClickEvent;
import com.example.clickstream.util.Json;
import com.example.clickstream.util.TimeUtils;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

/** Maps {@link ClickEvent} to a row of analytics.events. */
public class ClickEventRowMapper implements ClickHouseRowMapper<ClickEvent> {

    private static final long serialVersionUID = 1L;

    @Override
    public String toJsonRow(ClickEvent e) throws IOException {
        ObjectNode row = Json.MAPPER.createObjectNode();
        row.put("event_id", e.eventId);
        row.put("event_type", e.eventType);
        row.put("user_id", e.userId);
        row.put("session_id", e.sessionId);
        row.put("page_url", e.pageUrl);
        row.put("referrer", e.referrer);
        if (e.productId == null) {
            row.putNull("product_id");
        } else {
            row.put("product_id", e.productId);
        }
        if (e.price == null) {
            row.putNull("price");
        } else {
            row.put("price", e.price);
        }
        if (e.quantity == null) {
            row.putNull("quantity");
        } else {
            row.put("quantity", e.quantity);
        }
        row.put("device_type", e.deviceType);
        row.put("country", e.country);
        row.put("user_agent", e.userAgent);
        row.put("event_time", TimeUtils.toDateTime64(e.eventTime));
        row.put("ingested_at", TimeUtils.toDateTime64(e.ingestedAt));
        row.put("processed_at", TimeUtils.toDateTime64(now()));
        return Json.MAPPER.writeValueAsString(row);
    }

    /** Overridable for deterministic tests. */
    protected long now() {
        return System.currentTimeMillis();
    }
}
