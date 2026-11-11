package com.example.clickstream.sink;

import com.example.clickstream.model.PageMetric;
import com.example.clickstream.util.Json;
import com.example.clickstream.util.TimeUtils;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

/** Maps {@link PageMetric} to a row of analytics.page_metrics_1m. */
public class PageMetricRowMapper implements ClickHouseRowMapper<PageMetric> {

    private static final long serialVersionUID = 1L;

    @Override
    public String toJsonRow(PageMetric m) throws IOException {
        ObjectNode row = Json.MAPPER.createObjectNode();
        row.put("window_start", TimeUtils.toDateTime(m.windowStart));
        row.put("window_end", TimeUtils.toDateTime(m.windowEnd));
        row.put("page_url", m.pageUrl);
        row.put("event_type", m.eventType);
        row.put("event_count", m.eventCount);
        row.put("unique_users", m.uniqueUsers);
        row.put("unique_sessions", m.uniqueSessions);
        row.put("revenue", m.revenue);
        row.put("version", m.version);
        row.put("updated_at", TimeUtils.toDateTime64(m.updatedAt));
        return Json.MAPPER.writeValueAsString(row);
    }
}
