package com.example.adtech.sink;

import com.example.adtech.model.AttributedClick;
import com.example.adtech.util.Json;
import com.example.adtech.util.TimeUtils;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

/** Row for adtech.attributed_clicks_local. */
public class AttributedClickRowMapper implements ClickHouseRowMapper<AttributedClick> {

    private static final long serialVersionUID = 1L;

    @Override
    public String toJsonRow(AttributedClick a) throws IOException {
        ObjectNode row = Json.MAPPER.createObjectNode();
        row.put("click_id", a.clickId);
        row.put("impression_id", a.impressionId);
        row.put("request_id", a.requestId);
        row.put("user_id", a.userId);
        row.put("advertiser_id", a.advertiserId);
        row.put("campaign_id", a.campaignId);
        row.put("creative_id", a.creativeId);
        row.put("publisher_id", a.publisherId);
        row.put("site_domain", a.siteDomain);
        row.put("device_type", a.deviceType);
        row.put("country", a.country);
        row.put("impression_time", TimeUtils.toDateTime64(a.impressionTime));
        row.put("click_time", TimeUtils.toDateTime64(a.clickTime));
        row.put("time_to_click_ms", a.timeToClickMs);
        row.put("impression_cost_micros", a.impressionCostMicros);
        row.put("processed_at", TimeUtils.toDateTime64(System.currentTimeMillis()));
        return Json.MAPPER.writeValueAsString(row);
    }
}
