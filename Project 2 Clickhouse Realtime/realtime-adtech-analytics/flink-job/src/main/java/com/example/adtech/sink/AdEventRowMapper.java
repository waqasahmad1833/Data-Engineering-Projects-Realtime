package com.example.adtech.sink;

import com.example.adtech.model.AdEvent;
import com.example.adtech.util.Json;
import com.example.adtech.util.TimeUtils;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

/** Row for adtech.ad_events_local. Absent IDs are omitted so ClickHouse stores its default (zero UUID). */
public class AdEventRowMapper implements ClickHouseRowMapper<AdEvent> {

    private static final long serialVersionUID = 1L;

    @Override
    public String toJsonRow(AdEvent e) throws IOException {
        ObjectNode row = Json.MAPPER.createObjectNode();
        row.put("event_time", TimeUtils.toDateTime64(e.eventTime));
        row.put("event_type", e.eventType);
        row.put("event_id", e.eventId);
        row.put("request_id", e.requestId);
        if (e.impressionId != null) {
            row.put("impression_id", e.impressionId);
        }
        if (e.clickId != null) {
            row.put("click_id", e.clickId);
        }
        row.put("user_id", e.userId);
        row.put("advertiser_id", e.advertiserId);
        row.put("campaign_id", e.campaignId);
        row.put("creative_id", e.creativeId);
        row.put("publisher_id", e.publisherId);
        row.put("site_domain", e.siteDomain);
        row.put("ad_format", e.adFormat);
        row.put("device_type", e.deviceType);
        row.put("os", e.os);
        row.put("country", e.country);
        row.put("bid_price_micros", e.bidPriceMicros);
        row.put("win_price_micros", e.winPriceMicros);
        row.put("conversion_value_micros", e.conversionValueMicros);
        row.put("ingested_at", TimeUtils.toDateTime64(e.ingestedAt));
        return Json.MAPPER.writeValueAsString(row);
    }
}
