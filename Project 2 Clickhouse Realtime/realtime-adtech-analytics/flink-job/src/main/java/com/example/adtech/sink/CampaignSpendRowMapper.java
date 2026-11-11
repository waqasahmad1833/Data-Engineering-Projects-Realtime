package com.example.adtech.sink;

import com.example.adtech.model.CampaignSpend;
import com.example.adtech.util.Json;
import com.example.adtech.util.TimeUtils;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

/** Row for adtech.campaign_spend_10s_local, and the JSON value of the campaign.spend Kafka topic. */
public class CampaignSpendRowMapper implements ClickHouseRowMapper<CampaignSpend> {

    private static final long serialVersionUID = 1L;

    @Override
    public String toJsonRow(CampaignSpend s) throws IOException {
        ObjectNode row = Json.MAPPER.createObjectNode();
        row.put("window_start", TimeUtils.toDateTime(s.windowStart));
        row.put("window_end", TimeUtils.toDateTime(s.windowEnd));
        row.put("campaign_id", s.campaignId);
        row.put("advertiser_id", s.advertiserId);
        row.put("impressions", s.impressions);
        row.put("spend_micros", s.spendMicros);
        row.put("version", s.version);
        row.put("updated_at", TimeUtils.toDateTime64(s.updatedAt));
        return Json.MAPPER.writeValueAsString(row);
    }
}
