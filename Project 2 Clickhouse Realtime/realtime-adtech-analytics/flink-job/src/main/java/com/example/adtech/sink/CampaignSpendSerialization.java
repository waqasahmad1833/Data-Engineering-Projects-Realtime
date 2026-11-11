package com.example.adtech.sink;

import com.example.adtech.model.CampaignSpend;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/** Key/value serializers for the compacted campaign.spend topic. */
public final class CampaignSpendSerialization {

    private CampaignSpendSerialization() {
    }

    /** Key = campaign + window, so compaction keeps the newest version of each window. */
    public static final class Key implements SerializationSchema<CampaignSpend> {
        private static final long serialVersionUID = 1L;

        @Override
        public byte[] serialize(CampaignSpend spend) {
            return (spend.campaignId + ":" + spend.windowStart).getBytes(StandardCharsets.UTF_8);
        }
    }

    public static final class Value implements SerializationSchema<CampaignSpend> {
        private static final long serialVersionUID = 1L;
        private final CampaignSpendRowMapper mapper = new CampaignSpendRowMapper();

        @Override
        public byte[] serialize(CampaignSpend spend) {
            try {
                return mapper.toJsonRow(spend).getBytes(StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
