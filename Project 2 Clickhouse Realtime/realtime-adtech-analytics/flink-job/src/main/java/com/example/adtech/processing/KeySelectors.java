package com.example.adtech.processing;

import com.example.adtech.model.AdEvent;
import org.apache.flink.api.java.functions.KeySelector;

/** Named key selectors (explicit classes keep Flink's type extraction simple and stable). */
public final class KeySelectors {

    private KeySelectors() {
    }

    /** Deduplication key. */
    public static final class EventId implements KeySelector<AdEvent, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String getKey(AdEvent event) {
            return event.eventId;
        }
    }

    /** Join key between impressions and clicks. */
    public static final class ImpressionId implements KeySelector<AdEvent, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String getKey(AdEvent event) {
            return event.impressionId;
        }
    }

    /** Spend aggregation key. */
    public static final class CampaignId implements KeySelector<AdEvent, Integer> {
        private static final long serialVersionUID = 1L;

        @Override
        public Integer getKey(AdEvent event) {
            return event.campaignId;
        }
    }
}
