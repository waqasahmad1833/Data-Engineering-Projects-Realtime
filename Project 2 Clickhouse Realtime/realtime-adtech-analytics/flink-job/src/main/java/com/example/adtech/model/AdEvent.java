package com.example.adtech.model;

import java.io.Serializable;

/**
 * A validated, normalised ad event. Flink POJO (public fields, no-arg constructor) so it is
 * serialised with Flink's POJO serializer. Money is carried as integer micros to avoid
 * floating-point drift in spend aggregation.
 */
public class AdEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    public String eventId;
    public String eventType;
    /** Event time, epoch millis UTC. */
    public long eventTime;
    public String requestId;
    /** Present on impression, click and conversion events. */
    public String impressionId;
    /** Present on click and conversion events. */
    public String clickId;
    public long userId;
    public int advertiserId;
    public int campaignId;
    public int creativeId;
    public int publisherId;
    public String siteDomain;
    public String adFormat;
    public String deviceType;
    public String os;
    public String country;
    public long bidPriceMicros;
    public long winPriceMicros;
    public long conversionValueMicros;
    /** Time Flink parsed the event, epoch millis UTC. */
    public long ingestedAt;

    public AdEvent() {
    }

    public boolean is(EventType type) {
        return type.wireName().equals(eventType);
    }

    @Override
    public String toString() {
        return "AdEvent{" + eventType + ", eventId=" + eventId + ", campaignId=" + campaignId + ", userId=" + userId
                + ", eventTime=" + eventTime + '}';
    }
}
