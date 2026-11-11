package com.example.clickstream.model;

import java.io.Serializable;

/** Aggregated metrics for one (page, event type) pair in one event-time window. */
public class PageMetric implements Serializable {

    private static final long serialVersionUID = 1L;

    public long windowStart;
    public long windowEnd;
    public String pageUrl;
    public String eventType;
    public long eventCount;
    public long uniqueUsers;
    public long uniqueSessions;
    public double revenue;
    /** 1 for the on-time firing, +1 for every late re-firing; the ReplacingMergeTree version. */
    public long version;
    /** Wall-clock time this version was emitted (informational). */
    public long updatedAt;

    public PageMetric() {
    }

    @Override
    public String toString() {
        return "PageMetric{windowStart=" + windowStart + ", pageUrl='" + pageUrl + "', eventType='" + eventType
                + "', eventCount=" + eventCount + ", uniqueUsers=" + uniqueUsers
                + ", uniqueSessions=" + uniqueSessions + ", revenue=" + revenue + ", version=" + version + '}';
    }
}
