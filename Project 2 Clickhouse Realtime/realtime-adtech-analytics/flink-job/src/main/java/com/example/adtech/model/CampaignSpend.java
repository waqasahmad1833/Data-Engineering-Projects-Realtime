package com.example.adtech.model;

import java.io.Serializable;

/** Spend of one campaign in one event-time window. */
public class CampaignSpend implements Serializable {

    private static final long serialVersionUID = 1L;

    public long windowStart;
    public long windowEnd;
    public int campaignId;
    public int advertiserId;
    public long impressions;
    public long spendMicros;
    /** 1 for the on-time firing, +1 for each late re-firing. */
    public long version;
    public long updatedAt;

    public CampaignSpend() {
    }
}
