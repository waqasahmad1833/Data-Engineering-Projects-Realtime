package com.example.adtech.model;

import java.io.Serializable;

/** A click joined with the impression it belongs to. */
public class AttributedClick implements Serializable {

    private static final long serialVersionUID = 1L;

    public String clickId;
    public String impressionId;
    public String requestId;
    public long userId;
    public int advertiserId;
    public int campaignId;
    public int creativeId;
    public int publisherId;
    public String siteDomain;
    public String deviceType;
    public String country;
    public long impressionTime;
    public long clickTime;
    public long timeToClickMs;
    public long impressionCostMicros;

    public AttributedClick() {
    }
}
