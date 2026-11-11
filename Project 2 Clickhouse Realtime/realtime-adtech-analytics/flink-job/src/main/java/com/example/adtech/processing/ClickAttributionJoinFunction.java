package com.example.adtech.processing;

import com.example.adtech.model.AdEvent;
import com.example.adtech.model.AttributedClick;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

/** Combines an impression and a click on the same impression_id into an attribution record. */
public class ClickAttributionJoinFunction extends ProcessJoinFunction<AdEvent, AdEvent, AttributedClick> {

    private static final long serialVersionUID = 1L;

    @Override
    public void processElement(AdEvent impression, AdEvent click, Context ctx, Collector<AttributedClick> out) {
        out.collect(attribute(impression, click));
    }

    static AttributedClick attribute(AdEvent impression, AdEvent click) {
        AttributedClick a = new AttributedClick();
        a.clickId = click.clickId;
        a.impressionId = impression.impressionId;
        a.requestId = impression.requestId;
        a.userId = impression.userId;
        a.advertiserId = impression.advertiserId;
        a.campaignId = impression.campaignId;
        a.creativeId = impression.creativeId;
        a.publisherId = impression.publisherId;
        a.siteDomain = impression.siteDomain;
        a.deviceType = impression.deviceType;
        a.country = impression.country;
        a.impressionTime = impression.eventTime;
        a.clickTime = click.eventTime;
        a.timeToClickMs = click.eventTime - impression.eventTime;
        a.impressionCostMicros = impression.winPriceMicros;
        return a;
    }
}
