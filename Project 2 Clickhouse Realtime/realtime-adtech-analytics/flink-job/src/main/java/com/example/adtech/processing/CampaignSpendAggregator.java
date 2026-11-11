package com.example.adtech.processing;

import com.example.adtech.model.AdEvent;
import com.example.adtech.model.CampaignSpend;
import org.apache.flink.api.common.functions.AggregateFunction;

/** Incremental spend aggregation: O(1) state per campaign and window. */
public class CampaignSpendAggregator implements AggregateFunction<AdEvent, SpendAccumulator, CampaignSpend> {

    private static final long serialVersionUID = 1L;

    @Override
    public SpendAccumulator createAccumulator() {
        return new SpendAccumulator();
    }

    @Override
    public SpendAccumulator add(AdEvent impression, SpendAccumulator acc) {
        acc.impressions++;
        acc.spendMicros += impression.winPriceMicros;
        acc.advertiserId = impression.advertiserId;
        return acc;
    }

    @Override
    public CampaignSpend getResult(SpendAccumulator acc) {
        CampaignSpend spend = new CampaignSpend();
        spend.impressions = acc.impressions;
        spend.spendMicros = acc.spendMicros;
        spend.advertiserId = acc.advertiserId;
        return spend;
    }

    @Override
    public SpendAccumulator merge(SpendAccumulator a, SpendAccumulator b) {
        a.impressions += b.impressions;
        a.spendMicros += b.spendMicros;
        if (a.advertiserId == 0) {
            a.advertiserId = b.advertiserId;
        }
        return a;
    }
}
