package com.example.adtech.processing;

import com.example.adtech.model.CampaignSpend;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * Adds campaign id, window bounds and a version. Each firing of a window (on time, then once
 * per late impression) increments a per-window counter in Flink state, so ClickHouse
 * (ReplacingMergeTree(version)) and the compacted Kafka topic both keep the latest result.
 */
public class CampaignSpendWindowFunction
        extends ProcessWindowFunction<CampaignSpend, CampaignSpend, Integer, TimeWindow> {

    private static final long serialVersionUID = 1L;

    private final ValueStateDescriptor<Long> firings = new ValueStateDescriptor<>("window-firings", Types.LONG);

    @Override
    public void process(Integer campaignId, Context context, Iterable<CampaignSpend> results,
                        Collector<CampaignSpend> out) throws IOException {
        ValueState<Long> count = context.windowState().getState(firings);
        long version = count.value() == null ? 1L : count.value() + 1L;
        count.update(version);

        CampaignSpend spend = results.iterator().next();
        spend.campaignId = campaignId;
        spend.windowStart = context.window().getStart();
        spend.windowEnd = context.window().getEnd();
        spend.version = version;
        spend.updatedAt = System.currentTimeMillis();
        out.collect(spend);
    }

    @Override
    public void clear(Context context) {
        context.windowState().getState(firings).clear();
    }
}
