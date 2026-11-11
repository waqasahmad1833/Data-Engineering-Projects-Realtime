package com.example.clickstream.processing;

import com.example.clickstream.model.PageMetric;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * Attaches the key, window bounds and a version to the pre-aggregated result.
 *
 * <p>With allowed lateness, a window fires once on time and again for every late event. Each
 * firing gets the next number from a per-window counter held in Flink window state, so ClickHouse
 * (ReplacingMergeTree(version)) always keeps the most complete result. Wall-clock timestamps are
 * not safe for this: two firings can happen in the same millisecond.
 */
public class PageMetricsWindowFunction
        extends ProcessWindowFunction<PageMetric, PageMetric, Tuple2<String, String>, TimeWindow> {

    private static final long serialVersionUID = 1L;

    private final ValueStateDescriptor<Long> fireCountDescriptor =
            new ValueStateDescriptor<>("window-fire-count", Types.LONG);

    @Override
    public void process(Tuple2<String, String> key, Context context, Iterable<PageMetric> aggregates,
                        Collector<PageMetric> out) throws IOException {
        ValueState<Long> fireCount = context.windowState().getState(fireCountDescriptor);
        long version = fireCount.value() == null ? 1L : fireCount.value() + 1L;
        fireCount.update(version);

        PageMetric metric = aggregates.iterator().next();
        metric.pageUrl = key.f0;
        metric.eventType = key.f1;
        metric.windowStart = context.window().getStart();
        metric.windowEnd = context.window().getEnd();
        metric.version = version;
        metric.updatedAt = System.currentTimeMillis();
        out.collect(metric);
    }

    @Override
    public void clear(Context context) {
        // Called when the window is purged (after window end + allowed lateness).
        context.windowState().getState(fireCountDescriptor).clear();
    }
}
