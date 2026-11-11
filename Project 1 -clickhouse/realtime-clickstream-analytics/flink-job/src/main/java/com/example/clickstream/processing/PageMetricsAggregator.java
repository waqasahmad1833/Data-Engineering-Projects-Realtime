package com.example.clickstream.processing;

import com.example.clickstream.model.ClickEvent;
import com.example.clickstream.model.PageMetric;
import org.apache.flink.api.common.functions.AggregateFunction;

/** Incrementally aggregates events of one window so Flink never buffers the raw events. */
public class PageMetricsAggregator implements AggregateFunction<ClickEvent, PageMetricAccumulator, PageMetric> {

    private static final long serialVersionUID = 1L;

    @Override
    public PageMetricAccumulator createAccumulator() {
        return new PageMetricAccumulator();
    }

    @Override
    public PageMetricAccumulator add(ClickEvent event, PageMetricAccumulator acc) {
        acc.eventCount++;
        acc.userIds.put(event.userId, Boolean.TRUE);
        acc.sessionIds.put(event.sessionId, Boolean.TRUE);
        acc.revenue += event.revenue();
        return acc;
    }

    @Override
    public PageMetric getResult(PageMetricAccumulator acc) {
        PageMetric metric = new PageMetric();
        metric.eventCount = acc.eventCount;
        metric.uniqueUsers = acc.userIds.size();
        metric.uniqueSessions = acc.sessionIds.size();
        metric.revenue = Math.round(acc.revenue * 100.0) / 100.0;
        return metric;
    }

    @Override
    public PageMetricAccumulator merge(PageMetricAccumulator a, PageMetricAccumulator b) {
        a.eventCount += b.eventCount;
        a.userIds.putAll(b.userIds);
        a.sessionIds.putAll(b.sessionIds);
        a.revenue += b.revenue;
        return a;
    }
}
