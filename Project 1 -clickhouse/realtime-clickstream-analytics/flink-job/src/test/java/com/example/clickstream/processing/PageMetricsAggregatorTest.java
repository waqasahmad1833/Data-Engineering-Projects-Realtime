package com.example.clickstream.processing;

import com.example.clickstream.model.ClickEvent;
import com.example.clickstream.model.PageMetric;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PageMetricsAggregatorTest {

    private final PageMetricsAggregator aggregator = new PageMetricsAggregator();

    private static ClickEvent event(String type, String user, String session, Double price, Integer quantity) {
        ClickEvent e = new ClickEvent();
        e.eventType = type;
        e.userId = user;
        e.sessionId = session;
        e.price = price;
        e.quantity = quantity;
        return e;
    }

    @Test
    void countsEventsUsersSessionsAndRevenue() {
        PageMetricAccumulator acc = aggregator.createAccumulator();
        acc = aggregator.add(event("purchase", "u1", "s1", 10.0, 2), acc);
        acc = aggregator.add(event("purchase", "u1", "s2", 5.5, 1), acc);
        acc = aggregator.add(event("purchase", "u2", "s3", 1.0, null), acc);

        PageMetric result = aggregator.getResult(acc);

        assertEquals(3, result.eventCount);
        assertEquals(2, result.uniqueUsers);
        assertEquals(3, result.uniqueSessions);
        assertEquals(26.5, result.revenue, 1e-9);
    }

    @Test
    void nonPurchaseEventsHaveNoRevenue() {
        PageMetricAccumulator acc = aggregator.add(
                event("product_view", "u1", "s1", 99.0, null), aggregator.createAccumulator());
        assertEquals(0.0, aggregator.getResult(acc).revenue);
    }

    @Test
    void mergesAccumulators() {
        PageMetricAccumulator a = aggregator.add(event("purchase", "u1", "s1", 10.0, 1), aggregator.createAccumulator());
        PageMetricAccumulator b = aggregator.add(event("purchase", "u1", "s2", 20.0, 1), aggregator.createAccumulator());

        PageMetric merged = aggregator.getResult(aggregator.merge(a, b));

        assertEquals(2, merged.eventCount);
        assertEquals(1, merged.uniqueUsers);
        assertEquals(2, merged.uniqueSessions);
        assertEquals(30.0, merged.revenue, 1e-9);
    }
}
