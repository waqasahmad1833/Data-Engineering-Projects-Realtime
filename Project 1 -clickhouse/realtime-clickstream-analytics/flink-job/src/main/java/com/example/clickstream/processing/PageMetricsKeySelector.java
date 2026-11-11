package com.example.clickstream.processing;

import com.example.clickstream.model.ClickEvent;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

/** Keys events by (page_url, event_type) for windowed aggregation. */
public class PageMetricsKeySelector implements KeySelector<ClickEvent, Tuple2<String, String>> {

    private static final long serialVersionUID = 1L;

    @Override
    public Tuple2<String, String> getKey(ClickEvent event) {
        return Tuple2.of(event.pageUrl, event.eventType);
    }
}
