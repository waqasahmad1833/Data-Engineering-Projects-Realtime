package com.example.adtech.processing;

import com.example.adtech.model.AdEvent;
import com.example.adtech.model.EventType;
import org.apache.flink.api.common.functions.FilterFunction;

/** Keeps only events of one type. */
public class EventTypeFilter implements FilterFunction<AdEvent> {

    private static final long serialVersionUID = 1L;

    private final EventType type;

    public EventTypeFilter(EventType type) {
        this.type = type;
    }

    @Override
    public boolean filter(AdEvent event) {
        return event.is(type);
    }
}
