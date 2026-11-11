package com.example.clickstream.time;

import com.example.clickstream.model.ClickEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Duration;

/** Event-time watermarking for click events. */
public final class WatermarkStrategies {

    private WatermarkStrategies() {
    }

    /**
     * Bounded out-of-orderness watermarks on the client-side event time. Idle subtasks are
     * excluded so one quiet partition does not stall all windows.
     */
    public static WatermarkStrategy<ClickEvent> clickEvents(Duration maxOutOfOrderness, Duration idleness) {
        return WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(maxOutOfOrderness)
                .withTimestampAssigner((SerializableTimestampAssigner<ClickEvent>) (event, ts) -> event.eventTime)
                .withIdleness(idleness);
    }
}
