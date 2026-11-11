package com.example.adtech.time;

import com.example.adtech.model.AdEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Duration;

/** Event-time watermarks for ad events. */
public final class WatermarkStrategies {

    private WatermarkStrategies() {
    }

    public static WatermarkStrategy<AdEvent> adEvents(Duration maxOutOfOrderness, Duration idleness) {
        return WatermarkStrategy.<AdEvent>forBoundedOutOfOrderness(maxOutOfOrderness)
                .withTimestampAssigner((SerializableTimestampAssigner<AdEvent>) (event, ts) -> event.eventTime)
                .withIdleness(idleness);
    }
}
