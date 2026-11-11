package com.example.adtech.processing;

import com.example.adtech.model.AdEvent;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Drops events whose event_id was already seen within the TTL.
 *
 * <p>Ad exchanges deliver at-least-once, so duplicates are normal. Deduplicating in Flink keeps
 * the 30+ TB/month raw table a plain MergeTree: no ReplacingMergeTree, no FINAL on hot queries.
 * State is one flag per event_id with a TTL; with RocksDB and the compaction-filter cleanup,
 * expired entries are dropped during compaction instead of piling up.
 */
public class DeduplicationFunction extends KeyedProcessFunction<String, AdEvent, AdEvent> {

    private static final long serialVersionUID = 1L;

    private final long ttlMs;

    private transient ValueState<Boolean> seen;
    private transient Counter duplicates;
    private transient Counter uniqueEvents;

    public DeduplicationFunction(long ttlMs) {
        this.ttlMs = ttlMs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateTtlConfig ttl = StateTtlConfig.newBuilder(Time.milliseconds(ttlMs))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupInRocksdbCompactFilter(1000)
                .build();
        ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("seen", Types.BOOLEAN);
        descriptor.enableTimeToLive(ttl);
        seen = getRuntimeContext().getState(descriptor);

        duplicates = getRuntimeContext().getMetricGroup().addGroup("adtech").counter("duplicatesDropped");
        uniqueEvents = getRuntimeContext().getMetricGroup().addGroup("adtech").counter("uniqueEvents");
    }

    @Override
    public void processElement(AdEvent event, Context ctx, Collector<AdEvent> out) throws Exception {
        if (seen.value() != null) {
            duplicates.inc();
            return;
        }
        seen.update(Boolean.TRUE);
        uniqueEvents.inc();
        out.collect(event);
    }
}
