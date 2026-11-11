package com.example.adtech.processing;

import com.example.adtech.model.AdEvent;
import com.example.adtech.parsing.AdEventParser;
import com.example.adtech.parsing.ParseResult;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/** Valid events to the main output; invalid payloads to the dead-letter side output. */
public class AdEventParserFunction extends ProcessFunction<String, AdEvent> {

    private static final long serialVersionUID = 1L;

    public static final OutputTag<String> DEAD_LETTER_TAG = new OutputTag<>("dead-letter", Types.STRING);

    private final AdEventParser parser;

    private transient Counter validEvents;
    private transient Counter invalidEvents;

    public AdEventParserFunction(AdEventParser parser) {
        this.parser = parser;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MetricGroup metrics = getRuntimeContext().getMetricGroup().addGroup("adtech");
        validEvents = metrics.counter("validEvents");
        invalidEvents = metrics.counter("invalidEvents");
    }

    @Override
    public void processElement(String raw, Context ctx, Collector<AdEvent> out) {
        long now = System.currentTimeMillis();
        ParseResult result = parser.parse(raw, now);
        if (result.isValid()) {
            validEvents.inc();
            out.collect(result.getEvent());
        } else {
            invalidEvents.inc();
            ctx.output(DEAD_LETTER_TAG, DeadLetters.toJson(raw, result.getErrorType(), result.getErrorMessage(), now));
        }
    }
}
