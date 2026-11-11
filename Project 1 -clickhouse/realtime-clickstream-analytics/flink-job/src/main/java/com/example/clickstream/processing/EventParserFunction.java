package com.example.clickstream.processing;

import com.example.clickstream.model.ClickEvent;
import com.example.clickstream.parsing.ClickEventParser;
import com.example.clickstream.parsing.ParseResult;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Parses raw Kafka payloads. Valid events go to the main output; invalid ones are routed to the
 * {@link #DEAD_LETTER_TAG} side output instead of failing the job.
 */
public class EventParserFunction extends ProcessFunction<String, ClickEvent> {

    private static final long serialVersionUID = 1L;

    public static final OutputTag<String> DEAD_LETTER_TAG = new OutputTag<>("dead-letter", Types.STRING);

    private final ClickEventParser parser;
    private final String sourceTopic;

    private transient Counter validEvents;
    private transient Counter invalidEvents;

    public EventParserFunction(ClickEventParser parser, String sourceTopic) {
        this.parser = parser;
        this.sourceTopic = sourceTopic;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MetricGroup metrics = getRuntimeContext().getMetricGroup().addGroup("clickstream");
        validEvents = metrics.counter("validEvents");
        invalidEvents = metrics.counter("invalidEvents");
    }

    @Override
    public void processElement(String raw, Context ctx, Collector<ClickEvent> out) {
        long now = System.currentTimeMillis();
        ParseResult result = parser.parse(raw, now);
        if (result.isValid()) {
            validEvents.inc();
            out.collect(result.getEvent());
        } else {
            invalidEvents.inc();
            ctx.output(DEAD_LETTER_TAG,
                    DeadLetters.toJson(raw, result.getErrorType(), result.getErrorMessage(), sourceTopic, now));
        }
    }
}
