package com.example.clickstream;

import com.example.clickstream.config.JobConfig;
import com.example.clickstream.config.StreamEnvironmentFactory;
import com.example.clickstream.model.ClickEvent;
import com.example.clickstream.model.PageMetric;
import com.example.clickstream.parsing.ClickEventParser;
import com.example.clickstream.processing.EventEnricher;
import com.example.clickstream.processing.EventParserFunction;
import com.example.clickstream.processing.PageMetricsAggregator;
import com.example.clickstream.processing.PageMetricsKeySelector;
import com.example.clickstream.processing.PageMetricsWindowFunction;
import com.example.clickstream.sink.ClickEventRowMapper;
import com.example.clickstream.sink.ClickHouseSink;
import com.example.clickstream.sink.KafkaSinks;
import com.example.clickstream.sink.PageMetricRowMapper;
import com.example.clickstream.source.KafkaSources;
import com.example.clickstream.time.WatermarkStrategies;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Real-time clickstream pipeline.
 *
 * <pre>
 * Kafka (clickstream.events)
 *   -> parse + validate ----------------------> Kafka (clickstream.dlq)   [invalid events]
 *   -> enrich (device, url, referrer, country)
 *   -> event-time watermarks
 *       -> ClickHouse analytics.events            [raw enriched events]
 *       -> 1-min tumbling window per (page, type)
 *           -> ClickHouse analytics.page_metrics_1m
 * </pre>
 */
public final class ClickstreamJob {

    private static final Logger LOG = LoggerFactory.getLogger(ClickstreamJob.class);

    private ClickstreamJob() {
    }

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.fromArgs(args);
        LOG.info("Starting job with {}", config);

        StreamExecutionEnvironment env = StreamEnvironmentFactory.create(config);
        buildPipeline(env, config);
        env.execute(config.getJobName());
    }

    /** Wires the full job: Kafka source -> processing -> ClickHouse sinks, plus the Kafka DLQ sink. */
    static void buildPipeline(StreamExecutionEnvironment env, JobConfig config) {
        // Source: raw JSON strings from Kafka (watermarks are assigned after parsing).
        DataStream<String> raw = env
                .fromSource(KafkaSources.clickstream(config), WatermarkStrategy.noWatermarks(), "kafka-clickstream")
                .uid("kafka-clickstream-source");

        DataStream<String> deadLetters = buildProcessing(raw, config);

        deadLetters.sinkTo(KafkaSinks.deadLetters(config))
                .name("kafka-dead-letter-sink")
                .uid("kafka-dead-letter-sink");
    }

    /**
     * Parses, enriches and aggregates raw events and attaches the ClickHouse sinks.
     * Independent of the source so it can be driven by any stream (e.g. in integration tests).
     *
     * @return the stream of dead-letter records for invalid payloads
     */
    static DataStream<String> buildProcessing(DataStream<String> raw, JobConfig config) {
        // 1. Parse + validate; invalid payloads go to a side output.
        SingleOutputStreamOperator<ClickEvent> parsed = raw
                .process(new EventParserFunction(
                        new ClickEventParser(config.getMaxFutureSkewMs()), config.getSourceTopic()))
                .name("parse-and-validate")
                .uid("parse-and-validate");

        // 2. Enrich and assign event-time watermarks.
        DataStream<ClickEvent> events = parsed
                .map(new EventEnricher())
                .name("enrich")
                .uid("enrich")
                .assignTimestampsAndWatermarks(WatermarkStrategies.clickEvents(
                        Duration.ofMillis(config.getMaxOutOfOrdernessMs()),
                        Duration.ofMillis(config.getSourceIdlenessMs())))
                .name("event-time-watermarks")
                .uid("event-time-watermarks");

        // 3. Raw enriched events -> ClickHouse.
        events.addSink(new ClickHouseSink<>(config.clickHouseSink("events"), new ClickEventRowMapper()))
                .name("clickhouse-events-sink")
                .uid("clickhouse-events-sink");

        // 4. Windowed page metrics -> ClickHouse. Late events re-fire the window with an updated result.
        DataStream<PageMetric> pageMetrics = events
                .keyBy(new PageMetricsKeySelector())
                .window(TumblingEventTimeWindows.of(Time.seconds(config.getWindowSizeSeconds())))
                .allowedLateness(Time.seconds(config.getAllowedLatenessSeconds()))
                .aggregate(new PageMetricsAggregator(), new PageMetricsWindowFunction())
                .name("page-metrics-window")
                .uid("page-metrics-window");

        pageMetrics.addSink(new ClickHouseSink<>(config.clickHouseSink("page_metrics_1m"), new PageMetricRowMapper()))
                .name("clickhouse-page-metrics-sink")
                .uid("clickhouse-page-metrics-sink");

        return parsed.getSideOutput(EventParserFunction.DEAD_LETTER_TAG);
    }
}
