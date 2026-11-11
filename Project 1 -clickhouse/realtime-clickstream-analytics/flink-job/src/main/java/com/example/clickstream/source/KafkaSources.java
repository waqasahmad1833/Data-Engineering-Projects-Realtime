package com.example.clickstream.source;

import com.example.clickstream.config.JobConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/** Kafka source factory. */
public final class KafkaSources {

    private KafkaSources() {
    }

    /**
     * Reads raw JSON strings. Parsing happens in a downstream operator so malformed payloads
     * can be dead-lettered instead of crashing the source.
     */
    public static KafkaSource<String> clickstream(JobConfig config) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(config.getKafkaBootstrapServers())
                .setTopics(config.getSourceTopic())
                .setGroupId(config.getConsumerGroupId())
                .setClientIdPrefix(config.getConsumerGroupId())
                // Resume from committed offsets; start from the beginning on first run.
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("partition.discovery.interval.ms", "60000")
                .setProperty("commit.offsets.on.checkpoint", "true")
                .build();
    }
}
