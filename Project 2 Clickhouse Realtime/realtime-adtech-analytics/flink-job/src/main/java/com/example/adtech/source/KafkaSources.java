package com.example.adtech.source;

import com.example.adtech.config.JobConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/** Kafka source factory, tuned for large fetches. */
public final class KafkaSources {

    private KafkaSources() {
    }

    public static KafkaSource<String> adEvents(JobConfig config) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(config.getKafkaBootstrapServers())
                .setTopics(config.getEventsTopic())
                .setGroupId(config.getConsumerGroupId())
                .setClientIdPrefix(config.getConsumerGroupId())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("partition.discovery.interval.ms", "60000")
                .setProperty("commit.offsets.on.checkpoint", "true")
                .setProperty("fetch.min.bytes", "1048576")
                .setProperty("fetch.max.wait.ms", "200")
                .setProperty("max.partition.fetch.bytes", "8388608")
                .build();
    }
}
