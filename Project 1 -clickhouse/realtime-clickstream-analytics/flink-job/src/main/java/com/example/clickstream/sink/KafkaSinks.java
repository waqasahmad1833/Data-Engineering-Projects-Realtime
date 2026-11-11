package com.example.clickstream.sink;

import com.example.clickstream.config.JobConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

/** Kafka sink factory. */
public final class KafkaSinks {

    private KafkaSinks() {
    }

    public static KafkaSink<String> deadLetters(JobConfig config) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(config.getKafkaBootstrapServers())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(config.getDlqTopic())
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }
}
