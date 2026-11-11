package com.example.adtech.sink;

import com.example.adtech.config.JobConfig;
import com.example.adtech.model.CampaignSpend;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

/** Kafka sink factories. */
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

    /** Real-time spend feed for the budget-pacing service. */
    public static KafkaSink<CampaignSpend> campaignSpend(JobConfig config) {
        KafkaRecordSerializationSchema<CampaignSpend> serializer = KafkaRecordSerializationSchema.builder()
                .setTopic(config.getSpendTopic())
                .setKeySerializationSchema(new CampaignSpendSerialization.Key())
                .setValueSerializationSchema(new CampaignSpendSerialization.Value())
                .build();
        return KafkaSink.<CampaignSpend>builder()
                .setBootstrapServers(config.getKafkaBootstrapServers())
                .setRecordSerializer(serializer)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setProperty("linger.ms", "20")
                .setProperty("compression.type", "zstd")
                .build();
    }
}
