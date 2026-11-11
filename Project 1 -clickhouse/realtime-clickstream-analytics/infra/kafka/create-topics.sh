#!/bin/sh
# Creates the Kafka topics used by the platform. Idempotent (--if-not-exists).
set -eu

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
SOURCE_TOPIC="${SOURCE_TOPIC:-clickstream.events}"
DLQ_TOPIC="${DLQ_TOPIC:-clickstream.dlq}"
KAFKA_TOPICS=/opt/kafka/bin/kafka-topics.sh

echo "Waiting for Kafka at ${BOOTSTRAP} ..."
until "${KAFKA_TOPICS}" --bootstrap-server "${BOOTSTRAP}" --list > /dev/null 2>&1; do
  sleep 2
done

create_topic() {
  topic="$1"; partitions="$2"; retention_ms="$3"
  echo "Ensuring topic ${topic} (partitions=${partitions}, retention.ms=${retention_ms})"
  "${KAFKA_TOPICS}" --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor 1 \
    --config retention.ms="${retention_ms}" \
    --config cleanup.policy=delete
}

# 7 days of raw events, partitioned by session_id on the producer side
create_topic "${SOURCE_TOPIC}" 6 604800000
# 14 days of dead letters for investigation / replay
create_topic "${DLQ_TOPIC}" 1 1209600000

echo "Topics:"
"${KAFKA_TOPICS}" --bootstrap-server "${BOOTSTRAP}" --list
