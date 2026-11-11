#!/bin/sh
# Idempotent topic creation. Production partition counts are in k8s/kafka-topics.yaml.
set -eu

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
EVENTS_PARTITIONS="${EVENTS_PARTITIONS:-12}"
KT=/opt/kafka/bin/kafka-topics.sh

until "${KT}" --bootstrap-server "${BOOTSTRAP}" --list > /dev/null 2>&1; do
  echo "waiting for kafka..."; sleep 2
done

create() {
  topic="$1"; partitions="$2"; shift 2
  echo "ensuring ${topic} (${partitions} partitions)"
  "${KT}" --bootstrap-server "${BOOTSTRAP}" --create --if-not-exists \
    --topic "${topic}" --partitions "${partitions}" --replication-factor 1 "$@"
}

# Raw ad events: 3 days retention, compressed with zstd on the broker
create ad.events "${EVENTS_PARTITIONS}" \
  --config retention.ms=259200000 --config compression.type=zstd --config max.message.bytes=10485760
# Dead letters: 14 days
create ad.dlq 3 --config retention.ms=1209600000
# Real-time campaign spend for budget pacing: compacted, latest value per key
create campaign.spend 6 \
  --config cleanup.policy=compact,delete --config retention.ms=172800000 --config min.cleanable.dirty.ratio=0.1

"${KT}" --bootstrap-server "${BOOTSTRAP}" --list
