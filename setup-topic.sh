#!/bin/bash
# 创建 Kafka Topic: log-events (8 分区, 1 副本)
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KAFKA_HOME="$DIR/kafka_2.13-4.3.1"

TOPIC="log-events"
PARTITIONS=8
REPLICATION=1

echo "Creating topic '$TOPIC' with $PARTITIONS partitions..."

"$KAFKA_HOME/bin/kafka-topics.sh" \
    --bootstrap-server localhost:9092 \
    --create \
    --topic "$TOPIC" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION" \
    --if-not-exists

echo ""
echo "Topic details:"
"$KAFKA_HOME/bin/kafka-topics.sh" \
    --bootstrap-server localhost:9092 \
    --describe \
    --topic "$TOPIC"
