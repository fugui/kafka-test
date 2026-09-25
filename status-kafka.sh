#!/bin/bash
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KAFKA_HOME="$DIR/kafka_2.13-4.3.1"

echo "=== Kafka Process Status ==="
PIDS=$(pgrep -f 'kafka\.Kafka')
if [ -n "$PIDS" ]; then
    echo "Kafka is RUNNING (PID: $PIDS)"
else
    echo "Kafka is NOT running."
fi

echo ""
echo "=== Port Listeners ==="
ss -tulpn 2>/dev/null | grep -E ':(9092|9093)' || echo "Port 9092/9093 not listening."

echo ""
echo "=== Topics ==="
if [ -n "$PIDS" ]; then
    "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --list 2>/dev/null || echo "Unable to connect to Kafka."
fi
