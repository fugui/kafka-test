#!/bin/bash
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KAFKA_HOME="$DIR/kafka_2.13-4.3.1"

if pgrep -f 'kafka\.Kafka' > /dev/null; then
    echo "Kafka is already running!"
    exit 0
fi

echo "Starting Kafka..."
mkdir -p "$KAFKA_HOME/logs"
nohup "$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_HOME/config/server.properties" > "$KAFKA_HOME/logs/kafkaServer.out" 2>&1 &
echo $! > "$KAFKA_HOME/logs/kafka.pid"

for i in {1..10}; do
    if ss -tulpn 2>/dev/null | grep -q ':9092'; then
        echo "Kafka started successfully! Listening on localhost:9092"
        exit 0
    fi
    sleep 1
done

echo "Kafka started (PID: $(cat "$KAFKA_HOME/logs/kafka.pid")), please check $KAFKA_HOME/logs/kafkaServer.out"
