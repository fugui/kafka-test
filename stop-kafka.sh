#!/bin/bash
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KAFKA_HOME="$DIR/kafka_2.13-4.3.1"

echo "Stopping Kafka..."
"$KAFKA_HOME/bin/kafka-server-stop.sh"

for i in {1..10}; do
    if ! pgrep -f 'kafka\.Kafka' > /dev/null; then
        echo "Kafka stopped."
        rm -f "$KAFKA_HOME/logs/kafka.pid"
        exit 0
    fi
    sleep 1
done

if pgrep -f 'kafka\.Kafka' > /dev/null; then
    echo "Kafka did not stop gracefully, force killing..."
    pkill -9 -f 'kafka\.Kafka'
fi
echo "Kafka stopped."
rm -f "$KAFKA_HOME/logs/kafka.pid"
