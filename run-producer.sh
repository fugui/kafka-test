#!/bin/bash
# 启动 Producer
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR="$DIR/kafka-log-system/producer/target/producer-1.0.0-jar-with-dependencies.jar"

if [ ! -f "$JAR" ]; then
    echo "ERROR: Producer JAR not found at $JAR"
    echo "Please build first: cd kafka-log-system && mvn clean package -DskipTests"
    exit 1
fi

echo "Starting Kafka Log Producer..."
java -jar "$JAR" "$@"
