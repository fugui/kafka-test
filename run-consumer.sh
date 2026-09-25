#!/bin/bash
# 启动 Consumer
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR="$DIR/kafka-log-system/consumer/target/consumer-1.0.0-jar-with-dependencies.jar"

if [ ! -f "$JAR" ]; then
    echo "ERROR: Consumer JAR not found at $JAR"
    echo "Please build first: cd kafka-log-system && mvn clean package -DskipTests"
    exit 1
fi

# 确保输出目录存在
mkdir -p "$DIR/data/jsonl"

echo "Starting Kafka High-Throughput Consumer..."
echo "JSONL output: $DIR/data/jsonl/"
echo "Dashboard UI: http://localhost:8080"
java -jar "$JAR" --output "$DIR/data/jsonl" "$@"
