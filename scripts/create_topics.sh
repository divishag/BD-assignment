#!/usr/bin/env bash
set -euo pipefail
KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
BS="${BS:-172.25.244.150:9092}"
for topic in orders order_metrics; do
 "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BS" --create --if-not-exists --topic "$topic" --partitions 2 --replication-factor 2
 "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BS" --describe --topic "$topic"
done
