#!/usr/bin/env bash
set -euo pipefail
KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
CONFIG="${CONFIG:-$HOME/distributed-food-ordering/kafka/server.properties}"
BS="${BS:-172.25.244.150:9092}"
[[ -x "$KAFKA_HOME/bin/kafka-server-start.sh" ]] || { echo "Kafka not found at $KAFKA_HOME" >&2; exit 1; }
