#!/usr/bin/env bash
set -euo pipefail
KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
CONFIG="${CONFIG:-$HOME/team42-fresh/kafka/server.properties}"
BS="${BS:-172.25.124.60:9092}"
[[ -x "$KAFKA_HOME/bin/kafka-server-start.sh" ]] || { echo "Kafka not found at $KAFKA_HOME" >&2; exit 1; }
