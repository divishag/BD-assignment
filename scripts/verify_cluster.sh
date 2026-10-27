#!/usr/bin/env bash
set -euo pipefail
KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
BS="${BS:-172.25.124.60:9092}"
"$KAFKA_HOME/bin/kafka-metadata-quorum.sh" --bootstrap-controller 172.25.124.60:9093 describe --status
for topic in orders order_metrics; do "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BS" --describe --topic "$topic"; done
