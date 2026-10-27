#!/usr/bin/env bash
set -euo pipefail
KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
"$KAFKA_HOME/bin/kafka-server-stop.sh"
echo 'Check pgrep -af kafka.Kafka to verify shutdown before restarting.'
