#!/usr/bin/env bash
set -euo pipefail
KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
CONFIG="${CONFIG:-$HOME/team42-fresh/kafka/server.properties}"
[[ -f "$CONFIG" ]] || { echo 'Missing config' >&2; exit 1; }
if pgrep -f '[k]afka.Kafka' >/dev/null; then echo 'Kafka process already running; refusing second instance' >&2; exit 1; fi
DIR="$(sed -n 's/^log.dirs=//p' "$CONFIG")"
[[ -f "$DIR/meta.properties" ]] || { echo "Missing formatted storage: $DIR/meta.properties" >&2; exit 1; }
exec "$KAFKA_HOME/bin/kafka-server-start.sh" "$CONFIG"
