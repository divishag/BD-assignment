#!/usr/bin/env bash
set -euo pipefail
# ONLY for a newly created, empty broker data directory. Never use for old Kafka data.
KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
CONFIG="${CONFIG:-$HOME/distributed-food-ordering/kafka/server.properties}"
CLUSTER_ID="${CLUSTER_ID:?Set CLUSTER_ID to a NEW shared cluster ID for a FRESH cluster}"
DIR="$(sed -n 's/^log.dirs=//p' "$CONFIG")"
[[ -n "$DIR" && "$DIR" == "$HOME"/* ]] || { echo 'Refusing unsafe data path' >&2; exit 1; }
if [[ -e "$DIR/meta.properties" ]] || [[ -d "$DIR" && -n "$(ls -A "$DIR")" ]]; then echo "Storage not empty; refusing to format $DIR" >&2; exit 1; fi
mkdir -p "$DIR"
"$KAFKA_HOME/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$CONFIG"
