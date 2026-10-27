#!/usr/bin/env bash
set -euo pipefail

ROLE="${1:?usage: render_config.sh vm2|vm3}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

CONTROLLER_IP="${CONTROLLER_IP:-172.25.124.60}"
BROKER1_IP="${BROKER1_IP:-172.25.124.60}"
BROKER2_IP="${BROKER2_IP:-172.25.240.223}"

case "$ROLE" in
  vm2)
    NODE=1
    ROLES=broker,controller
    LISTENERS='PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093'
    ADVERTISED="PLAINTEXT://${BROKER1_IP}:9092"
    ;;
  vm3)
    NODE=2
    ROLES=broker
    LISTENERS='PLAINTEXT://0.0.0.0:9092'
    ADVERTISED="PLAINTEXT://${BROKER2_IP}:9092"
    ;;
  *)
    echo "Choose vm2 or vm3" >&2
    exit 1
    ;;
esac

OUT="${CONFIG:-$HOME/team42-fresh/kafka/server.properties}"

mkdir -p "$(dirname "$OUT")"

if [[ -e "$OUT" ]]; then
  echo "Refusing to overwrite $OUT" >&2
  exit 1
fi

sed \
  -e "s|@PROCESS_ROLES@|$ROLES|g" \
  -e "s|@NODE_ID@|$NODE|g" \
  -e "s|@CONTROLLER_QUORUM_VOTERS@|1@${CONTROLLER_IP}:9093|g" \
  -e "s|@LISTENERS@|$LISTENERS|g" \
  -e "s|@ADVERTISED_LISTENERS@|$ADVERTISED|g" \
  -e "s|@LOG_DIRS@|$HOME/team42-fresh/kafka-data|g" \
  "$ROOT/kafka/server.properties.template" > "$OUT"

echo "Created $OUT"

grep -E '^(node.id|process.roles|controller.quorum.voters|listeners|advertised.listeners|log.dirs)=' "$OUT"
