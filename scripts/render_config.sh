#!/usr/bin/env bash
set -euo pipefail
ROLE="${1:?usage: render_config.sh vm2|vm3}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
case "$ROLE" in
 vm2) NODE=1; ROLES=broker,controller; LISTENERS='PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093'; ADVERTISED='PLAINTEXT://172.25.244.150:9092';;
 vm3) NODE=2; ROLES=broker; LISTENERS='PLAINTEXT://0.0.0.0:9092'; ADVERTISED='PLAINTEXT://172.25.27.214:9092';;
 *) echo 'Choose vm2 or vm3' >&2; exit 1;;
esac
OUT="${CONFIG:-$HOME/distributed-food-ordering/kafka/server.properties}"
mkdir -p "$(dirname "$OUT")"
if [[ -e "$OUT" ]]; then echo "Refusing to overwrite $OUT; back it up and choose another CONFIG path" >&2; exit 1; fi
sed -e "s|@PROCESS_ROLES@|$ROLES|g" -e "s|@NODE_ID@|$NODE|g" -e 's|@CONTROLLER_QUORUM_VOTERS@|1@172.25.244.150:9093|g' -e "s|@LISTENERS@|$LISTENERS|g" -e "s|@ADVERTISED_LISTENERS@|$ADVERTISED|g" -e "s|@LOG_DIRS@|$HOME/team42-kafka-data|g" "$ROOT/kafka/server.properties.template" > "$OUT"
echo "Created $OUT"; grep -E '^(node.id|process.roles|controller.quorum.voters|listeners|advertised.listeners|log.dirs)=' "$OUT"
