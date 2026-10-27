#!/usr/bin/env bash
set -euo pipefail
CONFIG="${CONFIG:-$HOME/team42-fresh/kafka/server.properties}"
[[ -f "$CONFIG" ]] || { echo "Missing $CONFIG"; exit 1; }
if grep -q '@[A-Z_]*@' "$CONFIG"; then echo 'Unresolved placeholders' >&2; exit 1; fi
grep -E '^(process.roles|node.id|controller.quorum.voters|listeners|advertised.listeners|log.dirs)=' "$CONFIG"
