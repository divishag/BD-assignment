#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
"$ROOT/scripts/render_config.sh" vm2
echo 'NEXT: check README; initialize ONLY fresh empty storage with shared CLUSTER_ID.'
