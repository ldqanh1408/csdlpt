#!/usr/bin/env bash
# D1 smoke test — verify /health của coordinator + 4 node trả 200.
# D2 sẽ thêm test_02_happy.sh kiểm full happy path 200K event.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

source "$SCRIPT_DIR/lib.sh"

trap cleanup_cluster EXIT

echo "==> build images"
dc build

echo "==> up cluster (coordinator + 4 nodes)"
dc up -d coordinator node0 node1 node2 node3

echo "==> wait for /health on all 5 ports"
wait_all_healthy

echo "==> verify /health payloads"
for port in 8000 8101 8102 8103 8104; do
    body=$(curl -sf "http://localhost:$port/health")
    echo "  :$port -> $body"
done

echo "PASS: test_01_smoke (D1 skeleton gate)"
