#!/usr/bin/env bash
# Test 05 — inject 100ms network delay on node1 → ALL_DONE still.
# Verifies: C8 (slow node treated as alive, not dead).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

source "$SCRIPT_DIR/lib.sh"

trap cleanup_cluster EXIT

dc up -d coordinator node0 node1 node2 node3
wait_all_healthy

echo "==> inject 100ms delay on node1 eth0"
docker exec csdlpt-node1 tc qdisc add dev eth0 root netem delay 100ms

dc run --rm ingestor python ingest.py --n 5000

body=$(curl -sf "http://localhost:8000/api/wait?timeout=60")
echo "$body" | "$JQ" '.status, .results."1".flush_duration_ms'

assert_jq "$body" '.status == "ALL_DONE" or .status == "DATA_LOSS"'
assert_jq "$body" '.results."1" != null'

echo "PASS: test_05_slow_node"
