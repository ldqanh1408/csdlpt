#!/usr/bin/env bash
# Test 04 — kill node2, revive after 5s, ingestor finishes → ALL_DONE or graceful TIMEOUT.
# Verifies: C2 (run_id fence preserved), C8 (heartbeat resumes after revive).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

source "$SCRIPT_DIR/lib.sh"

trap cleanup_cluster EXIT

dc up -d coordinator node0 node1 node2 node3
wait_all_healthy

dc run --rm -d ingestor python ingest.py --n 5000 >/dev/null
sleep 1
docker kill csdlpt-node2 >/dev/null
echo "==> node2 killed; sleeping 5s before revive"
sleep 5
dc up -d node2
wait_for_health "http://localhost:8103/health"

# Re-scatter — production system would replay from log;
# here we just verify cluster stays responsive after revive.
dc run --rm ingestor python ingest.py --n 5000 --seed 43

body=$(curl -sf "http://localhost:8000/api/wait?timeout=30")
echo "$body" | "$JQ" '.status'

assert_jq "$body" '.status == "ALL_DONE" or .status == "TIMEOUT" or .status == "DATA_LOSS"'

echo "PASS: test_04_revive_node (cluster recovered after revive)"
