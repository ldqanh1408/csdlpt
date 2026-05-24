#!/usr/bin/env bash
# Test 10 — coordinator detects DATA_LOSS when scatter_total > sum(events_processed).
# Verifies: C11 (integrity check).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

source "$SCRIPT_DIR/lib.sh"

trap cleanup_cluster EXIT

dc up -d coordinator node0 node1 node2 node3
wait_all_healthy

# Generate a run_id and establish it with coordinator
RUN_ID="run-loss-test-$(date +%s)"
curl -sf -X POST "http://localhost:8000/api/run_started" \
    -H "Content-Type: application/json" \
    -d "{\"run_id\":\"$RUN_ID\",\"scatter_total\":-1}" >/dev/null

dc run --rm -e RUN_ID="$RUN_ID" ingestor python ingest.py --n 1000

# Bump scatter_total to simulate lost-in-flight events.
curl -sf -X POST "http://localhost:8000/api/run_started" \
    -H "Content-Type: application/json" \
    -d "{\"run_id\":\"$RUN_ID\",\"scatter_total\":99999}" >/dev/null

body=$(curl -sf "http://localhost:8000/api/wait?timeout=30")
echo "$body" | "$JQ" '.status, .missing_events'

assert_jq "$body" '.status == "DATA_LOSS"'
assert_jq "$body" '.missing_events > 0'

echo "PASS: test_10_data_loss"
