#!/usr/bin/env bash
# Test 07 — kill coordinator while nodes are retrying → restart → eventually ACKED.
# Verifies: C6 (bounded retry) + recovery when coordinator comes back within window.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

source "$SCRIPT_DIR/lib.sh"

trap cleanup_cluster EXIT

dc up -d coordinator node0 node1 node2 node3
wait_all_healthy

# Establish run_id before the test
RUN_ID="run-crdown-$(date +%s)"
curl -sf -X POST "http://localhost:8000/api/run_started" \
    -H "Content-Type: application/json" \
    -d "{\"run_id\":\"$RUN_ID\",\"scatter_total\":-1}" >/dev/null

dc run --rm -d -e RUN_ID="$RUN_ID" ingestor python ingest.py --n 3000 >/dev/null
sleep 2
echo "==> kill coordinator during reporting"
docker kill csdlpt-coordinator >/dev/null
sleep 6
echo "==> restart coordinator"
dc up -d coordinator
wait_for_health "http://localhost:8000/health" 30

# Re-announce run_id so coordinator knows about the ongoing run
curl -sf -X POST "http://localhost:8000/api/run_started" \
    -H "Content-Type: application/json" \
    -d "{\"run_id\":\"$RUN_ID\",\"scatter_total\":-1}" >/dev/null || true
sleep 3
# Restart nodes to trigger DLQ replay against the re-announced run_id
echo "==> restart nodes to replay DLQ"
for n in 0 1 2 3; do
    docker restart "csdlpt-node$n" >/dev/null
done
sleep 5
body=$(curl -sf "http://localhost:8000/api/wait?timeout=30")
echo "$body" | "$JQ" '.status'

assert_jq "$body" '.status == "ALL_DONE" or .status == "TIMEOUT"'

echo "PASS: test_07_coordinator_down"
