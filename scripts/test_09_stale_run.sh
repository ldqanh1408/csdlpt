#!/usr/bin/env bash
# Test 09 — POST /completed with stale run_id → ack:false with reason stale_run_id.
# Verifies: C3 (run_id fence epoch).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

source "$SCRIPT_DIR/lib.sh"

trap cleanup_cluster EXIT

dc up -d coordinator
wait_for_health "http://localhost:8000/health"

# First, establish a valid run_id
curl -sf -X POST http://localhost:8000/api/run_started \
  -H "Content-Type: application/json" \
  -d '{"run_id":"run-current-abc123","scatter_total":-1}' >/dev/null

payload='{"run_id":"run-stale-deadbeef","node_id":0,"events_processed":1,"watermark_final":0.0,"completeness":1.0,"flush_ts":0.0,"flush_duration_ms":0.1,"schema_version":1}'

r=$(curl -sf -X POST http://localhost:8000/api/completed -H "Content-Type: application/json" -d "$payload")
echo "$r" | "$JQ" .

assert_jq "$r" '.ack == false'
assert_jq "$r" '.reason == "stale_run_id"'

echo "PASS: test_09_stale_run"
