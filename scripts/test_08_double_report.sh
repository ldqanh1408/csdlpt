#!/usr/bin/env bash
# Test 08 — manually POST same node_id twice → coordinator dedups via dict[node_id].
# Verifies: C4 (idempotent set semantics).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

source "$SCRIPT_DIR/lib.sh"

trap cleanup_cluster EXIT

dc up -d coordinator
wait_for_health "http://localhost:8000/health"

RUN_ID="run-dedup-test-001"
echo "==> RUN_ID=$RUN_ID"
curl -sf -X POST http://localhost:8000/api/run_started \
  -H "Content-Type: application/json" \
  -d "{\"run_id\":\"$RUN_ID\",\"scatter_total\":-1}" >/dev/null

payload='{"run_id":"'"$RUN_ID"'","node_id":7,"events_processed":100,"watermark_final":0.0,"completeness":0.99,"flush_ts":0.0,"flush_duration_ms":1.0,"schema_version":1}'

echo "==> POST 1"
r1=$(curl -sf -X POST http://localhost:8000/api/completed -H "Content-Type: application/json" -d "$payload")
echo "  $r1"
echo "==> POST 2 (duplicate)"
r2=$(curl -sf -X POST http://localhost:8000/api/completed -H "Content-Type: application/json" -d "$payload")
echo "  $r2"

assert_jq "$r2" '.received == 1'

echo "PASS: test_08_double_report"
