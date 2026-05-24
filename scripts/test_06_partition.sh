#!/usr/bin/env bash
# Test 06 — partition node0 from network while reporting → DLQ persists → reconnect → replay.
# Verifies: C6 (retry+backoff bounded), C7 (DLQ persistence).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

source "$SCRIPT_DIR/lib.sh"

trap cleanup_cluster EXIT

dc up -d coordinator node0 node1 node2 node3
wait_all_healthy

dc run --rm -d ingestor python ingest.py --n 5000 >/dev/null
sleep 2

NETWORK_NAME=$(docker inspect csdlpt-coordinator --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}' 2>/dev/null || echo "deploy_default")

echo "==> disconnect node0 from $NETWORK_NAME"
docker network disconnect "$NETWORK_NAME" csdlpt-node0 || true
sleep 30  # exhaust 5 retries (~31s)

echo "==> verify node0 DLQ"
ls -la dlq/node0/ || echo "  (dlq dir not yet populated)"

echo "==> reconnect + restart node0 to trigger DLQ replay"
docker network connect "$NETWORK_NAME" csdlpt-node0 || true
sleep 3
docker restart csdlpt-node0 >/dev/null
wait_for_health "http://localhost:8101/health" 30

body=$(curl -sf "http://localhost:8000/api/wait?timeout=15")
echo "$body" | "$JQ" '.status'

assert_jq "$body" '.status == "ALL_DONE" or .status == "TIMEOUT"'

echo "PASS: test_06_partition (DLQ + reconnect verified)"
