#!/usr/bin/env bash
# Test 02 — happy path: 10K synthetic events, all 4 nodes report, ALL_DONE.
# Verifies: C1 (flush-before-report), C4 (idempotent), C11 (integrity ok).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

source "$SCRIPT_DIR/lib.sh"

trap cleanup_cluster EXIT

echo "==> build + up"
dc build
dc up -d coordinator node0 node1 node2 node3

echo "==> wait healthy"
wait_all_healthy

echo "==> run ingestor (10K synthetic events)"
dc run --rm ingestor python ingest.py --n 10000

echo "==> /api/wait"
body=$(curl -sf "http://localhost:8000/api/wait?timeout=60")
echo "$body" | "$JQ" .

assert_jq "$body" '.status == "ALL_DONE"'
assert_jq "$body" '.missing_events == 0'
assert_jq "$body" '(.results | length) == 4'

echo "PASS: test_02_happy"
