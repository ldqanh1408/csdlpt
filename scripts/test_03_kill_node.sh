#!/usr/bin/env bash
# Test 03 — kill node2 mid-stream → coordinator returns TIMEOUT with DEAD diagnosis.
# Verifies: C8 (heartbeat distinguishes dead vs slow), C9 (hard timeout + diagnosis).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

source "$SCRIPT_DIR/lib.sh"

trap cleanup_cluster EXIT

dc up -d coordinator node0 node1 node2 node3
wait_all_healthy

echo "==> start ingestor in background, then kill node2"
dc run --rm -d ingestor python ingest.py --n 20000 >/dev/null
sleep 2
docker kill csdlpt-node2 >/dev/null

echo "==> wait for hb gap to exceed threshold (>15s)"
sleep 18

body=$(curl -sf "http://localhost:8000/api/wait?timeout=5")
echo "$body" | "$JQ" .

assert_jq "$body" '.status == "TIMEOUT"'
assert_jq "$body" '.missing | contains([2])'
assert_jq "$body" '.diagnosis."2" | test("DEAD|NEVER_SEEN")'

echo "PASS: test_03_kill_node"
