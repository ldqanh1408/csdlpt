#!/usr/bin/env bash
# Orchestrate all 10 acceptance tests sequentially. Exit 0 only if all pass.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

TESTS=(
    test_01_smoke
    test_02_happy
    test_03_kill_node
    test_04_revive_node
    test_05_slow_node
    test_06_partition
    test_07_coordinator_down
    test_08_double_report
    test_09_stale_run
    test_10_data_loss
)

declare -a results
pass_count=0
fail_count=0

for t in "${TESTS[@]}"; do
    echo ""
    echo "============================================================"
    echo "RUN: $t"
    echo "============================================================"
    # Clean everything between tests to prevent cross-test contamination
    docker compose -f "$SCRIPT_DIR/../deploy/docker-compose.yml" down -v --remove-orphans 2>/dev/null || true
    # Kill any lingering ingestor containers (dc run --rm -d orphans)
    docker ps -q --filter "name=deploy-ingestor" 2>/dev/null | xargs -r docker kill 2>/dev/null || true
    docker container prune -f 2>/dev/null || true
    rm -rf "$SCRIPT_DIR/../dlq/" 2>/dev/null || true
    if bash "$SCRIPT_DIR/${t}.sh"; then
        results+=("PASS $t")
        pass_count=$((pass_count + 1))
    else
        results+=("FAIL $t (exit $?)")
        fail_count=$((fail_count + 1))
    fi
done

echo ""
echo "============================================================"
echo "SUMMARY"
echo "============================================================"
for r in "${results[@]}"; do
    echo "  $r"
done
echo ""
echo "TOTAL: $pass_count passed, $fail_count failed (out of ${#TESTS[@]})"

[ "$fail_count" -eq 0 ]
