#!/usr/bin/env bash
# Shared helpers for D1+ smoke and acceptance scripts.
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-deploy/docker-compose.yml}"

JQ="${JQ:-}"
if [ -z "$JQ" ]; then
    JQ=$(command -v jq 2>/dev/null || echo "/tmp/jq")
fi

dc() {
    docker compose -f "$COMPOSE_FILE" "$@"
}

wait_for_health() {
    local url="$1"
    local max="${2:-30}"
    local i=0
    while [ "$i" -lt "$max" ]; do
        if curl -sf -m 2 "$url" >/dev/null 2>&1; then
            return 0
        fi
        i=$((i + 1))
        sleep 1
    done
    echo "FAIL: $url never became healthy after ${max}s" >&2
    return 1
}

wait_all_healthy() {
    wait_for_health "http://localhost:8000/health"
    for p in 8101 8102 8103 8104; do
        wait_for_health "http://localhost:$p/health"
    done
}

assert_jq() {
    local body="$1"
    local expr="$2"
    if ! echo "$body" | "$JQ" -e "$expr" >/dev/null; then
        echo "FAIL assertion: $expr" >&2
        echo "  body: $body" >&2
        return 1
    fi
}

cleanup_cluster() {
    dc down -v 2>/dev/null || true
}
