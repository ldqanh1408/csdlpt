#!/bin/bash
# ── CSDLPT Entrypoint ───────────────────────────────────────────────────
# Cleans stale checkpoint data from previous runs before starting.
# Old RocksDB state (watermarks, open windows, seen-IDs) can cause:
#   - Stuck watermarks (W = timestamp from days ago)
#   - Memory leaks (100K+ open_windows restored from disk)
#   - 0% completeness (watermark behind all events)
#
# Set CLEAN_CHECKPOINT=false to skip cleanup (e.g. for crash recovery).
#
# IMPORTANT — shared /data volume across workers (design §6.2, Tier-2 Warm
# State): all worker nodes mount the SAME /data volume so a surviving node can
# read a failed node's /data/checkpoint/partition_k/ checkpoint on failover.
# Because of that, a worker MUST NOT wipe the whole /data on its own restart —
# doing so deletes the RocksDB dirs the other live workers have open and causes
# cascading "No such file or directory" crashes. So a worker cleans ONLY the
# data it owns (its node-keyed RocksDB + the partition_* checkpoints of the
# partitions assigned to THIS node). Roles with a dedicated volume
# (coordinator/aggregator) safely wipe the whole volume.

set -e

CLEAN="${CLEAN_CHECKPOINT:-true}"
CKPT_DIR="${CHECKPOINT_DIR:-/data/checkpoint}"

if [ "$CLEAN" != "true" ] && [ "$CLEAN" != "1" ] && [ "$CLEAN" != "yes" ]; then
    echo "[entrypoint] Skipping checkpoint cleanup (CLEAN_CHECKPOINT=$CLEAN)."
    exec "$@"
fi

if [ "$ROLE" = "worker" ] && [ -n "$NODE_ID" ]; then
    # Scoped cleanup: only this node's own data on the shared volume.
    echo "[entrypoint] Worker node $NODE_ID: cleaning own checkpoint data only (partitions: ${PARTITIONS:-none})."

    # Node-keyed RocksDB instances (strict/heuristic engines, dlq, replay).
    # The "-p*" / exact-id globs avoid matching sibling nodes (e.g. node 1 vs 11).
    rm -rf "$CKPT_DIR"/rocksdb-strict-"$NODE_ID"-p*    2>/dev/null || true
    rm -rf "$CKPT_DIR"/rocksdb-heuristic-"$NODE_ID"-p* 2>/dev/null || true
    rm -rf "$CKPT_DIR"/rocksdb-dlq-"$NODE_ID"          2>/dev/null || true
    rm -rf "$CKPT_DIR"/rocksdb-replay-"$NODE_ID"       2>/dev/null || true

    # Tier-2 per-partition checkpoints (partition_{pid}/) for THIS node's
    # partitions only. Other nodes' partition_* dirs are left intact so they
    # remain available for failover handoff.
    if [ -n "$PARTITIONS" ]; then
        IFS=',' read -ra _pids <<< "$PARTITIONS"
        for pid in "${_pids[@]}"; do
            pid="$(echo "$pid" | tr -d '[:space:]')"
            [ -n "$pid" ] && rm -rf "$CKPT_DIR/partition_$pid" 2>/dev/null || true
        done
    fi

    echo "[entrypoint] Worker node $NODE_ID checkpoint cleanup complete."
else
    # Dedicated volume (coordinator / aggregator / single-role): wipe all.
    echo "[entrypoint] Cleaning old checkpoint data in /data/ ..."
    find /data -mindepth 1 -maxdepth 1 -not -name '.*' -exec rm -rf {} + 2>/dev/null || true
    echo "[entrypoint] Checkpoint cleanup complete."
fi

exec "$@"
