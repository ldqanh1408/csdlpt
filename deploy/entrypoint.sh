#!/bin/bash
# ── CSDLPT Entrypoint ───────────────────────────────────────────────────
# Cleans stale checkpoint data from previous runs before starting.
# Old RocksDB state (watermarks, open windows, seen-IDs) can cause:
#   - Stuck watermarks (W = timestamp from days ago)
#   - Memory leaks (100K+ open_windows restored from disk)
#   - 0% completeness (watermark behind all events)
#
# Set CLEAN_CHECKPOINT=false to skip cleanup (e.g. for crash recovery).

set -e

CLEAN="${CLEAN_CHECKPOINT:-true}"
if [ "$CLEAN" = "true" ] || [ "$CLEAN" = "1" ] || [ "$CLEAN" = "yes" ]; then
    echo "[entrypoint] Cleaning old checkpoint data in /data/ ..."
    find /data -mindepth 1 -maxdepth 1 -not -name '.*' -exec rm -rf {} + 2>/dev/null || true
    echo "[entrypoint] Checkpoint cleanup complete."
else
    echo "[entrypoint] Skipping checkpoint cleanup (CLEAN_CHECKPOINT=$CLEAN)."
fi

exec "$@"
