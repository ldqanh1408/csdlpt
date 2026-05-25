"""Strict Coordinator - computes W_global from worker heartbeats."""

import json
import os
import time
from dataclasses import dataclass
from typing import Optional

from refactor.common.types import WorkerHeartbeat, WorkerStatus, PartitionState
from refactor.common.rocks_store import RocksStore


# RocksDB key prefix for partition state
_PFX_PART = "part:"
_PFX_META = "meta:"


@dataclass
class PartitionInfo:
    partition_id: int
    worker_id: str
    local_watermark: float
    last_update: float
    status: WorkerStatus
    state: PartitionState = PartitionState.ASSIGNED


class StrictCoordinator:

    def __init__(
        self,
        delta_base_s: float = 10.0,
        state_path: str = "/tmp/coordinator-state.json",
        db_path: Optional[str] = None,
    ):
        self.delta_base = delta_base_s
        self.state_path = state_path
        self.partitions: dict[int, PartitionInfo] = {}
        self.W_global: float = float("-inf")
        self.W_global_prev: float = float("-inf")
        self.term: int = 0
        self._worker_last_seen: dict[str, float] = {}
        self._worker_fencing_tokens: dict[str, int] = {}
        self._fencing_violations: int = 0
        self._node_watermarks: dict[str, float] = {}
        self._node_skew_max_ms: float = 0.0
        self._watermark_lag_s: float = 0.0
        self._skew_status: str = "OK"
        self._lag_status: str = "OK"
        self._combined_status: str = "Initializing"
        self._combined_diagnosis: str = "Initializing"

        # Optional failover manager reference (injected after construction)
        self._failover_manager: object | None = None

        # RocksDB persistent storage (None = in-memory-only, backward compatible)
        self._store: Optional[RocksStore] = None
        if db_path:
            self._store = RocksStore(db_path)
            self._load_from_store()

    # ------------------------------------------------------------------
    # RocksDB persistence helpers
    # ------------------------------------------------------------------

    def _persist_partition(self, part_id: int) -> None:
        if self._store is None:
            return
        p = self.partitions.get(part_id)
        if p is not None:
            self._store.put(f"{_PFX_PART}{part_id}", p)

    def _delete_partition(self, part_id: int) -> None:
        if self._store is None:
            return
        self._store.delete(f"{_PFX_PART}{part_id}")

    def _load_from_store(self) -> None:
        """Populate in-memory partitions and metadata from RocksDB."""
        if self._store is None:
            return
        for key, val in self._store.items(prefix=_PFX_PART):
            try:
                part_id = int(key[len(_PFX_PART):])
                self.partitions[part_id] = val
            except (ValueError, IndexError):
                continue
        meta = self._store.get(f"{_PFX_META}state")
        if meta is not None:
            self.term = meta.get("term", 0)
            self.W_global = meta.get("W_global", float("-inf"))
            self.W_global_prev = self.W_global

    def set_failover_manager(self, fm: object) -> None:
        """Inject a FailoverManager so broadcast() can include partition_types
        and recovery_info for workers."""
        self._failover_manager = fm

    def set_ingestor_health(self, health_monitor: object) -> None:
        """Inject an IngestorHealthMonitor so broadcast() can include
        W_meta_global and ingestor health state for Raft replication."""
        self._ingestor_health = health_monitor

    def receive_heartbeat(self, hb: WorkerHeartbeat) -> None:
        now = time.time()

        # Fencing token enforcement: reject stale-token heartbeats
        worker = hb.worker_id
        if worker in self._worker_fencing_tokens:
            if hb.fencing_token < self._worker_fencing_tokens[worker]:
                self._fencing_violations += 1
                return
        self._worker_fencing_tokens[worker] = max(
            self._worker_fencing_tokens.get(worker, 0), hb.fencing_token
        )
        if hb.fencing_token > self.term:
            self.term = hb.fencing_token

        self._worker_last_seen[worker] = now
        for part_id, lw in hb.partitions.items():
            if part_id not in self.partitions:
                self.partitions[part_id] = PartitionInfo(
                    partition_id=part_id,
                    worker_id=hb.worker_id,
                    local_watermark=lw,
                    last_update=now,
                    status=WorkerStatus.ACTIVE,
                )
                self._persist_partition(part_id)
            else:
                p = self.partitions[part_id]
                p.local_watermark = max(p.local_watermark, lw)
                p.last_update = now
                p.worker_id = hb.worker_id
                p.status = WorkerStatus.ACTIVE
                self._persist_partition(part_id)
        self._update_statuses()
        self._compute_global()

    def _update_statuses(self) -> None:
        now = time.time()
        for p in self.partitions.values():
            age = now - p.last_update
            if age <= 0.5:
                p.status = WorkerStatus.ACTIVE
            elif age <= 2.0:
                p.status = WorkerStatus.STALE
            elif age <= 30.0:
                p.status = WorkerStatus.IDLE
            else:
                p.status = WorkerStatus.FAILED

    def _compute_global(self) -> None:
        # §4.4.1: Strict includes IDLE partitions in min() — do NOT use Idleness Bypass
        # Only FAILED partitions are excluded.
        _ACTIVE_STATUSES = (WorkerStatus.ACTIVE, WorkerStatus.STALE, WorkerStatus.IDLE)

        active = [
            p.local_watermark
            for p in self.partitions.values()
            if p.status in _ACTIVE_STATUSES
        ]
        if active:
            candidate = min(active)
            self.W_global = max(self.W_global_prev, candidate)
            self.W_global_prev = self.W_global

        # §7 Node skew and watermark lag
        if self.partitions:
            self._node_watermarks.clear()
            for p in self.partitions.values():
                if p.status in _ACTIVE_STATUSES:
                    wid = p.worker_id
                    if wid not in self._node_watermarks:
                        self._node_watermarks[wid] = p.local_watermark
                    else:
                        self._node_watermarks[wid] = min(
                            self._node_watermarks[wid], p.local_watermark
                        )
            if self._node_watermarks:
                W_max = max(self._node_watermarks.values())
                skews = [W_max - lw for lw in self._node_watermarks.values()]
                skew_s = max(skews) if skews else 0.0
                self._node_skew_max_ms = skew_s * 1000.0
                if self._node_skew_max_ms <= 1000:
                    self._skew_status = "OK"
                elif self._node_skew_max_ms <= 5000:
                    self._skew_status = "Warning"
                else:
                    self._skew_status = "Critical"
        self._watermark_lag_s = time.time() - self.W_global - self.delta_base
        if self._watermark_lag_s < 12:
            self._lag_status = "OK"
        elif self._watermark_lag_s < 30:
            self._lag_status = "Warning"
        elif self._watermark_lag_s < 60:
            self._lag_status = "High"
        else:
            self._lag_status = "Critical"

        # Combined skew + lag diagnosis (spec §7.3)
        if self._skew_status == "OK" and self._lag_status == "OK":
            self._combined_status = "Healthy"
        elif self._skew_status == "Critical" or self._lag_status == "Critical":
            self._combined_status = "Critical"
        elif self._skew_status == "Warning" or self._lag_status in ("Warning", "High"):
            self._combined_status = "Degraded"
        else:
            self._combined_status = "Warning"

        # §7.3 Vietnamese diagnosis strings from 2x2 skew/lag matrix
        _high_skew = self._node_skew_max_ms > 1000
        _high_lag = self._watermark_lag_s > 30
        if not _high_skew and not _high_lag:
            self._combined_diagnosis = "Healthy"
        elif _high_skew and not _high_lag:
            self._combined_diagnosis = "Mot Node bottleneck cu the"
        elif not _high_skew and _high_lag:
            self._combined_diagnosis = "Cum cham deu"
        else:
            self._combined_diagnosis = "Cum cham + co Node yeu hon"

    def broadcast(self) -> dict:
        result = {
            "W_global": self.W_global,
            "term": self.term,
            "timestamp": time.time(),
            "partition_count": len(self.partitions),
            "active_workers": len(self._worker_last_seen),
            "fencing_violations": self._fencing_violations,
            "node_skew_max_ms": self._node_skew_max_ms,
            "watermark_lag_s": self._watermark_lag_s,
            "skew_status": self._skew_status,
            "lag_status": self._lag_status,
            "combined_status": self._combined_status,
            "combined_diagnosis": self._combined_diagnosis,
        }
        if self._failover_manager is not None:
            fm = self._failover_manager
            result["partition_types"] = fm.get_partition_types()
            recovery_info: dict[int, dict] = {}
            for pid in range(result["partition_count"]):
                owner = fm.get_partition_owner(pid)
                orig = getattr(fm, "_original_owner", {}).get(pid)
                if owner != orig:
                    recovery_info[pid] = fm.get_partition_recovery_info(pid)
            result["recovery_info"] = recovery_info
        ingestor = getattr(self, "_ingestor_health", None)
        if ingestor is not None:
            result["ingestor_health"] = {
                "W_meta_global": getattr(ingestor, "W_meta_global", 0.0),
                "summary": ingestor.summary() if hasattr(ingestor, "summary") else {},
            }
        return result

    def save_state(self) -> None:
        state = {
            "term": self.term,
            "W_global": self.W_global,
            "partitions": {
                str(k): {
                    "worker_id": v.worker_id,
                    "local_watermark": v.local_watermark,
                    "status": v.status.value,
                    "state": v.state.value,
                }
                for k, v in self.partitions.items()
            },
        }
        tmp = self.state_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(state, f)
        os.replace(tmp, self.state_path)

    def checkpoint(self) -> None:
        self.save_state()
        if self._store is not None:
            self._store.flush()

    def flush(self) -> None:
        self.save_state()
        if self._store is not None:
            self._store.flush()

    def close(self) -> None:
        self.save_state()
        if self._store is not None:
            self._store.close()
            self._store = None

        # RocksDB state persistence
        if self._store is not None:
            for part_id in self.partitions:
                self._persist_partition(part_id)
            meta = {
                "term": self.term,
                "W_global": self.W_global,
            }
            self._store.put(f"{_PFX_META}state", meta)
            self._store.flush()

    def load_state(self) -> bool:
        # If RocksDB is available, state was already restored in constructor
        if self._store is not None:
            return True

        if os.path.exists(self.state_path):
            with open(self.state_path) as f:
                state = json.load(f)
            self.term = state["term"]
            self.W_global = state["W_global"]
            self.W_global_prev = self.W_global
            for k_str, v in state.get("partitions", {}).items():
                pid = int(k_str)
                self.partitions[pid] = PartitionInfo(
                    partition_id=pid,
                    worker_id=v["worker_id"],
                    local_watermark=v["local_watermark"],
                    last_update=time.time(),
                    status=WorkerStatus(v["status"]),
                    state=PartitionState(v.get("state", "assigned")),
                )
            return True
        return False
