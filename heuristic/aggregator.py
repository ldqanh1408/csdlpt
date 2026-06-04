"""
Aggregator của nhánh Heuristic Watermark.

Nhận watermark cục bộ từ worker theo partition, lưu timestamp cập nhật và tính `W_global_h = min(W_h_i)` trên các partition còn hoạt động.
"""

import json
import os
import time
from dataclasses import dataclass
from typing import Optional

from common.types import WorkerStatus
from common.rocks_store import RocksStore


_PFX_PART = "part:"
_PFX_META = "meta:"


@dataclass
class PartitionWatermark:
    """Lớp `PartitionWatermark` gom dữ liệu và hành vi liên quan đến PartitionWatermark."""
    partition_id: int
    worker_id: str
    W_h: float
    last_update: float
    status: WorkerStatus


class HeuristicAggregator:
    """Lớp `HeuristicAggregator` gom dữ liệu và hành vi liên quan đến HeuristicAggregator.
    
    Ghi chú gốc:
    Merges per-partition heuristic watermarks into W_global_h.
    
        In production: 2 instances (Active-Standby) with ZK lock.
        Here: single-process with state persistence for failover simulation.
    """

    def __init__(self, state_path: str = "/tmp/aggregator-state.json",
                 db_path: Optional[str] = None):
        """Khởi tạo đối tượng của `HeuristicAggregator` và thiết lập trạng thái ban đầu."""
        self.state_path = state_path
        self.partitions: dict[int, PartitionWatermark] = {}
        self.W_global_h: float = float("-inf")
        self.W_global_h_prev: float = float("-inf")
        self._is_active: bool = True
        self._node_W_h: dict[str, float] = {}
        self._node_skew_max_ms: float = 0.0
        self._watermark_lag_s: float = 0.0

        # RocksDB state (None = in-memory only)
        self._store: Optional[RocksStore] = None
        if db_path is not None:
            self._store = RocksStore(db_path)
            self._load_from_store()

    # ------------------------------------------------------------------
    # RocksDB persistence helpers
    # ------------------------------------------------------------------

    def _persist_partition(self, part_id: int) -> None:
        """Ghi bền vững trạng thái `persist partition` xuống storage."""
        if self._store is None:
            return
        p = self.partitions.get(part_id)
        if p is not None:
            self._store.put(f"{_PFX_PART}{part_id}", p)

    def _load_from_store(self) -> None:
        """Nạp dữ liệu/trạng thái `load from store` từ lưu trữ hoặc cấu hình.
        
        Ghi chú gốc:
        Populate in-memory partitions and metadata from RocksDB.
        """
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
            self.W_global_h = meta.get("W_global_h", float("-inf"))
            self.W_global_h_prev = self.W_global_h

    def receive_worker_watermark(self, worker_id: str, partition_id: int, W_h: float) -> None:
        """Nhận dữ liệu hoặc thông điệp `receive worker watermark` từ thành phần nguồn."""
        now = time.time()
        if partition_id not in self.partitions:
            self.partitions[partition_id] = PartitionWatermark(
                partition_id=partition_id,
                worker_id=worker_id,
                W_h=W_h,
                last_update=now,
                status=WorkerStatus.ACTIVE,
            )
        else:
            p = self.partitions[partition_id]
            p.W_h = max(p.W_h, W_h)
            p.last_update = now
            p.worker_id = worker_id
            p.status = WorkerStatus.ACTIVE
        self._persist_partition(partition_id)
        self._update_statuses()
        self._compute_global()

    def _update_statuses(self) -> None:
        """Cập nhật trạng thái/metric `update statuses` dựa trên dữ liệu mới."""
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
        """Tính toán kết quả `compute global` từ dữ liệu hiện có."""
        active = [
            p.W_h for p in self.partitions.values()
            if p.status in (WorkerStatus.ACTIVE, WorkerStatus.STALE)
        ]
        if active:
            candidate = min(active)
            self.W_global_h = max(self.W_global_h_prev, candidate)
            self.W_global_h_prev = self.W_global_h

        # §10 Node skew and watermark lag
        if self.partitions:
            self._node_W_h.clear()
            for p in self.partitions.values():
                if p.status in (WorkerStatus.ACTIVE, WorkerStatus.STALE):
                    wid = p.worker_id
                    if wid not in self._node_W_h:
                        self._node_W_h[wid] = p.W_h
                    else:
                        self._node_W_h[wid] = min(self._node_W_h[wid], p.W_h)
            if self._node_W_h:
                W_max = max(self._node_W_h.values())
                skews = [W_max - wh for wh in self._node_W_h.values()]
                skew_s = max(skews) if skews else 0.0
                self._node_skew_max_ms = skew_s * 1000.0
        self._watermark_lag_s = time.time() - self.W_global_h

    def broadcast(self) -> dict:
        """Hàm `broadcast` thực hiện phần xử lý liên quan đến broadcast của `HeuristicAggregator`."""
        return {
            "W_global_h": self.W_global_h,
            "timestamp": time.time(),
            "partition_count": len(self.partitions),
            "active_count": sum(
                1 for p in self.partitions.values()
                if p.status in (WorkerStatus.ACTIVE, WorkerStatus.STALE)
            ),
            "node_skew_max_ms": self._node_skew_max_ms,
            "watermark_lag_s": self._watermark_lag_s,
        }

    def failover(self) -> None:
        """Hàm `failover` thực hiện phần xử lý liên quan đến failover của `HeuristicAggregator`.
        
        Ghi chú gốc:
        Simulate failover: standby becomes active.
        """
        self._is_active = True

    def save_state(self) -> None:
        """Lưu dữ liệu/trạng thái `state` để dùng lại sau."""
        state = {
            "W_global_h": self.W_global_h,
            "partitions": {
                str(k): {
                    "worker_id": v.worker_id,
                    "W_h": v.W_h,
                    "status": v.status.value,
                }
                for k, v in self.partitions.items()
            },
        }
        tmp = self.state_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(state, f)
        
        # Robust replace to handle transient WSL2/Docker filesystem sync delays
        for attempt in range(5):
            try:
                os.replace(tmp, self.state_path)
                break
            except FileNotFoundError:
                if attempt == 4:
                    raise
                os.makedirs(os.path.dirname(self.state_path), exist_ok=True)
                time.sleep(0.05)


        # RocksDB state persistence
        if self._store is not None:
            for part_id in self.partitions:
                self._persist_partition(part_id)
            meta = {"W_global_h": self.W_global_h}
            self._store.put(f"{_PFX_META}state", meta)
            self._store.flush()

    def load_state(self) -> bool:
        # If RocksDB is available, state was already restored in constructor
        """Nạp dữ liệu/trạng thái `load state` từ lưu trữ hoặc cấu hình."""
        if self._store is not None:
            return True

        if os.path.exists(self.state_path):
            with open(self.state_path) as f:
                state = json.load(f)
            self.W_global_h = state["W_global_h"]
            self.W_global_h_prev = self.W_global_h
            for k, v in state["partitions"].items():
                pid = int(k)
                self.partitions[pid] = PartitionWatermark(
                    partition_id=pid,
                    worker_id=v["worker_id"],
                    W_h=v["W_h"],
                    last_update=time.time(),
                    status=WorkerStatus(v["status"]),
                )
            return True
        return False

    @property
    def is_active(self) -> bool:
        """Kiểm tra điều kiện `is active` và trả về boolean."""
        return self._is_active
