"""
Chiến lược warm-up/cold-start cho watermark heuristic.

Khi DDSketch chưa đủ mẫu, module giữ L_eff an toàn, kiểm tra số mẫu/thời gian warmup và tải baseline nếu có.
"""

import time
from enum import Enum


class ColdStartPhase(Enum):
    """Lớp `ColdStartPhase` định nghĩa các trạng thái/hằng số dùng trong luồng xử lý."""
    PHASE_0 = 0
    PHASE_1 = 1
    NORMAL = 2


class ColdStartManager:
    """Lớp `ColdStartManager` quản lý trạng thái và thao tác nghiệp vụ tương ứng."""
    def __init__(
        self,
        warmup_min_seconds: float = 10.0,
        warmup_min_samples: int = 1000,
        L_max: float = 60.0,
        baseline_from_history: float = None,
        storage=None,
        partition_id: int = 0,
    ):
        """Khởi tạo đối tượng của `ColdStartManager` và thiết lập trạng thái ban đầu."""
        self.warmup_min_seconds = warmup_min_seconds
        self.warmup_min_samples = warmup_min_samples
        self.L_max = L_max
        self.baseline_from_history = baseline_from_history
        self.storage = storage
        self.partition_id = partition_id

        self.start_time: float = time.time()
        self.phase: ColdStartPhase = ColdStartPhase.PHASE_0
        self.sample_count: int = 0

    @property
    def is_warm(self) -> bool:
        """Kiểm tra điều kiện `is warm` và trả về boolean."""
        return self.phase == ColdStartPhase.NORMAL

    def update(self, sample_count: int) -> None:
        """Hàm `update` thực hiện phần xử lý liên quan đến update của `ColdStartManager`."""
        self.sample_count = sample_count
        elapsed = time.time() - self.start_time

        # Phase 2 (NORMAL): elapsed >= warmup_min_seconds AND sample_count >= warmup_min_samples
        if elapsed >= self.warmup_min_seconds and sample_count >= self.warmup_min_samples:
            self.phase = ColdStartPhase.NORMAL
        # Phase 0: elapsed < 5s AND sample_count < 100
        elif elapsed < 5.0 and sample_count < 100:
            self.phase = ColdStartPhase.PHASE_0
        # Phase 1: (elapsed >= 5s AND sample_count >= 100) OR (elapsed < 10s AND sample_count < 1000)
        else:
            self.phase = ColdStartPhase.PHASE_1

    def conservative_prior(self) -> float:
        """Hàm `conservative_prior` thực hiện phần xử lý liên quan đến conservative prior của `ColdStartManager`.
        
        Ghi chú gốc:
        Return conservative L_eff estimate during warm-up.
        """
        if self.baseline_from_history is not None:
            return max(self.L_max, self.baseline_from_history)
        return self.L_max

    def load_or_init(self, partition_id: int = None) -> dict | None:
        """Nạp dữ liệu/trạng thái `load or init` từ lưu trữ hoặc cấu hình.
        
        Ghi chú gốc:
        Try to load baseline from storage; return None if unavailable.
        """
        pid = partition_id if partition_id is not None else self.partition_id
        if self.storage is not None:
            return self.load_baseline(self.storage, pid)
        return None

    def should_emit_watermark(self) -> bool:
        """Quyết định có nên thực hiện `emit watermark` theo trạng thái hiện tại hay không.
        
        Ghi chú gốc:
        Whether to emit watermark based on current phase.
        """
        return self.phase in (ColdStartPhase.PHASE_1, ColdStartPhase.NORMAL)

    def get_L_eff(self, sketch_quantile: float = None) -> float:
        """Trả về thông tin `L eff` từ trạng thái hiện tại.
        
        Ghi chú gốc:
        Get effective lag based on current phase.
        
                Phase 2 (NORMAL): use sketch P99, capped at L_max.
                Phase 1 (warm-up): use sketch P99 as a floor ≥ conservative prior
                  (L_max).  This prevents the watermark from advancing aggressively
                  while the sketch is still collecting samples.
                Phase 0 or no sketch: use conservative prior (= L_max).
        """
        if self.phase == ColdStartPhase.NORMAL and sketch_quantile is not None:
            return min(sketch_quantile, self.L_max)
        if self.phase == ColdStartPhase.PHASE_1 and sketch_quantile is not None:
            # Conservative: take the LARGER of sketch estimate vs prior so
            # L_eff is at least L_max during warm-up.
            return max(sketch_quantile, self.conservative_prior())
        return self.conservative_prior()

    # ---- MinIO baseline persistence (Spec §6.5) ----

    def save_baseline(self, storage, partition_id: int, sketch_dict: dict) -> bool:
        """Lưu dữ liệu/trạng thái `baseline` để dùng lại sau.
        
        Ghi chú gốc:
        Save sketch baseline to MinIO for cold start on next restart.
        """
        if storage is None or storage.client is None:
            return False
        import io, json
        key = f"heuristic-watermark/baseline_history/{partition_id}/latest.json"
        data = json.dumps({
            "baseline_lag": self.conservative_prior(),
            "warmup_samples": self.warmup_min_samples,
            "saved_at": time.time(),
            "sketch": sketch_dict,
        }).encode()
        try:
            storage.client.put_object(storage.bucket, key, data=io.BytesIO(data), length=len(data))
            return True
        except Exception:
            return False

    def load_baseline(self, storage, partition_id: int) -> dict | None:
        """Nạp dữ liệu/trạng thái `load baseline` từ lưu trữ hoặc cấu hình.
        
        Ghi chú gốc:
        Load sketch baseline from MinIO. Returns dict or None.
        """
        if storage is None or storage.client is None:
            return None
        import json
        key = f"heuristic-watermark/baseline_history/{partition_id}/latest.json"
        try:
            response = storage.client.get_object(storage.bucket, key)
            data = json.loads(response.read())
            response.close()
            response.release_conn()
            return data
        except Exception:
            return None

    def status(self) -> dict:
        """Hàm `status` thực hiện phần xử lý liên quan đến status của `ColdStartManager`."""
        return {
            "phase": self.phase.name,
            "is_warm": self.is_warm,
            "elapsed_s": round(time.time() - self.start_time, 1),
            "sample_count": self.sample_count,
            "conservative_prior_s": self.conservative_prior(),
        }
