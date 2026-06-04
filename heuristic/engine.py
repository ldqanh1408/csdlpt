"""
Engine Heuristic Watermark chạy theo từng partition.

Engine dùng SlidingWindowDDSketch để ước lượng L_eff từ phân vị lateness, cập nhật `W_h`, đóng cửa sổ sớm để giảm latency, đưa event quá trễ vào DLQ và xử lý burst/replay/cold-start/negative-lag.
"""

import json
import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


def _flag_enabled(name: str, default: bool = True) -> bool:
    """Đọc feature flag dạng boolean từ biến môi trường."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.lower() in ("1", "true", "yes", "on")

from local_ddsketch import DDSketch, SlidingWindowDDSketch
from common.types import LogEvent, WindowResult
from common.window import TumblingWindow
from common.metrics import HighResTimer, SystemMetrics
from common.rocks_store import RocksStore
from heuristic.cold_start import ColdStartManager
from heuristic.negative_lag import NegativeLagHandler, LagTier


# Tiền tố key trong RocksDB cho từng nhóm dữ liệu bền vững.
_PFX_OPEN = "ow:"
_PFX_CLOSED = "cw:"
_PFX_SEEN = "si:"
_PFX_META = "meta:"


DEFAULT_PARAMS = {
    "window_size_s": 5.0,
    "alpha": 0.01,
    "p_normal": 0.99,
    "p_safe": 0.999,
    "max_buckets": 1024,
    "max_lag_accepted": 10000000.0,
    "window_seconds": 60,
    "sub_sketch_granularity": 1,
    "warmup_min_seconds": 10,
    "warmup_min_samples": 50000,
    "L_max": 10000000.0,
    "wm_max_advance_rate": 1.5,
    "l_eff_update_threshold": 0.10,
    "l_eff_decay_rate": 0.05,
    # Ngưỡng "lệch tương lai": event có event_time vượt arrival_time quá ngưỡng này
    # (lag âm lớn) bị coi là bất thường, không được phép đẩy max_event_time/watermark.
    "max_future_skew_s": 300.0,
    "baseline_multiplier": 10,
    "exit_multiplier": 2,
    "exit_streak_seconds": 10,
    "snapshot_interval": 10,
    "snapshot_count": 6,
    "burst_threshold": 2.0,
    "recovery_minutes": 5,
}


@dataclass
class WindowAggregate:
    """Lớp `WindowAggregate` gom dữ liệu và hành vi liên quan đến WindowAggregate."""
    count: int = 0
    status_500: int = 0


class HeuristicWatermarkEngine:
    """Lớp `HeuristicWatermarkEngine` thực thi logic xử lý chính của engine.
    
    Ghi chú gốc:
    Per-partition engine using DDSketch to estimate watermark heuristically.
    """

    def __init__(self, partition_id: int = 0, worker_id: str = "", **kwargs):
        """Khởi tạo đối tượng của `HeuristicWatermarkEngine` và thiết lập trạng thái ban đầu."""
        cfg = {**DEFAULT_PARAMS, **kwargs}
        cfg["L_max"] = float(os.environ.get("HEURISTIC_L_MAX", cfg["L_max"]))
        cfg["max_lag_accepted"] = float(os.environ.get("HEURISTIC_MAX_LAG_ACCEPTED", os.environ.get("HEURISTIC_L_MAX", cfg["max_lag_accepted"])))
        cfg["warmup_min_samples"] = int(os.environ.get("HEURISTIC_WARMUP_SAMPLES", cfg["warmup_min_samples"]))
        cfg["warmup_min_seconds"] = float(os.environ.get("HEURISTIC_WARMUP_S", cfg["warmup_min_seconds"]))
        cfg["wm_max_advance_rate"] = float(os.environ.get("HEURISTIC_WM_ADVANCE_RATE", cfg["wm_max_advance_rate"]))
        cfg["l_eff_decay_rate"] = float(os.environ.get("HEURISTIC_L_EFF_DECAY_RATE", cfg["l_eff_decay_rate"]))
        cfg["max_future_skew_s"] = float(os.environ.get("HEURISTIC_MAX_FUTURE_SKEW_S", cfg["max_future_skew_s"]))
        # Percentile đang sweep phải đi tới engine. Nếu không đọc env ở đây,
        # worker tạo engine mà không truyền p_normal/p_safe, khiến mọi điểm sweep
        # âm thầm dùng default p=0.99 và completeness bị phẳng.
        cfg["p_normal"] = float(os.environ.get("HEURISTIC_P_NORMAL", cfg["p_normal"]))
        cfg["p_safe"] = float(os.environ.get("HEURISTIC_P_SAFE", cfg["p_safe"]))
        self.partition_id = partition_id
        self.worker_id = worker_id

        # Đường dẫn RocksDB: lấy ra trước khi cfg bị tiêu thụ tiếp.
        db_path: Optional[str] = cfg.pop("db_path", None)
        tiered_storage = cfg.pop("tiered_storage", None)
        self.checkpoint_dir: str = cfg.pop("checkpoint_dir",
                                            os.path.dirname(db_path) if db_path else "/tmp")

        # Bộ chia tumbling window theo event-time.
        self.tumbling = TumblingWindow(cfg["window_size_s"])

        # DDSketch trượt để ước lượng phân vị lateness gần đây.
        self.sketch = SlidingWindowDDSketch(
            window_seconds=cfg["window_seconds"],
            sub_sketch_granularity=cfg["sub_sketch_granularity"],
            alpha=cfg["alpha"],
            max_buckets=cfg["max_buckets"],
            min_value=1e-3,
            max_value=cfg["max_lag_accepted"],
        )

        # Trạng thái watermark heuristic của partition hiện tại.
        self.W_h: float = float("-inf")
        self.W_h_prev: float = float("-inf")
        self.W_global_h: float = float("-inf")
        self.max_event_time: float = float("-inf")
        self.L_eff: float = cfg["L_max"]
        self.L_eff_prev: float = cfg["L_max"]

        # Trạng thái cửa sổ đang mở/đã đóng.
        self.open_windows: dict[float, WindowAggregate] = defaultdict(WindowAggregate)
        self.closed_windows: dict[float, WindowResult] = {}
        self._window_closed_at: dict[float, float] = {}
        self.late_events: list[dict] = []

        # Bộ đếm lag cực lớn để cảnh báo dữ liệu bất thường.
        self.extreme_lag_count: int = 0
        self.punctuation_total: int = 0

        # Chống trùng lặp bằng TTL để giới hạn bộ nhớ. Bộ lọc idempotent chỉ cần
        # nhớ các event_id gần đây; mỗi entry lưu arrival_time để purge theo tuổi.
        self.seen_ids: dict[str, float] = {}
        self._dedup_ttl_s: float = 60.0

        # Percentile thích nghi: tăng an toàn khi burst, quay lại bình thường khi ổn định.
        self.p_current: float = cfg["p_normal"]
        self.p_normal: float = cfg["p_normal"]
        self.p_safe: float = cfg["p_safe"]
        self.burst_threshold: float = cfg["burst_threshold"]
        self.in_burst: bool = False
        self.burst_start: float = 0.0
        self.recovery_minutes: int = cfg["recovery_minutes"]
        self._quantile_history: list[float] = []

        # Alpha thích nghi cho DDSketch.
        self._current_alpha: float = cfg["alpha"]
        self._last_alpha_check: float = 0.0

        # Hysteresis để watermark không nhảy quá gắt giữa các lần cập nhật.
        self.wm_max_advance_rate: float = cfg["wm_max_advance_rate"]
        self.l_eff_update_threshold: float = cfg["l_eff_update_threshold"]
        # Tốc độ giảm tối đa của L_eff mỗi lần cập nhật (asymmetric hysteresis).
        self.l_eff_decay_rate: float = cfg["l_eff_decay_rate"]
        self.max_future_skew_s: float = cfg["max_future_skew_s"]

        # Quản lý snapshot gần đây để phục hồi nhanh.
        self.snapshot_interval: float = cfg["snapshot_interval"]
        self.snapshot_count: int = cfg["snapshot_count"]
        self._snapshots: list[tuple[float, dict]] = []
        self._last_snapshot: float = 0.0

        # Replay mode giúp xử lý dữ liệu phát lại có thể rất out-of-order.
        self.in_replay_mode: bool = False
        self._replay_start_time: float = 0.0
        self.baseline_multiplier: float = cfg["baseline_multiplier"]
        self.exit_multiplier: float = cfg["exit_multiplier"]
        self.exit_streak_seconds: float = cfg["exit_streak_seconds"]
        self.baseline_lag: float = cfg["L_max"]
        self._replay_stable_since: float = 0.0

        # Ngưỡng lag tối đa được chấp nhận trước khi coi là cực trị.
        self.max_lag_accepted: float = cfg["max_lag_accepted"]
        self.L_max: float = cfg["L_max"]

        # Cold start manager giữ watermark thận trọng cho tới khi đủ mẫu.
        self.cold_start = ColdStartManager(
            warmup_min_seconds=cfg["warmup_min_seconds"],
            warmup_min_samples=cfg["warmup_min_samples"],
            L_max=cfg["L_max"],
            storage=tiered_storage,
            partition_id=partition_id,
        )

        # Restore baseline from storage if available (spec §6.5)
        if tiered_storage is not None:
            baseline_data = self.cold_start.load_or_init(partition_id)
            if baseline_data is not None:
                self.L_eff = baseline_data.get("baseline_lag", self.L_max)
                sketch_data = baseline_data.get("sketch")
                if sketch_data is not None:
                    self.sketch = SlidingWindowDDSketch.from_dict(sketch_data)

        # Negative lag handler (spec §7)
        self.neg_lag = NegativeLagHandler(window_seconds=60.0)

        # Per-window loss tracking (spec §10.3)
        self._window_loss: dict[str, dict] = {}

        # Pending results for Kafka topic emission (Gap 1)
        self._pending_results: list[WindowResult] = []

        # Closed window TTL purge tracking (Gap 5)
        self._last_closed_purge: float = 0.0

        # Metrics
        self.metrics = SystemMetrics()
        self.proc_latencies_ns: list[float] = []
        self.start_time: float = time.time()
        self._last_wm_update_time: float = 0.0

        # RocksDB persistent storage (None = in-memory-only, backward compatible)
        self._store: Optional[RocksStore] = None
        if db_path is not None:
            self._store = RocksStore(db_path)
            self._restore_from_store()
            self._restore_late_events()
        # Secondary recovery: if RocksDB restored no state, try sketch.bin
        self._restore_from_sketch_bin()

        # Dedup compaction
        self._last_dedup_sweep: float = 0.0

        # Kafka offset tracking for warm restart (spec §8.3)
        self.kafka_seek_offset: int = 0

        # Idleness detection
        self.last_event_time: float = time.time()

        # Inbound backpressure queue (spec §5.11)
        self._inbound_queue: list = []
        self._queue_maxsize: int = 500
        self._backpressure_drops: int = 0

    # ---- RocksDB persistence helpers ----

    def _persist_open_window(self, ws: float) -> None:
        """Ghi bền vững trạng thái `persist open window` xuống storage."""
        if self._store is None:
            return
        agg = self.open_windows.get(ws)
        if agg is not None:
            self._store.put(f"{_PFX_OPEN}{ws}", agg)

    def _delete_open_window(self, ws: float) -> None:
        """Xóa dữ liệu `delete open window` khỏi bộ nhớ hoặc storage."""
        if self._store is None:
            return
        self._store.delete(f"{_PFX_OPEN}{ws}")

    def _persist_closed_window(self, ws: float, result: WindowResult) -> None:
        """Ghi bền vững trạng thái `persist closed window` xuống storage."""
        if self._store is None:
            return
        self._store.put(f"{_PFX_CLOSED}{ws}", result)

    def _persist_seen_id(self, event_id: str) -> None:
        """Ghi bền vững trạng thái `persist seen id` xuống storage."""
        if self._store is None:
            return
        ts = self.seen_ids.get(event_id, time.time())
        self._store.put(f"{_PFX_SEEN}{event_id}", ts)

    def _restore_from_store(self) -> None:
        """Khôi phục trạng thái `restore from store` từ checkpoint hoặc storage.
        
        Ghi chú gốc:
        Populate in-memory state from RocksDB on startup.
        """
        if self._store is None:
            return

        for key, val in self._store.items(prefix=_PFX_OPEN):
            ws = float(key[len(_PFX_OPEN):])
            if isinstance(val, dict):
                self.open_windows[ws] = WindowAggregate(
                    count=val.get("count", 0),
                    status_500=val.get("status_500", 0),
                )
            else:
                self.open_windows[ws] = val

        for key, val in self._store.items(prefix=_PFX_CLOSED):
            ws = float(key[len(_PFX_CLOSED):])
            self.closed_windows[ws] = val

        for key, val in self._store.items(prefix=_PFX_SEEN):
            eid = key[len(_PFX_SEEN):]
            # Older RocksDB rows store True (legacy boolean format); treat
            # them as just-seen so the TTL purge can phase them out.
            ts = float(val) if isinstance(val, (int, float)) else time.time()
            self.seen_ids[eid] = ts

        # Restore metadata
        meta = self._store.get(f"{_PFX_META}checkpoint")
        if meta is not None:
            self.W_h = meta.get("W_h", float("-inf"))
            self.W_global_h = meta.get("W_global_h", float("-inf"))
            self.max_event_time = meta.get("max_event_time", float("-inf"))
            self.L_eff = meta.get("L_eff", self.L_max)
            self.in_replay_mode = meta.get("in_replay_mode", False)
            self.kafka_seek_offset = meta.get("kafka_seek_offset", 0)
            sketch_data = meta.get("sketch")
            if sketch_data is not None:
                self.sketch = SlidingWindowDDSketch.from_dict(sketch_data)

    def _persist_late_events(self) -> None:
        """Ghi bền vững trạng thái `persist late events` xuống storage.
        
        Ghi chú gốc:
        Persist late_events list to RocksDB as JSON (spec §12.1, §13.4).
        """
        if self._store is None:
            return
        # Serialize as JSON string (not pickle) so the data is inspectable
        serialized = json.dumps(self.late_events)
        self._store.put(f"{_PFX_META}late_events", serialized)

    def _restore_late_events(self) -> None:
        """Khôi phục trạng thái `restore late events` từ checkpoint hoặc storage.
        
        Ghi chú gốc:
        Restore late_events from RocksDB on startup (spec §12.1).
        """
        if self._store is None:
            return
        raw = self._store.get(f"{_PFX_META}late_events")
        if raw is None:
            return
        try:
            if isinstance(raw, str):
                self.late_events = json.loads(raw)
            elif isinstance(raw, bytes):
                self.late_events = json.loads(raw.decode("utf-8"))
            else:
                # Legacy pickle format fallback
                self.late_events = raw if isinstance(raw, list) else []
            # Restore extreme_lag_count from restored events
            self.extreme_lag_count = sum(
                1 for e in self.late_events if isinstance(e, dict) and e.get("extreme_lag")
            )
        except (json.JSONDecodeError, TypeError, UnicodeDecodeError):
            logger.warning("Failed to decode late_events from RocksDB, starting fresh")
            self.late_events = []

    def _clear_persisted_late_events(self) -> None:
        """Làm sạch dữ liệu/trạng thái `clear persisted late events` đang lưu tạm.
        
        Ghi chú gốc:
        Remove persisted late_events from RocksDB (called after DLQ drain).
        """
        if self._store is None:
            return
        self._store.delete(f"{_PFX_META}late_events")

    def checkpoint(self) -> None:
        """Hàm `checkpoint` thực hiện phần xử lý liên quan đến checkpoint của `HeuristicWatermarkEngine`.
        
        Ghi chú gốc:
        Persist engine metadata to RocksDB and export sketch.bin JSON.
        """
        if self._store is not None:
            meta = {
                "W_h": self.W_h,
                "W_global_h": self.W_global_h,
                "max_event_time": self.max_event_time,
                "L_eff": self.L_eff,
                "in_replay_mode": self.in_replay_mode,
                "sketch": self.sketch.to_dict(),
                "kafka_seek_offset": self.kafka_seek_offset,
            }
            self._store.put(f"{_PFX_META}checkpoint", meta)
            self._persist_late_events()
            self._store.flush()

        # Export sketch.bin JSON alongside RocksDB checkpoint for
        # operational compatibility (backup/DR scripts can find it at a known path).
        sketch_path = os.path.join(self.checkpoint_dir, "sketch.bin")
        sketch_tmp = sketch_path + ".tmp"
        try:
            os.makedirs(self.checkpoint_dir, exist_ok=True)
        except (OSError, PermissionError):
            return
        with open(sketch_tmp, "w") as f:
            json.dump({
                "W_h": self.W_h,
                "W_global_h": self.W_global_h,
                "L_eff": self.L_eff,
                "max_event_time": self.max_event_time,
                "sketch": self.sketch.to_dict(),
                "updated_at": time.time(),
            }, f)

        # Robust replace to handle transient WSL2/Docker filesystem sync delays
        for attempt in range(5):
            try:
                os.replace(sketch_tmp, sketch_path)
                break
            except FileNotFoundError:
                if attempt == 4:
                    raise
                try:
                    os.makedirs(self.checkpoint_dir, exist_ok=True)
                except Exception:
                    pass
                time.sleep(0.05)


    def _restore_from_sketch_bin(self) -> None:
        """Khôi phục trạng thái `restore from sketch bin` từ checkpoint hoặc storage.
        
        Ghi chú gốc:
        Try loading state from sketch.bin as a secondary recovery path.
        
                Called after RocksDB restore. If RocksDB is empty (state still at
                defaults), attempt to recover from the JSON export.
        """
        sketch_path = os.path.join(self.checkpoint_dir, "sketch.bin")
        if not os.path.exists(sketch_path):
            return
        # Only restore if core state was NOT already loaded from RocksDB
        if self.W_h != float("-inf") or self.max_event_time != float("-inf"):
            return
        try:
            with open(sketch_path, "r") as f:
                data = json.load(f)
            self.W_h = data.get("W_h", float("-inf"))
            self.W_global_h = data.get("W_global_h", float("-inf"))
            self.L_eff = data.get("L_eff", self.L_max)
            self.max_event_time = data.get("max_event_time", float("-inf"))
            sketch_data = data.get("sketch")
            if sketch_data is not None:
                self.sketch = SlidingWindowDDSketch.from_dict(sketch_data)
            logger.info("Restored state from sketch.bin (RocksDB was empty)")
        except (json.JSONDecodeError, OSError, KeyError) as e:
            logger.warning("Failed to restore from sketch.bin: %s", e)

    # ---- Core watermark computation ----
    def _compute_watermark(self) -> float:
        """Tính toán kết quả `compute watermark` từ dữ liệu hiện có.
        
        Ghi chú gốc:
        W_h(t) = max(W_h(t-1), max(T_event) - L_eff(t)) with hysteresis.
        """
        self.W_h_prev = self.W_h

        if self.max_event_time == float("-inf"):
            return float("-inf")

        # During cold start Phase 0, do not emit watermark
        if self.cold_start.phase.name == "PHASE_0":
            return float("-inf")

        # BOO fallback: use L_max directly
        if self.neg_lag.should_degrade_to_boo():
            self.L_eff = self.L_max

        candidate = self.max_event_time - self.L_eff

        # On the first real W_h computation (W_h_prev == -inf), initialize
        # W_h_prev to candidate so that rate-limiting applies from the very
        # first call and the watermark cannot jump unconstrained.
        if self.W_h_prev == float("-inf"):
            self.W_h_prev = candidate

        # Monotonic enforcement
        self.W_h = max(self.W_h_prev, candidate)

        # Sweep / local-watermark-close mode (topic #112): close windows on the
        # pure local watermark W_h = max(T_event) - L_eff, matching the offline
        # analytical model (analyze_dataset.sweep_heuristic_all). The wall-clock
        # rate limiter below couples W_h to elapsed wall-clock time instead of
        # L_eff — under paced replay (REPLAY_SPEED >> 1) it caps W_h advance far
        # below the arrival frontier, decoupling W_h from L_eff so the percentile
        # knob has almost no effect and immediate completeness never approaches
        # ~p at high percentiles. Skip rate limiting here so a higher p (larger
        # L_eff) actually pushes the watermark back and raises completeness.
        if _flag_enabled("HEURISTIC_LOCAL_WATERMARK_CLOSE", False):
            self._last_wm_update_time = time.time()
            return self.W_h

        # Rate limiting (hysteresis — bound advance by elapsed wall-clock time)
        if self._last_wm_update_time > 0:
            elapsed = time.time() - self._last_wm_update_time
        else:
            elapsed = time.time() - self.start_time
        max_advance = elapsed * self.wm_max_advance_rate
        added_size = self.tumbling.size if elapsed > 0.1 else 0.0
        self.W_h = min(self.W_h, self.W_h_prev + max_advance + added_size)
        self._last_wm_update_time = time.time()

        return self.W_h

    def _update_L_eff(self) -> None:
        """Cập nhật trạng thái/metric `update L eff` dựa trên dữ liệu mới.
        
        Ghi chú gốc:
        Update L_eff from sketch, with adaptive percentile and threshold.
        """
        # Cold start: use conservative prior until warm
        self.cold_start.update(self.metrics.total_received)
        if not self.cold_start.is_warm:
            self.L_eff_prev = self.L_eff
            self.L_eff = self.cold_start.get_L_eff(
                self.sketch.quantile(self.p_current) if self.sketch.total_count >= 10 else None
            )
            # Estimator drift during cold start
            drift = abs(self.L_eff - self.L_eff_prev) / max(self.L_eff_prev, 0.001)
            self.metrics.estimator_drift = drift
            return

        if self.sketch.total_count < 10:
            return

        # Sketch caching: skip quantile query if sample count hasn't changed
        # significantly since last query (avoids expensive merge across sub-sketches)
        cached_count = getattr(self, "_cached_sketch_count", 0)
        if self.sketch.total_count > 0 and self.sketch.total_count == cached_count:
            return

        self.L_eff_prev = self.L_eff
        ts = HighResTimer.now_ns()
        new_L_eff = self.sketch.quantile(self.p_current)
        self._cached_sketch_count = self.sketch.total_count
        self.metrics.T_sketch_query_ns.append(HighResTimer.now_ns() - ts)

        # Estimator drift: |L_eff(t) - L_eff(t-1)| / L_eff(t-1)
        drift = abs(new_L_eff - self.L_eff) / max(self.L_eff, 0.001)
        self.metrics.estimator_drift = drift

        # Track quantile history for burst detection
        self._quantile_history.append(new_L_eff)
        if len(self._quantile_history) > 60:
            self._quantile_history.pop(0)

        # Hysteresis. In local-watermark-close (sweep) mode the watermark is
        # monotonic AND not wall-clock rate limited, so a transient dip in the
        # sketch quantile (e.g. a brief low-lateness window right after warm-up,
        # or the cold→warm L_eff handover from the conservative L_max prior)
        # would otherwise drop L_eff in one step. That makes W_h = max_event - L_eff
        # jump to the event frontier and, being monotonic, LOCK there permanently —
        # every later out-of-order event is then falsely marked late (the 6%
        # completeness / identical-W_h-across-partitions failure mode).
        #
        # Asymmetric hysteresis fixes it: raise L_eff promptly when lateness grows,
        # but only let it DECAY slowly (<= l_eff_decay_rate per update, never below
        # the true quantile). A short low estimate can no longer collapse L_eff, so
        # W_h cannot overshoot the frontier.
        if _flag_enabled("HEURISTIC_LOCAL_WATERMARK_CLOSE", False):
            if new_L_eff >= self.L_eff:
                if (new_L_eff - self.L_eff) / max(self.L_eff, 0.001) >= self.l_eff_update_threshold:
                    self.L_eff = min(new_L_eff, self.L_max)
            else:
                floor = self.L_eff * (1.0 - self.l_eff_decay_rate)
                self.L_eff = min(self.L_eff, max(new_L_eff, floor))
        elif abs(new_L_eff - self.L_eff) / max(self.L_eff, 0.001) >= self.l_eff_update_threshold:
            self.L_eff = min(new_L_eff, self.L_max)

        # Burst detection
        self._check_burst()

        # Adaptive alpha check (DDSketch Strategy 3)
        self._check_adaptive_alpha()

    def _check_burst(self) -> None:
        # §6.4 feature flag: when disabled, do not switch to p_safe.
        """Kiểm tra điều kiện `check burst` và trả về kết quả đánh giá."""
        if not _flag_enabled("ENABLE_ADAPTIVE_PERCENTILE", True):
            return
        if len(self._quantile_history) < 30:
            return
        recent = self._quantile_history[-1]
        baseline = sorted(self._quantile_history[:-1])[len(self._quantile_history[:-1]) // 2]
        if baseline > 0 and recent / baseline > self.burst_threshold:
            if not self.in_burst:
                self.in_burst = True
                self.burst_start = time.time()
                self.p_current = self.p_safe
                self.metrics.adaptive_percentile_active = True
        elif self.in_burst:
            if time.time() - self.burst_start > self.recovery_minutes * 60:
                self.in_burst = False
                self.p_current = self.p_normal
                self.metrics.adaptive_percentile_active = False

    def _check_adaptive_alpha(self) -> None:
        """Kiểm tra điều kiện `check adaptive alpha` và trả về kết quả đánh giá.
        
        Ghi chú gốc:
        Adjust sketch alpha based on value range (DDSketch Strategy 3).
        
                Narrow range (all lags < 1s): reduce alpha to 0.001 for more precision.
                Wide range (max lag > 30s): restore alpha to default 0.01.
        """
        if self.sketch.total_count < 10:
            return
        now = time.time()
        if now - self._last_alpha_check < 30.0:
            return
        self._last_alpha_check = now

        try:
            p99 = self.sketch.quantile(0.99)
        except Exception:
            return

        if p99 < 1.0 and self._current_alpha > 0.001:
            self.sketch.alpha = 0.001
            self._current_alpha = 0.001
            self.sketch._sketches.clear()
            self.sketch._cached_quantiles.clear()
        elif p99 > 30.0 and self._current_alpha < 0.01:
            self.sketch.alpha = 0.01
            self._current_alpha = 0.01
            self.sketch._sketches.clear()
            self.sketch._cached_quantiles.clear()

    def update_global_watermark(self, W_global_h: float) -> None:
        """Cập nhật trạng thái/metric `update global watermark` dựa trên dữ liệu mới.
        
        Ghi chú gốc:
        Update global watermark copy and proactively close windows.
        """
        self.W_global_h = max(self.W_global_h, W_global_h)
        self._close_windows()

    def _close_windows(self) -> None:
        """Đóng tài nguyên `close windows` và giải phóng trạng thái liên quan.
        
        Ghi chú gốc:
        Close windows where W_h (or W_global_h) >= window_end.
        """
        # Skip window closing during cold start Phase 0
        if self.cold_start.phase.name == "PHASE_0":
            return

        # Normally windows close on the GLOBAL watermark (min across all
        # partitions) for cross-partition consistency when emitting results.
        # For the completeness-vs-wait sweep (topic #112) that global min is
        # dragged down by the slowest/least-warmed partition, so it lags the
        # arrival frontier by far more than L_eff — windows never close in time,
        # every event lands on_time, and completeness is a flat 100% at every
        # percentile. HEURISTIC_LOCAL_WATERMARK_CLOSE makes window closing use
        # the LOCAL per-partition watermark (max_event_time - L_eff), matching
        # the offline analytical model (analyze_dataset.py) so the percentile
        # knob actually trades immediate completeness against wait time.
        if _flag_enabled("HEURISTIC_LOCAL_WATERMARK_CLOSE", False):
            w_limit = self.W_h
        else:
            w_limit = self.W_global_h if self.W_global_h > float("-inf") else self.W_h
        to_close = [
            w for w in list(self.open_windows)
            if w + self.tumbling.size <= w_limit
        ]
        for w in sorted(to_close):
            agg = self.open_windows.pop(w, None)
            if agg is None:
                continue
            self._delete_open_window(w)
            win_id = self.tumbling.window_id(self.partition_id, w)
            result = WindowResult(
                window_id=win_id,
                partition_id=self.partition_id,
                window_start=w,
                window_end=w + self.tumbling.size,
                count=agg.count,
                status_500=agg.status_500,
                is_speculative=True,
                version=1,
            )
            self.closed_windows[w] = result
            self._window_closed_at[w] = time.time()
            self._persist_closed_window(w, result)
            self._pending_results.append(result)

    def get_expired_windows(self, age_s: float) -> list:
        """Trả về thông tin `expired windows` từ trạng thái hiện tại.
        
        Ghi chú gốc:
        Return closed windows where window_end < now - age_s as (window_id, count, emitted_at) tuples.
        
                Used by DownstreamEmitter.schedule_final_reconciliation() for 24h FINAL checks.
        """
        now = time.time()
        cutoff = now - age_s
        expired = []
        for w, result in list(self.closed_windows.items()):
            if result.window_end < cutoff:
                emitted_at = self._window_closed_at.get(w, 0.0)
                expired.append((result.window_id, result.count, emitted_at))
        return expired

    def drain_results(self) -> list:
        """Rút dữ liệu đang chờ trong `drain results` để xử lý tiếp.
        
        Ghi chú gốc:
        Return and clear pending results accumulated from closed windows.
        
                Called by the Kafka producer loop to emit WindowResults to the
                ``heuristic_results`` topic.
        """
        results = self._pending_results[:]
        self._pending_results.clear()
        return results

    def _purge_old_closed_windows(self) -> None:
        """Loại bỏ dữ liệu `purge old closed windows` đã hết hạn hoặc không còn cần thiết.
        
        Ghi chú gốc:
        Periodically delete closed windows older than 1 hour from RocksDB.
        
                Called from process() every 300s to prevent unbounded storage growth.
        """
        if self._store is None:
            return
        now = time.time()
        if now - self._last_closed_purge < 300.0:
            return
        self._last_closed_purge = now
        cutoff = now - 3600.0
        purged = 0
        for key, val in list(self._store.items(prefix=_PFX_CLOSED)):
            if isinstance(val, dict):
                window_end = val.get("window_end", 0)
            elif hasattr(val, "window_end"):
                window_end = val.window_end
            else:
                continue
            if window_end < cutoff:
                self._store.delete(key)
                purged += 1
        if purged > 0 and isinstance(self._store, object):
            pass  # deletions are immediate in RocksStore, no flush needed per key

    def _purge_seen_ids(self) -> None:
        """Loại bỏ dữ liệu `purge seen ids` đã hết hạn hoặc không còn cần thiết.
        
        Ghi chú gốc:
        TTL sweep on the in-memory dedup set + periodic RocksDB compaction.
        
                Spec §6.5: idempotent filter only needs to cover recent late arrivals
                (here `_dedup_ttl_s` = 60s). Drop expired entries from memory every
                call; clear-and-repopulate RocksDB every 5 minutes so SSTs compact.
        """
        now = time.time()
        cutoff = now - self._dedup_ttl_s
        expired = [eid for eid, ts in list(self.seen_ids.items()) if ts < cutoff]
        for eid in expired:
            del self.seen_ids[eid]
            if self._store is not None:
                self._store.delete(f"{_PFX_SEEN}{eid}")

        if self._store is None:
            return
        if now - self._last_dedup_sweep < 300.0:
            return
        self._last_dedup_sweep = now
        self._store.clear_prefix(_PFX_SEEN)
        for eid, ts in list(self.seen_ids.items()):
            self._store.put(f"{_PFX_SEEN}{eid}", ts)

    # ---- Event processing ----
    def is_idle(self) -> bool:
        """Kiểm tra điều kiện `is idle` và trả về boolean."""
        return time.time() - self.last_event_time > 2.0

    def process(self, event: LogEvent, arrival_time: float = None) -> Optional[float]:
        # Backpressure: drop if inbound queue is full (spec §5.11).
        """Hàm `process` thực hiện phần xử lý liên quan đến process của `HeuristicWatermarkEngine`."""
        if len(self._inbound_queue) >= self._queue_maxsize:
            self._backpressure_drops += 1
            return None
        self._inbound_queue.append(event)
        # Dequeue the oldest event for processing (FIFO order).
        event = self._inbound_queue.pop(0)

        t0 = HighResTimer.now_ns()
        self.metrics.total_received += 1

        if arrival_time is None:
            arrival_time = getattr(event, "arrival_time", None) or time.time()

        if arrival_time > 0:
            poll_received_at = getattr(event, "poll_received_at", 0.0)
            if poll_received_at > 0:
                self.metrics.T_network_ingest_ns.append(
                    (poll_received_at - event.event_time) * 1_000_000_000
                )
                self.metrics.T_poll_decode_ns.append(
                    (arrival_time - poll_received_at) * 1_000_000_000
                )
            else:
                self.metrics.T_network_ingest_ns.append(
                    (arrival_time - event.event_time) * 1_000_000_000
                )

        # Dedup (TTL-bounded — §6.5)
        t_dedup = HighResTimer.now_ns()
        if event.event_id in self.seen_ids:
            self.metrics.duplicates += 1
            self.metrics.T_deduplication_ns.append(HighResTimer.now_ns() - t_dedup)
            return None
        self.seen_ids[event.event_id] = arrival_time
        self._persist_seen_id(event.event_id)

        # Periodic RocksDB dedup compaction
        self._purge_seen_ids()
        self.metrics.T_deduplication_ns.append(HighResTimer.now_ns() - t_dedup)

        # Periodic closed-window TTL purge (Gap 5)
        self._purge_old_closed_windows()

        et = event.event_time

        # Future-skew guard: an event timestamped implausibly far in the FUTURE
        # relative to when it arrived (et - arrival_time > max_future_skew_s, i.e.
        # a large negative lag) cannot be a real observation — e.g. a malformed
        # message whose event_time defaulted to wall-clock, or a corrupt row. If
        # allowed to advance max_event_time it would jam W_h = max_event_time - L_eff
        # ahead of every genuine event (mass false-late) and trip BOO fallback.
        # Treat it as an anomaly: route to DLQ and return WITHOUT touching
        # max_event_time / sketch / watermark / windows.
        if (et - arrival_time) > self.max_future_skew_s:
            self.extreme_lag_count += 1
            self.metrics.late_dropped += 1
            self.late_events.append({
                "event_id": event.event_id,
                "T_event": et,
                "arrival_time": arrival_time,
                "lag": arrival_time - et,
                "W_h_at_arrival": self.W_h,
                "lateness": 0.0,
                "partition_id": self.partition_id,
                "original_status": event.status,
                "worker_id": self.worker_id,
                "payload": getattr(event, "payload", None),
                "future_skew": True,
            })
            self.metrics.dlq_backlog = len(self.late_events)
            return HighResTimer.now_ns() - t0

        self.max_event_time = max(self.max_event_time, et)
        self.last_event_time = arrival_time

        # Compute lag
        lag = arrival_time - et

        # Negative lag handling (spec §7). §6.4 feature flag: when
        # recalibration is disabled, observation/tier evaluation still runs
        # (for metrics and BOO fallback) but the median-offset correction is
        # skipped — original lag goes into the sketch unmodified.
        self.neg_lag.observe(lag)
        tier = self.neg_lag.evaluate()

        # Tier 2 (0.1%-1%): log warning per spec §7.2
        if tier == LagTier.WARNING:
            neg_count = sum(1 for _, is_neg in list(self.neg_lag._observations) if is_neg)
            logger.warning(
                "Negative lag rate in warning range (Tier 2, 0.1%%-1%%): "
                "rate=%.4f (%d negative / %d total observations)",
                self.neg_lag.rate, neg_count, len(self.neg_lag._observations),
            )

        if _flag_enabled("ENABLE_NEGATIVE_LAG_RECALIBRATION", True):
            adjusted_lag = self.neg_lag.adjust_lag(lag)
        else:
            adjusted_lag = lag
        self.metrics.negative_lag_rate = self.neg_lag.rate

        # Extreme lag check (spec §5.5): lag > max_lag_accepted -> skip sketch, route to DLQ
        _extreme_lag = lag > self.max_lag_accepted

        # Replay detection (spec §11.3). Replay mode (and its sketch rollback)
        # is a production live-stream feature: it stops feeding the DDSketch
        # while a "replay burst" is suspected. In the analytical sweep mode
        # (HEURISTIC_LOCAL_WATERMARK_CLOSE) it is actively harmful — once
        # baseline_lag collapses, the replay threshold (10 × baseline_lag) gets
        # tiny so ordinary lateness is perpetually misread as replay, the sketch
        # FREEZES (sketch_total_count stuck), L_eff sticks at the warm-up value
        # and the per-partition watermark breaks (the 3-8% completeness / frozen
        # sketch failure mode). The sweep wants a stable percentile estimate, so
        # keep feeding the sketch and skip replay detection entirely.
        sweep_mode = _flag_enabled("HEURISTIC_LOCAL_WATERMARK_CLOSE", False)
        if (not sweep_mode) and self.detect_replay(lag):
            pass  # skip sketch update during replay
        elif self.neg_lag.should_degrade_to_boo():
            pass  # skip sketch update during BOO fallback
        elif _extreme_lag:
            pass  # skip sketch update for extreme lag (treat as anomaly)
        else:
            # Update sketch with adjusted lag
            ts = HighResTimer.now_ns()
            self.sketch.add(adjusted_lag, arrival_time)
            self.metrics.T_sketch_update_ns.append(HighResTimer.now_ns() - ts)

        # Update L_eff and watermark
        self._update_L_eff()
        self._compute_watermark()
        self._close_windows()

        # Check replay exit
        self.check_exit_replay()

        # Check BOO recovery
        if self.neg_lag.check_recovery():
            self.L_eff = min(self.sketch.quantile(self.p_current), self.L_max)

        # Route event to window or DLQ
        ws = self.tumbling.window_start(et)

        # Per-window loss tracking
        win_id = self.tumbling.window_id(self.partition_id, ws)
        if win_id not in self._window_loss:
            self._window_loss[win_id] = {"late": 0, "total": 0}

        if _extreme_lag:
            # Extreme lag (spec §5.5): skip window, route directly to DLQ
            self.extreme_lag_count += 1
            self.metrics.late_dropped += 1
            self.late_events.append({
                "event_id": event.event_id,
                "T_event": et,
                "arrival_time": arrival_time,
                "lag": lag,
                "W_h_at_arrival": self.W_h,
                "lateness": lag,
                "partition_id": self.partition_id,
                "original_status": event.status,
                "worker_id": self.worker_id,
                "payload": event.payload,
                "extreme_lag": True,
            })
        else:
            self._window_loss[win_id]["total"] += 1

            if ws in self.closed_windows:
                self.metrics.late_dropped += 1
                self._window_loss[win_id]["late"] += 1
                self.late_events.append({
                    "event_id": event.event_id,
                    "T_event": et,
                    "arrival_time": arrival_time,
                    "lag": lag,
                    "W_h_at_arrival": self.W_h,
                    "lateness": self.W_h - et if et < self.W_h else 0.0,
                    "partition_id": self.partition_id,
                    "original_status": event.status,
                    "worker_id": self.worker_id,
                    "payload": event.payload,
                })
            elif ws in self.open_windows:
                agg = self.open_windows[ws]
                agg.count += 1
                if event.status == 500:
                    agg.status_500 += 1
                t_state = HighResTimer.now_ns()
                self._persist_open_window(ws)
                self.metrics.T_state_write_ns.append(HighResTimer.now_ns() - t_state)
                self.metrics.on_time += 1
            else:
                self.open_windows[ws] = WindowAggregate(
                    count=1,
                    status_500=1 if event.status == 500 else 0,
                )
                t_state = HighResTimer.now_ns()
                self._persist_open_window(ws)
                self.metrics.T_state_write_ns.append(HighResTimer.now_ns() - t_state)
                self.metrics.on_time += 1

        # Periodic snapshot
        if time.time() - self._last_snapshot >= self.snapshot_interval:
            self._take_snapshot()
            self._last_snapshot = time.time()

        lat_ns = HighResTimer.now_ns() - t0
        self.proc_latencies_ns.append(lat_ns)
        if len(self.proc_latencies_ns) > 10000:
            self.proc_latencies_ns.pop(0)

        # Update metrics
        self.metrics.sketch_total_count = self.sketch.total_count
        if self.sketch.total_count > 0:
            self.metrics.sketch_quantile_p50_ms = self.sketch.quantile(0.50) * 1000
            self.metrics.sketch_quantile_p95_ms = self.sketch.quantile(0.95) * 1000
            self.metrics.sketch_quantile_p99_ms = self.sketch.quantile(0.99) * 1000
        self.metrics.dlq_backlog = len(self.late_events)
        self.metrics.replay_mode_active = self.in_replay_mode

        # Per-window loss samples
        wl_snapshot = list(self._window_loss.items())
        self.metrics.per_window_loss_samples = [
            {"window_id": wid, "late": wl["late"], "total": wl["total"]}
            for wid, wl in wl_snapshot
        ]

        return lat_ns

    # ---- Snapshot + Rollback ----
    def _take_snapshot(self) -> None:
        """Hàm `_take_snapshot` thực hiện phần xử lý liên quan đến take snapshot của `HeuristicWatermarkEngine`.
        """
        snap = (time.time(), self.sketch.to_dict())
        self._snapshots.append(snap)
        if len(self._snapshots) > self.snapshot_count:
            self._snapshots.pop(0)

    def rollback_to(self, target_time: float) -> bool:
        """Hàm `rollback_to` thực hiện phần xử lý liên quan đến rollback to của `HeuristicWatermarkEngine`."""
        for ts, snap_dict in reversed(self._snapshots):
            if ts <= target_time:
                self.sketch = SlidingWindowDDSketch.from_dict(snap_dict)
                return True
        return False

    # ---- Replay mode ----
    def detect_replay(self, lag: float) -> bool:
        """Hàm `detect_replay` thực hiện phần xử lý liên quan đến detect replay của `HeuristicWatermarkEngine`."""
        if self.baseline_lag > 0 and lag > self.baseline_multiplier * self.baseline_lag:
            was_already_in_replay = self.in_replay_mode
            self.in_replay_mode = True
            self.metrics.replay_mode_active = True
            if not was_already_in_replay:
                self._replay_start_time = time.time()
            rollback_target = time.time() - 5.0
            self.rollback_to(rollback_target)
            return True
        return False

    def check_exit_replay(self) -> None:
        """Kiểm tra điều kiện `check exit replay` và trả về kết quả đánh giá."""
        if not self.in_replay_mode:
            return
        # Enforce 30s wall-clock minimum for replay mode (§8.3, §11.3)
        replay_elapsed = time.time() - self._replay_start_time
        if replay_elapsed < 30.0:
            return
        recent_lag = self.L_eff
        if recent_lag < self.exit_multiplier * self.baseline_lag:
            now = time.time()
            if self._replay_stable_since == 0.0:
                self._replay_stable_since = now
            elif now - self._replay_stable_since >= self.exit_streak_seconds:
                self.in_replay_mode = False
                self.metrics.replay_mode_active = False
                self._replay_stable_since = 0.0
                self.baseline_lag = recent_lag
        else:
            self._replay_stable_since = 0.0

    def flush(self) -> None:
        """Flush dữ liệu đệm của `flush` xuống đích lưu trữ hoặc downstream."""
        for w in sorted(self.open_windows):
            agg = self.open_windows[w]
            result = WindowResult(
                window_id=self.tumbling.window_id(self.partition_id, w),
                partition_id=self.partition_id,
                window_start=w,
                window_end=w + self.tumbling.size,
                count=agg.count,
                status_500=agg.status_500,
                is_speculative=True,
                version=1,
            )
            self.closed_windows[w] = result
            self._window_closed_at[w] = result.window_end
            self._persist_closed_window(w, result)
            self._pending_results.append(result)

        # Batch-delete all open windows from RocksDB
        if self._store is not None:
            self._store.clear_prefix(_PFX_OPEN)
            self._store.flush()
        self.open_windows.clear()

    def close(self) -> None:
        """Đóng tài nguyên `close` và giải phóng trạng thái liên quan.
        
        Ghi chú gốc:
        Close the RocksDB store if open.
        """
        if self._store is not None:
            self._store.close()
            self._store = None

    @property
    def queue_size(self) -> int:
        """Hàm `queue_size` thực hiện phần xử lý liên quan đến queue size của `HeuristicWatermarkEngine`.
        
        Ghi chú gốc:
        Current depth of the inbound backpressure queue (spec §5.11).
        """
        return len(self._inbound_queue)

    def get_seek_offset(self) -> int:
        """Trả về thông tin `seek offset` từ trạng thái hiện tại.
        
        Ghi chú gốc:
        Return Kafka seek offset (offset + 1 per spec §8.3).
        """
        return self.kafka_seek_offset + 1

    def summary(self) -> dict:
        """Tạo bản tóm tắt trạng thái `summary` để trả về API hoặc báo cáo."""
        closed_count = (
            self._store.count(prefix=_PFX_CLOSED)
            if self._store is not None
            else len(self.closed_windows)
        )
        # Per-window loss accounting (top 20 by loss rate)
        wl_items = list(self._window_loss.items())
        per_window_loss = sorted(
            [
                {
                    "window_id": wid,
                    "late": wl["late"],
                    "total": wl["total"],
                    "loss_rate": round(wl["late"] / max(wl["total"], 1), 4),
                }
                for wid, wl in wl_items
            ],
            key=lambda x: x["loss_rate"],
            reverse=True,
        )[:20]

        wl_values = list(self._window_loss.values())
        total_late = sum(wl["late"] for wl in wl_values)
        total_events = sum(wl["total"] for wl in wl_values)
        overall_loss_rate = round(total_late / max(total_events, 1), 4)

        latencies = self.proc_latencies_ns
        p50 = 0.0
        p95 = 0.0
        p99 = 0.0
        if latencies:
            sorted_l = sorted(latencies)
            n = len(sorted_l)
            p50 = sorted_l[int(n * 0.5)] / 1000.0
            p95 = sorted_l[int(n * 0.95)] / 1000.0
            p99 = sorted_l[int(n * 0.99)] / 1000.0

        def _lat_us(values: list[float], percentile: float) -> float:
            """Hàm `_lat_us` thực hiện phần xử lý liên quan đến lat us của `HeuristicWatermarkEngine`."""
            values = [v for v in values if v >= 0]
            if not values:
                return 0.0
            sorted_v = sorted(values)
            idx = min(int(len(sorted_v) * percentile), len(sorted_v) - 1)
            return round(sorted_v[idx] / 1000.0, 2)

        base = self.metrics.summary()
        base.update({
            "mode": "heuristic",
            "watermark": self.W_h,
            "L_eff_s": round(self.L_eff, 3),
            "p_current": self.p_current,
            "sketch_total_count": self.sketch.total_count,
            "sketch_samples": self.sketch.total_count,
            "adaptive_active": self.metrics.adaptive_percentile_active,
            "replay_mode": self.in_replay_mode,
            "open_windows": len(self.open_windows),
            "closed_windows": closed_count,
            "dlq_backlog": len(self.late_events),
            "extreme_lag_count": self.extreme_lag_count,
            "cold_start": self.cold_start.status(),
            "negative_lag": self.neg_lag.status(),
            "per_window_loss": per_window_loss,
            "overall_loss_rate": overall_loss_rate,
            "proc_latency_p50_us": round(p50, 2),
            "proc_latency_p95_us": round(p95, 2),
            "proc_latency_p99_us": round(p99, 2),
            "sketch_update_latency_p50_us": _lat_us(self.metrics.T_sketch_update_ns, 0.50),
            "sketch_update_latency_p95_us": _lat_us(self.metrics.T_sketch_update_ns, 0.95),
            "sketch_update_latency_p99_us": _lat_us(self.metrics.T_sketch_update_ns, 0.99),
            "sketch_query_latency_p50_us": _lat_us(self.metrics.T_sketch_query_ns, 0.50),
            "sketch_query_latency_p95_us": _lat_us(self.metrics.T_sketch_query_ns, 0.95),
            "sketch_query_latency_p99_us": _lat_us(self.metrics.T_sketch_query_ns, 0.99),
            # Gap 3 metrics
            "active_partitions": getattr(self, "active_partitions", 1),
            "clock_skew_ms": getattr(self, "clock_skew_ms", 0.0),
            "punctuation_total": self.punctuation_total,
            "eviction_state": 0,  # no tiered eviction in heuristic mode
            "window_id": f"w_{self.partition_id}_none",
            "ingestor_id": f"ingestor_{self.partition_id}",
            "ingestor_health_rtt_ms": 0.0,
            "ingestor_network_rtt_seconds": 0.0,
        })
        return base
