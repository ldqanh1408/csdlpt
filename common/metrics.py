"""
Thu thập metric hệ thống và đo thời gian xử lý độ phân giải cao.

`HighResTimer` đo latency; `SystemMetrics` lưu số event, late event, cửa sổ đã đóng, queue depth và các chỉ số cho dashboard/report.
"""

import time
from dataclasses import dataclass, field


class HighResTimer:
    """Lớp `HighResTimer` gom dữ liệu và hành vi liên quan đến HighResTimer."""
    @staticmethod
    def now_ns() -> int:
        """Hàm `now_ns` thực hiện phần xử lý liên quan đến now ns của `HighResTimer`."""
        return time.perf_counter_ns()

    @staticmethod
    def elapsed_ms(start_ns: int) -> float:
        """Hàm `elapsed_ms` thực hiện phần xử lý liên quan đến elapsed ms của `HighResTimer`."""
        return (time.perf_counter_ns() - start_ns) / 1_000_000.0


@dataclass
class SystemMetrics:
    """Lớp `SystemMetrics` gom dữ liệu và hành vi liên quan đến SystemMetrics."""
    T_network_ingest_ns: list[float] = field(default_factory=list)
    T_poll_decode_ns: list[float] = field(default_factory=list)
    T_deduplication_ns: list[float] = field(default_factory=list)
    T_state_write_ns: list[float] = field(default_factory=list)
    T_sketch_update_ns: list[float] = field(default_factory=list)
    T_sketch_query_ns: list[float] = field(default_factory=list)

    watermark_lag_s: float = 0.0
    node_skew_ms: float = 0.0

    total_received: int = 0
    on_time: int = 0
    late_dropped: int = 0
    duplicates: int = 0
    backpressure_drops: int = 0

    sketch_total_count: int = 0
    sketch_quantile_p50_ms: float = 0.0
    sketch_quantile_p95_ms: float = 0.0
    sketch_quantile_p99_ms: float = 0.0
    negative_lag_rate: float = 0.0
    estimator_drift: float = 0.0
    non_monotonic_punctuation: int = 0
    dlq_backlog: int = 0
    replay_mode_active: bool = False
    adaptive_percentile_active: bool = False

    # Spec gap features
    node_skew_s: float = 0.0
    idleness_detected: bool = False
    last_event_arrival: float = 0.0
    idleness_duration_s: float = 0.0
    watermark_lag_s: float = 0.0
    fencing_token_violations: int = 0
    per_window_loss_samples: list[dict] = field(default_factory=list)
    sub_checkpoint_count: int = 0

    def data_completeness(self) -> float:
        """Hàm `data_completeness` thực hiện phần xử lý liên quan đến data completeness của `SystemMetrics`."""
        unique = max(self.total_received - self.duplicates, 1)
        return 100.0 * self.on_time / unique

    def late_arrival_rate(self) -> float:
        """Hàm `late_arrival_rate` thực hiện phần xử lý liên quan đến late arrival rate của `SystemMetrics`."""
        total = max(self.total_received, 1)
        return 100.0 * self.late_dropped / total

    @staticmethod
    def _latency_us(values: list[float], percentile: float) -> float:
        """Hàm `_latency_us` thực hiện phần xử lý liên quan đến latency us của `SystemMetrics`."""
        values = [v for v in values if v >= 0]
        if not values:
            return 0.0
        sorted_v = sorted(values)
        idx = min(int(len(sorted_v) * percentile), len(sorted_v) - 1)
        return round(sorted_v[idx] / 1000.0, 2)

    def latency_summary(self) -> dict:
        """Tạo bản tóm tắt trạng thái `latency summary` để trả về API hoặc báo cáo."""
        stages = {
            "network_ingest": self.T_network_ingest_ns,
            "poll_decode": self.T_poll_decode_ns,
            "dedup": self.T_deduplication_ns,
            "state_write": self.T_state_write_ns,
            "sketch_update": self.T_sketch_update_ns,
            "sketch_query": self.T_sketch_query_ns,
        }
        result = {}
        for name, values in stages.items():
            result[f"{name}_latency_p50_us"] = self._latency_us(values, 0.50)
            result[f"{name}_latency_p95_us"] = self._latency_us(values, 0.95)
            result[f"{name}_latency_p99_us"] = self._latency_us(values, 0.99)
        return result

    def summary(self) -> dict:
        """Tạo bản tóm tắt trạng thái `summary` để trả về API hoặc báo cáo."""
        result = {
            "total_received": self.total_received,
            "on_time": self.on_time,
            "late_dropped": self.late_dropped,
            "duplicates": self.duplicates,
            "backpressure_drops": self.backpressure_drops,
            "data_completeness_pct": round(self.data_completeness(), 3),
            "late_arrival_rate_pct": round(self.late_arrival_rate(), 3),
            "watermark_lag_s": round(self.watermark_lag_s, 3),
            "node_skew_ms": round(self.node_skew_ms, 3),
            "sketch_total_count": self.sketch_total_count,
            "sketch_quantile_p50_ms": round(self.sketch_quantile_p50_ms, 3),
            "sketch_quantile_p95_ms": round(self.sketch_quantile_p95_ms, 3),
            "sketch_quantile_p99_ms": round(self.sketch_quantile_p99_ms, 3),
            "negative_lag_rate": round(self.negative_lag_rate, 5),
            "estimator_drift": round(self.estimator_drift, 5),
            "non_monotonic_punctuation": self.non_monotonic_punctuation,
            "dlq_backlog": self.dlq_backlog,
            "replay_mode_active": self.replay_mode_active,
            "adaptive_percentile_active": self.adaptive_percentile_active,
            "node_skew_s": round(self.node_skew_s, 3),
            "idleness_detected": self.idleness_detected,
            "idleness_duration_s": round(self.idleness_duration_s, 3),
            "watermark_lag_s": round(self.watermark_lag_s, 3),
            "fencing_token_violations": self.fencing_token_violations,
        }
        result.update(self.latency_summary())
        return result
