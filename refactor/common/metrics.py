"""High-resolution profiling and system metrics collection."""

import time
from dataclasses import dataclass, field


class HighResTimer:
    @staticmethod
    def now_ns() -> int:
        return time.perf_counter_ns()

    @staticmethod
    def elapsed_ms(start_ns: int) -> float:
        return (time.perf_counter_ns() - start_ns) / 1_000_000.0


@dataclass
class SystemMetrics:
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
        unique = max(self.total_received - self.duplicates, 1)
        return 100.0 * self.on_time / unique

    def late_arrival_rate(self) -> float:
        total = max(self.total_received, 1)
        return 100.0 * self.late_dropped / total

    def summary(self) -> dict:
        return {
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
