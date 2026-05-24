"""Central Prometheus metrics registry for the stream processor.

Provides a MonitoringManager that wraps prometheus_client metrics (Counter,
Gauge, Histogram) and exposes helpers to update them from coordinator broadcasts
and per-engine summaries.  Metrics follow Prometheus naming conventions:
snake_case, _total suffix for counters, _seconds for time-valued gauges
(where appropriate).
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    generate_latest,
    CollectorRegistry,
    REGISTRY,
)


# ---------------------------------------------------------------------------
# Alert rule definition (used by alerting module, defined here for cohesion)
# ---------------------------------------------------------------------------

@dataclass
class AlertRule:
    name: str
    description: str
    severity: str  # critical | warning | high
    condition: str  # human-readable expression
    evaluate: callable  # (metrics_snapshot: dict) -> bool


# ---------------------------------------------------------------------------
# MonitoringManager
# ---------------------------------------------------------------------------

class MonitoringManager:
    """Central Prometheus metrics registry for the stream processor.

    Parameters
    ----------
    registry : CollectorRegistry | None
        If None, uses the default global registry.
    """

    def __init__(self, registry: CollectorRegistry | None = None):
        _reg = registry or REGISTRY

        # ---- Watermark gauges ----
        self.watermark_global = Gauge(
            "csdlpt_watermark_global",
            "Global watermark (seconds since epoch)",
            ["mode"],
            registry=_reg,
        )
        self.watermark_local = Gauge(
            "csdlpt_watermark_local",
            "Local watermark per partition (seconds since epoch)",
            ["worker_id", "partition_id"],
            registry=_reg,
        )
        self.watermark_lag_s = Gauge(
            "csdlpt_watermark_lag_seconds",
            "Watermark lag behind wall clock",
            ["mode"],
            registry=_reg,
        )
        self.node_skew_ms = Gauge(
            "csdlpt_node_skew_ms",
            "Inter-node watermark skew in milliseconds",
            ["mode"],
            registry=_reg,
        )

        # ---- Event counters ----
        self.events_total = Counter(
            "csdlpt_events_total",
            "Total events processed",
            ["worker_id", "partition_id", "status"],
            registry=_reg,
        )
        self.events_late = Counter(
            "csdlpt_events_late_total",
            "Late events dropped",
            ["worker_id", "partition_id"],
            registry=_reg,
        )
        self.events_duplicate = Counter(
            "csdlpt_events_duplicate_total",
            "Duplicate events filtered",
            ["worker_id", "partition_id"],
            registry=_reg,
        )

        # ---- Processing latency histogram ----
        self.processing_latency = Histogram(
            "csdlpt_processing_latency_ns",
            "Event processing latency in nanoseconds",
            ["worker_id"],
            registry=_reg,
        )

        # ---- Sketch lag histogram ----
        self.sketch_lag = Histogram(
            "csdlpt_sketch_lag_seconds",
            "Estimated event lag from DDSketch",
            ["worker_id"],
            buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60],
            registry=_reg,
        )

        # ---- Health gauges ----
        _status_map = {"Healthy": 0, "Degraded": 1, "Warning": 2, "Critical": 3}
        self._status_map = _status_map
        self.combined_status = Gauge(
            "csdlpt_combined_status",
            "Combined health status: 0=Healthy,1=Degraded,2=Warning,3=Critical",
            registry=_reg,
        )
        self.backpressure_active = Gauge(
            "csdlpt_backpressure_active",
            "Backpressure active per partition (1=active, 0=inactive)",
            ["worker_id", "partition_id"],
            registry=_reg,
        )
        self.fencing_violations = Counter(
            "csdlpt_fencing_violations_total",
            "Fencing token violations",
            registry=_reg,
        )

        # ---- RocksDB storage gauge ----
        self.rocksdb_size_bytes = Gauge(
            "csdlpt_rocksdb_size_bytes",
            "RocksDB storage size in bytes",
            ["component"],
            registry=_reg,
        )

        # ---- Tiered storage gauges ----
        self.tiered_storage_objects = Gauge(
            "csdlpt_tiered_storage_objects",
            "Objects in tiered storage",
            ["tier"],
            registry=_reg,
        )
        self.tiered_storage_bytes = Gauge(
            "csdlpt_tiered_storage_bytes",
            "Bytes in tiered storage",
            ["tier"],
            registry=_reg,
        )

        # ---- Kafka partition lag ----
        self.kafka_partition_lag = Gauge(
            "csdlpt_kafka_partition_lag",
            "Kafka partition lag (next_offset - committed_offset)",
            ["topic", "group_id", "client_id", "partition_id"],
            registry=_reg,
        )

        # ---- Ingestion health ----
        self.ingestor_silent = Gauge(
            "csdlpt_ingestor_silent",
            "Number of ingestors currently silent",
            registry=_reg,
        )
        self.ingestor_stuck = Gauge(
            "csdlpt_ingestor_stuck",
            "Number of ingestors currently stuck",
            registry=_reg,
        )

        # ---- DLQ backlog (spec §12, heuristic mode) ----
        self.dlq_backlog = Gauge(
            "csdlpt_dlq_backlog",
            "Messages pending in the DLQ pipeline",
            ["worker_id"],
            registry=_reg,
        )

        # ---- §10.3 Heuristic-specific required metrics ----
        self.late_arrival_rate_pct = Gauge(
            "csdlpt_late_arrival_rate_pct",
            "Percentage of late-arriving events (T_event < W_global_h)",
            ["worker_id", "partition_id"],
            registry=_reg,
        )
        self.sketch_total_count = Gauge(
            "csdlpt_sketch_total_count",
            "Total samples in the sliding DDSketch window",
            ["worker_id"],
            registry=_reg,
        )
        self.sketch_quantile_ms = Gauge(
            "csdlpt_sketch_quantile_ms",
            "DDSketch lag quantile value in milliseconds",
            ["worker_id", "quantile"],
            registry=_reg,
        )
        self.estimator_drift = Gauge(
            "csdlpt_estimator_drift",
            "Relative L_eff estimator drift |L_eff(t)-L_eff(t-1)| / L_eff(t-1)",
            ["worker_id"],
            registry=_reg,
        )
        self.negative_lag_rate = Gauge(
            "csdlpt_negative_lag_rate",
            "Rate of events with negative lag (T_event > now)",
            ["worker_id"],
            registry=_reg,
        )
        self.replay_mode_active = Gauge(
            "csdlpt_replay_mode_active",
            "1 if worker partition is in replay mode, 0 otherwise",
            ["worker_id", "partition_id"],
            registry=_reg,
        )
        self.adaptive_percentile_active = Gauge(
            "csdlpt_adaptive_percentile_active",
            "1 if adaptive p=0.999 is active (burst mode), 0 if p=0.99 (normal)",
            ["worker_id"],
            registry=_reg,
        )

        # Snapshot cache for alert evaluation
        self._snapshot: dict = {}

    # ------------------------------------------------------------------
    # Public update helpers
    # ------------------------------------------------------------------

    def update_from_coordinator(self, broadcast: dict) -> None:
        """Update metrics from a coordinator broadcast dict.

        Expected keys (from StrictCoordinator.broadcast):
          W_global, term, timestamp, partition_count, active_workers,
          fencing_violations, node_skew_max_ms, watermark_lag_s,
          skew_status, lag_status, combined_status
        """
        mode = broadcast.get("mode", "strict")
        wg = broadcast.get("W_global", float("-inf"))
        lag_s = broadcast.get("watermark_lag_s", 0.0)
        skew_ms = broadcast.get("node_skew_max_ms", 0.0)
        status_str = broadcast.get("combined_status", "Healthy")
        fencing = broadcast.get("fencing_violations", 0)

        if wg > float("-inf"):
            self.watermark_global.labels(mode=mode).set(wg)
        self.watermark_lag_s.labels(mode=mode).set(max(lag_s, 0.0))
        self.node_skew_ms.labels(mode=mode).set(skew_ms)

        status_val = self._status_map.get(status_str, 0)
        self.combined_status.set(status_val)

        # Increment fencing violations counter by diff
        prev = self._snapshot.get("_fencing_prev", 0)
        delta = max(0, fencing - prev)
        if delta > 0:
            self.fencing_violations.inc(delta)
        self._snapshot["_fencing_prev"] = fencing

    def update_from_engine(self, engine_summary: dict, worker_id: str, partition_id: int) -> None:
        """Update metrics from an engine summary dict.

        Expected keys (from SystemMetrics.summary / engine.summary):
          total_received, on_time, late_dropped, duplicates, backpressure_drops,
          watermark_lag_s, node_skew_ms, sketch_total_count,
          sketch_quantile_p50_ms, sketch_quantile_p95_ms, sketch_quantile_p99_ms,
          dlq_backlog, replay_mode_active, adaptive_percentile_active,
          fencing_token_violations, data_completeness_pct, late_arrival_rate_pct,
          non_monotonic_punctuation, idleness_detected
        """
        pid = str(partition_id)

        # Counters are cumulative; use the values from summary directly
        total = engine_summary.get("total_received", 0)
        on_time = engine_summary.get("on_time", 0)
        late = engine_summary.get("late_dropped", 0)
        dup = engine_summary.get("duplicates", 0)

        # Track previous values to emit deltas
        pkey_total = f"events_total_{worker_id}_{pid}"
        pkey_late = f"events_late_{worker_id}_{pid}"
        pkey_dup = f"events_dup_{worker_id}_{pid}"

        prev_total = self._snapshot.get(pkey_total, 0)
        prev_late = self._snapshot.get(pkey_late, 0)
        prev_dup = self._snapshot.get(pkey_dup, 0)

        if total > prev_total:
            self.events_total.labels(
                worker_id=worker_id, partition_id=pid, status="on_time"
            ).inc(total - prev_total)
        if late > prev_late:
            self.events_late.labels(
                worker_id=worker_id, partition_id=pid
            ).inc(late - prev_late)
        if dup > prev_dup:
            self.events_duplicate.labels(
                worker_id=worker_id, partition_id=pid
            ).inc(dup - prev_dup)

        self._snapshot[pkey_total] = total
        self._snapshot[pkey_late] = late
        self._snapshot[pkey_dup] = dup

        # Watermark
        wm = engine_summary.get("watermark", float("-inf"))
        if wm > float("-inf"):
            self.watermark_local.labels(worker_id=worker_id, partition_id=pid).set(wm)

        # Lag -- use the engine's own watermark_lag_s if present
        eng_lag = engine_summary.get("watermark_lag_s", 0.0)
        if eng_lag > 0:
            self.watermark_lag_s.labels(mode="heuristic").set(eng_lag)

        # Sketch lag
        sketch_lag = engine_summary.get("sketch_quantile_p50_ms", 0.0)
        if sketch_lag > 0:
            self.sketch_lag.labels(worker_id=worker_id).observe(sketch_lag / 1000.0)

        # Backpressure
        bp = 1 if engine_summary.get("backpressure_drops", 0) > 0 else 0
        self.backpressure_active.labels(worker_id=worker_id, partition_id=pid).set(bp)

        # §10.3 Heuristic-specific required metrics
        late_rate = engine_summary.get("late_arrival_rate_pct", 0.0)
        self.late_arrival_rate_pct.labels(worker_id=worker_id, partition_id=pid).set(late_rate)

        sketch_count = engine_summary.get("sketch_total_count", 0)
        self.sketch_total_count.labels(worker_id=worker_id).set(sketch_count)

        p50 = engine_summary.get("sketch_quantile_p50_ms", 0.0)
        p95 = engine_summary.get("sketch_quantile_p95_ms", 0.0)
        p99 = engine_summary.get("sketch_quantile_p99_ms", 0.0)
        self.sketch_quantile_ms.labels(worker_id=worker_id, quantile="p50").set(p50)
        self.sketch_quantile_ms.labels(worker_id=worker_id, quantile="p95").set(p95)
        self.sketch_quantile_ms.labels(worker_id=worker_id, quantile="p99").set(p99)

        drift = engine_summary.get("estimator_drift", 0.0)
        self.estimator_drift.labels(worker_id=worker_id).set(drift)

        neg_rate = engine_summary.get("negative_lag_rate", 0.0)
        self.negative_lag_rate.labels(worker_id=worker_id).set(neg_rate)

        replay = 1 if engine_summary.get("replay_mode_active", False) else 0
        self.replay_mode_active.labels(worker_id=worker_id, partition_id=pid).set(replay)

        adaptive = 1 if engine_summary.get("adaptive_percentile_active", False) else 0
        self.adaptive_percentile_active.labels(worker_id=worker_id).set(adaptive)

    def update_from_health_monitor(self, health_eval: dict) -> None:
        """Update ingestion health gauges from IngestorHealthMonitor.evaluate()."""
        silent = health_eval.get("silent_count", 0)
        stuck = sum(
            1
            for a in health_eval.get("alerts", [])
            if a.get("condition") == "stuck_punctuation"
        )
        self.ingestor_silent.set(silent)
        self.ingestor_stuck.set(stuck)
        self._snapshot["_ingestor_silent"] = silent
        self._snapshot["_ingestor_stuck"] = stuck

    def set_rocksdb_size(self, component: str, size_bytes: float) -> None:
        """Set RocksDB storage size for a component (e.g. 'state', 'window')."""
        self.rocksdb_size_bytes.labels(component=component).set(size_bytes)

    def update_kafka_lag(self, topic: str, group_id: str, client_id: str,
                         partition: int, lag: int) -> None:
        """Update Kafka partition lag for a consumer group partition."""
        self.kafka_partition_lag.labels(
            topic=topic, group_id=group_id, client_id=client_id,
            partition_id=str(partition),
        ).set(lag)

    def set_tiered_storage(self, tier: str, objects: int, bytes_: int) -> None:
        """Set MinIO tiered storage metrics for a tier (e.g. 'hot', 'warm', 'cold')."""
        self.tiered_storage_objects.labels(tier=tier).set(objects)
        self.tiered_storage_bytes.labels(tier=tier).set(bytes_)

    # ------------------------------------------------------------------
    # Snapshot / export
    # ------------------------------------------------------------------

    def snapshot(self) -> dict:
        """Return a JSON-serializable snapshot of current gauge values for alert
        evaluation."""
        # Collect current values from each gauge by iterating samples
        wm_lag_val: float = 0.0
        skew_val: float = 0.0
        combined_val: float = 0.0
        for sample in self.watermark_lag_s.collect():
            for s in sample.samples:
                wm_lag_val = s.value
                break
        for sample in self.node_skew_ms.collect():
            for s in sample.samples:
                skew_val = s.value
                break
        for sample in self.combined_status.collect():
            for s in sample.samples:
                combined_val = s.value
                break

        return {
            "watermark_lag_s": wm_lag_val,
            "node_skew_ms": skew_val,
            "combined_status": combined_val,
            "fencing_violations": self._snapshot.get("_fencing_prev", 0),
            "ingestor_silent": self._snapshot.get("_ingestor_silent", 0),
            "ingestor_stuck": self._snapshot.get("_ingestor_stuck", 0),
        }

    def generate_metrics(self) -> bytes:
        """Generate Prometheus text format (application/openmetrics-text)."""
        return generate_latest()
