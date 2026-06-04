"""
Registry Prometheus trung tâm cho toàn hệ thống.

Module tạo và cập nhật Counter/Gauge/Histogram cho watermark, worker, DLQ, failover, backpressure, replay, correction latency và timer profile.
"""

from __future__ import annotations

import math
import os
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
    """Lớp `AlertRule` gom dữ liệu và hành vi liên quan đến AlertRule."""
    name: str
    description: str
    severity: str  # critical | warning | high
    condition: str  # human-readable expression
    evaluate: callable  # (metrics_snapshot: dict) -> bool


# ---------------------------------------------------------------------------
# MonitoringManager
# ---------------------------------------------------------------------------

class MonitoringManager:
    """Lớp `MonitoringManager` quản lý trạng thái và thao tác nghiệp vụ tương ứng.
    
    Ghi chú gốc:
    Central Prometheus metrics registry for the stream processor.
    
        Parameters
        ----------
        registry : CollectorRegistry | None
            If None, uses the default global registry.
    """

    def __init__(self, registry: CollectorRegistry | None = None):
        """Khởi tạo đối tượng của `MonitoringManager` và thiết lập trạng thái ban đầu."""
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
            "csdlpt_node_skew_max_ms",
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
        self.tiered_eviction_failure_total = Counter(
            "csdlpt_tiered_eviction_failure_total",
            "Total number of tiered eviction failures",
            registry=_reg,
        )
        self.tiered_eviction_attempt_total = Counter(
            "csdlpt_tiered_eviction_attempt_total",
            "Total number of tiered eviction attempts",
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
        self.dlq_oldest_entry_age_s = Gauge(
            "csdlpt_dlq_oldest_entry_age_seconds",
            "Age of the oldest entry currently waiting in the DLQ (spec §12 SLA tracking)",
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

        # ---- §12.7 DLQ correction SLA compliance ----
        self.sla_compliant_pct = Gauge(
            "csdlpt_sla_compliant_pct",
            "Percentage of corrections delivered within SLA (normal ≤1h, burst ≤15min)",
            ["worker_id"],
            registry=_reg,
        )

        # ---- Trien_Khai §3 Tier-1 cluster-health metrics referenced by
        # deploy/prometheus-rules.yml. These are emitted by the coordinator
        # (worker registry, leader changes) or worker (backpressure event
        # counters, tiered-storage errors) so the YAML rules can actually fire.
        self.worker_alive_count = Gauge(
            "csdlpt_worker_alive_count",
            "Number of workers reporting active heartbeats",
            registry=_reg,
        )
        self.worker_total_count = Gauge(
            "csdlpt_worker_total_count",
            "Total registered workers (alive + failed)",
            registry=_reg,
        )
        self.coordinator_leader_changes = Counter(
            "csdlpt_coordinator_leader_changes_total",
            "Raft coordinator leadership transitions",
            registry=_reg,
        )
        self.aggregator_leader_changes = Counter(
            "csdlpt_aggregator_leader_changes_total",
            "Aggregator active-standby failovers",
            registry=_reg,
        )
        self.aggregator_failover_total = Counter(
            "csdlpt_aggregator_failover_total",
            "Aggregator HA failover events (alias of leader changes)",
            registry=_reg,
        )
        self.backpressure_pause_count = Counter(
            "csdlpt_backpressure_pause_count",
            "Cumulative number of backpressure PAUSE events across partitions",
            ["worker_id"],
            registry=_reg,
        )
        self.failover_events_total = Counter(
            "csdlpt_failover_events_total",
            "Partition reassignment / failback events emitted by FailoverManager",
            ["event"],
            registry=_reg,
        )
        self.tier_storage_status = Gauge(
            "csdlpt_tier_storage_status",
            "1 when MinIO client is connected, 0 when disabled/unreachable",
            registry=_reg,
        )
        self.minio_upload_errors_total = Counter(
            "csdlpt_minio_upload_errors_total",
            "Failed TieredStorageManager upload attempts",
            registry=_reg,
        )
        self.minio_upload_lag_seconds = Histogram(
            "csdlpt_minio_upload_lag_seconds",
            "MinIO object upload latency in seconds",
            buckets=[0.1, 0.5, 1, 5, 10, 30, 60, 300, 600, 900],
            registry=_reg,
        )
        self.data_completeness_pct = Gauge(
            "csdlpt_data_completeness_pct",
            "On-time events as a fraction of unique events received (100 = no loss)",
            ["worker_id", "partition_id"],
            registry=_reg,
        )
        self.data_loss_rate = Gauge(
            "csdlpt_data_loss_rate",
            "Data loss rate percentage (0 = no loss)",
            ["mode"],
            registry=_reg,
        )
        self.non_monotonic_punctuation_total = Counter(
            "csdlpt_non_monotonic_punctuation_total",
            "Punctuation tokens rejected because T_commit did not advance (§4.5)",
            ["worker_id", "partition_id"],
            registry=_reg,
        )
        self.extreme_lag_count = Gauge(
            "csdlpt_extreme_lag_count",
            "Number of events with extreme lag (>L_max) routed to DLQ",
            registry=_reg,
        )
        self.correction_latency_s = Gauge(
            "csdlpt_correction_latency_seconds",
            "Average seconds between correction compute cycles",
            registry=_reg,
        )
        self.all_workers_idle = Gauge(
            "csdlpt_all_workers_idle",
            "1 if all registered workers are idle, 0 otherwise",
            registry=_reg,
        )
        self.replay_mode_extended = Gauge(
            "csdlpt_replay_mode_extended",
            "1 if replay mode has been active for >5min on >=2 workers",
            registry=_reg,
        )

        # ---- Worker resource gauges (for WorkerRAMHigh/WorkerDiskHigh alerts) ----
        self.worker_ram_usage_bytes = Gauge(
            "csdlpt_worker_ram_usage_bytes",
            "Worker RAM usage bytes",
            ["worker_id"],
            registry=_reg,
        )
        self.worker_ram_limit_bytes = Gauge(
            "csdlpt_worker_ram_limit_bytes",
            "Worker RAM limit bytes",
            ["worker_id"],
            registry=_reg,
        )
        self.worker_disk_usage_bytes = Gauge(
            "csdlpt_worker_disk_usage_bytes",
            "Worker disk usage bytes",
            ["worker_id"],
            registry=_reg,
        )
        self.worker_disk_limit_bytes = Gauge(
            "csdlpt_worker_disk_limit_bytes",
            "Worker disk limit bytes",
            ["worker_id"],
            registry=_reg,
        )

        # ---- Dashboard panel metrics (Gap 3) ----
        self.active_partitions = Gauge(
            "csdlpt_active_partitions",
            "Number of active partitions",
            ["worker_id"],
            registry=_reg,
        )
        self.clock_skew_ms = Gauge(
            "csdlpt_clock_skew_ms",
            "Clock skew between nodes in ms",
            ["worker_id"],
            registry=_reg,
        )
        self.punctuation_total = Gauge(
            "csdlpt_punctuation_total",
            "Total punctuation tokens received",
            ["worker_id"],
            registry=_reg,
        )
        self.eviction_state = Gauge(
            "csdlpt_eviction_state",
            "Current eviction state (0=CLOSED,1=UPLOADING,2=UPLOADED,3=PURGED)",
            ["window_id"],
            registry=_reg,
        )
        self.ingestor_health_rtt_ms = Gauge(
            "csdlpt_ingestor_health_rtt_ms",
            "Ingestor health check RTT in ms",
            ["ingestor_id"],
            registry=_reg,
        )
        self.ingestor_network_rtt_seconds = Gauge(
            "csdlpt_ingestor_network_rtt_seconds",
            "Ingestor network RTT in seconds",
            ["ingestor_id"],
            registry=_reg,
        )

        # Snapshot cache for alert evaluation
        self._snapshot: dict = {}

    # ------------------------------------------------------------------
    # Public update helpers
    # ------------------------------------------------------------------

    def update_from_coordinator(self, broadcast: dict) -> None:
        """Cập nhật trạng thái/metric `update from coordinator` dựa trên dữ liệu mới.
        
        Ghi chú gốc:
        Update metrics from a coordinator broadcast dict.
        
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

        # Defensive: never let a non-finite (inf/NaN) value reach the gauges.
        # A killed/restarted coordinator can emit lag=+inf or skew=NaN before
        # its first watermark advances; those would corrupt Prometheus output
        # and trip alert rules with garbage values.
        if not math.isfinite(lag_s):
            lag_s = 0.0
        if not math.isfinite(skew_ms):
            skew_ms = 0.0

        if math.isfinite(wg):
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

        # Trien_Khai §3 cluster-health gauges. `active_workers` is the count
        # of distinct workers we have seen heartbeats from recently;
        # `partition_count` is the total partitions known to the coordinator
        # (one per registered worker partition assignment).
        active = broadcast.get("active_workers", 0)
        total = broadcast.get("worker_total_count", active)
        self.worker_alive_count.set(active)
        self.worker_total_count.set(max(total, active))

        # Raft leader transitions — Counter inc by diff against last snapshot.
        prev_term = self._snapshot.get("_raft_term_prev", 0)
        cur_term = broadcast.get("raft_term", broadcast.get("term", 0))
        if cur_term > prev_term:
            self.coordinator_leader_changes.inc(cur_term - prev_term)
        self._snapshot["_raft_term_prev"] = cur_term

    def update_from_engine(self, engine_summary: dict, worker_id: str, partition_id: int) -> None:
        """Cập nhật trạng thái/metric `update from engine` dựa trên dữ liệu mới.
        
        Ghi chú gốc:
        Update metrics from an engine summary dict.
        
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

        # Trien_Khai §3 — data completeness for the DataLossRateHigh alert.
        completeness = engine_summary.get("data_completeness_pct", 100.0)
        self.data_completeness_pct.labels(worker_id=worker_id, partition_id=pid).set(completeness)
        loss_rate = max(0.0, 100.0 - completeness)
        mode = engine_summary.get("mode", "strict")
        self.data_loss_rate.labels(mode=mode).set(loss_rate)

        # §4.5 monotonic-punctuation rejections — counter inc by diff
        nmp = engine_summary.get("non_monotonic_punctuation", 0)
        pkey_nmp = f"nmp_{worker_id}_{pid}"
        prev_nmp = self._snapshot.get(pkey_nmp, 0)
        if nmp > prev_nmp:
            self.non_monotonic_punctuation_total.labels(
                worker_id=worker_id, partition_id=pid).inc(nmp - prev_nmp)
        self._snapshot[pkey_nmp] = nmp

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

        # Heuristic-specific gauges for alert rules 8-15
        dlq = engine_summary.get("dlq_backlog", 0)
        self.dlq_backlog.set(dlq)

        ext_lag = engine_summary.get("extreme_lag_count", 0)
        self.extreme_lag_count.set(ext_lag)

        # ---- Dashboard panel metrics (Gap 3) ----
        # active_partitions: count partitions known to this worker
        active_parts = engine_summary.get("active_partitions", 1)
        self.active_partitions.labels(worker_id=worker_id).set(active_parts)

        # clock_skew_ms: per-worker clock skew estimate
        clock_skew = engine_summary.get("clock_skew_ms", 0.0)
        self.clock_skew_ms.labels(worker_id=worker_id).set(clock_skew)

        # punctuation_total: total punctuation tokens cumulatively
        punct = engine_summary.get("punctuation_total", total)
        self.punctuation_total.labels(worker_id=worker_id).set(punct)

        # eviction_state: from engine_summary if available
        eviction = engine_summary.get("eviction_state", 0)
        win_id = engine_summary.get("window_id", f"{worker_id}_{pid}")
        self.eviction_state.labels(window_id=win_id).set(eviction)

        # ingestor health RTT from engine_summary (ingestor-level metrics)
        ingestor_id = engine_summary.get("ingestor_id", worker_id)
        rtt_ms = engine_summary.get("ingestor_health_rtt_ms", 0.0)
        self.ingestor_health_rtt_ms.labels(ingestor_id=ingestor_id).set(rtt_ms)

        # ingestor network RTT
        net_rtt_s = engine_summary.get("ingestor_network_rtt_seconds", 0.0)
        self.ingestor_network_rtt_seconds.labels(ingestor_id=ingestor_id).set(net_rtt_s)

    def update_from_health_monitor(self, health_eval: dict) -> None:
        """Cập nhật trạng thái/metric `update from health monitor` dựa trên dữ liệu mới.
        
        Ghi chú gốc:
        Update ingestion health gauges from IngestorHealthMonitor.evaluate().
        """
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

        # Update ingestor-specific gauges
        ingestors = health_eval.get("ingestors", {})
        for ingestor_id, info in ingestors.items():
            rtt_ms = info.get("network_rtt_ms", 0.0)
            self.ingestor_health_rtt_ms.labels(ingestor_id=ingestor_id).set(rtt_ms)
            self.ingestor_network_rtt_seconds.labels(ingestor_id=ingestor_id).set(rtt_ms / 1000.0)

    def update_worker_resources(self, worker_id: str) -> None:
        """Cập nhật trạng thái/metric `update worker resources` dựa trên dữ liệu mới.
        
        Ghi chú gốc:
        Update worker RAM/disk gauges from psutil (best-effort).
        
                Called periodically by the worker health loop so that WorkerRAMHigh,
                WorkerDiskHigh, and WorkerDiskHighWarning alert rules have data to
                evaluate against.
        """
        try:
            import psutil
        except ImportError:
            return

        try:
            vm = psutil.virtual_memory()
            self.worker_ram_usage_bytes.labels(worker_id=worker_id).set(vm.used)
            self.worker_ram_limit_bytes.labels(worker_id=worker_id).set(vm.total)
        except Exception:
            pass

        try:
            data_dir = "/data" if os.path.exists("/data") else "/tmp"
            usage = psutil.disk_usage(data_dir)
            self.worker_disk_usage_bytes.labels(worker_id=worker_id).set(usage.used)
            self.worker_disk_limit_bytes.labels(worker_id=worker_id).set(usage.total)
        except Exception:
            pass

    def update_alert_snapshot(self, **kwargs) -> None:
        """Cập nhật trạng thái/metric `update alert snapshot` dựa trên dữ liệu mới.
        
        Ghi chú gốc:
        Push values into the snapshot dict for alert rule evaluation.
        
                Keys: correction_latency_s, all_workers_idle, replay_mode_extended,
                      extreme_lag_count, dlq_backlog, etc.
        """
        for key, val in kwargs.items():
            self._snapshot[f"_{key}"] = val
        # Also set the corresponding Prometheus gauges where they exist
        if "correction_latency_s" in kwargs:
            self.correction_latency_s.set(kwargs["correction_latency_s"])
        if "all_workers_idle" in kwargs:
            self.all_workers_idle.set(kwargs["all_workers_idle"])
        if "replay_mode_extended" in kwargs:
            self.replay_mode_extended.set(kwargs["replay_mode_extended"])

    def update_data_loss_rate(self, mode: str, rate: float) -> None:
        """Cập nhật trạng thái/metric `update data loss rate` dựa trên dữ liệu mới.
        
        Ghi chú gốc:
        Set the data loss rate gauge for the given mode.
        """
        self.data_loss_rate.labels(mode=mode).set(rate)

    def update_minio_upload_lag(self, lag_s: float) -> None:
        """Cập nhật trạng thái/metric `update minio upload lag` dựa trên dữ liệu mới.
        
        Ghi chú gốc:
        Record a MinIO object upload latency observation.
        """
        self.minio_upload_lag_seconds.observe(lag_s)

    def set_rocksdb_size(self, component: str, size_bytes: float) -> None:
        """Cập nhật giá trị `rocksdb size` vào trạng thái hiện tại.
        
        Ghi chú gốc:
        Set RocksDB storage size for a component (e.g. 'state', 'window').
        """
        self.rocksdb_size_bytes.labels(component=component).set(size_bytes)

    def update_kafka_lag(self, topic: str, group_id: str, client_id: str,
                         partition: int, lag: int) -> None:
        """Cập nhật trạng thái/metric `update kafka lag` dựa trên dữ liệu mới.
        
        Ghi chú gốc:
        Update Kafka partition lag for a consumer group partition.
        """
        self.kafka_partition_lag.labels(
            topic=topic, group_id=group_id, client_id=client_id,
            partition_id=str(partition),
        ).set(lag)

    def set_tiered_storage(self, tier: str, objects: int, bytes_: int) -> None:
        """Cập nhật giá trị `tiered storage` vào trạng thái hiện tại.
        
        Ghi chú gốc:
        Set MinIO tiered storage metrics for a tier (e.g. 'hot', 'warm', 'cold').
        """
        self.tiered_storage_objects.labels(tier=tier).set(objects)
        self.tiered_storage_bytes.labels(tier=tier).set(bytes_)

    # ------------------------------------------------------------------
    # Snapshot / export
    # ------------------------------------------------------------------

    def _read_gauge(self, gauge) -> float:
        """Hàm `_read_gauge` thực hiện phần xử lý liên quan đến read gauge của `MonitoringManager`."""
        for sample in gauge.collect():
            for s in sample.samples:
                return float(s.value)
        return 0.0

    def snapshot(self) -> dict:
        """Hàm `snapshot` thực hiện phần xử lý liên quan đến snapshot của `MonitoringManager`.
        
        Ghi chú gốc:
        Return a JSON-serializable snapshot of current gauge values for alert
                evaluation. Includes ALL keys required by registered alert rules (1-15).
        """
        return {
            # Tier 1: system health
            "watermark_lag_s": self._read_gauge(self.watermark_lag_s),
            "node_skew_ms": self._read_gauge(self.node_skew_ms),
            "combined_status": self._read_gauge(self.combined_status),
            "fencing_violations": self._snapshot.get("_fencing_prev", 0),
            # Tier 3: ingestor health
            "ingestor_silent": self._snapshot.get("_ingestor_silent", 0),
            "ingestor_stuck": self._snapshot.get("_ingestor_stuck", 0),
            # Heuristic-specific (rules 8-15)
            "negative_lag_rate": self._read_gauge(self.negative_lag_rate),
            "dlq_backlog": self._read_gauge(self.dlq_backlog),
            "estimator_drift": self._read_gauge(self.estimator_drift),
            "extreme_lag_count": self._read_gauge(self.extreme_lag_count),
            "correction_latency_s": self._read_gauge(self.correction_latency_s),
            "all_workers_idle": self._read_gauge(self.all_workers_idle),
            "replay_mode_extended": self._read_gauge(self.replay_mode_extended),
        }

    def generate_metrics(self) -> bytes:
        """Hàm `generate_metrics` thực hiện phần xử lý liên quan đến generate metrics của `MonitoringManager`.
        
        Ghi chú gốc:
        Generate Prometheus text format (application/openmetrics-text).
        """
        return generate_latest()
