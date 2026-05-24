"""Heuristic Watermark Engine - low-latency with DDSketch-based lag estimation.

Watermark: W_h(t) = max(W_h(t-1), max(T_event) - L_eff(t))
where L_eff(t) = DDSketch.quantile(p), p = adaptive percentile.

Expected loss <= 1% (steady state), <= 5% (burst, with adaptive p=0.999).
End-to-end latency ~5s (vs ~15s for Strict).
"""

import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

from refactor.ddsketch import DDSketch, SlidingWindowDDSketch
from refactor.common.types import LogEvent, WindowResult
from refactor.common.window import TumblingWindow
from refactor.common.metrics import HighResTimer, SystemMetrics
from refactor.common.rocks_store import RocksStore
from refactor.heuristic.cold_start import ColdStartManager
from refactor.heuristic.negative_lag import NegativeLagHandler, LagTier


# RocksDB key prefixes
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
    "max_lag_accepted": 3600.0,
    "window_seconds": 60,
    "sub_sketch_granularity": 1,
    "warmup_min_seconds": 10,
    "warmup_min_samples": 1000,
    "L_max": 60.0,
    "wm_max_advance_rate": 1.5,
    "l_eff_update_threshold": 0.10,
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
    count: int = 0
    status_500: int = 0


class HeuristicWatermarkEngine:
    """Per-partition engine using DDSketch to estimate watermark heuristically."""

    def __init__(self, partition_id: int = 0, worker_id: str = "", **kwargs):
        cfg = {**DEFAULT_PARAMS, **kwargs}
        self.partition_id = partition_id
        self.worker_id = worker_id

        # RocksDB path (extract before consuming kwargs)
        db_path: Optional[str] = cfg.pop("db_path", None)
        tiered_storage = cfg.pop("tiered_storage", None)

        # Windowing
        self.tumbling = TumblingWindow(cfg["window_size_s"])

        # DDSketch
        self.sketch = SlidingWindowDDSketch(
            window_seconds=cfg["window_seconds"],
            sub_sketch_granularity=cfg["sub_sketch_granularity"],
            alpha=cfg["alpha"],
            max_buckets=cfg["max_buckets"],
            min_value=1e-3,
            max_value=cfg["max_lag_accepted"],
        )

        # Watermark state
        self.W_h: float = float("-inf")
        self.W_h_prev: float = float("-inf")
        self.max_event_time: float = float("-inf")
        self.L_eff: float = cfg["L_max"]
        self.L_eff_prev: float = cfg["L_max"]

        # Window state
        self.open_windows: dict[float, WindowAggregate] = defaultdict(WindowAggregate)
        self.closed_windows: dict[float, WindowResult] = {}
        self.late_events: list[dict] = []

        # Dedup
        self.seen_ids: set[str] = set()

        # Adaptive percentile
        self.p_current: float = cfg["p_normal"]
        self.p_normal: float = cfg["p_normal"]
        self.p_safe: float = cfg["p_safe"]
        self.burst_threshold: float = cfg["burst_threshold"]
        self.in_burst: bool = False
        self.burst_start: float = 0.0
        self.recovery_minutes: int = cfg["recovery_minutes"]
        self._quantile_history: list[float] = []

        # Adaptive alpha (DDSketch Strategy 3)
        self._current_alpha: float = cfg["alpha"]
        self._last_alpha_check: float = 0.0

        # Hysteresis
        self.wm_max_advance_rate: float = cfg["wm_max_advance_rate"]
        self.l_eff_update_threshold: float = cfg["l_eff_update_threshold"]

        # Snapshot manager
        self.snapshot_interval: float = cfg["snapshot_interval"]
        self.snapshot_count: int = cfg["snapshot_count"]
        self._snapshots: list[tuple[float, dict]] = []
        self._last_snapshot: float = 0.0

        # Replay mode
        self.in_replay_mode: bool = False
        self.baseline_multiplier: float = cfg["baseline_multiplier"]
        self.exit_multiplier: float = cfg["exit_multiplier"]
        self.exit_streak_seconds: float = cfg["exit_streak_seconds"]
        self.baseline_lag: float = cfg["L_max"]
        self._replay_exit_counter: int = 0

        # Max allowed lag
        self.max_lag_accepted: float = cfg["max_lag_accepted"]
        self.L_max: float = cfg["L_max"]

        # Cold start manager (spec §6)
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

        # Dedup compaction
        self._last_dedup_sweep: float = 0.0

        # Kafka offset tracking for warm restart (spec §8.3)
        self.kafka_seek_offset: int = 0

        # Idleness detection
        self.last_event_time: float = time.time()

    # ---- RocksDB persistence helpers ----

    def _persist_open_window(self, ws: float) -> None:
        if self._store is None:
            return
        agg = self.open_windows.get(ws)
        if agg is not None:
            self._store.put(f"{_PFX_OPEN}{ws}", agg)

    def _delete_open_window(self, ws: float) -> None:
        if self._store is None:
            return
        self._store.delete(f"{_PFX_OPEN}{ws}")

    def _persist_closed_window(self, ws: float, result: WindowResult) -> None:
        if self._store is None:
            return
        self._store.put(f"{_PFX_CLOSED}{ws}", result)

    def _persist_seen_id(self, event_id: str) -> None:
        if self._store is None:
            return
        self._store.put(f"{_PFX_SEEN}{event_id}", True)

    def _restore_from_store(self) -> None:
        """Populate in-memory state from RocksDB on startup."""
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

        for key, _val in self._store.items(prefix=_PFX_SEEN):
            eid = key[len(_PFX_SEEN):]
            self.seen_ids.add(eid)

        # Restore metadata
        meta = self._store.get(f"{_PFX_META}checkpoint")
        if meta is not None:
            self.W_h = meta.get("W_h", float("-inf"))
            self.max_event_time = meta.get("max_event_time", float("-inf"))
            self.L_eff = meta.get("L_eff", self.L_max)
            self.in_replay_mode = meta.get("in_replay_mode", False)
            self.kafka_seek_offset = meta.get("kafka_seek_offset", 0)
            sketch_data = meta.get("sketch")
            if sketch_data is not None:
                self.sketch = SlidingWindowDDSketch.from_dict(sketch_data)

    def checkpoint(self) -> None:
        """Persist engine metadata to RocksDB."""
        if self._store is None:
            return
        meta = {
            "W_h": self.W_h,
            "max_event_time": self.max_event_time,
            "L_eff": self.L_eff,
            "in_replay_mode": self.in_replay_mode,
            "sketch": self.sketch.to_dict(),
            "kafka_seek_offset": self.kafka_seek_offset,
        }
        self._store.put(f"{_PFX_META}checkpoint", meta)
        self._store.flush()

    # ---- Core watermark computation ----
    def _compute_watermark(self) -> float:
        """W_h(t) = max(W_h(t-1), max(T_event) - L_eff(t)) with hysteresis."""
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

        # Monotonic enforcement
        self.W_h = max(self.W_h_prev, candidate)

        # Rate limiting (hysteresis — bound advance by elapsed wall-clock time)
        if self.W_h_prev > float("-inf"):
            if self._last_wm_update_time > 0:
                elapsed = time.time() - self._last_wm_update_time
            else:
                elapsed = time.time() - self.start_time
            max_advance = elapsed * self.wm_max_advance_rate
            self.W_h = min(self.W_h, self.W_h_prev + max_advance + self.tumbling.size)
        self._last_wm_update_time = time.time()

        return self.W_h

    def _update_L_eff(self) -> None:
        """Update L_eff from sketch, with adaptive percentile and threshold."""
        # Cold start: use conservative prior until warm
        self.cold_start.update(self.sketch.total_count)
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

        # Hysteresis: only update if change > 10%
        if abs(new_L_eff - self.L_eff) / max(self.L_eff, 0.001) >= self.l_eff_update_threshold:
            self.L_eff = min(new_L_eff, self.L_max)

        # Burst detection
        self._check_burst()

        # Adaptive alpha check (DDSketch Strategy 3)
        self._check_adaptive_alpha()

    def _check_burst(self) -> None:
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
        """Adjust sketch alpha based on value range (DDSketch Strategy 3).

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
        elif p99 > 30.0 and self._current_alpha < 0.01:
            self.sketch.alpha = 0.01
            self._current_alpha = 0.01

    def _close_windows(self) -> None:
        """Close windows where W_h >= window_end."""
        # Skip window closing during cold start Phase 0
        if self.cold_start.phase.name == "PHASE_0":
            return

        to_close = [
            w for w in self.open_windows
            if w + self.tumbling.size <= self.W_h
        ]
        for w in sorted(to_close):
            agg = self.open_windows.pop(w)
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
            self._persist_closed_window(w, result)
            self._pending_results.append(result)

    def get_expired_windows(self, age_s: float) -> list:
        """Return closed windows where window_end < now - age_s as (window_id, count) tuples.

        Used by DownstreamEmitter.schedule_final_reconciliation() for 24h FINAL checks.
        """
        now = time.time()
        cutoff = now - age_s
        expired = []
        for w, result in self.closed_windows.items():
            if result.window_end < cutoff:
                expired.append((result.window_id, result.count))
        return expired

    def drain_results(self) -> list:
        """Return and clear pending results accumulated from closed windows.

        Called by the Kafka producer loop to emit WindowResults to the
        ``heuristic_results`` topic.
        """
        results = self._pending_results[:]
        self._pending_results.clear()
        return results

    def _purge_old_closed_windows(self) -> None:
        """Periodically delete closed windows older than 1 hour from RocksDB.

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
        """Periodically clear RocksDB seen-ID storage for compaction every 60s."""
        if self._store is None:
            return
        now = time.time()
        if now - self._last_dedup_sweep < 60.0:
            return
        self._store.clear_prefix(_PFX_SEEN)
        self._last_dedup_sweep = now

    # ---- Event processing ----
    def is_idle(self) -> bool:
        return time.time() - self.last_event_time > 2.0

    def process(self, event: LogEvent, arrival_time: float = None) -> Optional[float]:
        t0 = HighResTimer.now_ns()
        self.metrics.total_received += 1

        if arrival_time is None:
            arrival_time = time.time()

        # Dedup
        if event.event_id in self.seen_ids:
            self.metrics.duplicates += 1
            return None
        self.seen_ids.add(event.event_id)
        self._persist_seen_id(event.event_id)

        # Periodic RocksDB dedup compaction
        self._purge_seen_ids()

        # Periodic closed-window TTL purge (Gap 5)
        self._purge_old_closed_windows()

        et = event.event_time
        self.max_event_time = max(self.max_event_time, et)
        self.last_event_time = arrival_time

        # Compute lag
        lag = arrival_time - et

        # Negative lag handling (spec §7)
        self.neg_lag.observe(lag)
        tier = self.neg_lag.evaluate()
        adjusted_lag = self.neg_lag.adjust_lag(lag)
        self.metrics.negative_lag_rate = self.neg_lag.rate

        # Replay detection (spec §11.3)
        if self.detect_replay(lag):
            pass  # skip sketch update during replay
        elif not self.neg_lag.should_degrade_to_boo():
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
            self._persist_open_window(ws)
            self.metrics.on_time += 1
        else:
            self.open_windows[ws] = WindowAggregate(
                count=1,
                status_500=1 if event.status == 500 else 0,
            )
            self._persist_open_window(ws)
            self.metrics.on_time += 1

        # Periodic snapshot
        if time.time() - self._last_snapshot >= self.snapshot_interval:
            self._take_snapshot()
            self._last_snapshot = time.time()

        lat_ns = HighResTimer.now_ns() - t0
        self.proc_latencies_ns.append(lat_ns)

        # Update metrics
        self.metrics.sketch_total_count = self.sketch.total_count
        if self.sketch.total_count > 0:
            self.metrics.sketch_quantile_p50_ms = self.sketch.quantile(0.50) * 1000
            self.metrics.sketch_quantile_p95_ms = self.sketch.quantile(0.95) * 1000
            self.metrics.sketch_quantile_p99_ms = self.sketch.quantile(0.99) * 1000
        self.metrics.dlq_backlog = len(self.late_events)
        self.metrics.replay_mode_active = self.in_replay_mode

        # Per-window loss samples
        self.metrics.per_window_loss_samples = [
            {"window_id": wid, "late": wl["late"], "total": wl["total"]}
            for wid, wl in self._window_loss.items()
        ]

        return lat_ns

    # ---- Snapshot + Rollback ----
    def _take_snapshot(self) -> None:
        snap = (time.time(), self.sketch.to_dict())
        self._snapshots.append(snap)
        if len(self._snapshots) > self.snapshot_count:
            self._snapshots.pop(0)

    def rollback_to(self, target_time: float) -> bool:
        for ts, snap_dict in reversed(self._snapshots):
            if ts <= target_time:
                self.sketch = SlidingWindowDDSketch.from_dict(snap_dict)
                return True
        return False

    # ---- Replay mode ----
    def detect_replay(self, lag: float) -> bool:
        if self.baseline_lag > 0 and lag > self.baseline_multiplier * self.baseline_lag:
            self.in_replay_mode = True
            self.metrics.replay_mode_active = True
            rollback_target = time.time() - 5.0
            self.rollback_to(rollback_target)
            return True
        return False

    def check_exit_replay(self) -> None:
        if not self.in_replay_mode:
            return
        recent_lag = self.L_eff
        if recent_lag < self.exit_multiplier * self.baseline_lag:
            self._replay_exit_counter += 1
            if self._replay_exit_counter >= self.exit_streak_seconds:
                self.in_replay_mode = False
                self.metrics.replay_mode_active = False
                self._replay_exit_counter = 0
                self.baseline_lag = recent_lag
        else:
            self._replay_exit_counter = 0

    def flush(self) -> None:
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
            self._persist_closed_window(w, result)
            self._pending_results.append(result)

        # Batch-delete all open windows from RocksDB
        if self._store is not None:
            self._store.clear_prefix(_PFX_OPEN)
            self._store.flush()
        self.open_windows.clear()

    def close(self) -> None:
        """Close the RocksDB store if open."""
        if self._store is not None:
            self._store.close()
            self._store = None

    def get_seek_offset(self) -> int:
        """Return Kafka seek offset (offset + 1 per spec §8.3)."""
        return self.kafka_seek_offset + 1

    def summary(self) -> dict:
        closed_count = (
            self._store.count(prefix=_PFX_CLOSED)
            if self._store is not None
            else len(self.closed_windows)
        )
        # Per-window loss accounting (top 20 by loss rate)
        per_window_loss = sorted(
            [
                {
                    "window_id": wid,
                    "late": wl["late"],
                    "total": wl["total"],
                    "loss_rate": round(wl["late"] / max(wl["total"], 1), 4),
                }
                for wid, wl in self._window_loss.items()
            ],
            key=lambda x: x["loss_rate"],
            reverse=True,
        )[:20]

        total_late = sum(wl["late"] for wl in self._window_loss.values())
        total_events = sum(wl["total"] for wl in self._window_loss.values())
        overall_loss_rate = round(total_late / max(total_events, 1), 4)

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
            "cold_start": self.cold_start.status(),
            "negative_lag": self.neg_lag.status(),
            "per_window_loss": per_window_loss,
            "overall_loss_rate": overall_loss_rate,
        })
        return base
