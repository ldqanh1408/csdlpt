"""Downstream Emitter — correction delivery with latency tracking (Spec §12.2-12.7)."""

import heapq
import logging
import threading
import time
from dataclasses import dataclass, field

from common.types import CorrectionMessage

logger = logging.getLogger("downstream_emitter")


@dataclass(order=True)
class PriorityCorrection:
    priority: float
    correction: CorrectionMessage = field(compare=False)
    enqueued_at: float = field(compare=False, default_factory=time.time)


class DownstreamEmitter:
    """Emits corrections with priority queue (largest deltas first)."""

    def __init__(self, max_queue: int = 10000):
        self._lock = threading.Lock()
        self._queue: list[PriorityCorrection] = []
        self._emitted: dict[str, CorrectionMessage] = {}
        self._latencies_s: list[float] = []  # all latencies for avg tracking
        self._latencies_normal: list[float] = []  # normal window latencies
        self._latencies_burst: list[float] = []  # burst window latencies
        self._correction_window_types: dict[str, str] = {}  # correction_id -> "normal" | "burst"
        self._emission_count: int = 0
        self._dropped: int = 0
        self.max_queue = max_queue
        # 24h FINAL reconciliation tracking
        self._final_sent: set[str] = set()  # window_ids that already received FINAL

    def enqueue(self, correction: CorrectionMessage, window_type: str = "normal"):
        with self._lock:
            if len(self._queue) >= self.max_queue:
                self._dropped += 1
                return
            heapq.heappush(self._queue, PriorityCorrection(
                priority=-correction.delta_count, correction=correction))
            self._correction_window_types[correction.correction_id] = window_type

    def drain(self, batch_size: int = 100) -> list[CorrectionMessage]:
        emitted = []
        with self._lock:
            for _ in range(min(batch_size, len(self._queue))):
                pc = heapq.heappop(self._queue)
                corr = pc.correction
                if corr.correction_id not in self._emitted:
                    self._emitted[corr.correction_id] = corr
                    self._emission_count += 1
                    latency = time.time() - corr.correction_timestamp
                    self._latencies_s.append(latency)
                    # Track latency per window type for SLA checks
                    wtype = self._correction_window_types.get(corr.correction_id, "normal")
                    if wtype == "burst":
                        self._latencies_burst.append(latency)
                    else:
                        self._latencies_normal.append(latency)
                    emitted.append(corr)
        return emitted

    def emit_final_reconciliation(self, window_id: str, final_count: int,
                                   previous_emit_timestamp: float = 0.0) -> CorrectionMessage | None:
        with self._lock:
            # Skip if this window already received its FINAL
            if window_id in self._final_sent:
                return None
            corr = CorrectionMessage(
                message_type="FINAL_RECONCILIATION", window_id=window_id,
                correction_id=f"final-{window_id}", corrected_count=final_count,
                delta_count=0, previous_emit_timestamp=previous_emit_timestamp,
                correction_timestamp=time.time())
            if corr.correction_id not in self._emitted:
                self._emitted[corr.correction_id] = corr
                self._emission_count += 1
                self._final_sent.add(window_id)
                return corr
        return None

    def pending(self) -> int:
        with self._lock:
            return len(self._queue)

    def avg_correction_latency_s(self) -> float:
        with self._lock:
            if not self._latencies_s:
                return 0.0
            return sum(self._latencies_s) / len(self._latencies_s)

    def summary(self) -> dict:
        with self._lock:
            return {
                "queue_depth": len(self._queue),
                "total_emitted": self._emission_count,
                "total_dropped": self._dropped,
                "avg_correction_latency_s": round(self.avg_correction_latency_s(), 3),
                "max_latency_s": round(max(self._latencies_s) if self._latencies_s else 0, 3),
            }

    # ------------------------------------------------------------------
    # B1: Correction Latency SLA enforcement
    # ------------------------------------------------------------------

    def check_sla(self) -> dict:
        """Check correction latency SLA compliance.

        SLA thresholds:
          - Normal window: correction must be emitted within 1 hour (3600s)
          - Burst window:  correction must be emitted within 15 minutes (900s)

        Returns a dict with:
          normal_window_violations : int
          burst_window_violations : int
          sla_compliant_pct        : float (0-100)
        """
        with self._lock:
            normal_violations = sum(
                1 for lt in self._latencies_normal if lt > 3600.0
            )
            burst_violations = sum(
                1 for lt in self._latencies_burst if lt > 900.0
            )
            total = len(self._latencies_normal) + len(self._latencies_burst)
            total_violations = normal_violations + burst_violations
            sla_compliant_pct = (
                0.0 if total == 0
                else round((1 - total_violations / total) * 100, 2)
            )
            return {
                "normal_window_violations": normal_violations,
                "burst_window_violations": burst_violations,
                "sla_compliant_pct": sla_compliant_pct,
            }

    # ------------------------------------------------------------------
    # B2: 24h FINAL reconciliation scheduling
    # ------------------------------------------------------------------

    def schedule_final_reconciliation(
        self, stop_event: threading.Event, window_store,
    ) -> None:
        """Start a background thread that checks for windows older than 24h
        and emits FINAL_RECONCILIATION for each eligible window.

        Parameters
        ----------
        stop_event : threading.Event
            When set, the loop exits cleanly.
        window_store : object
            A store with a ``get_expired_windows(age_s)`` method that returns
            an iterable of (window_id, current_count, emitted_at) tuples for
            windows whose end time is older than ``age_s`` seconds.  Typical
            ``age_s`` is 86400 (24 hours).
        """
        def _loop():
            logger.info("24h FINAL reconciliation scheduler started")
            while not stop_event.is_set():
                try:
                    expired = window_store.get_expired_windows(86400)
                    for window_id, count, emitted_at in expired:
                        corr = self.emit_final_reconciliation(
                            window_id, count, previous_emit_timestamp=emitted_at)
                        if corr is not None:
                            logger.info(
                                "FINAL_RECONCILIATION emitted for window=%s count=%d",
                                window_id, count,
                            )
                except Exception:
                    logger.exception("FINAL reconciliation scheduler error")
                # Check every 5 minutes (300s) in 1s increments
                for _ in range(300):
                    if stop_event.is_set():
                        break
                    time.sleep(1)
            logger.info(
                "24h FINAL reconciliation scheduler stopped. "
                "FINALS sent: %d", len(self._final_sent),
            )

        t = threading.Thread(
            target=_loop, daemon=True, name="final-reconciliation-scheduler",
        )
        t.start()
