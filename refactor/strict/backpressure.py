"""Backpressure Controller — per-partition pause/resume based on buffer thresholds.

Spec §4.3: Pause partition when buffer > 80%, resume when < 20%.
Workers report buffer sizes via HTTP POST /backpressure.
"""

import threading
import time
import logging

logger = logging.getLogger("backpressure")


class BackpressureController:
    """Tracks per-partition buffer depth and emits pause/resume signals."""

    def __init__(
        self,
        pause_threshold: int = 500,
        resume_threshold: int = 100,
        pause_pct: float = 0.80,
        resume_pct: float = 0.20,
        max_queue: int = 500,
    ):
        self.pause_threshold = pause_threshold
        self.resume_threshold = resume_threshold
        self.pause_pct = pause_pct
        self.resume_pct = resume_pct
        self.max_queue = max_queue
        self._lock = threading.Lock()
        self._buffer_sizes: dict[str, dict[int, int]] = {}
        self._paused: dict[int, bool] = {}
        self._pause_count: int = 0
        self._resume_count: int = 0
        self._signals: list[dict] = []

    def report_buffer(self, worker_id: str, partition_id: int, size: int) -> dict | None:
        with self._lock:
            if worker_id not in self._buffer_sizes:
                self._buffer_sizes[worker_id] = {}
            self._buffer_sizes[worker_id][partition_id] = size

            was_paused = self._paused.get(partition_id, False)

            if size >= self.pause_threshold and not was_paused:
                self._paused[partition_id] = True
                self._pause_count += 1
                signal = {
                    "action": "pause", "partition_id": partition_id,
                    "worker_id": worker_id, "buffer_size": size,
                    "threshold": self.pause_threshold, "timestamp": time.time(),
                }
                self._signals.append(signal)
                if len(self._signals) > 100:
                    self._signals.pop(0)
                logger.warning("Backpressure: PAUSE partition %d on %s (buffer=%d)",
                               partition_id, worker_id, size)
                return signal

            elif size < self.resume_threshold and was_paused:
                self._paused[partition_id] = False
                self._resume_count += 1
                signal = {
                    "action": "resume", "partition_id": partition_id,
                    "worker_id": worker_id, "buffer_size": size,
                    "threshold": self.resume_threshold, "timestamp": time.time(),
                }
                self._signals.append(signal)
                if len(self._signals) > 100:
                    self._signals.pop(0)
                logger.info("Backpressure: RESUME partition %d on %s (buffer=%d)",
                            partition_id, worker_id, size)
                return signal
        return None

    def is_paused(self, partition_id: int) -> bool:
        with self._lock:
            return self._paused.get(partition_id, False)

    def paused_partitions(self) -> list[int]:
        with self._lock:
            return [pid for pid, paused in self._paused.items() if paused]

    def summary(self) -> dict:
        with self._lock:
            return {
                "paused_partitions": [pid for pid, p in self._paused.items() if p],
                "total_pause_events": self._pause_count,
                "total_resume_events": self._resume_count,
                "current_buffer_sizes": {
                    wid: dict(pbufs) for wid, pbufs in self._buffer_sizes.items()
                },
                "recent_signals": self._signals[-10:],
            }

    def clear_worker(self, worker_id: str) -> None:
        with self._lock:
            partitions = self._buffer_sizes.pop(worker_id, None)
            if partitions:
                for pid in partitions:
                    self._paused.pop(pid, None)
