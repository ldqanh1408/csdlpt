"""Strict Worker - bounded priority queue + per-partition engine management."""

import heapq
import os
import time

from strict.engine import StrictWatermarkEngine
from common.types import LogEvent, PunctuationToken, WorkerHeartbeat

# Idleness detection: partition idle if no event for > 2.0s
IDLE_TIMEOUT_S = 2.0

# Backpressure: reject new events when queue >= 500, resume when < 100
BACKPRESSURE_MAX_QUEUE = 500
BACKPRESSURE_RESUME_AT = 100


class BoundedPriorityQueue:
    """Client-side min-heap that sorts events by event_time.
    Maxsize: 10000, max wait: 1000ms timeout before forced pop."""

    def __init__(self, maxsize: int = 10000, max_wait_ms: int = 1000):
        self.maxsize = maxsize
        self.max_wait_ms = max_wait_ms
        self._buffer: list[tuple[float, int, LogEvent]] = []
        self._counter: int = 0
        self._last_pop: float = time.monotonic()

    def push(self, event: LogEvent) -> LogEvent | None:
        heapq.heappush(self._buffer, (event.event_time, self._counter, event))
        self._counter += 1
        if len(self._buffer) >= self.maxsize:
            return self._pop_min()
        return None

    def pop_ready(self) -> LogEvent | None:
        elapsed_ms = (time.monotonic() - self._last_pop) * 1000
        if self._buffer and elapsed_ms >= self.max_wait_ms:
            return self._pop_min()
        return None

    def pop_all_ready(self, batch_size: int = 200) -> list[LogEvent]:
        """Drain up to batch_size events on every call."""
        result = []
        if not self._buffer:
            return result
        target = min(batch_size, len(self._buffer))
        if target == 0:
            return result
        for _ in range(target):
            ev = self._pop_min()
            if ev is not None:
                result.append(ev)
        return result

    def _pop_min(self) -> LogEvent | None:
        if not self._buffer:
            return None
        _, _, event = heapq.heappop(self._buffer)
        self._last_pop = time.monotonic()
        return event

    def flush_all(self) -> list[LogEvent]:
        events = [e for _, _, e in sorted(self._buffer, key=lambda x: x[0])]
        self._buffer.clear()
        return events

    def __len__(self) -> int:
        return len(self._buffer)


class StrictWorker:

    def __init__(
        self,
        worker_id: str,
        partition_ids: list[int],
        window_size_s: float = 5.0,
        delta_base_s: float = 10.0,
        max_queue: int = 500,
        tiered_storage=None,
        db_path: str = None,
        output_mode: str = "idempotent",
        kafka_producer=None,
        kafka_results_topic: str = "strict_results",
        diff_eviction=None,
        kafka_audit_producer=None,
        kafka_audit_topic: str = "audit_results",
    ):
        self.worker_id = worker_id
        self.partition_ids = partition_ids
        self.engines: dict[int, StrictWatermarkEngine] = {}
        self.buffers: dict[int, BoundedPriorityQueue] = {}
        self.max_event_times: dict[int, float] = {}

        # Idleness detection
        self._last_event_time: dict[int, float] = {}

        # Backpressure
        self._backpressure_active: dict[int, bool] = {}
        self._pending_count: dict[int, int] = {}
        self.backpressure_pause_count: int = 0

        self.known_term: int = 0
        self.seen_commands: set[str] = set()

        from strict.output_manager import OutputManager
        from common.rocks_store import RocksStore

        for pid in partition_ids:
            engine_db = (db_path + f"-p{pid}") if db_path else None
            engine_store = RocksStore(engine_db) if engine_db else None
            om = OutputManager(mode=output_mode, kafka_producer=kafka_producer,
                               kafka_topic=kafka_results_topic,
                               audit_producer=kafka_audit_producer,
                               audit_topic=kafka_audit_topic,
                               store=engine_store)
            eng = StrictWatermarkEngine(
                window_size_s=window_size_s,
                delta_base_s=delta_base_s,
                max_queue=max_queue,
                checkpoint_dir=os.path.join(
                    os.environ.get("CHECKPOINT_DIR", "/data/checkpoint"),
                    f"partition_{pid}"),
                tiered_storage=tiered_storage,
                db_path=engine_db,
                output_manager=om,
                diff_eviction=diff_eviction,
                store=engine_store,
            )
            eng.partition_id = pid  # §9.3: deterministic window_id requires correct partition_id
            self.engines[pid] = eng
            self.buffers[pid] = BoundedPriorityQueue()
            self.max_event_times[pid] = float("-inf")
            self._last_event_time[pid] = time.time()
            self._backpressure_active[pid] = False
            self._pending_count[pid] = 0

    def validate_command(self, term: int, command_id: str) -> bool:
        if term < self.known_term:
            return False  # stale term
        if command_id and command_id in self.seen_commands:
            return False  # duplicate (idempotent)
        self.known_term = max(self.known_term, term)
        if command_id:
            self.seen_commands.add(command_id)
        return True

    def on_punctuation(self, token: PunctuationToken) -> None:
        if token.partition_id in self.engines:
            self.engines[token.partition_id].on_punctuation(token)

    # ---- Idleness detection ----
    def is_idle(self, partition_id: int) -> bool:
        """Return True if no event received for this partition within IDLE_TIMEOUT_S."""
        last = self._last_event_time.get(partition_id)
        if last is None:
            return False
        return time.time() - last > IDLE_TIMEOUT_S

    def idle_partitions(self) -> list[int]:
        """Return list of partition IDs currently considered idle."""
        return [pid for pid in self.partition_ids if self.is_idle(pid)]

    def process(self, event: LogEvent, partition_id: int) -> float | None:
        if partition_id not in self.engines:
            return None

        buf = self.buffers[partition_id]

        # Check backpressure threshold BEFORE accepting event
        if self._backpressure_active.get(partition_id, False):
            if len(buf) < BACKPRESSURE_RESUME_AT:
                self._backpressure_active[partition_id] = False
            return None
        if len(buf) >= BACKPRESSURE_MAX_QUEUE:
            self._backpressure_active[partition_id] = True
            self.backpressure_pause_count += 1
            return None

        event.arrival_time = time.time()

        # Track last event time for idleness detection
        self._last_event_time[partition_id] = time.time()

        ready = buf.push(event)

        # Check backpressure threshold after push
        if len(buf) >= BACKPRESSURE_MAX_QUEUE:
            if not self._backpressure_active[partition_id]:
                self._backpressure_active[partition_id] = True
                self.backpressure_pause_count += 1

        result = None
        if ready:
            result = self._process_event(ready, partition_id)
        else:
            ready = buf.pop_ready()
            if ready:
                result = self._process_event(ready, partition_id)

        # Resume backpressure: clear flag if queue drained below threshold
        if self._backpressure_active.get(partition_id, False):
            if len(buf) < BACKPRESSURE_RESUME_AT:
                self._backpressure_active[partition_id] = False

        return result

    def _process_event(self, event: LogEvent, partition_id: int) -> float | None:
        self.max_event_times[partition_id] = max(
            self.max_event_times[partition_id], event.event_time
        )
        return self.engines[partition_id].process(
            event, len(self.buffers[partition_id])
        )

    def heartbeat(self) -> WorkerHeartbeat:
        partitions = {}
        for pid, eng in self.engines.items():
            partitions[pid] = eng.local_watermark
        return WorkerHeartbeat(
            worker_id=self.worker_id,
            partitions=partitions,
            max_event_time=max(self.max_event_times.values(), default=0.0),
            timestamp=time.time(),
            fencing_token=self.known_term,
            idle_partitions=self.idle_partitions(),
            backpressure_partitions=dict(self._backpressure_active),
        )

    def update_global_watermark(
        self, W_global: float, term: int = 0, command_id: str = ""
    ) -> None:
        if not self.validate_command(term, command_id):
            return
        for eng in self.engines.values():
            eng.watermark = max(eng.watermark, W_global)
            eng.raft_term = max(eng.raft_term, term)

    def summary(self) -> dict:
        total_recv = 0
        total_on_time = 0
        total_late = 0
        total_dupes = 0
        total_bp = 0
        partitions = {}
        for pid, eng in self.engines.items():
            s = eng.summary()
            m = eng.metrics
            total_recv += m.total_received
            total_on_time += m.on_time
            total_late += m.late_dropped
            total_dupes += m.duplicates
            total_bp += m.backpressure_drops
            partitions[pid] = s
        unique = max(total_recv - total_dupes, 1)
        completeness = 100.0 * total_on_time / unique
        late_rate = 100.0 * total_late / max(total_recv, 1)
        return {
            "worker_id": self.worker_id,
            "mode": "strict",
            "total_received": total_recv,
            "on_time": total_on_time,
            "late_dropped": total_late,
            "duplicates": total_dupes,
            "backpressure_drops": total_bp,
            "data_completeness_pct": round(completeness, 3),
            "late_arrival_rate_pct": round(late_rate, 3),
            "non_monotonic_punctuation": sum(
                eng.metrics.non_monotonic_punctuation for eng in self.engines.values()
            ),
            "partitions": partitions,
        }

    def broadcast(self) -> dict:
        partitions = {}
        for pid, eng in self.engines.items():
            partitions[pid] = {
                "watermark": eng.watermark,
                "local_watermark": eng.local_watermark,
                "open_windows": len(eng.open_windows),
                "closed_windows": len(eng.closed_windows),
                "data_completeness_pct": eng.metrics.data_completeness(),
            }
        return {
            "worker_id": self.worker_id,
            "mode": "strict",
            "partitions": partitions,
        }

    def flush_all(self) -> None:
        for pid, buf in self.buffers.items():
            for event in buf.flush_all():
                self._process_event(event, pid)
        for eng in self.engines.values():
            eng.flush()
            eng.checkpoint()
