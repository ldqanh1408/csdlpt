"""DLQ Pipeline - Dead Letter Queue for late-arriving data with correction protocol.

Late events (T_event < W_global_h at arrival) are routed to DLQ instead of
being silently dropped. A correction consumer periodically processes batches
and emits correction messages to downstream sinks.

DLQ entries are persisted in RocksDB (prefix 'dlq:') for durability.
"""

import json
import logging
import threading
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable, Optional

from refactor.common.types import WindowResult, CorrectionMessage
from refactor.common.rocks_store import RocksStore

logger = logging.getLogger("dlq")

_DLQ_PFX = "dlq:"


@dataclass
class DLQEntry:
    event_id: str
    T_event: float
    arrival_time: float
    lag: float
    W_h_at_arrival: float
    lateness: float
    partition_id: int
    original_status: int
    worker_id: str = ""
    payload: dict = field(default_factory=dict)


class DLQPipeline:
    """Stores late events and produces correction messages for downstream reconciliation.

    When a Kafka producer is provided, each late event enqueued to the DLQ is
    also produced to the configured Kafka topic (default: late_logs_dlq).
    """

    def __init__(self, dlq_path: str = "/tmp/dlq", retention_days: int = 7,
                 store: Optional[RocksStore] = None, kafka_producer=None,
                 kafka_topic: str = "late_logs_dlq"):
        self.dlq_path = dlq_path
        self.retention_days = retention_days
        self._entries: list[DLQEntry] = []
        self._corrections_sent: dict[str, CorrectionMessage] = {}
        self._store = store
        self._entry_counter: int = 0
        self._kafka_producer = kafka_producer
        self._kafka_topic = kafka_topic
        # Hourly consumer state
        self.on_corrections_ready: callable | None = None
        self._last_correction_time: float = 0.0
        self._correction_count: int = 0
        # Load existing entries from RocksDB
        if self._store is not None:
            self._load_from_store()

    def _load_from_store(self):
        for key, val in self._store.items(prefix=_DLQ_PFX):
            try:
                if isinstance(val, dict):
                    self._entries.append(DLQEntry(**val))
                else:
                    self._entries.append(val)
                self._entry_counter += 1
            except Exception:
                pass

    def _persist_entry(self, entry: DLQEntry):
        if self._store is None:
            return
        key = f"{_DLQ_PFX}{entry.event_id}"
        self._store.put(key, entry)

    def _delete_entry(self, event_id: str):
        if self._store is None:
            return
        self._store.delete(f"{_DLQ_PFX}{event_id}")

    def enqueue(self, entry: dict) -> None:
        dlq_entry = DLQEntry(
            event_id=entry.get("event_id", ""),
            T_event=entry.get("T_event", 0.0),
            arrival_time=entry.get("arrival_time", 0.0),
            lag=entry.get("lag", 0.0),
            W_h_at_arrival=entry.get("W_h_at_arrival", 0.0),
            lateness=entry.get("lateness", 0.0),
            partition_id=entry.get("partition_id", 0),
            original_status=entry.get("original_status", 0),
            worker_id=entry.get("worker_id", ""),
            payload=entry.get("payload", {}),
        )
        self._entries.append(dlq_entry)
        self._entry_counter += 1
        self._persist_entry(dlq_entry)

        # Also produce to Kafka DLQ topic if producer is configured
        if self._kafka_producer is not None:
            try:
                self._kafka_producer.send(self._kafka_topic, {
                    "event_id": dlq_entry.event_id,
                    "T_event": dlq_entry.T_event,
                    "arrival_time": dlq_entry.arrival_time,
                    "lag": dlq_entry.lag,
                    "W_h_at_arrival": dlq_entry.W_h_at_arrival,
                    "lateness": dlq_entry.lateness,
                    "partition_id": dlq_entry.partition_id,
                    "original_status": dlq_entry.original_status,
                    "worker_id": dlq_entry.worker_id,
                }, key=dlq_entry.event_id, partition=dlq_entry.partition_id)
            except Exception:
                pass

    @property
    def backlog(self) -> int:
        return len(self._entries)

    def oldest_entry_age_s(self) -> float:
        if not self._entries:
            return 0.0
        return time.time() - min(e.arrival_time for e in self._entries)

    def purge_expired(self) -> int:
        """Remove entries older than retention_days. Returns count of purged entries."""
        if not self._entries or self.retention_days <= 0:
            return 0
        cutoff = time.time() - (self.retention_days * 86400)
        expired = [e for e in self._entries if e.arrival_time < cutoff]
        for e in expired:
            self._delete_entry(e.event_id)
            try:
                self._entries.remove(e)
            except ValueError:
                pass
        if expired:
            logger.info("DLQ TTL purge: removed %d entries older than %d days",
                        len(expired), self.retention_days)
        return len(expired)

    def drain(self, batch_size: int = 100) -> list[DLQEntry]:
        """Pop up to batch_size oldest entries from DLQ."""
        if not self._entries:
            return []
        # Sort by arrival_time, take oldest first
        self._entries.sort(key=lambda e: e.arrival_time)
        batch = self._entries[:batch_size]
        self._entries = self._entries[batch_size:]
        # Delete from RocksDB
        for e in batch:
            self._delete_entry(e.event_id)
        return batch

    def compute_corrections(
        self, entries: list[DLQEntry], window_size_s: float = 5.0,
        is_final: bool = False,
        results_lookup: Callable[[str], dict | None] | None = None,
    ) -> list[CorrectionMessage]:
        """Group DLQ entries by window, compute corrections with proper deltas.

        Parameters
        ----------
        is_final : bool
            If True, sets message_type to "FINAL_RECONCILIATION"; otherwise
            uses "WINDOW_CORRECTION".
        results_lookup : callable | None
            Optional callable(window_id) -> dict | None that returns the
            previously emitted result. When provided, previous_count/sum and
            corrected_count/sum are computed from the actual prior result.
        """
        import math
        msg_type = "FINAL_RECONCILIATION" if is_final else "WINDOW_CORRECTION"
        groups: dict[str, list[DLQEntry]] = defaultdict(list)
        for e in entries:
            ws = math.floor(e.T_event / window_size_s) * window_size_s
            key = f"{e.partition_id}_{ws}"
            groups[key].append(e)

        corrections = []
        for window_id, evts in groups.items():
            delta_count = len(evts)
            delta_sum = sum(
                e.payload.get("response", 0) if isinstance(e.payload, dict) else 0
                for e in evts
            )

            previous_count = 0
            previous_sum = 0.0
            previous_emit_ts = 0.0
            if results_lookup is not None:
                try:
                    prev = results_lookup(window_id)
                    if prev is not None and isinstance(prev, dict):
                        previous_count = prev.get("count", 0)
                        previous_sum = prev.get("sum", 0.0)
                        previous_emit_ts = prev.get("emitted_at", 0.0)
                except Exception:
                    pass

            correction = CorrectionMessage(
                message_type=msg_type,
                window_id=window_id,
                correction_id=str(uuid.uuid4()),
                previous_count=previous_count,
                previous_sum=previous_sum,
                corrected_count=previous_count + delta_count,
                corrected_sum=previous_sum + delta_sum,
                delta_count=delta_count,
                delta_sum=delta_sum,
                late_log_ids=[e.event_id for e in evts],
                previous_emit_timestamp=previous_emit_ts,
                correction_timestamp=time.time(),
            )
            corrections.append(correction)
            self._corrections_sent[window_id] = correction
        return corrections

    def dlq_metrics(self) -> dict:
        """Return DLQ monitoring metrics."""
        m = {
            "backlog": self.backlog,
            "oldest_entry_age_s": round(self.oldest_entry_age_s(), 1),
            "total_entries_processed": self._entry_counter,
            "corrections_sent": len(self._corrections_sent),
        }
        if self._kafka_producer is not None:
            m["kafka_topic"] = self._kafka_topic
        return m

    def start_hourly_consumer(self, stop_event: threading.Event) -> None:
        """Start a background thread that drains DLQ entries every hour.

        Every 3600 seconds, this consumer drains up to 1000 entries from the
        DLQ, calls compute_corrections() on them, and invokes the
        on_corrections_ready callback (if set) with the resulting corrections.

        Parameters
        ----------
        stop_event : threading.Event
            When set, the consumer loop exits cleanly.
        """
        def _loop():
            logger.info("Hourly DLQ correction consumer started")
            while not stop_event.is_set():
                try:
                    self.purge_expired()
                    entries = self.drain(batch_size=1000)
                    if entries:
                        corrections = self.compute_corrections(entries, is_final=False)
                        self._last_correction_time = time.time()
                        self._correction_count += len(corrections)
                        logger.info(
                            "Hourly DLQ consumer: drained %d entries, "
                            "produced %d corrections (total corrections: %d)",
                            len(entries), len(corrections), self._correction_count,
                        )
                        if self.on_corrections_ready is not None:
                            self.on_corrections_ready(corrections)
                except Exception:
                    logger.exception("Hourly DLQ consumer error")
                # Sleep in 1s increments so stop_event is checked promptly
                for _ in range(3600):
                    if stop_event.is_set():
                        break
                    time.sleep(1)
            logger.info(
                "Hourly DLQ correction consumer stopped. "
                "Corrections sent: %d", self._correction_count,
            )

        t = threading.Thread(target=_loop, daemon=True, name="dlq-hourly-consumer")
        t.start()

    def save(self) -> None:
        """Save in-memory DLQ to JSON file (backup). RocksDB is write-through."""
        import os
        os.makedirs(os.path.dirname(self.dlq_path), exist_ok=True)
        data = {
            "entries": [{
                "event_id": e.event_id, "T_event": e.T_event,
                "arrival_time": e.arrival_time, "lag": e.lag,
                "W_h_at_arrival": e.W_h_at_arrival, "lateness": e.lateness,
                "partition_id": e.partition_id, "original_status": e.original_status,
                "worker_id": e.worker_id, "payload": e.payload,
            } for e in self._entries],
            "corrections_sent": {
                k: {"window_id": v.window_id, "correction_id": v.correction_id,
                    "delta_count": v.delta_count, "correction_timestamp": v.correction_timestamp}
                for k, v in self._corrections_sent.items()
            },
            "backlog": self.backlog,
        }
        tmp = self.dlq_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, self.dlq_path)


class CorrectionProtocol:
    """Handles correction message delivery to downstream sinks.

    Supports 3 patterns:
    1. Incremental Update - SQL UPDATE with delta
    2. Replace - PUT entire corrected result
    3. Append + Versioning - event log with increasing version
    """

    def __init__(self, pattern: str = "incremental",
                 store: Optional[RocksStore] = None):
        self.pattern = pattern
        self._store = store
        self._processed_corrections: set[str] = set()
        # Load existing corrections from RocksDB on init
        if self._store is not None:
            for key, _val in self._store.items(prefix="correction:"):
                cid = key[len("correction:"):]
                if cid:
                    self._processed_corrections.add(cid)

    def is_duplicate(self, correction_id: str) -> bool:
        if correction_id in self._processed_corrections:
            return True
        # Also check RocksDB for persisted dedup
        if self._store is not None:
            if self._store.get(f"correction:{correction_id}") is not None:
                self._processed_corrections.add(correction_id)
                return True
        self._processed_corrections.add(correction_id)
        # Persist to RocksDB for durability
        if self._store is not None:
            self._store.put(f"correction:{correction_id}",
                          {"processed_at": time.time()})
        return False

    def apply_correction(
        self, correction: CorrectionMessage, current_result: dict
    ) -> dict:
        if self.is_duplicate(correction.correction_id):
            return current_result

        if self.pattern == "incremental":
            return {
                **current_result,
                "count": current_result.get("count", 0) + correction.delta_count,
                "version": current_result.get("version", 1) + 1,
            }
        elif self.pattern == "replace":
            return {
                **current_result,
                "count": correction.corrected_count,
                "version": current_result.get("version", 1) + 1,
            }
        elif self.pattern == "append":
            return {
                **current_result,
                "count": correction.corrected_count,
                "version": current_result.get("version", 1) + 1,
                "correction_history": current_result.get("correction_history", []) + [
                    {
                        "correction_id": correction.correction_id,
                        "delta_count": correction.delta_count,
                        "correction_timestamp": correction.correction_timestamp,
                    }
                ],
            }
        return current_result
