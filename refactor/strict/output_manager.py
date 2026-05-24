"""Window Output Manager — emission with Exactly-Once guarantees.

Spec §9.2-9.3: Idempotent Sink (dedup key = window_id) and
Transactional Sink (Two-Phase Commit via MockTransactionalSink).
"""

import logging
import threading
import time
from enum import Enum
from typing import Optional

from refactor.common.types import WindowResult
from refactor.common.rocks_store import RocksStore

logger = logging.getLogger("output_manager")


class TxState(Enum):
    IDLE = "idle"
    BEGIN = "begin"
    PRE_COMMIT = "pre_commit"
    COMMITTED = "committed"
    ROLLED_BACK = "rolled_back"


class MockTransactionalSink:
    """Simulates Kafka transactional producer for exactly-once window output.

    Two-phase commit protocol: begin_tx -> pre_commit -> commit.
    Supports crash recovery via recover() that scans in-progress transactions.
    """

    def __init__(self, store: Optional[RocksStore] = None):
        self._lock = threading.Lock()
        self._committed: dict[str, dict] = {}
        self._pre_committed: dict[str, dict] = {}   # tx_id -> {window_id, data, timestamp}
        self._in_progress: dict[str, dict] = {}      # tx_id -> {window_id, state, timestamp}
        self._tx_counter: int = 0
        self._store: Optional[RocksStore] = store

        # Restore from RocksDB if available (crash recovery)
        if self._store is not None:
            self._load_tx_state()

    def _load_tx_state(self) -> None:
        """Load _pre_committed and _in_progress from RocksDB on recovery."""
        if self._store is None:
            return
        for key, val in self._store.items(prefix="tx:pre:"):
            tx_id = key[len("tx:pre:"):]
            self._pre_committed[tx_id] = val
        for key, val in self._store.items(prefix="tx:inprog:"):
            tx_id = key[len("tx:inprog:"):]
            self._in_progress[tx_id] = val

    def _persist_tx_state(self) -> None:
        """Persist _pre_committed and _in_progress to RocksDB.

        Called after every state change to ensure durability of in-flight
        transactions across restarts.
        """
        if self._store is None:
            return
        # Clear old entries, then rewrite current state
        self._store.clear_prefix("tx:pre:")
        self._store.clear_prefix("tx:inprog:")
        for tx_id, entry in self._pre_committed.items():
            self._store.put(f"tx:pre:{tx_id}", entry)
        for tx_id, entry in self._in_progress.items():
            self._store.put(f"tx:inprog:{tx_id}", entry)

    def begin_tx(self, window_id: str) -> str:
        with self._lock:
            self._tx_counter += 1
            tx_id = f"tx-{self._tx_counter:06d}"
            self._in_progress[tx_id] = {
                "window_id": window_id,
                "state": TxState.BEGIN.value,
                "timestamp": time.time(),
            }
            self._persist_tx_state()
            return tx_id

    def pre_commit(self, tx_id: str, window_id: str, data: dict) -> bool:
        """Store the transaction in PRE_COMMIT state (phase 1 of 2PC).

        On crash, recover() will re-commit PRE_COMMIT transactions whose data
        was not yet written to _committed.
        """
        with self._lock:
            self._pre_committed[tx_id] = {
                "window_id": window_id,
                "data": data,
                "timestamp": time.time(),
            }
            if tx_id in self._in_progress:
                self._in_progress[tx_id]["state"] = TxState.PRE_COMMIT.value
            self._persist_tx_state()
            logger.debug("TX PRE_COMMIT: %s for window %s", tx_id, window_id)
            return True

    def commit(self, tx_id: str, window_id: str, data: dict) -> bool:
        with self._lock:
            if window_id in self._committed:
                return False
            self._committed[window_id] = data
            # Clean up tracking state after successful commit
            self._pre_committed.pop(tx_id, None)
            self._in_progress.pop(tx_id, None)
            self._persist_tx_state()
            logger.debug("TX COMMIT: %s for window %s", tx_id, window_id)
            return True

    def rollback(self, tx_id: str) -> bool:
        with self._lock:
            self._pre_committed.pop(tx_id, None)
            self._in_progress.pop(tx_id, None)
            self._persist_tx_state()
        logger.debug("TX ROLLBACK: %s", tx_id)
        return True

    def is_committed(self, window_id: str) -> bool:
        with self._lock:
            return window_id in self._committed

    def recover(self) -> int:
        """Scan in-progress transactions on restart and resolve them.

        Recovery rules:
          - If window_id is already committed: discard the pre_commit entry
          - If in PRE_COMMIT state: re-commit the data
          - If in BEGIN state (no pre_commit): rollback (discard)

        Returns the number of transactions recovered.
        """
        # First, load any persisted state from RocksDB (crash recovery)
        if self._store is not None:
            self._load_tx_state()

        with self._lock:
            recovered = 0

            # Recover PRE_COMMIT transactions
            for tx_id, entry in list(self._pre_committed.items()):
                window_id = entry["window_id"]
                if window_id in self._committed:
                    # Already committed, safe to discard
                    self._pre_committed.pop(tx_id, None)
                    self._in_progress.pop(tx_id, None)
                    logger.debug("TX RECOVER: %s already committed, discarding pre_commit", tx_id)
                else:
                    # Re-commit the pre_committed data
                    self._committed[window_id] = entry["data"]
                    self._pre_committed.pop(tx_id, None)
                    self._in_progress.pop(tx_id, None)
                    recovered += 1
                    logger.info("TX RECOVER: re-committed %s for window %s", tx_id, window_id)

            # Rollback any BEGIN-only transactions (no pre_commit)
            for tx_id, entry in list(self._in_progress.items()):
                if entry.get("state") == TxState.BEGIN.value:
                    self._in_progress.pop(tx_id, None)
                    self._pre_committed.pop(tx_id, None)
                    logger.info("TX RECOVER: rolled back BEGIN tx %s", tx_id)

            return recovered

    def summary(self) -> dict:
        with self._lock:
            return {
                "committed_windows": len(self._committed),
                "total_tx": self._tx_counter,
                "pre_committed_pending": len(self._pre_committed),
                "in_progress": len(self._in_progress),
            }


class OutputManager:
    """Manages window result emission with dedup-key-based exactly-once.

    When a Kafka producer is provided, emitted windows are also produced to
    the configured results topic (e.g. strict_results, heuristic_results).

    Optional audit sink: when audit_producer is set, a copy of every
    committed window is also emitted to the audit topic (best-effort,
    fire-and-forget).
    """

    def __init__(self, mode: str = "idempotent", kafka_producer=None,
                 kafka_topic: str = "strict_results",
                 audit_producer=None, audit_topic: str = "audit_results"):
        self.mode = mode
        self._emitted: set[str] = set()
        self._lock = threading.Lock()
        self._sink = MockTransactionalSink() if mode == "transactional" else None
        self._emission_log: list[dict] = []
        self._emission_count: int = 0
        self._kafka_producer = kafka_producer
        self._kafka_topic = kafka_topic
        self._audit_producer = audit_producer
        self._audit_topic = audit_topic

    def _emit_audit(self, data: dict, window_id: str, partition_id: int) -> None:
        """Fire-and-forget audit emission — best-effort, does not block."""
        if self._audit_producer is None:
            return
        try:
            self._audit_producer.send(self._audit_topic, dict(data),
                                      key=window_id,
                                      partition=partition_id)
        except Exception:
            logger.debug("Audit produce failed for window %s", window_id)

    def emit(self, result: WindowResult) -> bool:
        with self._lock:
            if result.window_id in self._emitted:
                return False

            data = {
                "window_id": result.window_id, "partition_id": result.partition_id,
                "window_start": result.window_start, "window_end": result.window_end,
                "count": result.count, "status_500": result.status_500,
                "is_speculative": result.is_speculative, "version": result.version,
                "emitted_at": time.time(),
            }

            if self.mode == "transactional" and self._sink is not None:
                tx_id = self._sink.begin_tx(result.window_id)
                if not self._sink.pre_commit(tx_id, result.window_id, data):
                    self._sink.rollback(tx_id)
                    return False
                ok = self._sink.commit(tx_id, result.window_id, data)
                if not ok:
                    self._sink.rollback(tx_id)
                    return False

            # Also produce to Kafka results topic if producer is configured
            if self._kafka_producer is not None:
                try:
                    self._kafka_producer.send(self._kafka_topic, data,
                                              key=result.window_id,
                                              partition=result.partition_id)
                except Exception:
                    logger.debug("Kafka produce failed for window %s", result.window_id)

            # Fire-and-forget audit emission (best-effort)
            self._emit_audit(data, result.window_id, result.partition_id)

            self._emitted.add(result.window_id)
            self._emission_count += 1
            self._emission_log.append({"window_id": result.window_id, "count": result.count, "timestamp": time.time()})
            if len(self._emission_log) > 1000:
                self._emission_log = self._emission_log[-500:]
            return True

    def is_emitted(self, window_id: str) -> bool:
        with self._lock:
            return window_id in self._emitted

    def summary(self) -> dict:
        with self._lock:
            s = {"mode": self.mode, "total_emitted": self._emission_count, "tracked_windows": len(self._emitted)}
            if self._sink is not None:
                s["sink"] = self._sink.summary()
            if self._kafka_producer is not None:
                s["kafka_topic"] = self._kafka_topic
            if self._audit_producer is not None:
                s["audit_topic"] = self._audit_topic
            return s
