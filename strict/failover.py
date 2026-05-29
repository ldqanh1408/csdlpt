"""Partition Failover Manager — dynamic reassignment on worker failure.

Spec §8.2-8.5:
  - Partition state machine: ASSIGNED -> REASSIGNING -> ORPHANED -> PAUSED
  - Even redistribution: N alive workers each get 12/N partitions
  - Cascading failure protocol
  - 5-step Strict Failback Protocol
"""

import logging
import threading
import time
from dataclasses import dataclass, field
from enum import Enum

from common.types import PartitionState, WorkerStatus

logger = logging.getLogger("failover")


class FailbackStep(Enum):
    """Five-step Strict Failback Protocol (Spec §8.4)."""
    PAUSE = "pause"
    FLUSH_ACK = "flush_ack"
    KAFKA_REASSIGN = "kafka_reassign"
    SEEK_RESUME = "seek_resume"
    COMPLETE = "complete"


class FailoverEvent(Enum):
    WORKER_FAILED = "worker_failed"
    WORKER_RECOVERED = "worker_recovered"
    PARTITION_REASSIGNED = "partition_reassigned"
    FAILBACK_STARTED = "failback_started"
    FAILBACK_COMPLETE = "failback_complete"
    FAILBACK_STEP_PAUSE = "failback_step_pause"
    FAILBACK_STEP_FLUSH_ACK = "failback_step_flush_ack"
    FAILBACK_STEP_KAFKA_REASSIGN = "failback_step_kafka_reassign"
    FAILBACK_STEP_SEEK_RESUME = "failback_step_seek_resume"


@dataclass
class WorkerRecord:
    worker_id: str
    last_heartbeat: float = 0.0
    status: WorkerStatus = WorkerStatus.ACTIVE
    assigned_partitions: set[int] = field(default_factory=set)
    failure_count: int = 0


@dataclass
class FailoverEventRecord:
    event: FailoverEvent
    worker_id: str = ""
    partition_ids: list[int] = field(default_factory=list)
    timestamp: float = 0.0
    details: str = ""


@dataclass
class FailbackState:
    """Tracks the progress of a single partition through the 5-step failback protocol."""
    partition_id: int
    step: FailbackStep
    started_at: float
    last_step_at: float
    from_worker: str   # who currently holds the partition
    to_worker: str     # recovered worker getting the partition back


class FailoverManager:
    """Detects worker failures and reassigns partitions among survivors."""

    def __init__(self, heartbeat_timeout_s: float = 10.0, total_partitions: int = 12,
                 rocks_store: "RocksStore | None" = None):
        self.heartbeat_timeout_s = heartbeat_timeout_s
        self._original_timeout_s = heartbeat_timeout_s
        self.total_partitions = total_partitions
        self._lock = threading.RLock()
        self._workers: dict[str, WorkerRecord] = {}
        self._partition_state: dict[int, PartitionState] = {}
        self._partition_owner: dict[int, str] = {}
        self._original_owner: dict[int, str] = {}
        self._event_log: list[FailoverEventRecord] = []
        self._pending_reassignments: dict[int, str] = {}
        self._failback_states: dict[int, FailbackState] = {}
        self._rocks_store: "RocksStore | None" = rocks_store
        self._all_healthy_since: float = 0.0
        self.on_partition_type_change: callable | None = None
        self._partition_offsets: dict[int, int] = {}

        for pid in range(total_partitions):
            self._partition_state[pid] = PartitionState.ASSIGNED

        # Restore in-progress failback states from RocksDB (coordinator-change resilience)
        if self._rocks_store is not None:
            self.restore_failback_state()

    def register_worker(self, worker_id: str, partition_ids: list[int]) -> None:
        with self._lock:
            if worker_id not in self._workers:
                self._workers[worker_id] = WorkerRecord(worker_id=worker_id)
            wr = self._workers[worker_id]
            wr.last_heartbeat = time.time()
            wr.status = WorkerStatus.ACTIVE
            for pid in partition_ids:
                wr.assigned_partitions.add(pid)
                self._partition_owner[pid] = worker_id
                if pid not in self._original_owner:
                    self._original_owner[pid] = worker_id

    def heartbeat(self, worker_id: str, partition_ids: list[int],
                  offsets: dict[int, int] | None = None) -> None:
        with self._lock:
            if offsets:
                self._partition_offsets.update(offsets)
            if worker_id not in self._workers:
                self.register_worker(worker_id, partition_ids)
                return
            wr = self._workers[worker_id]
            wr.last_heartbeat = time.time()
            wr.status = WorkerStatus.ACTIVE
            wr.failure_count = 0
            for pid in partition_ids:
                wr.assigned_partitions.add(pid)
                self._partition_owner[pid] = worker_id
                if pid not in self._original_owner:
                    self._original_owner[pid] = worker_id
                # Confirm reassignment: if this worker is the expected new owner
                # and the partition is still REASSIGNING, mark it ASSIGNED.
                if (self._partition_state.get(pid) == PartitionState.REASSIGNING
                        and self._pending_reassignments.get(pid) == worker_id):
                    self._partition_state[pid] = PartitionState.ASSIGNED
                    self._pending_reassignments.pop(pid, None)
                    logger.info(
                        "Reassignment confirmed: partition %d now ASSIGNED to %s",
                        pid, worker_id,
                    )

    def detect_failures(self) -> list[str]:
        now = time.time()
        failed = []
        with self._lock:
            for wid, wr in self._workers.items():
                age = now - wr.last_heartbeat
                if age > self.heartbeat_timeout_s:
                    if wr.status != WorkerStatus.FAILED:
                        wr.status = WorkerStatus.FAILED
                        self._log_event(FailoverEvent.WORKER_FAILED, wid,
                                        list(wr.assigned_partitions),
                                        f"Heartbeat timeout after {age:.1f}s")
                    wr.failure_count += 1
                    failed.append(wid)
        self.adjust_timeout()
        return failed

    def reassign_failed_partitions(self) -> dict[int, str]:
        with self._lock:
            alive = [wid for wid, wr in self._workers.items()
                     if wr.status in (WorkerStatus.ACTIVE, WorkerStatus.STALE)]
            orphaned = []
            for pid, owner in self._partition_owner.items():
                wr = self._workers.get(owner)
                if wr is None or wr.status == WorkerStatus.FAILED:
                    if self._partition_state.get(pid) != PartitionState.REASSIGNING:
                        orphaned.append(pid)
                        self._partition_state[pid] = PartitionState.ORPHANED
            if not orphaned or not alive:
                return {}
            reassignments = {}
            for i, pid in enumerate(sorted(orphaned)):
                target = alive[i % len(alive)]
                self._partition_state[pid] = PartitionState.REASSIGNING
                self._partition_owner[pid] = target
                self._pending_reassignments[pid] = target
                self._workers[target].assigned_partitions.add(pid)
                reassignments[pid] = target
                if self.on_partition_type_change is not None:
                    self.on_partition_type_change(pid, "recovery")
                self._log_event(FailoverEvent.PARTITION_REASSIGNED, target, [pid],
                                "Reassigned from failed worker")
            return reassignments

    def cascading_failover(self, failed_workers: list[str]) -> dict[int, str]:
        if len(failed_workers) <= 1:
            return self.reassign_failed_partitions()
        logger.warning("Cascading failure: %d workers failed (%s)",
                       len(failed_workers), ", ".join(failed_workers))
        with self._lock:
            alive = [wid for wid in self._workers if wid not in failed_workers]
            if not alive:
                logger.critical("No alive workers — cluster down")
                return {}
            all_orphaned = []
            for wid in failed_workers:
                wr = self._workers.get(wid)
                if wr:
                    for pid in list(wr.assigned_partitions):
                        if self._partition_state.get(pid) not in (PartitionState.PAUSED,):
                            all_orphaned.append(pid)
                            self._partition_state[pid] = PartitionState.ORPHANED
            reassignments = {}
            for i, pid in enumerate(sorted(set(all_orphaned))):
                target = alive[i % len(alive)]
                self._partition_state[pid] = PartitionState.REASSIGNING
                self._partition_owner[pid] = target
                self._pending_reassignments[pid] = target
                self._workers[target].assigned_partitions.add(pid)
                reassignments[pid] = target
                if self.on_partition_type_change is not None:
                    self.on_partition_type_change(pid, "recovery")
            self._log_event(FailoverEvent.PARTITION_REASSIGNED, "",
                            list(reassignments.keys()),
                            f"Cascading: {len(reassignments)} partitions -> {len(alive)} workers")
            return reassignments

    # ------------------------------------------------------------------
    # 5-step Strict Failback Protocol  (§8.4)
    # ------------------------------------------------------------------

    def start_failback(self, recovered_worker: str) -> dict:
        """Begin failback for all partitions originally owned by *recovered_worker*.

        Only executes Step 1 (PAUSE) — subsequent steps are advanced via
        :meth:`advance_failback` one partition at a time.
        """
        with self._lock:
            wr = self._workers.get(recovered_worker)
            if wr is None:
                return {"error": f"Unknown worker: {recovered_worker}"}
            wr.status = WorkerStatus.ACTIVE
            wr.last_heartbeat = time.time()

            now = time.time()
            started_partitions: list[int] = []

            for pid, current_owner in list(self._partition_owner.items()):
                if self._original_owner.get(pid) != recovered_worker:
                    continue
                if current_owner == recovered_worker:
                    continue  # already owned, nothing to failback

                # Step 1 PAUSE: mark paused, create failback state tracking
                self._partition_state[pid] = PartitionState.PAUSED
                fb_state = FailbackState(
                    partition_id=pid,
                    step=FailbackStep.PAUSE,
                    started_at=now,
                    last_step_at=now,
                    from_worker=current_owner,
                    to_worker=recovered_worker,
                )
                self._failback_states[pid] = fb_state
                started_partitions.append(pid)

                self._log_event(
                    FailoverEvent.FAILBACK_STEP_PAUSE,
                    recovered_worker,
                    [pid],
                    f"Quiesce partition {pid}: {current_owner} -> {recovered_worker}",
                )

            if started_partitions:
                self._log_event(
                    FailoverEvent.FAILBACK_STARTED,
                    recovered_worker,
                    started_partitions,
                    f"Step 1 PAUSE: Quiesced {len(started_partitions)} partition(s)",
                )
                self._persist_all_failback()

            next_actions = {
                str(pid): {"current_step": fb.step.value,
                           "next_step": FailbackStep.FLUSH_ACK.value,
                           "from_worker": fb.from_worker,
                           "to_worker": fb.to_worker}
                for pid, fb in self._failback_states.items()
            }

            return {
                "worker_id": recovered_worker,
                "quiesced_partitions": started_partitions,
                "step": "pause",
                "next_action": "Call advance_failback(partition_id) for each partition",
                "pending": next_actions,
            }

    def advance_failback(self, partition_id: int) -> dict:
        """Advance a partition one step through the 5-step failback protocol.

        Returns the *current* step (after advancing) and the next action required.
        When COMPLETE is reached the partition is reassigned and its failback
        state is removed.
        """
        _STEP_EVENT = {
            FailbackStep.PAUSE: FailoverEvent.FAILBACK_STEP_PAUSE,
            FailbackStep.FLUSH_ACK: FailoverEvent.FAILBACK_STEP_FLUSH_ACK,
            FailbackStep.KAFKA_REASSIGN: FailoverEvent.FAILBACK_STEP_KAFKA_REASSIGN,
            FailbackStep.SEEK_RESUME: FailoverEvent.FAILBACK_STEP_SEEK_RESUME,
        }
        _NEXT_STEP = {
            FailbackStep.PAUSE: FailbackStep.FLUSH_ACK,
            FailbackStep.FLUSH_ACK: FailbackStep.KAFKA_REASSIGN,
            FailbackStep.KAFKA_REASSIGN: FailbackStep.SEEK_RESUME,
            FailbackStep.SEEK_RESUME: FailbackStep.COMPLETE,
        }

        with self._lock:
            fb = self._failback_states.get(partition_id)
            if fb is None:
                return {"error": f"Partition {partition_id} is not in failback",
                        "step": "unknown"}

            if fb.step == FailbackStep.COMPLETE:
                return {"partition_id": partition_id, "step": "complete",
                        "message": "Already complete"}

            now = time.time()
            prev_step = fb.step
            next_step = _NEXT_STEP[fb.step]

            if next_step == FailbackStep.COMPLETE:
                # Step 4 -> 5: Reassign partition to original owner
                recovered = fb.to_worker
                wr = self._workers.get(recovered)
                self._partition_owner[partition_id] = recovered
                self._partition_state[partition_id] = PartitionState.ASSIGNED
                if wr is not None:
                    wr.assigned_partitions.add(partition_id)
                # Also remove from the surrogate owner's set
                if fb.from_worker in self._workers:
                    self._workers[fb.from_worker].assigned_partitions.discard(partition_id)
                self._pending_reassignments.pop(partition_id, None)

                self._log_event(
                    FailoverEvent.FAILBACK_COMPLETE,
                    recovered,
                    [partition_id],
                    f"Failback complete: partition {partition_id} assigned to {recovered}",
                )

                # Remove from in-memory and persisted failback state
                del self._failback_states[partition_id]
                self._delete_persisted_failback(partition_id)
                if self.on_partition_type_change is not None:
                    self.on_partition_type_change(partition_id, "normal")
            else:
                # Intermediate step transition
                fb.step = next_step
                fb.last_step_at = now

                event = _STEP_EVENT.get(next_step)
                if event:
                    self._log_event(
                        event,
                        fb.to_worker,
                        [partition_id],
                        f"Step {prev_step.value} -> {next_step.value}: "
                        f"partition {partition_id} ({fb.from_worker} -> {fb.to_worker})",
                    )

                self._persist_failback_state(partition_id)

            return {
                "partition_id": partition_id,
                "current_step": (next_step if next_step != FailbackStep.COMPLETE
                                 else FailbackStep.COMPLETE).value,
                "previous_step": prev_step.value,
                "from_worker": fb.from_worker,
                "to_worker": fb.to_worker,
                "next_action": ("Done" if next_step == FailbackStep.COMPLETE
                                else f"Call advance_failback({partition_id}) to proceed to "
                                     f"{_NEXT_STEP.get(next_step, FailbackStep.COMPLETE).value}"),
            }

    # ------------------------------------------------------------------
    # Failback persistence  (RocksDB, coordinator-change resilience)
    # ------------------------------------------------------------------

    _FBPFX = "fb:"  # RocksDB key prefix for failback state entries

    def _persist_failback_state(self, partition_id: int) -> None:
        """Write a single failback state to RocksDB."""
        if self._rocks_store is None:
            return
        fb = self._failback_states.get(partition_id)
        if fb is not None:
            self._rocks_store.put(f"{self._FBPFX}{partition_id}", fb)
        else:
            self._rocks_store.delete(f"{self._FBPFX}{partition_id}")

    def _persist_all_failback(self) -> None:
        """Write all current failback states to RocksDB (bulk)."""
        if self._rocks_store is None:
            return
        # Clear old persisted failback keys, then re-write current set
        self._rocks_store.clear_prefix(self._FBPFX)
        for pid, fb in self._failback_states.items():
            self._rocks_store.put(f"{self._FBPFX}{pid}", fb)

    def _delete_persisted_failback(self, partition_id: int) -> None:
        """Remove a completed failback state from RocksDB."""
        if self._rocks_store is None:
            return
        self._rocks_store.delete(f"{self._FBPFX}{partition_id}")

    def persist_failback_state(self) -> int:
        """Persist all in-progress failback states to RocksDB.

        Returns the number of partitions persisted.
        Called before a coordinator handoff or on a regular interval
        to enable coordinator-change resilience.
        """
        with self._lock:
            if self._rocks_store is None:
                return 0
            self._persist_all_failback()
            return len(self._failback_states)

    def restore_failback_state(self) -> int:
        """Restore in-progress failback states from RocksDB into memory.

        Returns the number of partitions restored.
        Called on coordinator startup or after a leadership change.
        """
        if self._rocks_store is None:
            return 0
        count = 0
        with self._lock:
            for key, fb_state in self._rocks_store.items(prefix=self._FBPFX):
                try:
                    if isinstance(fb_state, FailbackState):
                        pid = fb_state.partition_id
                        self._failback_states[pid] = fb_state
                        count += 1
                        logger.info(
                            "Restored failback state: partition %d at step %s "
                            "(%s -> %s)",
                            pid, fb_state.step.value,
                            fb_state.from_worker, fb_state.to_worker,
                        )
                except Exception:
                    logger.warning("Failed to restore failback state from key %s", key)
            return count

    def resume_failback(self) -> list[dict]:
        """Scan persisted states and resume any in-progress failbacks.

        Returns a list of partitions that need to continue failback.
        A new coordinator leader calls this after election to pick up
        where the previous leader left off.
        """
        restored = self.restore_failback_state()
        pending: list[dict] = []
        with self._lock:
            for pid, fb in self._failback_states.items():
                # Ensure the partition is still in the expected state
                current_state = self._partition_state.get(pid)
                if current_state not in (PartitionState.PAUSED, PartitionState.ASSIGNED):
                    self._partition_state[pid] = PartitionState.PAUSED
                pending.append({
                    "partition_id": pid,
                    "current_step": fb.step.value,
                    "from_worker": fb.from_worker,
                    "to_worker": fb.to_worker,
                    "started_at": fb.started_at,
                    "last_step_at": fb.last_step_at,
                    "next_action": (f"Call advance_failback({pid})"
                                    if fb.step != FailbackStep.COMPLETE
                                    else "Complete"),
                })
            logger.info("resume_failback: restored %d state(s), %d pending",
                        restored, len(pending))
            return pending

    def failback_summary(self) -> dict:
        """Return current failback progress across all partitions."""
        with self._lock:
            by_step: dict[str, list[int]] = {}
            for pid, fb in self._failback_states.items():
                step_name = fb.step.value
                by_step.setdefault(step_name, []).append(pid)
            total = len(self._failback_states)
            return {
                "in_progress": total,
                "by_step": {k: sorted(v) for k, v in sorted(by_step.items())},
                "details": [
                    {
                        "partition_id": pid,
                        "step": fb.step.value,
                        "from_worker": fb.from_worker,
                        "to_worker": fb.to_worker,
                        "started_at": fb.started_at,
                        "elapsed_s": round(time.time() - fb.started_at, 2),
                    }
                    for pid, fb in sorted(self._failback_states.items())
                ],
            }

    def get_partition_state(self, partition_id: int) -> PartitionState:
        with self._lock:
            return self._partition_state.get(partition_id, PartitionState.ASSIGNED)

    def get_partition_owner(self, partition_id: int) -> str | None:
        with self._lock:
            return self._partition_owner.get(partition_id)

    def get_partition_types(self) -> dict[int, str]:
        """Return {pid: 'recovery'|'normal'} based on current owner vs original.

        Partitions owned by a different worker than their original owner are
        in 'recovery' mode and should use aggressive eviction.
        """
        with self._lock:
            return {
                pid: ("recovery" if self._partition_owner.get(pid) != self._original_owner.get(pid)
                      else "normal")
                for pid in range(self.total_partitions)
            }

    def get_recovery_offset(self, partition_id: int) -> int:
        """Return the last known Kafka offset for *partition_id*."""
        with self._lock:
            return self._partition_offsets.get(partition_id, 0)

    def get_partition_recovery_info(self, partition_id: int) -> dict:
        """Return recovery metadata for a survivor taking over *partition_id*.

        Returns dict with ``original_worker``, ``last_offset``, and a simulated
        ``checkpoint_path`` so the receiving worker knows what offset to seek to.
        """
        with self._lock:
            orig = self._original_owner.get(partition_id, "unknown")
            curr = self._partition_owner.get(partition_id, "unknown")
            return {
                "original_worker": orig,
                "original_owner": orig,
                "current_owner": curr,
                "last_offset": self._partition_offsets.get(partition_id, 0),
                "checkpoint_path": f"/data/checkpoint/partition-{partition_id}/checkpoint.json",
            }

    def alive_workers(self) -> list[str]:
        with self._lock:
            return [wid for wid, wr in self._workers.items()
                    if wr.status in (WorkerStatus.ACTIVE, WorkerStatus.STALE)]

    def adjust_timeout(self) -> None:
        """Adaptive failover: reduce heartbeat timeout during cascading failures.

        When >1 worker has failed, reduce timeout by 30% (min 3s) to detect
        and respond to cascading failures faster. When all workers have been
        healthy for > 60s, restore the original configured timeout.
        """
        now = time.time()
        with self._lock:
            failed_count = sum(
                1 for wr in self._workers.values()
                if wr.status == WorkerStatus.FAILED
            )
            alive = [wid for wid, wr in self._workers.items()
                     if wr.status in (WorkerStatus.ACTIVE, WorkerStatus.STALE)]
            all_healthy = len(alive) == len(self._workers) and failed_count == 0

            if failed_count > 1:
                reduced = max(3.0, self._original_timeout_s * 0.7)
                if self.heartbeat_timeout_s != reduced:
                    logger.warning(
                        "Adaptive failover: %d workers failed, reducing timeout "
                        "%.1fs -> %.1fs (min 3s)",
                        failed_count, self.heartbeat_timeout_s, reduced,
                    )
                    self.heartbeat_timeout_s = reduced

            if all_healthy:
                if self._all_healthy_since == 0.0:
                    self._all_healthy_since = now
                elif now - self._all_healthy_since > 60.0:
                    if self.heartbeat_timeout_s != self._original_timeout_s:
                        logger.info(
                            "Adaptive failover: all workers healthy for >60s, "
                            "restoring timeout %.1fs -> %.1fs",
                            self.heartbeat_timeout_s, self._original_timeout_s,
                        )
                        self.heartbeat_timeout_s = self._original_timeout_s
                    self._all_healthy_since = 0.0
            else:
                self._all_healthy_since = 0.0

    def _log_event(self, event: FailoverEvent, worker_id: str,
                   partition_ids: list[int], details: str = ""):
        record = FailoverEventRecord(event=event, worker_id=worker_id,
                                     partition_ids=partition_ids,
                                     timestamp=time.time(), details=details)
        self._event_log.append(record)
        if len(self._event_log) > 200:
            self._event_log = self._event_log[-100:]
        msg = f"[failover] EVENT: {event.value} | worker={worker_id} | partitions={partition_ids} | details={details}"
        logger.warning(msg)
        import sys
        print(msg, file=sys.stderr, flush=True)

    def recent_events(self, count: int = 20) -> list[dict]:
        with self._lock:
            return [{"event": e.event.value, "worker": e.worker_id,
                     "partitions": e.partition_ids, "time": e.timestamp,
                     "details": e.details} for e in self._event_log[-count:]]

    def summary(self) -> dict:
        with self._lock:
            alive = [wid for wid, wr in self._workers.items()
                     if wr.status in (WorkerStatus.ACTIVE, WorkerStatus.STALE)]
            return {
                "workers": {
                    wid: {"status": wr.status.value, "partitions": sorted(wr.assigned_partitions),
                          "last_heartbeat_s": round(time.time() - wr.last_heartbeat, 1),
                          "failure_count": wr.failure_count}
                    for wid, wr in self._workers.items()
                },
                "partition_states": {str(pid): state.value for pid, state in self._partition_state.items()},
                "pending_reassignments": {str(pid): target for pid, target in self._pending_reassignments.items()},
                "alive_workers": alive,
            }
