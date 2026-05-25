"""Replay Sub-Checkpointing — progress tracking during recovery replay.

Spec §8.7: Periodic sub-checkpoints during replay catch-up allow resuming
from interruption mid-replay.
"""

import logging
import time
from dataclasses import dataclass

logger = logging.getLogger("replay_checkpoint")


@dataclass
class ReplayCheckpoint:
    partition_id: int
    start_offset: int = 0
    current_offset: int = 0
    total_to_replay: int = 0
    is_replay_checkpoint: bool = True
    last_checkpoint_time: float = 0.0
    events_since_checkpoint: int = 0

    @property
    def progress_pct(self) -> float:
        if self.total_to_replay <= 0:
            return 0.0
        return self.current_offset / self.total_to_replay

    @property
    def is_complete(self) -> bool:
        return self.current_offset >= self.total_to_replay > 0

    @property
    def is_replay(self) -> bool:
        return self.is_replay_checkpoint

    def to_dict(self) -> dict:
        return {
            "partition_id": self.partition_id,
            "start_offset": self.start_offset,
            "current_offset": self.current_offset,
            "total_to_replay": self.total_to_replay,
            "progress_pct": self.progress_pct,
            "is_complete": self.is_complete,
            "is_replay_checkpoint": self.is_replay_checkpoint,
            # Spec field name aliases
            "replay_progress": self.progress_pct,
            "replay_offset_target": self.total_to_replay,
            "replay_offset_current": self.current_offset,
            "replay_offset_start": self.start_offset,
        }


class ReplayCheckpointManager:
    """Manages periodic sub-checkpoints during replay catch-up."""

    CHECKPOINT_INTERVAL_EVENTS = 1000

    def __init__(self, store=None, db_path: str = None, checkpoint_interval: int = None):
        # db_path: if provided, open a RocksStore for durable checkpoint persistence
        if store is None and db_path is not None:
            try:
                from refactor.common.rocks_store import RocksStore
                store = RocksStore(db_path)
            except Exception:
                pass
        self._store = store
        # checkpoint_interval overrides the class default when provided
        if checkpoint_interval is not None:
            self._checkpoint_interval = checkpoint_interval
        else:
            self._checkpoint_interval = self.CHECKPOINT_INTERVAL_EVENTS
        self._checkpoints: dict[int, ReplayCheckpoint] = {}

    def detect_replay_mode(self, event, watermark: float = float("-inf"),
                           delta_base_s: float = 10.0) -> bool:
        """Return True if the event appears to be a replay (catch-up) event.

        Accepts either a LogEvent (uses event.event_time) or a raw float timestamp.
        Delegates to the module-level detect_replay_mode() function.
        """
        if hasattr(event, "event_time"):
            event_time = event.event_time
        else:
            event_time = float(event)
        return detect_replay_mode(event_time, watermark, delta_base_s)

    def start_replay(self, partition_id: int, start_offset: int, total_to_replay: int):
        ckpt = ReplayCheckpoint(partition_id=partition_id, start_offset=start_offset,
                                current_offset=start_offset, total_to_replay=total_to_replay,
                                is_replay_checkpoint=True,
                                last_checkpoint_time=time.time())
        self._checkpoints[partition_id] = ckpt
        logger.info("Replay started: p=%d, offset=%d, total=%d", partition_id, start_offset, total_to_replay)

    def record_event(self, partition_id: int) -> ReplayCheckpoint | None:
        ckpt = self._checkpoints.get(partition_id)
        if ckpt is None:
            return None
        ckpt.current_offset += 1
        ckpt.events_since_checkpoint += 1
        if ckpt.events_since_checkpoint >= self._checkpoint_interval:
            ckpt.last_checkpoint_time = time.time()
            ckpt.events_since_checkpoint = 0
            if self._store is not None:
                self._store.put(f"replay:ckpt:{partition_id}", ckpt)
            return ckpt
        return None

    def load_checkpoint(self, partition_id: int) -> ReplayCheckpoint | None:
        if self._store is None:
            return None
        ckpt = self._store.get(f"replay:ckpt:{partition_id}")
        if ckpt is not None:
            self._checkpoints[partition_id] = ckpt
            if ckpt.is_replay_checkpoint:
                logger.info("Loaded replay partial checkpoint: p=%d, offset=%d/%d",
                            partition_id, ckpt.current_offset, ckpt.total_to_replay)
        return ckpt

    def finish_replay(self, partition_id: int):
        ckpt = self._checkpoints.pop(partition_id, None)
        if ckpt is not None and self._store is not None:
            self._store.delete(f"replay:ckpt:{partition_id}")
            logger.info("Replay complete: p=%d, events=%d", partition_id,
                        ckpt.current_offset - ckpt.start_offset)

    def is_replaying(self, partition_id: int) -> bool:
        ckpt = self._checkpoints.get(partition_id)
        return ckpt is not None and not ckpt.is_complete

    def summary(self) -> dict:
        return {str(pid): ckpt.to_dict() for pid, ckpt in self._checkpoints.items()}


def detect_replay_mode(event_time: float, watermark: float, delta_base_s: float) -> bool:
    """Event is replay if event_time is more than 2*delta_base behind watermark."""
    if watermark == float("-inf"):
        return False
    return (watermark - event_time) > (2 * delta_base_s)
