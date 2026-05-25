"""Shared dataclasses and enums for the stream processing system."""

from dataclasses import dataclass, field
from enum import Enum


class WatermarkMode(Enum):
    STRICT = "strict"
    HEURISTIC = "heuristic"


class WindowStatus(Enum):
    OPEN = "open"
    CLOSED = "closed"


class WorkerStatus(Enum):
    ACTIVE = "active"
    STALE = "stale"
    IDLE = "idle"
    FAILED = "failed"


class EvictionState(Enum):
    CLOSED = "closed"
    UPLOADING = "uploading"
    UPLOADED = "uploaded"
    PURGED = "purged"


class PartitionState(Enum):
    ASSIGNED = "assigned"
    REASSIGNING = "reassigning"
    ORPHANED = "orphaned"
    PAUSED = "paused"


@dataclass
class LogEvent:
    event_id: str
    event_time: float
    status: int
    payload: dict = field(default_factory=dict)
    arrival_time: float = 0.0
    poll_received_at: float = 0.0
    offset: int = 0
    schema_version: int = 1


@dataclass
class PunctuationToken:
    T_commit: float
    partition_id: int
    ingestor_id: str
    is_empty: bool = False


@dataclass
class WindowResult:
    window_id: str
    partition_id: int
    window_start: float
    window_end: float
    count: int
    status_500: int
    is_speculative: bool
    version: int = 1
    schema_version: int = 1


@dataclass
class CorrectionMessage:
    message_type: str = "WINDOW_CORRECTION"
    window_id: str = ""
    correction_id: str = ""
    previous_count: int = 0
    previous_sum: float = 0.0
    corrected_count: int = 0
    corrected_sum: float = 0.0
    delta_count: int = 0
    delta_sum: float = 0.0
    late_log_ids: list[str] = field(default_factory=list)
    previous_emit_timestamp: float = 0.0
    correction_timestamp: float = 0.0


@dataclass
class WorkerHeartbeat:
    worker_id: str
    partitions: dict[int, float] = field(default_factory=dict)
    max_event_time: float = 0.0
    timestamp: float = 0.0
    fencing_token: int = 0
    idle_partitions: list[int] = field(default_factory=list)
    backpressure_partitions: dict[int, bool] = field(default_factory=dict)


@dataclass
class AggregatorState:
    partition_id: int
    worker_id: str
    W_h: float
    last_update: float
    status: WorkerStatus


@dataclass
class CheckpointMetadata:
    partition_id: int
    checkpoint_timestamp: float
    kafka_committed_offset: int
    active_windows: list[str] = field(default_factory=list)
    sst_files_manifest: list[str] = field(default_factory=list)
    term_at_checkpoint: int = 0


@dataclass
class SketchCheckpoint:
    alpha: float
    window_seconds: int
    sub_sketches: list[dict] = field(default_factory=list)
    monotonic_W_h: float = 0.0
