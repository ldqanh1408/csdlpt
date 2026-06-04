"""
Định nghĩa dataclass và enum dùng chung trong toàn bộ pipeline.

Bao gồm LogEvent, PunctuationToken, WindowResult, CorrectionMessage, heartbeat worker, trạng thái partition/window và metadata checkpoint/sketch.
"""

from dataclasses import dataclass, field
from enum import Enum


class WatermarkMode(Enum):
    """Lớp `WatermarkMode` định nghĩa các trạng thái/hằng số dùng trong luồng xử lý."""
    STRICT = "strict"
    HEURISTIC = "heuristic"


class WindowStatus(Enum):
    """Lớp `WindowStatus` định nghĩa các trạng thái/hằng số dùng trong luồng xử lý."""
    OPEN = "open"
    CLOSED = "closed"


class WorkerStatus(Enum):
    """Lớp `WorkerStatus` định nghĩa các trạng thái/hằng số dùng trong luồng xử lý."""
    ACTIVE = "active"
    STALE = "stale"
    IDLE = "idle"
    FAILED = "failed"


class EvictionState(Enum):
    """Lớp `EvictionState` định nghĩa các trạng thái/hằng số dùng trong luồng xử lý."""
    CLOSED = "closed"
    UPLOADING = "uploading"
    UPLOADED = "uploaded"
    PURGED = "purged"


class PartitionState(Enum):
    """Lớp `PartitionState` định nghĩa các trạng thái/hằng số dùng trong luồng xử lý."""
    ASSIGNED = "assigned"
    REASSIGNING = "reassigning"
    ORPHANED = "orphaned"
    PAUSED = "paused"


@dataclass
class LogEvent:
    """Lớp `LogEvent` gom dữ liệu và hành vi liên quan đến LogEvent."""
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
    """Lớp `PunctuationToken` gom dữ liệu và hành vi liên quan đến PunctuationToken."""
    T_commit: float
    partition_id: int
    ingestor_id: str
    is_empty: bool = False


@dataclass
class WindowResult:
    """Lớp `WindowResult` gom dữ liệu và hành vi liên quan đến WindowResult."""
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
    """Lớp `CorrectionMessage` gom dữ liệu và hành vi liên quan đến CorrectionMessage."""
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
    """Lớp `WorkerHeartbeat` gom dữ liệu và hành vi liên quan đến WorkerHeartbeat."""
    worker_id: str
    partitions: dict[int, float] = field(default_factory=dict)
    max_event_time: float = 0.0
    timestamp: float = 0.0
    fencing_token: int = 0
    idle_partitions: list[int] = field(default_factory=list)
    backpressure_partitions: dict[int, bool] = field(default_factory=dict)


@dataclass
class AggregatorState:
    """Lớp `AggregatorState` gom dữ liệu và hành vi liên quan đến AggregatorState."""
    partition_id: int
    worker_id: str
    W_h: float
    last_update: float
    status: WorkerStatus


@dataclass
class CheckpointMetadata:
    """Lớp `CheckpointMetadata` gom dữ liệu và hành vi liên quan đến CheckpointMetadata."""
    partition_id: int
    checkpoint_timestamp: float
    kafka_committed_offset: int
    active_windows: list[str] = field(default_factory=list)
    sst_files_manifest: list[str] = field(default_factory=list)
    term_at_checkpoint: int = 0


@dataclass
class SketchCheckpoint:
    """Lớp `SketchCheckpoint` gom dữ liệu và hành vi liên quan đến SketchCheckpoint."""
    alpha: float
    window_seconds: int
    sub_sketches: list[dict] = field(default_factory=list)
    monotonic_W_h: float = 0.0
