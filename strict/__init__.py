"""
Package triển khai nhánh Strict Watermark.

Nhánh này ưu tiên không mất dữ liệu bằng punctuation, global watermark qua coordinator, checkpoint, exactly-once output, failover/failback và disaster recovery.
"""

from strict.engine import StrictWatermarkEngine, WindowState
from strict.coordinator import StrictCoordinator, PartitionInfo
from strict.worker import StrictWorker, BoundedPriorityQueue
from strict.ingestor_health import IngestorHealthMonitor, HealthRecord

__all__ = [
    "StrictWatermarkEngine",
    "WindowState",
    "StrictCoordinator",
    "PartitionInfo",
    "StrictWorker",
    "BoundedPriorityQueue",
    "IngestorHealthMonitor",
    "HealthRecord",
]
