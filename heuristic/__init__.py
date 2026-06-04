"""
Package triển khai nhánh Heuristic Watermark.

Nhánh này ưu tiên độ trễ thấp bằng DDSketch để ước lượng phân vị lateness, định tuyến event trễ sang DLQ và phát correction để đạt eventual consistency.
"""

from heuristic.engine import HeuristicWatermarkEngine
from heuristic.aggregator import HeuristicAggregator
from heuristic.dlq import DLQPipeline, CorrectionProtocol
from heuristic.cold_start import ColdStartManager, ColdStartPhase
from heuristic.negative_lag import NegativeLagHandler, LagTier

__all__ = [
    "HeuristicWatermarkEngine",
    "HeuristicAggregator",
    "DLQPipeline",
    "CorrectionProtocol",
    "ColdStartManager",
    "ColdStartPhase",
    "NegativeLagHandler",
    "LagTier",
]
