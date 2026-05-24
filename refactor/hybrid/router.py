"""Hybrid Router — routes events to Strict or Heuristic path based on priority.

Critical events -> Strict (0% loss, punctuation-based, ~15s latency)
Standard events -> Heuristic (<=1% bounded loss, DDSketch-based, ~5s latency)

Unified query across both paths with loss accounting per window.
"""

import time
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

from refactor.common.types import LogEvent, WindowResult, PunctuationToken
from refactor.common.window import TumblingWindow
from refactor.strict.engine import StrictWatermarkEngine
from refactor.heuristic.engine import HeuristicWatermarkEngine
from refactor.heuristic.dlq import DLQPipeline, CorrectionProtocol


class EventPriority(Enum):
    CRITICAL = "critical"
    STANDARD = "standard"


@dataclass
class RouterConfig:
    window_size_s: float = 5.0
    delta_base_s: float = 10.0
    alpha: float = 0.01
    p_normal: float = 0.99
    p_safe: float = 0.999
    L_max: float = 60.0
    max_queue: int = 500
    checkpoint_dir: str = "/tmp/hybrid-checkpoint"
    priority_field: str = "priority"
    default_priority: EventPriority = EventPriority.STANDARD


@dataclass
class LossAccounting:
    """Per-window expected vs actual counts for both paths."""
    window_id: str = ""
    partition_id: int = 0
    strict_expected: int = 0
    strict_actual: int = 0
    strict_loss_pct: float = 0.0
    heuristic_expected: int = 0
    heuristic_actual: int = 0
    heuristic_loss_pct: float = 0.0
    heuristic_dlq_corrected: int = 0
    window_start: float = 0.0
    window_end: float = 0.0


class HybridRouter:
    """Routes events by priority, provides unified window results and loss accounting."""

    def __init__(self, partition_id: int = 0, **kwargs):
        cfg_fields = {f.name for f in RouterConfig.__dataclass_fields__.values()}
        cfg = RouterConfig(**{k: v for k, v in kwargs.items() if k in cfg_fields})
        self.partition_id = partition_id
        self.config = cfg

        self.tumbling = TumblingWindow(cfg.window_size_s)

        # Dual engines
        self.strict_engine = StrictWatermarkEngine(
            window_size_s=cfg.window_size_s,
            delta_base_s=cfg.delta_base_s,
            max_queue=cfg.max_queue,
            checkpoint_dir=cfg.checkpoint_dir + "/strict",
        )
        self.heuristic_engine = HeuristicWatermarkEngine(
            partition_id=partition_id,
            window_size_s=cfg.window_size_s,
            alpha=cfg.alpha,
            p_normal=cfg.p_normal,
            p_safe=cfg.p_safe,
            L_max=cfg.L_max,
        )

        # DLQ for heuristic path corrections
        self.dlq = DLQPipeline()
        self.correction_protocol = CorrectionProtocol(pattern="incremental")

        # Unified window state
        self.closed_windows: dict[str, WindowResult] = {}
        self.loss_accounting: dict[str, LossAccounting] = defaultdict(LossAccounting)

        # Routing stats
        self.route_counts: dict[EventPriority, int] = defaultdict(int)
        self._since_checkpoint: int = 0

    def on_punctuation(self, token: PunctuationToken) -> None:
        self.strict_engine.on_punctuation(token)

    def process(self, event: LogEvent, arrival_time: float = None) -> float | None:
        if arrival_time is None:
            arrival_time = time.time()

        priority = self._resolve_priority(event)
        self.route_counts[priority] += 1

        if priority == EventPriority.CRITICAL:
            return self.strict_engine.process(event)
        else:
            result = self.heuristic_engine.process(event, arrival_time)
            self._drain_heuristic_dlq()
            return result

    def _resolve_priority(self, event: LogEvent) -> EventPriority:
        raw = event.payload.get(self.config.priority_field, None)
        if raw is not None:
            try:
                return EventPriority(str(raw).lower())
            except ValueError:
                pass
        return self.config.default_priority

    def _drain_heuristic_dlq(self) -> None:
        if not self.heuristic_engine.late_events:
            return
        entries = []
        for late in self.heuristic_engine.late_events:
            self.dlq.enqueue(late)
            entries.append(late)
        self.heuristic_engine.late_events.clear()

        if entries and len(entries) >= 10:
            corrections = self.dlq.compute_corrections(
                entries, window_size_s=self.config.window_size_s
            )
            for corr in corrections:
                if not self.correction_protocol.is_duplicate(corr.correction_id):
                    wid = corr.window_id
                    if wid in self.closed_windows:
                        self.correction_protocol.apply_correction(corr, self.closed_windows[wid])
                    if wid in self.loss_accounting:
                        self.loss_accounting[wid].heuristic_dlq_corrected += corr.delta_count

    def close_windows(self) -> list[WindowResult]:
        """Unified window closing — merges results from both paths."""
        newly_closed = []

        for w_start, result in list(self.strict_engine.closed_windows.items()):
            wid = result.window_id
            self.closed_windows[wid] = result
            newly_closed.append(result)
            del self.strict_engine.closed_windows[w_start]

            la = self.loss_accounting[wid]
            la.window_id = wid
            la.partition_id = self.partition_id
            la.strict_actual = result.count
            la.window_start = result.window_start
            la.window_end = result.window_end

        for w_start, result in list(self.heuristic_engine.closed_windows.items()):
            wid = result.window_id
            if wid in self.closed_windows:
                existing = self.closed_windows[wid]
                existing.count += result.count
                existing.status_500 += result.status_500
            else:
                self.closed_windows[wid] = result
                newly_closed.append(result)

            la = self.loss_accounting[wid]
            la.window_id = wid
            la.heuristic_actual = result.count
            la.window_start = result.window_start
            la.window_end = result.window_end

            del self.heuristic_engine.closed_windows[w_start]

        for la in self.loss_accounting.values():
            if la.strict_expected > 0:
                la.strict_loss_pct = 100.0 * max(0, la.strict_expected - la.strict_actual) / la.strict_expected
            if la.heuristic_expected > 0:
                la.heuristic_loss_pct = 100.0 * max(0, la.heuristic_expected - la.heuristic_actual) / la.heuristic_expected

        self._since_checkpoint += len(newly_closed)
        return newly_closed

    def record_expected(self, window_id: str, strict_count: int = 0, heuristic_count: int = 0) -> None:
        la = self.loss_accounting[window_id]
        la.strict_expected += strict_count
        la.heuristic_expected += heuristic_count

    def flush(self) -> None:
        self.heuristic_engine.flush()
        self.strict_engine.flush()
        self.close_windows()

    def summary(self) -> dict:
        return {
            "mode": "hybrid",
            "partition_id": self.partition_id,
            "strict": self.strict_engine.summary(),
            "heuristic": self.heuristic_engine.summary(),
            "route_distribution": {
                "critical": self.route_counts[EventPriority.CRITICAL],
                "standard": self.route_counts[EventPriority.STANDARD],
            },
            "closed_windows": len(self.closed_windows),
            "dlq_backlog": self.dlq.backlog,
        }

    def loss_report(self) -> list[dict]:
        return [
            {
                "window_id": la.window_id,
                "strict_loss_pct": round(la.strict_loss_pct, 3),
                "heuristic_loss_pct": round(la.heuristic_loss_pct, 3),
                "heuristic_dlq_corrected": la.heuristic_dlq_corrected,
            }
            for la in sorted(self.loss_accounting.values(), key=lambda x: x.window_start or 0)
        ]
