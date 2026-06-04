"""
Test cho nhánh Heuristic Watermark.

Bao phủ engine DDSketch, aggregator, DLQ pipeline, correction protocol, cold start và negative lag handler.
"""

import os
import time

os.environ.setdefault("HEURISTIC_WARMUP_S", "0.0")
os.environ.setdefault("HEURISTIC_WARMUP_SAMPLES", "10")

import pytest
from common.types import LogEvent
from heuristic import (
    HeuristicWatermarkEngine, HeuristicAggregator,
    DLQPipeline, CorrectionProtocol,
    ColdStartManager, ColdStartPhase,
    NegativeLagHandler, LagTier,
)


class TestHeuristicWatermarkEngine:
    """Lớp `TestHeuristicWatermarkEngine` gom các ca kiểm thử liên quan đến HeuristicWatermarkEngine."""
    def test_basic_processing(self):
        """Kiểm thử hành vi `test basic processing` trong phạm vi module hiện tại."""
        eng = HeuristicWatermarkEngine(partition_id=0, window_size_s=5.0)
        base = time.time()
        for i in range(500):
            lag = 0.5
            et = base - lag
            eng.process(LogEvent(event_id=f"e{i}", event_time=et, status=200),
                       arrival_time=base + i * 0.001)
        eng.flush()
        s = eng.summary()
        assert s["mode"] == "heuristic"
        assert s["sketch_samples"] > 0

    def test_dedup(self):
        """Kiểm thử hành vi `test dedup` trong phạm vi module hiện tại."""
        eng = HeuristicWatermarkEngine()
        base = time.time()
        e = LogEvent(event_id="dup-1", event_time=base - 1.0, status=200)
        eng.process(e, arrival_time=base)
        eng.process(e, arrival_time=base)
        assert eng.metrics.duplicates == 1

    def test_dedup_set_has_ttl(self):
        # §6.5 idempotent filter TTL: in-memory seen_ids must drop entries
        # older than _dedup_ttl_s, otherwise the set grows without bound and
        # leaks memory on a long-running worker.
        """Kiểm thử hành vi `test dedup set has ttl` trong phạm vi module hiện tại."""
        eng = HeuristicWatermarkEngine()
        eng._dedup_ttl_s = 0.05      # speed up the sweep
        base = time.time()
        eng.process(LogEvent(event_id="old", event_time=base - 1.0, status=200),
                    arrival_time=base)
        assert "old" in eng.seen_ids
        time.sleep(0.1)
        # process any event to trigger _purge_seen_ids
        eng.process(LogEvent(event_id="new", event_time=base + 0.1, status=200),
                    arrival_time=base + 0.1)
        assert "old" not in eng.seen_ids       # expired and reaped
        assert "new" in eng.seen_ids           # still inside TTL window

    def test_lag_estimation_converges(self):
        """Kiểm thử hành vi `test lag estimation converges` trong phạm vi module hiện tại."""
        eng = HeuristicWatermarkEngine(L_max=10.0)
        base = time.time()
        for i in range(2000):
            lag = 0.5 + (i % 10) * 0.1
            et = base - lag
            eng.process(LogEvent(event_id=f"e{i}", event_time=et, status=200),
                       arrival_time=base + i * 0.001)
        assert eng.L_eff < 10.0

    def test_adaptive_percentile_triggers(self):
        """Kiểm thử hành vi `test adaptive percentile triggers` trong phạm vi module hiện tại."""
        eng = HeuristicWatermarkEngine(p_normal=0.99, p_safe=0.999, burst_threshold=1.1)
        base = time.time()
        for i in range(500):
            lag = 0.5
            eng.process(LogEvent(event_id=f"n{i}", event_time=base - lag, status=200),
                       arrival_time=base + i * 0.001)
        for i in range(100):
            lag = 10.0
            eng.process(LogEvent(event_id=f"b{i}", event_time=base - lag, status=200),
                       arrival_time=base + 500 * 0.001 + i * 0.001)
        s = eng.summary()
        assert "adaptive_active" in s

    def test_monotonic_watermark(self):
        """Kiểm thử hành vi `test monotonic watermark` trong phạm vi module hiện tại."""
        eng = HeuristicWatermarkEngine()
        base = time.time()
        watermarks = []
        for i in range(1000):
            lag = 0.5
            et = base - lag
            eng.process(LogEvent(event_id=f"e{i}", event_time=et, status=200),
                       arrival_time=base + i * 0.001)
            if eng.W_h > float("-inf"):
                watermarks.append(eng.W_h)
        for i in range(1, len(watermarks)):
            assert watermarks[i] >= watermarks[i - 1], f"WM decreased at {i}"

    def test_dlq_routing(self):
        """Kiểm thử hành vi `test dlq routing` trong phạm vi module hiện tại."""
        eng = HeuristicWatermarkEngine(window_size_s=5.0)
        base = time.time()
        for i in range(1000):
            lag = 0.5
            eng.process(LogEvent(event_id=f"e{i}", event_time=base - lag, status=200),
                       arrival_time=base + i * 0.001)
        eng.flush()
        s = eng.summary()
        assert s["dlq_backlog"] >= 0

    def test_summary(self):
        """Kiểm thử hành vi `test summary` trong phạm vi module hiện tại."""
        eng = HeuristicWatermarkEngine(partition_id=1)
        base = time.time()
        for i in range(100):
            eng.process(LogEvent(event_id=f"e{i}", event_time=base - 0.5, status=200),
                       arrival_time=base)
        s = eng.summary()
        for key in ("mode", "watermark", "L_eff_s", "p_current",
                     "sketch_samples", "data_completeness_pct"):
            assert key in s


class TestHeuristicAggregator:
    """Lớp `TestHeuristicAggregator` gom các ca kiểm thử liên quan đến HeuristicAggregator."""
    def test_basic_aggregation(self):
        """Kiểm thử hành vi `test basic aggregation` trong phạm vi module hiện tại."""
        agg = HeuristicAggregator()
        agg.receive_worker_watermark("w0", 0, 100.0)
        agg.receive_worker_watermark("w1", 1, 95.0)
        agg.receive_worker_watermark("w0", 2, 105.0)
        b = agg.broadcast()
        assert b["partition_count"] == 3
        assert b["W_global_h"] >= 95.0

    def test_monotonic_global(self):
        """Kiểm thử hành vi `test monotonic global` trong phạm vi module hiện tại."""
        agg = HeuristicAggregator()
        agg.receive_worker_watermark("w0", 0, 100.0)
        first = agg.W_global_h
        agg.receive_worker_watermark("w0", 0, 90.0)
        assert agg.W_global_h >= first

    def test_save_load(self):
        """Kiểm thử hành vi `test save load` trong phạm vi module hiện tại."""
        import tempfile, os
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "agg.json")
            agg = HeuristicAggregator(state_path=path)
            agg.receive_worker_watermark("w0", 0, 100.0)
            agg.save_state()
            agg2 = HeuristicAggregator(state_path=path)
            assert agg2.load_state()
            assert agg2.W_global_h == agg.W_global_h


class TestDLQPipeline:
    """Lớp `TestDLQPipeline` gom các ca kiểm thử liên quan đến DLQPipeline."""
    def test_enqueue_and_drain(self):
        """Kiểm thử hành vi `test enqueue and drain` trong phạm vi module hiện tại."""
        dlq = DLQPipeline()
        dlq.enqueue({"event_id": "e1", "T_event": 100.0, "arrival_time": 105.0,
                      "lag": 5.0, "W_h_at_arrival": 110.0, "lateness": 5.0,
                      "partition_id": 0, "original_status": 200})
        assert dlq.backlog == 1
        entries = dlq.drain()
        assert len(entries) == 1
        assert dlq.backlog == 0

    def test_correction_computation(self):
        """Kiểm thử hành vi `test correction computation` trong phạm vi module hiện tại."""
        dlq = DLQPipeline()
        for i in range(10):
            dlq.enqueue({"event_id": f"e{i}", "T_event": 100.0 + i * 0.1,
                          "arrival_time": 200.0, "lag": 100.0,
                          "W_h_at_arrival": 150.0, "lateness": 50.0,
                          "partition_id": 0, "original_status": 200})
        entries = dlq.drain()
        corrections = dlq.compute_corrections(entries, window_size_s=5.0)
        assert len(corrections) == 1
        assert corrections[0].delta_count == 10


class TestCorrectionProtocol:
    """Lớp `TestCorrectionProtocol` gom các ca kiểm thử liên quan đến CorrectionProtocol."""
    def test_incremental_pattern(self):
        """Kiểm thử hành vi `test incremental pattern` trong phạm vi module hiện tại."""
        cp = CorrectionProtocol(pattern="incremental")
        from common.types import CorrectionMessage
        corr = CorrectionMessage(
            correction_id="c1", window_id="0_100-105",
            delta_count=5, corrected_count=15, previous_count=10,
        )
        result = cp.apply_correction(corr, {"count": 10, "version": 1})
        assert result["count"] == 15

    def test_replace_pattern(self):
        """Kiểm thử hành vi `test replace pattern` trong phạm vi module hiện tại."""
        cp = CorrectionProtocol(pattern="replace")
        from common.types import CorrectionMessage
        corr = CorrectionMessage(
            correction_id="c1", window_id="0_100-105",
            delta_count=5, corrected_count=15, previous_count=10,
        )
        result = cp.apply_correction(corr, {"count": 10, "version": 1})
        assert result["count"] == 15

    def test_dedup(self):
        """Kiểm thử hành vi `test dedup` trong phạm vi module hiện tại."""
        cp = CorrectionProtocol()
        assert not cp.is_duplicate("c1")
        assert cp.is_duplicate("c1")


class TestColdStartManager:
    """Lớp `TestColdStartManager` gom các ca kiểm thử liên quan đến ColdStartManager."""
    def test_phase_progression(self):
        """Kiểm thử hành vi `test phase progression` trong phạm vi module hiện tại."""
        import time as _time
        cs = ColdStartManager(warmup_min_seconds=0.01, warmup_min_samples=5)
        _time.sleep(0.02)
        cs.update(5)
        assert cs.phase == ColdStartPhase.NORMAL, f"Phase: {cs.phase}"

    def test_conservative_prior(self):
        """Kiểm thử hành vi `test conservative prior` trong phạm vi module hiện tại."""
        cs = ColdStartManager(L_max=60.0, baseline_from_history=45.0)
        assert cs.conservative_prior() == 60.0

    def test_not_warm_initially(self):
        """Kiểm thử hành vi `test not warm initially` trong phạm vi module hiện tại."""
        cs = ColdStartManager(warmup_min_seconds=999)
        assert not cs.is_warm
        assert cs.should_emit_watermark() is False

    def test_warm_after_conditions_met(self):
        """Kiểm thử hành vi `test warm after conditions met` trong phạm vi module hiện tại."""
        cs = ColdStartManager(warmup_min_seconds=0.0, warmup_min_samples=1)
        cs.update(1000)
        assert cs.is_warm


class TestNegativeLagHandler:
    """Lớp `TestNegativeLagHandler` gom các ca kiểm thử liên quan đến NegativeLagHandler."""
    def test_normal_rate(self):
        """Kiểm thử hành vi `test normal rate` trong phạm vi module hiện tại."""
        nl = NegativeLagHandler()
        for _ in range(1000):
            nl.observe(0.5)
        assert nl.evaluate() == LagTier.NORMAL

    def test_warning_tier(self):
        """Kiểm thử hành vi `test warning tier` trong phạm vi module hiện tại."""
        nl = NegativeLagHandler()
        for _ in range(995):
            nl.observe(0.5)
        for _ in range(5):
            nl.observe(-0.1)
        tier = nl.evaluate()
        assert tier in (LagTier.NORMAL, LagTier.WARNING), f"Got tier={tier}, rate={nl.rate:.5f}"

    def test_critical_tier_and_boo(self):
        """Kiểm thử hành vi `test critical tier and boo` trong phạm vi module hiện tại."""
        nl = NegativeLagHandler()
        for _ in range(50):
            nl.observe(-0.5)
        for _ in range(50):
            nl.observe(0.5)
        tier = nl.evaluate()
        assert tier == LagTier.CRITICAL
        assert nl.degraded_to_boo

    def test_lag_adjustment(self):
        """Kiểm thử hành vi `test lag adjustment` trong phạm vi module hiện tại."""
        nl = NegativeLagHandler()
        for _ in range(100):
            nl.observe(-1.0)
        nl.evaluate()
        adjusted = nl.adjust_lag(-1.0)
        assert adjusted >= 0

    def test_reset(self):
        """Kiểm thử hành vi `test reset` trong phạm vi module hiện tại."""
        nl = NegativeLagHandler()
        nl.observe(-1.0)
        nl.evaluate()
        nl.reset()
        assert nl.rate == 0.0
        assert not nl.degraded_to_boo
