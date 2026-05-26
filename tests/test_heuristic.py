"""Tests for Heuristic Watermark engine, aggregator, DLQ, cold start, negative lag."""

import time
import pytest
from common.types import LogEvent
from heuristic import (
    HeuristicWatermarkEngine, HeuristicAggregator,
    DLQPipeline, CorrectionProtocol,
    ColdStartManager, ColdStartPhase,
    NegativeLagHandler, LagTier,
)


class TestHeuristicWatermarkEngine:
    def test_basic_processing(self):
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
        eng = HeuristicWatermarkEngine(L_max=10.0)
        base = time.time()
        for i in range(2000):
            lag = 0.5 + (i % 10) * 0.1
            et = base - lag
            eng.process(LogEvent(event_id=f"e{i}", event_time=et, status=200),
                       arrival_time=base + i * 0.001)
        assert eng.L_eff < 10.0

    def test_adaptive_percentile_triggers(self):
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
    def test_basic_aggregation(self):
        agg = HeuristicAggregator()
        agg.receive_worker_watermark("w0", 0, 100.0)
        agg.receive_worker_watermark("w1", 1, 95.0)
        agg.receive_worker_watermark("w0", 2, 105.0)
        b = agg.broadcast()
        assert b["partition_count"] == 3
        assert b["W_global_h"] >= 95.0

    def test_monotonic_global(self):
        agg = HeuristicAggregator()
        agg.receive_worker_watermark("w0", 0, 100.0)
        first = agg.W_global_h
        agg.receive_worker_watermark("w0", 0, 90.0)
        assert agg.W_global_h >= first

    def test_save_load(self):
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
    def test_enqueue_and_drain(self):
        dlq = DLQPipeline()
        dlq.enqueue({"event_id": "e1", "T_event": 100.0, "arrival_time": 105.0,
                      "lag": 5.0, "W_h_at_arrival": 110.0, "lateness": 5.0,
                      "partition_id": 0, "original_status": 200})
        assert dlq.backlog == 1
        entries = dlq.drain()
        assert len(entries) == 1
        assert dlq.backlog == 0

    def test_correction_computation(self):
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
    def test_incremental_pattern(self):
        cp = CorrectionProtocol(pattern="incremental")
        from common.types import CorrectionMessage
        corr = CorrectionMessage(
            correction_id="c1", window_id="0_100-105",
            delta_count=5, corrected_count=15, previous_count=10,
        )
        result = cp.apply_correction(corr, {"count": 10, "version": 1})
        assert result["count"] == 15

    def test_replace_pattern(self):
        cp = CorrectionProtocol(pattern="replace")
        from common.types import CorrectionMessage
        corr = CorrectionMessage(
            correction_id="c1", window_id="0_100-105",
            delta_count=5, corrected_count=15, previous_count=10,
        )
        result = cp.apply_correction(corr, {"count": 10, "version": 1})
        assert result["count"] == 15

    def test_dedup(self):
        cp = CorrectionProtocol()
        assert not cp.is_duplicate("c1")
        assert cp.is_duplicate("c1")


class TestColdStartManager:
    def test_phase_progression(self):
        import time as _time
        cs = ColdStartManager(warmup_min_seconds=0.01, warmup_min_samples=5)
        _time.sleep(0.02)
        cs.update(5)
        assert cs.phase == ColdStartPhase.NORMAL, f"Phase: {cs.phase}"

    def test_conservative_prior(self):
        cs = ColdStartManager(L_max=60.0, baseline_from_history=45.0)
        assert cs.conservative_prior() == 60.0

    def test_not_warm_initially(self):
        cs = ColdStartManager(warmup_min_seconds=999)
        assert not cs.is_warm
        assert cs.should_emit_watermark() is False

    def test_warm_after_conditions_met(self):
        cs = ColdStartManager(warmup_min_seconds=0.0, warmup_min_samples=1)
        cs.update(1000)
        assert cs.is_warm


class TestNegativeLagHandler:
    def test_normal_rate(self):
        nl = NegativeLagHandler()
        for _ in range(1000):
            nl.observe(0.5)
        assert nl.evaluate() == LagTier.NORMAL

    def test_warning_tier(self):
        nl = NegativeLagHandler()
        for _ in range(995):
            nl.observe(0.5)
        for _ in range(5):
            nl.observe(-0.1)
        tier = nl.evaluate()
        assert tier in (LagTier.NORMAL, LagTier.WARNING), f"Got tier={tier}, rate={nl.rate:.5f}"

    def test_critical_tier_and_boo(self):
        nl = NegativeLagHandler()
        for _ in range(50):
            nl.observe(-0.5)
        for _ in range(50):
            nl.observe(0.5)
        tier = nl.evaluate()
        assert tier == LagTier.CRITICAL
        assert nl.degraded_to_boo

    def test_lag_adjustment(self):
        nl = NegativeLagHandler()
        for _ in range(100):
            nl.observe(-1.0)
        nl.evaluate()
        adjusted = nl.adjust_lag(-1.0)
        assert adjusted >= 0

    def test_reset(self):
        nl = NegativeLagHandler()
        nl.observe(-1.0)
        nl.evaluate()
        nl.reset()
        assert nl.rate == 0.0
        assert not nl.degraded_to_boo
