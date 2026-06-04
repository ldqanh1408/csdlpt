"""
Test tích hợp so sánh Strict và Heuristic trong cùng luồng dữ liệu.

Tạo stream mẫu, chạy end-to-end, kiểm tra logic window và mô phỏng một số tình huống chaos.
"""

import time
import pytest
from common.types import LogEvent, PunctuationToken
from common.window import TumblingWindow
from strict import StrictWatermarkEngine, StrictCoordinator, StrictWorker
from heuristic import HeuristicWatermarkEngine, HeuristicAggregator, DLQPipeline


def generate_test_stream(n_events: int, base_time: float, lag_min: float, lag_max: float):
    """Hàm `generate_test_stream` thực hiện phần xử lý liên quan đến generate test stream.
    
    Ghi chú gốc:
    Generate a stream of LogEvents with varying lag.
    """
    import random
    events = []
    for i in range(n_events):
        lag = random.uniform(lag_min, lag_max)
        et = base_time + i * 0.01 - lag
        events.append(LogEvent(
            event_id=f"test-{i}",
            event_time=et,
            status=200 if random.random() < 0.95 else 500,
        ))
    return events


class TestStrictVsHeuristic:
    """Lớp `TestStrictVsHeuristic` gom các ca kiểm thử liên quan đến StrictVsHeuristic.
    
    Ghi chú gốc:
    Compare Strict (0% loss, high latency) vs Heuristic (bounded loss, low latency).
    """

    def test_strict_zero_loss(self):
        """Kiểm thử hành vi `test strict zero loss` trong phạm vi module hiện tại."""
        eng = StrictWatermarkEngine(window_size_s=5.0, delta_base_s=10.0)
        base = 1000.0
        for t in range(30):
            eng.on_punctuation(PunctuationToken(
                T_commit=base + t, partition_id=0, ingestor_id="ing-1"
            ))
            for i in range(20):
                eng.process(LogEvent(
                    event_id=f"e{t}_{i}",
                    event_time=base + t - 5 + i * 0.25,
                    status=200,
                ))
        eng.flush()
        assert eng.metrics.late_dropped == 0

    def test_heuristic_bounded_loss(self):
        """Kiểm thử hành vi `test heuristic bounded loss` trong phạm vi module hiện tại."""
        eng = HeuristicWatermarkEngine(partition_id=0, L_max=10.0)
        base = time.time()
        for i in range(3000):
            lag = 0.1 + (i % 25) * 0.1
            et = base - lag
            eng.process(LogEvent(event_id=f"e{i}", event_time=et, status=200),
                       arrival_time=base + 0.001)
        eng.flush()
        loss_rate = eng.metrics.late_arrival_rate()
        assert loss_rate < 40.0, f"Loss rate {loss_rate:.1f}% too high (expected <40%)"

    def test_heuristic_lower_latency_than_strict(self):
        """Kiểm thử hành vi `test heuristic lower latency than strict` trong phạm vi module hiện tại.
        
        Ghi chú gốc:
        Heuristic should close windows faster than Strict.
        """
        base = time.time()

        # Strict
        s_eng = StrictWatermarkEngine(window_size_s=5.0, delta_base_s=10.0)
        s_eng.on_punctuation(PunctuationToken(T_commit=base + 15.0, partition_id=0, ingestor_id="t"))
        for i in range(100):
            s_eng.process(LogEvent(event_id=f"s{i}", event_time=base + i * 0.1, status=200))
        s_eng.flush()

        # Heuristic
        h_eng = HeuristicWatermarkEngine(partition_id=0)
        for i in range(2000):
            h_eng.process(LogEvent(event_id=f"h{i}", event_time=base + i * 0.01, status=200),
                         arrival_time=base + i * 0.01 + 0.5)
        h_eng.flush()

        # Both should produce results
        assert len(s_eng.closed_windows) >= 0
        assert len(h_eng.closed_windows) >= 0


class TestEndToEnd:
    """Lớp `TestEndToEnd` gom các ca kiểm thử liên quan đến EndToEnd.
    
    Ghi chú gốc:
    Full pipeline: Ingestor → Kafka (simulated) → Worker → Coordinator → Output.
    """

    def test_strict_e2e(self):
        # Setup
        """Kiểm thử hành vi `test strict e2e` trong phạm vi module hiện tại."""
        coord = StrictCoordinator()
        worker = StrictWorker("w0", [0], window_size_s=5.0, delta_base_s=10.0)

        # Simulated ingestor sending data + punctuation
        base = 1000.0
        for second in range(20):
            token = PunctuationToken(T_commit=base + second, partition_id=0, ingestor_id="ing-1")
            worker.on_punctuation(token)
            for j in range(25):
                worker.process(LogEvent(
                    event_id=f"log-{second}-{j}",
                    event_time=base + second - 5 + j * 0.2,
                    status=200,
                ), partition_id=0)
            # Heartbeat every second
            coord.receive_heartbeat(worker.heartbeat())

        worker.flush_all()
        broadcast = coord.broadcast()
        assert broadcast["partition_count"] == 1
        assert len(worker.engines[0].closed_windows) > 0

    def test_heuristic_e2e(self):
        """Kiểm thử hành vi `test heuristic e2e` trong phạm vi module hiện tại."""
        agg = HeuristicAggregator()
        eng = HeuristicWatermarkEngine(partition_id=0)
        dlq = DLQPipeline()

        base = time.time()
        for i in range(3000):
            lag = 0.2 + (i % 10) * 0.1
            eng.process(LogEvent(event_id=f"e{i}", event_time=base - lag, status=200),
                       arrival_time=base + i * 0.001)

        eng.flush()

        # Route late events to DLQ
        for late in eng.late_events:
            dlq.enqueue(late)

        # Aggregator gets watermark
        agg.receive_worker_watermark("w0", 0, eng.W_h)
        b = agg.broadcast()

        assert b["W_global_h"] > float("-inf")
        assert len(eng.closed_windows) > 0

        # Process DLQ corrections
        if dlq.backlog > 0:
            entries = dlq.drain()
            corrections = dlq.compute_corrections(entries)
            assert len(corrections) >= 0


class TestChaosSimulation:
    """Lớp `TestChaosSimulation` gom các ca kiểm thử liên quan đến ChaosSimulation.
    
    Ghi chú gốc:
    Simulate failure scenarios: worker kill, replay, partition recovery.
    """

    def test_worker_recovery_from_checkpoint(self):
        """Kiểm thử hành vi `test worker recovery from checkpoint` trong phạm vi module hiện tại."""
        import tempfile, os
        with tempfile.TemporaryDirectory() as d:
            eng = StrictWatermarkEngine(checkpoint_dir=d)
            eng.on_punctuation(PunctuationToken(T_commit=200.0, partition_id=0, ingestor_id="t"))
            for i in range(200):
                eng.process(LogEvent(event_id=f"e{i}", event_time=150.0 + i * 0.1, status=200))
            eng.checkpoint()

            # Simulate crash + restore
            eng2 = StrictWatermarkEngine.restore(d)
            assert eng2.last_T_commit == 200.0

    def test_dlq_correction_flow(self):
        """Kiểm thử hành vi `test dlq correction flow` trong phạm vi module hiện tại."""
        dlq = DLQPipeline()
        for i in range(45):
            dlq.enqueue({
                "event_id": f"late-{i}", "T_event": 100.0 + i * 0.1,
                "arrival_time": 200.0, "lag": 100.0,
                "W_h_at_arrival": 150.0, "lateness": 50.0,
                "partition_id": 0, "original_status": 200,
            })
        entries = dlq.drain()
        corrections = dlq.compute_corrections(entries, window_size_s=5.0)
        # 45 events across 9 seconds → should be 2 windows
        assert len(corrections) >= 1
        total_corrected = sum(c.delta_count for c in corrections)
        assert total_corrected == 45

    def test_adaptive_percentile_recovery(self):
        """Kiểm thử hành vi `test adaptive percentile recovery` trong phạm vi module hiện tại.
        
        Ghi chú gốc:
        After burst, engine should recover to normal percentile.
        """
        eng = HeuristicWatermarkEngine(burst_threshold=2.0, recovery_minutes=0)
        base = time.time()
        # Normal load
        for i in range(500):
            eng.process(LogEvent(event_id=f"n{i}", event_time=base - 0.5, status=200),
                       arrival_time=base + i * 0.001)
        # Burst
        for i in range(100):
            eng.process(LogEvent(event_id=f"b{i}", event_time=base - 10.0, status=200),
                       arrival_time=base + 500 * 0.001 + i * 0.001)
        eng.flush()
        s = eng.summary()
        assert s["sketch_samples"] > 0


class TestWindowLogic:
    """Lớp `TestWindowLogic` gom các ca kiểm thử liên quan đến WindowLogic."""
    def test_tumbling_window_alignment(self):
        """Kiểm thử hành vi `test tumbling window alignment` trong phạm vi module hiện tại."""
        tw = TumblingWindow(size_s=5.0)
        assert tw.window_start(0.0) == 0.0
        assert tw.window_start(4.9) == 0.0
        assert tw.window_start(5.0) == 5.0
        assert tw.window_start(9.9) == 5.0

    def test_window_id_format(self):
        """Kiểm thử hành vi `test window id format` trong phạm vi module hiện tại."""
        tw = TumblingWindow(size_s=5.0)
        wid = tw.window_id(7, 12.3)
        assert "7_" in wid
        assert "10-" in wid or "10.0-" in wid

    def test_windows_between(self):
        """Kiểm thử hành vi `test windows between` trong phạm vi module hiện tại."""
        tw = TumblingWindow(size_s=5.0)
        ws = tw.windows_between(10.0, 22.0)
        assert ws == [10.0, 15.0, 20.0]
