"""Tests for Strict Watermark engine, coordinator, and worker."""

import time
import pytest
from common.types import LogEvent, PunctuationToken
from strict import (
    StrictWatermarkEngine, StrictCoordinator, StrictWorker, BoundedPriorityQueue,
)


class TestStrictWatermarkEngine:
    def test_zero_loss_with_punctuation(self):
        eng = StrictWatermarkEngine(window_size_s=5.0, delta_base_s=10.0)
        # Simulate punctuation every 1s advancing T_commit
        base = 1000.0
        for t in range(20):
            eng.on_punctuation(PunctuationToken(
                T_commit=base + t, partition_id=0, ingestor_id="ing-1"
            ))
            # Events with event_time = base + t - 5 (5s old, within window)
            for i in range(10):
                eng.process(LogEvent(
                    event_id=f"e{t}_{i}",
                    event_time=base + t - 5 + i * 0.5,
                    status=200,
                ))
        eng.flush()
        s = eng.summary()
        assert s["late_dropped"] == 0, "Strict mode should have 0 late drops"
        assert s["data_completeness_pct"] == 100.0

    def test_monotonic_punctuation(self):
        eng = StrictWatermarkEngine()
        eng.on_punctuation(PunctuationToken(T_commit=100.0, partition_id=0, ingestor_id="t"))
        eng.on_punctuation(PunctuationToken(T_commit=99.0, partition_id=0, ingestor_id="t"))
        # Non-monotonic should be rejected
        assert eng.last_T_commit == 100.0

    def test_dedup(self):
        # §8.4 + §6.5: dedup is the inner filter — the outer Watermark Filter
        # runs first and drops events whose window has already closed. Use an
        # event whose window is still open under the current watermark so the
        # inner hash filter is the one that catches the duplicate.
        eng = StrictWatermarkEngine()
        eng.on_punctuation(PunctuationToken(T_commit=110.0, partition_id=0, ingestor_id="t"))
        e = LogEvent(event_id="dup-1", event_time=200.0, status=200)
        eng.process(e)
        eng.process(e)  # duplicate
        assert eng.metrics.duplicates == 1
        assert eng.metrics.on_time == 1

    def test_watermark_filter_runs_before_hash_filter(self):
        # §6.5 invariant: any event with window_end <= W must be dropped by
        # the outer Watermark Filter BEFORE reaching the hash filter, so the
        # dedup TTL only needs to cover ~δ_base seconds of recent IDs.
        eng = StrictWatermarkEngine()
        eng.on_punctuation(PunctuationToken(T_commit=300.0, partition_id=0, ingestor_id="t"))
        # event_time=100 → window=[100,105], which is far behind W=290.
        late = LogEvent(event_id="late-1", event_time=100.0, status=200)
        eng.process(LogEvent(event_id="trigger", event_time=200.0, status=200))
        before_late = eng.metrics.late_dropped
        before_dup = eng.metrics.duplicates
        eng.process(late)
        assert eng.metrics.late_dropped == before_late + 1
        assert eng.metrics.duplicates == before_dup  # never reached the hash filter
        assert "late-1" not in eng._seen_ids_ttl

    def test_window_closing(self):
        eng = StrictWatermarkEngine(window_size_s=5.0, delta_base_s=10.0)
        eng.on_punctuation(PunctuationToken(T_commit=120.0, partition_id=0, ingestor_id="t"))
        for i in range(50):
            eng.process(LogEvent(event_id=f"e{i}", event_time=100.0 + i * 0.1, status=200))
        eng.flush()
        assert len(eng.closed_windows) >= 1

    def test_backpressure(self):
        eng = StrictWatermarkEngine(max_queue=5)
        eng.on_punctuation(PunctuationToken(T_commit=200.0, partition_id=0, ingestor_id="t"))
        for i in range(100):
            eng.process(LogEvent(event_id=f"e{i}", event_time=100.0, status=200), queue_len=10)
        assert eng.metrics.backpressure_drops > 0

    def test_checkpoint_restore(self):
        import tempfile, os
        with tempfile.TemporaryDirectory() as d:
            eng = StrictWatermarkEngine(checkpoint_dir=d)
            eng.on_punctuation(PunctuationToken(T_commit=200.0, partition_id=0, ingestor_id="t"))
            for i in range(100):
                eng.process(LogEvent(event_id=f"e{i}", event_time=150.0, status=200))
            eng.checkpoint()

            eng2 = StrictWatermarkEngine.restore(d)
            assert eng2.last_T_commit == 200.0
            assert eng2.watermark == eng.watermark


class TestStrictCoordinator:
    def test_W_global_computation(self):
        coord = StrictCoordinator()
        from common.types import WorkerHeartbeat
        coord.receive_heartbeat(WorkerHeartbeat(
            worker_id="w0", partitions={0: 90.0}, max_event_time=100.0, timestamp=time.time()
        ))
        coord.receive_heartbeat(WorkerHeartbeat(
            worker_id="w1", partitions={1: 85.0}, max_event_time=95.0, timestamp=time.time()
        ))
        assert coord.W_global >= 85.0 - 10.0  # min - delta_base

    def test_stale_partition_excluded(self):
        coord = StrictCoordinator()
        from common.types import WorkerHeartbeat
        # Only one worker reporting, other partition stale
        coord.receive_heartbeat(WorkerHeartbeat(
            worker_id="w0", partitions={0: 90.0}, max_event_time=100.0, timestamp=time.time()
        ))
        assert coord.W_global >= 80.0

    def test_serialization(self):
        import tempfile, os
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "state.json")
            coord = StrictCoordinator(state_path=path)
            from common.types import WorkerHeartbeat
            coord.receive_heartbeat(WorkerHeartbeat(
                worker_id="w0", partitions={0: 90.0}, max_event_time=100.0, timestamp=time.time()
            ))
            coord.save_state()
            coord2 = StrictCoordinator(state_path=path)
            assert coord2.load_state()


class TestBoundedPriorityQueue:
    def test_fifo_ordering(self):
        q = BoundedPriorityQueue(maxsize=100)
        q.push(LogEvent(event_id="e2", event_time=200.0, status=200))
        q.push(LogEvent(event_id="e1", event_time=100.0, status=200))
        q.push(LogEvent(event_id="e3", event_time=300.0, status=200))
        events = q.flush_all()
        assert events[0].event_id == "e1"
        assert events[1].event_id == "e2"
        assert events[2].event_id == "e3"

    def test_maxsize_overflow(self):
        q = BoundedPriorityQueue(maxsize=3)
        for i in range(5):
            result = q.push(LogEvent(event_id=f"e{i}", event_time=float(5 - i), status=200))
        assert len(q) == 2  # 3 max, push 5 with pop-1 per overflow = 2 remaining

    def test_timeout_pop(self):
        import time as _time
        q = BoundedPriorityQueue(max_wait_ms=1)
        q.push(LogEvent(event_id="e1", event_time=100.0, status=200))
        _time.sleep(0.01)
        result = q.pop_ready()
        assert result is not None


class TestStrictWorker:
    def test_multi_partition(self):
        w = StrictWorker("w0", [0, 1, 2])
        assert len(w.engines) == 3
        hb = w.heartbeat()
        assert hb.worker_id == "w0"
        assert len(hb.partitions) == 3

    def test_process_routes_to_partition(self):
        w = StrictWorker("w0", [0, 1])
        w.on_punctuation(PunctuationToken(T_commit=200.0, partition_id=0, ingestor_id="t"))
        w.on_punctuation(PunctuationToken(T_commit=200.0, partition_id=1, ingestor_id="t"))
        for i in range(50):
            w.process(LogEvent(event_id=f"e{i}", event_time=190.0 + i * 0.1, status=200), partition_id=0)
        w.flush_all()
        eng0 = w.engines[0]
        assert len(eng0.closed_windows) >= 1
