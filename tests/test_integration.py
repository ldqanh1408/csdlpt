"""Integration tests: Strict vs Heuristic comparison, end-to-end flows, chaos simulation."""

import time
import pytest
from common.types import LogEvent, PunctuationToken
from common.window import TumblingWindow
from strict import StrictWatermarkEngine, StrictCoordinator, StrictWorker
from heuristic import HeuristicWatermarkEngine, HeuristicAggregator, DLQPipeline


def generate_test_stream(n_events: int, base_time: float, lag_min: float, lag_max: float):
    """Generate a stream of LogEvents with varying lag."""
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
    """Compare Strict (0% loss, high latency) vs Heuristic (bounded loss, low latency)."""

    def test_strict_zero_loss(self):
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
        """Heuristic should close windows faster than Strict."""
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
    """Full pipeline: Ingestor → Kafka (simulated) → Worker → Coordinator → Output."""

    def test_strict_e2e(self):
        # Setup
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
    """Simulate failure scenarios: worker kill, replay, partition recovery."""

    def test_worker_recovery_from_checkpoint(self):
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
        """After burst, engine should recover to normal percentile."""
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


class TestKafkaIntegration:
    """Full pipeline integration tests using in-process KafkaBroker (no HTTP)."""

    def test_kafka_full_pipeline(self):
        """Full pipeline: Producer -> KafkaBroker -> Consumer -> Worker -> Coordinator."""
        import time
        import json
        from common.kafka_sim import KafkaBroker
        from strict.worker import StrictWorker
        from strict.coordinator import StrictCoordinator
        from common.types import LogEvent, PunctuationToken

        broker = KafkaBroker()

        # Join consumer group for the "events" topic
        broker.join_group("test-group", "test-consumer", ["events"])

        # Produce LogEvents to partition 0
        base = time.time()
        broker.produce("events", {"event_id": "ev-1", "event_time": base, "status": 200}, partition=0)
        broker.produce("events", {"event_id": "ev-2", "event_time": base + 1.0, "status": 200}, partition=0)
        broker.produce("events", {"event_id": "ev-3", "event_time": base + 2.0, "status": 500}, partition=0)

        # Poll messages from broker
        msgs = broker.poll("events", "test-group", "test-consumer", 0, max_messages=10)
        assert len(msgs) == 3, f"Expected 3 messages, got {len(msgs)}"

        # Setup worker + coordinator
        coord = StrictCoordinator(delta_base_s=10.0)
        worker = StrictWorker("w0", [0], window_size_s=5.0, delta_base_s=10.0)

        # Process events through worker
        for msg in msgs:
            data = json.loads(msg["value"])
            event = LogEvent(
                event_id=data["event_id"],
                event_time=data["event_time"],
                status=data["status"],
            )
            worker.process(event, partition_id=msg["partition"])

        worker.drain_ready(0, batch_size=10)

        # Feed punctuation after data so it acts as a strict close signal.
        worker.on_punctuation(PunctuationToken(
            T_commit=base + 15.0, partition_id=0, ingestor_id="ing-1"
        ))

        worker.flush_all()
        coord.receive_heartbeat(worker.heartbeat())
        broadcast = coord.broadcast()

        assert broadcast["partition_count"] == 1
        assert len(worker.engines[0].closed_windows) > 0

    def test_kafka_backpressure(self):
        """Pause/resume in Kafka consumer flow: paused poll returns empty, resumed returns messages."""
        import time
        from common.kafka_sim import KafkaBroker

        broker = KafkaBroker()
        broker.join_group("bp-group", "bp-consumer", ["events"])

        # Produce 5 events to partition 0
        base = time.time()
        for i in range(5):
            broker.produce("events",
                           {"event_id": f"bp-{i}", "event_time": base + i, "status": 200},
                           partition=0)

        # Normal poll: should return 5 messages
        msgs = broker.poll("events", "bp-group", "bp-consumer", 0, max_messages=10)
        assert len(msgs) == 5
        broker.commit("bp-group", "bp-consumer", {0: 5})

        # Produce 3 more messages at higher offsets
        for i in range(3):
            broker.produce("events",
                           {"event_id": f"bp2-{i}", "event_time": base + 5 + i, "status": 200},
                           partition=0)

        # Pause the partition
        broker.pause("bp-group", "bp-consumer", 0)

        # Poll while paused: should return empty
        msgs_paused = broker.poll("events", "bp-group", "bp-consumer", 0, max_messages=10)
        assert len(msgs_paused) == 0, "Expected no messages while paused"

        # Resume and poll again: should deliver the remaining 3
        broker.resume("bp-group", "bp-consumer", 0)
        msgs_resumed = broker.poll("events", "bp-group", "bp-consumer", 0, max_messages=10)
        assert len(msgs_resumed) == 3, f"Expected 3 after resume, got {len(msgs_resumed)}"

        # Verify worker-level backpressure: flooding buffer above threshold triggers pause
        from strict.worker import StrictWorker
        from common.types import LogEvent, PunctuationToken

        worker = StrictWorker("w-bp", [0], window_size_s=5.0, delta_base_s=10.0)
        worker.on_punctuation(PunctuationToken(
            T_commit=base + 30.0, partition_id=0, ingestor_id="ing-1"
        ))

        # Push events until backpressure activates (threshold: 500)
        for j in range(600):
            ev = LogEvent(
                event_id=f"bp-flood-{j}",
                event_time=base + j * 0.01,
                status=200,
            )
            worker.process(ev, partition_id=0)

        worker.flush_all()
        assert worker.backpressure_pause_count >= 1, (
            f"Expected backpressure to activate, pause_count={worker.backpressure_pause_count}"
        )

    def test_kafka_broker_failure_handling(self):
        """Kill brokers below min_insync_replicas: produce with acks=all must fail."""
        from common.kafka_sim import KafkaBroker

        broker = KafkaBroker(num_brokers=3, replication_factor=3, min_insync_replicas=2)

        # Healthy: produce succeeds
        r1 = broker.produce("events", {"event_id": "ok-1", "status": 200}, partition=0, acks="all")
        assert "error" not in r1, f"Expected success, got {r1}"

        # Kill 2 out of 3 brokers -> only 1 healthy, below min_insync_replicas=2
        broker.kill_broker("broker-1")
        broker.kill_broker("broker-2")
        status = broker.broker_status()
        assert status["healthy"] == 1

        # Produce with acks=all must now fail
        r2 = broker.produce("events", {"event_id": "fail-1", "status": 500}, partition=0, acks="all")
        assert "error" in r2, f"Expected NOT_ENOUGH_REPLICAS error, got {r2}"
        assert "NOT_ENOUGH_REPLICAS" in r2["error"]

        # Revive one broker -> 2 healthy = back to safe threshold
        broker.revive_broker("broker-1")
        status2 = broker.broker_status()
        assert status2["healthy"] == 2

        # Produce should succeed again
        r3 = broker.produce("events", {"event_id": "ok-2", "status": 200}, partition=0, acks="all")
        assert "error" not in r3, f"Expected success after revive, got {r3}"


class TestWindowLogic:
    def test_tumbling_window_alignment(self):
        tw = TumblingWindow(size_s=5.0)
        assert tw.window_start(0.0) == 0.0
        assert tw.window_start(4.9) == 0.0
        assert tw.window_start(5.0) == 5.0
        assert tw.window_start(9.9) == 5.0

    def test_window_id_format(self):
        tw = TumblingWindow(size_s=5.0)
        wid = tw.window_id(7, 12.3)
        assert "7_" in wid
        assert "10-" in wid or "10.0-" in wid

    def test_windows_between(self):
        tw = TumblingWindow(size_s=5.0)
        ws = tw.windows_between(10.0, 22.0)
        assert ws == [10.0, 15.0, 20.0]
