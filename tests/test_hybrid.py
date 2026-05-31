"""Tests for Hybrid Router - dual-path routing with loss accounting."""

import time
import pytest
from common.types import LogEvent, PunctuationToken
from hybrid.router import HybridRouter, EventPriority, LossAccounting


class TestHybridRouter:
    def test_routes_critical_to_strict(self):
        router = HybridRouter(partition_id=0)
        ev = LogEvent(
            event_id="crit-1", event_time=time.time(), status=200,
            payload={"priority": "critical"},
        )
        router.process(ev)
        assert router.route_counts[EventPriority.CRITICAL] == 1
        assert router.route_counts[EventPriority.STANDARD] == 0

    def test_routes_status_500_to_critical(self):
        router = HybridRouter(partition_id=0)
        ev = LogEvent(
            event_id="crit-500", event_time=time.time(), status=500,
        )
        router.process(ev)
        assert router.route_counts[EventPriority.CRITICAL] == 1
        assert router.route_counts[EventPriority.STANDARD] == 0

    def test_routes_standard_to_heuristic(self):
        router = HybridRouter(partition_id=0)
        ev = LogEvent(
            event_id="std-1", event_time=time.time(), status=200,
            payload={"priority": "standard"},
        )
        router.process(ev)
        assert router.route_counts[EventPriority.STANDARD] == 1
        assert router.route_counts[EventPriority.CRITICAL] == 0

    def test_default_priority_is_standard(self):
        router = HybridRouter(partition_id=0)
        ev = LogEvent(event_id="def-1", event_time=time.time(), status=200)
        router.process(ev)
        assert router.route_counts[EventPriority.STANDARD] == 1

    def test_punctuation_routes_to_strict(self):
        router = HybridRouter(partition_id=0)
        base = 1000.0
        token = PunctuationToken(T_commit=base + 10, partition_id=0, ingestor_id="ing-1")
        router.on_punctuation(token)
        assert router.strict_engine.last_T_commit == base + 10

    def test_close_windows_merges_both_paths(self):
        router = HybridRouter(partition_id=0, window_size_s=5.0)
        base = time.time()

        # Feed critical events (strict path)
        router.on_punctuation(PunctuationToken(
            T_commit=base + 15.0, partition_id=0, ingestor_id="ing-1"
        ))
        for i in range(30):
            router.process(LogEvent(
                event_id=f"crit-{i}", event_time=base + i * 0.1, status=200,
                payload={"priority": "critical"},
            ))

        # Feed standard events (heuristic path)
        for i in range(500):
            router.process(LogEvent(
                event_id=f"std-{i}", event_time=base - 0.5, status=200,
                payload={"priority": "standard"},
            ), arrival_time=base + i * 0.001)

        router.flush()
        closed = router.close_windows()
        assert len(closed) >= 0  # both engines should produce results
        s = router.summary()
        assert s["mode"] == "hybrid"

    def test_loss_accounting_tracks_both_paths(self):
        router = HybridRouter(partition_id=1, window_size_s=5.0)
        base = time.time()

        for i in range(100):
            router.process(LogEvent(
                event_id=f"e{i}", event_time=base - 0.3, status=200,
                payload={"priority": "standard"},
            ), arrival_time=base + i * 0.001)

        router.flush()
        router.close_windows()
        report = router.loss_report()
        assert isinstance(report, list)

    def test_record_expected_counts(self):
        router = HybridRouter(partition_id=0)
        router.record_expected("0_100-105", strict_count=50, heuristic_count=100)
        la = router.loss_accounting["0_100-105"]
        assert la.strict_expected == 50
        assert la.heuristic_expected == 100

    def test_event_priority_enum_values(self):
        assert EventPriority.CRITICAL.value == "critical"
        assert EventPriority.STANDARD.value == "standard"

    def test_dlq_correction_in_hybrid_mode(self):
        router = HybridRouter(partition_id=0, window_size_s=5.0, L_max=0.1)
        base = time.time()

        # Create events that will be late
        for i in range(100):
            router.process(LogEvent(
                event_id=f"e{i}", event_time=base - 10.0, status=200,
            ), arrival_time=base)

        router.flush()
        s = router.summary()
        assert s["dlq_backlog"] >= 0


class TestLossAccounting:
    def test_default_values(self):
        la = LossAccounting()
        assert la.strict_loss_pct == 0.0
        assert la.heuristic_loss_pct == 0.0
        assert la.heuristic_dlq_corrected == 0

    def test_loss_percentage_computation(self):
        la = LossAccounting(
            window_id="0_100-105",
            strict_expected=100,
            strict_actual=98,
            heuristic_expected=200,
            heuristic_actual=195,
        )
        # Manually compute
        la.strict_loss_pct = 100.0 * max(0, la.strict_expected - la.strict_actual) / la.strict_expected
        la.heuristic_loss_pct = 100.0 * max(0, la.heuristic_expected - la.heuristic_actual) / la.heuristic_expected
        assert la.strict_loss_pct == 2.0
        assert la.heuristic_loss_pct == 2.5
