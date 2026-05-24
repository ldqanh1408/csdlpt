"""Tests for BackpressureController."""

import pytest
from refactor.strict.backpressure import BackpressureController


class TestBackpressureController:
    def test_pause_on_threshold(self):
        bp = BackpressureController(pause_threshold=500, resume_threshold=100)
        signal = bp.report_buffer("w1", 0, 500)
        assert signal is not None
        assert signal["action"] == "pause"
        assert bp.is_paused(0)

    def test_no_pause_below_threshold(self):
        bp = BackpressureController(pause_threshold=500, resume_threshold=100)
        signal = bp.report_buffer("w1", 0, 400)
        assert signal is None
        assert not bp.is_paused(0)

    def test_resume_below_threshold(self):
        bp = BackpressureController(pause_threshold=500, resume_threshold=100)
        bp.report_buffer("w1", 0, 500)
        assert bp.is_paused(0)
        signal = bp.report_buffer("w1", 0, 50)
        assert signal is not None
        assert signal["action"] == "resume"
        assert not bp.is_paused(0)

    def test_idempotent_pause(self):
        bp = BackpressureController(pause_threshold=500, resume_threshold=100)
        bp.report_buffer("w1", 0, 500)
        signal = bp.report_buffer("w1", 0, 600)
        assert signal is None

    def test_paused_partitions(self):
        bp = BackpressureController(pause_threshold=500, resume_threshold=100)
        bp.report_buffer("w1", 0, 500)
        bp.report_buffer("w1", 1, 500)
        paused = bp.paused_partitions()
        assert 0 in paused
        assert 1 in paused

    def test_clear_worker(self):
        bp = BackpressureController()
        bp.report_buffer("w1", 0, 500)
        bp.clear_worker("w1")
        assert not bp.is_paused(0)

    def test_summary(self):
        bp = BackpressureController(pause_threshold=500, resume_threshold=100)
        bp.report_buffer("w1", 0, 500)
        s = bp.summary()
        assert 0 in s["paused_partitions"]
        assert s["total_pause_events"] == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
