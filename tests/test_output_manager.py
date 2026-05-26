"""Tests for OutputManager and MockTransactionalSink."""

import pytest
from strict.output_manager import OutputManager, MockTransactionalSink
from common.types import WindowResult


def make_result(window_id="p0_0-5", count=10):
    return WindowResult(window_id=window_id, partition_id=0, window_start=0.0,
                        window_end=5.0, count=count, status_500=0,
                        is_speculative=False, version=1)


class TestMockTransactionalSink:
    def test_begin_commit(self):
        sink = MockTransactionalSink()
        tx_id = sink.begin_tx("w1")
        assert sink.commit(tx_id, "w1", {"count": 5})
        assert sink.is_committed("w1")

    def test_rollback(self):
        sink = MockTransactionalSink()
        tx_id = sink.begin_tx("w2")
        assert sink.rollback(tx_id)

    def test_duplicate_commit(self):
        sink = MockTransactionalSink()
        tx_id = sink.begin_tx("w3")
        sink.commit(tx_id, "w3", {"count": 3})
        assert not sink.commit(tx_id, "w3", {"count": 3})

    def test_summary(self):
        sink = MockTransactionalSink()
        tx_id = sink.begin_tx("w4")
        sink.commit(tx_id, "w4", {"count": 4})
        assert sink.summary()["committed_windows"] == 1


class TestOutputManager:
    def test_emit_idempotent(self):
        om = OutputManager(mode="idempotent")
        r = make_result()
        assert om.emit(r)
        assert om.is_emitted(r.window_id)

    def test_emit_duplicate_skipped(self):
        om = OutputManager(mode="idempotent")
        r = make_result()
        om.emit(r)
        assert not om.emit(r)

    def test_emit_transactional(self):
        om = OutputManager(mode="transactional")
        r = make_result()
        assert om.emit(r)
        assert om.is_emitted(r.window_id)

    def test_summary(self):
        om = OutputManager(mode="idempotent")
        om.emit(make_result("w1"))
        om.emit(make_result("w2"))
        assert om.summary()["total_emitted"] == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
