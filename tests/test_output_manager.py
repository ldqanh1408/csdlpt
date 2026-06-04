"""
Test OutputManager và MockTransactionalSink.

Kiểm tra idempotency, transaction begin/commit/abort và phát WindowResult không trùng.
"""

import pytest
from strict.output_manager import OutputManager, MockTransactionalSink
from common.types import WindowResult


def make_result(window_id="p0_0-5", count=10):
    """Hàm `make_result` thực hiện phần xử lý liên quan đến make result."""
    return WindowResult(window_id=window_id, partition_id=0, window_start=0.0,
                        window_end=5.0, count=count, status_500=0,
                        is_speculative=False, version=1)


class TestMockTransactionalSink:
    """Lớp `TestMockTransactionalSink` gom các ca kiểm thử liên quan đến MockTransactionalSink."""
    def test_begin_commit(self):
        """Kiểm thử hành vi `test begin commit` trong phạm vi module hiện tại."""
        sink = MockTransactionalSink()
        tx_id = sink.begin_tx("w1")
        assert sink.commit(tx_id, "w1", {"count": 5})
        assert sink.is_committed("w1")

    def test_rollback(self):
        """Kiểm thử hành vi `test rollback` trong phạm vi module hiện tại."""
        sink = MockTransactionalSink()
        tx_id = sink.begin_tx("w2")
        assert sink.rollback(tx_id)

    def test_duplicate_commit(self):
        """Kiểm thử hành vi `test duplicate commit` trong phạm vi module hiện tại."""
        sink = MockTransactionalSink()
        tx_id = sink.begin_tx("w3")
        sink.commit(tx_id, "w3", {"count": 3})
        assert not sink.commit(tx_id, "w3", {"count": 3})

    def test_summary(self):
        """Kiểm thử hành vi `test summary` trong phạm vi module hiện tại."""
        sink = MockTransactionalSink()
        tx_id = sink.begin_tx("w4")
        sink.commit(tx_id, "w4", {"count": 4})
        assert sink.summary()["committed_windows"] == 1


class TestOutputManager:
    """Lớp `TestOutputManager` gom các ca kiểm thử liên quan đến OutputManager."""
    def test_emit_idempotent(self):
        """Kiểm thử hành vi `test emit idempotent` trong phạm vi module hiện tại."""
        om = OutputManager(mode="idempotent")
        r = make_result()
        assert om.emit(r)
        assert om.is_emitted(r.window_id)

    def test_emit_duplicate_skipped(self):
        """Kiểm thử hành vi `test emit duplicate skipped` trong phạm vi module hiện tại."""
        om = OutputManager(mode="idempotent")
        r = make_result()
        om.emit(r)
        assert not om.emit(r)

    def test_emit_transactional(self):
        """Kiểm thử hành vi `test emit transactional` trong phạm vi module hiện tại."""
        om = OutputManager(mode="transactional")
        r = make_result()
        assert om.emit(r)
        assert om.is_emitted(r.window_id)

    def test_summary(self):
        """Kiểm thử hành vi `test summary` trong phạm vi module hiện tại."""
        om = OutputManager(mode="idempotent")
        om.emit(make_result("w1"))
        om.emit(make_result("w2"))
        assert om.summary()["total_emitted"] == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
