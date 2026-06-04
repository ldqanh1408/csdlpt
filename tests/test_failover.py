"""
Test cho FailoverManager.

Kiểm tra đăng ký worker, timeout, phân phối lại partition và trạng thái failback.
"""

import time
import pytest
from strict.failover import FailoverManager
from common.types import PartitionState, WorkerStatus


class TestFailoverManager:
    """Lớp `TestFailoverManager` gom các ca kiểm thử liên quan đến FailoverManager."""
    def test_register_worker(self):
        """Kiểm thử hành vi `test register worker` trong phạm vi module hiện tại."""
        fm = FailoverManager()
        fm.register_worker("w1", [0, 1, 2])
        assert fm.get_partition_owner(0) == "w1"

    def test_detect_failures(self):
        """Kiểm thử hành vi `test detect failures` trong phạm vi module hiện tại."""
        fm = FailoverManager(heartbeat_timeout_s=0.1)
        fm.register_worker("w1", [0, 1, 2])
        time.sleep(0.2)
        failed = fm.detect_failures()
        assert "w1" in failed

    def test_reassign_on_failure(self):
        """Kiểm thử hành vi `test reassign on failure` trong phạm vi module hiện tại."""
        fm = FailoverManager(heartbeat_timeout_s=0.05)
        fm.register_worker("w1", [0, 1, 2, 3, 4, 5])
        fm.register_worker("w2", [6, 7, 8, 9, 10, 11])
        time.sleep(0.1)
        # w2 is still alive (heartbeat renewed), w1 times out
        fm.heartbeat("w2", [6, 7, 8, 9, 10, 11])
        fm.detect_failures()
        reassignments = fm.reassign_failed_partitions()
        assert len(reassignments) == 6
        for pid in reassignments:
            assert fm.get_partition_owner(pid) == "w2"

    def test_cascading_failover(self):
        """Kiểm thử hành vi `test cascading failover` trong phạm vi module hiện tại."""
        fm = FailoverManager(heartbeat_timeout_s=0.05)
        fm.register_worker("w1", [0, 1, 2])
        fm.register_worker("w2", [3, 4, 5])
        fm.register_worker("w3", [6, 7, 8])
        fm.register_worker("w4", [9, 10, 11])
        time.sleep(0.1)
        fm.detect_failures()
        reassignments = fm.cascading_failover(["w1", "w2"])
        assert len(reassignments) > 0

    def test_failback_protocol(self):
        """Kiểm thử hành vi `test failback protocol` trong phạm vi module hiện tại."""
        fm = FailoverManager()
        fm.register_worker("w1", [0, 1, 2])
        fm.register_worker("w2", [3, 4, 5])
        fm._workers["w1"].status = WorkerStatus.FAILED
        fm.reassign_failed_partitions()
        result = fm.start_failback("w1")
        assert result["worker_id"] == "w1"

    def test_partition_state_transitions(self):
        """Kiểm thử hành vi `test partition state transitions` trong phạm vi module hiện tại."""
        fm = FailoverManager()
        fm.register_worker("w1", [0, 1, 2])
        assert fm.get_partition_state(0) == PartitionState.ASSIGNED

    def test_alive_workers(self):
        """Kiểm thử hành vi `test alive workers` trong phạm vi module hiện tại."""
        fm = FailoverManager()
        fm.register_worker("w1", [0, 1])
        fm.register_worker("w2", [2, 3])
        assert "w1" in fm.alive_workers()

    def test_summary(self):
        """Kiểm thử hành vi `test summary` trong phạm vi module hiện tại."""
        fm = FailoverManager()
        fm.register_worker("w1", [0, 1])
        s = fm.summary()
        assert "workers" in s
        assert "w1" in s["workers"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
