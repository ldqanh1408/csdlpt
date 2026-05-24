"""Tests for FailoverManager."""

import time
import pytest
from refactor.strict.failover import FailoverManager
from refactor.common.types import PartitionState, WorkerStatus


class TestFailoverManager:
    def test_register_worker(self):
        fm = FailoverManager()
        fm.register_worker("w1", [0, 1, 2])
        assert fm.get_partition_owner(0) == "w1"

    def test_detect_failures(self):
        fm = FailoverManager(heartbeat_timeout_s=0.1)
        fm.register_worker("w1", [0, 1, 2])
        time.sleep(0.2)
        failed = fm.detect_failures()
        assert "w1" in failed

    def test_reassign_on_failure(self):
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
        fm = FailoverManager()
        fm.register_worker("w1", [0, 1, 2])
        fm.register_worker("w2", [3, 4, 5])
        fm._workers["w1"].status = WorkerStatus.FAILED
        fm.reassign_failed_partitions()
        result = fm.start_failback("w1")
        assert result["worker_id"] == "w1"

    def test_partition_state_transitions(self):
        fm = FailoverManager()
        fm.register_worker("w1", [0, 1, 2])
        assert fm.get_partition_state(0) == PartitionState.ASSIGNED

    def test_alive_workers(self):
        fm = FailoverManager()
        fm.register_worker("w1", [0, 1])
        fm.register_worker("w2", [2, 3])
        assert "w1" in fm.alive_workers()

    def test_summary(self):
        fm = FailoverManager()
        fm.register_worker("w1", [0, 1])
        s = fm.summary()
        assert "workers" in s
        assert "w1" in s["workers"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
