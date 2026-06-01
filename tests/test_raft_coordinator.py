"""Tests for RaftCoordinator HA simulation."""

import pytest
from strict.raft_coordinator import RaftCoordinator, RaftRole
from common.types import WorkerHeartbeat


class TestRaftCoordinator:
    def test_initial_role_follower(self):
        rc = RaftCoordinator("c1", [])
        assert rc.role == RaftRole.FOLLOWER

    def test_vote_request_grants(self):
        rc = RaftCoordinator("c1", [])
        result = rc.handle_vote_request(term=1, candidate_id="c2", W_global=100.0)
        assert result["granted"]

    def test_vote_request_denies_lower_term(self):
        rc = RaftCoordinator("c1", [])
        rc.current_term = 5
        result = rc.handle_vote_request(term=3, candidate_id="c2", W_global=100.0)
        assert not result["granted"]

    def test_receive_state_updates_watermark(self):
        rc = RaftCoordinator("c1", [])
        rc.receive_state({"term": 1, "leader_id": "c2", "W_global": 200.0})
        assert rc.W_global == 200.0

    def test_broadcast_includes_raft_info(self):
        rc = RaftCoordinator("c1", [])
        b = rc.broadcast()
        assert "raft_role" in b
        assert b["raft_role"] == "follower"

    def test_delegates_heartbeat(self):
        rc = RaftCoordinator("c1", [])
        rc.role = RaftRole.LEADER
        hb = WorkerHeartbeat(worker_id="w1", partitions={0: 10.0})
        rc.receive_heartbeat(hb)
        assert len(rc.coordinator.partitions) == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
