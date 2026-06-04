"""
Test chaos engineering cho các tình huống lỗi phân tán.

Bao gồm worker kill, backpressure flood, clock skew, split-brain coordinator HA, aggregator HA và MinIO outage.
"""

import time
import pytest


class TestChaosWorkerKill:
    """Lớp `TestChaosWorkerKill` gom các ca kiểm thử liên quan đến ChaosWorkerKill."""
    def test_worker_failure_detected(self):
        """Kiểm thử hành vi `test worker failure detected` trong phạm vi module hiện tại."""
        from strict.failover import FailoverManager
        fm = FailoverManager(heartbeat_timeout_s=0.1)
        fm.register_worker("w1", [0, 1, 2])
        fm.register_worker("w2", [3, 4, 5])
        time.sleep(0.2)
        # w2 keeps heartbeating, w1 times out
        fm.heartbeat("w2", [3, 4, 5])
        failed = fm.detect_failures()
        assert "w1" in failed
        fm.reassign_failed_partitions()
        for pid in [0, 1, 2]:
            assert fm.get_partition_owner(pid) == "w2"

    def test_cascading_two_workers_fail(self):
        """Kiểm thử hành vi `test cascading two workers fail` trong phạm vi module hiện tại."""
        from strict.failover import FailoverManager
        fm = FailoverManager(heartbeat_timeout_s=0.05)
        for i in range(4):
            fm.register_worker(f"w{i}", list(range(i*3, (i+1)*3)))
        time.sleep(0.1)
        fm.detect_failures()
        reassignments = fm.cascading_failover(["w0", "w1"])
        assert len(reassignments) > 0

    def test_worker_recovery_failback(self):
        """Kiểm thử hành vi `test worker recovery failback` trong phạm vi module hiện tại."""
        from strict.failover import FailoverManager, FailbackStep
        from common.types import WorkerStatus
        fm = FailoverManager()
        fm.register_worker("w1", [0, 1, 2])
        fm.register_worker("w2", [3, 4, 5])
        fm._workers["w1"].status = WorkerStatus.FAILED
        fm.reassign_failed_partitions()
        # Step 1: start failback (PAUSE only)
        result = fm.start_failback("w1")
        assert result["worker_id"] == "w1"
        assert result["step"] == "pause"
        assert len(result["quiesced_partitions"]) > 0
        # Advance through remaining steps for one partition
        for pid in result["quiesced_partitions"]:
            r = fm.advance_failback(pid)
            assert r["current_step"] == "flush_ack"
            r = fm.advance_failback(pid)
            assert r["current_step"] == "kafka_reassign"
            r = fm.advance_failback(pid)
            assert r["current_step"] == "seek_resume"
            r = fm.advance_failback(pid)
            assert r["current_step"] == "complete"
        summary = fm.failback_summary()
        assert summary["in_progress"] == 0

    def test_decoupled_liveness_vs_progress(self):
        """Kiểm thử hành vi `test decoupled liveness vs progress` trong phạm vi module hiện tại."""
        from strict.failover import FailoverManager
        fm = FailoverManager(heartbeat_timeout_s=0.1)
        fm.register_worker("w1", [0, 1])
        # Ping updates last_heartbeat
        time.sleep(0.05)
        # Mock WorkerPing update:
        with fm._lock:
            fm._workers["w1"].last_heartbeat = time.time()
        time.sleep(0.05)
        # detect_failures should NOT find w1 failed because the last_heartbeat was refreshed
        failed = fm.detect_failures()
        assert "w1" not in failed

    def test_active_idle_classification(self):
        """Kiểm thử hành vi `test active idle classification` trong phạm vi module hiện tại."""
        from strict.coordinator import StrictCoordinator
        from common.types import WorkerHeartbeat
        coord = StrictCoordinator()
        
        # Report progress where partition 0 is active and partition 1 is idle
        hb = WorkerHeartbeat(
            worker_id="w1",
            partitions={0: 100.0, 1: 50.0},
            idle_partitions=[1],
        )
        coord.receive_heartbeat(hb)
        
        # Partition 1 should be marked is_temporary_idle
        assert coord.partitions[1].is_temporary_idle is True
        assert coord.partitions[0].is_temporary_idle is False
        
        # Global watermark should be computed as min of non-idle active partitions
        # Which is 100.0 (from partition 0), since partition 1 is idle and excluded
        assert coord.W_global == 100.0

    def test_minio_compression_and_reuse(self):
        """Kiểm thử hành vi `test minio compression and reuse` trong phạm vi module hiện tại."""
        from common.tiered_storage import TieredStorageManager
        import tempfile
        ts = TieredStorageManager("localhost:9000", "minio", "minio123", "test-bucket")
        
        # Even if minio client is None (disabled in test env), let's mock it to test compression
        class DummyMinio:
            """Lớp `DummyMinio` gom dữ liệu và hành vi liên quan đến DummyMinio."""
            def __init__(self):
                """Khởi tạo đối tượng của `DummyMinio` và thiết lập trạng thái ban đầu."""
                self.store = {}
            def put_object(self, bucket, key, data, length, headers=None):
                """Hàm `put_object` thực hiện phần xử lý liên quan đến put object của `DummyMinio`."""
                self.store[key] = data.read()
                class Result:
                    """Lớp `Result` gom dữ liệu và hành vi liên quan đến Result."""
                    etag = "mock-etag"
                return Result()
            def get_object(self, bucket, key):
                """Trả về thông tin `object` từ trạng thái hiện tại."""
                class Response:
                    """Lớp `Response` gom dữ liệu và hành vi liên quan đến Response."""
                    def __init__(self, data):
                        """Khởi tạo đối tượng của `Response` và thiết lập trạng thái ban đầu."""
                        self.data = data
                    def read(self):
                        """Hàm `read` thực hiện phần xử lý liên quan đến read của `Response`."""
                        return self.data
                    def close(self):
                        """Đóng tài nguyên `close` và giải phóng trạng thái liên quan."""
                        pass
                    def release_conn(self):
                        """Hàm `release_conn` thực hiện phần xử lý liên quan đến release conn của `Response`."""
                        pass
                return Response(self.store[key])
        
        ts.client = DummyMinio()
        ts.bucket = "test-bucket"
        
        test_data = {"window_start": 100, "window_end": 105, "count": 42}
        
        # Sync upload window
        ts.upload_window("win-1", test_data, partition_id=0, sync=True)
        
        # Ensure it was compressed: starts with gzip magic bytes b'\x1f\x8b'
        key = ts._object_key("win-1", 0)
        raw_data = ts.client.store[key]
        assert raw_data.startswith(b'\x1f\x8b')
        
        # Download and verify it decompresses and returns correct data
        restored = ts.download_window("win-1", partition_id=0)
        assert restored == test_data


class TestChaosBackpressure:
    """Lớp `TestChaosBackpressure` gom các ca kiểm thử liên quan đến ChaosBackpressure."""
    def test_backpressure_flood(self):
        """Kiểm thử hành vi `test backpressure flood` trong phạm vi module hiện tại."""
        from strict.backpressure import BackpressureController
        bp = BackpressureController(pause_threshold=500, resume_threshold=100)
        for pid in range(12):
            bp.report_buffer("w1", pid, 500)
        assert len(bp.paused_partitions()) == 12
        for pid in range(12):
            bp.report_buffer("w1", pid, 50)
        assert len(bp.paused_partitions()) == 0


class TestChaosClockSkew:
    """Lớp `TestChaosClockSkew` gom các ca kiểm thử liên quan đến ChaosClockSkew."""
    def test_clock_skew_four_tiers(self):
        """Kiểm thử hành vi `test clock skew four tiers` trong phạm vi module hiện tại."""
        from strict.ingestor_health import HealthRecord, IngestorStatus
        rec = HealthRecord("i1", clock_skew_ms=50)
        assert rec.diagnose_clock_skew() == IngestorStatus.CLOCK_SKEW_OK
        rec.clock_skew_ms = 200
        assert rec.diagnose_clock_skew() == IngestorStatus.CLOCK_SKEW_INFO
        rec.clock_skew_ms = 1000
        assert rec.diagnose_clock_skew() == IngestorStatus.CLOCK_SKEW_WARNING
        rec.clock_skew_ms = 3000
        assert rec.diagnose_clock_skew() == IngestorStatus.CLOCK_SKEW_CRITICAL


class TestChaosCoordinatorHA:
    """Lớp `TestChaosCoordinatorHA` gom các ca kiểm thử liên quan đến ChaosCoordinatorHA."""
    def test_follower_receives_state(self):
        """Kiểm thử hành vi `test follower receives state` trong phạm vi module hiện tại."""
        from strict.raft_coordinator import RaftCoordinator
        rc = RaftCoordinator("c1", [])
        rc.receive_state({"term": 5, "leader_id": "c2", "W_global": 500.0})
        assert rc.W_global == 500.0
        assert rc.current_term == 5

    def test_election_with_no_peers(self):
        """Kiểm thử hành vi `test election with no peers` trong phạm vi module hiện tại."""
        from strict.raft_coordinator import RaftCoordinator
        rc = RaftCoordinator("c1", [])
        rc._start_election()
        assert rc.role.value in ("leader", "follower")

    def test_split_brain_higher_term_wins(self):
        """Kiểm thử hành vi `test split brain higher term wins` trong phạm vi module hiện tại.
        
        Ghi chú gốc:
        Trien_Khai §9.3 — Coordinator Cluster Partition.
        
                After a network partition, the majority side elects a new leader with a
                higher term. When the partition heals, the old leader sees the higher
                term in a vote request and steps down — no split-brain.
        """
        from strict.raft_coordinator import RaftCoordinator
        rc_old = RaftCoordinator("c1", [])
        rc_old._start_election()      # becomes leader at term=1
        old_term = rc_old.current_term
        # New leader on majority side already advanced to term=5
        result = rc_old.handle_vote_request(
            term=old_term + 4, candidate_id="c2", W_global=500.0,
        )
        assert result["granted"] is True
        assert rc_old.role.value == "follower"
        assert rc_old.current_term == old_term + 4

    def test_ingestor_stuck_alert(self):
        """Kiểm thử hành vi `test ingestor stuck alert` trong phạm vi module hiện tại.
        
        Ghi chú gốc:
        Trien_Khai §9.3 — Ingestor Stuck.
        
                Health monitor must raise a SILENT alert when an ingestor stops
                heartbeating for longer than silent_timeout_s.
        """
        from strict.ingestor_health import IngestorHealthMonitor, IngestorStatus
        hm = IngestorHealthMonitor(silent_timeout_s=0.1, stuck_timeout_s=0.05)
        hm.receive_heartbeat("ing-1", partitions=[0, 1], last_T_commit=1000.0)
        time.sleep(0.15)
        result = hm.evaluate(W_global=1000.0)
        alerts = [a for a in result["alerts"] if a["condition"] == "silent"]
        assert len(alerts) == 1
        assert hm.ingestors["ing-1"].status == IngestorStatus.SILENT


class TestChaosAggregatorHA:
    """Lớp `TestChaosAggregatorHA` gom các ca kiểm thử liên quan đến ChaosAggregatorHA."""
    def test_aggregator_leader_takeover(self, tmp_path):
        """Kiểm thử hành vi `test aggregator leader takeover` trong phạm vi module hiện tại.
        
        Ghi chú gốc:
        Trien_Khai §9.3 — Aggregator Leader Kill.
        
                When the active aggregator releases its lock, a standby can acquire it
                and become active. Verifies the FileLockLeader handoff path used by
                AggregatorHA.
        """
        from heuristic.aggregator_ha import FileLockLeader
        lock_path = str(tmp_path / "agg.lock")
        leader = FileLockLeader(lock_path)
        assert leader.try_acquire() is True
        assert leader.is_active is True
        leader.release()
        assert leader.is_active is False
        # Standby simulates takeover by re-acquiring the same lock.
        standby = FileLockLeader(lock_path)
        assert standby.try_acquire() is True
        standby.release()


class TestChaosMinIOOutage:
    """Lớp `TestChaosMinIOOutage` gom các ca kiểm thử liên quan đến ChaosMinIOOutage."""
    def test_tiered_storage_disabled_when_minio_unreachable(self):
        """Kiểm thử hành vi `test tiered storage disabled when minio unreachable` trong phạm vi module hiện tại.
        
        Ghi chú gốc:
        Trien_Khai §9.3 — MinIO Outage.
        
                When MinIO cannot be reached the manager should disable itself rather
                than crash workers; engines run in tier-1-only mode until MinIO
                returns.
        """
        from common.tiered_storage import TieredStorageManager
        mgr = TieredStorageManager(
            endpoint="127.0.0.1:1",       # closed port → unreachable
            access_key="x", secret_key="x",
            bucket_name="never",
        )
        assert mgr.client is None
        # purge / upload calls must remain side-effect-free when disabled
        assert mgr.upload_window("w1", {"count": 1}, partition_id=0) is True
        assert mgr.purge_window("w1", partition_id=0) is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
