"""
Test hạ tầng ZooKeeper lock và Kafka adapter thật.

Các test dùng mock để xác nhận leader election, Kafka producer/consumer wrapper và logic coordinator election qua ZooKeeper.
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch

from common.zk_lock import ZKLeaderElection
from common.kafka_real import KafkaProducer, KafkaConsumer


class TestZKLeaderElection(unittest.TestCase):
    """Lớp `TestZKLeaderElection` gom các ca kiểm thử liên quan đến ZKLeaderElection."""
    @patch("common.zk_lock.KazooClient")
    def test_zk_lock_lifecycle(self, mock_kazoo):
        """Kiểm thử hành vi `test zk lock lifecycle` trong phạm vi module hiện tại."""
        mock_client = MagicMock()
        mock_lock = MagicMock()
        mock_kazoo.return_value = mock_client
        mock_client.Lock.return_value = mock_lock

        election = ZKLeaderElection(zk_hosts="localhost:2181", lock_path="/test/lock")
        election.start()

        mock_kazoo.assert_called_once_with(hosts="localhost:2181", auth_data=None, default_acl=None)
        mock_client.start.assert_called_once()
        mock_client.Lock.assert_called_once_with("/test/lock")

        # Mock successful acquire
        mock_lock.acquire.return_value = True
        self.assertTrue(election.try_acquire())
        self.assertTrue(election.is_leader)

        # Mock release
        election.release()
        mock_lock.release.assert_called_once()
        self.assertFalse(election.is_leader)
        mock_client.stop.assert_called_once()
        mock_client.close.assert_called_once()


class TestKafkaRealAdapters(unittest.TestCase):
    """Lớp `TestKafkaRealAdapters` gom các ca kiểm thử liên quan đến KafkaRealAdapters."""
    @patch("common.kafka_real.PyKafkaProducer")
    def test_producer_send(self, mock_producer_cls):
        """Kiểm thử hành vi `test producer send` trong phạm vi module hiện tại."""
        mock_producer = MagicMock()
        mock_producer_cls.return_value = mock_producer
        mock_meta = MagicMock()
        mock_meta.topic = "test_topic"
        mock_meta.partition = 1
        mock_meta.offset = 100

        mock_future = MagicMock()
        mock_future.get.return_value = mock_meta
        mock_producer.send.return_value = mock_future

        producer = KafkaProducer(broker_url="localhost:9092", acks="all", client_id="test-prod")
        mock_producer_cls.assert_called_once()

        res = producer.send(topic="test_topic", value={"event_id": "1"}, key="key1", partition=1)
        self.assertEqual(res["offset"], 100)
        self.assertEqual(res["partition"], 1)

        producer.flush()
        mock_producer.flush.assert_called_once()

    @patch("common.kafka_real.PyKafkaConsumer")
    @patch.dict(os.environ, {"PARTITIONS": "0,1,2"})
    def test_consumer_operations(self, mock_consumer_cls):
        """Kiểm thử hành vi `test consumer operations` trong phạm vi module hiện tại."""
        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer

        consumer = KafkaConsumer(broker_url="localhost:9092", group_id="test-group", client_id="test-cons")
        assigned = consumer.subscribe(["events"])
        self.assertEqual(assigned, [0, 1, 2])

        # Mock poll records
        mock_record = MagicMock()
        mock_record.value = b'{"event_id": "101"}'
        mock_record.key = b"key101"
        mock_record.partition = 0
        mock_record.offset = 200
        mock_record.timestamp = 1600000000000.0

        from kafka import TopicPartition
        mock_consumer.poll.return_value = {
            TopicPartition("events", 0): [mock_record]
        }

        polled = consumer.poll(timeout_ms=100)
        self.assertIn(0, polled)
        self.assertEqual(len(polled[0]), 1)
        self.assertEqual(polled[0][0]["offset"], 200)

        consumer.pause([0])
        mock_consumer.pause.assert_called_once()

        consumer.resume([0])
        mock_consumer.resume.assert_called_once()

        consumer.seek(partition=0, offset=150)
        mock_consumer.seek.assert_called_once()

        consumer.commit({0: 201})
        mock_consumer.commit.assert_called_once()

        consumer.close()
        mock_consumer.close.assert_called_once()


class TestZKCoordinatorElection(unittest.TestCase):
    """Lớp `TestZKCoordinatorElection` gom các ca kiểm thử liên quan đến ZKCoordinatorElection."""
    @patch("kazoo.client.KazooClient")
    @patch.dict(os.environ, {"ZK_HOSTS": "localhost:2181"})
    def test_zk_coordinator_election_leader(self, mock_kazoo):
        """Kiểm thử hành vi `test zk coordinator election leader` trong phạm vi module hiện tại."""
        mock_client = MagicMock()
        mock_lock = MagicMock()
        mock_kazoo.return_value = mock_client
        mock_client.Lock.return_value = mock_lock

        mock_lock.acquire.return_value = True
        mock_client.exists.return_value = False

        from strict.raft_coordinator import RaftCoordinator, RaftRole
        rc = RaftCoordinator(coordinator_id="c1", peers=[], zk_ensemble=True)

        import time
        time.sleep(0.5)

        mock_kazoo.assert_called_with(hosts="localhost:2181", auth_data=None, default_acl=None)
        mock_client.Lock.assert_called_with("/csdlpt/coordinator-lock", identifier="c1")
        mock_lock.acquire.assert_called()

        self.assertEqual(rc.role, RaftRole.LEADER)
        self.assertEqual(rc.leader_id, "c1")

        rc.shutdown()
        mock_lock.release.assert_called()
        mock_client.stop.assert_called()
        mock_client.close.assert_called()

    @patch("kazoo.client.KazooClient")
    @patch.dict(os.environ, {"ZK_HOSTS": "localhost:2181"})
    def test_zk_coordinator_election_follower(self, mock_kazoo):
        """Kiểm thử hành vi `test zk coordinator election follower` trong phạm vi module hiện tại."""
        mock_client = MagicMock()
        mock_lock = MagicMock()
        mock_kazoo.return_value = mock_client
        mock_client.Lock.return_value = mock_lock

        mock_lock.acquire.return_value = False
        mock_client.exists.return_value = True
        mock_client.get.return_value = (b"c2", MagicMock())

        from strict.raft_coordinator import RaftCoordinator, RaftRole
        rc = RaftCoordinator(coordinator_id="c1", peers=[], zk_ensemble=True)

        import time
        time.sleep(0.5)

        self.assertEqual(rc.role, RaftRole.FOLLOWER)
        self.assertEqual(rc.leader_id, "c2")

        rc.shutdown()

