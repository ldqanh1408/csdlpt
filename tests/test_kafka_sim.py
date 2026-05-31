"""Tests for refactor.common.kafka_sim -- in-process broker API (no HTTP).

All tests interact directly with KafkaBroker methods (produce, poll, join_group,
leave_group, commit, seek, position, pause, resume, kill_broker, revive_broker,
broker_status, partition_lag, evict) -- no HTTP servers are started.
"""

import time

import pytest

from common.kafka_sim import KafkaBroker, KafkaMessage


# ---------------------------------------------------------------------------
# KafkaBroker tests
# ---------------------------------------------------------------------------


class TestKafkaBroker:
    """Tests for the KafkaBroker direct (in-process) API."""

    # --- 1. create_topic ---------------------------------------------------

    def test_create_topic(self):
        """Create topic with default 12 partitions; verify all partitions exist."""
        broker = KafkaBroker()
        topic = broker.create_topic("test-events")
        assert topic.name == "test-events"
        assert topic.num_partitions == 12
        assert len(topic._partitions) == 12
        for pid in range(12):
            assert pid in topic._partitions

        # Creating a topic that already exists returns the same instance
        same = broker.create_topic("test-events")
        assert same is topic

    # --- 2. produce + poll -------------------------------------------------

    def test_produce_and_poll(self):
        """Produce a message to a known partition, poll it back via consumer group."""
        broker = KafkaBroker()
        # Produce to a specific partition so the test is deterministic
        result = broker.produce("events", value="hello-world", key="k1", partition=0)
        assert "error" not in result, result
        assert result["partition"] == 0
        assert result["offset"] == 0

        # Consumer joins the group and gets assigned all 12 partitions
        joined = broker.join_group("g1", "c1", ["events"])
        assert 0 in joined["assigned_partitions"]

        msgs = broker.poll("events", "g1", "c1", 0)
        assert len(msgs) == 1
        assert msgs[0]["value"] == "hello-world"
        assert msgs[0]["key"] == "k1"
        assert msgs[0]["partition"] == 0

    # --- 3. acks=all rejects when too few healthy brokers ------------------

    def test_produce_with_acks_all(self):
        """acks=all rejects produce when healthy brokers < min_insync_replicas."""
        broker = KafkaBroker(num_brokers=3, min_insync_replicas=2)

        # With all 3 brokers healthy, produce succeeds
        r = broker.produce("events", value="ok", acks="all")
        assert "error" not in r

        # Kill 2 brokers so only 1 is healthy (< min_insync_replicas=2)
        broker.kill_broker("broker-1")
        broker.kill_broker("broker-2")
        r = broker.produce("events", value="x", acks="all")
        assert r.get("error") == "NOT_ENOUGH_REPLICAS"
        assert r["available"] == 1
        assert r["required"] == 2

    # --- 4. consumer group join --------------------------------------------

    def test_consumer_group_join(self):
        """Consumer joins group and gets assigned all partitions (sole member)."""
        broker = KafkaBroker()
        joined = broker.join_group("g1", "c1", ["events"])
        assert joined["group_id"] == "g1"
        assert joined["client_id"] == "c1"
        assert joined["generation"] >= 1
        # With 12 partitions and a single consumer, all 12 are assigned
        assert joined["assigned_partitions"] == list(range(12))

    # --- 5. consumer group rebalance ---------------------------------------

    def test_consumer_group_rebalance(self):
        """Two consumers join; partitions are distributed evenly with no overlap."""
        broker = KafkaBroker()
        broker.join_group("g1", "c1", ["events"])
        broker.join_group("g1", "c2", ["events"])

        members = broker.stats()["consumer_groups"]["g1"]["members"]
        c1_parts = members["c1"]["assigned_partitions"]
        c2_parts = members["c2"]["assigned_partitions"]

        # 12 partitions / 2 consumers = 6 each
        assert len(c1_parts) == 6
        assert len(c2_parts) == 6

        # No overlapping partitions
        assert set(c1_parts).isdisjoint(c2_parts)

        # All 12 partitions are covered
        assert set(c1_parts) | set(c2_parts) == set(range(12))

    # --- 6. pause / resume -------------------------------------------------

    def test_consumer_pause_resume(self):
        """pause() prevents poll from returning messages; resume() allows it again."""
        broker = KafkaBroker()
        joined = broker.join_group("g1", "c1", ["events"])
        pid = joined["assigned_partitions"][0]  # picks partition 0

        # Produce two messages into the same partition
        broker.produce("events", "first", key="k1", partition=pid)
        broker.produce("events", "second", key="k2", partition=pid)

        # Poll one message; commit to advance past it
        msgs = broker.poll("events", "g1", "c1", pid, max_messages=1)
        assert len(msgs) == 1
        assert msgs[0]["value"] == "first"
        broker.commit("g1", "c1", {pid: msgs[0]["offset"] + 1})

        # Pause the partition -- poll should return nothing
        broker.pause("g1", "c1", pid)
        paused_msgs = broker.poll("events", "g1", "c1", pid)
        assert len(paused_msgs) == 0

        # Resume -- poll should now return the second message
        broker.resume("g1", "c1", pid)
        resumed_msgs = broker.poll("events", "g1", "c1", pid)
        assert len(resumed_msgs) == 1
        assert resumed_msgs[0]["value"] == "second"

    # --- 7. seek -----------------------------------------------------------

    def test_consumer_seek(self):
        """seek() sets the committed offset; position() reflects it; poll starts there."""
        broker = KafkaBroker()
        joined = broker.join_group("g1", "c1", ["events"])
        pid = joined["assigned_partitions"][0]

        # Produce two messages at offsets 0 and 1
        broker.produce("events", "msg-offset-0", key="a", partition=pid)
        broker.produce("events", "msg-offset-1", key="b", partition=pid)

        # Seek to offset 1 -- skip the first message
        broker.seek("g1", "c1", pid, 1)
        assert broker.position("g1", "c1", pid) == 1

        # Poll should return the message at offset 1 only
        msgs = broker.poll("events", "g1", "c1", pid)
        assert len(msgs) == 1
        assert msgs[0]["offset"] == 1
        assert msgs[0]["value"] == "msg-offset-1"

    # --- 8. commit offset --------------------------------------------------

    def test_consumer_commit_offset(self):
        """commit() updates the committed offset; position() reflects it."""
        broker = KafkaBroker()
        joined = broker.join_group("g1", "c1", ["events"])
        pid = joined["assigned_partitions"][0]

        assert broker.position("g1", "c1", pid) == 0

        broker.commit("g1", "c1", {pid: 5})
        assert broker.position("g1", "c1", pid) == 5

        broker.commit("g1", "c1", {pid: 10})
        assert broker.position("g1", "c1", pid) == 10

    # --- 9. broker failure + recovery --------------------------------------

    def test_broker_failure(self):
        """kill_broker makes acks=all fail; revive_broker restores produce capability."""
        broker = KafkaBroker(num_brokers=3, min_insync_replicas=2)

        # All healthy -- produce works
        r = broker.produce("events", value="ok", acks="all")
        assert "error" not in r

        # Kill 2 out of 3 brokers (healthy=1 < min_insync_replicas=2)
        broker.kill_broker("broker-1")
        broker.kill_broker("broker-2")
        r = broker.produce("events", value="fail", acks="all")
        assert r["error"] == "NOT_ENOUGH_REPLICAS"

        # Revive one broker -- healthy=2, produce works again
        broker.revive_broker("broker-1")
        r = broker.produce("events", value="ok-again", acks="all")
        assert "error" not in r

    # --- 10. broker status -------------------------------------------------

    def test_broker_status(self):
        """broker_status() accurately tracks healthy/total counts and per-broker state."""
        broker = KafkaBroker(num_brokers=3)
        status = broker.broker_status()
        assert status["total"] == 3
        assert status["healthy"] == 3
        assert status["brokers"]["broker-0"] is True
        assert status["brokers"]["broker-1"] is True
        assert status["brokers"]["broker-2"] is True

        broker.kill_broker("broker-0")
        status = broker.broker_status()
        assert status["total"] == 3
        assert status["healthy"] == 2
        assert status["brokers"]["broker-0"] is False
        assert status["brokers"]["broker-1"] is True

    # --- 11. topic eviction ------------------------------------------------

    def test_topic_eviction(self):
        """Messages older than the topic retention period are removed by evict()."""
        broker = KafkaBroker(default_partitions=1)
        # Use a topic with very short retention and an already-expired message
        topic = broker.create_topic("ephemeral", num_partitions=1, retention_s=0.01)
        broker.produce("ephemeral", "stale-message", partition=0)
        # Sleep past the retention period so the message is evicted
        time.sleep(0.02)
        topic.evict()
        assert topic.stats()["total_messages"] == 0

        # Fresh messages are retained when retention is generous
        topic.retention_s = 86400
        broker.produce("ephemeral", "fresh-message", partition=0)
        topic.evict()
        assert topic.stats()["total_messages"] == 1

    # --- 12. partition lag ------------------------------------------------

    def test_partition_lag_metric(self):
        """partition_lag() computes the difference between next_offset and committed offset."""
        broker = KafkaBroker()
        broker.join_group("g1", "c1", ["events"])

        # Produce 5 messages; round-robin distributes them across partitions
        for i in range(5):
            broker.produce("events", f"msg-{i}")

        # Without any commits every partition that received a message has lag
        total_lag = sum(
            broker.partition_lag("events", "g1", "c1", pid) for pid in range(12)
        )
        assert total_lag == 5

    # --- 13. KafkaMessage roundtrip ----------------------------------------

    def test_kafka_message_roundtrip(self):
        """to_dict() / from_dict() produce identical values after roundtrip."""
        original = KafkaMessage(
            key="my-key", value='{"nested": true}', partition=7,
            offset=99, timestamp=1717000000.0,
        )
        restored = KafkaMessage.from_dict(original.to_dict())

        assert restored.key == original.key
        assert restored.value == original.value
        assert restored.partition == original.partition
        assert restored.offset == original.offset
        assert restored.timestamp == original.timestamp

    def test_kafka_message_from_dict_defaults(self):
        """from_dict({}) fills sensible defaults."""
        msg = KafkaMessage.from_dict({})
        assert msg.key == ""
        assert msg.value == ""
        assert msg.partition == 0
        assert msg.offset == 0
        assert msg.timestamp == 0.0

    # --- 14. multiple topics -----------------------------------------------

    def test_multiple_topics(self):
        """Messages produced to separate topics poll independently of each other."""
        broker = KafkaBroker(default_partitions=4)
        broker.create_topic("topic-a", num_partitions=4)
        broker.create_topic("topic-b", num_partitions=4)

        # Produce to specific partitions for determinism
        broker.produce("topic-a", "alpha", key="ka", partition=0)
        broker.produce("topic-b", "beta", key="kb", partition=0)

        broker.join_group("ga", "ca", ["topic-a"])
        broker.join_group("gb", "cb", ["topic-b"])

        msgs_a = broker.poll("topic-a", "ga", "ca", 0)
        msgs_b = broker.poll("topic-b", "gb", "cb", 0)

        assert len(msgs_a) == 1
        assert msgs_a[0]["value"] == "alpha"
        assert msgs_a[0]["key"] == "ka"

        assert len(msgs_b) == 1
        assert msgs_b[0]["value"] == "beta"
        assert msgs_b[0]["key"] == "kb"

    # --- 15. consumer leave + rebalance ------------------------------------

    def test_consumer_leave_rebalance(self):
        """When a consumer leaves, its partitions are redistributed to the remaining member."""
        broker = KafkaBroker()
        broker.join_group("g1", "c1", ["events"])
        broker.join_group("g1", "c2", ["events"])

        # c1 leaves; c2 should now own all 12 partitions
        broker.leave_group("g1", "c1", ["events"])

        members = broker.stats()["consumer_groups"]["g1"]["members"]
        assert "c1" not in members
        assert members["c2"]["assigned_partitions"] == list(range(12))

        # c2 leaves as well; no members remain
        broker.leave_group("g1", "c2", ["events"])
        members = broker.stats()["consumer_groups"]["g1"]["members"]
        assert members == {}


# ---------------------------------------------------------------------------
# Edge-case / supplementary tests
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Additional focused tests for error paths and boundary conditions."""

    def test_produce_to_unknown_topic(self):
        """Producing to a non-existent topic returns an error dict."""
        broker = KafkaBroker()
        r = broker.produce("no-such-topic", "v")
        assert r == {"error": "Unknown topic: no-such-topic"}

    def test_produce_invalid_partition(self):
        """Producing to a partition beyond the topic's range returns an error."""
        broker = KafkaBroker()
        r = broker.produce("events", "v", partition=99)
        assert r == {"error": "Invalid partition 99"}

    def test_poll_unknown_group_returns_empty(self):
        """Polling with a group that was never joined returns an empty list."""
        broker = KafkaBroker()
        # Group "ghost" does not exist
        msgs = broker.poll("events", "ghost", "nobody", 0)
        assert msgs == []

    def test_heartbeat_unknown_group_returns_false(self):
        """Heartbeat on an unknown group returns False."""
        broker = KafkaBroker()
        assert broker.heartbeat("no-group", "c1") is False

    def test_commit_unknown_group_returns_error(self):
        """Committing to an unregistered group returns an error."""
        broker = KafkaBroker()
        r = broker.commit("ghost", "c1", {0: 5})
        assert r == {"error": "Unknown group: ghost"}

    def test_seek_unknown_group_returns_error(self):
        """Seeking on an unknown group returns an error."""
        broker = KafkaBroker()
        r = broker.seek("ghost", "c1", 0, 42)
        assert r == {"error": "Unknown group: ghost"}

    def test_leave_unknown_group_returns_error(self):
        """Leaving an unknown group returns an error."""
        broker = KafkaBroker()
        r = broker.leave_group("ghost", "c1", ["events"])
        assert r == {"error": "Unknown group: ghost"}

    def test_create_topic_custom_partitions(self):
        """create_topic with an explicit partition count honours it."""
        broker = KafkaBroker()
        t = broker.create_topic("custom-size", num_partitions=6)
        assert t.num_partitions == 6
        assert len(t._partitions) == 6

    def test_acks_all_skipped_when_acks_not_all(self):
        """acks='1' (or any non-'all' value) skips the health check."""
        broker = KafkaBroker(num_brokers=3, min_insync_replicas=2)
        broker.kill_broker("broker-1")
        broker.kill_broker("broker-2")  # healthy=1, would fail acks=all

        r = broker.produce("events", "still-works", acks="1")
        assert "error" not in r

    def test_poll_honors_max_messages(self):
        """poll() respects the max_messages cap."""
        broker = KafkaBroker()
        broker.join_group("g1", "c1", ["events"])
        pid = 0

        # Produce 5 messages to the same partition
        for i in range(5):
            broker.produce("events", f"msg-{i}", partition=pid)

        msgs = broker.poll("events", "g1", "c1", pid, max_messages=3)
        assert len(msgs) == 3

    def test_stats_includes_all_components(self):
        """stats() returns broker info, topics, consumer groups, and metrics."""
        broker = KafkaBroker()
        s = broker.stats()
        assert "broker_id" in s
        assert "brokers" in s
        assert "topics" in s
        assert "consumer_groups" in s
        assert "messages_produced" in s
        assert "acks_config" in s
