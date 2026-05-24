"""Kafka Simulation Layer — spec-compliant Topic/Producer/Consumer/Broker via HTTP.

Provides the full Kafka abstraction model described across all 3 spec documents:
  - Named topics with configurable partitions and offsets
  - Producer: send(topic, value) with acks=all simulation
  - Consumer: poll(), pause(), resume(), seek(), commit()
  - ConsumerGroup: rebalancing on member join/leave
  - Broker: multi-broker simulation with replication

All messaging is HTTP-based (no real Kafka broker). The abstractions are real;
the transport is simulated. Runs embedded in coordinator or standalone.

Spec refs:
  Strict §3 (Kafka Cluster 12 partitions), §6.3 (offset checkpoint),
      §8.1 (consumer.pause/resume), §8.2-8.5 (consumer group rebalancing),
      §8.3 (seek Offset+1 on restore)
  Heuristic §3 (Kafka Cluster), §12.1-12.2 (DLQ topic + consumer)
  Deploy §2.4 (broker sizing), §3.1 (kafka_partition_lag metric),
         §8.4 (acks=all + min.insync.replicas), §9.3 (broker failure chaos)
"""

import json
import logging
import threading
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Optional

logger = logging.getLogger("kafka_sim")

# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass
class KafkaMessage:
    key: str
    value: str
    partition: int
    offset: int
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "key": self.key, "value": self.value,
            "partition": self.partition, "offset": self.offset,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "KafkaMessage":
        return cls(
            key=d.get("key", ""), value=d.get("value", ""),
            partition=d.get("partition", 0), offset=d.get("offset", 0),
            timestamp=d.get("timestamp", 0.0),
        )


@dataclass
class PartitionState:
    messages: list[KafkaMessage] = field(default_factory=list)
    next_offset: int = 0
    paused: bool = False

    def produce(self, msg: KafkaMessage) -> int:
        msg.offset = self.next_offset
        self.messages.append(msg)
        self.next_offset += 1
        return msg.offset

    def poll(self, from_offset: int, max_messages: int = 500) -> list[KafkaMessage]:
        if from_offset >= self.next_offset:
            return []
        end = min(from_offset + max_messages, self.next_offset)
        return self.messages[from_offset:end]

    def _evict_old(self, retention_s: float) -> None:
        if not self.messages:
            return
        cutoff = time.time() - retention_s
        keep_from = 0
        for i, msg in enumerate(self.messages):
            if msg.timestamp >= cutoff:
                keep_from = i
                break
        else:
            keep_from = len(self.messages)
        if keep_from > 0:
            self.messages = self.messages[keep_from:]


@dataclass
class KafkaTopic:
    name: str
    num_partitions: int = 12
    retention_s: float = 7 * 86400
    _partitions: dict[int, PartitionState] = field(default_factory=dict)

    def __post_init__(self):
        for pid in range(self.num_partitions):
            self._partitions[pid] = PartitionState()

    def produce(self, msg: KafkaMessage) -> KafkaMessage:
        p = self._partitions.get(msg.partition)
        if p is None:
            raise ValueError(f"Invalid partition {msg.partition} for topic {self.name}")
        msg.offset = p.produce(msg)
        return msg

    def poll(self, partition: int, from_offset: int, max_messages: int = 500) -> list[KafkaMessage]:
        p = self._partitions.get(partition)
        if p is None or p.paused:
            return []
        return p.poll(from_offset, max_messages)

    def pause(self, partition: int) -> None:
        p = self._partitions.get(partition)
        if p:
            p.paused = True

    def resume(self, partition: int) -> None:
        p = self._partitions.get(partition)
        if p:
            p.paused = False

    def is_paused(self, partition: int) -> bool:
        p = self._partitions.get(partition)
        return p.paused if p else False

    def lag(self, partition: int, consumer_offset: int) -> int:
        p = self._partitions.get(partition)
        if p is None:
            return 0
        return max(0, p.next_offset - consumer_offset)

    def evict(self) -> int:
        removed = 0
        for ps in self._partitions.values():
            before = len(ps.messages)
            ps._evict_old(self.retention_s)
            removed += before - len(ps.messages)
        return removed

    def stats(self) -> dict:
        total_msgs = sum(len(p.messages) for p in self._partitions.values())
        return {
            "name": self.name, "partitions": self.num_partitions,
            "total_messages": total_msgs,
            "per_partition": {
                pid: {"messages": len(p.messages), "next_offset": p.next_offset, "paused": p.paused}
                for pid, p in self._partitions.items()
            },
        }


# ---------------------------------------------------------------------------
# Consumer Group (server-side)
# ---------------------------------------------------------------------------


@dataclass
class ConsumerGroupMember:
    client_id: str
    assigned_partitions: list[int] = field(default_factory=list)
    last_heartbeat: float = field(default_factory=time.time)
    committed_offsets: dict[int, int] = field(default_factory=dict)
    current_offsets: dict[int, int] = field(default_factory=dict)
    paused_partitions: set[int] = field(default_factory=set)


class ConsumerGroup:
    """Server-side consumer group with offset tracking and rebalancing."""

    def __init__(self, group_id: str):
        self.group_id = group_id
        self._lock = threading.RLock()
        self._members: dict[str, ConsumerGroupMember] = {}
        self._generation: int = 0

    def join(self, client_id: str) -> list[int]:
        with self._lock:
            if client_id not in self._members:
                self._members[client_id] = ConsumerGroupMember(client_id=client_id)
            self._members[client_id].last_heartbeat = time.time()
            return self._members[client_id].assigned_partitions

    def leave(self, client_id: str) -> None:
        with self._lock:
            self._members.pop(client_id, None)

    def heartbeat(self, client_id: str) -> bool:
        with self._lock:
            if client_id in self._members:
                self._members[client_id].last_heartbeat = time.time()
                return True
            return False

    def rebalance(self, total_partitions: int) -> dict[str, list[int]]:
        with self._lock:
            member_ids = sorted(self._members.keys())
            if not member_ids:
                return {}
            self._generation += 1
            assignments: dict[str, list[int]] = {m: [] for m in member_ids}
            for pid in range(total_partitions):
                target = member_ids[pid % len(member_ids)]
                assignments[target].append(pid)
            for mid, parts in assignments.items():
                self._members[mid].assigned_partitions = parts
            return assignments

    def commit_offset(self, client_id: str, partition: int, offset: int) -> None:
        with self._lock:
            if client_id in self._members:
                self._members[client_id].committed_offsets[partition] = offset

    def get_committed_offset(self, client_id: str, partition: int) -> int:
        with self._lock:
            if client_id in self._members:
                return self._members[client_id].committed_offsets.get(partition, 0)
            return 0

    def update_current_offset(self, client_id: str, partition: int, offset: int) -> None:
        with self._lock:
            if client_id in self._members:
                self._members[client_id].current_offsets[partition] = offset

    def pause_partition(self, client_id: str, partition: int) -> None:
        with self._lock:
            if client_id in self._members:
                self._members[client_id].paused_partitions.add(partition)

    def resume_partition(self, client_id: str, partition: int) -> None:
        with self._lock:
            if client_id in self._members:
                self._members[client_id].paused_partitions.discard(partition)

    def is_paused(self, client_id: str, partition: int) -> bool:
        with self._lock:
            if client_id in self._members:
                return partition in self._members[client_id].paused_partitions
            return False

    def summary(self) -> dict:
        with self._lock:
            return {
                "group_id": self.group_id, "generation": self._generation,
                "members": {
                    mid: {
                        "assigned_partitions": m.assigned_partitions,
                        "committed_offsets": m.committed_offsets,
                        "current_offsets": m.current_offsets,
                        "paused_partitions": sorted(m.paused_partitions),
                    }
                    for mid, m in self._members.items()
                },
            }


# ---------------------------------------------------------------------------
# Kafka Broker
# ---------------------------------------------------------------------------


class KafkaBroker:
    """Simulated multi-broker Kafka cluster with topics, consumer groups, acks."""

    def __init__(
        self,
        broker_id: str = "broker-0",
        num_brokers: int = 3,
        default_partitions: int = 12,
        replication_factor: int = 3,
        min_insync_replicas: int = 2,
        retention_s: float = 7 * 86400,
    ):
        self.broker_id = broker_id
        self.num_brokers = num_brokers
        self.default_partitions = default_partitions
        self.replication_factor = min(replication_factor, num_brokers)
        self.min_insync_replicas = min(min_insync_replicas, replication_factor)
        self.retention_s = retention_s
        self._lock = threading.RLock()
        self._topics: dict[str, KafkaTopic] = {}
        self._consumer_groups: dict[str, ConsumerGroup] = {}
        self._broker_health: dict[str, bool] = {}
        self._msg_counter: int = 0

        for i in range(num_brokers):
            self._broker_health[f"broker-{i}"] = True

        # Pre-create spec-defined topics
        self.create_topic("events", default_partitions)
        self.create_topic("strict_results", default_partitions)
        self.create_topic("heuristic_results", default_partitions)
        self.create_topic("late_logs_dlq", default_partitions, retention_s)

    # ---- Topic management ----

    def create_topic(self, name: str, num_partitions: int = None,
                     retention_s: float = None) -> KafkaTopic:
        with self._lock:
            if name in self._topics:
                return self._topics[name]
            topic = KafkaTopic(
                name=name,
                num_partitions=num_partitions or self.default_partitions,
                retention_s=retention_s or self.retention_s,
            )
            self._topics[name] = topic
            return topic

    def get_topic(self, name: str) -> Optional[KafkaTopic]:
        with self._lock:
            return self._topics.get(name)

    def list_topics(self) -> list[str]:
        with self._lock:
            return sorted(self._topics.keys())

    # ---- Produce ----

    def produce(self, topic_name: str, value, key: str = "",
                partition: int = None, acks: str = "all") -> dict:
        with self._lock:
            topic = self._topics.get(topic_name)
            if topic is None:
                return {"error": f"Unknown topic: {topic_name}"}

            if partition is None:
                partition = self._msg_counter % topic.num_partitions

            if partition >= topic.num_partitions:
                return {"error": f"Invalid partition {partition}"}

            msg = KafkaMessage(
                key=key or str(uuid.uuid4()),
                value=value if isinstance(value, str) else json.dumps(value, default=str),
                partition=partition,
                offset=0,
            )
            produced = topic.produce(msg)
            self._msg_counter += 1

            if acks == "all":
                healthy = sum(1 for h in self._broker_health.values() if h)
                if healthy < self.min_insync_replicas:
                    return {"error": "NOT_ENOUGH_REPLICAS", "available": healthy, "required": self.min_insync_replicas}

            return {"topic": topic_name, "partition": produced.partition,
                    "offset": produced.offset, "timestamp": produced.timestamp}

    # ---- Consumer poll ----

    def poll(self, topic_name: str, group_id: str, client_id: str,
             partition: int, max_messages: int = 500) -> list[dict]:
        with self._lock:
            topic = self._topics.get(topic_name)
            cg = self._consumer_groups.get(group_id)
            if topic is None or cg is None:
                return []
            if cg.is_paused(client_id, partition) or topic.is_paused(partition):
                return []
            from_offset = cg.get_committed_offset(client_id, partition)
            msgs = topic.poll(partition, from_offset, max_messages)
            if msgs:
                cg.update_current_offset(client_id, partition, msgs[-1].offset + 1)
            return [m.to_dict() for m in msgs]

    # ---- Consumer group management ----

    def join_group(self, group_id: str, client_id: str, topics: list[str]) -> dict:
        with self._lock:
            if group_id not in self._consumer_groups:
                self._consumer_groups[group_id] = ConsumerGroup(group_id)
            cg = self._consumer_groups[group_id]
            cg.join(client_id)
            total_parts = 0
            for tname in topics:
                t = self._topics.get(tname)
                if t:
                    total_parts = max(total_parts, t.num_partitions)
            assignments = cg.rebalance(total_parts)
            return {"group_id": group_id, "client_id": client_id,
                    "generation": cg._generation,
                    "assigned_partitions": assignments.get(client_id, [])}

    def leave_group(self, group_id: str, client_id: str, topics: list[str]) -> dict:
        with self._lock:
            cg = self._consumer_groups.get(group_id)
            if cg is None:
                return {"error": f"Unknown group: {group_id}"}
            cg.leave(client_id)
            total_parts = 0
            for tname in topics:
                t = self._topics.get(tname)
                if t:
                    total_parts = max(total_parts, t.num_partitions)
            assignments = cg.rebalance(total_parts)
            return {"group_id": group_id, "assignments": assignments}

    def heartbeat(self, group_id: str, client_id: str) -> bool:
        with self._lock:
            cg = self._consumer_groups.get(group_id)
            return cg.heartbeat(client_id) if cg else False

    def commit(self, group_id: str, client_id: str, offsets: dict[int, int]) -> dict:
        with self._lock:
            cg = self._consumer_groups.get(group_id)
            if cg is None:
                return {"error": f"Unknown group: {group_id}"}
            for pid, off in offsets.items():
                cg.commit_offset(client_id, pid, off)
            return {"committed": offsets}

    def seek(self, group_id: str, client_id: str, partition: int, offset: int) -> dict:
        with self._lock:
            cg = self._consumer_groups.get(group_id)
            if cg is None:
                return {"error": f"Unknown group: {group_id}"}
            cg.commit_offset(client_id, partition, offset)
            cg.update_current_offset(client_id, partition, offset)
            return {"partition": partition, "offset": offset}

    def position(self, group_id: str, client_id: str, partition: int) -> int:
        with self._lock:
            cg = self._consumer_groups.get(group_id)
            return cg.get_committed_offset(client_id, partition) if cg else 0

    def pause(self, group_id: str, client_id: str, partition: int) -> None:
        with self._lock:
            cg = self._consumer_groups.get(group_id)
            if cg:
                cg.pause_partition(client_id, partition)

    def resume(self, group_id: str, client_id: str, partition: int) -> None:
        with self._lock:
            cg = self._consumer_groups.get(group_id)
            if cg:
                cg.resume_partition(client_id, partition)

    # ---- Broker health (simulated failures) ----

    def kill_broker(self, broker_id: str) -> None:
        with self._lock:
            if broker_id in self._broker_health:
                self._broker_health[broker_id] = False

    def revive_broker(self, broker_id: str) -> None:
        with self._lock:
            if broker_id in self._broker_health:
                self._broker_health[broker_id] = True

    def broker_status(self) -> dict:
        with self._lock:
            healthy = sum(1 for v in self._broker_health.values() if v)
            return {"total": len(self._broker_health), "healthy": healthy,
                    "brokers": dict(self._broker_health)}

    # ---- Metrics ----

    def partition_lag(self, topic_name: str, group_id: str, client_id: str,
                      partition: int) -> int:
        with self._lock:
            topic = self._topics.get(topic_name)
            if topic is None:
                return 0
            cg = self._consumer_groups.get(group_id)
            if cg is None:
                return topic._partitions[partition].next_offset if partition in topic._partitions else 0
            return topic.lag(partition, cg.get_committed_offset(client_id, partition))

    def stats(self) -> dict:
        with self._lock:
            return {
                "broker_id": self.broker_id,
                "brokers": self.broker_status(),
                "topics": {n: t.stats() for n, t in self._topics.items()},
                "consumer_groups": {gid: cg.summary() for gid, cg in self._consumer_groups.items()},
                "messages_produced": self._msg_counter,
                "acks_config": {"replication_factor": self.replication_factor,
                                "min_insync_replicas": self.min_insync_replicas},
            }

    def evict(self) -> None:
        with self._lock:
            for topic in self._topics.values():
                topic.evict()

    # ---- HTTP server ----

    def start_http_server(self, port: int = 9092) -> HTTPServer:
        broker_ref = self

        class BrokerHandler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                from urllib.parse import urlparse, parse_qs
                p = urlparse(self.path)
                qs = parse_qs(p.query)

                if p.path == "/kafka/topics":
                    self._json(200, {"topics": broker_ref.list_topics()})
                elif p.path == "/kafka/brokers":
                    self._json(200, broker_ref.broker_status())
                elif p.path == "/kafka/stats":
                    self._json(200, broker_ref.stats())
                elif p.path == "/kafka/poll":
                    msgs = broker_ref.poll(
                        qs.get("topic", [None])[0],
                        qs.get("group", ["default"])[0],
                        qs.get("client", ["unknown"])[0],
                        int(qs.get("partition", ["0"])[0]),
                        int(qs.get("max", ["500"])[0]),
                    )
                    self._json(200, {"messages": msgs, "count": len(msgs)})
                elif p.path == "/kafka/position":
                    pos = broker_ref.position(
                        qs.get("group", ["default"])[0],
                        qs.get("client", ["unknown"])[0],
                        int(qs.get("partition", ["0"])[0]),
                    )
                    self._json(200, {"offset": pos})
                elif p.path == "/kafka/lag":
                    lag = broker_ref.partition_lag(
                        qs.get("topic", ["events"])[0],
                        qs.get("group", ["default"])[0],
                        qs.get("client", ["unknown"])[0],
                        int(qs.get("partition", ["0"])[0]),
                    )
                    self._json(200, {"lag": lag})
                else:
                    self._json(404, {"error": "not found"})

            def do_POST(self):
                from urllib.parse import urlparse
                path = urlparse(self.path).path
                cl = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(cl) if cl else b"{}"
                try:
                    data = json.loads(body)
                except json.JSONDecodeError:
                    self._json(400, {"error": "invalid json"})
                    return

                if path == "/kafka/produce":
                    r = broker_ref.produce(
                        data.get("topic", "events"), data.get("value", data.get("payload", {})),
                        data.get("key", ""), data.get("partition"), data.get("acks", "all"),
                    )
                    self._json(200 if "error" not in r else 503, r)
                elif path == "/kafka/create-topic":
                    t = broker_ref.create_topic(data["name"], data.get("partitions"), data.get("retention_s"))
                    self._json(200, t.stats())
                elif path == "/kafka/join-group":
                    r = broker_ref.join_group(data.get("group_id", "default"),
                                              data.get("client_id", "unknown"),
                                              data.get("topics", ["events"]))
                    self._json(200, r)
                elif path == "/kafka/leave-group":
                    r = broker_ref.leave_group(data.get("group_id", "default"),
                                               data.get("client_id", "unknown"),
                                               data.get("topics", ["events"]))
                    self._json(200, r)
                elif path == "/kafka/heartbeat":
                    ok = broker_ref.heartbeat(data.get("group_id", "default"), data.get("client_id", "unknown"))
                    self._json(200, {"ok": ok})
                elif path == "/kafka/commit":
                    r = broker_ref.commit(data.get("group_id", "default"),
                                          data.get("client_id", "unknown"),
                                          {int(k): int(v) for k, v in data.get("offsets", {}).items()})
                    self._json(200, r)
                elif path == "/kafka/seek":
                    r = broker_ref.seek(data.get("group_id", "default"),
                                        data.get("client_id", "unknown"),
                                        int(data.get("partition", 0)), int(data.get("offset", 0)))
                    self._json(200, r)
                elif path == "/kafka/pause":
                    broker_ref.pause(data.get("group_id", "default"),
                                     data.get("client_id", "unknown"),
                                     int(data.get("partition", 0)))
                    self._json(200, {"ok": True})
                elif path == "/kafka/resume":
                    broker_ref.resume(data.get("group_id", "default"),
                                      data.get("client_id", "unknown"),
                                      int(data.get("partition", 0)))
                    self._json(200, {"ok": True})
                elif path == "/kafka/kill-broker":
                    broker_ref.kill_broker(data.get("broker_id", "broker-1"))
                    self._json(200, broker_ref.broker_status())
                elif path == "/kafka/revive-broker":
                    broker_ref.revive_broker(data.get("broker_id", "broker-1"))
                    self._json(200, broker_ref.broker_status())
                else:
                    self._json(404, {"error": "not found"})

            def _json(self, code, data):
                body = json.dumps(data, default=str).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        server = HTTPServer(("0.0.0.0", port), BrokerHandler)
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()
        logger.info("KafkaBroker HTTP: listening on :%d", port)
        return server


# ---------------------------------------------------------------------------
# KafkaProducer (HTTP client)
# ---------------------------------------------------------------------------


class KafkaProducer:
    """HTTP-based Kafka producer."""

    def __init__(self, broker_url: str = "http://localhost:9092", acks: str = "all",
                 client_id: str = None):
        self.broker_url = broker_url.rstrip("/")
        self.acks = acks
        self.client_id = client_id or f"producer-{uuid.uuid4().hex[:8]}"

    def send(self, topic: str, value, key: str = None, partition: int = None) -> dict:
        import urllib.request
        payload = {
            "topic": topic,
            "value": value if isinstance(value, str) else json.dumps(value, default=str),
            "key": key or "",
            "acks": self.acks,
        }
        if partition is not None:
            payload["partition"] = partition
        try:
            data = json.dumps(payload).encode()
            req = urllib.request.Request(f"{self.broker_url}/kafka/produce", data=data,
                                         headers={"Content-Type": "application/json"})
            resp = urllib.request.urlopen(req, timeout=2)
            return json.loads(resp.read())
        except Exception as e:
            logger.warning("KafkaProducer send failed: %s", e)
            return {"error": str(e)}

    def flush(self) -> None:
        pass


# ---------------------------------------------------------------------------
# KafkaConsumer (HTTP client)
# ---------------------------------------------------------------------------


class KafkaConsumer:
    """HTTP-based Kafka consumer with poll/pause/resume/seek/commit."""

    def __init__(self, broker_url: str = "http://localhost:9092",
                 group_id: str = "default", client_id: str = None):
        self.broker_url = broker_url.rstrip("/")
        self.group_id = group_id
        self.client_id = client_id or f"consumer-{uuid.uuid4().hex[:8]}"
        self._subscribed_topics: list[str] = []
        self._assigned_partitions: list[int] = []
        self._closed: bool = False
        self._stop_heartbeat: threading.Event = threading.Event()
        self._heartbeat_thread: Optional[threading.Thread] = None

    def subscribe(self, topics: list[str]) -> list[int]:
        import urllib.request
        self._subscribed_topics = list(topics)
        payload = {"group_id": self.group_id, "client_id": self.client_id, "topics": topics}
        try:
            data = json.dumps(payload).encode()
            req = urllib.request.Request(f"{self.broker_url}/kafka/join-group", data=data,
                                         headers={"Content-Type": "application/json"})
            resp = urllib.request.urlopen(req, timeout=2)
            result = json.loads(resp.read())
            self._assigned_partitions = result.get("assigned_partitions", [])
            self._start_heartbeat()
            return self._assigned_partitions
        except Exception as e:
            logger.warning("KafkaConsumer subscribe failed: %s", e)
            return []

    def poll(self, topic: str = None, timeout_ms: int = 1000, max_messages: int = 500) -> dict[int, list[dict]]:
        import urllib.request
        if self._closed:
            return {}
        topic = topic or (self._subscribed_topics[0] if self._subscribed_topics else "events")
        results: dict[int, list[dict]] = {}
        for pid in self._assigned_partitions:
            try:
                qs = f"topic={topic}&group={self.group_id}&client={self.client_id}&partition={pid}&max={max_messages}"
                req = urllib.request.Request(f"{self.broker_url}/kafka/poll?{qs}")
                resp = urllib.request.urlopen(req, timeout=max(timeout_ms / 1000.0, 0.5))
                result = json.loads(resp.read())
                msgs = result.get("messages", [])
                if msgs:
                    results[pid] = msgs
            except Exception:
                pass
        return results

    def pause(self, partitions: list[int]) -> None:
        import urllib.request
        for pid in partitions:
            try:
                data = json.dumps({"group_id": self.group_id, "client_id": self.client_id, "partition": pid}).encode()
                req = urllib.request.Request(f"{self.broker_url}/kafka/pause", data=data,
                                             headers={"Content-Type": "application/json"})
                urllib.request.urlopen(req, timeout=1)
            except Exception:
                pass

    def resume(self, partitions: list[int]) -> None:
        import urllib.request
        for pid in partitions:
            try:
                data = json.dumps({"group_id": self.group_id, "client_id": self.client_id, "partition": pid}).encode()
                req = urllib.request.Request(f"{self.broker_url}/kafka/resume", data=data,
                                             headers={"Content-Type": "application/json"})
                urllib.request.urlopen(req, timeout=1)
            except Exception:
                pass

    def seek(self, partition: int, offset: int) -> None:
        import urllib.request
        try:
            data = json.dumps({"group_id": self.group_id, "client_id": self.client_id,
                               "partition": partition, "offset": offset}).encode()
            req = urllib.request.Request(f"{self.broker_url}/kafka/seek", data=data,
                                         headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=1)
        except Exception as e:
            logger.warning("KafkaConsumer seek failed: %s", e)

    def commit(self, offsets: dict[int, int] = None) -> None:
        import urllib.request
        try:
            data = json.dumps({"group_id": self.group_id, "client_id": self.client_id,
                               "offsets": offsets or {}}).encode()
            req = urllib.request.Request(f"{self.broker_url}/kafka/commit", data=data,
                                         headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=1)
        except Exception:
            pass

    def position(self, partition: int) -> int:
        import urllib.request
        try:
            qs = f"group={self.group_id}&client={self.client_id}&partition={partition}"
            req = urllib.request.Request(f"{self.broker_url}/kafka/position?{qs}")
            resp = urllib.request.urlopen(req, timeout=1)
            return json.loads(resp.read()).get("offset", 0)
        except Exception:
            return 0

    def lag(self, topic: str, partition: int) -> int:
        import urllib.request
        try:
            qs = f"topic={topic}&group={self.group_id}&client={self.client_id}&partition={partition}"
            req = urllib.request.Request(f"{self.broker_url}/kafka/lag?{qs}")
            resp = urllib.request.urlopen(req, timeout=1)
            return json.loads(resp.read()).get("lag", 0)
        except Exception:
            return 0

    def assigned_partitions(self) -> list[int]:
        return list(self._assigned_partitions)

    def close(self) -> None:
        import urllib.request
        self._closed = True
        self._stop_heartbeat.set()
        try:
            data = json.dumps({"group_id": self.group_id, "client_id": self.client_id,
                               "topics": self._subscribed_topics}).encode()
            req = urllib.request.Request(f"{self.broker_url}/kafka/leave-group", data=data,
                                         headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=1)
        except Exception:
            pass

    def _start_heartbeat(self) -> None:
        import urllib.request
        self._stop_heartbeat.clear()

        def _beat():
            while not self._stop_heartbeat.is_set():
                time.sleep(1.0)
                try:
                    data = json.dumps({"group_id": self.group_id, "client_id": self.client_id}).encode()
                    req = urllib.request.Request(f"{self.broker_url}/kafka/heartbeat", data=data,
                                                 headers={"Content-Type": "application/json"})
                    urllib.request.urlopen(req, timeout=1)
                except Exception:
                    pass

        self._heartbeat_thread = threading.Thread(target=_beat, daemon=True)
        self._heartbeat_thread.start()
