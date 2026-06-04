"""
Adapter Kafka thật dùng thư viện `kafka-python`.

Bao bọc producer/consumer để hệ thống chạy với Kafka broker thật: đảm bảo topic tồn tại, gửi record theo key/partition và poll batch có kiểm soát offset.
"""

import os
import json
import logging
import time
from kafka import KafkaConsumer as PyKafkaConsumer, KafkaProducer as PyKafkaProducer, TopicPartition, OffsetAndMetadata
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

logger = logging.getLogger("kafka_real")


def ensure_topics(broker_url: str, topics: list[str], num_partitions: int = 12,
                  replication_factor: int = 1, timeout_s: float = 30.0) -> None:
    """Đảm bảo điều kiện/tài nguyên `ensure topics` đã sẵn sàng trước khi dùng.
    
    Ghi chú gốc:
    Create Kafka topics if they don't exist. Safe to call multiple times.
    """
    bootstrap = broker_url.replace("http://", "").replace("https://", "").split(",")
    deadline = time.monotonic() + timeout_s
    admin = None
    while time.monotonic() < deadline:
        try:
            admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="topic-init")
            break
        except Exception:
            time.sleep(1)
    if admin is None:
        logger.warning("ensure_topics: could not connect to Kafka at %s", bootstrap)
        return
    try:
        existing = set(admin.list_topics())
        new_topics = [
            NewTopic(name=t, num_partitions=num_partitions, replication_factor=replication_factor)
            for t in topics if t not in existing
        ]
        if new_topics:
            admin.create_topics(new_topics=new_topics, validate_only=False)
            logger.info("ensure_topics: created %s", [t.name for t in new_topics])
    except TopicAlreadyExistsError:
        pass
    except Exception as e:
        logger.warning("ensure_topics: %s", e)
    finally:
        admin.close()


class KafkaConsumer:
    """Lớp `KafkaConsumer` gom dữ liệu và hành vi liên quan đến KafkaConsumer.
    
    Ghi chú gốc:
    Real Kafka Consumer Adapter using kafka-python.
    """

    def __init__(self, broker_url: str = "localhost:9092",
                 group_id: str = "default", client_id: str = None):
        """Khởi tạo đối tượng của `KafkaConsumer` và thiết lập trạng thái ban đầu."""
        self.bootstrap_servers = broker_url.replace("http://", "").replace("https://", "").split(",")
        self.group_id = group_id
        self.client_id = client_id
        self._subscribed_topics = []
        self._assigned_partitions = []
        self._paused_partitions = set()

        logger.info("RealKafkaConsumer: initializing with bootstrap_servers=%s, group_id=%s",
                    self.bootstrap_servers, self.group_id)

        # Initialize kafka-python consumer with optional SASL credentials
        sasl_username = os.environ.get("KAFKA_SASL_USERNAME", "")
        sasl_password = os.environ.get("KAFKA_SASL_PASSWORD", "")
        sasl_mechanism = os.environ.get("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
        security_protocol = os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")

        kafka_kwargs = {
            "bootstrap_servers": self.bootstrap_servers,
            "group_id": self.group_id,
            "client_id": self.client_id,
            "enable_auto_commit": False,
            "auto_offset_reset": "earliest",
        }
        if sasl_username and sasl_password:
            kafka_kwargs["security_protocol"] = security_protocol
            kafka_kwargs["sasl_mechanism"] = sasl_mechanism
            kafka_kwargs["sasl_plain_username"] = sasl_username
            kafka_kwargs["sasl_plain_password"] = sasl_password

        self.consumer = PyKafkaConsumer(**kafka_kwargs)

    def subscribe(self, topics: list[str]) -> list[int]:
        """Hàm `subscribe` thực hiện phần xử lý liên quan đến subscribe của `KafkaConsumer`."""
        self._subscribed_topics = list(topics)
        # Parse partitions configured for this worker
        partitions_env = os.environ.get("PARTITIONS")
        if partitions_env:
            pids = [int(p.strip()) for p in partitions_env.split(",") if p.strip()]
        else:
            pids = [0, 1, 2]

        self._assigned_partitions = list(pids)
        tps = [TopicPartition(topic, pid) for topic in topics for pid in pids]
        logger.info("RealKafkaConsumer: assigning partition layout=%s", tps)
        self.consumer.assign(tps)
        return self._assigned_partitions

    def poll(self, topic: str = None, timeout_ms: int = 1000, max_messages: int = 500) -> dict[int, list[dict]]:
        """Hàm `poll` thực hiện phần xử lý liên quan đến poll của `KafkaConsumer`."""
        records_dict = self.consumer.poll(timeout_ms=timeout_ms, max_records=max_messages)

        results = {}
        for tp, records in records_dict.items():
            msgs = []
            for r in records:
                val = r.value.decode('utf-8') if isinstance(r.value, bytes) else r.value
                key = r.key.decode('utf-8') if isinstance(r.key, bytes) else r.key
                msgs.append({
                    "key": key or "",
                    "value": val,
                    "partition": tp.partition,
                    "offset": r.offset,
                    "timestamp": r.timestamp / 1000.0 if r.timestamp else time.time(),
                })
            if msgs:
                results[tp.partition] = msgs
        return results

    def pause(self, partitions: list[int]) -> None:
        """Hàm `pause` thực hiện phần xử lý liên quan đến pause của `KafkaConsumer`."""
        topic = self._subscribed_topics[0] if self._subscribed_topics else "events"
        to_pause = [pid for pid in partitions if pid not in self._paused_partitions]
        if not to_pause:
            return
        tps = [TopicPartition(topic, pid) for pid in to_pause]
        logger.info("RealKafkaConsumer: pausing partitions %s", tps)
        self.consumer.pause(*tps)
        self._paused_partitions.update(to_pause)

    def resume(self, partitions: list[int]) -> None:
        """Hàm `resume` thực hiện phần xử lý liên quan đến resume của `KafkaConsumer`."""
        topic = self._subscribed_topics[0] if self._subscribed_topics else "events"
        to_resume = [pid for pid in partitions if pid in self._paused_partitions]
        if not to_resume:
            return
        tps = [TopicPartition(topic, pid) for pid in to_resume]
        logger.info("RealKafkaConsumer: resuming partitions %s", tps)
        self.consumer.resume(*tps)
        self._paused_partitions.difference_update(to_resume)

    def seek(self, partition: int, offset: int) -> None:
        """Hàm `seek` thực hiện phần xử lý liên quan đến seek của `KafkaConsumer`."""
        topic = self._subscribed_topics[0] if self._subscribed_topics else "events"
        tp = TopicPartition(topic, partition)
        logger.info("RealKafkaConsumer: seek partition %s to offset %d", tp, offset)
        self.consumer.seek(tp, offset)

    def commit(self, offsets: dict[int, int] = None) -> None:
        """Hàm `commit` thực hiện phần xử lý liên quan đến commit của `KafkaConsumer`."""
        if not offsets:
            return
        topic = self._subscribed_topics[0] if self._subscribed_topics else "events"
        py_offsets = {}
        for pid, offset in offsets.items():
            tp = TopicPartition(topic, pid)
            py_offsets[tp] = OffsetAndMetadata(offset, "", -1)
        logger.debug("RealKafkaConsumer: committing offsets %s", py_offsets)
        self.consumer.commit(py_offsets)

    def position(self, partition: int) -> int:
        """Hàm `position` thực hiện phần xử lý liên quan đến position của `KafkaConsumer`."""
        topic = self._subscribed_topics[0] if self._subscribed_topics else "events"
        tp = TopicPartition(topic, partition)
        try:
            return self.consumer.position(tp)
        except Exception:
            return 0

    def lag(self, topic: str, partition: int) -> int:
        """Hàm `lag` thực hiện phần xử lý liên quan đến lag của `KafkaConsumer`."""
        tp = TopicPartition(topic, partition)
        try:
            end_offsets = self.consumer.end_offsets([tp])
            end_offset = end_offsets.get(tp, 0)
            curr_pos = self.consumer.position(tp)
            return max(0, end_offset - curr_pos)
        except Exception:
            return 0

    def assigned_partitions(self) -> list[int]:
        """Hàm `assigned_partitions` thực hiện phần xử lý liên quan đến assigned partitions của `KafkaConsumer`.
        """
        return list(self._assigned_partitions)

    def update_assignment(self, pids: list[int]) -> None:
        """Cập nhật trạng thái/metric `update assignment` dựa trên dữ liệu mới."""
        self._assigned_partitions = list(pids)
        self._paused_partitions.intersection_update(pids)
        topic = self._subscribed_topics[0] if self._subscribed_topics else "events"
        tps = [TopicPartition(topic, pid) for pid in pids]
        logger.info("RealKafkaConsumer: dynamically re-assigning partitions=%s", tps)
        self.consumer.assign(tps)

    def close(self) -> None:
        """Đóng tài nguyên `close` và giải phóng trạng thái liên quan."""
        logger.info("RealKafkaConsumer: closing consumer connection")
        try:
            self.consumer.close()
        except Exception:
            pass


class KafkaProducer:
    """Lớp `KafkaProducer` gom dữ liệu và hành vi liên quan đến KafkaProducer.
    
    Ghi chú gốc:
    Real Kafka Producer Adapter using kafka-python.
    """

    def __init__(self, broker_url: str = "localhost:9092", acks: str = "all",
                 client_id: str = None):
        """Khởi tạo đối tượng của `KafkaProducer` và thiết lập trạng thái ban đầu."""
        self.bootstrap_servers = broker_url.replace("http://", "").replace("https://", "").split(",")
        self.acks = 1 if acks == "1" else ("all" if acks == "all" else acks)
        self.client_id = client_id

        logger.info("RealKafkaProducer: initializing with bootstrap_servers=%s, acks=%s",
                    self.bootstrap_servers, self.acks)

        # Initialize kafka-python producer with optional SASL credentials
        sasl_username = os.environ.get("KAFKA_SASL_USERNAME", "")
        sasl_password = os.environ.get("KAFKA_SASL_PASSWORD", "")
        sasl_mechanism = os.environ.get("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
        security_protocol = os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")

        kafka_kwargs = {
            "bootstrap_servers": self.bootstrap_servers,
            "acks": self.acks,
            "client_id": self.client_id,
        }
        if sasl_username and sasl_password:
            kafka_kwargs["security_protocol"] = security_protocol
            kafka_kwargs["sasl_mechanism"] = sasl_mechanism
            kafka_kwargs["sasl_plain_username"] = sasl_username
            kafka_kwargs["sasl_plain_password"] = sasl_password

        self.producer = PyKafkaProducer(**kafka_kwargs)

    def send(self, topic: str, value, key: str = None, partition: int = None, sync: bool = True) -> dict:
        """Hàm `send` thực hiện phần xử lý liên quan đến send của `KafkaProducer`."""
        if isinstance(value, dict):
            val_bytes = json.dumps(value).encode('utf-8')
        elif isinstance(value, str):
            val_bytes = value.encode('utf-8')
        else:
            val_bytes = bytes(value)

        key_bytes = None
        if key:
            key_bytes = key.encode('utf-8') if isinstance(key, str) else key

        try:
            future = self.producer.send(topic, value=val_bytes, key=key_bytes, partition=partition)
            if not sync:
                return {
                    "topic": topic,
                    "partition": partition if partition is not None else -1,
                    "offset": -1,
                }
            metadata = future.get(timeout=5.0)
            return {
                "topic": metadata.topic,
                "partition": metadata.partition,
                "offset": metadata.offset,
            }
        except Exception as e:
            logger.warning("RealKafkaProducer: send failed: %s", e)
            return {"error": str(e)}

    def flush(self) -> None:
        """Flush dữ liệu đệm của `flush` xuống đích lưu trữ hoặc downstream."""
        self.producer.flush()
