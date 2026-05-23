# Kiến trúc Triển khai Phân tán & Hiệu năng Điều phối EOS Barrier

Tài liệu phân tích hiệu năng của cơ chế đồng bộ kết thúc luồng (EOS Barrier)
khi triển khai trên **hạ tầng phân tán thực tế** — mỗi Node xử lý là 1 server
(bare-metal / VM / container trên máy riêng), Coordinator là 1 server riêng,
tất cả giao tiếp qua **mạng vật lý** (LAN cùng datacenter hoặc WAN xuyên datacenter).

---

## 1. Topology triển khai thực tế

```
                        ┌─────────────────────────────────┐
                        │         DATACENTER / VPC         │
                        │                                  │
  ┌──────────────┐      │   ┌──────────┐   ┌──────────┐   │
  │   Ingestor   │──LAN─┼──▶│ Server 0 │   │ Server 1 │   │
  │  (nguồn log) │      │   │ Node 0   │   │ Node 1   │   │
  │  10.0.0.10   │      │   │ 10.0.1.1 │   │ 10.0.1.2 │   │
  └──────────────┘      │   └────┬─────┘   └────┬─────┘   │
                        │        │               │         │
                        │   ┌────┴───────────────┴────┐    │
                        │   │    LAN / VPC Network     │    │
                        │   │    RTT: 0.1 – 0.5 ms    │    │
                        │   └────┬───────────────┬────┘    │
                        │        │               │         │
                        │   ┌──────────┐   ┌──────────┐    │
                        │   │ Server 2 │   │ Server 3 │    │
                        │   │ Node 2   │   │ Node 3   │    │
                        │   │ 10.0.1.3 │   │ 10.0.1.4 │    │
                        │   └────┬─────┘   └────┬─────┘    │
                        │        │               │         │
                        │        └───────┬───────┘         │
                        │                │                 │
                        │        ┌───────▼──────┐          │
                        │        │ Coordinator  │          │
                        │        │  10.0.2.1    │          │
                        │        │  (server     │          │
                        │        │   riêng)     │          │
                        │        └──────────────┘          │
                        └──────────────────────────────────┘
```

### Đặc điểm khác biệt so với chạy cùng 1 máy

| Yếu tố | Cùng máy (Docker bridge) | Server riêng (LAN) | Xuyên datacenter (WAN) |
|---|---|---|---|
| **Network RTT** | ~0.05 ms | **0.1 – 0.5 ms** | **1 – 100 ms** |
| **Packet loss** | ~0% | 0.01 – 0.1% | 0.1 – 1% |
| **Server chết** | Container restart ~1s | **Server reboot 30s – 5 phút** | Tương tự + failover DNS |
| **Network partition** | Gần như không xảy ra | **Có thể** (switch hỏng, cable đứt) | **Thường xuyên** |
| **Service discovery** | Docker DNS tự động | **Cần DNS / Consul / etcd** | Cần load balancer + health check |
| **Bảo mật** | Không cần (internal) | **TLS giữa các server** | TLS + mTLS + firewall rules |
| **Clock drift** | Cùng clock | **Lệch vài ms** (NTP) | Lệch 10–100 ms |

> **Kết luận:** Trên server riêng, cơ chế coordination phải chịu được
> **latency cao hơn**, **packet loss**, **node chết thật**, và **network partition**.
> Đây là lý do cần thiết kế kỹ hơn so với chạy trên 1 máy.

---

## 2. Luồng dữ liệu End-to-End trên hạ tầng thực

```
Ingestor (10.0.0.10)         Node i (10.0.1.x)            Coordinator (10.0.2.1)
   │                              │                              │
   │── TCP connect ──────────────▶│                              │
   │   (+ TLS handshake ~1ms)     │                              │
   │                              │                              │
   │── POST /ingest {log_1} ─────▶│                              │
   │   RTT: 0.3ms LAN             │  process(log_1)              │
   │── POST /ingest {log_2} ─────▶│                              │
   │          ...                  │       ...                    │
   │── POST /ingest {log_N} ─────▶│                              │
   │                              │                              │
   │── POST /ingest {EOS} ───────▶│  ← nhận EOS sau log cuối     │
   │                              │  flush() đóng mọi window     │
   │                              │                              │
   │                              │── Báo coordinator ──────────▶│
   │                              │   (qua mạng LAN/WAN thật)    │
   │                              │                              │
   │                              │          ... chờ N node ...   │
   │                              │                              │
   │                              │      Nhận đủ N → aggregate   │
```

---

## 3. Ba phương án Coordination trên hạ tầng phân tán thật

### Phương án A: HTTP Webhook qua mạng LAN

Coordinator chạy HTTP server trên 1 server riêng, Node gọi qua LAN.

```python
# Coordinator (server 10.0.2.1) — FastAPI
from fastapi import FastAPI
import threading, os

app = FastAPI()
N = int(os.environ["TOTAL_NODES"])
completed = set()
lock = threading.Lock()

@app.post("/api/completed")
def node_completed(node_id: int):
    with lock:
        completed.add(node_id)
        if len(completed) == N:
            aggregate_and_report()
    return {"ack": True}
```

```python
# Node i (server 10.0.1.x) — 3 khi flush()
import requests, time

COORDINATOR = "http://10.0.2.1:8000"

def report_eos(node_id, max_retries=5):
    for attempt in range(max_retries):
        try:
            resp = requests.post(
                f"{COORDINATOR}/api/completed",
                json={"node_id": node_id},
                timeout=5,
            )
            if resp.status_code == 200:
                return True
        except (requests.ConnectionError, requests.Timeout):
            pass
        time.sleep(min(2 ** attempt, 10))  # Exponential backoff
    raise RuntimeError(f"Node {node_id}: không thể liên lạc Coordinator sau {max_retries} lần")
```

#### Hiệu năng trên hạ tầng thật

| Metric | Cùng datacenter (LAN) | Xuyên datacenter (WAN) |
|---|---|---|
| RTT 1 lần gọi | **0.3 – 1 ms** | **10 – 100 ms** |
| + TLS handshake (lần đầu) | +1 – 3 ms | +20 – 50 ms |
| Overhead tổng (N=4, song song) | **~1 – 3 ms** | **~50 – 150 ms** |
| Retry nếu fail (1 lần) | +2 – 10 ms | +2 – 10 s |

#### Failure modes

| Lỗi | Hậu quả | Giải pháp |
|---|---|---|
| Coordinator server chết | Node retry vô hạn, pipeline treo | Health check + **standby coordinator** (active-passive) |
| Node chết trước khi gọi | Coordinator chờ mãi, không bao giờ đủ N | **Timeout** — nếu sau T giây thiếu node → báo lỗi |
| Network partition | POST không đến | Retry + timeout + alert |
| Duplicate call (retry gọi 2 lần) | Coordinator đếm sai? | **Idempotent** — dùng `set.add()` nên gọi lặp không sao |

---

### Phương án B: Redis trên server riêng

Redis chạy trên 1 server riêng (hoặc cùng server Coordinator).
Tất cả Node và Coordinator kết nối tới Redis qua mạng.

```
Node 0 (10.0.1.1) ──┐
Node 1 (10.0.1.2) ──┼── LAN ──▶ Redis (10.0.2.5:6379) ◀── Coordinator (10.0.2.1)
Node 2 (10.0.1.3) ──┤
Node 3 (10.0.1.4) ──┘
```

```python
# Node i — sau khi flush()
import redis
r = redis.Redis(host="10.0.2.5", port=6379, socket_timeout=5)

pipe = r.pipeline()
pipe.incr("eos:barrier")                        # Atomic counter
pipe.hset("eos:node_meta", NODE_ID, json.dumps({ # Ghi metadata kết quả
    "events": total_processed,
    "completeness": completeness_pct,
    "flush_ts": time.time(),
}))
results = pipe.execute()                         # 1 round-trip duy nhất

count = results[0]
if count == N:
    r.publish("eos:done", "ALL_COMPLETED")
```

```python
# Coordinator — subscribe và chờ
r = redis.Redis(host="10.0.2.5", port=6379)
pubsub = r.pubsub()
pubsub.subscribe("eos:done")

for msg in pubsub.listen():
    if msg["type"] == "message" and msg["data"] == b"ALL_COMPLETED":
        node_meta = r.hgetall("eos:node_meta")
        aggregate_and_report(node_meta)
        break
```

#### Hiệu năng trên hạ tầng thật

| Metric | Cùng datacenter | Xuyên datacenter |
|---|---|---|
| RTT tới Redis (1 command) | **0.1 – 0.5 ms** | **5 – 50 ms** |
| Pipeline 2 commands (INCR + HSET) | **0.2 – 0.7 ms** (1 RTT) | **5 – 50 ms** (1 RTT) |
| Pub/Sub delivery | **< 0.1 ms** sau publish | **~5 ms** |
| Overhead tổng (N=4) | **~1 ms** | **~50 ms** |

#### Failure modes

| Lỗi | Hậu quả | Giải pháp |
|---|---|---|
| Redis server chết | Mọi node mất kênh liên lạc | Redis Sentinel (auto-failover) hoặc Redis Cluster |
| Node chết trước INCR | Counter không bao giờ đạt N | **Timeout ở Coordinator** — chờ T giây rồi kiểm tra thiếu node nào |
| Redis restart giữa chừng | Counter reset về 0 | `appendonly yes` (AOF persistence) — Redis khôi phục data |
| Network partition Node↔Redis | INCR không thực hiện được | Retry + fallback sang HTTP webhook |

---

### Phương án C: gRPC Streaming — Persistent Connection

Mỗi Node duy trì 1 **persistent gRPC stream** tới Coordinator từ lúc khởi động.
Coordinator biết trạng thái realtime của toàn cluster.

```
Node 0 ◄═══ gRPC stream (persistent, HTTP/2) ═══▶ Coordinator
Node 1 ◄═══ gRPC stream (persistent, HTTP/2) ═══▶ Coordinator
Node 2 ◄═══ gRPC stream (persistent, HTTP/2) ═══▶ Coordinator
Node 3 ◄═══ gRPC stream (persistent, HTTP/2) ═══▶ Coordinator
```

```protobuf
// coordination.proto
service CoordinationService {
    // Bidirectional stream: node báo trạng thái, coordinator gửi lệnh
    rpc NodeStream (stream NodeReport) returns (stream CoordinatorCommand);
}

message NodeReport {
    int32  node_id          = 1;
    string phase            = 2;   // "READY" | "PROCESSING" | "EOS_FLUSHED"
    int64  events_processed = 3;
    double completeness     = 4;
}

message CoordinatorCommand {
    string action = 1;   // "ACK" | "ALL_DONE" | "ABORT"
}
```

```python
# Node i — gRPC client, persistent stream
import grpc

channel = grpc.insecure_channel("10.0.2.1:50051")
stub = CoordinationServiceStub(channel)

def report_generator():
    # Báo READY khi khởi động
    yield NodeReport(node_id=NODE_ID, phase="READY")

    # Chờ xử lý xong...
    while not eos_received:
        time.sleep(0.5)

    # EOS → flush → báo
    yield NodeReport(
        node_id=NODE_ID,
        phase="EOS_FLUSHED",
        events_processed=engine.metrics["total"],
        completeness=engine.get_completeness(),
    )

# Nhận lệnh từ Coordinator
for cmd in stub.NodeStream(report_generator()):
    if cmd.action == "ALL_DONE":
        print("Cluster hoàn tất!")
        break
```

```python
# Coordinator — gRPC server
class CoordinationServicer(CoordinationServiceServicer):
    def __init__(self, n_nodes):
        self.n = n_nodes
        self.flushed = set()
        self.lock = threading.Lock()
        self.all_done = threading.Event()

    def NodeStream(self, request_iterator, context):
        node_id = None
        for report in request_iterator:
            node_id = report.node_id
            if report.phase == "EOS_FLUSHED":
                with self.lock:
                    self.flushed.add(node_id)
                    if len(self.flushed) == self.n:
                        self.all_done.set()

            # Chờ tất cả node hoàn thành
            if self.all_done.wait(timeout=1.0):
                yield CoordinatorCommand(action="ALL_DONE")
                return

        # Node stream đứt = node chết
        print(f"⚠️ Node {node_id} disconnected!")
```

#### Hiệu năng trên hạ tầng thật

| Metric | Cùng datacenter | Xuyên datacenter |
|---|---|---|
| Latency báo EOS_FLUSHED | **0.05 – 0.3 ms** | **1 – 10 ms** |
| Connection setup (1 lần, lúc khởi động) | ~5 ms | ~50 ms |
| Phát hiện node chết | **~instant** (stream đứt) | **~instant** + keepalive probe |
| Overhead tổng (N=4) | **< 0.5 ms** | **~10 ms** |

#### Failure modes

| Lỗi | Hậu quả | Giải pháp |
|---|---|---|
| Node chết | gRPC stream đứt → Coordinator biết **ngay lập tức** | Tự động, không cần timeout |
| Coordinator chết | Tất cả stream đứt, node biết ngay | Node retry connect + **standby coordinator** |
| Network partition | Stream timeout → cả 2 bên biết | gRPC keepalive ping (mặc định 20s) |
| Coordinator restart | Mất trạng thái `flushed` set | Node re-connect + re-report trạng thái |

---

## 4. Bảng so sánh tổng hợp — Trên hạ tầng phân tán thật

| Tiêu chí | HTTP Webhook | Redis Counter | gRPC Stream |
|---|---|---|---|
| **Latency (LAN)** | 0.3 – 3 ms | 0.2 – 1 ms | **0.05 – 0.3 ms** |
| **Latency (WAN)** | 50 – 150 ms | 50 – 100 ms | **1 – 10 ms** ★ |
| **Phát hiện node chết** | ❌ Timeout thụ động | ⚠️ TTL key (~30s) | ✅ **Ngay lập tức** |
| **Network partition** | Retry + timeout | Retry + timeout | **Keepalive detect** |
| **Server thêm** | 0 | +1 Redis server | 0 |
| **Connection model** | Short-lived (mở/đóng) | Short-lived tới Redis | **Persistent** (mở 1 lần) |
| **Bandwidth overhead** | Thấp (N requests) | Thấp (N commands) | **Rất thấp** (multiplexed) |
| **TLS/mTLS** | Dễ (HTTPS) | redis-TLS (phức tạp hơn) | gRPC-TLS (built-in) |
| **Scalability 100+ node** | ⚠️ Thundering herd | ✅ Atomic INCR | ✅ **Multiplexed streams** |
| **Hệ thống thực dùng** | Webhook microservices | Celery, Sidekiq | **Flink, Spark, Ray, K8s** |

★ gRPC nhanh hơn nhiều trên WAN vì dùng **persistent connection** — không cần
TCP handshake + TLS handshake mỗi lần gọi (tiết kiệm ~50–100 ms/call).

---

## 5. Phân tích hiệu năng — Kịch bản server thực tế

### Kịch bản: 4 Server Node + 1 Server Coordinator + 200,000 events

```
Giả định:
- 4 server Node (mỗi server 4 core, 8 GB RAM)
- 1 server Coordinator
- Cùng datacenter, LAN RTT ~0.3 ms
- Mỗi Node xử lý 50,000 events
- Processing rate: ~80,000 events/sec/node (Python 3.12, single-core)
- Thời gian xử lý: ~625 ms/node (lý tưởng)
- Node chậm nhất (do skew): ~750 ms
```

#### Timeline — HTTP Webhook

```
t=0ms       Ingestor bắt đầu scatter log qua LAN
t=10ms      Tất cả Node bắt đầu nhận
t=625ms     Node 0 xong → nhận EOS → flush (0.5ms)
t=626ms     POST http://coordinator:8000/api/completed  ← TCP+TLS: 3ms
t=629ms     Coordinator nhận node_0
t=700ms     Node 2 xong → POST → 3ms
t=720ms     Node 1 xong → POST → 3ms
t=750ms     Node 3 (chậm nhất) xong → POST → 3ms
t=753ms     Coordinator nhận đủ 4/4 → aggregate

Overhead coordination:   ~3 ms
Pipeline total:          ~753 ms
Coordination / Total:    0.4%
```

#### Timeline — Redis INCR

```
t=625ms     Node 0: INCR (0.4ms RTT to Redis server)
t=700ms     Node 2: INCR
t=720ms     Node 1: INCR
t=750ms     Node 3: INCR → count=4 → PUBLISH (0.1ms)
t=750.5ms   Coordinator nhận → aggregate

Overhead coordination:   ~0.5 ms
Pipeline total:          ~750.5 ms
Coordination / Total:    0.07%
```

#### Timeline — gRPC Stream

```
t=0ms       Tất cả Node đã có persistent stream tới Coordinator
t=625ms     Node 0: yield NodeReport(phase="EOS_FLUSHED") → 0.1ms
t=700ms     Node 2: yield ... → 0.1ms
t=720ms     Node 1: yield ... → 0.1ms
t=750ms     Node 3: yield ... → 0.1ms → Coordinator nhận đủ 4/4
t=750.2ms   Coordinator send ALL_DONE → aggregate

Overhead coordination:   ~0.2 ms
Pipeline total:          ~750.2 ms
Coordination / Total:    0.03%
```

### So sánh timeline tổng hợp

```
HTTP:  ████████████████████████████████████████████████░░░  753 ms  (coordination: ░░░)
Redis: ████████████████████████████████████████████████░    750.5 ms
gRPC:  ████████████████████████████████████████████████░    750.2 ms
       0       100     200     300     400     500     600    700    750 ms

█ = processing time     ░ = coordination overhead
```

> **Nhận xét:** Trên LAN cùng datacenter, cả 3 phương án đều có overhead
> **không đáng kể** (< 1%). **Bottleneck là processing time, không phải
> coordination.** Sự khác biệt chỉ quan trọng khi:
> - Scale lên **100+ server** (thundering herd với HTTP)
> - Triển khai **xuyên datacenter** (gRPC thắng nhờ persistent connection)
> - Yêu cầu **phát hiện lỗi nhanh** (gRPC thắng tuyệt đối)

---

## 6. Khi nào phương án nào thắng?

### Quy mô nhỏ (4–16 server, cùng datacenter) — Phần lớn đồ án / startup

```
🥇 HTTP Webhook
   - Đơn giản nhất, debug bằng curl, log bằng Nginx access.log
   - Overhead: 1–3 ms → không đáng kể
   - Coordinator chỉ cần 1 file FastAPI 30 dòng
```

### Quy mô trung bình (16–100 server, cùng datacenter) — Công ty vừa

```
🥇 Redis Counter
   - Atomic INCR không bị thundering herd
   - Pub/Sub cho coordinator nhận tức thì
   - Redis đã có sẵn trong hầu hết hạ tầng
```

### Quy mô lớn (100+ server, multi-datacenter) — Big Tech / Cloud

```
🥇 gRPC Bidirectional Stream
   - Persistent connection → tiết kiệm hàng trăm TCP+TLS handshake
   - Phát hiện node chết ngay lập tức
   - Multiplexed → 1 connection cho mọi loại message
   - Đây là lý do Flink, Spark, Ray, Kubernetes đều dùng gRPC
```

---

## 7. Khuyến nghị cho đồ án

| Quyết định | Lựa chọn | Lý do |
|---|---|---|
| **Implement** | HTTP Webhook | Dễ demo, dễ hiểu, Coordinator = 1 FastAPI server. Giảng viên có thể `curl -X POST` để test |
| **Trình bày trong báo cáo** | So sánh cả 3 | Thể hiện hiểu biết kiến trúc phân tán. Ghi rõ trade-off ở từng quy mô |
| **Đề cập hướng phát triển** | gRPC Stream | "Nếu hệ thống scale lên 100+ node, chuyển sang gRPC để có persistent connection và fault detection tức thì" |

> **Lưu ý quan trọng:** Phần **In-band EOS Marker** (tín hiệu kết thúc
> đi chung luồng FIFO với dữ liệu) **không thay đổi** ở bất kỳ phương án nào.
> EOS Marker giải quyết **race condition giữa data và tín hiệu kết thúc**.
> Phương án A/B/C chỉ khác nhau ở bước **sau cùng** — Node đã flush xong,
> báo Coordinator bằng kênh nào. Bước này không có race condition.

---

## 8. Làm sao để đồ án "giống thực tế" — Lộ trình triển khai

Phần 1–7 ở trên là **lý thuyết kiến trúc**. Phần này hướng dẫn cụ thể cách
biến đồ án từ **mô phỏng trong 1 process Python** thành **hệ thống phân tán
chạy được, có thể kill node thật, inject network latency thật** — mà vẫn
vừa sức một đồ án sinh viên.

### 8.1. Ba mức "giống thực tế" — Chọn tier phù hợp

| Tier | Mức độ thật | Effort | Phù hợp |
|---|---|---|---|
| **T1** — Multi-process trên 1 máy | Mỗi node 1 process Python, giao tiếp qua `localhost:port` | 1–2 ngày | Demo cơ bản, đủ qua môn |
| **T2** — Docker Compose multi-container | Mỗi node 1 container, mạng Docker bridge thật, có thể `docker kill` | 2–4 ngày | ⭐ **Sweet spot — khuyến nghị** |
| **T3** — Multi-VM (Vagrant) hoặc cloud thật (2–3 EC2/GCE) | Mạng vật lý, RTT thật, NTP thật, có thể rút cable | 1+ tuần + tốn tiền | Quá tham vọng, không cần cho đồ án |

→ Tài liệu này hướng dẫn **T2 (Docker Compose)** — đủ "thật" để chứng minh
mọi failure mode trong Section 3, không cần thuê cloud, demo gọn.

### 8.2. Kiến trúc triển khai T2

```
┌──────────────────────────────────────────────────────────────┐
│                    docker-compose.yml                         │
│                                                                │
│  ingestor   coordinator   node0   node1   node2   node3       │
│   :9000       :8000       :8101   :8102   :8103   :8104       │
│     │           ▲           ▲       ▲       ▲       ▲          │
│     │           │           │       │       │       │          │
│     └─POST log──┼───────────┴───────┴───────┴───────┘          │
│                 │                                              │
│                 └─ HTTP /api/completed (từ mỗi node)          │
│                                                                │
│  prometheus    grafana    (optional: redis, jaeger)           │
│   :9090         :3000                                          │
│                                                                │
│  Mạng: Docker bridge network (giả lập LAN cùng datacenter)    │
└──────────────────────────────────────────────────────────────┘
```

Mỗi service = 1 container = "1 server riêng". Mạng Docker bridge mô phỏng
LAN datacenter với RTT thật (~0.05–0.2 ms).

### 8.3. Cấu trúc thư mục đề xuất

```
csdlpt/
├── wm/                    # Core engine (giữ nguyên)
├── coordinator/
│   ├── Dockerfile
│   ├── main.py            # FastAPI coordinator
│   └── requirements.txt
├── node/
│   ├── Dockerfile
│   ├── main.py            # FastAPI node wrapper quanh wm.engine
│   └── requirements.txt
├── ingestor/
│   ├── Dockerfile
│   ├── ingest.py          # Đọc CSV, scatter sang node theo hash
│   └── requirements.txt
├── docker-compose.yml
├── prometheus.yml
└── scripts/
    ├── demo_happy_path.sh
    ├── demo_kill_node.sh
    └── demo_network_partition.sh
```

### 8.4. Code cốt lõi — 3 service

> **Phiên bản basic** — đủ để demo happy path. Để xử lý đúng các failure mode
> trong Section 3 (node chết, partition, double-report, data loss), xem
> **Section 8.13 — Protocol phát EOS chuẩn**, có upgraded code với ACK check,
> DLQ, heartbeat tách kênh, và integrity validation.

#### Node service (`node/main.py`)

```python
from fastapi import FastAPI
from pydantic import BaseModel
from wm.engine import WatermarkEngine
import os, httpx, asyncio

app = FastAPI()
engine = WatermarkEngine(window_size=60, wait_time=30)
NODE_ID = int(os.environ["NODE_ID"])
COORDINATOR = os.environ["COORDINATOR_URL"]
RUN_ID = os.environ.get("RUN_ID", "default")

class Event(BaseModel):
    type: str          # "data" | "EOS"
    key: str | None = None
    ts: float | None = None
    value: dict | None = None

@app.post("/ingest")
async def ingest(event: Event):
    if event.type == "EOS":
        engine.flush()
        # Báo coordinator với retry + backoff
        async with httpx.AsyncClient(timeout=5) as client:
            for attempt in range(5):
                try:
                    await client.post(
                        f"{COORDINATOR}/api/completed",
                        json={
                            "run_id": RUN_ID,
                            "node_id": NODE_ID,
                            "events": engine.metrics["total"],
                            "completeness": engine.get_completeness(),
                        },
                    )
                    break
                except (httpx.ConnectError, httpx.TimeoutException):
                    await asyncio.sleep(min(2**attempt, 10))
        return {"status": "EOS_FLUSHED"}

    engine.process(event.dict())
    return {"ok": True}

@app.get("/health")
def health():
    return {
        "node_id": NODE_ID,
        "watermark": engine.watermark,
        "processed": engine.metrics["total"],
    }

@app.get("/metrics")  # Prometheus scrape endpoint
def metrics():
    return (
        f'wm_watermark{{node="{NODE_ID}"}} {engine.watermark}\n'
        f'wm_events_total{{node="{NODE_ID}"}} {engine.metrics["total"]}\n'
        f'wm_completeness{{node="{NODE_ID}"}} {engine.get_completeness()}\n'
    )
```

#### Coordinator service (`coordinator/main.py`)

```python
from fastapi import FastAPI
from pydantic import BaseModel
import asyncio, os, uuid

app = FastAPI()
N = int(os.environ["TOTAL_NODES"])
RUN_ID = os.environ.get("RUN_ID", str(uuid.uuid4()))

completed: dict[int, dict] = {}
done_event = asyncio.Event()
lock = asyncio.Lock()

class Report(BaseModel):
    run_id: str
    node_id: int
    events: int
    completeness: float

@app.post("/api/completed")
async def node_completed(r: Report):
    if r.run_id != RUN_ID:
        return {"ack": False, "reason": "stale_run_id"}    # Idempotent + safe
    async with lock:
        completed[r.node_id] = r.dict()                    # Set semantics
        if len(completed) == N:
            done_event.set()
    return {"ack": True, "received": len(completed), "expected": N}

@app.get("/api/wait")
async def wait(timeout: int = 60):
    try:
        await asyncio.wait_for(done_event.wait(), timeout)
        return {"status": "ALL_DONE", "run_id": RUN_ID, "results": completed}
    except asyncio.TimeoutError:
        missing = sorted(set(range(N)) - set(completed.keys()))
        return {
            "status": "TIMEOUT",
            "run_id": RUN_ID,
            "received": list(completed.keys()),
            "missing": missing,
        }

@app.get("/health")
def health():
    return {"run_id": RUN_ID, "received": len(completed), "expected": N}
```

#### Ingestor (`ingestor/ingest.py`)

```python
import httpx, csv, hashlib, os, sys

NODE_HOSTS = os.environ["NODE_HOSTS"].split(",")    # "node0:8000,node1:8000,..."
N = len(NODE_HOSTS)

def route(key: str) -> str:
    h = int(hashlib.md5(key.encode()).hexdigest(), 16)
    return NODE_HOSTS[h % N]

def main(csv_path):
    with httpx.Client(timeout=10) as client, open(csv_path) as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            target = route(row["host"])
            client.post(f"http://{target}/ingest",
                        json={"type": "data", "key": row["host"],
                              "ts": float(row["ts"]), "value": row})
            if i % 10000 == 0:
                print(f"sent {i} events")

        # Gửi EOS tới TẤT CẢ node
        for host in NODE_HOSTS:
            client.post(f"http://{host}/ingest", json={"type": "EOS"})
        print("EOS sent to all nodes")

if __name__ == "__main__":
    main(sys.argv[1])
```

### 8.5. `docker-compose.yml` hoàn chỉnh

```yaml
version: "3.9"

x-node-common: &node-common
  build: ./node
  environment:
    COORDINATOR_URL: http://coordinator:8000
    RUN_ID: ${RUN_ID:-default}
  depends_on:
    coordinator:
      condition: service_healthy

services:
  coordinator:
    build: ./coordinator
    environment:
      TOTAL_NODES: 4
      RUN_ID: ${RUN_ID:-default}
    ports: ["8000:8000"]
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 2s
      retries: 10

  node0:
    <<: *node-common
    environment: { NODE_ID: 0, COORDINATOR_URL: http://coordinator:8000 }
    ports: ["8101:8000"]

  node1:
    <<: *node-common
    environment: { NODE_ID: 1, COORDINATOR_URL: http://coordinator:8000 }
    ports: ["8102:8000"]

  node2:
    <<: *node-common
    environment: { NODE_ID: 2, COORDINATOR_URL: http://coordinator:8000 }
    ports: ["8103:8000"]

  node3:
    <<: *node-common
    environment: { NODE_ID: 3, COORDINATOR_URL: http://coordinator:8000 }
    ports: ["8104:8000"]

  ingestor:
    build: ./ingestor
    environment:
      NODE_HOSTS: "node0:8000,node1:8000,node2:8000,node3:8000"
    depends_on: [node0, node1, node2, node3]
    volumes: ["./dataset:/data:ro"]
    profiles: ["manual"]     # Không tự chạy, gọi `docker compose run ingestor`

  prometheus:
    image: prom/prometheus:latest
    volumes: ["./prometheus.yml:/etc/prometheus/prometheus.yml:ro"]
    ports: ["9090:9090"]

  grafana:
    image: grafana/grafana:latest
    ports: ["3000:3000"]
    environment:
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: Admin
```

### 8.6. Inject failure thật — Đây là phần "wow" của demo

Đây là cái **mô phỏng trong 1 process không bao giờ làm được**, và là điểm
giảng viên sẽ thấy ấn tượng nhất.

#### Kill 1 node giữa chừng (Failure mode "Node chết")
```bash
docker kill csdlpt-node2-1
# → Coordinator timeout sau T giây, /api/wait trả về missing: [2]
# → Grafana: throughput node2 tụt về 0
```

#### Network latency (Failure mode "WAN latency")
```bash
# Cần image có `tc` — sửa Dockerfile node thêm: RUN apt-get install -y iproute2
docker exec csdlpt-node1-1 tc qdisc add dev eth0 root netem delay 100ms
# → Grafana: latency báo EOS của node1 tăng 100ms
```

#### Packet loss (Failure mode "Network unreliable")
```bash
docker exec csdlpt-node3-1 tc qdisc add dev eth0 root netem loss 5%
# → Retry logic phải đảm bảo cuối cùng vẫn báo được
```

#### Network partition (Failure mode "Switch hỏng")
```bash
docker network disconnect csdlpt_default csdlpt-node0-1
sleep 10
docker network connect csdlpt_default csdlpt-node0-1
# → Coordinator timeout, sau khi reconnect node0 retry và báo thành công
```

#### Coordinator chết (Failure mode "Coordinator down")
```bash
docker kill csdlpt-coordinator-1
sleep 5
docker compose up -d coordinator
# → Node retry vẫn còn trong queue, eventually báo được khi coordinator hồi sinh
```

### 8.7. Observability — Prometheus + Grafana

`prometheus.yml`:
```yaml
global:
  scrape_interval: 2s
scrape_configs:
  - job_name: nodes
    static_configs:
      - targets: ['node0:8000', 'node1:8000', 'node2:8000', 'node3:8000']
    metrics_path: /metrics
```

Grafana dashboard nên có 4 panel:
1. **Watermark per node** (line chart) — thấy node nào tụt watermark
2. **Throughput per node** (events/sec) — thấy node nào chết = đường tụt về 0
3. **Completeness per node** (gauge) — % data đã đóng window
4. **Coordinator status** (stat panel) — `received / expected`

Mỗi lần demo `docker kill`, dashboard sẽ phản ánh tức thì → trực quan cho
giảng viên xem.

### 8.8. Kịch bản demo trình bày (10 phút)

```bash
# (0:00) Start cluster + mở Grafana ở browser tab khác
docker compose up -d
docker compose ps                           # show 4 nodes + coordinator UP
open http://localhost:3000                  # Grafana dashboard

# (1:00) Bắn 200K events từ dataset thật
docker compose run --rm ingestor python ingest.py /data/nasa_http.csv &

# (2:00) Trong lúc đang chạy → kill 1 node
docker kill csdlpt-node2-1
# → Grafana: throughput node2 = 0, watermark đứng yên
# → curl localhost:8000/api/wait?timeout=5 → TIMEOUT, missing: [2]

# (4:00) Revive node2
docker compose up -d node2
# → Replay từ DLQ (nếu có), eventually báo coordinator

# (6:00) Inject network latency vào node1
docker exec csdlpt-node1-1 tc qdisc add dev eth0 root netem delay 100ms
# → Grafana: completeness node1 giảm, lag tăng

# (8:00) Show kết quả cuối
curl localhost:8000/api/wait | jq
# → ALL_DONE, results đầy đủ 4 node với completeness từng node

# (9:00) Stop & cleanup
docker compose down -v
```

### 8.9. Acceptance criteria — Đồ án "đạt" khi nào?

| # | Tiêu chí | Cách kiểm chứng |
|---|---|---|
| 1 | Happy path 4 node chạy xong < 2 phút với 200K event | `time docker compose run ingestor ...` |
| 2 | Kill 1 node giữa chừng → Coordinator phát hiện timeout | `curl /api/wait?timeout=10` trả `TIMEOUT` |
| 3 | Revive node → tiếp tục báo thành công | Coordinator nhận đủ 4/4 sau revive |
| 4 | Network latency 100ms không làm sai kết quả | Tổng `events` không đổi, chỉ có latency tăng |
| 5 | Partition + reconnect → idempotent, không double-count | `completed[node_id]` là dict, gọi 2 lần vẫn 1 entry |
| 6 | Coordinator restart → node retry pending vẫn báo được | Kill coordinator, restart, kiểm tra `/api/wait` |
| 7 | Grafana dashboard hiển thị realtime đủ 4 metric | Screenshot trong báo cáo |

### 8.10. Lộ trình thực hiện đề xuất (1 tuần)

| Ngày | Việc | Output |
|---|---|---|
| 1 | Wrap `wm/engine.py` thành `node/main.py` FastAPI, test local | `uvicorn node.main:app` chạy được |
| 2 | Viết `coordinator/main.py` + `ingestor/ingest.py`, test multi-process local | 4 process trên 1 máy chạy thông |
| 3 | Viết 3 Dockerfile + `docker-compose.yml`, `docker compose up` chạy được | Cluster T2 chạy ổn |
| 4 | Thêm Prometheus + Grafana, làm dashboard | Screenshot Grafana đẹp |
| 5 | Viết 3 demo script (happy / kill / partition) | `scripts/*.sh` chạy được |
| 6 | Chạy lại tất cả acceptance criteria, ghi lại số liệu thật | Bảng số liệu thay cho ước lượng ở Section 5 |
| 7 | Cập nhật `REPORT.md` với screenshot + số liệu mới | Báo cáo hoàn chỉnh |

### 8.11. Cái KHÔNG nên làm (overkill cho đồ án)

| ❌ Cái này | Lý do bỏ qua |
|---|---|
| Kubernetes | Quá phức tạp, không thêm điểm; docker-compose đủ chứng minh kiến trúc |
| gRPC thay HTTP | Proto-gen rườm rà, demo bằng `curl` không được, lợi ích chỉ rõ ở 100+ node |
| Raft / Consul / etcd | Coordinator không cần consensus — chỉ cần đếm |
| Multi-region cloud | Tốn tiền, không kiểm chứng được trong phòng lab |
| Service mesh (Istio, Linkerd) | Sai context — đây là batch coordination, không phải request routing |
| Kafka thay HTTP webhook | Kafka cluster + ZooKeeper là 1 đồ án riêng, không nên gộp |

### 8.12. Mapping với Section 7

| Section 7 nói | Section 8 hiện thực hóa bằng |
|---|---|
| "Implement: HTTP Webhook" | `coordinator/main.py` + `node/main.py` HTTP |
| "Coordinator = 1 FastAPI server" | Container `coordinator` trong compose |
| "Giảng viên có thể `curl -X POST` để test" | Port `8000:8000` mở ra host |
| "So sánh cả 3 trong báo cáo" | Section 4 đã có; bổ sung số đo thật từ T2 |
| "gRPC là hướng phát triển" | Section 8.11 ghi rõ khi nào cần chuyển |

---

## 8.13. Protocol phát EOS chuẩn lên Coordination

Section 8.4 đủ cho happy path. Phần này định nghĩa **protocol production-grade**
để hệ thống chịu được mọi failure mode trong Section 3 mà vẫn cho kết quả đúng.

### 8.13.1. Nguyên tắc nền tảng

**In-band EOS marker** và **out-of-band EOS report** là hai cơ chế khác nhau,
giải hai vấn đề khác nhau — **không được gộp**:

| Cơ chế | Kênh | Giải bài toán |
|---|---|---|
| **In-band EOS marker** | Ingestor → Node (cùng FIFO với data) | Race giữa data cuối cùng và tín hiệu kết thúc |
| **Out-of-band EOS report** | Node → Coordinator (kênh riêng) | Notify aggregator rằng node đã flush xong |

Node **chỉ phát out-of-band report sau khi `flush()` blocking xong**. Báo sớm
một mili-giây = `completeness` sai = aggregate sai.

### 8.13.2. Sequence diagram

```
Ingestor             Node i                  Coordinator             DLQ (local)
   │                   │                          │                       │
   │ POST /run_started ┼─────────────────────────▶│ scatter_total=?       │
   │                   │                          │                       │
   │── data_1..N ─────▶│ process()                │                       │
   │                   │── HB (5s/lần) ──────────▶│ last_hb[i]=now        │
   │                   │                          │                       │
   │── EOS{run_id} ───▶│ drain input              │                       │
   │                   │ flush() blocking         │                       │
   │                   │ build_report(run_id,…)   │                       │
   │                   │── POST /completed ──────▶│ check run_id          │
   │                   │                          │ completed[i]=report   │
   │                   │◀──── 200 {ack:true} ─────│                       │
   │                   │ log(ACKED) → exit OK     │                       │
   │                   │                          │                       │
   │                   │ (alt: hết 5 retry)       │                       │
   │                   │ persist ──────────────────────────────────────▶ │
   │                   │ exit DEGRADED            │                       │
   │                   │                          │                       │
   │ POST /run_started(scatter_total=N_sent) ────▶│ validate sau khi đủ N │
   │                   │                          │                       │
   │                   │ (next start: replay)     │                       │
   │                   │◀── load ──────────────────────────────────────── │
   │                   │── POST /completed ──────▶│                       │
```

### 8.13.3. Contract bắt buộc

| # | Quy tắc | Lý do |
|---|---|---|
| C1 | Node chỉ POST `/completed` **sau khi** `flush()` blocking trả về | Báo sớm = aggregate sai completeness |
| C2 | `run_id` sinh tại ingestor, đi cùng EOS marker (in-band) | Tránh node của run cũ báo nhầm vào run mới |
| C3 | Coordinator reject mọi report `run_id != current` | Fence epoch — gateway chống dirty data |
| C4 | Coordinator dùng `dict[node_id]`, **không** `INCR` counter | Idempotent: retry 2 lần vẫn 1 entry |
| C5 | Node chỉ coi "thành công" khi response body có `ack: true` | Chỉ check no-exception → false positive khi proxy đứt giữa |
| C6 | Retry exponential `min(2^k, 10s)`, tối đa 5 lần, tổng ≤60s | Tránh treo node vô hạn khi coordinator chết |
| C7 | Hết retry → ghi DLQ + emit metric, **không** crash node | Cho phép node được respawn và replay DLQ |
| C8 | Heartbeat ở kênh riêng, 5s/lần, lifecycle độc lập với EOS | Phân biệt "slow node" vs "dead node" |
| C9 | `/api/wait` có hard timeout, trả `diagnosis` cho missing nodes | Aggregator không treo vô hạn |
| C10 | Log JSON line ở mọi phase transition | Truy vết được trên Loki/jq |
| C11 | Coordinator validate `sum(events_processed) == scatter_total` | Phát hiện data loss trên đường truyền |

### 8.13.4. Payload schema (v1)

```json
{
  "run_id": "run-1716480000-abc123",
  "node_id": 2,
  "events_processed": 50000,
  "watermark_final": 1716480600.0,
  "completeness": 0.987,
  "dropped_late": 47,
  "flush_ts": 1716480600.412,
  "flush_duration_ms": 12.4,
  "schema_version": 1
}
```

| Trường | Mục đích |
|---|---|
| `run_id` | Fence epoch — coordinator reject stale (C3) |
| `node_id` | Key cho set-semantics ở coordinator (C4) |
| `events_processed` | Đối chiếu với `scatter_total` từ ingestor → phát hiện data loss (C11) |
| `watermark_final` | Chứng minh node đã đóng tới đâu |
| `completeness` | % data đã trong window đóng |
| `dropped_late` | Số event đến sau watermark (drop policy) |
| `flush_ts`, `flush_duration_ms` | Đo skew & tốc độ flush giữa các node |
| `schema_version` | Cho phép tiến hóa contract về sau |

### 8.13.5. Lifecycle phases

Mỗi transition log 1 dòng JSON, dùng làm tag cho heartbeat:

```
INIT → READY → PROCESSING → EOS_RECEIVED → FLUSHING → FLUSHED
     → REPORTING → ACKED → DONE
                         └─ (retry exhausted) → DEGRADED (DLQ persisted)
```

Ví dụ log:
```json
{"ts": 1716480600.412, "node_id": 2, "run_id": "run-...", "phase": "FLUSHED", "events": 50000, "duration_ms": 12.4}
```

### 8.13.6. Upgraded code

#### Node (`node/main.py`)

```python
from fastapi import FastAPI
from pydantic import BaseModel
from wm.engine import WatermarkEngine
import os, json, time, asyncio, httpx
from pathlib import Path

app = FastAPI()
engine = WatermarkEngine(window_size=60, wait_time=30)
NODE_ID = int(os.environ["NODE_ID"])
COORDINATOR = os.environ["COORDINATOR_URL"]
DLQ_PATH = Path(f"/var/lib/csdlpt/dlq/node-{NODE_ID}.jsonl")

state = {"phase": "INIT", "run_id": None, "eos_acked": False}

def log_phase(phase: str, **extra):
    state["phase"] = phase
    print(json.dumps({"ts": time.time(), "node_id": NODE_ID,
                      "run_id": state["run_id"], "phase": phase, **extra}),
          flush=True)

class Event(BaseModel):
    type: str                    # "data" | "EOS"
    run_id: str | None = None    # đi cùng EOS marker (C2)
    key: str | None = None
    ts: float | None = None
    value: dict | None = None

async def send_with_ack(client: httpx.AsyncClient, report: dict) -> bool:
    """C5: chỉ coi thành công khi body có ack:true."""
    try:
        r = await client.post(f"{COORDINATOR}/api/completed", json=report)
        return r.status_code == 200 and r.json().get("ack") is True
    except (httpx.ConnectError, httpx.TimeoutException, httpx.HTTPError):
        return False

def persist_dlq(report: dict) -> None:
    """C7: ghi DLQ local thay vì crash."""
    DLQ_PATH.parent.mkdir(parents=True, exist_ok=True)
    with DLQ_PATH.open("a") as f:
        f.write(json.dumps({"queued_at": time.time(), **report}) + "\n")

async def replay_dlq(client: httpx.AsyncClient) -> None:
    """Gửi lại report tồn đọng từ run trước khi node respawn."""
    if not DLQ_PATH.exists():
        return
    pending = [json.loads(l) for l in DLQ_PATH.read_text().splitlines() if l]
    kept = [r for r in pending if not await send_with_ack(client, r)]
    DLQ_PATH.write_text(
        "\n".join(json.dumps(r) for r in kept) + ("\n" if kept else "")
    )

async def heartbeat_loop():
    """C8: kênh riêng, lifecycle độc lập."""
    async with httpx.AsyncClient(timeout=2) as c:
        while not state["eos_acked"]:
            try:
                await c.post(f"{COORDINATOR}/api/heartbeat",
                             json={"run_id": state["run_id"],
                                   "node_id": NODE_ID,
                                   "phase": state["phase"],
                                   "ts": time.time()})
            except Exception:
                pass    # HB lost is OK
            await asyncio.sleep(5)

@app.on_event("startup")
async def on_startup():
    log_phase("READY")
    asyncio.create_task(heartbeat_loop())
    async with httpx.AsyncClient(timeout=5) as c:
        await replay_dlq(c)

@app.post("/ingest")
async def ingest(event: Event):
    if event.type == "EOS":
        state["run_id"] = event.run_id
        log_phase("EOS_RECEIVED")

        # C1: flush BLOCKING trước khi build report
        log_phase("FLUSHING")
        t0 = time.time()
        engine.flush()
        flush_ms = (time.time() - t0) * 1000
        log_phase("FLUSHED", duration_ms=flush_ms,
                  events=engine.metrics["total"])

        report = {
            "run_id": state["run_id"],
            "node_id": NODE_ID,
            "events_processed": engine.metrics["total"],
            "watermark_final": engine.watermark,
            "completeness": engine.get_completeness(),
            "dropped_late": engine.metrics.get("dropped_late", 0),
            "flush_ts": time.time(),
            "flush_duration_ms": flush_ms,
            "schema_version": 1,
        }

        log_phase("REPORTING")
        async with httpx.AsyncClient(timeout=5) as client:
            for attempt in range(5):                  # C6: ≤5 lần
                if await send_with_ack(client, report):
                    state["eos_acked"] = True
                    log_phase("ACKED")
                    return {"status": "EOS_ACKED"}
                await asyncio.sleep(min(2 ** attempt, 10))

        # C7: hết retry, persist DLQ, không crash
        persist_dlq(report)
        log_phase("DEGRADED", reason="ack_timeout_after_5_retries")
        return {"status": "EOS_DEGRADED_DLQ"}

    engine.process(event.dict())
    return {"ok": True}

@app.get("/health")
def health():
    return {"node_id": NODE_ID, "phase": state["phase"],
            "watermark": engine.watermark,
            "processed": engine.metrics["total"]}
```

Mount DLQ volume trong `docker-compose.yml`:
```yaml
node0:
  <<: *node-common
  environment: { NODE_ID: 0, COORDINATOR_URL: http://coordinator:8000 }
  volumes: ["./dlq/node0:/var/lib/csdlpt/dlq"]
  ports: ["8101:8000"]
```

#### Coordinator (`coordinator/main.py`)

```python
from fastapi import FastAPI
from pydantic import BaseModel
import asyncio, os, time, uuid

app = FastAPI()
N = int(os.environ["TOTAL_NODES"])
RUN_ID = os.environ.get("RUN_ID") or f"run-{int(time.time())}-{uuid.uuid4().hex[:6]}"

completed: dict[int, dict] = {}
last_hb: dict[int, float] = {}
scatter_total: int | None = None
done_event = asyncio.Event()
lock = asyncio.Lock()

class Report(BaseModel):
    run_id: str
    node_id: int
    events_processed: int
    watermark_final: float
    completeness: float
    dropped_late: int = 0
    flush_ts: float
    flush_duration_ms: float
    schema_version: int = 1

class Heartbeat(BaseModel):
    run_id: str | None = None
    node_id: int
    phase: str
    ts: float

class RunStarted(BaseModel):
    run_id: str
    scatter_total: int       # -1 nếu ingestor chưa biết tổng

@app.post("/api/run_started")
async def run_started(r: RunStarted):
    global scatter_total
    if r.run_id != RUN_ID:
        return {"ack": False, "reason": "stale_run_id"}
    if r.scatter_total >= 0:
        scatter_total = r.scatter_total
    return {"ack": True, "run_id": RUN_ID}

@app.post("/api/heartbeat")
async def heartbeat(hb: Heartbeat):
    if hb.run_id and hb.run_id != RUN_ID:
        return {"ack": False, "reason": "stale_run_id"}
    last_hb[hb.node_id] = hb.ts
    return {"ack": True}

@app.post("/api/completed")
async def node_completed(r: Report):
    if r.run_id != RUN_ID:                        # C3
        return {"ack": False, "reason": "stale_run_id"}
    async with lock:
        completed[r.node_id] = r.dict()           # C4: idempotent
        if len(completed) == N:
            done_event.set()
    return {"ack": True, "received": len(completed), "expected": N}

@app.get("/api/wait")
async def wait(timeout: int = 60):
    try:
        await asyncio.wait_for(done_event.wait(), timeout)
    except asyncio.TimeoutError:
        # C9: trả diagnosis cho missing
        now = time.time()
        missing = sorted(set(range(N)) - set(completed.keys()))
        diagnosis = {}
        for nid in missing:
            if nid not in last_hb:
                diagnosis[nid] = "NEVER_SEEN"
            else:
                gap = now - last_hb[nid]
                diagnosis[nid] = "DEAD" if gap > 15 else f"SLOW({gap:.1f}s_since_hb)"
        return {"status": "TIMEOUT", "run_id": RUN_ID,
                "received": sorted(completed.keys()),
                "missing": missing,
                "diagnosis": diagnosis}

    # All done — C11: integrity validation
    received_events = sum(r["events_processed"] for r in completed.values())
    integrity_ok = (scatter_total is None) or (received_events == scatter_total)
    return {
        "status": "ALL_DONE" if integrity_ok else "DATA_LOSS",
        "run_id": RUN_ID,
        "events_received": received_events,
        "scatter_total": scatter_total,
        "missing_events": (scatter_total or 0) - received_events,
        "avg_completeness": sum(r["completeness"] for r in completed.values()) / N,
        "results": completed,
    }

@app.get("/health")
def health():
    return {"run_id": RUN_ID, "received": len(completed),
            "expected": N, "heartbeats_seen": len(last_hb)}
```

#### Ingestor (`ingestor/ingest.py`) — delta so với 8.4

```python
import httpx, csv, hashlib, os, sys, uuid, time

NODE_HOSTS = os.environ["NODE_HOSTS"].split(",")
COORDINATOR = os.environ.get("COORDINATOR_URL", "http://coordinator:8000")
N = len(NODE_HOSTS)
RUN_ID = os.environ.get("RUN_ID") or f"run-{int(time.time())}-{uuid.uuid4().hex[:6]}"

def route(key: str) -> str:
    h = int(hashlib.md5(key.encode()).hexdigest(), 16)
    return NODE_HOSTS[h % N]

def main(csv_path):
    sent = 0
    with httpx.Client(timeout=10) as client, open(csv_path) as f:
        # Bắt đầu run — coordinator biết run_id hiện tại
        client.post(f"{COORDINATOR}/api/run_started",
                    json={"run_id": RUN_ID, "scatter_total": -1})

        for row in csv.DictReader(f):
            target = route(row["host"])
            client.post(f"http://{target}/ingest",
                        json={"type": "data", "key": row["host"],
                              "ts": float(row["ts"]), "value": row})
            sent += 1
            if sent % 10000 == 0:
                print(f"sent {sent} events")

        # EOS in-band với run_id (C2) — fence cho mọi node
        for host in NODE_HOSTS:
            client.post(f"http://{host}/ingest",
                        json={"type": "EOS", "run_id": RUN_ID})

        # Đóng scatter — coordinator sẽ validate integrity
        client.post(f"{COORDINATOR}/api/run_started",
                    json={"run_id": RUN_ID, "scatter_total": sent})
        print(f"EOS sent. run_id={RUN_ID}, scatter_total={sent}")

if __name__ == "__main__":
    main(sys.argv[1])
```

### 8.13.7. Test matrix — Validate protocol

Mỗi failure mode trong Section 3 phải có 1 test script đi kèm:

| Test | Hành động | Kết quả mong đợi |
|---|---|---|
| **happy** | Chạy đủ 4 node, không inject lỗi | `ALL_DONE`, `missing_events: 0`, `avg_completeness > 0.95` |
| **kill_node** | `docker kill node2` sau 2s | `TIMEOUT`, `diagnosis[2]: "DEAD"`, `missing: [2]` |
| **revive_node** | Kill node2, sau 5s `docker compose up -d node2` | `ALL_DONE` sau revive (DLQ trống vì chưa kịp ghi) |
| **slow_node** | `tc qdisc add netem delay 100ms` lên node1 | `ALL_DONE`, latency `flush_duration_ms` node1 tăng |
| **partition** | `docker network disconnect` node0 sau khi nó flushed | Hết 5 retry → DLQ. Reconnect → replay → `ALL_DONE` |
| **coordinator_down** | Kill coordinator sau khi node0 đã FLUSHED, restart sau 8s | Node0 trong retry loop → ACKED khi coordinator hồi sinh |
| **double_report** | Manually `curl POST /completed` 2 lần với cùng node_id | `len(completed)` không đổi (C4) |
| **stale_run** | POST `/completed` với `run_id` cũ | Nhận `{ack: false, reason: "stale_run_id"}` (C3) |
| **data_loss** | Drop 1 event trên 1 node (sửa code inject) | `status: "DATA_LOSS"`, `missing_events: 1` (C11) |

Mỗi test = 1 file `scripts/test_<name>.sh` returning exit 0/1 → có thể chạy
trong CI hoặc demo từng cái cho giảng viên.

### 8.13.8. Tóm tắt "đủ ổn" là gì

Hệ thống được coi là phát EOS lên coordination **đúng và ổn** khi đồng thời:

1. **Không bao giờ aggregate sai**: report luôn đến sau flush (C1), idempotent (C4), reject stale (C3).
2. **Không bao giờ treo vô hạn**: bounded retry (C6), hard timeout (C9), heartbeat-based liveness (C8).
3. **Không bao giờ mất silently**: DLQ + replay (C7), integrity check `scatter == sum(received)` (C11).
4. **Debug được khi sai**: structured log + phase + diagnosis trong response (C10, C9).

Đạt 9/9 test trong 8.13.7 = đủ điều kiện trình bày là **"distributed EOS barrier hoạt động đúng trên hạ tầng container giả lập datacenter"**.

---

# 9. REBUILD PLAN — Làm lại toàn bộ tầng triển khai

Section 1–8 là **lý thuyết kiến trúc + design contract**. Section 9 này là
**playbook thực thi** — bước-từng-bước để xây dựng lại đồ án từ con số không
trên tầng container, áp dụng protocol §8.13 làm spec cứng.

> **Tiền đề:** Giữ nguyên `wm/engine.py` (đã đạt R1–R4 theo `OBJECTIVES.md`).
> Xây mới tầng deployment: 3 service (node, coordinator, ingestor) + observability
> + chaos testing. Mục tiêu: chứng minh distributed watermark tracker chạy được
> trên hạ tầng container giả lập datacenter, đối phó được mọi failure mode.

---

## 9.1. Nguyên tắc chỉ đạo

1. **Engine không sửa.** `wm/engine.py` đã pass tất cả unit test cho watermark logic. Mọi thay đổi distributed nằm ở wrapper, không sửa core.
2. **Protocol §8.13 là spec cứng.** 11 contract (C1–C11) là điều kiện chấp nhận. Mọi commit phải bảo toàn các contract này.
3. **Mỗi failure mode = 1 test script.** Không tin "design đúng" cho đến khi có script chạy được.
4. **Demo-driven.** Mọi feature phải show được trên Grafana dashboard hoặc curl. Code không demo được = code không tồn tại.
5. **Idempotent build.** `make clean && make all` phải tái lập 100% từ zero.

---

## 9.2. Cấu trúc thư mục mục tiêu

```
csdlpt/
├── wm/                          # CORE — KHÔNG SỬA
│   ├── engine.py                # WatermarkEngine (existing, R1-R4)
│   ├── sweep.py
│   ├── demos.py
│   └── data/
│       ├── nasa.py
│       └── synthetic.py
│
├── deploy/                      # NEW — toàn bộ tầng triển khai
│   ├── node/
│   │   ├── Dockerfile
│   │   ├── main.py              # FastAPI wrapper quanh WatermarkEngine
│   │   ├── dlq.py               # DLQ persist + replay
│   │   ├── heartbeat.py         # Background heartbeat loop
│   │   └── requirements.txt
│   │
│   ├── coordinator/
│   │   ├── Dockerfile
│   │   ├── main.py              # FastAPI barrier + diagnosis
│   │   ├── validator.py         # Integrity check (C11)
│   │   └── requirements.txt
│   │
│   ├── ingestor/
│   │   ├── Dockerfile
│   │   ├── ingest.py            # Scatter + RUN_ID + scatter_total
│   │   └── requirements.txt
│   │
│   ├── docker-compose.yml
│   ├── docker-compose.chaos.yml # Override: thêm tc/iptables capabilities
│   ├── prometheus.yml
│   └── grafana/
│       ├── dashboards/
│       │   └── watermark.json
│       └── provisioning/
│
├── scripts/                     # NEW — chaos + acceptance tests
│   ├── lib.sh                   # Helper: curl_json, wait_for, assert_*
│   ├── test_01_happy.sh
│   ├── test_02_kill_node.sh
│   ├── test_03_revive_node.sh
│   ├── test_04_slow_node.sh
│   ├── test_05_partition.sh
│   ├── test_06_coordinator_down.sh
│   ├── test_07_double_report.sh
│   ├── test_08_stale_run.sh
│   ├── test_09_data_loss.sh
│   └── run_all.sh               # Chạy tuần tự + tổng kết
│
├── Makefile                     # NEW — entry point cho mọi thao tác
├── dataset/                     # Mount vào ingestor (existing)
├── dlq/                         # Mount per-node DLQ volume (gitignored)
├── docs/                        # Existing
├── pending/                     # Existing
└── app.py                       # Existing dashboard (giữ nguyên)
```

---

## 9.3. Roadmap 7 ngày — Sprint cứng

| Ngày | Sprint | Output bắt buộc | Cách verify |
|---|---|---|---|
| **D1** | **Skeleton** | 3 Dockerfile build pass, container chạy `--help` không crash | `make build && make smoke` exit 0 |
| **D2** | **Happy path** | `test_01_happy.sh` pass: 4 node + ingestor + coordinator, 200K event, ALL_DONE | `bash scripts/test_01_happy.sh` exit 0, completeness ≥ 0.95 |
| **D3** | **Failure C1–C5** | Test 02 (kill), 07 (double), 08 (stale) pass | `bash scripts/run_all.sh` 3/9 pass |
| **D4** | **Failure C6–C8** | Test 03 (revive), 04 (slow), 06 (coord down) pass | 6/9 pass |
| **D5** | **Failure C9–C11** | Test 05 (partition), 09 (data loss) pass | 9/9 pass |
| **D6** | **Observability** | Prometheus scrape OK, Grafana dashboard 4 panel hoạt động | Screenshot mỗi panel có data |
| **D7** | **Polish + Demo** | Demo script 10 phút (§8.8), README rebuild, REPORT.md cập nhật số đo thật | Quay video, deploy lại từ zero ≤ 5 phút |

**Gate:** Không sang sprint sau nếu sprint trước chưa pass acceptance. Đây là tránh "code chồng code" mà không có gì chạy.

---

## 9.4. Sprint D1 — Skeleton (Day 1)

### Mục tiêu

Build được 3 Docker image, `docker compose up` chạy không crash, mỗi service expose `/health` trả 200.

### Bước thực hiện

```bash
# 1. Tạo cấu trúc
mkdir -p deploy/{node,coordinator,ingestor,grafana/{dashboards,provisioning}}
mkdir -p scripts dlq

# 2. Tạo 3 Dockerfile tối thiểu
cat > deploy/node/Dockerfile <<'EOF'
FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends iproute2 curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY deploy/node/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY wm/ ./wm/
COPY deploy/node/ ./deploy/node/
ENV PYTHONPATH=/app
CMD ["uvicorn", "deploy.node.main:app", "--host", "0.0.0.0", "--port", "8000"]
EOF

# Tương tự cho coordinator + ingestor

# 3. requirements.txt
echo "fastapi==0.115.*
uvicorn[standard]==0.32.*
httpx==0.27.*
pydantic==2.9.*" > deploy/node/requirements.txt

# 4. Stub main.py — chỉ có /health
cat > deploy/node/main.py <<'EOF'
from fastapi import FastAPI
import os
app = FastAPI()
NODE_ID = int(os.environ.get("NODE_ID", -1))

@app.get("/health")
def health():
    return {"node_id": NODE_ID, "phase": "STUB"}
EOF

# 5. docker-compose.yml tối thiểu — chưa cần network logic
# (xem template ở 9.5)

# 6. Verify
docker compose -f deploy/docker-compose.yml build
docker compose -f deploy/docker-compose.yml up -d
for port in 8000 8101 8102 8103 8104; do
  curl -sf http://localhost:$port/health || echo "FAIL: $port"
done
docker compose -f deploy/docker-compose.yml down
```

### Gate D1 (must pass)

- [ ] `docker compose build` xong, không error.
- [ ] `docker compose up -d` → 6 container UP (coord + 4 node + ingestor stub).
- [ ] `curl /health` trên cả 5 cổng (8000, 8101–8104) trả 200.
- [ ] `docker compose down -v` cleanup sạch.

---

## 9.5. Sprint D2 — Happy path (Day 2)

### Mục tiêu

Implement đầy đủ §8.13.6 (node, coordinator, ingestor upgraded code). Chạy được 200K event NASA log → `ALL_DONE`.

### File trọng yếu

#### `deploy/node/main.py` — copy nguyên từ §8.13.6, có 3 thay đổi:

- Import `WatermarkEngine` từ `wm.engine`.
- Thêm endpoint `/metrics` cho Prometheus (định dạng text exposition).
- `engine = WatermarkEngine(window_size=..., wait_time=...)` đọc từ env.

#### `deploy/coordinator/main.py` — copy nguyên §8.13.6.

#### `deploy/ingestor/ingest.py` — copy nguyên §8.13.6.

#### `deploy/docker-compose.yml` (đầy đủ)

```yaml
services:
  coordinator:
    build:
      context: ..
      dockerfile: deploy/coordinator/Dockerfile
    environment:
      TOTAL_NODES: 4
    ports: ["8000:8000"]
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 2s
      retries: 10

  node0: &node-base
    build:
      context: ..
      dockerfile: deploy/node/Dockerfile
    environment:
      NODE_ID: 0
      COORDINATOR_URL: http://coordinator:8000
      WINDOW_SIZE: 60
      WAIT_TIME: 30
    volumes:
      - ./dlq/node0:/var/lib/csdlpt/dlq
    ports: ["8101:8000"]
    depends_on:
      coordinator:
        condition: service_healthy
    cap_add: [NET_ADMIN]            # cho phép tc netem trong chaos test

  node1:
    <<: *node-base
    environment: { NODE_ID: 1, COORDINATOR_URL: http://coordinator:8000, WINDOW_SIZE: 60, WAIT_TIME: 30 }
    volumes: ["./dlq/node1:/var/lib/csdlpt/dlq"]
    ports: ["8102:8000"]

  node2:
    <<: *node-base
    environment: { NODE_ID: 2, COORDINATOR_URL: http://coordinator:8000, WINDOW_SIZE: 60, WAIT_TIME: 30 }
    volumes: ["./dlq/node2:/var/lib/csdlpt/dlq"]
    ports: ["8103:8000"]

  node3:
    <<: *node-base
    environment: { NODE_ID: 3, COORDINATOR_URL: http://coordinator:8000, WINDOW_SIZE: 60, WAIT_TIME: 30 }
    volumes: ["./dlq/node3:/var/lib/csdlpt/dlq"]
    ports: ["8104:8000"]

  ingestor:
    build:
      context: ..
      dockerfile: deploy/ingestor/Dockerfile
    environment:
      NODE_HOSTS: "node0:8000,node1:8000,node2:8000,node3:8000"
      COORDINATOR_URL: http://coordinator:8000
    volumes:
      - ../dataset:/data:ro
    depends_on: [node0, node1, node2, node3]
    profiles: ["manual"]            # chỉ chạy khi gọi explicit

  prometheus:
    image: prom/prometheus:latest
    volumes: ["./prometheus.yml:/etc/prometheus/prometheus.yml:ro"]
    ports: ["9090:9090"]

  grafana:
    image: grafana/grafana:latest
    ports: ["3000:3000"]
    environment:
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: Admin
    volumes:
      - ./grafana/provisioning:/etc/grafana/provisioning:ro
      - ./grafana/dashboards:/var/lib/grafana/dashboards:ro
```

### `scripts/test_01_happy.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"

cleanup() { docker compose -f deploy/docker-compose.yml down -v; }
trap cleanup EXIT

docker compose -f deploy/docker-compose.yml up -d coordinator node0 node1 node2 node3
wait_for_health "http://localhost:8000/health"
for p in 8101 8102 8103 8104; do wait_for_health "http://localhost:$p/health"; done

docker compose -f deploy/docker-compose.yml run --rm ingestor \
  python ingest.py /data/nasa_sample_200k.csv

result=$(curl -sf "http://localhost:8000/api/wait?timeout=30")
echo "$result" | jq .
assert_jq "$result" '.status == "ALL_DONE"'
assert_jq "$result" '.missing_events == 0'
assert_jq "$result" '.avg_completeness >= 0.95'

echo "PASS: test_01_happy"
```

### Gate D2

- [ ] `bash scripts/test_01_happy.sh` exit 0.
- [ ] Tổng thời gian < 5 phút cho 200K event.
- [ ] `result.events_received == 200000` (đúng integrity).

---

## 9.6. Sprint D3–D5 — Chaos testing (Day 3–5)

### Nguyên tắc viết test

Mỗi test script tuân thủ template:

```bash
#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"

# 1. SETUP — fresh cluster
cleanup() { docker compose -f deploy/docker-compose.yml down -v; }
trap cleanup EXIT
docker compose -f deploy/docker-compose.yml up -d coordinator node0 node1 node2 node3
wait_all_healthy

# 2. ACTION — inject failure
docker compose -f deploy/docker-compose.yml run -d ingestor python ingest.py /data/...
sleep 2
<INJECT FAILURE HERE>   # docker kill / tc qdisc / docker network disconnect

# 3. ASSERT — verify expected behavior
result=$(curl -sf "http://localhost:8000/api/wait?timeout=30")
assert_jq "$result" '<expected condition>'

# 4. CLEANUP (qua trap)
echo "PASS: <test_name>"
```

### Bảng test → contract mapping (cho biết test nào verify contract nào)

| Test | Inject | Assert | Contracts verified |
|---|---|---|---|
| **02 kill_node** | `docker kill csdlpt-node2-1` sau 2s | `status=TIMEOUT`, `diagnosis[2]=DEAD` | C8, C9 |
| **03 revive_node** | Kill node2, sau 5s `up -d node2` | `ALL_DONE` sau revive | C2, C8 |
| **04 slow_node** | `tc qdisc add ... netem delay 100ms` lên node1 | `ALL_DONE`, `flush_duration_ms[1] > 100` | C8 (slow ≠ dead) |
| **05 partition** | `docker network disconnect` node0 ở phase REPORTING | DLQ ghi xong, reconnect → replay → `ALL_DONE` | C6, C7 |
| **06 coordinator_down** | Kill coordinator khi node0 đang retry, restart sau 8s | Node retry tiếp → ACKED | C6 |
| **07 double_report** | `curl POST /completed` 2 lần cùng `node_id` | `len(completed) == N`, không đếm trùng | C4 |
| **08 stale_run** | POST với `run_id` cũ | `ack=false`, `reason=stale_run_id` | C3 |
| **09 data_loss** | Sửa ingestor drop ngẫu nhiên 1 event | `status=DATA_LOSS`, `missing_events=1` | C11 |

### Gate D3, D4, D5

- D3: test 02, 07, 08 pass.
- D4: + test 03, 04, 06 pass.
- D5: + test 05, 09 pass. **Tổng 9/9 + test 01 happy → 10/10**.
- `scripts/run_all.sh` chạy tuần tự, exit 0 chỉ khi 10/10 pass.

---

## 9.7. Sprint D6 — Observability (Day 6)

### Prometheus scrape

`deploy/prometheus.yml`:
```yaml
global:
  scrape_interval: 2s
scrape_configs:
  - job_name: nodes
    static_configs:
      - targets: [node0:8000, node1:8000, node2:8000, node3:8000]
    metrics_path: /metrics
  - job_name: coordinator
    static_configs:
      - targets: [coordinator:8000]
    metrics_path: /metrics
```

### Metrics endpoint trong node

```python
@app.get("/metrics")
def metrics():
    return Response(
        content=(
            f'wm_watermark{{node="{NODE_ID}"}} {engine.watermark}\n'
            f'wm_events_total{{node="{NODE_ID}"}} {engine.metrics["total"]}\n'
            f'wm_completeness{{node="{NODE_ID}"}} {engine.get_completeness()}\n'
            f'wm_dropped_late{{node="{NODE_ID}"}} {engine.metrics.get("dropped_late",0)}\n'
            f'wm_phase{{node="{NODE_ID}",phase="{state["phase"]}"}} 1\n'
        ),
        media_type="text/plain",
    )
```

### Grafana 4 panel bắt buộc

| Panel | Loại | Query |
|---|---|---|
| **Watermark per node** | Time series | `wm_watermark{job="nodes"}` |
| **Throughput** | Time series (rate) | `rate(wm_events_total[10s])` |
| **Completeness gauge** | Gauge | `wm_completeness{job="nodes"}` |
| **Coordinator status** | Stat | `coord_received / coord_expected` |

### Gate D6

- [ ] `curl http://localhost:9090/api/v1/targets` show 5 target UP.
- [ ] Mở Grafana, dashboard hiển thị data trong vòng 10s sau khi ingest.
- [ ] Khi chạy `test_02_kill_node`, panel Throughput của node2 tụt xuống 0 trong realtime.

---

## 9.8. Sprint D7 — Demo + Doc (Day 7)

### Makefile entry point

```makefile
.PHONY: build up down test demo clean

build:        ## Build all images
	docker compose -f deploy/docker-compose.yml build

up:           ## Start cluster
	docker compose -f deploy/docker-compose.yml up -d coordinator node0 node1 node2 node3 prometheus grafana

down:         ## Stop + remove volumes
	docker compose -f deploy/docker-compose.yml down -v
	rm -rf dlq/

test:         ## Run all 10 acceptance tests
	bash scripts/run_all.sh

demo:         ## 10-minute live demo (§8.8)
	bash scripts/demo_live.sh

clean:        ## Full reset
	docker compose -f deploy/docker-compose.yml down -v --rmi local
	rm -rf dlq/ deploy/grafana/data/

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}; {printf "%-12s %s\n", $$1, $$2}'
```

### Demo script `scripts/demo_live.sh` (10 phút theo §8.8)

Đã có template đầy đủ ở §8.8. Chỉ cần wrap thành script và thêm `pause()` giữa các bước cho giảng viên kịp nhìn.

### Cập nhật tài liệu cuối sprint

| File | Cập nhật |
|---|---|
| `README.md` | Thêm section "Quick start: `make demo`" |
| `pending/REPORT.md` | Thay số liệu ước lượng §5 bằng số đo thật từ test 01–09 |
| `docs/INDEX.md` | Thêm dòng cho `deploy/`, `scripts/`, `Makefile` |
| `docs/OBJECTIVES.md` §4 | Đánh dấu "đã có" cho row "R4 — Node death" (hiện đang "code cần build") |

### Gate D7 (final acceptance)

- [ ] `make clean && make build && make up && make test` exit 0 trong < 15 phút trên máy sạch.
- [ ] Video demo 10 phút quay được (Grafana + curl + docker kill on screen).
- [ ] `REPORT.md` có ≥ 1 bảng "Latency thật (đo từ test_01)" thay cho số ước lượng.
- [ ] Đối chiếu rubric §4 trong `OBJECTIVES.md`: R1, R2, R3, R4 đều "Excellent" với evidence link tới test cụ thể.

---

## 9.9. Definition of Done — Cờ trắng khi nào?

Đồ án rebuild được coi là **xong** khi đồng thời:

1. **10/10 acceptance test pass** (`make test` exit 0).
2. **9/9 contract §8.13.3 verified** (mỗi contract có ≥ 1 test cover).
3. **4 Grafana panel có data realtime**, screenshot trong báo cáo.
4. **`make demo` chạy được** từ zero → ALL_DONE → cleanup trong 10 phút.
5. **`REPORT.md` cập nhật số đo thật**, không còn số ước lượng.
6. **Rebuild từ zero**: xóa hết containers + images + dlq, chạy `make all` → pass test trong 15 phút.
7. **Video demo quay xong**, upload, link trong README.

Khi 7/7 đạt → rebuild kết thúc. Đối chiếu lại `OBJECTIVES.md` §6 → đủ điều kiện Excellent.

---

## 9.10. Anti-pattern — Đừng làm những việc này

| ❌ | Lý do |
|---|---|
| Sửa `wm/engine.py` "tiện thể" trong khi rebuild deployment | Engine đã pass test, đổi = phá vỡ R1–R4 |
| Viết test pass trước, code sau (TDD) cho chaos test | Chaos test cần infra chạy thật mới verify được — viết script song song code, không trước |
| Bỏ qua gate D1–D7 để "code nhanh hơn" | Skip gate = nợ kỹ thuật, ngày D7 sẽ vỡ trận |
| Thêm Kafka/Redis/etcd "cho có" | §8.11 đã nêu rõ — overkill, không ăn điểm |
| Viết Dockerfile dài 50 dòng với multi-stage | Demo đồ án không cần, slim base image đủ |
| Quên mount DLQ volume | Test 05 partition sẽ fail vì DLQ mất khi container restart |
| Hardcode `localhost` thay vì service name Docker | Container không gọi được nhau, debug rất khó |
| Không pin version Python/FastAPI | Build ngày khác = behavior khác = test flaky |

---

## 9.11. Rollback strategy — Nếu rebuild thất bại

Mỗi sprint commit lên 1 branch riêng `rebuild/d1-skeleton`, `rebuild/d2-happy`, …, `rebuild/d7-demo`. Nếu sprint Dx fail:

1. **D1–D2 fail**: lùi về branch `main`, dùng `wm/` + `app.py` cũ làm fallback cho deliverable. Đã có sẵn `tradeoff.png` + `REPORT.md` từ single-process — đủ pass nhưng không ăn điểm distributed.
2. **D3–D5 fail (chaos)**: giữ D2 happy path, ghi rõ trong REPORT.md "distributed happy path đã có, chaos test còn 3/9 chưa pass" — vẫn ăn điểm phần lớn rubric.
3. **D6–D7 fail**: bỏ Grafana, chỉ giữ Prometheus + screenshot raw text endpoint `/metrics`. Vẫn đủ minh chứng observability.

Mục đích: **luôn có deliverable submittable** ở mỗi mốc, không bao giờ rơi vào trạng thái "đang rebuild, chưa nộp được gì".
