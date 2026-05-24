# Distributed Database Project Proposal

**Due Date:** Tuần 3 — Học kỳ 2, 2025–2026
**Project ID & Category:** #112 — Distributed Watermark Tracker
("Log Delay Compensator") · Danh mục: **Horizontal Fragmentation +
Distributed State Management & Fault Tolerance**
[Mã Category: CSDLPT-112]

---

## 1. Project Identity

| | |
|---|---|
| **Team Name** | Nhóm 112 |
| **Team Members** | Lê Đắc Quốc Anh · [điền tên thành viên còn lại] |
| **Project Title** | Distributed Watermark Tracker — quản lý event-time watermark và khắc phục sự cố node trong xử lý stream log phân tán |

---

## 2. Objective & Problem Statement

### The "Why" — Vấn đề giải quyết

Trong một hệ phân tán xử lý **stream log**, các bản ghi gần như không bao
giờ tới đúng thứ tự thời gian (out-of-order): một log xảy ra lúc 10:00:03
có thể tới hệ thống *sau* một log xảy ra lúc 10:00:07 do trễ mạng,
buffer, hoặc retransmission.

Chúng tôi giải quyết bài toán: **khi nào được phép "chốt" kết quả của một
cửa sổ thời gian?** Chốt quá sớm → mất các log tới trễ (giảm độ chính
xác). Chốt quá muộn → kết quả có độ trễ cao (giảm tính realtime). Đây
chính là đánh đổi **Consistency vs Latency** trong định lý **PACELC**.

Cụ thể, đề tài **đo lường định lượng tham số `Wait Time`** (thời gian cho
phép trễ — *allowed lateness*) ảnh hưởng thế nào tới **Data Completeness %**
và **Result Latency**, đồng thời chứng minh hệ vẫn giữ **Exactly-Once**
khi một node bị chết giữa chừng.

### Core Logic — Thuật toán / giao thức chính

1. **Event-time Watermarking** — cốt lõi. Cửa sổ (tumbling window 10 s)
   được gán theo **event-time**, nhưng stream lại được nạp theo
   **arrival-time**. Engine duy trì một *watermark*:

   ```
   watermark = max_event_time_đã_thấy − allowed_lateness (Wait Time)
   ```

   Khi `watermark ≥ window_end`, cửa sổ được đóng và chốt kết quả. Log
   tới sau khi cửa sổ của nó đã đóng bị coi là *late* và bị loại.

2. **Horizontal Fragmentation** — phân mảnh ngang stream qua N node bằng
   `node_id = hash(event_id) % N`. Mỗi node là một engine watermark độc lập,
   có state và checkpoint riêng.

3. **Atomic Checkpoint + Dead-Letter Queue (DLQ)** — cơ chế chịu lỗi:
   state mỗi node được snapshot atomic (`ghi .tmp → os.replace`); khi
   một node hoặc coordinator chết, các EOS report pending được ghi nối tiếp
   vào DLQ bền vững trên bind-mounted volume để replay khi hệ thống hồi sinh.

4. **EOS Protocol (11 Production Contracts C1–C11)** — giao thức đồng bộ hóa
   cuối luồng với in-band EOS marker, heartbeat kênh riêng, diagnosis
   DEAD/SLOW/NEVER_SEEN, và integrity check `scatter_total`.

---

## 3. Dataset Specification

### Source

**NASA-HTTP Web Server Logs** — Internet Traffic Archive (Jul/Aug 1995),
log truy cập máy chủ web thật của NASA Kennedy Space Center.
Link gốc: <https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html>
(repo dùng bản đã giải nén tại `dataset/data.csv`).

### Size

- Tổng: **~2.96 triệu dòng** (`dataset/data.csv`).
- Mẫu phân tích chuẩn: **200 000 dòng đầu** (đủ lớn để thấy rõ phân phối
  trễ bimodal mà vẫn chạy nhanh ~2 phút).
- Có nguồn **Synthetic** dự phòng (tự sinh 5 000 – 20 000 event out-of-order +
  duplicate) khi không có file dataset.

### Schema — các thuộc tính sử dụng

| Thuộc tính | Kiểu | Vai trò |
|---|---|---|
| `event_id` | string | Khoá định danh duy nhất — dùng cho **deduplication** (Exactly-Once) |
| `event_time` | float (epoch giây) | **Event-time** — thời điểm log thực sự xảy ra; dùng gán cửa sổ |
| `arrival_time` | float (epoch giây) | **Processing-time** — thời điểm log tới engine (sinh thêm để mô phỏng out-of-order) |
| `host` | string | Khoá phân mảnh (v1) — `hash(host) % N` |
| `status` | int | Mã HTTP (200/404/500…) — dữ liệu nghiệp vụ tổng hợp trong cửa sổ |

### Fragmentation Strategy

**Horizontal fragmentation theo `event_id`:**
`node_id = hash(event_id) % N`. Cách này cho phân phối đều hơn so với
phân mảnh theo `host` (v1), tránh hot-key skew từ các host có tần suất
request vượt trội. Mỗi node là một engine watermark độc lập — không cần
shuffle hay trao đổi dữ liệu chéo.

---

## 4. System Architecture

### 4.1 Mô hình triển khai — Docker Compose Microservice

Hệ thống được triển khai dưới dạng **7 Docker container** giao tiếp qua
HTTP/REST trên một bridge network:

| Container | Image | Port | Vai trò |
|---|---|---|---|
| coordinator | `csdlpt/coordinator:d1` | :8000 | HTTP barrier, nhận heartbeat + EOS report, diagnosis, integrity check |
| node0–3 | `csdlpt/node:d1` | :8101–:8104 | WatermarkEngine + FastAPI, checkpoint atomic, DLQ append-only |
| ingestor | `csdlpt/ingestor:d1` | manual | Scatter event + EOS marker in-band |
| prometheus | `prom/prometheus` | :9090 | Scrape /metrics mỗi 15s |
| grafana | `grafana/grafana` | :3000 | Dashboard real-time |

### 4.2 Communication Layer

Khác với v1 (mô phỏng in-process), v3 triển khai giao tiếp thực qua HTTP:

- **Scatter (HTTP POST):** Ingestor gửi từng event tới node qua
  `POST http://node{N}:8000/ingest`. Phân mảnh: `node_id = hash(event_id) % 4`.
- **EOS in-band (FIFO):** EOS marker được gửi qua cùng kênh `/ingest` với
  event — đảm bảo mọi event trước EOS đã được xử lý trước khi flush.
- **Heartbeat (HTTP POST, 5s):** Mỗi node gửi `POST /api/heartbeat` tới
  coordinator trên kênh riêng, độc lập với EOS flow.
- **EOS Report (HTTP POST + retry):** Node flush → gửi `POST /api/completed`
  với bounded retry (5 lần, exponential backoff). Thất bại → DLQ.
- **Dead-Letter Queue (bind-mounted volume):** `dlq/node{N}/` — JSONL
  append-only, tồn tại độc lập với vòng đời container. Replay khi node
  khởi động.

### 4.3 Storage — dữ liệu lưu vật lý ở đâu

| Thành phần | Lưu trữ vật lý |
|---|---|
| Dataset đầu vào | Local CSV — `dataset/data.csv` (mount read-only vào ingestor) |
| State checkpoint mỗi node | JSON atomic — `/var/lib/csdlpt/dlq/ckpt-{N}.json` (trong container) |
| Dead-Letter Queue mỗi node | JSONL append-only — `dlq/node{N}/node-{N}.jsonl` (bind-mounted từ host) |
| Kết quả phân tích | `tradeoff.csv` + `tradeoff.png` |

---

## 5. Tech Stack & Implementation Plan

### Programming Language

**Python 3.12** — engine lõi (`wm/`) viết bằng thư viện chuẩn (`json`, `os`,
`time`, `hashlib`, `dataclasses`). Container services dùng **FastAPI** +
**httpx** cho HTTP client/server.

### Deployment

**Docker Compose** — 7 container trên bridge network. Khởi động: `make up`.
Test toàn bộ: `make test-all`. Demo: `make demo`.

Ngoài ra còn có mô hình **in-process** (v1–v2) cho phân tích lý thuyết:
`python analysis.py` cho sweep Wait Time, `streamlit run app.py` cho
dashboard tương tác.

### Libraries / Frameworks

| Thư viện | Mục đích |
|---|---|
| `fastapi`, `uvicorn` | HTTP server cho coordinator + node |
| `httpx` | HTTP client (scatter, heartbeat, EOS report) |
| `pydantic` | Request/response validation |
| `pandas`, `numpy` | Nạp & xử lý dataset, gom số liệu sweep |
| `streamlit` | Dashboard 6 tab — interface chính để demo (v2) |
| `plotly` | Biểu đồ tương tác (tradeoff curve, sơ đồ cluster) |
| `matplotlib` | Biểu đồ tĩnh xuất ra `tradeoff.png` |
| Docker, docker compose | Containerization + orchestration |

### Implementation Plan (mốc chính)

1. Engine watermark + windowing theo event-time (`wm/engine.py`). ✓
2. Checkpoint atomic + restore + deduplication. ✓
3. Sweep Wait Time → xuất `tradeoff.csv/png` (`wm/sweep.py`). ✓
4. Horizontal partitioning N-node + merge metric (`wm/partition.py`). ✓
5. Dashboard Streamlit 6 tab (`app.py`). ✓
6. Docker containerization: coordinator + node + ingestor (`deploy/`). ✓
7. EOS protocol với 11 production contracts C1–C11. ✓
8. 10 acceptance tests với fault injection thực tế (`scripts/`). ✓
9. Observability: Prometheus + Grafana (`deploy/prometheus.yml`). ✓

---

## 6. Success Metrics & Analysis

### Quantitative Metric — đo cái gì

**Chỉ số chính: Data Completeness % = 100 × on_time / unique_events**,
đo theo từng mức **Wait Time (0 → 8000 ms)** để vẽ đường cong đánh đổi.

Chỉ số phụ:

- **Result Latency (ms)** — độ trễ chốt kết quả theo event-time.
- **Processing Latency p50 / p99 (µs)** — đo bằng `time.perf_counter_ns()`
  để xác định bottleneck.
- **Late dropped** — số event mất do tới sau khi cửa sổ đã đóng.
- **Backpressure drops** — số event bị drop có kiểm soát khi queue quá tải.
- **Hot-key skew %** — độ lệch tải giữa các node sau phân mảnh.
- **Integrity check (C11)** — `scatter_total == sum(events_processed)` →
  `ALL_DONE` hoặc `DATA_LOSS`.

### The "Failure" Scenario — kịch bản lỗi chứng minh hệ hoạt động

**10 acceptance tests trên Docker container thật với fault injection:**

1. **Kill một node giữa stream (test_03):** `docker kill node2` → coordinator
   phát hiện DEAD qua heartbeat gap > 15s (C8), `GET /api/wait` trả `TIMEOUT`
   + `diagnosis.2 = "DEAD"` (C9).
2. **Revive node (test_04):** Kill node2 → chờ 5s → `docker compose up -d node2`
   → node replay DLQ (C7), re-scatter → `ALL_DONE`.
3. **Slow node (test_05):** `tc netem delay 100ms` trên node1 → coordinator
   phân biệt SLOW ≠ DEAD (C8) → `ALL_DONE`.
4. **Network partition (test_06):** `docker network disconnect node1` →
   `TIMEOUT` + DEAD.
5. **Coordinator failure (test_07):** `docker kill coordinator` → node retry
   với bounded backoff (C6) + DLQ persist (C7) → revive coordinator →
   `ALL_DONE`.
6. **Double report (test_08):** Node gửi trùng EOS report → coordinator
   idempotent (C4) → `ALL_DONE`.
7. **Stale run_id (test_09):** RUN_ID cũ bị từ chối (C3).
8. **Data loss detection (test_10):** Drop 5% event → `DATA_LOSS` (C11).

**Kết quả: 10/10 PASS.** Hệ thống chứng minh Exactly-Once, fault detection,
và recovery trên container deployment thật — không mô phỏng.
