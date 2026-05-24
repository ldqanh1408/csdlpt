# BÁO CÁO HỆ THỐNG: DISTRIBUTED WATERMARK TRACKER
## Xử Lý Log Bất Đồng Bộ Với Event-Time Watermarks & Khắc Phục Sự Cố Phân Tán

**Nhóm thực hiện:** Nhóm 112
**Môn học:** Cơ sở dữ liệu phân tán (CSDLPT)
**Tham chiếu lý thuyết:** M. Tamer Özsu & Patrick Valduriez, *Principles of Distributed Database Systems*, 4th Edition.

---

## 1. Đặt Vấn Đề & Mục Tiêu

Trong các hệ thống phân tán quy mô lớn, dữ liệu log (truy cập web, giao dịch, telemetry) được sinh ra liên tục tại nhiều server biên (edge servers) khác nhau. Do sự lệch múi giờ, độ trễ mạng bất định (network jitter) và lỗi truyền dẫn, các log này khi được gửi về hệ thống xử lý stream trung tâm thường bị **lệch thứ tự nghiêm trọng (out-of-order)**. 

Nếu hệ thống gom log và thống kê dựa trên thời gian nhận được log (**Processing-time**), kết quả thống kê (ví dụ: số lượng request/phút) sẽ bị sai lệch hoàn toàn so với thực tế xảy ra. Do đó, hệ thống bắt buộc phải xử lý theo thời gian thực tế xảy ra sự kiện (**Event-time**).

### Mục tiêu của dự án:
1. **Windowing chính xác theo Event-Time**: Gom nhóm log vào các cửa sổ thời gian (Tumbling Windows) dựa trên trường `event_time` của log.
2. **Quản lý Watermark thông minh**: Tự động ước lượng độ trễ tối đa của log (`allowed_lateness` hay **Wait Time**) để đưa ra biên thời gian Watermark, làm cơ sở chốt cửa sổ an toàn.
3. **Phân tán & Cân bằng tải**: Phân mảnh ngang luồng log đầu vào theo cơ chế băm `hash(event_id) % N` về các Processing Node độc lập trong Docker container.
4. **Khả năng chịu lỗi cao (Fault Tolerance)**: Thiết kế cơ chế checkpoint atomic đảm bảo tính bền vững của trạng thái in-memory, kết hợp mô hình hàng đợi thư chết (Dead-Letter Queue - DLQ) trên bind-mounted volume để khôi phục Exactly-Once khi node hoặc coordinator bị sập đột ngột.
5. **Định lượng đánh đổi PACELC**: Thực nghiệm quét (sweep) trên dữ liệu thực tế NASA-HTTP (200.000 events) để phân tích đường cong đánh đổi giữa **Độ đầy đủ dữ liệu (Data Completeness)** và **Độ trễ kết quả (Result Latency)**.

---

## 2. Kiến Trúc Hệ Thống & Phân Mảnh Ngang

### 2.1 Mô hình triển khai — Docker Compose Microservice

Hệ thống được triển khai dưới dạng **7 Docker container** giao tiếp qua HTTP trên một bridge network:

```
┌──────────────────────────────────────────────────────────────────┐
│                      Docker Bridge Network                        │
│                                                                   │
│   ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│   │  Node 0  │  │  Node 1  │  │  Node 2  │  │  Node 3  │        │
│   │  :8101   │  │  :8102   │  │  :8103   │  │  :8104   │        │
│   │ FastAPI  │  │ FastAPI  │  │ FastAPI  │  │ FastAPI  │        │
│   │ Engine   │  │ Engine   │  │ Engine   │  │ Engine   │        │
│   │ DLQ vol  │  │ DLQ vol  │  │ DLQ vol  │  │ DLQ vol  │        │
│   └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘        │
│        │             │             │             │                │
│        │   POST /api/heartbeat (5s)              │                │
│        └─────────────┼─────────────┼─────────────┘                │
│                      ▼             ▼                              │
│               ┌──────────────────────────┐                        │
│               │   Coordinator :8000       │                        │
│               │   FastAPI, stateless      │                        │
│               │   barrier + diagnosis     │                        │
│               │   + integrity check       │                        │
│               └────┬──────────┬───────────┘                        │
│                    │          │                                    │
│          ┌─────────┘          └─────────┐                          │
│          ▼                              ▼                          │
│   ┌─────────────┐              ┌─────────────────┐                │
│   │  Ingestor   │              │  Prometheus:9090 │                │
│   │  (manual)   │              │  Grafana:3000    │                │
│   │  scatter    │              │  dashboards      │                │
│   └─────────────┘              └─────────────────┘                │
└──────────────────────────────────────────────────────────────────┘
```

**Thành phần chính:**
- **Coordinator** (`deploy/coordinator/main.py`): HTTP barrier không trạng thái. Nhận heartbeat từ node (mỗi 5s), nhận EOS report, barrier `ALL_DONE` hoặc `TIMEOUT` kèm diagnosis (DEAD/SLOW/NEVER_SEEN), kiểm tra integrity `scatter_total == sum(events_processed)`.
- **Node ×4** (`deploy/node/main.py`): Mỗi node bọc một `WatermarkEngine` với state in-memory, checkpoint atomic, DLQ JSONL append-only trên bind-mounted volume (`dlq/node{N}/`), heartbeat loop 5s độc lập.
- **Ingestor** (`deploy/ingestor/ingest.py`): Scatter event qua HTTP POST tới node theo `hash(event_id) % N`, gửi EOS marker in-band (FIFO), announce `scatter_total` cho integrity check.
- **Prometheus + Grafana**: Observability stack, scrape `/metrics` mỗi 15s, hiển thị real-time watermark, completeness, throughput.

### 2.2 Phân mảnh ngang (Horizontal Fragmentation)

Theo lý thuyết thiết kế CSDL phân tán `[Ö&V, Ch. 3]`, luồng dữ liệu được phân chia dựa trên `event_id`:
- **Quy tắc định tuyến**: `node_id = hash(event_id) % N`.
- **Mục đích**: Phân phối đều tải giữa các node, mỗi node là một engine watermark độc lập — không cần shuffle hay trao đổi dữ liệu chéo giữa các node.
- **So với v1 (hash theo host)**: Phân mảnh theo `host` tạo state locality tốt cho aggregation theo host nhưng gây hot-key skew nghiêm trọng (một vài host có tần suất request vượt trội). Phân mảnh theo `event_id` cho phân phối đều hơn, phù hợp bài toán watermark tracking không yêu cầu aggregation theo host.

---

## 3. Giao Thức EOS — Đồng Bộ Hóa Cuối Luồng

### 3.1 In-band EOS Marker (C2)

Khác với thiết kế truyền thống dùng kênh điều khiển riêng (out-of-band), hệ thống sử dụng **in-band EOS marker** — marker được gửi qua cùng kênh dữ liệu (POST `/ingest`) với các event thông thường. Điều này đảm bảo **FIFO ordering**: mọi event gửi trước EOS marker sẽ được xử lý trước khi node flush và báo cáo.

### 3.2 11 Production Contracts (C1–C11)

| Contract | Mô tả | Cài đặt |
|----------|-------|---------|
| C1 | Flush-before-report: flush tất cả window đang mở trước khi gửi report | `engine.flush()` blocking trước `_report_eos()` |
| C2 | RUN_ID fence in-band: RUN_ID qua kênh dữ liệu với EOS marker | `event.type == "EOS"` → set `state["run_id"]` |
| C3 | RUN_ID fence out-of-band: coordinator kiểm tra run_id | `r.run_id != RUN_ID` → `ack: false` |
| C4 | Set-semantics idempotent: ghi đè report trùng | `completed[node_id] = report` (idempotent) |
| C5 | Wait-for-ACK: kiểm tra `ack: true` trong body | `send_with_ack()` kiểm tra `r.json().get("ack")` |
| C6 | Bounded retry: 5 lần, exponential backoff | `min(2^k, 10s)`, sau 5 lần → DLQ |
| C7 | DLQ persist: JSONL append-only trên volume | `dlq/node{N}/node-{N}.jsonl` |
| C8 | Heartbeat kênh riêng: loop 5s độc lập | `heartbeat_loop()` không phụ thuộc EOS flow |
| C9 | Hard timeout + diagnosis: DEAD/SLOW/NEVER_SEEN | `GET /api/wait?timeout=N` |
| C10 | Late-event load shedding: drop event trễ | engine drop khi `watermark >= window_end` |
| C11 | Integrity check: scatter_total validation | `received == scatter_total` → ALL_DONE / DATA_LOSS |

---

## 4. Quản Lý Trạng Thái & Khả Năng Chịu Lỗi (Fault Tolerance)

Để đáp ứng tiêu chí **State Management** mức xuất sắc trong rubric `[Ö&V, Ch. 12 - Distributed Reliability]`, hệ thống cài đặt ba kỹ thuật then chốt:

### 4.1 Checkpoint Atomic (Nhất quán & Bền vững)

Mỗi node xử lý lưu trữ trạng thái in-memory gồm các cửa sổ đang mở (`open_windows`), các cửa sổ đã đóng (`closed_windows`) và tập hợp các ID sự kiện đã thấy (`seen_ids`). 
- Định kỳ sau mỗi $K$ bản ghi, node tiến hành ghi lại checkpoint xuống đĩa.
- **Ghi Atomic**: Tránh tình trạng file checkpoint bị lỗi hoặc mất dữ liệu khi tiến trình bị sập đúng lúc đang ghi file (partial write). Hệ thống ghi dữ liệu vào một file tạm `.tmp`, sau khi hoàn tất mới gọi hàm đổi tên hệ thống `os.replace` sang file checkpoint chính thức. Đây là thao tác nguyên tử (atomic write) được hỗ trợ bởi hệ điều hành.

### 4.2 Cơ chế Dead-Letter Queue (DLQ) & Khôi phục Exactly-Once

DLQ được lưu trên **Docker bind-mounted volume** (`dlq/node{N}/`), tồn tại độc lập với vòng đời container:

1. **Ghi DLQ (C7)**: Khi node không thể gửi EOS report tới coordinator sau 5 lần retry (C6) — ví dụ coordinator đang down — report được ghi append-only vào file JSONL trên volume. DLQ không bao giờ gây crash node.
2. **Replay khi khởi động**: Khi node start (lifespan), node đọc toàn bộ pending reports từ DLQ, retry gửi tới coordinator. Report nào gửi thành công → xóa khỏi DLQ. Report nào vẫn thất bại → giữ lại.
3. **Exactly-Once Semantics**: Nhờ `seen_ids` (Deduplication Store) trong `WatermarkEngine`, mọi event trùng lặp do replay hoặc gửi lại đều bị lọc bỏ. Kết hợp checkpoint atomic + DLQ replay đảm bảo không mất dữ liệu và không đếm trùng.

### 4.3 Heartbeat & Phát Hiện Lỗi (C8, C9)

- **Heartbeat loop 5s** chạy độc lập trên mỗi node, không phụ thuộc EOS flow.
- Coordinator theo dõi `last_hb[node_id]`. Khi `GET /api/wait` timeout:
  - `gap > 15s` → **DEAD** (node đã chết hoặc mất kết nối)
  - `gap ≤ 15s` → **SLOW** (node còn sống nhưng xử lý chậm)
  - Chưa từng thấy heartbeat → **NEVER_SEEN** (node chưa khởi động)

---

## 5. Kiểm Soát Lưu Lượng & Quá Tải (Backpressure)

Trong xử lý luồng phân tán, sự mất cân bằng giữa tốc độ sản xuất dữ liệu ($\lambda$) và tốc độ tiêu thụ của engine xử lý ($\mu$) sẽ dẫn tới tràn bộ nhớ.
Hệ thống tích hợp hàng đợi có giới hạn (`max_queue`):
- Khi kích thước hàng đợi vượt quá giới hạn, hệ thống kích hoạt cơ chế **Backpressure**.
- **Chính sách tải lỗi**: Hệ thống thực hiện chiến lược **Load Shedding** (chủ động loại bỏ các bản ghi mới nhất và tăng biến đếm `backpressure_drops`) để bảo vệ tài nguyên RAM của hệ thống, tránh lỗi OOM (Out Of Memory) crash. Khi tải giảm, hàng đợi co lại dưới ngưỡng, hệ thống tự động nhận dữ liệu bình thường trở lại.

---

## 6. Kết Quả Thực Nghiệm

### 6.1 Acceptance Test Suite — Container Deployment

Toàn bộ 10 test chạy trên Docker container thật với fault injection:

| # | Test | Kịch bản | Hợp đồng | Kết quả |
|---|------|----------|----------|---------|
| 01 | smoke | Cluster khởi động, /health gate | — | PASS |
| 02 | happy | 10K events, luồng bình thường | C1, C2, C5, C10 | ALL_DONE |
| 03 | kill_node | Kill node2 giữa stream | C8, C9 | TIMEOUT + DEAD |
| 04 | revive_node | Kill node2, revive sau 5s | C2, C7, C8 | ALL_DONE |
| 05 | slow_node | 100ms netem delay trên node1 | C8 (SLOW ≠ DEAD) | ALL_DONE |
| 06 | partition | Ngắt node1 khỏi network | C8, C9 | TIMEOUT + DEAD |
| 07 | coordinator_down | Kill coordinator giữa stream | C3, C6, C7 | ALL_DONE |
| 08 | double_report | Node gửi EOS report 2 lần | C4 | ALL_DONE |
| 09 | stale_run | Old run_id bị từ chối | C3 | stale_run_id rejected |
| 10 | data_loss | Drop 5% event trước scatter | C11 | DATA_LOSS |

**Kết quả: 10/10 PASS.** Chạy toàn bộ: `make test-all`.

### 6.2 Sweep Wait Time — Dữ Liệu NASA-HTTP (200.000 events)

Kết quả sweep thực nghiệm trên 200.000 sự kiện NASA-HTTP với kích thước cửa sổ $W = 10s$:

| Wait Time (ms) | Data Completeness % | Late dropped | Result latency (ms) | Proc p99 (µs) |
|---:|---:|---:|---:|---:|
| 0    | 97.18  | 5.638 | 66.091 | 5.62 |
| 100  | 98.28  | 3.439 | 74.118 | 5.19 |
| 250  | 98.28  | 3.439 | 74.118 | 4.84 |
| 500  | 98.28  | 3.439 | 74.118 | 4.73 |
| 1000 | 98.28  | 3.439 | 74.118 | 4.40 |
| 2000 | 98.97  | 2.055 | 78.266 | 4.29 |
| 4000 | 99.72  |   571 | 84.932 | 4.09 |
| 6000 | 99.96  |    74 | 89.851 | 3.96 |
| 8000 | **100.00** | 0 | 94.298 | 4.18 |

### Phân tích biểu đồ:
1. **Định luật ELC (Eventual Consistency vs. Latency)**: 
   - Tại mức **Wait Time = 0ms** (Heuristic cao nhất), kết quả có ngay lập tức (Result Latency thấp nhất ~66s) nhưng độ đầy đủ dữ liệu chỉ đạt **97.18%** (mất 5.638 events trễ).
   - Tại mức **Wait Time = 8000ms** (Strict nhất), độ đầy đủ đạt **100%** nhưng Result Latency tăng lên **94.29s** (chậm hơn 28s).
2. **Hiện tượng đi ngang (Completeness Plateau)**:
   - Số liệu thực tế cho thấy tỷ lệ đầy đủ đứng yên ở mức **98.28%** trong suốt dải Wait Time từ 100ms đến 1000ms. Điều này phản ánh phân phối trễ của log NASA có tính chất **bimodal**: Log hoặc đến rất nhanh (dưới 200ms) hoặc bị trễ hẳn trên 1000ms (do bão truyền dẫn hoặc retry kết nối). Việc cấu hình Wait Time nằm trong khoảng trống bimodal (ví dụ: 500ms) chỉ làm tăng thêm độ trễ hệ thống vô ích mà không gom thêm được bản ghi nào.
3. **Phân tích điểm nghẽn (Bottlenecks)**:
   - Thời gian xử lý thực tế mỗi bản ghi của engine cực kỳ nhanh (p99 chỉ khoảng **4 - 6 micro-giây**), chứng tỏ năng lực tính toán của CPU không phải là bottleneck.
   - Bottleneck trễ kết quả (Result Latency ~66 - 94s) hoàn toàn đến từ đặc tính dữ liệu log: Cửa sổ event-time phải chờ đợi sự xuất hiện của các bản ghi thuộc cửa sổ tiếp theo để kéo watermark vượt qua mốc đóng cửa sổ.

---

## 7. Observability — Prometheus + Grafana

Hệ thống tích hợp monitoring stack:

- **Prometheus** (`:9090`): Scrape `/metrics` endpoint từ coordinator và 4 node mỗi 15 giây. Metrics bao gồm:
  - `wm_watermark`, `wm_events_total`, `wm_events_unique`, `wm_completeness` (per-node)
  - `wm_dropped_late`, `wm_duplicates`, `wm_backpressure` (per-node)
  - `coord_received`, `coord_expected`, `coord_heartbeats_seen`, `coord_done` (coordinator)
- **Grafana** (`:3000`): Dashboard hiển thị real-time:
  - Watermark progression chart (từng node)
  - Completeness gauge (toàn cluster)
  - Event throughput (events/s)
  - Heartbeat status panel

Khởi động: `make up-obs`.

---

## 8. Kết Luận

Hệ thống đã chứng minh tính đúng đắn của mô hình lý thuyết xử lý stream phân tán qua cả hai phương diện:

**Lý thuyết (in-process sweep trên NASA-HTTP):**
- Đảm bảo tính nhất quán dữ liệu dựa trên event-time độc lập với thời gian xử lý thực tế.
- Cung cấp số liệu thực nghiệm định lượng rõ ràng cho việc cấu hình hệ thống tối ưu theo nguyên lý PACELC.

**Thực hành (container microservice với 10 acceptance tests):**
- Đảm bảo tính bền vững và khả năng phục hồi Exactly-Once sau sự cố sập node/coordinator nhờ sự kết hợp giữa Checkpoint Atomic, Dead-Letter Queue, và EOS protocol với 11 production contracts.
- Fault injection thực tế (`docker kill`, `tc netem`, `docker network disconnect`) cho kết quả đúng như dự đoán lý thuyết.
- Observability stack (Prometheus + Grafana) cung cấp giám sát real-time toàn cluster.
