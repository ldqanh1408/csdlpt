# Design Document — Distributed Watermark Tracker ("Log Delay Compensator")

**Project 112 · Môn Cơ sở dữ liệu phân tán** · Phiên bản 3 (container microservice)
Tham chiếu: Özsu & Valduriez, *Principles of Distributed Database Systems*,
4th ed. Mã `[Ö&V, Ch.X]` là placeholder — điền số chương theo PDF của bạn.

---

## 1. Bài toán & mục tiêu

Hệ nhận luồng log web server từ nhiều nguồn. Do trễ truyền và lệch đồng
hồ giữa các site, log tới **sai thứ tự** (event-time skew) — biểu hiện của
*communication failure / message delay* `[Ö&V, Distributed Reliability]`.
Hệ phải: (a) thống kê theo cửa sổ event-time đúng, (b) tự quyết định chờ
log trễ bao lâu trước khi chốt, (c) sống sót sự cố node không mất/không
nhân đôi state, (d) định lượng đánh đổi *độ đầy đủ ↔ độ trễ*.

## 2. Tổng quan kiến trúc

### 2.1 Mô hình triển khai — Docker Compose Microservice

Hệ triển khai dưới dạng **7 Docker container** trên một bridge network:

```
┌──────────────────────────────────────────────────────────────┐
│                    Docker Bridge Network                      │
│                                                               │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐  │
│  │  Node 0  │   │  Node 1  │   │  Node 2  │   │  Node 3  │  │
│  │ :8101    │   │ :8102    │   │ :8103    │   │ :8104    │  │
│  │ Engine   │   │ Engine   │   │ Engine   │   │ Engine   │  │
│  │ ckpt+N   │   │ ckpt+N   │   │ ckpt+N   │   │ ckpt+N   │  │
│  └────┬─────┘   └────┬─────┘   └────┬─────┘   └────┬─────┘  │
│       │              │              │              │          │
│       │    heartbeat (5s, POST /api/heartbeat)     │          │
│       └──────────────┼──────────────┼──────────────┘          │
│                      ▼              ▼                         │
│               ┌──────────────────────────┐                    │
│               │      Coordinator :8000    │                    │
│               │   Stateless HTTP barrier  │                    │
│               │   + diagnosis + integrity │                    │
│               └──────────────────────────┘                    │
│                      ▲              ▲                         │
│               ┌──────┘              └──────┐                  │
│  ┌────────────┴───────┐          ┌────────┴───────────┐      │
│  │ Ingestor (manual)  │          │ Prometheus :9090    │      │
│  │ scatter → N nodes  │          │ Grafana    :3000    │      │
│  │ EOS fence → all    │          └────────────────────┘      │
│  └────────────────────┘                                      │
└──────────────────────────────────────────────────────────────┘
```

- **Coordinator** (`deploy/coordinator/main.py`): FastAPI, stateless HTTP barrier.
  Nhận heartbeat từ node (C8), nhận EOS report (C1–C6), barrier `ALL_DONE` hoặc
  `TIMEOUT` + diagnosis (C9), integrity check `scatter_total` (C11).
- **Node ×4** (`deploy/node/main.py`): FastAPI, mỗi node bọc một `WatermarkEngine`.
  Heartbeat loop 5s riêng (C8), flush-before-report (C1), EOS report background
  fire-and-forget (C5), bounded retry 5 lần exponential backoff (C6), DLQ
  append-only trên bind-mounted volume (C7).
- **Ingestor** (`deploy/ingestor/ingest.py`): Scatter `hash(event_id) % N` qua
  HTTP POST tới node, gửi EOS marker in-band (C2), announce `scatter_total` cho
  integrity check (C11).
- **Prometheus + Grafana**: Observability stack, scrape `/metrics` endpoint từ
  coordinator và tất cả node.

### 2.2 Phân mảnh (fragmentation)

Horizontal fragmentation theo `event_id`: `node_id = hash(event_id) % N`.
Khác với v1 (phân mảnh theo `host`), cách này cho phân phối đều hơn và
không cần state locality theo host — mỗi node là một engine watermark độc lập,
gộp kết quả ở tầng coordinator.

### 2.3 Luồng dữ liệu

```
CSV / Synthetic → Ingestor → scatter POST /ingest → Node ×4
                  ↓ EOS marker (in-band, FIFO)       ↓ flush + report
                  ↓ run_started (scatter_total)       ↓ POST /api/completed
                  └────────────→ Coordinator ←────────┘
                                    ↓
                              GET /api/wait → ALL_DONE | TIMEOUT | DATA_LOSS
```

> **Code layout.** Logic lõi nằm trong package `wm/` (`engine`, `sweep`, `demos`,
> `partition`, `data/{synthetic,nasa}`). Container services trong `deploy/`.
> Test scripts trong `scripts/`. `Makefile` làm CLI thống nhất.

---

## 3. Giao thức EOS — 11 Production Contracts

### C1 — Flush-before-report
Node phải flush tất cả window đang mở trước khi gửi EOS report. Đảm bảo
mọi event đã nhận đều được tính vào `events_processed`.

### C2 — RUN_ID fence (in-band)
RUN_ID được truyền **qua cùng kênh dữ liệu** với event (in-band EOS marker).
Node nhận EOS marker, set `run_id`, flush, rồi báo cáo. Đảm bảo fence: mọi
event trước EOS thuộc run này, mọi event sau EOS bị từ chối nếu khác run_id.

### C3 — RUN_ID fence (out-of-band)
Coordinator kiểm tra `run_id` trong `POST /api/completed`. Nếu khác run hiện
tại → trả `ack: false, reason: stale_run_id`.

### C4 — Set-semantics idempotent
Coordinator ghi report vào `completed[node_id]` — ghi đè (idempotent). Gửi
trùng report (double report) không gây sai lệch.

### C5 — Wait-for-ACK
Node gửi report → đợi `ack: true` trong response body, không chỉ check
HTTP 200. Tránh false-positive khi coordinator nhận request nhưng từ chối.

### C6 — Bounded retry
Node retry tối đa 5 lần với exponential backoff `min(2^k, 10s)`. Sau 5 lần
thất bại → ghi DLQ, đánh dấu DEGRADED.

### C7 — DLQ persist
DLQ là JSONL append-only trên bind-mounted volume (`dlq/node{N}/`). Node
replay DLQ khi khởi động (lifespan), retry từng pending report. Không bao
giờ crash node vì lỗi ghi đĩa.

### C8 — Heartbeat separate channel
Heartbeat là loop độc lập (5s), không phụ thuộc EOS flow. Coordinator phân
biệt DEAD (gap > 15s) vs SLOW (gap ≤ 15s) trong diagnosis.

### C9 — Hard timeout + diagnosis
`GET /api/wait?timeout=N` — nếu không đủ N node trong timeout → trả
`TIMEOUT` kèm `missing[]` và `diagnosis{nid: "DEAD"|"SLOW"|"NEVER_SEEN"}`.

### C10 — Late-event load shedding
Event tới sau khi cửa sổ của nó đã đóng bị drop, đếm `late_dropped`.

### C11 — Integrity check (scatter_total)
Ingestor announce `scatter_total` qua `/api/run_started`. Coordinator so
`sent == received` → `ALL_DONE` hoặc `DATA_LOSS`.

---

## 4. Bốn tiêu chí rubric — cách đáp ứng

**Windowing Logic.** Gán tumbling window theo *event-time*
(`window_start_for(event_time)`), không theo processing-time. Cửa sổ đóng
khi watermark `WM = max_event_time − allowed_lateness ≥ window.end` (low-
water mark, tinh thần commit-wait Spanner). Log của cửa sổ đã đóng → bỏ
(load shedding, C10) `[Ö&V, Data Stream Management]`.

**State Management.** State theo window; checkpoint atomic định kỳ;
`restore()` + `crash_recovery_demo()` chứng minh kill→recover, state khớp,
không đếm trùng (dedup ⇒ exactly-once ⇒ Atomicity) `[Ö&V, Reliability]`.
Trong container deployment: DLQ JSONL append-only + replay khi lifespan
khởi động (C7), heartbeat phân biệt DEAD vs SLOW (C8).

**Latency Analysis.** Phân biệt **2 loại latency**: (a) *processing
latency* mỗi event (đo bằng `time.perf_counter_ns()` — timer độ phân giải
nano giây, đơn điệu), báo cáo **p50/p99**, ≈3 µs; (b) *result latency* —
từ lúc cửa sổ kết thúc (event-time) đến lúc chốt, từ ~105 ms đến ~7.8 s
theo Wait Time. Kết luận bottleneck: độ trễ end-to-end **không** đến từ
tính toán (3 µs) mà từ **Wait Time do watermark cố ý áp đặt** ⇒ nối thẳng
sang PACELC.

**Robustness — container fault injection.** Thay vì mô phỏng in-process, hệ
dùng `docker kill`, `docker compose up -d` (revive), `tc netem delay` (slow
node), và `docker network disconnect` (partition) để kiểm tra khả năng chịu
lỗi thực tế. Backpressure queue giới hạn + chính sách drop/block. Dedup
(gửi lặp) + bỏ qua dòng hỏng ⇒ không sập.

---

## 5. Acceptance Test Matrix (10 tests)

| # | Test | Scenario | Contracts verified | Expected |
|---|------|----------|-------------------|----------|
| 01 | smoke | Cluster up, /health gate | — | all healthy |
| 02 | happy | 10K events, normal flow | C1, C2, C5, C10 | ALL_DONE |
| 03 | kill_node | Kill node2 mid-stream | C8, C9 | TIMEOUT + DEAD |
| 04 | revive_node | Kill node2, revive, re-scatter | C2, C7, C8 | ALL_DONE |
| 05 | slow_node | 100ms netem delay on node1 | C8 (SLOW ≠ DEAD) | ALL_DONE |
| 06 | partition | Disconnect node1 from network | C8, C9 | TIMEOUT + DEAD |
| 07 | coordinator_down | Kill coordinator mid-stream | C3, C6, C7 | ALL_DONE after revive |
| 08 | double_report | Node sends EOS report twice | C4 | ALL_DONE (idempotent) |
| 09 | stale_run | Old run_id rejected | C3 | stale_run_id rejected |
| 10 | data_loss | Drop 5% events before scatter | C11 | DATA_LOSS detected |

Tất cả 10 test đều pass trên container deployment thật. Chạy: `make test-all`.

---

## 6. Quyết định thiết kế & lý do (theory-justified)

| Quyết định | Lý do | Ö&V |
|---|---|---|
| Window theo event-time | Gom theo lúc log tới sai ngữ nghĩa thời gian | Data Stream Mgmt |
| Watermark = max−lateness | Điểm chốt điều chỉnh được; xử lý out-of-order | Reliability; PACELC |
| Phân vùng hash(event_id)%N | Phân phối đều, mỗi node watermark độc lập | Distributed DB Design |
| Checkpoint atomic per-node | Recovery sau site failure; Durability | Distributed Reliability |
| Dedup → exactly-once | Replay sau crash không đếm trùng; Atomicity | Reliability / Recovery |
| EOS in-band (FIFO) | Fence chính xác: mọi event trước EOS thuộc run hiện tại | Distributed Reliability |
| Heartbeat kênh riêng | Phân biệt DEAD vs SLOW, không phụ thuộc EOS flow | Fault Detection |
| DLQ append-only + replay | Không mất dữ liệu khi coordinator down (C7) | Durability |
| Docker container thật | Fault injection thực tế (`docker kill`, `tc netem`) | Experimental CS |

## 7. Mô hình lỗi được xử lý

Site failure (kill→recover từ checkpoint + DLQ replay) · communication failure
(trễ/sai thứ tự → watermark) · partition (network disconnect → TIMEOUT +
diagnosis) · duplicate (dedup → idempotent) · malformed input (đếm+bỏ qua) ·
backpressure (queue giới hạn + chính sách drop/block) · coordinator failure
(DLQ append-only + node retry khi coordinator hồi sinh).

## 8. Đánh đổi trung tâm — CAP / PACELC

`allowed_lateness` là núm trượt: **Strict** (lateness lớn) ưu tiên
Consistency trả giá Latency; **Heuristic** (nhỏ) ưu tiên Latency, chấp
nhận dữ liệu thiếu (eventual). Đúng vế **ELC của PACELC**: không phân
hoạch mạng vẫn phải chọn L vs C `[Ö&V, Replication / CAP-PACELC]`.
Deliverable *Data Completeness % vs Wait Time* = bằng chứng thực nghiệm
định lượng cho đánh đổi lý thuyết.

## 9. Observability

Prometheus scrape `/metrics` từ coordinator và 4 node mỗi 15s. Metrics:
`wm_watermark`, `wm_events_total`, `wm_events_unique`, `wm_completeness`,
`wm_dropped_late`, `wm_duplicates`, `wm_backpressure` (per-node);
`coord_received`, `coord_expected`, `coord_done` (coordinator). Grafana
dashboard hiển thị real-time watermark progression, completeness gauge,
và event throughput.

## 10. Giới hạn & hướng mở rộng

Hot-key skew khi phân vùng theo `host` → dùng `event_id` thay thế (đã làm).
`seen_ids` tăng tuyến tính → Bloom filter + TTL theo watermark. Recovery
ở mức một node độc lập → coordinated snapshot Chandy–Lamport khi các node
trao đổi message trực tiếp. Thêm Kafka/RabbitMQ làm message broker thay vì
HTTP scatter trực tiếp → backpressure tự nhiên + replay từ offset.
