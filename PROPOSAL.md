# Distributed Database Project Proposal

**Due Date:** [điền hạn nộp — Tuần 3]
**Project ID & Category:** #112 — Distributed Watermark Tracker
("Log Delay Compensator") · Danh mục: **Horizontal Fragmentation +
Distributed State Management & Fault Tolerance**
[điền mã Category chính thức theo đề bài]

---

## 1. Project Identity

| | |
|---|---|
| **Team Name** | Nhóm 112 [có thể đổi sang tên gọi riêng] |
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
   `node_id = hash(host) % N`. Mỗi node là một engine watermark độc lập,
   có state và checkpoint riêng.

3. **Atomic Checkpoint + Dead-Letter Queue (DLQ)** — cơ chế chịu lỗi:
   state mỗi node được snapshot atomic (`ghi .tmp → os.replace`); khi
   một node chết, các event định tuyến tới nó được ghi nối tiếp vào một
   DLQ bền vững trên đĩa để replay khi node hồi sinh.

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
- Có nguồn **Synthetic** dự phòng (tự sinh 5 000 event out-of-order +
  duplicate) khi không có file dataset.

### Schema — các thuộc tính sử dụng

| Thuộc tính | Kiểu | Vai trò |
|---|---|---|
| `event_id` | string | Khoá định danh duy nhất — dùng cho **deduplication** (Exactly-Once) |
| `event_time` | float (epoch giây) | **Event-time** — thời điểm log thực sự xảy ra; dùng gán cửa sổ |
| `arrival_time` | float (epoch giây) | **Processing-time** — thời điểm log tới engine (sinh thêm để mô phỏng out-of-order) |
| `host` | string | Khoá phân mảnh — `hash(host) % N` quyết định node xử lý |
| `status` | int | Mã HTTP (200/404/500…) — dữ liệu nghiệp vụ tổng hợp trong cửa sổ |

### Fragmentation Strategy

**Horizontal fragmentation theo `host`:**
`node_id = int.from_bytes(md5(host)[:4]) % N`. Mọi event của cùng một
host luôn về cùng một node → đảm bảo tính cục bộ của state. Phân phối
tải giữa các node được đo bằng chỉ số **hot-key skew** (chênh lệch
events giữa node tải cao nhất và thấp nhất).

---

## 4. System Architecture

### Nodes

**4 site (cấu hình được 2–8)**, mỗi site là một `WatermarkEngine` độc
lập với: bộ state cửa sổ riêng, file checkpoint riêng, file DLQ riêng.
Một thành phần **Coordinator** đứng trước, phân mảnh và định tuyến event;
sau stream gộp (merge) metric toàn cluster.

### Communication Layer

Hệ được mô phỏng **in-process** (một tiến trình Python mô phỏng N site —
phù hợp phạm vi môn học, loại bỏ nhiễu từ mạng để cô lập biến đo).
Hai kênh "giao tiếp" giữa các thành phần:

- **Định tuyến trực tiếp (shared memory):** Coordinator gọi
  `engine.process(event)` của node còn sống — tương đương lời gọi RPC.
- **Dead-Letter Queue trên đĩa (durable message channel):** khi node
  *chết*, Coordinator ghi event vào file `.jsonl` append-only của node
  đó. Đây là kênh bất đồng bộ, bền vững — node đọc lại khi hồi sinh.

> Kiến trúc tách bạch logic (`wm/`) khỏi giao tiếp, nên có thể nâng cấp
> kênh giao tiếp lên HTTP/REST đa tiến trình mà không sửa engine.

### Storage — dữ liệu lưu vật lý ở đâu

| Thành phần | Lưu trữ vật lý |
|---|---|
| Dataset đầu vào | Local CSV — `dataset/data.csv` |
| State checkpoint mỗi node | JSON atomic — `.simdata/checkpoints/kill_node_X.json` |
| Dead-Letter Queue mỗi node | JSONL append-only — `.simdata/dlq/dlq_node_X.jsonl` |
| Kết quả phân tích | `tradeoff.csv` + `tradeoff.png` |

---

## 5. Tech Stack & Implementation Plan

### Programming Language

**Python 3** — engine lõi viết bằng thư viện chuẩn (`json`, `os`,
`time`, `hashlib`, `dataclasses`), không phụ thuộc framework nặng.

### Deployment

**Localhost — một tiến trình** mô phỏng N node, trình bày qua
**Dashboard Streamlit** (`streamlit run app.py`). Ngoài ra có 2 script
CLI (`analysis.py`, `distributed_sweep.py`) cho chạy không giao diện.
Không dùng Docker/Kubernetes — quy mô môn học ưu tiên đơn giản, tái lập
được.

### Libraries / Frameworks

| Thư viện | Mục đích |
|---|---|
| `pandas`, `numpy` | Nạp & xử lý dataset, gom số liệu sweep |
| `streamlit` | Dashboard 6 tab — interface chính để demo |
| `plotly` | Biểu đồ tương tác (tradeoff curve, sơ đồ cluster, diễn biến realtime) |
| `matplotlib` | Biểu đồ tĩnh xuất ra `tradeoff.png` |

### Implementation Plan (mốc chính)

1. Engine watermark + windowing theo event-time (`wm/engine.py`).
2. Checkpoint atomic + restore + deduplication.
3. Sweep Wait Time → xuất `tradeoff.csv/png` (`wm/sweep.py`).
4. Horizontal partitioning N-node + merge metric (`wm/partition.py`).
5. Dashboard Streamlit 6 tab + mô phỏng kill/revive node (`app.py`).

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

### The "Failure" Scenario — kịch bản lỗi chứng minh hệ hoạt động

**Kill một node giữa stream rồi hồi sinh, kiểm chứng Exactly-Once.**

1. Cho cluster chạy; tại một cursor định trước, **kill Node B** — engine
   của nó ghi checkpoint atomic cuối cùng rồi bị đánh dấu *dead*.
2. Trong lúc Node B chết, mọi event `hash(host) % N == B` được Coordinator
   **ghi nối tiếp vào DLQ trên đĩa** (`dlq_node_B.jsonl`) thay vì xử lý.
3. Sau một khoảng, **revive Node B**: engine **restore** từ checkpoint
   atomic, sau đó **replay** từng dòng trong DLQ rồi xoá file.
4. **Tiêu chí thành công:** tổng `unique` và `on_time` cuối cùng **khớp
   chính xác** baseline khi không có sự cố — không mất event (nhờ DLQ +
   checkpoint), không đếm trùng (nhờ deduplication theo `event_id`) ⇒
   **Exactly-Once đạt được dù node chết giữa chừng**.

Kịch bản này chạy trực quan realtime ở tab **Kill Node Live** của
Dashboard: có thể click trực tiếp lên node trong sơ đồ để kill/revive,
hoặc lập lịch sự cố tự động và quan sát DLQ phình ra rồi xẹp về 0 sau
khi revive.
