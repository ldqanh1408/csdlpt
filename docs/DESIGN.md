# Design Document — Distributed Watermark Tracker ("Log Delay Compensator")

**Project 112 · Môn Cơ sở dữ liệu phân tán** · Phiên bản 2 (cập nhật)
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

Luồng một chiều, 7 thành phần: **NASA-HTTP CSV → Ingestion → Partitioner
→ N Processing nodes → Checkpoint store ← Coordinator → Merge → Report /
Live visualization**.

- **Nguồn:** NASA-HTTP CSV (`host, time, method, url, response, bytes`).
  `time`=event-time thật; `response`=status; `host`=khóa phân vùng.
- **Ingestion (`wm/data/nasa.py`):** parse log; sinh `arrival-time =
  event-time + độ trễ mô phỏng` (~30% out-of-order, 0.5–8 s); dòng hỏng
  đếm rồi bỏ qua, không sập.
- **Partitioner (`wm/partition.py`):** `node_id = hash(host) % N` —
  horizontal fragmentation; chạy được qua `distributed_sweep.py`.
- **Processing node ×N (`wm/engine.py`):** xem mục 4.
- **Checkpoint store:** snapshot atomic per-node (mục 3).
- **Coordinator:** `kill(node)` / `recover(node)` — minh hoạ bằng
  `wm/demos.py:crash_recovery_demo()` và tab **Kill Node Live** của
  `app.py` (DLQ append-only trên đĩa + replay khi revive → Exactly-Once).
- **Merge / Sink:** gộp cửa sổ đã đóng → report (`wm/sweep.py`) +
  Dashboard Streamlit 6 tab (`app.py`).

> **Code layout.** Logic core nằm trong package `wm/`
> (`engine`, `sweep`, `demos`, `partition`, `data/{synthetic,nasa}`).
> Ba script ở root — `analysis.py`, `distributed_sweep.py`, `app.py` —
> chỉ làm CLI mỏng / Streamlit driver, không chứa nghiệp vụ.

## 3. Thiết kế dữ liệu & lưu trữ (Data & Storage Design)

Đây là hệ stream nên "database" = *distributed state phân mảnh* + *durable
checkpoint*, không phải bảng quan hệ.

**3.1 Phân mảnh (fragmentation).** Horizontal fragmentation theo `host`:
`node_id = hash(host) % N`. Lý do: state locality (mọi log của 1 host về
cùng node để aggregate đúng). Rủi ro: hot-key skew `[Ö&V, Distributed
Database Design]`.

**3.2 Lược đồ state mỗi node (in-memory, key→value).**
```
WindowStateStore : key = window_start (event-time bucket)
                   value = { count, status_500 }
DedupSet         : key = event_id            (đảm bảo exactly-once)
WatermarkReg     : max_event_time, watermark
```

**3.3 Lược đồ checkpoint (durable, per-node, atomic file).**
```
Checkpoint = { node_id, watermark, max_event_time, metrics,
               open_windows  : map<window_start,{count,status_500}>,
               closed_windows: map<window_start, result> }
```
Ghi atomic `.tmp → os.replace` ⇒ **Durability**; mỗi snapshot là một
*consistent local snapshot* `[Ö&V, Distributed Reliability]`.

## 4. Bốn tiêu chí rubric — cách đáp ứng

**Windowing Logic.** Gán tumbling window theo *event-time*
(`window_start_for(event_time)`), không theo processing-time. Cửa sổ đóng
khi watermark `WM = max_event_time − allowed_lateness ≥ window.end` (low-
water mark, tinh thần commit-wait Spanner). Log của cửa sổ đã đóng → bỏ
(load shedding) `[Ö&V, Data Stream Management]`.

**State Management.** State theo window; checkpoint atomic định kỳ;
`restore()` + `crash_recovery_demo()` chứng minh kill→recover, state khớp,
không đếm trùng (dedup ⇒ exactly-once ⇒ Atomicity) `[Ö&V, Reliability]`.

**Latency Analysis.** Phân biệt **2 loại latency**: (a) *processing
latency* mỗi event (đo bằng `time.perf_counter_ns()` — timer độ phân giải
nano giây, đơn điệu), báo cáo **p50/p99**, ≈3 µs; (b) *result latency* —
từ lúc cửa sổ kết thúc (event-time) đến lúc chốt, từ ~105 ms đến ~7.8 s
theo Wait Time. Kết luận bottleneck: độ trễ end-to-end **không** đến từ
tính toán (3 µs) mà từ **Wait Time do watermark cố ý áp đặt** ⇒ nối thẳng
sang PACELC. Mở rộng: breakdown latency theo giai đoạn (dedup / window-
update / checkpoint I/O) để chỉ rõ bước tốn nhất.

**Robustness — mô hình throughput.** Backpressure chỉ thật khi *ingestion
rate > processing throughput*, nên hệ mô hình hóa:
```
Producer (ingestion_rate, có burst)
   → BoundedQueue (max_queue)
      → Consumer/engine (max_throughput đo bằng perf_counter)
```
Khi λ (arrival) > μ (service): queue phình → kích hoạt 1 trong 2 chính
sách (nêu rõ trong báo cáo): *drop-newest* (load shedding, ưu tiên latency,
completeness giảm) hoặc *block producer* (đẩy backpressure ngược, không
mất data). Cộng dedup (gửi lặp) + bỏ qua dòng hỏng ⇒ không sập. Đo &
báo cáo: throughput đạt được, queue depth theo thời gian khi burst, số
`backpressure_drops` `[Ö&V, Data Stream Mgmt — flow control / Fault
tolerance]`.

## 5. Quyết định thiết kế & lý do (theory-justified)

| Quyết định | Lý do | Ö&V |
|---|---|---|
| Window theo event-time | Gom theo lúc log tới sai ngữ nghĩa thời gian | Data Stream Mgmt |
| Watermark = max−lateness | Điểm chốt điều chỉnh được; xử lý out-of-order | Reliability; PACELC |
| Phân vùng hash(host)%N | Song song + state locality; tạo node để kill | Distributed DB Design |
| Checkpoint atomic per-node | Recovery sau site failure; Durability | Distributed Reliability |
| Dedup → exactly-once | Replay sau crash không đếm trùng; Atomicity | Reliability / Recovery |
| Bounded queue + throughput | Chịu backpressure khi λ>μ, không sập | Flow control / Fault tol. |

## 6. Mô hình lỗi được xử lý

Site failure (kill→recover từ checkpoint) · communication failure (trễ/
sai thứ tự → watermark) · duplicate (dedup → idempotent) · malformed
input (đếm+bỏ qua) · backpressure (queue giới hạn + chính sách drop/block).
Dữ liệu thật: NASA-HTTP có *site failure thật* — server tắt vì bão
Hurricane Erin (01→03/Aug/1995), dùng làm ví dụ minh họa.

## 7. Đánh đổi trung tâm — CAP / PACELC

`allowed_lateness` là núm trượt: **Strict** (lateness lớn) ưu tiên
Consistency trả giá Latency; **Heuristic** (nhỏ) ưu tiên Latency, chấp
nhận dữ liệu thiếu (eventual). Đúng vế **ELC của PACELC**: không phân
hoạch mạng vẫn phải chọn L vs C `[Ö&V, Replication / CAP-PACELC]`.
Deliverable *Data Completeness % vs Wait Time* = bằng chứng thực nghiệm
định lượng cho đánh đổi lý thuyết.

## 8. Giới hạn & hướng mở rộng

Hot-key skew khi phân vùng theo `host` → consistent hashing / rebalancing.
`seen_ids` tăng tuyến tính → Bloom filter + TTL theo watermark. Recovery
ở mức một node độc lập → coordinated snapshot Chandy–Lamport khi các node
trao đổi message trực tiếp.
