# OBJECTIVES — Project 112 · Distributed Watermark Tracker

> File này là **single source of truth** cho mục tiêu đồ án và rubric chấm điểm.
> Mọi quyết định kỹ thuật, thiết kế, và phạm vi đều phải đối chiếu lại file này.

---

## 1. Đề bài gốc (verbatim)

> **112. Distributed Watermark Tracker: "Log Delay Compensator"**
>
> - **Dataset:** `Web_Server_Logs` where some packets arrive **"Out-of-Order"**
>   (e.g., a log from 10:01 arrives at 10:05).
> - **The Task:** Implement **Watermarks** to handle **event-time skew**. The
>   system must decide how long to wait for "late" data before closing a time
>   window.
> - **Analysis:** Compare **"Strict Watermarks"** (no data loss, high latency)
>   vs. **"Heuristic Watermarks"** (some data loss, low latency).
> - **Deliverable:** A report showing the **"Data Completeness %" vs.
>   "Wait Time" (ms)**.

### Diễn giải tiếng Việt

| Thành phần | Yêu cầu |
|---|---|
| **Dataset** | Web server log thật, có log "đến muộn" so với event-time |
| **Bài toán** | Hệ thống phải quyết định chờ bao lâu cho late-data trước khi đóng window |
| **Cơ chế** | **Watermark** (event-time skew compensation) |
| **Phân tích** | So sánh Strict (chờ lâu, không mất data) vs Heuristic (chờ ngắn, chấp nhận mất một phần) |
| **Output cuối** | Báo cáo với biểu đồ **Data Completeness %** vs **Wait Time (ms)** |

---

## 2. Rubric — Tiêu chí "Excellent (90–100%)"

Để đạt mức **Excellent**, bốn tiêu chí dưới đây phải đều đạt:

| # | Tiêu chí | Yêu cầu Excellent (90–100%) |
|---|---|---|
| **R1** | **Windowing Logic** | Correct use of **Event-Time vs. Processing-Time** |
| **R2** | **State Management** | Efficiently manages and **checkpoints distributed state** |
| **R3** | **Latency Analysis** | Uses **high-resolution timers**; identifies **bottlenecks** |
| **R4** | **Robustness** | Handles **backpressure or duplicates** without crashing |

### Giải nghĩa cụ thể từng tiêu chí

#### R1 — Windowing Logic (Excellent)

- Gán event vào window **bằng event-time** (timestamp trong log), **không** bằng processing-time (lúc engine nhận).
- Watermark = `max(event_time_seen) − allowed_lateness` (Wait Time).
- Window đóng khi `watermark ≥ window_end`, không phải khi `wall_clock ≥ window_end`.
- Late-data đến sau khi window đóng → đếm vào `dropped_late`, **không** mở lại window.

#### R2 — State Management (Excellent)

- Window state (counter, partial aggregate) được **giữ riêng cho từng key/partition**.
- **Checkpoint atomically** ra disk (tmp file + `os.replace`) — restart không mất state.
- **Restore** từ checkpoint khi node respawn — không đếm trùng (giữ exactly-once).
- Trong distributed setup: state phân theo `partition_key`, mỗi node độc lập, sync qua coordinator.

#### R3 — Latency Analysis (Excellent)

- Đo bằng **`time.perf_counter_ns()`** (nanosecond), không phải `time.time()` (chỉ ms).
- Phân tách **processing latency** (per-event xử lý) vs **result latency** (event → khi window close).
- Xuất **p50, p95, p99** — không chỉ trung bình.
- **Identify bottleneck**: latency tăng ở đâu khi tăng load? GIL? IO? Lock contention?

#### R4 — Robustness (Excellent)

- **Backpressure**: queue có giới hạn, drop policy rõ ràng, đếm `backpressure_drops`.
- **Duplicates**: `seen_ids` set → exactly-once semantics, gửi 2 lần cùng event chỉ đếm 1.
- **Crash recovery**: kill engine giữa chừng → restart từ checkpoint → kết quả không đổi.
- **Node death** (distributed): coordinator phát hiện missing node, không treo vô hạn.
- **Out-of-order**: log đến đảo thứ tự, engine vẫn cho kết quả đúng nếu trong allowed lateness.

---

## 3. Deliverable bắt buộc

Theo đề bài, **deliverable chính** là:

> *A report showing the **"Data Completeness %" vs. "Wait Time" (ms)"**.*

Cụ thể hóa:

| Artifact | Nội dung | File |
|---|---|---|
| **Biểu đồ chính** | Trục X: Wait Time (ms) — trục Y: Data Completeness % — Pareto curve | `tradeoff.png` |
| **Số liệu thô** | ≥9 mức Wait Time (0ms → ∞), mỗi mức ghi completeness, latency p50/p99, dropped_late | `tradeoff.csv` |
| **Báo cáo phân tích** | So sánh Strict (Wait=∞) vs Heuristic (Wait=200ms, 500ms, …) — chọn sweet spot | `pending/REPORT.md` |
| **Code chạy được** | Engine + sweep script tái lập số liệu trên bằng 1 lệnh | `wm/engine.py`, `wm/sweep.py` |
| **Demo trực quan** | Dashboard cho giảng viên kéo slider Wait Time và xem completeness biến đổi realtime | `app.py` (Streamlit) |

---

## 4. Mapping rubric → code hiện tại

| Rubric | File / function chứng minh | Trạng thái |
|---|---|---|
| **R1 — Event-time** | `wm/engine.py:window_start_for(event_time)` — gán theo event-time | đã có |
| **R1 — Watermark** | `wm/engine.py` — `watermark = max_event_time − allowed_lateness` | đã có |
| **R2 — Checkpoint** | `wm/engine.py:checkpoint()` — atomic write (`.tmp` → `os.replace`) | đã có |
| **R2 — Restore** | `wm/engine.py:restore()` — load state khi respawn | đã có |
| **R2 — Distributed** | `wm/partition.py:partition_key`, `run_cluster` — N-node với coordinator | đã có |
| **R3 — High-res timer** | `wm/engine.py` — dùng `time.perf_counter_ns()` | đã có |
| **R3 — Bottleneck ID** | `wm/sweep.py` — bảng latency p50/p99 theo Wait Time | đã có |
| **R4 — Backpressure** | `wm/engine.py` — `max_queue`, `backpressure_drops` | đã có |
| **R4 — Dedup** | `wm/engine.py` — `seen_ids` (exactly-once) | đã có |
| **R4 — Crash recovery** | `wm/demos.py:crash_recovery_demo()` — kill→restore | đã có |
| **R4 — Node death** | `deploy/{node,coordinator}/main.py` — heartbeat + diagnosis (§8.13 + §9.5) | đã có (chờ Docker build/test) |
| **Deliverable — chart** | `tradeoff.png` + `wm/sweep.py:write_report` | đã có |
| **Deliverable — data** | `tradeoff.csv` (9 mức Wait Time) | đã có |
| **Deliverable — report** | `pending/REPORT.md` (Strict vs Heuristic analysis) | đã có |
| **Deliverable — demo** | `app.py` Streamlit 7-tab dashboard | đã có |

---

## 5. Phạm vi (Scope) — Cái có và cái không

### Có trong scope

- Watermark + event-time windowing trên Python single-process.
- Distributed mode N-node với coordinator (mô phỏng bằng `partition.py`, hardened bằng Docker Compose theo §8.13 của `CONTAINER_COORDINATION.md`).
- Sweep Wait Time → biểu đồ tradeoff completeness vs latency.
- Crash recovery + dedup + backpressure (3 cơ chế robustness).
- Dataset thật: **NASA-HTTP logs** (đã wrap trong `wm/data/nasa.py`).
- Dataset dự phòng: synthetic (`wm/data/synthetic.py`) khi không tải được NASA.

### Ngoài scope (overkill cho đồ án)

- Kubernetes / service mesh (xem `CONTAINER_COORDINATION.md` §8.11).
- gRPC / Kafka thay HTTP webhook — `INFRASTRUCTURE.md` đã ghi rõ trade-off.
- Multi-region / cross-datacenter — chỉ là phần "hướng phát triển" trong báo cáo.
- Consensus protocol (Raft) — coordinator không cần consensus, chỉ cần đếm.

---

## 6. Tiêu chí đạt — Project được coi là "xong" khi

Đối chiếu đồng thời với rubric 4 tiêu chí và deliverable:

1. **R1 đạt**: Có test minh chứng event-time vs processing-time cho kết quả khác nhau khi data out-of-order; engine dùng đúng event-time.
2. **R2 đạt**: Demo crash recovery cho thấy kill engine giữa chừng → restart → tổng count không đổi.
3. **R3 đạt**: `tradeoff.csv` có cột `p50_ns`, `p99_ns`; report phân tích bottleneck (GIL? IO?).
4. **R4 đạt**: Demo backpressure (queue full → drop có kiểm soát) + dedup (gửi trùng → đếm 1).
5. **Deliverable**: `tradeoff.png` hiển thị curve Completeness vs Wait Time với ≥ 9 điểm.
6. **Reproducibility**: Một lệnh `python analysis.py` tái sinh được `tradeoff.csv` + `tradeoff.png`.
7. **Demo**: `streamlit run app.py` mở dashboard, giảng viên tương tác được realtime.

Khi 7 điều trên cùng đạt → đồ án xếp **Excellent (90–100%)** theo rubric.

---

## 7. Liên kết tới tài liệu chi tiết

| Cần xem | File |
|---|---|
| Mục lục toàn bộ deliverable | `docs/INDEX.md` |
| Thiết kế hệ thống tổng quát | `docs/DESIGN.md` |
| Báo cáo khoa học đầy đủ | `docs/SYSTEM_REPORT.md` |
| Cơ chế đồng bộ kết thúc luồng (EOS) | `docs/EOS_MARKER.md` |
| Lựa chọn hạ tầng | `docs/INFRASTRUCTURE.md` |
| Tối ưu hiệu năng | `docs/PERFORMANCE.md` |
| Thuật ngữ tiếng Anh chuyên ngành | `docs/GLOSSARY.md` |
| Đề cương dự án (Project Proposal) | `pending/PROPOSAL.md` |
| Báo cáo phân tích Strict vs Heuristic | `pending/REPORT.md` |
| Protocol EOS chuẩn trên container | `pending/CONTAINER_COORDINATION.md` §8.13 |
