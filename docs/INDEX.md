# INDEX — Bộ file thiết kế hệ thống cuối cùng
### Project 112 · Distributed Watermark Tracker ("Log Delay Compensator") · v3 (container)

Đây là toàn bộ deliverable. Đọc theo thứ tự: `docs/DESIGN.md` → `pending/REPORT.md` →
code → chạy `make test-all` để xác nhận.

> **Cấu trúc thư mục tài liệu:**
> - `docs/` — Tài liệu chính thức của dự án
> - `pending/` — Tài liệu đang xem xét / nháp
> - `deploy/` — Docker container services (coordinator, node, ingestor)
> - `scripts/` — 10 acceptance tests + lib.sh

## Danh sách file

| File | Thư mục | Vai trò | Dùng để nộp mục nào |
|---|---|---|---|
| `OBJECTIVES.md`        | `docs/` | Đề bài gốc + rubric chấm điểm + mapping về code | Mục tiêu / tiêu chí Excellent |
| `DESIGN.md`            | `docs/` | Tài liệu thiết kế hệ thống v3 (container, EOS C1–C11, 10-test matrix) | Design document 2 trang |
| `SYSTEM_REPORT.md`     | `docs/` | Báo cáo khoa học hệ thống đầy đủ bằng tiếng Việt (v3 container) | Tài liệu báo cáo chính thức |
| `GLOSSARY.md`          | `docs/` | Giải nghĩa chi tiết thuật ngữ tiếng Anh chuyên ngành | Thuật ngữ dự án |
| `EOS_MARKER.md`        | `docs/` | Giải pháp đồng bộ kết thúc luồng và Coordinator barrier | Thiết kế luồng / Đồng bộ |
| `INFRASTRUCTURE.md`    | `docs/` | Kiến trúc tối giản và so sánh hạ tầng với Spark/Hadoop | Lựa chọn hạ tầng |
| `PERFORMANCE.md`       | `docs/` | Giải pháp tối ưu hóa hiệu năng nâng cao (Bloom Filter, PyPy, Rust, v.v.) | Tối ưu hóa hiệu năng |
| `INDEX.md`             | `docs/` | Mục lục tổng quát (file này) | Mục lục |
| `PROPOSAL.md`          | `pending/` | Đề cương dự án theo template (6 mục) — đã cập nhật container | Project Proposal (Tuần 3) |
| `REPORT.md`            | `pending/` | Báo cáo phân tích Strict vs Heuristic + số liệu + container test results | The Analysis |
| `CONTAINER_COORDINATION.md` | `pending/` | Đặc tả giao thức EOS + 11 production contracts C1–C11 | Thiết kế giao thức |
| `ARCHITECTURE.md`      | `pending/` | Thiết kế kiến trúc nháp | Tham khảo nội bộ |
| `tradeoff.png`         | root | Biểu đồ Data Completeness % vs Wait Time | Deliverable chính |
| `tradeoff.csv`         | root | Số liệu thô (9 mức Wait Time) | Phụ lục báo cáo |
| `Makefile`             | root | CLI thống nhất: build, up, test-all, demo, clean | DevOps |
| `deploy/docker-compose.yml` | `deploy/` | 7-service orchestration (coord + 4 node + ingestor + prom + grafana) | Deployment |
| `deploy/coordinator/main.py` | `deploy/` | Coordinator: FastAPI barrier + diagnosis + integrity check | The Code (container) |
| `deploy/node/main.py`  | `deploy/` | Node: FastAPI + WatermarkEngine + heartbeat + DLQ replay | The Code (container) |
| `deploy/ingestor/ingest.py` | `deploy/` | Ingestor: scatter + EOS in-band + scatter_total announce | The Code (container) |
| `scripts/test_01_smoke.sh` … `test_10_data_loss.sh` | `scripts/` | 10 acceptance tests với fault injection thực tế | Kiểm thử |
| `scripts/lib.sh`       | `scripts/` | Shared test helpers (wait_all_healthy, assert_jq, cleanup_cluster) | Kiểm thử |
| `scripts/run_all.sh`   | `scripts/` | Orchestrate toàn bộ 10 test | Kiểm thử |
| `wm/engine.py`         | `wm/` | Engine lõi: watermark, window, state, checkpoint, dedup | The Code |
| `wm/sweep.py`          | `wm/` | Sweep Wait Time + write_report (csv/png) | The Code |
| `wm/demos.py`          | `wm/` | `crash_recovery_demo`, `backpressure_demo` | The Code |
| `wm/partition.py`      | `wm/` | Distributed N-node: `partition_key`, `run_cluster` | The Code |
| `wm/data/synthetic.py` | `wm/` | Sinh dataset mô phỏng (không cần tải) | The Code (dự phòng) |
| `wm/data/nasa.py`      | `wm/` | Đọc NASA-HTTP (.gz + CSV) + sinh arrival-time | The Code (dữ liệu thật) |
| `analysis.py`          | root | CLI mỏng: sweep + recovery + backpressure demo | The Code |
| `distributed_sweep.py` | root | CLI mỏng: cluster N-node | The Code |
| `app.py`               | root | Dashboard Streamlit 7 tab (live stream · sweep · recovery · cluster · kill-node) | The Proof / demo |
| `README.md`            | root | Hướng dẫn cài & chạy | README repo |

## Map vào 4 tiêu chí rubric

- **Windowing Logic** → `wm/engine.py`: `window_start_for(event_time)`
  (gán theo event-time) + watermark `= max_event_time − allowed_lateness`
  quyết định đóng cửa sổ. C10: late-event load shedding.
- **State Management** → `wm/engine.py`: `checkpoint()` ghi atomic
  (`.tmp`→`os.replace`), `restore()`; `deploy/node/main.py`: DLQ JSONL
  append-only + replay khi lifespan khởi động (C7). `wm/demos.py`:
  `crash_recovery_demo()` chứng minh kill→recover, không mất state,
  không đếm trùng.
- **Latency Analysis** → `wm/engine.py`: đo bằng `time.perf_counter_ns()`,
  xuất p50/p99 processing latency + result latency; `wm/sweep.py` +
  `tradeoff.csv` có bảng latency theo Wait Time.
- **Robustness** → `wm/engine.py`: `seen_ids` (dedup/exactly-once),
  `max_queue` + `backpressure_drops`. Container fault injection:
  `docker kill` (test_03, test_07), `tc netem delay` (test_05),
  `docker network disconnect` (test_06). 10/10 tests pass.
- **Distributed (DESIGN.md §2.2)** → `hash(event_id) % N` → N engines
  độc lập + checkpoint riêng + EOS protocol C1–C11. Coordinator barrier
  + diagnosis (DEAD/SLOW/NEVER_SEEN) + integrity check (C11).
- **Observability** → Prometheus scrape `/metrics` từ coordinator + 4 node.
  Grafana dashboard: watermark progression, completeness gauge, throughput.
  Khởi động: `make up-obs`.

## Cách chạy nhanh

```bash
# === In-process (phân tích lý thuyết) ===
pip install pandas numpy matplotlib plotly streamlit

# Synthetic (mặc định, nhanh vài giây):
python analysis.py

# NASA-HTTP THẬT (dataset/data.csv, 200K event, ~2 phút):
python analysis.py --source real --csv dataset/data.csv -n 200000

# Live visualization:
streamlit run app.py

# === Container deployment (kiểm thử thực tế) ===
make build          # Build 3 images (coordinator + node + ingestor)
make up             # Start cluster (coordinator + 4 nodes)
make smoke          # D1 — /health gate
make happy          # D2 — 10K events, ALL_DONE
make test-all       # Run all 10 acceptance tests
make chaos          # Run only the 8 chaos tests (03–10)
make up-obs         # Start full stack + Prometheus + Grafana
make demo           # 10-minute live demo
make clean          # Full reset
```

`pending/REPORT.md` và `tradeoff.{csv,png}` chứa số liệu trên dữ liệu THẬT
(NASA-HTTP 200 K event) + kết quả 10 acceptance test trên container.

## Còn lại tự làm

Điền tên thành viên còn lại vào `pending/PROPOSAL.md`; quay video demo
failure case; điền số chương chính xác vào các chỗ `[Ö&V, Ch.X]`
trong `docs/DESIGN.md` và `pending/REPORT.md`.
