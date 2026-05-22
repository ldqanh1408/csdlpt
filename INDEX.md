# INDEX — Bộ file thiết kế hệ thống cuối cùng
### Project 112 · Distributed Watermark Tracker ("Log Delay Compensator")

Đây là toàn bộ deliverable. Đọc theo thứ tự: `DESIGN.md` → `REPORT.md` →
code → chạy `app.py` để demo.

## Danh sách file

| File | Vai trò | Dùng để nộp mục nào |
|---|---|---|
| `PROPOSAL.md`          | Đề cương dự án theo template (6 mục) | Project Proposal (Tuần 3) |
| `DESIGN.md`            | Tài liệu thiết kế hệ thống (kiến trúc, lý thuyết Ö&V) | Design document 2 trang |
| `REPORT.md`            | Báo cáo phân tích Strict vs Heuristic + số liệu | The Analysis |
| `tradeoff.png`         | Biểu đồ Data Completeness % vs Wait Time | Deliverable chính |
| `tradeoff.csv`         | Số liệu thô (9 mức Wait Time) | Phụ lục báo cáo |
| `wm/engine.py`         | Engine lõi: watermark, window, state, checkpoint, dedup | The Code |
| `wm/sweep.py`          | Sweep Wait Time + write_report (csv/png) | The Code |
| `wm/demos.py`          | `crash_recovery_demo`, `backpressure_demo` | The Code |
| `wm/partition.py`      | Distributed N-node: `partition_key`, `run_cluster` | The Code |
| `wm/data/synthetic.py` | Sinh dataset mô phỏng (không cần tải) | The Code (dự phòng) |
| `wm/data/nasa.py`      | Đọc NASA-HTTP (.gz + CSV) + sinh arrival-time | The Code (dữ liệu thật) |
| `analysis.py`          | CLI mỏng: sweep + recovery + backpressure demo | The Code |
| `distributed_sweep.py` | CLI mỏng: cluster N-node | The Code |
| `app.py`               | Dashboard Streamlit 6 tab (live stream · sweep · recovery · cluster · kill-node) | The Proof / demo |
| `README.md`            | Hướng dẫn cài & chạy | README repo |

## Map vào 4 tiêu chí rubric

- **Windowing Logic** → `wm/engine.py`: `window_start_for(event_time)`
  (gán theo event-time) + watermark `= max_event_time − allowed_lateness`
  quyết định đóng cửa sổ.
- **State Management** → `wm/engine.py`: `checkpoint()` ghi atomic
  (`.tmp`→`os.replace`), `restore()`; `wm/demos.py:crash_recovery_demo()`
  chứng minh kill→recover, không mất state, không đếm trùng.
- **Latency Analysis** → `wm/engine.py`: đo bằng `time.perf_counter_ns()`,
  xuất p50/p99 processing latency + result latency; `wm/sweep.py` +
  `tradeoff.csv` có bảng latency theo Wait Time.
- **Robustness** → `wm/engine.py`: `seen_ids` (dedup/exactly-once),
  `max_queue` + `backpressure_drops`; `wm/data/nasa.py`: đếm + bỏ qua
  dòng hỏng, không sập.
- **Distributed (DESIGN.md §3.1)** → `wm/partition.py`: `hash(host) % N`
  → N engines độc lập + checkpoint riêng + merge metrics; báo cáo hot-key
  skew.

## Live Visualization (theo gợi ý giảng viên)

`app.py` là **Dashboard Streamlit 6 tab** (đúng lựa chọn khuyến nghị).
Chạy `streamlit run app.py`:

- Tab **Live Stream** — cửa sổ đóng dần theo thời gian thực, watermark
  bò lên: minh hoạ "Data in Motion".
- Tab **Kill Node Live** — kill/revive node giữa stream, DLQ trên đĩa,
  Auto-Play có lập lịch sự cố + biểu đồ diễn biến cluster: kịch bản
  failure case sống động cho video.

Quay 2 tab này vào video 3–5 phút (The Proof).

## Cách chạy nhanh

```bash
pip install pandas numpy matplotlib plotly streamlit

# Synthetic (mặc định, nhanh vài giây):
python analysis.py

# NASA-HTTP THẬT (dataset/data.csv, 200K event, ~2 phút):
python analysis.py --source real --csv dataset/data.csv -n 200000

# Live visualization:
streamlit run app.py

# Multi-node distributed sweep (partition theo host):
python distributed_sweep.py --nodes 4 -n 100000
```

`REPORT.md` và `tradeoff.{csv,png}` hiện chứa số liệu trên dữ liệu THẬT
(NASA-HTTP 200 K event). Để xem số synthetic cũ, chạy lại
`python analysis.py` không tham số.

## Còn lại tự làm (không thuộc file thiết kế)

Điền tên thành viên + hạn nộp + mã Category vào `PROPOSAL.md`; quay video
demo failure case; điền số chương chính xác vào các chỗ `[Ö&V, Ch.X]`
trong `DESIGN.md` và `REPORT.md`.
