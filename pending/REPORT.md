# REPORT — Strict vs Heuristic Watermarks
### Data Completeness % vs Wait Time (ms)

**Dataset:** NASA-HTTP (Internet Traffic Archive, Jul/Aug 1995) — log truy
cập web server thật của NASA. Sample 200 000 dòng đầu của
`dataset/data.csv` (~3 923 bản ghi trùng do mô phỏng retransmission;
~30 % log tới out-of-order với độ trễ 0.5–8 s, ~70 % trễ bình thường
0–200 ms). Tumbling window = 10 s.

## Kết quả sweep (NASA-HTTP thật)

| Wait Time (ms) | Data Completeness % | Late dropped | Result latency (ms) | Proc p99 (µs) |
|---:|---:|---:|---:|---:|
| 0    | 97.18  | 5 638 | 66 091 | 5.62 |
| 100  | 98.28  | 3 439 | 74 118 | 5.19 |
| 250  | 98.28  | 3 439 | 74 118 | 4.84 |
| 500  | 98.28  | 3 439 | 74 118 | 4.73 |
| 1000 | 98.28  | 3 439 | 74 118 | 4.40 |
| 2000 | 98.97  | 2 055 | 78 266 | 4.29 |
| 4000 | 99.72  |   571 | 84 932 | 4.09 |
| 6000 | 99.96  |    74 | 89 851 | 3.96 |
| 8000 | **100.00** | 0 | 94 298 | 4.18 |

Biểu đồ: `tradeoff.png`. Số liệu thô: `tradeoff.csv`.

## Phân tích

**Heuristic Watermark (Wait nhỏ, vd 0–100 ms):** completeness chỉ
**97.2 – 98.3 %** — mất 3 400 – 5 600 log tới trễ — nhưng cửa sổ chốt
sớm hơn ~28 s so với Strict. Phù hợp dashboard real-time cần phản hồi
nhanh, chấp nhận sai số nhỏ.

**Strict Watermark (Wait lớn, 8000 ms):** completeness **100 %**, không
mất log nào, nhưng mỗi kết quả chờ lâu hơn. Phù hợp báo cáo
billing/đối soát cần chính xác tuyệt đối.

**Điểm cân bằng — "knee" thực tế:** đường cong dốc mạnh đến ~2000 ms rồi
thoải dần. Tại **2000 ms** đã đạt **99.0 %** với latency thấp hơn ~12 s
so với Strict — thường là lựa chọn thực tế tốt nhất nếu không bắt buộc
100 %.

**Quan sát đặc thù dữ liệu thật — plateau 100 → 1000 ms:** completeness
KHÔNG cải thiện trong khoảng Wait Time = 100 – 1000 ms (đều 98.28 %).
Lý do: độ trễ trong dataset có phân phối **bimodal** (normal 0–200 ms
và late 500–8000 ms, gap rỗng ở 200–500 ms). Tăng Wait Time trong vùng
gap không cứu thêm event nào. Đây là bài học khi tune Wait Time trên
dữ liệu thật: phải biết phân phối trễ để không "trả tiền latency mà
không nhận lại completeness".

**Bottleneck (Latency Analysis):** thời gian xử lý mỗi event vẫn rất nhỏ
và ổn định (p99 ≈ 4 – 6 µs trên 200 K event, đo bằng `perf_counter_ns`).
Result latency lớn (~66 – 94 s/window) **không** đến từ tính toán mà
đến từ **gap event-time giữa các burst traffic của NASA**: cửa sổ phải
chờ event đầu tiên của burst sau để watermark vượt
`window_end + allowed_lateness`. Đây là đặc tính bản chất của log thật
(session bursts), không phải lỗi engine. Tổng kết: end-to-end latency
= `gap giữa burst + allowed_lateness`, không phải `~allowed_lateness`
như trên dữ liệu mô phỏng dày event.

**Robustness:** 3 923 bản ghi trùng được lọc đúng (deduplication
idempotent). Sweep chính chạy KHÔNG bật backpressure để cô lập biến đo
completeness. Riêng `backpressure_demo()` đẩy queue depth vượt ngưỡng
10 000 → 67 229 drop có kiểm soát, completeness vẫn 98.95 %, engine
**không crash**.

**Crash recovery:** kill engine sau khi xử lý ~50 % stream
(101 961 event, on_time=98 923) → restore từ checkpoint → on_time
KHỚP CHÍNH XÁC 98 923, chạy tiếp nốt nửa sau, completeness cuối 98.97 %.
State không mất, không đếm trùng (dedup + checkpoint atomic ⇒
exactly-once).

## Container Deployment — Acceptance Test Results

Phiên bản 3 của hệ thống được triển khai dưới dạng Docker Compose
microservice (7 container: coordinator + 4 node + ingestor + prometheus +
grafana). 10 acceptance test với fault injection thực tế:

| # | Test | Fault injection | Kết quả |
|---|------|----------------|---------|
| 01 | smoke | — | PASS |
| 02 | happy | — | ALL_DONE (10K events) |
| 03 | kill_node | `docker kill node2` | TIMEOUT + diagnosis DEAD |
| 04 | revive_node | kill → `docker compose up -d` | ALL_DONE |
| 05 | slow_node | `tc netem delay 100ms` | ALL_DONE (SLOW ≠ DEAD) |
| 06 | partition | `docker network disconnect` | TIMEOUT + DEAD |
| 07 | coordinator_down | `docker kill coordinator` | ALL_DONE (DLQ replay) |
| 08 | double_report | Gửi trùng EOS report | ALL_DONE (idempotent C4) |
| 09 | stale_run | RUN_ID cũ | stale_run_id rejected (C3) |
| 10 | data_loss | Drop 5% event | DATA_LOSS detected (C11) |

**10/10 PASS.** Mỗi test xác nhận một tập hợp con các production contract
C1–C11. Hệ thống chứng minh khả năng chịu lỗi thực tế: kill node →
coordinator phát hiện DEAD qua heartbeat gap, revive → DLQ replay khôi
phục, kill coordinator → node retry với bounded backoff + DLQ persist,
double report → idempotent.

## Tái lập kết quả

```bash
# In-process sweep (NASA-HTTP):
python analysis.py --source real --csv dataset/data.csv -n 200000

# Container acceptance tests:
make build
make test-all

# Observability stack:
make up-obs
```

Sinh `tradeoff.csv`, `tradeoff.png`, in demo recovery + backpressure.
Mặc định (`python analysis.py`) chạy trên synthetic 5 000 event để test
nhanh.
