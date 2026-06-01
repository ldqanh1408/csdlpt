# Distributed Watermark Tracker — Hướng dẫn sử dụng

<p align="center">
  <strong>Chủ đề #112 · Distributed Database Project</strong><br>
  So sánh <em>Strict Watermark</em> (0% mất mát) vs <em>Heuristic Watermark</em> (DDSketch + DLQ)
</p>

---

## Mục lục

- [1. Tổng quan](#1-tổng-quan)
- [2. Cài đặt](#2-cài-đặt)
- [3. Chuẩn bị dữ liệu](#3-chuẩn-bị-dữ-liệu)
- [4. Chạy hệ thống](#4-chạy-hệ-thống)
- [5. Cấu hình](#5-cấu-hình)
- [6. Giám sát](#6-giám-sát)
- [7. API Endpoints](#7-api-endpoints)
- [8. Sinh báo cáo tự động](#8-sinh-báo-cáo-tự-động)
- [9. Cấu trúc thư mục](#9-cấu-trúc-thư-mục)
- [10. Xử lý sự cố](#10-xử-lý-sự-cố)

---

## 1. Tổng quan

**Distributed Watermark Tracker** là hệ thống stream processing phân tán, giải quyết bài toán **tổng hợp theo cửa sổ thời gian** khi dữ liệu đến **bất tuần tự** (out-of-order). Hệ thống so sánh trực tiếp hai chiến lược:

| Chiến lược | Cơ chế | Cam kết | Độ trễ |
|---|---|---|---|
| **Strict** | `W_global = min(LW_i)` qua Coordinator Raft 3-node | 0% loss | Cao (phụ thuộc straggler) |
| **Heuristic** | `W_h = max(T_event) - DDSketch.quantile(p)` + DLQ | 1% immediate, 100% eventual | Thấp |

**Dữ liệu:** 2,961,423 chuyến taxi từ NYC TLC Yellow Taxi (tháng 1/2024), nén thời gian 60x -> 12.4 giờ mô phỏng.

**Kiến trúc phân tán 5 domain:**

```
                         ┌──────────────────────────┐
                         │      Control Plane        │
                         │  ┌──────────┐ ┌────────┐ │
                         │  │Coordinator│ │Aggregator│
                         │  │ (Raft x3) │ │(HA Pair)│ │
                         │  └─────▲────┘ └───▲─────┘ │
                         └────────┼───────────┼──────┘
                                  │ heartbeat │ W_h
 ┌──────────┐   produce     ┌────┴───────────┴──────┐   emit    ┌──────────┐
 │ Ingestor │──────────────>│   Kafka (12 partitions) │<---------│  Worker  │
 │          │  events +     └──┬──────┬──────┬───────┘  results  │  (4 nodes│
 │          │  punctuations   │      │      │                    │   each 3 │
 └──────────┘                 ▼      ▼      ▼                    │  parts)  │
                           poll   poll   poll                    └──────────┘
```

---

## 2. Cài đặt

### 2.1 Yêu cầu

| Thành phần | Yêu cầu |
|---|---|
| **Python** | 3.10+ |
| **Docker** | 24.0+ (kèm Docker Compose v2) |
| **RAM** | >= 8 GB |
| **Disk** | >= 5 GB |

### 2.2 Setup

```bash
# Clone repo
git clone <repo-url> csdlpt
cd csdlpt

# Virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Cai dependencies
pip install -r requirements.txt
pip install pytest matplotlib streamlit
```

---

## 3. Chuẩn bị dữ liệu

### 3.1 Cơ chế nén thời gian (Time Compression)

Dữ liệu gốc NYC TLC Yellow Taxi trải dài **31 ngày thực** (~2,678,368 giây). Nếu chạy trực tiếp, toàn bộ sự kiện sẽ có vẻ đến đúng giờ (lateness ~ 0), khiến đường cong completeness-vs-wait phẳng ở 100% — vô nghĩa cho nghiên cứu watermark.

**Giải pháp:** Nén thời gian với hệ số `DIV = 60`.

```
t_ref  = min(pickup_timestamps)

T_event   = (pickup_timestamp  - t_ref) / 60
T_arrival = (dropoff_timestamp - t_ref) / 60

lateness  = T_arrival - T_event = trip_duration / 60
```

**Tính chất then chốt:** Cả hai timestamp cùng chia cho 1 hệ số -> lateness = duration / DIV -> **giữ nguyên phân phối bất tuần tự thực tế.**

**Bảng quy đổi:**

| Chỉ số | Thực tế | Sau nén (DIV=60) |
|---|---|---|
| Tổng thời gian | 31 ngày (2,678,368s) | **12.4 giờ (44,639s)** |
| lateness p50 | ~12 phút | **11.63s** |
| lateness p95 | ~38 phút | **37.78s** |
| lateness p99 | ~60 phút | **59.70s** |
| lateness max | ~5 giờ | **359.73s** |

### 3.2 Tải dữ liệu

**Link chính thức từ NYC TLC:**

```bash
# Tải dữ liệu thô (Parquet, 104 MB)
wget -P dataset/ https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
```

**Chuẩn hóa + nén thời gian:**

```bash
# Tự động detect Parquet hoặc CSV, nén DIV=60 -> schema engine
python tools/nyc_taxi_to_events.py
```

Sau khi chạy, thư mục `dataset/` sẽ có:

| File | Kích thước | Mô tả |
|---|---|---|
| `yellow_tripdata_2024-01.parquet` | ~104 MB | Dữ liệu thô NYC TLC (tải từ link trên) |
| `nyc_taxi_events_full.csv` | ~152 MB | Đã chuẩn hóa + nén thời gian — 2.96M dòng, sẵn sàng cho engine |

### 3.3 Schema chuẩn của Engine

Engine đọc CSV qua **`csv.DictReader`** (theo tên cột, không theo vị trí). Mọi dataset muốn chạy được với hệ thống phải có header đúng định dạng sau:

```
,host,time,method,url,response,bytes,arrival
```

| # | Cột | Bắt buộc | Kiểu | Vai trò |
|---|---|---|---|---|
| 0 | *(index)* | Không | int | Bỏ qua khi đọc (tự sinh bởi `to_csv(index=True)`) |
| 1 | `host` | **Có** | str/int | **Partition key** — `hash(host) % 12` quyết định partition |
| 2 | `time` | **Có** | float | **Event-time** (epoch giây). Thời điểm sự kiện xảy ra thực tế |
| 3 | `method` | Không | str | HTTP method ("GET", "POST",...) — cosmetic |
| 4 | `url` | Không | str | URL path — cosmetic |
| 5 | `response` | Không | int | HTTP status code, mặc định = 200 |
| 6 | `bytes` | Không | int | Kích thước response, mặc định = 0 |
| 7 | `arrival` | Không | float | **Arrival-time** (epoch giây). Thời điểm sự kiện ĐẾN hệ thống |

> **Hai cột quyết định hành vi watermark:** `time` và `arrival`.
>
> ```
> lateness = arrival - time
> ```
>
> - `lateness > 0` → sự kiện đến muộn (out-of-order) → watermark phải chờ
> - `lateness = 0` → dữ liệu đến đúng giờ → completeness luôn 100%, đường cong phẳng
> - **Nếu không có cột `arrival`:** engine tự sinh `arrival = event_time + SIMULATED_LAG_S` (mặc định +2s) → mọi sự kiện trễ đúng 2s, không có out-of-order thực
>
> **Để có đường cong completeness-vs-wait có ý nghĩa**, dataset cần có phân phối lateness thực tế (p50, p95, p99, outliers). Dữ liệu NYC taxi thỏa điều kiện này vì lateness = trip_duration/DIV.


### 3.4 Workflow chuẩn bị dataset

```
Dữ liệu thô               Chuẩn hóa                    Thống kê             Sẵn sàng
(parquet/csv/log)  ──►  nyc_taxi_to_events.py  ──►  dataset_stats.py  ──►  engine
                        (time compression)           (kiểm tra phân phối)
```

**Nếu bạn có dataset riêng**, chỉ cần tạo CSV với các cột bắt buộc (`host`, `time`) và tùy chọn (`arrival`, `method`, `url`, `response`, `bytes`). Cột `arrival` là quan trọng nhất để tạo ra out-of-order pattern có ý nghĩa.

### 3.5 Công cụ trong `tools/`

Tất cả script nằm trong `tools/`. Chạy từ **thư mục gốc** của repo.

#### 3.5.1 `nyc_taxi_to_events.py` — Chuyển đổi NYC Taxi → Engine Schema

**Chức năng:** Đọc dữ liệu NYC TLC Yellow Taxi (Parquet hoặc CSV), làm sạch (lọc duration 0-6h, giới hạn tháng 1/2024), nén thời gian DIV=60, xuất CSV đúng schema engine.

| Flag | Mặc định | Mô tả |
|---|---|---|
| `--src` | `dataset/yellow_tripdata_2024-01.parquet` | File nguồn (tự detect `.parquet` / `.csv`) |
| `--out` | `dataset/nyc_taxi_events_full.csv` | File output |
| `--rows` | `0` (all) | Giới hạn số dòng (0 = toàn bộ 2.96M) |
| `--div` | `60` | Hệ số nén thời gian |
| `--late-target-s` | `0` (tắt) | Nếu >0, tự tính `DIV = duration_p95 / target` |

```bash
python tools/nyc_taxi_to_events.py                          # Full dataset
python tools/nyc_taxi_to_events.py --rows 150000            # Test nhanh 150K dòng
python tools/nyc_taxi_to_events.py --late-target-s 40       # Tự động chọn DIV để p95 lateness ≈ 40s
python tools/nyc_taxi_to_events.py --src my_data.csv --out my_events.csv
```

#### 3.5.2 `dataset_stats.py` — Thống kê dữ liệu thô

**Chức năng:** Đọc file NYC TLC gốc (19 cột), in toàn bộ thống kê: datetime range, trip duration percentiles, lateness sau nén ở các DIV, out-of-order analysis, phân bố zone, total_amount.

```bash
python tools/dataset_stats.py                           # Default: yellow_tripdata_2024-01.csv
python tools/dataset_stats.py --src other_file.csv      # File khác
```

**Output mẫu:** duration p50/p95/p99, lateness ở DIV=30/60/120, số bước out-of-order, top zones, total_amount stats.

#### 3.5.3 `make_out_of_order.py` — Inject out-of-order có kiểm soát

**Chức năng:** Từ dataset đã chuẩn hóa, inject độ trễ nhân tạo vào `arrival` để kiểm soát chính xác phân phối lateness. Hữu ích khi muốn test watermark với độ trễ biết trước.

| Flag | Mặc định | Mô tả |
|---|---|---|
| `--rows` | `150000` | Số dòng xử lý |
| `--p-late` | `0.30` | Tỷ lệ dòng bị trễ (0-1) |
| `--max-late-s` | `20.0` | Độ trễ tối đa (giây) |
| `--src` | `dataset/nyc_taxi_events_full.csv` | Input |
| `--out` | `dataset/oo_sample.csv` | Output |
| `--compress-span-s` | `0` (tắt) | Nếu >0, nén thêm event-time span |

```bash
# 30% dòng bị trễ 0-20s → completeness ~70% ở δ=0, ~100% ở δ=20
python tools/make_out_of_order.py --rows 150000 --p-late 0.30 --max-late-s 20

# 50% dòng bị trễ 0-60s, nén span về 3600s
python tools/make_out_of_order.py --rows 50000 --p-late 0.50 --max-late-s 60 --compress-span-s 3600
```


---

## 4. Chạy hệ thống

### 4.1 Strict Mode — Docker Compose

Khởi chạy cụm đầy đủ: 1x Ingestor, 3x Coordinator (Raft), 4x Worker, Kafka, ZooKeeper, MinIO, Prometheus, Grafana.

```bash
cd deploy

# Build + chạy
MODE=strict DATASET_FILE=nyc_taxi_events_full.csv docker compose --profile strict up --build

# Hoặc chạy background
MODE=strict DATASET_FILE=nyc_taxi_events_full.csv docker compose --profile strict up --build -d
```

**Các service khởi tạo:**

| Service | Container | Port | Vai trò |
|---|---|---|---|
| `coordinator-1` | Leader | 9000 | Tính `W_global = min(LW_i)`, Raft cluster |
| `coordinator-2` | Follower | 9003 | Dự phòng Raft |
| `coordinator-3` | Follower | 9004 | Dự phòng Raft |
| `node0` | Worker (P0-2) | 9101 | Poll Kafka -> Min-Heap -> Window -> Emit |
| `node1` | Worker (P3-5) | 9102 | Poll Kafka -> Min-Heap -> Window -> Emit |
| `node2` | Worker (P6-8) | 9103 | Poll Kafka -> Min-Heap -> Window -> Emit |
| `node3` | Worker (P9-11) | 9104 | Poll Kafka -> Min-Heap -> Window -> Emit |
| `ingestor` | Ingestor | (8100 nội bộ) | Đọc CSV -> Produce Kafka + Punctuation |
| `kafka` | Kafka Broker | 29092 | 12 partitions topic `events` |
| `zookeeper` | ZooKeeper | 2181 | Kafka metadata, leader election |
| `minio` | MinIO | 9002 | Tier-3 cold storage (S3) |
| `prometheus` | Prometheus | 9090 | Scrape metrics |
| `grafana` | Grafana | 3000 | Dashboard (admin/admin) |

**Phân công partition -> worker:**

```
Partition  0, 1, 2  -> node0
Partition  3, 4, 5  -> node1
Partition  6, 7, 8  -> node2
Partition  9,10,11  -> node3
```

**Kiểm tra trạng thái:**

```bash
# Trạng thái coordinator
curl http://localhost:9000/health
curl http://localhost:9000/state | python -m json.tool

# Metrics worker
curl http://localhost:9101/api/metrics | python -m json.tool

# Log container
docker logs -f refactor-coordinator-1
docker logs -f refactor-worker-0
docker logs -f refactor-ingestor
```

### 4.2 Heuristic Mode — Docker Compose

Khởi chạy cụm: 1x Ingestor, 2x Aggregator (Active-Standby), 4x Worker, Kafka, ZooKeeper, MinIO, Prometheus, Grafana.

```bash
cd deploy

# Build + chạy
MODE=heuristic DATASET_FILE=nyc_taxi_events_full.csv docker compose --profile heuristic up --build
```

**Các service khởi tạo:**

| Service | Container | Port | Vai trò |
|---|---|---|---|
| `aggregator` | Primary | 9007 | Tính `W_global_h = min(W_h)` |
| `aggregator-standby` | Standby | 9005 | Dự phòng nóng, failover tự động |
| `node0`-`node3` | Worker x4 | 9101-9104 | DDSketch + DLQ per partition |
| `ingestor` | Ingestor | (8100 nội bộ) | Đọc CSV -> Produce Kafka |
| `kafka` | Kafka Broker | 29092 | 12 partitions + topic `late_logs_dlq` |

**Kiểm tra trạng thái:**

```bash
curl http://localhost:9007/health   # Aggregator primary/standby status
curl http://localhost:9007/state    # W_global_h + L_eff
curl http://localhost:9101/api/metrics
```

### 4.3 Dừng & Dọn dẹp

```bash
# Dừng tất cả

# Dừng + xóa volumes (checkpoint, RocksDB, MinIO data)
```

### 4.4 Test Suite

```bash
# Toàn bộ test
python -m pytest tests/ -v

# Test theo nhóm
python -m pytest tests/test_strict.py -v        # Strict engine + coordinator
python -m pytest tests/test_heuristic.py -v     # Heuristic engine + aggregator + DLQ
python -m pytest tests/test_ddsketch.py -v      # DDSketch accuracy
python -m pytest tests/test_failover.py -v      # Failover scenarios
python -m pytest tests/test_chaos.py -v         # Chaos testing
python -m pytest tests/test_integration.py -v   # Integration tests

# Với coverage
pip install pytest-cov
python -m pytest tests/ --cov=. --cov-report=html
```

### 4.5 Dashboard (Streamlit)

```bash
streamlit run deploy/dashboard.py --server.port 8501
```

5 trang: Cluster Health, Node Control, Completeness vs Wait, Logs, Compare.

---

## 5. Cấu hình

Toàn bộ cấu hình qua biến môi trường — không cần file `.env`. Tập trung trong `common/config.py`.

### 5.1 Biến toàn cục

| Biến | Mặc định | Mô tả |
|---|---|---|
| `PORT` | `8000` | Cổng HTTP |
| `WINDOW_SIZE_S` | `5.0` | Kích thước tumbling window |
| `TOTAL_PARTITIONS` | `12` | Số Kafka partition |
| `DATASET_FILE` | `nyc_taxi_events_full.csv` | File dataset cho ingestor |
| `LOG_LEVEL` | `debug` | `debug`, `info`, `warning` |

### 5.2 Strict Mode

| Biến | Mặc định | Mô tả |
|---|---|---|
| `DELTA_BASE_S` | `10.0` | Biên an toàn (giây) |
| `HEARTBEAT_TIMEOUT_S` | `10.0` | Ngưỡng timeout heartbeat worker |
| `PUNCTUATION_INTERVAL_S` | `1.0` | Chu kỳ Empty Punctuation khi partition idle |
| `FAILOVER_ENABLED` | `false` | Bật failover tự động |
| `COORDINATOR_PEERS` | `""` | Danh sách peer (cách nhau `,`) |

### 5.3 Heuristic Mode

| Biến | Mặc định | Mô tả |
|---|---|---|
| `HEURISTIC_ALPHA` | `0.01` | Sai số tương đối DDSketch (1%) |
| `HEURISTIC_P_NORMAL` | `0.99` | Phân vị p cho chế độ thường |
| `HEURISTIC_P_SAFE` | `0.999` | Phân vị p cho chế độ an toàn |
| `HEURISTIC_L_MAX` | `60.0` | Trần L_eff (giây) |
| `DLQ_RETENTION_DAYS` | `7` | Thời gian giữ DLQ |
| `AGGREGATOR_HA_ENABLED` | `false` | Bật Active-Standby HA |

---

## 6. Giám sát

### 6.1 Prometheus + Grafana

```bash
# Tự động khởi chạy cùng docker compose
# Prometheus:  http://localhost:9090
# Grafana:     http://localhost:3000  (admin/admin)
```

### 6.2 Metrics chính

| Metric | Loại | Ý nghĩa |
|---|---|---|
| `watermark_global_seconds` | Gauge | `W_global` hiện tại |
| `watermark_lag_seconds` | Gauge | Độ trễ watermark so với wall-clock |
| `window_completeness_ratio` | Gauge | Tỷ lệ completeness cửa sổ gần nhất |
| `partition_queue_depth` | Gauge | Số event trong BoundedPriorityQueue |
| `heartbeat_missed_total` | Counter | Số heartbeat bị miss |
| `failover_events_total` | Counter | Số lần failover |
| `dlq_pending_events` | Gauge | Số event đang chờ trong DLQ |

---

## 7. API Endpoints

Tất cả service có HTTP REST API. gRPC endpoint tại `port + 50`.

### Coordinator (`:9000`)

| Endpoint | Method | Trả về |
|---|---|---|
| `/health` | GET | `200 OK` |
| `/state` | GET | `{W_global, partitions, term, ...}` |
| `/metrics` | GET | Prometheus text |
| `/api/metrics` | GET | Metrics JSON |

### Worker (`:9101`-`:9104`)

| Endpoint | Method | Trả về |
|---|---|---|
| `/health` | GET | `200 OK` |
| `/state` | GET | `{LW_i, partitions, open_windows, ...}` |
| `/metrics` | GET | Prometheus text |
| `/api/metrics` | GET | Metrics JSON |

### Aggregator (`:9007`)

| Endpoint | Method | Trả về |
|---|---|---|
| `/health` | GET | `200 OK` + primary/standby |
| `/state` | GET | `{W_global_h, partitions, L_eff, ...}` |

---

## 8. Sinh báo cáo tự động (`reports/`)

Một lệnh duy nhất chạy toàn bộ pipeline phân tích và sinh báo cáo so sánh Strict vs Heuristic:

```bash
# Offline — phân tích số + vẽ đường cong (không cần Docker, ~3 phút)
python reports/run_all.py

# Docker — chạy thực nghiệm đầy đủ với cụm phân tán (~30 phút)
python reports/run_all.py --docker
```

**Kết quả lưu trong `reports/artifacts/`:**

| File | Mô tả |
|---|---|
| `analysis_report.md` | Phân tích offline toàn bộ dataset |
| `comparison_report.md` | So sánh Strict vs Heuristic |

**Chạy từng bước riêng lẻ:**

```bash
# Phân tích offline dataset
python reports/analyze_dataset.py

# Chạy thực nghiệm Strict (Docker)
python reports/run_experiment.py --mode strict \
    --dataset nyc_taxi_events_full.csv --deltas 0,5,10,20,40,60

# Chạy thực nghiệm Heuristic (Docker)
python reports/run_experiment.py --mode heuristic \
    --dataset nyc_taxi_events_full.csv --ps 0.50,0.75,0.90,0.95,0.99
```

---

## 9. Cấu trúc thư mục

```
csdlpt/
├── run.py                     # Entry point duy nhất (mọi role + mode)
├── requirements.txt
│
├── common/                    # Thư viện dùng chung
│   ├── config.py              # Centralized config (env vars)
│   ├── types.py               # Dataclass: LogEvent, WindowResult, ...
│   ├── window.py              # TumblingWindow

│   ├── rocks_store.py         # RocksDB wrapper
│   └── metrics.py             # SystemMetrics
│
├── strict/                    # Strict Watermark (0% loss)
│   ├── engine.py              # Per-partition watermark engine
│   ├── worker.py              # BoundedPriorityQueue + heartbeat
│   ├── coordinator.py         # W_global = min(LW_i)
│   ├── raft_coordinator.py    # Raft consensus 3-node
│   ├── failover.py            # 5-step failover/failback
│   └── output_manager.py      # Exactly-once emission
│
├── heuristic/                 # Heuristic Watermark (DDSketch + DLQ)
│   ├── engine.py              # DDSketch per partition
│   ├── aggregator.py          # W_global_h = min(W_h)
│   ├── aggregator_ha.py       # Active-Standby failover
│   └── dlq.py                 # Dead Letter Queue pipeline
│
├── ddsketch/                  # DDSketch implementation
│   ├── sketch.py              # Core: log buckets, mergeable
│   └── compat.py              # Compatibility wrapper
│
├── reports/                   # Tự động sinh báo cáo
│   ├── run_all.py              # Một lệnh -> strict + heuristic + report
│   ├── analyze_dataset.py      # Phân tích offline toàn bộ dataset
│   └── run_experiment.py       # Docker-based Completeness vs Wait Time
│
├── tools/                     # Dataset preprocessing tools
│   ├── nyc_taxi_to_events.py  # Raw taxi -> engine schema (DIV=60, Parquet+CSV)
│   ├── make_out_of_order.py   # Inject out-of-order lateness
│   └── dataset_stats.py       # In thống kê dataset
│
├── deploy/                    # Deployment
│   ├── docker-compose.yml     # Full cluster (14 services, 3 profiles)
│   ├── Dockerfile             # Python 3.10-slim image
│   ├── entrypoint.sh          # Checkpoint cleanup
│   ├── dashboard.py           # Streamlit dashboard
│   └── prometheus.yml         # Prometheus scrape config
│
├── dataset/                   # Dữ liệu NYC Taxi
│   ├── yellow_tripdata_2024-01.parquet  # Raw (tải từ NYC TLC)
│   └── nyc_taxi_events_full.csv        # Chuẩn hóa + nén (152 MB)
│
├── tests/                     # 15+ test files
└── docs/                      # Tài liệu thiết kế (tiếng Việt)
    ├── project_proposal_summary.md        # Đề xuất dự án
    ├── REPORT_academic_watermark_*.md     # Báo cáo học thuật
    └── REPORT_full_dataset_*.md          # Phân tích dataset
```

---

## 10. Xử lý sự cố

### Worker crash -> Failover

1. Coordinator không nhận heartbeat quá 10s -> xác nhận crash
2. Tăng `fencing token` -> vô hiệu hóa lệnh cũ từ worker chết
3. Reassign partition: chia đều cho worker còn sống
4. Worker mới nạp checkpoint từ Shared Volume, `seek(offset+1)` trên Kafka
5. Failback 5 bước: Request -> Lock -> Pause -> Reassign -> Resume

### Partition Idle (Straggler)

- Ingestor phát Empty Punctuation mỗi 1s -> `T_commit` vẫn tiến
- Nếu idle quá lâu: Worker đánh dấu `TEMPORARY_IDLE`
- Coordinator loại partition idle khỏi `min()` -> `W_global` không bị nghẽn
- Có dữ liệu trở lại -> tự động quay lại danh sách active

### Split-Brain Prevention

- Mọi lệnh (PAUSE, RESUME, REASSIGN) kèm `fencing_token = (term, command_id)`
- Worker từ chối lệnh có `term` cũ hơn
- Raft đảm bảo 1 Leader duy nhất

### Lỗi thường gặp

| Lỗi | Nguyên nhân | Cách sửa |
|---|---|---|
| `FileNotFoundError: ...parquet` hoặc `...csv` | Dataset chưa tải/chưa tạo | `wget -P dataset/ <link>` rồi `python tools/nyc_taxi_to_events.py` |
| `port already in use` | Container cũ chưa tắt | `docker compose ... down` |
| Worker không poll được Kafka | Kafka chưa sẵn sàng | Đợi 30s cho Kafka warm-up |
| `MODE` mismatch | Worker chạy strict, coordinator chạy heuristic | Kiểm tra `MODE=` nhất quán |
| OOM khi chạy full dataset | RocksDB + queue quá lớn | Tăng RAM hoặc dùng `--rows` giới hạn |

---

<p align="center">
  <strong>Team StreamPioneers</strong><br>
  Le Dang Quynh Anh · Nguyen Van An
</p>
