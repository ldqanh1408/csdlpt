# Distributed Watermark Tracker — Hướng dẫn sử dụng

<p align="center">
  <strong>Chủ đề #112 · Distributed Database Project</strong><br>
  So sánh <em>Strict Watermark</em> (0% mất mát) vs <em>Heuristic Watermark</em> (DDSketch + DLQ)
</p>

---

## Mục lục

- [⚡ Quick Start](#-quick-start)
- [1. Tổng quan](#1-tổng-quan)
- [2. Cài đặt](#2-cài-đặt)
- [3. Chuẩn bị dữ liệu](#3-chuẩn-bị-dữ-liệu)
- [4. Chạy hệ thống](#4-chạy-hệ-thống)
- [5. Cấu hình](#5-cấu-hình)
- [6. Giám sát](#6-giám-sát)
- [7. API Endpoints](#7-api-endpoints)
- [8. Chạy thực nghiệm & Sinh báo cáo](#8-chạy-thực-nghiệm--sinh-báo-cáo-reports)
- [9. Cấu trúc thư mục](#9-cấu-trúc-thư-mục)
- [10. Xử lý sự cố](#10-xử-lý-sự-cố)

---

## ⚡ Quick Start

> Đường đi ngắn nhất để chạy được hệ thống trong ~10 phút. Chi tiết từng bước xem các mục bên dưới.

**Yêu cầu tối thiểu:** Python 3.10+, Docker 24+ (Compose v2), RAM ≥ 8 GB. Mọi lệnh chạy từ **thư mục gốc repo** (trừ khi ghi `cd deploy`).

```bash
# 1) Cài môi trường Python (chỉ cần cho công cụ dataset, test, dashboard)
python3 -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 2) Chuẩn bị dữ liệu: tải dữ liệu thô NYC TLC rồi nén thời gian DIV=60
wget -P dataset/ https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
python tools/nyc_taxi_to_events.py
#   -> sinh dataset/nyc_taxi_events_full.csv  (2.96M dòng, sẵn sàng cho engine)
#   Muốn chạy thử nhanh:  python tools/nyc_taxi_to_events.py --rows 150000

# 3) Khởi chạy cụm phân tán bằng Docker Compose (chọn 1 trong 2 chế độ)
cd deploy
MODE=strict    DATASET_FILE=nyc_taxi_events_full.csv docker compose --profile strict    up --build -d
# hoặc
MODE=heuristic DATASET_FILE=nyc_taxi_events_full.csv docker compose --profile heuristic up --build -d

# 4) Kiểm tra hệ thống đang chạy
curl http://localhost:9000/health        # strict: coordinator   | heuristic: dùng :9007
curl http://localhost:9101/api/metrics    # metrics của node0
#   Grafana: http://localhost:3000 (admin/admin)   ·   Prometheus: http://localhost:9090

# 5) (Tùy chọn) Dashboard trực quan
streamlit run deploy/dashboard.py --server.port 8501

# 6) Dừng cụm khi xong (đúng profile đã dùng ở bước 3)
docker compose --profile strict down          # thêm -v để xóa luôn dữ liệu/checkpoint
```

**Chạy thực nghiệm Completeness vs Wait Time** (so sánh Strict vs Heuristic) → xem [Mục 8](#8-chạy-thực-nghiệm--sinh-báo-cáo-reports).
**Chạy test** → `python -m pytest tests/ -v` ([Mục 4.4](#44-test-suite)).

| Bạn muốn... | Đi tới |
|---|---|
| Hiểu kiến trúc & 2 chiến lược | [Mục 1](#1-tổng-quan) |
| Chuẩn bị / dùng dataset riêng | [Mục 3](#3-chuẩn-bị-dữ-liệu) |
| Chỉnh tham số (window, δ, percentile p...) | [Mục 5](#5-cấu-hình) |
| Quét thực nghiệm & sinh báo cáo | [Mục 8](#8-chạy-thực-nghiệm--sinh-báo-cáo-reports) |
| Gặp lỗi khi chạy | [Mục 10](#10-xử-lý-sự-cố) |

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
| 1 | `host` | **Có** | str/int | **Partition key** — `stable_partition(host, 12)` quyết định partition |
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
cd deploy

# Dừng tất cả container (giữ lại volume/checkpoint)
docker compose --profile strict down       # nếu đang chạy strict
docker compose --profile heuristic down    # nếu đang chạy heuristic

# Dừng + XÓA volumes (checkpoint, RocksDB, MinIO data) — chạy lại từ đầu sạch sẽ
docker compose --profile strict down -v
docker compose --profile heuristic down -v

# Dọn triệt để nếu còn container/orphan sót lại
docker compose down --remove-orphans -v
```

> Lưu ý: phải truyền đúng `--profile` đã dùng khi `up`, nếu không `down` sẽ không thấy các service của profile đó.

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
| `PUNCTUATION_MODE` | `data-driven` | Chế độ phát Punctuation: `data-driven`, `max-event-time`, `wall-clock` |
| `PUNCTUATION_INTERVAL_S` | `1.0` | Chu kỳ phát Punctuation Token (Empty/Progress) |

### 5.2 Strict Mode

| Biến | Mặc định | Mô tả |
|---|---|---|
| `DELTA_BASE_S` | `10.0` | Biên an toàn (giây) |
| `HEARTBEAT_TIMEOUT_S` | `10.0` | Ngưỡng timeout heartbeat worker |
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

### 5.4 Cấu hình Chế độ Punctuation (`PUNCTUATION_MODE`)

Hệ thống hỗ trợ 3 chế độ phát thông điệp kiểm soát mốc thời gian (**Punctuation**) từ Ingestor để cập nhật Watermark. Cấu hình thông qua biến môi trường `PUNCTUATION_MODE`:

1. **`data-driven` (Mặc định)**:
   * **Nguyên lý**: Trong quá trình nạp (ingestion), watermark toàn cục được giữ ở $-\infty$. Khi kết thúc luồng dữ liệu (EOF), Ingestor sẽ phát một xung Punctuation có mốc thời gian bằng $T_{max\_event} + \delta + \text{window}$ để chốt và giải phóng toàn bộ các cửa sổ cùng một lúc.
   * **Ưu điểm**: Đảm bảo độ hoàn thiện dữ liệu (Data Completeness) đạt tuyệt đối **100%** khi chạy lại (replay) tập dữ liệu CSV thô, không có bản ghi nào bị đánh dấu trễ.
   * **Nhược điểm**: Không chốt cửa sổ lũy tiến trong khi chạy; tất cả các cửa sổ được giữ trong RocksDB và chỉ chốt ở cuối luồng (tốn bộ nhớ RAM/RocksDB hơn).

2. **`max-event-time`**:
   * **Nguyên lý**: Watermark cục bộ của mỗi phân mảnh tịnh tiến lũy tiến dựa trên mốc thời gian sự kiện lớn nhất thực tế ghi nhận được trên phân mảnh đó ($T_{\text{commit}} = T_{\text{event\_max}}$).
   * **Ưu điểm**: Cho phép đóng cửa sổ một cách liên tục và lũy tiến (progressive) theo dòng chảy thời gian của dữ liệu, giảm thiểu bộ nhớ đệm. Không có sự lây nhiễm chéo độ trễ giữa các phân mảnh (cross-partition contamination).
   * **Nhược điểm**: Nếu dữ liệu bị đảo lộn thứ tự mạnh vượt quá biên an toàn, một số bản ghi có thể bị coi là đến muộn và bị loại bỏ (Strict) hoặc định tuyến sang DLQ (Heuristic).

3. **`wall-clock`**:
   * **Nguyên lý**: Watermark tịnh tiến dựa trên thời gian vật lý của hệ thống phát ($T_{\text{commit}} = \text{now} - \delta$).
   * **Ưu điểm**: Phù hợp cho môi trường streaming thời gian thực (real-time streaming) khi Ingestor nhận dữ liệu live liên tục từ Web Server.
   * **Nhược điểm**: Phụ thuộc vào tốc độ phát lại và đồng bộ đồng hồ (clock skew) giữa các node.

#### Hướng dẫn chạy cụ thể cho các chế độ Punctuation:

* **Khi chạy bằng Docker Compose**:
  Thiết lập biến môi trường `PUNCTUATION_MODE` trước khi khởi chạy lệnh compose:
  ```bash
  # Chạy Strict Mode với punctuation dựa trên thời gian sự kiện lớn nhất
  PUNCTUATION_MODE=max-event-time MODE=strict docker compose --profile strict up --build

  # Chạy Heuristic Mode với punctuation dựa trên thời gian thực hệ thống
  PUNCTUATION_MODE=wall-clock MODE=heuristic docker compose --profile heuristic up --build
  ```

* **Khi chạy quét thực nghiệm (Experiment Sweeps)**:
  Sử dụng tham số `--punctuation` của script `reports/run_experiment.py`:
  ```bash
  # Chạy quét Strict
  python reports/run_experiment.py --mode strict --punctuation max-event-time --dataset nyc_taxi_events_sliced.csv

  # Chạy quét Heuristic
  python reports/run_experiment.py --mode heuristic --punctuation max-event-time --dataset nyc_taxi_events_sliced.csv
  ```


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

## 8. Chạy thực nghiệm & Sinh báo cáo (`reports/`)

Hệ thống hỗ trợ chạy thực nghiệm trên môi trường Docker để quét và đo lường đường cong **Completeness % vs Wait Time (ms)** giữa **Strict Mode** và **Heuristic Mode** bằng cờ `--mode`.

### 8.1 Chuẩn bị dữ liệu rút gọn (Sliced Dataset - Khuyên dùng)
Dữ liệu gốc `nyc_taxi_events_full.csv` có gần 3 triệu dòng và mất khoảng 30 phút cho mỗi điểm chạy. Để chạy thử nghiệm nhanh (chỉ mất 2-3 phút cho mỗi cấu hình), hãy cắt ra **200,000 dòng đầu tiên** làm tập test:
* **Windows (PowerShell):**
  ```powershell
  python -c "with open('dataset/nyc_taxi_events_full.csv','r',encoding='utf-8') as f: h=f.readline(); r=[f.readline() for _ in range(200000)]; open('dataset/nyc_taxi_events_sliced.csv','w',encoding='utf-8',newline='').write(h+''.join(r))"
  ```
* **Linux/macOS (Bash):**
  ```bash
  head -n 200001 dataset/nyc_taxi_events_full.csv > dataset/nyc_taxi_events_sliced.csv
  ```

---

### 8.2 Chạy thực nghiệm trên cụm phân tán (Docker)

#### Bước 1: Chạy quét các điểm của Strict Mode
Strict Mode chốt cửa sổ dựa trên Watermark toàn cục. Ta tiến hành quét biên an toàn `DELTA_BASE_S` (giây):
```bash
python reports/run_experiment.py --mode strict \
    --punctuation max-event-time \
    --dataset nyc_taxi_events_sliced.csv \
    --deltas 0,2,5,10,20,40,60,90,120 \
    --max-wait 2000 --settle 30
```

#### Bước 2: Chạy quét các điểm của Heuristic Mode
Để Heuristic Mode hoạt động đúng và vẽ được đường cong suy giảm completeness thực tế (không bị tràn dữ liệu hoặc kích hoạt cơ chế BOO fallback do trễ âm), ta **bắt buộc** phải cấu hình các biến môi trường để kích hoạt **Paced Replay** (phát lại theo nhịp độ thời gian thực) và cho phép **đóng cửa sổ theo watermark cục bộ**:

* **Windows (PowerShell):**
  ```powershell
  # 1. Thiết lập cấu hình phát lại và thu hẹp warmup
  $env:INGESTOR_REPLAY = "arrival"
  $env:REPLAY_SPEED = "50"
  $env:HEURISTIC_LOCAL_WATERMARK_CLOSE = "true"
  $env:HEURISTIC_WARMUP_SAMPLES = "2000"
  $env:HEURISTIC_WARMUP_S = "5.0"
  $env:PYTHONUNBUFFERED = "1"

  # 2. Chạy sweep thực nghiệm heuristic
  python reports/run_experiment.py --mode heuristic \
      --punctuation max-event-time \
      --dataset nyc_taxi_events_sliced.csv \
      --ps 0.1,0.2,0.3,0.4,0.5,0.75,0.9,0.95,0.99,0.999,0.9999 \
      --max-wait 2000 --settle 30
  ```

* **Linux/macOS (Bash):**
  ```bash
  # Chạy sweep heuristic kèm thiết lập biến môi trường
  INGESTOR_REPLAY="arrival" \
  REPLAY_SPEED="50" \
  HEURISTIC_LOCAL_WATERMARK_CLOSE="true" \
  HEURISTIC_WARMUP_SAMPLES="2000" \
  HEURISTIC_WARMUP_S="5.0" \
  PYTHONUNBUFFERED="1" \
  python reports/run_experiment.py --mode heuristic \
      --punctuation max-event-time \
      --dataset nyc_taxi_events_sliced.csv \
      --ps 0.1,0.2,0.3,0.4,0.5,0.75,0.9,0.95,0.99,0.999,0.9999 \
      --max-wait 2000 --settle 30
  ```

---

### 8.3 Chạy phân tích offline và sinh báo cáo tổng hợp

Hệ thống cung cấp sẵn các script để tự động hóa việc tổng hợp dữ liệu hoặc phân tích offline:

* **Tự động hóa chạy Heuristic Sweep bằng script bọc sẵn:**
  ```bash
  # Tự động cấu hình các biến môi trường và chạy sweep heuristic lên full dataset
  python reports/run_heuristic_sweep.py
  ```

* **Phân tích offline toàn bộ dataset:**
  ```bash
  python reports/analyze_dataset.py
  ```

* **Chạy toàn bộ pipeline tích hợp (Offline):**
  ```bash
  # Chạy phân tích toán học và vẽ biểu đồ lý thuyết (không cần khởi tạo cụm Docker)
  python reports/run_all.py
  ```

### 8.4 Kết quả đầu ra
Tất cả các tệp thống kê và báo cáo markdown so sánh sẽ được tạo ra tại thư mục `docs/` dưới dạng:
* `completeness_vs_wait_strict_<timestamp>.csv` và `.md`
* `completeness_vs_wait_heuristic_<timestamp>.csv` và `.md`

Bạn có thể copy/di chuyển các tệp này vào thư mục kết quả chính thức: **`reports/results/`** hoặc **`reports/artifacts/`**.

---

## 9. Cấu trúc thư mục và vai trò từng chương trình Python

### 9.1 Cấu trúc thư mục tổng quan

```text
csdlpt/
├── __init__.py                 # Khai báo package chính của dự án
├── run.py                      # Entry point chạy coordinator / aggregator / worker / ingestor
├── requirements.txt            # Dependency Python tối thiểu cho engine và hạ tầng
├── Dockerfile                  # Image Python dùng khi build ở cấp repo
│
├── common/                     # Thành phần dùng chung cho Strict và Heuristic
│   ├── config.py               # Đọc cấu hình từ biến môi trường
│   ├── types.py                # Dataclass / enum chung cho event, watermark, checkpoint
│   ├── window.py               # Tumbling window theo event-time
│   ├── metrics.py              # Metric nội bộ và timer độ phân giải cao
│   ├── monitoring.py           # Registry Prometheus và snapshot metric
│   ├── alerting.py             # Luật cảnh báo và PagerDuty fallback
│   ├── kafka_real.py           # Adapter Kafka thật
│   ├── rocks_store.py          # Wrapper RocksDB / RocksDict
│   ├── tiered_storage.py       # MinIO tiered storage
│   ├── differentiated_eviction.py # Chính sách eviction theo loại partition
│   ├── schema_registry.py      # Schema evolution / registry
│   ├── zk_lock.py              # ZooKeeper leader lock
│   ├── tls.py                  # TLS helper
│   ├── csdlpt.proto            # Định nghĩa protobuf gRPC
│   ├── csdlpt_pb2.py           # Python protobuf sinh tự động
│   └── csdlpt_pb2_grpc.py      # Stub / servicer gRPC sinh tự động
│
├── strict/                     # Nhánh Strict Watermark: ưu tiên 0% mất dữ liệu
│   ├── engine.py               # Engine watermark theo partition
│   ├── worker.py               # Worker quản lý nhiều partition engine
│   ├── coordinator.py          # Tính W_global = min(LW_i)
│   ├── raft_coordinator.py     # Coordinator HA mô phỏng Raft
│   ├── failover.py             # Reassign / failback partition
│   ├── output_manager.py       # Idempotent / transactional output
│   ├── backpressure.py         # Pause / resume theo queue depth
│   ├── ingestor_health.py      # Theo dõi heartbeat ingestor
│   ├── disaster_recovery.py    # Backup active window lên MinIO
│   └── replay_checkpoint.py    # Sub-checkpoint khi replay phục hồi
│
├── heuristic/                  # Nhánh Heuristic Watermark: ưu tiên latency thấp
│   ├── engine.py               # DDSketch-based per-partition watermark
│   ├── aggregator.py           # Tổng hợp W_global_h từ worker watermark
│   ├── aggregator_ha.py        # Active / standby aggregator
│   ├── dlq.py                  # Dead Letter Queue và correction protocol
│   ├── downstream_emitter.py   # Phát correction xuống downstream
│   ├── cold_start.py           # Warm-up trước khi DDSketch đủ mẫu
│   └── negative_lag.py         # Xử lý clock skew / lag âm
│
├── local_ddsketch/             # DDSketch nội bộ, không phụ thuộc package ngoài khi cần
│   ├── sketch.py               # Cài đặt DDSketch và SlidingWindowDDSketch
│   └── compat.py               # Lớp tương thích API DDSketch
│
├── reports/                    # Script chạy thí nghiệm và sinh báo cáo
│   ├── analyze_dataset.py      # Phân tích offline full dataset
│   ├── run_experiment.py       # Chạy sweep Docker Completeness vs Wait Time
│   ├── run_heuristic_sweep.py  # Wrapper sweep heuristic full dataset
│   ├── run_all.py              # Pipeline tổng hợp báo cáo
│   └── results/                # Báo cáo / CSV kết quả đã gom
│
├── tools/                      # Tiền xử lý và kiểm tra dataset
│   ├── nyc_taxi_to_events.py   # NYC Taxi -> schema CSV của engine
│   ├── make_out_of_order.py    # Inject lateness nhân tạo
│   └── dataset_stats.py        # Thống kê dataset thô
│
├── deploy/                     # Triển khai local bằng Docker Compose
│   ├── dashboard.py            # Dashboard Streamlit điều khiển cụm
│   ├── docker-compose.yml      # Cụm đầy đủ: Kafka, ZK, MinIO, coordinator, worker...
│   ├── Dockerfile              # Image runtime cho service trong compose
│   ├── entrypoint.sh           # Entrypoint container
│   ├── prometheus.yml          # Cấu hình scrape Prometheus
│   ├── prometheus-rules.yml    # Rule cảnh báo Prometheus
│   ├── grafana-dashboard.json  # Dashboard Grafana import sẵn
│   └── requirements-dashboard.txt # Dependency riêng cho Streamlit dashboard
│
├── tests/                      # Unit, integration, chaos và Docker E2E tests
├── docs/                       # Tài liệu thiết kế, báo cáo, CSV sweep và PDF minh họa
├── dataset/                    # Dữ liệu đầu vào local, không bắt buộc commit
├── .simdata/                   # Dữ liệu mô phỏng / state sinh ra khi chạy local
├── .pytest_cache/              # Cache pytest sinh tự động
└── __pycache__/                # Bytecode cache Python sinh tự động
```

> Các thư mục `__pycache__/`, `.pytest_cache/`, `.simdata/` và các file `.pyc` là dữ liệu sinh tự động khi chạy chương trình hoặc test. Chúng không phải mã nguồn chính cần chỉnh sửa.

### 9.2 Mô tả từng file `.py`

#### Gốc repo

| File | Làm gì |
|---|---|
| `__init__.py` | Khai báo package chính, mô tả ngắn hệ thống Strict + Heuristic Watermark và danh sách package export. |
| `run.py` | Entry point quan trọng nhất: parse CLI, chạy role `coordinator`, `aggregator`, `worker`, `ingestor`; dựng HTTP API, gRPC, TLS, monitoring, alerting, Kafka, failover và luồng ingest dữ liệu. |

#### `common/`

| File | Làm gì |
|---|---|
| `common/__init__.py` | Khai báo package `common`, gom ý nghĩa các module dùng chung. |
| `common/alerting.py` | Định nghĩa luật cảnh báo, đánh giá metric và gửi PagerDuty hoặc log fallback khi chạy local. |
| `common/config.py` | Đọc toàn bộ cấu hình từ biến môi trường: window size, partition, Kafka, MinIO, TLS, DDSketch, DLQ, failover, backpressure. |
| `common/csdlpt_pb2.py` | File protobuf sinh tự động từ `csdlpt.proto`, chứa message class cho heartbeat, state, Raft và watermark. |
| `common/csdlpt_pb2_grpc.py` | File gRPC sinh tự động, chứa stub/servicer cho CoordinatorService và AggregatorService. |
| `common/differentiated_eviction.py` | Chọn chiến lược eviction theo loại partition để giảm áp lực bộ nhớ/lưu trữ. |
| `common/kafka_real.py` | Adapter producer/consumer cho Kafka thật bằng `kafka-python`. |
| `common/metrics.py` | Cấu trúc metric nội bộ và timer độ phân giải cao để đo processing/network latency. |
| `common/monitoring.py` | Tạo Prometheus metric registry, cập nhật gauge/counter/histogram và snapshot phục vụ dashboard/cảnh báo. |
| `common/rocks_store.py` | Wrapper RocksDB/RocksDict cho checkpoint, DLQ, idempotency, failback state và metadata bền vững. |
| `common/schema_registry.py` | Validate JSON schema, kiểm tra backward compatibility và cung cấp schema registry/client đơn giản. |
| `common/tiered_storage.py` | Upload/download window state lên MinIO theo giao thức eviction bốn trạng thái. |
| `common/tls.py` | Tạo SSL context và bọc socket HTTP khi bật TLS. |
| `common/types.py` | Chứa enum/dataclass chung: `LogEvent`, `PunctuationToken`, `WindowResult`, heartbeat, correction, checkpoint. |
| `common/window.py` | Tính tumbling window theo event-time. |
| `common/zk_lock.py` | Leader election bằng ZooKeeper cho aggregator/coordinator HA. |

#### `strict/`

| File | Làm gì |
|---|---|
| `strict/__init__.py` | Khai báo package Strict Watermark. |
| `strict/backpressure.py` | Theo dõi queue depth từng partition và phát tín hiệu pause/resume. |
| `strict/coordinator.py` | Nhận heartbeat worker/ingestor, theo dõi partition và tính `W_global = min(LW_i)`. |
| `strict/disaster_recovery.py` | Backup active window định kỳ lên MinIO và hỗ trợ restore khi sự cố. |
| `strict/engine.py` | Engine strict theo partition: nhận event/punctuation, cập nhật local watermark, đóng window an toàn, checkpoint và eviction. |
| `strict/failover.py` | Phát hiện worker lỗi, reassign partition, lưu trạng thái failback và xử lý failback 5 bước. |
| `strict/ingestor_health.py` | Lưu heartbeat ingestor, phát hiện ingestor im lặng hoặc T_commit không đơn điệu. |
| `strict/output_manager.py` | Phát `WindowResult` theo cơ chế idempotent/transactional và chống phát trùng sau crash. |
| `strict/raft_coordinator.py` | Mô phỏng Raft leader election/state replication cho coordinator HA. |
| `strict/replay_checkpoint.py` | Ghi sub-checkpoint khi replay để recovery tiếp tục từ mốc gần nhất. |
| `strict/worker.py` | Worker strict: quản lý engine theo partition, heap buffer event-time, heartbeat, backpressure, reassignment và output. |

#### `heuristic/`

| File | Làm gì |
|---|---|
| `heuristic/__init__.py` | Khai báo package Heuristic Watermark. |
| `heuristic/aggregator.py` | Nhận watermark từng partition và tính `W_global_h`. |
| `heuristic/aggregator_ha.py` | Active/standby failover cho aggregator bằng file lock. |
| `heuristic/cold_start.py` | Quản lý giai đoạn warm-up để DDSketch đủ mẫu trước khi tin L_eff. |
| `heuristic/dlq.py` | Lưu late event vào DLQ, gom correction và phát `CorrectionMessage`. |
| `heuristic/downstream_emitter.py` | Xếp hàng, ưu tiên và phát correction xuống downstream/Kafka; đo correction latency. |
| `heuristic/engine.py` | Engine heuristic theo partition: DDSketch ước lượng lateness, tính `W_h`, đóng window sớm, xử lý late event, burst, replay, cold start và lag âm. |
| `heuristic/negative_lag.py` | Phân loại/đếm lag âm do clock skew để bảo vệ watermark và DDSketch. |

#### `local_ddsketch/`

| File | Làm gì |
|---|---|
| `local_ddsketch/__init__.py` | Export DDSketch nội bộ cho nhánh heuristic. |
| `local_ddsketch/compat.py` | Lớp tương thích API DDSketch để code/test dùng interface quen thuộc. |
| `local_ddsketch/sketch.py` | Cài đặt DDSketch logarithmic bucket và SlidingWindowDDSketch. |

#### `reports/`

| File | Làm gì |
|---|---|
| `reports/__init__.py` | Khai báo package báo cáo/thí nghiệm. |
| `reports/analyze_dataset.py` | Phân tích offline toàn bộ dataset: lateness, strict/heuristic sweep, loss theo window/partition, fitting curve, xuất Markdown/CSV. |
| `reports/run_all.py` | Chạy pipeline tổng hợp: đảm bảo dataset, chạy phân tích, tùy chọn Docker experiment và tạo comparison report. |
| `reports/run_experiment.py` | Chạy Docker Compose theo từng điểm delta/percentile, thu metric và tạo báo cáo Completeness vs Wait Time. |
| `reports/run_heuristic_sweep.py` | Wrapper đặt env phù hợp cho heuristic full dataset rồi gọi `run_experiment.py` và copy kết quả mới nhất. |

#### `tools/`

| File | Làm gì |
|---|---|
| `tools/__init__.py` | Khai báo package công cụ dataset. |
| `tools/dataset_stats.py` | In thống kê dữ liệu NYC Taxi thô: range thời gian, duration/lateness percentile, out-of-order, zone, amount. |
| `tools/make_out_of_order.py` | Tạo dataset mới với cột `arrival` bị trễ có kiểm soát để test watermark. |
| `tools/nyc_taxi_to_events.py` | Chuyển NYC Yellow Taxi Parquet/CSV sang schema engine `host,time,method,url,response,bytes,arrival`. |

#### `deploy/`

| File | Làm gì |
|---|---|
| `deploy/dashboard.py` | Dashboard Streamlit để start/stop cụm Docker, xem health, metrics, logs, bottleneck, kết quả sweep và failover lab. |

#### `tests/`

| File | Làm gì |
|---|---|
| `tests/__init__.py` | Khai báo package test. |
| `tests/check_state.py` | Script tiện ích gọi `http://127.0.0.1:9000/state` để xem state coordinator đang chạy. |
| `tests/test_backpressure.py` | Test ngưỡng pause/resume và summary của `BackpressureController`. |
| `tests/test_chaos.py` | Test chaos: worker kill, backpressure flood, clock skew, coordinator/aggregator HA, MinIO outage. |
| `tests/test_ddsketch.py` | Test DDSketch, quantile, merge, bucket clamp và sliding window. |
| `tests/test_docker_compose_chaos.py` | Runner chaos trên Docker Compose strict profile, kiểm tra node kill/recovery và reassign partition. |
| `tests/test_docker_compose_e2e.py` | Runner E2E Docker Compose cho strict và heuristic, theo dõi EOF, state và logs. |
| `tests/test_failover.py` | Test đăng ký worker, heartbeat timeout và phân phối lại partition của `FailoverManager`. |
| `tests/test_heuristic.py` | Test engine heuristic, aggregator, DLQ, correction protocol, cold start và negative lag. |
| `tests/test_infra.py` | Test ZooKeeper lock và Kafka adapter bằng mock. |
| `tests/test_integration.py` | Test tích hợp Strict vs Heuristic, end-to-end flow, chaos simulation và window logic. |
| `tests/test_output_manager.py` | Test idempotent/transactional output và mock sink. |
| `tests/test_raft_coordinator.py` | Test mô phỏng Raft coordinator HA. |
| `tests/test_schema_evolution.py` | Test JSON schema validation, compatibility và Schema Registry HTTP client. |
| `tests/test_strict.py` | Test strict engine, coordinator, priority queue và worker. |


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
  Lê Đắc Quốc Anh
</p>
