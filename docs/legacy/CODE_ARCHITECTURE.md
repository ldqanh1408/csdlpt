# KIẾN TRÚC CODE — Distributed Watermark Tracker (Refactor)

## Ánh xạ Module Python → Đặc tả Thiết kế

| Trường        | Giá trị                                                  |
|---------------|----------------------------------------------------------|
| Phiên bản     | 1.0                                                      |
| Trạng thái    | Code Architecture Reference                              |
| Phạm vi       | Toàn bộ package `refactor/`                              |
| Tham chiếu    | `Thiet_Ke_Strict_Watermark.md`, `Thiet_Ke_Heuristic_Watermark.md`, `Trien_Khai_He_Thong.md` |
| Đối tượng     | Developers, Reviewers, Onboarders                        |

---

## Mục lục

1. [Tổng quan cấu trúc thư mục](#1-tổng-quan-cấu-trúc-thư-mục)
2. [Entry Point — run.py](#2-entry-point--runpy)
3. [Package `common/` — Hạ tầng dùng chung](#3-package-common--hạ-tầng-dùng-chung)
4. [Package `strict/` — Strict Watermark Path](#4-package-strict--strict-watermark-path)
5. [Package `heuristic/` — Heuristic Watermark Path](#5-package-heuristic--heuristic-watermark-path)
6. [Package `hybrid/` — Hybrid Router](#6-package-hybrid--hybrid-router)
7. [Package `ddsketch/` — DDSketch Estimator](#7-package-ddsketch--ddsketch-estimator)
8. [Package `tests/` — Test Suite](#8-package-tests--test-suite)
9. [Luồng dữ liệu qua hệ thống](#9-luồng-dữ-liệu-qua-hệ-thống)
10. [HTTP API — Toàn bộ Endpoints](#10-http-api--toàn-bộ-endpoints)
11. [Ánh xạ Design Spec → Code Module](#11-ánh-xạ-design-spec--code-module)
12. [Biến môi trường — Toàn bộ danh sách](#12-biến-môi-trường--toàn-bộ-danh-sách)

---

## 1. Tổng quan cấu trúc thư mục

```
refactor/
├── run.py                          # Entry point — CLI dispatch 4 roles
├── __init__.py
├── common/                         # Hạ tầng dùng chung
│   ├── types.py                    #   Dataclasses + Enums toàn hệ thống
│   ├── config.py                   #   Centralized config — đọc từ env vars
│   ├── window.py                   #   TumblingWindow — gom log vào cửa sổ
│   ├── metrics.py                  #   HighResTimer, SystemMetrics (per-engine)
│   ├── monitoring.py               #   MonitoringManager — Prometheus registry
│   ├── alerting.py                 #   AlertManager — PagerDuty + rule evaluation
│   ├── rocks_store.py              #   RocksStore — RocksDB wrapper
│   ├── tiered_storage.py           #   TieredStorageManager — MinIO 4-state eviction
│   ├── differentiated_eviction.py  #   DifferentiatedEvictionManager
│   ├── kafka_sim.py                #   KafkaBroker, KafkaProducer, KafkaConsumer (HTTP sim)
│   └── tls.py                      #   TLS helpers
├── strict/                         # Strict Watermark Path (0% loss, ~15s latency)
│   ├── engine.py                   #   StrictWatermarkEngine — punctuation-based watermark
│   ├── worker.py                   #   StrictWorker — multi-partition worker
│   ├── coordinator.py              #   StrictCoordinator — W_global từ heartbeats
│   ├── raft_coordinator.py         #   RaftCoordinator — HA wrapper (Raft + ZK)
│   ├── backpressure.py             #   BackpressureController — pause/resume
│   ├── output_manager.py           #   OutputManager — exactly-once (idempotent/transactional)
│   ├── replay_checkpoint.py        #   ReplayCheckpointManager — sub-checkpoint
│   ├── failover.py                 #   FailoverManager — heartbeat + reassign
│   ├── disaster_recovery.py        #   DisasterRecovery — MinIO backup W_global
│   └── ingestor_health.py          #   IngestorHealthMonitor — 4-tier diagnosis
├── heuristic/                      # Heuristic Watermark Path (≤1% loss, ~5s latency)
│   ├── engine.py                   #   HeuristicWatermarkEngine — DDSketch-based watermark
│   ├── aggregator.py               #   HeuristicAggregator — W_global_h merge
│   ├── aggregator_ha.py            #   AggregatorHA — file-lock active-standby
│   ├── cold_start.py               #   ColdStartManager — 2-condition exit + MinIO baseline
│   ├── negative_lag.py             #   NegativeLagHandler — 4-tier diagnosis + BOO fallback
│   ├── dlq.py                      #   DLQPipeline — late event queue + correction protocol
│   └── downstream_emitter.py       #   DownstreamEmitter — correction delivery + SLA check
├── hybrid/                         # Hybrid Router (dual-path per partition)
│   └── router.py                   #   HybridRouter — CRITICAL→Strict, STANDARD→Heuristic
├── ddsketch/                       # DDSketch implementation
│   ├── sketch.py                   #   DDSketch + SlidingWindowDDSketch
│   └── compat.py                   #   pickle/JSON serialization compat
└── tests/                          # Test suite (11 files)
    ├── test_strict.py
    ├── test_heuristic.py
    ├── test_hybrid.py
    ├── test_ddsketch.py
    ├── test_backpressure.py
    ├── test_output_manager.py
    ├── test_raft_coordinator.py
    ├── test_failover.py
    ├── test_kafka_sim.py
    ├── test_chaos.py
    └── test_integration.py
```

---

## 2. Entry Point — `run.py`

File `run.py` (1643 dòng) là entry point duy nhất, dispatch 4 role:

```bash
python3 -m refactor.run --role coordinator --mode strict
python3 -m refactor.run --role aggregator
python3 -m refactor.run --role worker --mode strict|heuristic|hybrid
python3 -m refactor.run --role ingestor --source /data/events.jsonl
```

### 2.1 Role Dispatch

| Role | Hàm | Mode | Trách nhiệm chính |
|------|-----|------|-------------------|
| `coordinator` | `run_coordinator()` | strict | Tính W_global, failover, health monitor, Raft HA |
| `aggregator` | `run_aggregator()` | heuristic | Tính W_global_h từ worker watermarks, active-standby HA |
| `worker` | `run_worker()` | strict, heuristic, hybrid | Xử lý event, duy trì window state, DLQ |
| `ingestor` | `run_ingestor()` | strict | Nạp event, phát punctuation token |

### 2.2 HTTP Server dùng chung

Mọi role dùng chung `HealthHandler` (stdlib `http.server.BaseHTTPRequestHandler`):

- `GET /health`, `/ready`, `/metrics`, `/api/metrics`, `/state`
- `POST /ingest`, `/punctuation`, `/reassign`, `/raft-state`, `/raft-vote`, `/zk-vote`, `/zk-state`, `/backpressure`, `/ingestor-heartbeat`

Server state truyền qua `HealthHandler.server_state` (class variable) — dict chứa references đến component, handlers, managers.

### 2.3 Vòng đời

```
parse_args() → run_<role>() → start_http_server() → background threads → _wait_shutdown()
```

Background thread chung:
- **save_loop** (2s): checkpoint state xuống RocksDB/JSON
- **monitoring_loop** (5s): đẩy metrics vào MonitoringManager → Prometheus
- **alerting** (10s): đánh giá alert rules → PagerDuty

---

## 3. Package `common/` — Hạ tầng dùng chung

### 3.1 `types.py` — Shared Dataclasses & Enums

| Class/Enum | Vai trò | Spec |
|------------|--------|------|
| `WatermarkMode` | `STRICT` / `HEURISTIC` | Heuristic §1.1 |
| `WindowStatus` | `OPEN` / `CLOSED` | — |
| `WorkerStatus` | `ACTIVE` / `STALE` / `IDLE` / `FAILED` | Strict §5.7 |
| `EvictionState` | `CLOSED → UPLOADING → UPLOADED → PURGED` | Strict §6.4 |
| `PartitionState` | `ASSIGNED` / `REASSIGNING` / `ORPHANED` / `PAUSED` | Strict §5.8 |
| `LogEvent` | `event_id, event_time, status, payload, arrival_time` | §2 |
| `PunctuationToken` | `T_commit, partition_id, ingestor_id, is_empty` | Strict §4.4 |
| `WindowResult` | `window_id, partition_id, count, status_500, is_speculative, version` | Strict §9 |
| `CorrectionMessage` | `window_id, correction_id, delta_count, late_log_ids` | Heuristic §12.3 |
| `WorkerHeartbeat` | `worker_id, partitions, max_event_time, fencing_token` | Strict §5.3 |
| `AggregatorState` | `partition_id, W_h, status` | Heuristic §9.2 |
| `CheckpointMetadata` | `partition_id, kafka_committed_offset, active_windows, sst_files_manifest` | Strict §8.5 |
| `SketchCheckpoint` | `alpha, sub_sketches, monotonic_W_h` | Heuristic §8.7 |

### 3.2 `config.py` — Centralized Configuration

`Config` dataclass — single source of truth. Tất cả đọc từ environment variables, có spec-defined defaults. Xem [§12](#12-biến-môi-trường--toàn-bộ-danh-sách).

### 3.3 `window.py` — TumblingWindow

```
TumblingWindow(window_size_s=5.0)
  → assign(event_time) → window_start (floor to window_size_s)
  → windows_in_range(t_min, t_max) → list[window_start]
```

Không giữ state — chỉ tính boundary. State được giữ bởi engine.

### 3.4 `metrics.py` — Per-Engine Metrics

`SystemMetrics` — Gauge/Counter nội bộ: `events_processed`, `windows_closed`, `events_late`, `watermark_current`, `lag_estimate`, `queue_depth`. `summary()` → dict → `MonitoringManager.update_from_engine()`.

### 3.5 `monitoring.py` — Prometheus Metrics Registry

`MonitoringManager` bọc `prometheus_client`. Các metric chính:

| Metric | Loại | Nhãn |
|--------|------|------|
| `csdlpt_watermark_global` | Gauge | mode |
| `csdlpt_watermark_lag_seconds` | Gauge | — |
| `csdlpt_node_skew_seconds` | Gauge | worker_id |
| `csdlpt_events_total` | Counter | worker_id, partition_id, status |
| `csdlpt_windows_closed_total` | Counter | worker_id, partition_id |
| `csdlpt_events_late_total` | Counter | worker_id, partition_id |
| `csdlpt_queue_depth` | Gauge | worker_id, partition_id |
| `csdlpt_sketch_quantile_p50/p95/p99/p999` | Gauge | worker_id, partition_id |
| `csdlpt_fencing_violations_total` | Counter | — |
| `csdlpt_aggregator_leader_changes_total` | Counter | — |
| `csdlpt_backpressure_pause_total` | Counter | worker_id |
| `csdlpt_tier_storage_status` | Gauge | — |
| `csdlpt_minio_upload_errors_total` | Counter | — |
| `csdlpt_dlq_backlog` | Gauge | worker_id |
| `csdlpt_dlq_oldest_entry_age_seconds` | Gauge | worker_id |
| `csdlpt_kafka_consumer_lag` | Gauge | topic, group_id, client_id, partition |
| `csdlpt_sla_compliant_pct` | Gauge | worker_id |
| `csdlpt_replay_active` | Gauge | worker_id, partition_id |

### 3.6 `alerting.py` — Alert Evaluation + PagerDuty

`AlertManager` — 13 alert rules, đánh giá mỗi 10s, có cooldown per-rule. Gửi PagerDuty qua Events API v2.

### 3.7 `rocks_store.py` — RocksDB Wrapper

```python
store = RocksStore(db_path)
store.put("meta:W_h", str(value))
store.get_range("ow:", "ow;\xFF")  # prefix scan
```

### 3.8 `tiered_storage.py` — MinIO Tiered Storage

```
EvictionManager — in-process state machine:
  CLOSED → UPLOADING → UPLOADED → PURGED
  3 retries, exponential backoff (base 1s)

TieredStorageManager — MinIO client:
  put_object(bucket, key, data) → ETag
  get_object(bucket, key) → bytes
  delete_object(bucket, key)
```

### 3.9 `differentiated_eviction.py`

`DifferentiatedEvictionManager` — eviction policy riêng cho recovery partitions (giữ state lâu hơn trên SSD).

### 3.10 `kafka_sim.py` — Kafka Simulation (HTTP)

| Class | Vai trò |
|-------|---------|
| `KafkaBroker` | Broker HTTP server — topic, partition, consumer group offset |
| `KafkaProducer` | `send(topic, value)`, `acks=all` |
| `KafkaConsumer` | `subscribe()`, `poll()`, `commit()`, `pause()/resume()` |

---

## 4. Package `strict/` — Strict Watermark Path

**Cam kết: 0% data loss, Exactly-Once, ~15s latency.**

### 4.1 `engine.py` — StrictWatermarkEngine

```
StrictWatermarkEngine(window_size_s, delta_base_s, max_queue, checkpoint_dir,
                      tiered_storage, db_path, output_manager, diff_eviction)

  process(LogEvent):
    → TumblingWindow.assign(event.event_time)
    → Nếu event_time > W_local: buffer (out-of-order)
    → Nếu event_time <= W_local: cập nhật WindowState
    → Close windows có window_end <= W_local
    → Eviction: CLOSED → UPLOADING → UPLOADED → PURGED

  on_punctuation(PunctuationToken):
    → W_local = T_commit - delta_base
    → Drain buffer, close windows, evict

  RocksDB keys (per-partition isolate):
    "ow:{window_start}" → WindowState
    "cw:{window_start}" → WindowState (closed, chưa evict)
    "si:{event_id}"    → seen set (dedup, TTL)
    "meta:W_local"     → current watermark
```

### 4.2 `worker.py` — StrictWorker

```
StrictWorker(worker_id, partition_ids, window_size_s, delta_base_s,
             tiered_storage, db_path, output_mode, ...)

  engines: dict[partition_id → StrictWatermarkEngine]
  buffers: dict[partition_id → deque]

  process(LogEvent, pid) → engines[pid].process()
  on_punctuation(token) → engines[token.partition_id].on_punctuation()
```

Tích hợp `BackpressureController`, `ReplayCheckpointManager`, `OutputManager`.

### 4.3 `coordinator.py` — StrictCoordinator

```
StrictCoordinator(delta_base_s, state_path, db_path)

  receive_heartbeat(WorkerHeartbeat):
    → Cập nhật PartitionInfo per (worker_id, partition_id)
    → W_max = max(LW_i của Active nodes)
    → W_global = max(W_global, W_max)
    → Node Skew = W_max - LW_i

  broadcast() → {W_global, term, partitions, node_skews, fencing_violations}
```

### 4.4 `raft_coordinator.py` — RaftCoordinator (HA)

Kế thừa `StrictCoordinator`, thêm:
- Raft leader election (term-based)
- State replication sang followers
- Fencing tokens chống split-brain
- ZK mode: ZooKeeper ephemeral znodes

### 4.5 `backpressure.py` — BackpressureController

```
BackpressureController(pause_threshold=500, resume_threshold=100)
  report_buffer(worker_id, pid, size) → "pause"|"resume"|"ok"
  is_paused(pid) → bool
```

### 4.6 `output_manager.py` — OutputManager

```
OutputManager(mode="idempotent"|"transactional")
  Idempotent: dedup trên (window_id, version), TTL cache
  Transactional: 2-phase commit + audit sink mirror
```

### 4.7 `replay_checkpoint.py` — ReplayCheckpointManager

Sub-checkpoint mỗi 1000 event khi replay — resume sau crash không mất tiến độ.

### 4.8 `failover.py` — FailoverManager

```
heartbeat() → detect_failures() → reassign_failed_partitions()
  → Even redistribution + cascading failover
```

### 4.9 `disaster_recovery.py` — DisasterRecovery

Backup W_global + partition state lên MinIO mỗi 5 phút. RTO ≤ 30ph, RPO ≤ 5ph.

### 4.10 `ingestor_health.py` — IngestorHealthMonitor

W_meta_global meta-metric + 4-tier clock-skew diagnosis (NORMAL, CLOCK_DRIFT, CLOCK_SKEW, CLOCK_FAULT).

---

## 5. Package `heuristic/` — Heuristic Watermark Path

**Cam kết: ≤1% loss (steady), ≤5% (burst), ~5s latency.**

### 5.1 `engine.py` — HeuristicWatermarkEngine

```
HeuristicWatermarkEngine(partition_id, worker_id, alpha=0.01, p_normal=0.99, p_safe=0.999, ...)

  Công thức lõi:
    W_h(t) = max(W_h(t-1), max(T_event) - L_eff(t))
    L_eff(t) = sketch.quantile(p)   ← DDSketch
    p = adaptive_percentile(state)   ← p_normal / p_safe

  process(LogEvent, arrival_time):
    → sketch.insert(lag = arrival_time - event_time)
    → Cập nhật adaptive percentile state
    → Gán event vào TumblingWindow
    → Nếu window_end + L_eff < now → close window (speculative)
    → Nếu event quá muộn → late_events queue → DLQ

  Thành phần:
    - DDSketch (α=0.01, log-scale buckets)
    - SlidingWindowDDSketch (60s window, 1s granularity)
    - ColdStartManager (2-condition exit)
    - NegativeLagHandler (4-tier)
    - AdaptivePercentileController (burst detection + recovery)
```

### 5.2 `aggregator.py` — HeuristicAggregator

```
  receive_worker_watermark(worker_id, partition_id, W_h)
    → W_global_h = min(W_h của tất cả active workers)
```

### 5.3 `aggregator_ha.py` — AggregatorHA

Active-Standby qua `fcntl` file lock. Failover ≤ 2 giây.

### 5.4 `cold_start.py` — ColdStartManager

2-condition exit: `elapsed >= warmup_min_seconds AND samples >= warmup_min_samples`. Baseline lưu/load từ MinIO.

### 5.5 `negative_lag.py` — NegativeLagHandler

4-tier diagnosis: TIER_1 (clock skew < 100ms) → recalibrate, TIER_2 (< 1s) → hold W_h, TIER_3 (> 1s) → BOO fallback, TIER_4 → NTP sync.

### 5.6 `dlq.py` — DLQPipeline

```
DLQPipeline(dlq_path, retention_days=7, store)
  enqueue(LogEvent) → drain(batch_size) → compute_corrections() → list[CorrectionMessage]
  Correction patterns: incremental / replace / append-versioning
```

### 5.7 `downstream_emitter.py` — DownstreamEmitter

```
  enqueue(CorrectionMessage, window_type="normal"|"burst")
  check_sla() → {normal_window_violations, burst_window_violations, sla_compliant_pct}
  schedule_final_reconciliation() → 24h FINAL scheduler
```

---

## 6. Package `hybrid/` — Hybrid Router

### 6.1 `router.py` — HybridRouter

```
HybridRouter(partition_id, window_size_s, delta_base_s, alpha, ...)

  Kiến trúc dual-engine per partition:
    LogEvent → _resolve_priority()
                  │
         ┌────────┴────────┐
         ▼                 ▼
    CRITICAL           STANDARD
         │                 │
  StrictEngine     HeuristicEngine
         │                 │
         └────────┬────────┘
                  ▼
          close_windows()
          → Unified WindowResult
          → LossAccounting(strict_loss_pct, heuristic_loss_pct, dlq_corrected)

  Priority resolution:
    - event.payload["priority"] nếu có
    - status >= 500 → CRITICAL (default)
    - status < 500  → STANDARD (default)
```

---

## 7. Package `ddsketch/` — DDSketch Estimator

### 7.1 `sketch.py` — Core

```
DDSketch(alpha=0.01, max_buckets=1024, max_lag_accepted=3600.0)
  - Log-scale buckets: γ = (1+α)/(1-α) ≈ 1.0204
  - Bucket index = ceil(log_γ(value))
  - O(1) insert, O(1) quantile
  - Relative error guarantee: |est - true| ≤ α × true
  - Mergeable (distributed): a.merge(b) exact

  insert(value) → None
  quantile(p) → float
  merge(other) → DDSketch
  to_dict() / from_dict(d) → serialization

SlidingWindowDDSketch(window_seconds=60, sub_sketch_granularity=1)
  - 60 sub-sketch (1 per second), slide forward
  - quantile(p) trên toàn bộ sub-sketch trong window
```

### 7.2 `compat.py` — Serialization Compat

Bridge pickle cũ ↔ JSON mới — backward compatibility khi load checkpoint.

---

## 8. Package `tests/` — Test Suite

| File | Phạm vi | Loại |
|------|---------|------|
| `test_strict.py` | StrictWatermarkEngine, StrictCoordinator | Unit |
| `test_heuristic.py` | HeuristicWatermarkEngine, ColdStart, NegativeLag | Unit |
| `test_hybrid.py` | HybridRouter, LossAccounting | Unit |
| `test_ddsketch.py` | DDSketch, SlidingWindowDDSketch | Unit |
| `test_backpressure.py` | BackpressureController | Unit |
| `test_output_manager.py` | OutputManager (idempotent + transactional) | Unit |
| `test_raft_coordinator.py` | RaftCoordinator (election, replication) | Unit |
| `test_failover.py` | FailoverManager (detection, reassign) | Unit |
| `test_kafka_sim.py` | KafkaBroker, Producer, Consumer | Unit |
| `test_chaos.py` | Kill worker, kill coordinator, network partition | Chaos |
| `test_integration.py` | End-to-end: ingest → process → window close | Integration |

---

## 9. Luồng dữ liệu qua hệ thống

### 9.1 Strict Path

```
Ingestor                    Worker                      Coordinator
   │                          │                             │
   ├─ POST /ingest ──────────►│                             │
   │  (LogEvent batch)        ├─ process()                  │
   │                          │  → TumblingWindow.assign()  │
   │                          │  → Buffer (out-of-order)    │
   │                          │  → Update WindowState       │
   │                          │                             │
   ├─ POST /punctuation ─────►│                             │
   │  (PunctuationToken)      ├─ on_punctuation()           │
   │                          │  → W_local = T_commit - δ   │
   │                          │  → Drain buffer             │
   │                          │  → Close windows            │
   │                          │  → Emit WindowResult        │
   │                          │                             │
   │                          ├─ Heartbeat ────────────────►│
   │                          │  (WorkerHeartbeat)          ├─ receive_heartbeat()
   │                          │                             │  → W_global = max(LW_i)
```

### 9.2 Heuristic Path

```
Worker                      Aggregator
   │                             │
   ├─ process(LogEvent)          │
   │  → sketch.insert(lag)       │
   │  → L_eff = sketch.quantile(p)
   │  → W_h = max(T_event) - L_eff
   │  → Close window nếu hết hạn │
   │                             │
   ├─ POST /punctuation ────────►│
   │  (W_h per partition)        ├─ receive_worker_watermark()
   │                             │  → W_global_h = min(W_h)
   │                             │
   │  Late events → DLQ          │
   │  → dlq.enqueue()            │
   │  → dlq_correction_loop(1h)  │
   │  → compute_corrections()    │
   │  → downstream_emitter       │
   │  → 24h FINAL reconciliation │
```

### 9.3 Hybrid Path

```
LogEvent → HybridRouter._resolve_priority()
              │
     ┌────────┴────────┐
     ▼                 ▼
CRITICAL           STANDARD
     │                 │
StrictEngine     HeuristicEngine
     │                 │
     └────────┬────────┘
              ▼
      close_windows()
      → Unified WindowResult + LossAccounting
```

---

## 10. HTTP API — Toàn bộ Endpoints

### 10.1 Chung (mọi role)

| Method | Path | Response |
|--------|------|----------|
| GET | `/health` | `{"status":"ok","role":"..."}` |
| GET | `/ready` | `{"ready":true}` hoặc 503 |
| GET | `/metrics` | Prometheus text format |
| GET | `/api/metrics` | JSON metrics |
| GET | `/state` | Component state JSON |

### 10.2 Coordinator

| Method | Path | Request Body |
|--------|------|-------------|
| POST | `/punctuation` | WorkerHeartbeat fields |
| POST | `/ingestor-heartbeat` | `{ingestor_id, T_commit, partitions_assigned, offsets}` |
| POST | `/reassign` | `{worker_id, partitions}` |
| POST | `/raft-state` | Raft state dict |
| POST | `/raft-vote` | `{term, candidate_id, W_global}` |
| POST | `/zk-vote` | ZK vote dict |
| POST | `/zk-state` | ZK state dict |
| GET | `/failover` | Failover summary |

### 10.3 Aggregator

| Method | Path | Request Body |
|--------|------|-------------|
| POST | `/punctuation` | `{worker_id, partition_id, W_h}` |

### 10.4 Worker

| Method | Path | Request Body |
|--------|------|-------------|
| POST | `/ingest` | `[{event_id, event_time, status, partition_id}]` |
| POST | `/punctuation` | `{T_commit, partition_id, ingestor_id}` |
| POST | `/backpressure` | `{partition_id, buffer_size}` |

### 10.5 Ingestor

| Method | Path | Response |
|--------|------|----------|
| GET | `/health` | Ingestor health |
| GET | `/metrics` | Events ingested counter |

---

## 11. Ánh xạ Design Spec → Code Module

### Strict Spec → Code

| Spec Section | Code Module |
|-------------|-------------|
| §4 Windowing Logic | `common/window.py` |
| §5 Cluster Coordination | `strict/coordinator.py`, `strict/raft_coordinator.py` |
| §5.3 Heartbeat Protocol | `common/types.py:WorkerHeartbeat` |
| §5.7 Fencing Tokens | `strict/coordinator.py:_worker_fencing_tokens` |
| §5.8 Partition Reassignment | `strict/failover.py` |
| §6 State Management | `common/rocks_store.py`, `common/tiered_storage.py` |
| §6.4 Eviction State Machine | `common/tiered_storage.py:EvictionManager` |
| §6.6 Disaster Recovery | `strict/disaster_recovery.py` |
| §7 Latency Analysis | `common/metrics.py:HighResTimer` |
| §8 Robustness & Replay | `strict/replay_checkpoint.py` |
| §9 Exactly-Once Output | `strict/output_manager.py` |
| §10 Ingestor Health | `strict/ingestor_health.py` |
| §11 Backpressure | `strict/backpressure.py` |

### Heuristic Spec → Code

| Spec Section | Code Module |
|-------------|-------------|
| §5 DDSketch Core | `ddsketch/sketch.py` |
| §6 Cold Start | `heuristic/cold_start.py` |
| §7 Negative Lag | `heuristic/negative_lag.py` |
| §8 State Management | `common/rocks_store.py`, `ddsketch/compat.py` |
| §9 Aggregator HA | `heuristic/aggregator.py`, `heuristic/aggregator_ha.py` |
| §11 Adaptive Percentile | `heuristic/engine.py` (AdaptivePercentileController) |
| §12 DLQ Pipeline | `heuristic/dlq.py`, `heuristic/downstream_emitter.py` |

### Operations Spec → Code

| Spec Section | Code Module |
|-------------|-------------|
| §3 Monitoring & Alerting | `common/monitoring.py`, `common/alerting.py` |
| §5 Hybrid Architecture | `hybrid/router.py` |
| §7 Schema Evolution | `common/types.py:WindowResult.version` |
| §8 Network Partition | `strict/raft_coordinator.py` (fencing) |
| §9 Testing | `tests/` |
| §10 Security | `common/tls.py`, `run.py` TLS flags |

---

## 12. Biến môi trường — Toàn bộ danh sách

### 12.1 Core

| Biến | Default | Dùng bởi |
|------|---------|----------|
| `MODE` | `strict` | run.py |
| `ROLE` | `worker` | run.py |
| `PORT` | `8000` | run.py |
| `NODE_HOSTS` | `localhost:9101` | ingestor |
| `NODE_ID` | `0` | worker, coordinator |
| `WORKER_ID` | `$NODE_ID` | worker |
| `PARTITIONS` | `0,1,2` | worker |
| `TOTAL_PARTITIONS` | `12` | coordinator |
| `COORDINATOR_URL` | (empty) | worker |
| `AGGREGATOR_URL` | `http://localhost:8001` | worker |
| `AGGREGATOR_STANDBY_URL` | `$AGGREGATOR_URL` | worker |
| `SOURCE` | (empty) | ingestor |

### 12.2 Window & Watermark

| Biến | Default | Mô tả |
|------|---------|-------|
| `WINDOW_SIZE_S` | `5.0` | Tumbling window size (giây) |
| `DELTA_BASE_S` | `10.0` | Watermark delay cơ sở |
| `PUNCTUATION_INTERVAL_S` | `1.0` | Khoảng phát punctuation |

### 12.3 Storage

| Biến | Default | Mô tả |
|------|---------|-------|
| `CHECKPOINT_DIR` | `/data/checkpoint` | Thư mục RocksDB + JSON |
| `DB_ENABLED` | `true` | Bật RocksDB persistence |
| `MINIO_ENDPOINT` | (empty) | MinIO endpoint URL |
| `MINIO_ACCESS_KEY` | (empty) | MinIO access key |
| `MINIO_SECRET_KEY` | (empty) | MinIO secret key |
| `MINIO_BUCKET` | `csdlpt-windows` | MinIO bucket name |
| `MINIO_SECURE` | `false` | Dùng HTTPS |

### 12.4 Heuristic

| Biến | Default | Mô tả |
|------|---------|-------|
| `HEURISTIC_ALPHA` | `0.01` | DDSketch relative error |
| `HEURISTIC_P_NORMAL` | `0.99` | Percentile steady state |
| `HEURISTIC_P_SAFE` | `0.999` | Percentile burst mode |
| `HEURISTIC_L_MAX` | `60.0` | Lag tối đa (giây) |
| `HEURISTIC_WARMUP_S` | `10.0` | Warm-up duration tối thiểu |
| `HEURISTIC_WARMUP_SAMPLES` | `1000` | Warm-up samples tối thiểu |

### 12.5 DLQ

| Biến | Default | Mô tả |
|------|---------|-------|
| `DLQ_RETENTION_DAYS` | `7` | Thời gian giữ entries |
| `DLQ_RETRY_BATCH_SIZE` | `100` | Batch size correction |
| `DLQ_RETRY_INTERVAL_S` | `3600.0` | Chu kỳ correction (1h) |

### 12.6 Backpressure & Failover

| Biến | Default | Mô tả |
|------|---------|-------|
| `BACKPRESSURE_MAX_QUEUE` | `500` | Ngưỡng pause |
| `BACKPRESSURE_RESUME_AT` | `100` | Ngưỡng resume |
| `BP_PAUSE_THRESHOLD` | `500` | Override worker |
| `BP_RESUME_THRESHOLD` | `100` | Override worker |
| `HEARTBEAT_TIMEOUT_S` | `10.0` | Timeout failover |
| `FAILOVER_ENABLED` | `false` | Bật failover manager |
| `COORDINATOR_ID` | (empty) | Raft node ID |
| `COORDINATOR_PEERS` | (empty) | Raft peer list |
| `ZK_ENSEMBLE` | `false` | Dùng ZK thay Raft |
| `AGGREGATOR_HA_ENABLED` | `false` | Bật aggregator HA |
| `AGGREGATOR_LOCK_PATH` | `/tmp/aggregator.lock` | File lock path |

### 12.7 Monitoring & Alerting

| Biến | Default | Mô tả |
|------|---------|-------|
| `METRICS_ENABLED` | `true` | Bật Prometheus metrics |
| `ALERT_INTERVAL_S` | `10.0` | Chu kỳ alert |
| `PAGERDUTY_ROUTING_KEY` | (empty) | PagerDuty key |

### 12.8 Security & DR

| Biến | Default | Mô tả |
|------|---------|-------|
| `TLS_CERT_FILE` | (empty) | TLS certificate path |
| `TLS_KEY_FILE` | (empty) | TLS private key path |
| `DR_BACKUP_INTERVAL_S` | `300` | Backup interval |

### 12.9 Feature Flags

| Biến | Default | Mô tả |
|------|---------|-------|
| `ENABLE_ADAPTIVE_PERCENTILE` | `true` | Adaptive p switching |
| `ENABLE_REPLAY_SUB_CHECKPOINTING` | `true` | Sub-checkpoint replay |
| `ENABLE_TWO_PHASE_EVICTION` | `true` | Differentiated eviction |
| `ENABLE_NEGATIVE_LAG_RECALIBRATION` | `true` | Auto-recalibrate |
| `ENABLE_HYBRID_ROUTING` | `false` | Bật hybrid mode |

### 12.10 Kafka Simulation

| Biến | Default | Mô tả |
|------|---------|-------|
| `KAFKA_BROKER_URL` | (empty) | Kafka broker endpoint |
| `KAFKA_PORT` | `9092` | Broker HTTP port |
| `REPLAY_CKPT_INTERVAL` | `1000` | Events per sub-checkpoint |
| `OUTPUT_MODE` | `idempotent` | `idempotent` / `transactional` |

---

## Tham khảo nhanh — Bắt đầu phát triển

```bash
# Chạy Strict mode (coordinator + worker + ingestor)
python3 -m refactor.run --role coordinator --mode strict --port 8000 &
python3 -m refactor.run --role worker --mode strict --port 8001 --partitions 0,1,2,3 &
python3 -m refactor.run --role ingestor --mode strict --source /data/events.jsonl &

# Chạy Heuristic mode (worker + aggregator)
python3 -m refactor.run --role aggregator --port 8001 &
python3 -m refactor.run --role worker --mode heuristic --port 8002 --partitions 0,1,2,3 &

# Chạy Hybrid mode
python3 -m refactor.run --role worker --mode hybrid --port 8003 --partitions 0,1,2,3 &

# Chạy test
python3 -m pytest refactor/tests/ -v

# Với Kafka simulation
python3 -m refactor.run --role worker --mode strict --enable-kafka --kafka-broker-url http://localhost:9092 &

# Với TLS
python3 -m refactor.run --role coordinator --mode strict --tls-cert cert.pem --tls-key key.pem
```

---

| Phiên bản | Ngày | Thay đổi |
|-----------|------|----------|
| 1.0 | 2026-05-25 | Tài liệu ban đầu — ánh xạ toàn bộ codebase refactor/ |
