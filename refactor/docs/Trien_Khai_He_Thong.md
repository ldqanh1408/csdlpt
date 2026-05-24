# TÀI LIỆU TRIỂN KHAI HỆ THỐNG

## Triển khai toàn diện Stateful Stream Processing — Strict + Heuristic + Hybrid

| Trường        | Giá trị                                                  |
|---------------|----------------------------------------------------------|
| Phiên bản     | 1.0                                                      |
| Trạng thái    | Implementation Guide                                     |
| Phạm vi       | Vận hành — Capacity, Monitoring, Deployment, Testing, Runbook |
| Tham chiếu    | [Thiết kế Strict Watermark], [Thiết kế Heuristic Watermark + DDSketch] |
| Đối tượng     | DevOps, SRE, Architects, On-call Engineers               |

---

## Mục lục

1. [Tổng quan triển khai và Lộ trình 4 phase](#1-tổng-quan-triển-khai-và-lộ-trình-4-phase)
2. [Capacity Planning](#2-capacity-planning)
3. [Monitoring & Alerting Matrix](#3-monitoring--alerting-matrix)
4. [Operational Runbook](#4-operational-runbook)
5. [Kiến trúc Hybrid (Strict + Heuristic)](#5-kiến-trúc-hybrid-strict--heuristic)
6. [Deployment Strategy](#6-deployment-strategy)
7. [Schema Evolution](#7-schema-evolution)
8. [Network Partition Handling](#8-network-partition-handling)
9. [Testing Strategy](#9-testing-strategy)
10. [Security Checklist](#10-security-checklist)
11. [Pre-Production Checklist](#11-pre-production-checklist)
12. [Post Go-Live Plan](#12-post-go-live-plan)

---

## 1. Tổng quan triển khai và Lộ trình 4 phase

### 1.1. Mục tiêu triển khai

Đưa hệ thống Stateful Stream Processing dựa trên Strict Watermark và Heuristic Watermark + DDSketch vào production an toàn, có monitoring đầy đủ, và team Ops/SRE có đủ runbook để vận hành.

### 1.2. Lộ trình 4 phase

#### Phase 1 — Foundation & PoC (4 tuần)

**Mục tiêu**: Verify thiết kế đã giải quyết được các rủi ro chính qua PoC scale 1/10.

**Deliverables**:
- Team đọc và hiểu đầy đủ 2 tài liệu thiết kế.
- Triển khai PoC staging: 1 Coordinator HA cluster, 2 Worker, 4 Partitions, Strict mode.
- Verify chaos test cơ bản (kill Worker, kill Coordinator).
- Lập capacity baseline (§2).
- Triển khai Heuristic PoC riêng với 1 Worker.

**Exit criteria**: 72h chaos test, 0% data loss với Strict.

#### Phase 2 — Pilot (4 tuần)

**Mục tiêu**: Chạy production-like với shadow traffic.

**Deliverables**:
- Triển khai full scale staging: Coordinator HA (3 nodes), 4 Worker, 12 Partitions.
- Bật Strict path, shadow traffic từ production.
- Triển khai Monitoring & Alerting (§3).
- Triển khai Operational Runbook (§4) — train team.
- Hoàn thành Load test + Replay test (§9).

**Exit criteria**: 2 tuần liên tục không có incident severity cao.

#### Phase 3 — Production Rollout (4 tuần)

**Mục tiêu**: Bật Strict cho production, sau đó bật Heuristic.

**Deliverables**:
- Tuần 1-2: Canary Strict (10% → 50% → 100%).
- Tuần 3: Bật Heuristic cho dashboard use case.
- Tuần 4: Kích hoạt Hybrid Architecture (§5).

**Exit criteria**: Cả 2 path đạt SLA trong 1 tuần.

#### Phase 4 — Optimization & Steady State (ongoing)

**Activities**:
- Tinh chỉnh tham số (δ_base, α, p) theo dữ liệu thực.
- Khắc phục lỗ hổng "Trung bình" còn lại theo priority.
- Chaos test hàng quý.
- Capacity review hàng tháng.

---

## 2. Capacity Planning

### 2.1. Input Required

Trước khi sizing, cần đo:

| Tham số                | Cách đo                                           |
|------------------------|---------------------------------------------------|
| Peak log rate (logs/s) | Đo từ Web Server log giờ cao điểm                 |
| Average log size       | Sampling từ log thực tế                           |
| Burst factor           | peak / average ratio                              |
| Late arrival rate      | Từ shadow run                                      |
| Lag distribution       | P50, P95, P99 từ shadow run                       |

**Ví dụ baseline**: 100k logs/s, 500 bytes/log, burst 3x, late rate 0.5%, P99 lag = 3s.

### 2.2. Sizing Worker Nodes

**RAM per Worker**:

```
RAM_worker = 
    asyncio_queue (500 × log_size)
  + RocksDB block cache (64MB × N_partitions_per_worker)
  + DDSketch state (50KB × N_partitions_per_worker)   [Heuristic only]
  + Snapshot Manager (300KB × N_partitions)            [Heuristic only]
  + Active Window state MemTable (avg 100MB)
  + JVM/Python overhead (1GB)

Ví dụ: 3 partitions/worker
  = 250KB + 192MB + 150KB + 900KB + 100MB + 1024MB
  ≈ 1.3 GB
```

**Đề xuất Worker**: **4 GB RAM** (headroom 3x).

**SSD per Worker (Tier 1)**:

```
Steady state: 1-2 GB.
Peak (replay scenarios): 5-10 GB.
```

**Đề xuất Worker SSD**: **50 GB NVMe**.

### 2.3. Sizing Shared Volume (Tier 2)

```
Disk_tier2 = N_partitions × avg_checkpoint_size × N_recent_checkpoints
           ≈ 12 × 500MB × 3 = 18 GB
```

**Đề xuất Shared Volume**: **100 GB SSD** (network-attached, IOPS guaranteed).

### 2.4. Sizing Kafka Cluster

```
Kafka_throughput = peak_log_rate × burst × log_size
                 = 100k × 3 × 500B = 150 MB/s

Kafka_disk = Kafka_in × 7 days × replication_factor
           = 150MB/s × 86400 × 7 × 3
           ≈ 270 TB total
```

**Đề xuất**: 3 brokers, retention 7 ngày, replication 3. **128 TB NVMe per broker**.

### 2.5. Sizing Coordinator Cluster (Strict)

| Tài nguyên | Đề xuất per node          |
|------------|---------------------------|
| Instance   | 3 (Raft quorum)           |
| RAM        | 2 GB                      |
| Disk       | 20 GB (Raft log)          |
| CPU        | 2 cores                   |
| Network    | 1 Gbps trở lên            |

### 2.6. Sizing Aggregator Cluster (Heuristic)

| Tài nguyên | Đề xuất per node          |
|------------|---------------------------|
| Instance   | 2 (Active-Standby)        |
| RAM        | 2 GB                      |
| Disk       | 10 GB                     |
| CPU        | 1 core                    |

### 2.7. MinIO Sizing

```
Cold State growth = N_partitions × Closed Windows/day × avg_window_size
                  ≈ 12 × (86400/5) × 50KB
                  ≈ 100 GB/day per partition
                  = 1.2 TB/day total

Active backup (DR) = N_partitions × snapshot_size × 24h/5min
                   ≈ 12 × 500MB × 288
                   ≈ 1.7 TB rolling
```

**Đề xuất**: 5 TB/tháng starting, scale theo retention.

### 2.8. Capacity Worksheet

| Component                | Qty | Per-unit                  | Total              |
|--------------------------|-----|---------------------------|--------------------|
| Worker Node              | 4   | 4GB RAM / 50GB SSD        | 16GB / 200GB       |
| Coordinator (Strict HA)  | 3   | 2GB RAM / 20GB            | 6GB / 60GB         |
| Aggregator (Heuristic)   | 2   | 2GB RAM / 10GB            | 4GB / 20GB         |
| Kafka Broker             | 3   | 32GB RAM / 128TB          | 96GB / 384TB       |
| Shared Volume            | 1   | 100GB SSD network         | 100GB              |
| ZooKeeper Ensemble       | 3   | 2GB RAM / 10GB            | 6GB / 30GB         |
| MinIO                    | -   | -                         | 5 TB/tháng         |

Tổng: ~128GB RAM, ~410GB SSD, ~384TB Kafka storage, ~5TB MinIO.

---

## 3. Monitoring & Alerting Matrix

### 3.1. Cấu trúc 4 Tier Golden Signals

#### Tier 1 — System Health (mọi user phải watch)

| Metric                          | OK             | Warning     | Critical    | Action               |
|---------------------------------|----------------|-------------|-------------|----------------------|
| `data_loss_rate` (Strict)       | 0%             | -           | > 0%        | Page on-call         |
| `data_loss_rate` (Heuristic)    | ≤ 0.1%         | 0.1–1%      | > 1%        | Page on-call         |
| `watermark_lag` (Strict)        | < 12s          | 12–30s      | > 60s       | Investigate          |
| `watermark_lag` (Heuristic)     | < 5s           | 5–15s       | > 30s       | Investigate          |
| `coordinator_leader_changes/h`  | 0              | 1–2         | > 5         | Investigate          |
| `aggregator_leader_changes/h`   | 0              | 1–2         | > 5         | Investigate          |
| `worker_alive_count`            | 4              | 3           | ≤ 2         | Page                 |

#### Tier 2 — Component Health

| Metric                          | OK             | Warning     | Critical    |
|---------------------------------|----------------|-------------|-------------|
| `node_skew_max_ms`              | < 1000         | 1000–5000   | > 5000      |
| `backpressure_pause_rate/min`   | < 1            | 1–10        | > 10        |
| `dlq_lag_seconds` (Heuristic)   | < 60           | 60–600      | > 3600      |
| `tiered_eviction_failure_rate`  | 0%             | < 1%        | > 1%        |
| `non_monotonic_punctuation/min` | 0              | < 1         | > 5         |
| `ingestor_silent_count`         | 0              | -           | > 0         |

#### Tier 3 — Resource Health

| Metric                          | OK             | Warning     | Critical    |
|---------------------------------|----------------|-------------|-------------|
| `worker_ram_usage_pct`          | < 70%          | 70–85%      | > 85%       |
| `worker_disk_usage_pct`         | < 60%          | 60–80%      | > 80%       |
| `kafka_partition_lag`           | < 1000         | 1000–10000  | > 100000    |
| `shared_volume_iops_used`       | < 70%          | 70–85%      | > 85%       |
| `minio_upload_lag_seconds`      | < 60           | 60–600      | > 900       |

#### Tier 4 — Heuristic-Specific

| Metric                          | OK             | Warning     | Critical    |
|---------------------------------|----------------|-------------|-------------|
| `sketch_quantile_p99_ms`        | < 1000         | 1000–5000   | > 5000      |
| `sketch_total_count`            | > 1000         | 100–1000    | < 100       |
| `negative_lag_rate_pct`         | < 0.1%         | 0.1–1%      | > 1%        |
| `replay_mode_active`            | 0 workers      | 1 worker    | ≥ 2 workers |
| `adaptive_percentile_active`    | -              | track only  | -           |

### 3.2. Alert Severity Levels

| Level     | Response Time | Channel                    | Example                                |
|-----------|---------------|----------------------------|----------------------------------------|
| Critical  | ≤ 5 phút      | PagerDuty + Phone call     | data_loss > 0, all workers down        |
| High      | ≤ 30 phút     | PagerDuty                  | worker_skew > 5000ms                   |
| Warning   | ≤ 4 giờ       | Slack channel              | dlq_lag > 600s                         |
| Info      | Best effort   | Email digest               | Non-monotonic punctuation observed     |

### 3.3. Dashboard Layout

**Page 1 — Executive Summary**: data_loss_rate, throughput, watermark_lag, worker_alive_count.

**Page 2 — Per-Partition Detail**: từng partition status, LW_i, Skew, Backpressure events.

**Page 3 — Strict Specific**: Coordinator HA status, Tiered Eviction, Failback events.

**Page 4 — Heuristic Specific**: sketch quantiles, DLQ status, correction emission rate, Aggregator HA.

**Page 5 — Resources**: RAM/CPU/Disk/Network của Worker, Kafka, Coordinator/Aggregator.

### 3.4. Suggested Tooling

- **Metrics**: Prometheus + Grafana.
- **Logs**: ELK / Loki.
- **Traces**: Jaeger / Tempo (distributed tracing).
- **Alerting**: PagerDuty / Opsgenie.
- **Audit log**: Stored ≥ 90 ngày, immutable.

---

## 4. Operational Runbook

### 4.1. Top 5 Sự cố thường gặp

#### Sự cố A — "Data loss rate > 0 trong Strict mode"

**Symptoms**: Critical alert fired, downstream nhận thiếu dữ liệu.

**Diagnosis**:
1. Check `watermark_lag` — có spike không?
2. Check `non_monotonic_punctuation` count — clock skew?
3. Check Ingestor heartbeat (xem §10 Strict design) — Ingestor sống?
4. Check Worker log — có `T_event < W_global` lúc nhận?

**Mitigation**:
- Clock skew → fix NTP, restart Ingestor.
- Ingestor stuck → restart Ingestor.
- Data đã mất → restore từ Kafka replay (nếu trong retention) hoặc log gốc.

#### Sự cố B — "Coordinator leader changes thường xuyên"

**Symptoms**: > 5 leader changes/hour, intermittent service degradation.

**Diagnosis**:
1. Check ZK/Raft logs.
2. Check Coordinator node CPU/Network.
3. Check network connectivity giữa Coordinator instances.

**Mitigation**:
- Tăng heartbeat timeout (3s → 5s).
- Cô lập network noise.
- Restart noisy node.

#### Sự cố C — "Worker bị OOM"

**Symptoms**: Worker crash với OOM error.

**Diagnosis**:
1. Check `backpressure_pause_rate` — pause kịp không?
2. Check `worker_ram_usage_pct` trước crash.
3. Check Window count active per partition.

**Mitigation**:
- Giảm queue maxsize từ 500 xuống 300.
- Giảm RocksDB block cache.
- Scale up Worker RAM.

#### Sự cố D — "Heuristic DLQ backlog tăng nhanh"

**Symptoms**: dlq_lag > 1 giờ, alert critical.

**Diagnosis**:
1. Check DLQ consumer alive.
2. Check sketch P99 — ước lượng thấp?
3. Check incoming traffic — burst?

**Mitigation**:
- Restart DLQ consumer nếu stuck.
- Bump percentile lên P99.9 manually (kích hoạt adaptive).
- Scale DLQ consumer parallelism.

#### Sự cố E — "Cụm replay không kết thúc (stuck recovering)"

**Symptoms**: Node 3 trong Replay-Mode hơn 30 phút.

**Diagnosis**:
1. Check Kafka throughput from Worker 3.
2. Check disk I/O của Worker 3.
3. Check Worker log — có `T_event < W_global` lúc nhận?
4. Check Kafka consumer offset của Worker 3.
5. Check disk I/O của Worker 3.

**Mitigation**:
- Nếu IO bottleneck → migrate to faster disk.
- Nếu Kafka consumer slow → tăng `fetch.max.bytes`.
- Last resort: skip replay, accept data loss với documented incident.

### 4.2. Maintenance Procedures

#### Procedure 1 — Graceful Worker Restart

```
1. Notify Coordinator: pause new assignments to Worker X.
2. Wait Worker X drain in-flight Windows (~30s).
3. Trigger Strict Failback — transfer partitions to other workers.
4. Stop Worker X.
5. Update binary / config.
6. Start Worker X.
7. Wait warm-up (Heuristic: §6).
8. Coordinator gradually reassign partitions back.
```

#### Procedure 2 — Coordinator Rolling Upgrade

```
1. Upgrade follower 1 → wait sync → verify.
2. Upgrade follower 2 → wait sync → verify.
3. Trigger leader election (manually fail current leader).
4. New leader = upgraded version.
5. Upgrade old leader → joins as follower.
```

#### Procedure 3 — Schema Migration

Xem §7.

#### Procedure 4 — Disaster Recovery (Shared Volume mất)

```
1. Confirm Shared Volume unrecoverable.
2. Stop all Workers.
3. Provision new Shared Volume.
4. Restore từ MinIO active_state_backup mới nhất per-partition:
   mc cp --recursive minio/bucket/strict-watermark/active_state_backup/ /data/checkpoint/
5. Verify metadata consistency.
6. Restart cluster.
7. Monitor recovery (replay-mode).
```

RTO ≤ 30 phút. RPO ≤ 5 phút.

---

## 5. Kiến trúc Hybrid (Strict + Heuristic)

### 5.1. Khi nào dùng Hybrid

Khi tổ chức có **cả 2 loại consumer**:
- Critical (billing, audit, compliance) → cần Strict (0% loss).
- Real-time (dashboard, alerting, ML features) → cần Heuristic (low latency).

### 5.2. Kiến trúc Hybrid

```
                    ┌──────────────────────────────────┐
                    │       Web Server Log Stream       │
                    └────────────────┬─────────────────┘
                                     │
                    ┌────────────────▼────────────────┐
                    │    Kafka Cluster (shared)        │
                    │    12 partitions                 │
                    └────────────────┬─────────────────┘
                                     │
            ┌────────────────────────┴────────────────────────┐
            ▼                                                  ▼
┌────────────────────────────┐                  ┌──────────────────────────────┐
│  STRICT PATH                │                  │  HEURISTIC PATH               │
│                             │                  │                               │
│  Worker Cluster (Strict)    │                  │  Worker Cluster (Heuristic)   │
│  - Punctuation Token        │                  │  - DDSketch                   │
│  - Coordinator HA (Raft)    │                  │  - Aggregator HA (Standby)    │
│  - δ_base = 10s             │                  │  - p = 0.99                   │
│  - Transactional Output     │                  │  - DLQ + Correction           │
│                             │                  │                               │
│  Latency: ~15s              │                  │  Latency: ~5s                 │
│  Loss: 0%                   │                  │  Loss: ≤ 0.1% (expected)      │
└────────────┬────────────────┘                  └──────────────┬────────────────┘
             │                                                  │
             ▼                                                  ▼
   [Strict Results Topic]                          [Heuristic Results Topic]
   Schema: { is_final: true }                      Schema: { is_speculative: true }
             │                                                  │
             ▼                                                  ▼
   [Billing / Audit Sink]                          [Dashboard / Alert / ML]
   [Compliance Reports]                            [Real-time KPI]
```

### 5.3. Resource Sharing

- **Shared**: Kafka cluster, Ingestor, Shared Volume (separate paths), MinIO (separate prefixes), ZooKeeper.
- **Separate**: Worker pools (RocksDB instance khác biệt, tham số khác).
- **Separate**: Coordinator (Strict) vs Aggregator (Heuristic).

### 5.4. Consumer Routing

| Consumer Type      | Source Topic                            | Lý do                              |
|--------------------|-----------------------------------------|------------------------------------|
| Billing            | strict_results                           | Cần 0% loss                        |
| Audit log          | strict_results                           | Compliance                         |
| Financial recon    | strict_results                           | Audit-grade                        |
| Dashboard          | heuristic_results                        | Cần latency thấp                   |
| Alert engine       | heuristic_results                        | Real-time critical                 |
| ML feature store   | heuristic_results + late_logs_dlq        | Realtime + correction              |

### 5.5. Cost Analysis

Hybrid tốn ~1.7x cost của một path đơn lẻ (không phải 2x vì share Kafka/storage):

| Component           | Single Path | Hybrid     | Delta  |
|---------------------|-------------|------------|--------|
| Worker Nodes        | 4           | 8          | +4     |
| Coordinator         | 3           | 3          | -      |
| Aggregator          | 0 (Strict)  | 2          | +2     |
| Kafka               | shared      | shared     | -      |
| Storage             | 100 GB      | 150 GB     | +50    |
| MinIO               | 5 TB        | 6 TB       | +1 TB  |

Justify Hybrid khi: business value của cả 2 path > 1.7x cost của 1 path.

---

## 6. Deployment Strategy

### 6.1. Blue-Green Deployment (Major version)

```
1. Deploy v_new vào "Green" environment (identical to "Blue" production).
2. Shadow traffic: copy Kafka offsets to Green, không emit downstream.
3. Compare results Green vs Blue cho N giờ.
4. Switch consumer pointer từ Blue's output sang Green's.
5. Monitor 24h.
6. Decommission Blue.
```

### 6.2. Canary Deployment (Minor changes)

```
1. Deploy v_new vào 1 worker (25% traffic).
2. Monitor metrics đặc thù 1 giờ.
3. OK → 50% traffic.
4. Sau 1 giờ → 100% traffic.
5. Rollback nếu alert fire.
```

### 6.3. Rollback Plan

Mỗi deploy phải có rollback procedure:

- **Binary rollback**: keep previous version trong registry, deploy lại.
- **Config rollback**: keep N versions trong Git, revert commit.
- **Data rollback**: nếu Window kết quả đã emit downstream → cần correction message.

### 6.4. Feature Flags

Sử dụng feature flag cho các cơ chế phức tạp:

- `enable_adaptive_percentile` (Heuristic)
- `enable_replay_sub_checkpointing` (Strict)
- `enable_two_phase_eviction` (Strict)
- `enable_negative_lag_recalibration` (Heuristic)
- `enable_hybrid_routing`

Cho phép turn off/on mà không cần re-deploy.

### 6.5. Configuration Management

- Centralized config: Consul, etcd, hoặc Kubernetes ConfigMap.
- Versioned: lưu trong Git, audit changes.
- Validation: schema check trước khi apply.
- Rollback: revert config commit + restart.

---

## 7. Schema Evolution

### 7.1. Vấn đề

Log format thay đổi (thêm field, đổi type, đổi name). Cần backward compatibility.

### 7.2. Schema Registry

Triển khai Schema Registry (Confluent Schema Registry hoặc tương đương):

- Mỗi log có `schema_version` field.
- Registry lưu lịch sử schema với compatibility rules.
- Worker đọc schema_version để chọn parser phù hợp.

### 7.3. Compatibility Rules

| Loại thay đổi              | Compatibility          | Action                                 |
|-----------------------------|------------------------|----------------------------------------|
| Thêm optional field         | Backward compatible    | OK, deploy normally                    |
| Thêm required field         | Breaking               | Cần migration plan                     |
| Đổi type                    | Breaking               | Cần migration plan                     |
| Xóa field                   | Breaking nếu downstream dùng | Cần migration plan               |
| Đổi tên field               | Breaking               | Dùng alias support                     |

### 7.4. Migration Pattern (Breaking Change)

**Bước 1 — Dual write**: Ingestor phát cả v1 và v2 trong N ngày.

**Bước 2 — Dual consume**: Worker biết parse cả 2 versions.

**Bước 3 — Switch primary**: Worker default sang v2.

**Bước 4 — Drop v1**: sau khi xác nhận downstream không cần v1 nữa.

### 7.5. Validation

- Test schema change trong staging với replay test (§9.4).
- Verify backward compatibility với log sample lịch sử.
- Monitor parser error rate sau deploy.

---

## 8. Network Partition Handling

### 8.1. Scenario

Network partition chia cluster thành 2 phía:
- Phía A: Coordinator Leader + Worker 1, 2.
- Phía B: Coordinator Follower + Worker 3, 4.

Phía B nghĩ Leader chết → election → new leader bên B. Hai leader cùng tồn tại → **split-brain**.

### 8.2. Quorum-based Election

Yêu cầu majority để elect leader:
- Cluster 3 Coordinator → cần 2/3 ack.
- Phía A có 1 instance, Phía B có 2 instance → chỉ phía B có quorum → đúng 1 leader.

### 8.3. Fencing Token

Mỗi command có term number. Worker reject command term thấp.

Khi network heal:
- Phía A's old leader detect term mới cao hơn → step down.
- Workers phía A reconnect to new leader.
- State được reconcile qua Raft log.

### 8.4. Data Loss Window

Trong partition, phía minority có thể đã accept log mà không replicate → mất.

**Mitigation**: Kafka `acks=all` + `min.insync.replicas=2` → log không "accepted" nếu không replicate đủ.

### 8.5. Recovery Steps

1. Network heal.
2. Minority side detect higher term → step down.
3. Workers reconnect.
4. Replicated state sync.
5. Resume normal operation.

---

## 9. Testing Strategy

### 9.1. Unit Tests

- Watermark calculation correctness (Strict + Heuristic).
- DDSketch quantile accuracy với golden dataset.
- Window assignment edge cases (boundary, leap second).
- Idempotent filter dedup.
- Fencing token validation.
- Snapshot rollback logic.

**Coverage target**: ≥ 80% cho core logic.

### 9.2. Integration Tests

- End-to-end Strict path với synthetic data có known late arrival.
- Heuristic path với known lag distribution → verify expected loss rate.
- Tiered eviction state transitions.
- DLQ Correction protocol with downstream sink.

### 9.3. Chaos Engineering

**Tests bắt buộc trước Production**:

| Test                          | Scenario                                       | Expected Result                     |
|-------------------------------|------------------------------------------------|-------------------------------------|
| Random Worker Kill            | Kill 1 worker mỗi 5 phút                       | 0% data loss (Strict)               |
| Cascading Worker Failure      | Kill 2 workers đồng thời                       | Failover to 2 remaining workers     |
| Coordinator Leader Kill       | Kill leader, observe new leader election       | < 5s RTO                            |
| Aggregator Leader Kill        | Kill aggregator leader                         | < 2s RTO                            |
| Coordinator Cluster Partition | Partition coordinator 1+2 vs 3                 | Majority side keeps leadership      |
| Shared Volume Latency Spike   | Inject 5s latency to /data/checkpoint          | Backpressure activates, no OOM      |
| Kafka Broker Failure          | Kill 1 broker                                  | Continues with replica brokers      |
| Ingestor Stuck                | Stop Ingestor heartbeat                        | Alert fires within 15s              |
| Clock Skew Inject             | Set Ingestor clock +5s                         | Non-monotonic punctuation rejected  |
| Network Partition             | Network split workers from coordinator         | Reconnect after heal, no split-brain|
| MinIO Outage                  | Block MinIO access                              | Alert after 15min, Tier 2 accumulates|
| Disk Full (Worker)            | Fill Worker disk to 99%                        | Backpressure + graceful degradation |

**Tool**: Chaos Mesh, Litmus, hoặc custom scripts.

### 9.4. Replay Testing

Replay historical log với known correct results:

1. Capture 1 ngày log production.
2. Feed vào staging.
3. Compare output với batch-computed ground truth.

**Acceptance**:
- Strict: ≥ 99.9999% match (chỉ accept difference do precision).
- Heuristic: ≥ 99% match (sai số do expected loss).

### 9.5. Load Testing

Ramp up load step-wise:
- 1x baseline → measure (basic correctness).
- 2x baseline → measure (capacity headroom).
- 5x baseline (peak burst) → measure (stress).
- 10x baseline (extreme) → measure breakpoint.

**Metrics theo dõi**: throughput, end-to-end latency p50/p99/p999, error rate, resource utilization.

**SLA verification**:
- Strict: 0% loss tại 5x burst.
- Heuristic: ≤ 5% loss tại 5x burst (adaptive activated).

### 9.6. Continuous Testing

Sau go-live:
- Daily smoke test (1 min, basic functionality).
- Weekly replay test (1 hour data).
- Monthly load test (full ramp).
- Quarterly chaos test (full chaos matrix).

---

## 10. Security Checklist

### 10.1. Network Security

- [ ] TLS giữa Worker ↔ Kafka.
- [ ] TLS giữa Worker ↔ Coordinator/Aggregator.
- [ ] TLS giữa Worker ↔ MinIO.
- [ ] Firewall rules: Worker chỉ accessible from Coordinator/Kafka.
- [ ] VPC isolation cho production cluster.
- [ ] Ingestor heartbeat endpoint behind authenticated gateway.

### 10.2. Authentication & Authorization

- [ ] Kafka SASL/SCRAM cho Worker auth.
- [ ] MinIO access policy (least privilege).
- [ ] Coordinator API auth (mTLS).
- [ ] Operations runbook access requires MFA.
- [ ] ZooKeeper ACL configured.

### 10.3. Data Protection

- [ ] RocksDB encryption at rest (LUKS, dm-crypt).
- [ ] MinIO bucket encryption (SSE-KMS).
- [ ] Kafka topic encryption.
- [ ] Log content PII handling (tokenization nếu chứa PII).
- [ ] Shared Volume encrypted.

### 10.4. Audit

- [ ] Coordinator action audit log (immutable, ≥ 90 ngày).
- [ ] Failover events logged.
- [ ] Configuration changes logged.
- [ ] Operations team access logged.

### 10.5. Compliance

Tùy domain:
- GDPR: data retention, right to be forgotten.
- PCI-DSS: nếu log liên quan thanh toán.
- SOC 2: audit trail đầy đủ.
- HIPAA: nếu log y tế.
- Vietnamese Cybersecurity Law: data localization.

---

## 11. Pre-Production Checklist

Trước khi go-live, verify đầy đủ:

### 11.1. Strict Path

- [ ] Coordinator HA cluster deployed (3 instance) + tested failover < 5s.
- [ ] Output Exactly-Once implemented (Transactional sink hoặc Idempotent key) + verified.
- [ ] Tiered Storage 4-state eviction protocol tested.
- [ ] Strict Failback State Machine verified với Coordinator leader switch.
- [ ] Idempotent Filter TTL working.
- [ ] Replay Sub-Checkpointing tested.
- [ ] DR procedure tested (restore từ MinIO active_state_backup).
- [ ] Heartbeat Punctuation health monitoring active.
- [ ] Clock Skew alert configured.
- [ ] Watermark Lag metric exposed + alerted.

### 11.2. Heuristic Path

- [ ] Aggregator HA deployed (2 instance, ZK lock).
- [ ] Cold Start Strategy verified với cold restart test.
- [ ] DDSketch state size bounded (max_buckets, MAX_LAG_ACCEPTED).
- [ ] Adaptive Percentile triggered + verified in burst test.
- [ ] Negative Lag handler tested.
- [ ] DLQ pipeline working end-to-end.
- [ ] Correction protocol verified với downstream consumer.
- [ ] Snapshot Manager + Rollback tested.
- [ ] All 10 Operational Mandates implemented.

### 11.3. Cluster-wide

- [ ] Capacity planning completed, headroom ≥ 30%.
- [ ] All monitoring dashboards live.
- [ ] Alert paging configured + tested.
- [ ] Runbook trained với on-call team.
- [ ] All chaos tests passed.
- [ ] Replay test ≥ 99.9999% match (Strict) / ≥ 99% (Heuristic).
- [ ] Load test passes 5x burst.
- [ ] Security checklist completed.
- [ ] Schema registry deployed.
- [ ] Feature flags configured.
- [ ] Rollback procedure documented + tested.
- [ ] Disaster Recovery plan documented + drilled.

### 11.4. Operational Readiness

- [ ] On-call rotation set up.
- [ ] Escalation policy documented.
- [ ] SLA agreed với stakeholders.
- [ ] Communication plan cho incidents.
- [ ] Status page configured.

---

## 12. Post Go-Live Plan

### 12.1. Tuần 1-4 — Hyper-care

- **Daily**: 
  - On-call review metric.
  - Standup meeting daily với extended team.
  - Document any incident (kể cả minor).
- **Triggers for action**:
  - Bất kỳ critical alert → immediate investigation.
  - Pattern bất thường trong dashboard → proactive investigation.

### 12.2. Tháng 1-3 — Stabilization

- **Weekly**:
  - Review metric trends.
  - Tinh chỉnh tham số (δ_base, α, p) based on data.
  - Update runbook based on real incidents.
- **Monthly**:
  - Capacity review.
  - Cost review.
  - Postmortem cho mọi incident severity High+.

### 12.3. Tháng 3+ — Steady State

- **Quarterly**:
  - Chaos test (full matrix).
  - DR drill.
  - Architecture review (có cần optimize?).
  - Khắc phục các lỗ hổng "Trung bình" còn lại theo priority.
- **Bi-annual**:
  - Security audit.
  - Schema review (cleanup deprecated fields).
- **Annual**:
  - Major version upgrade planning.
  - Capacity plan cho năm sau.

### 12.4. Continuous Improvement Metrics

Track over time:
- Incident frequency theo severity.
- Mean Time to Detect (MTTD).
- Mean Time to Recover (MTTR).
- SLA compliance percentage.
- Customer satisfaction (downstream consumer).

### 12.5. Knowledge Management

- Maintain wiki với updated runbook.
- Postmortem repository (search và link cross-incident).
- Onboarding guide cho team mới.
- Architecture decision records (ADRs) cho mọi major change.

---

## Phụ lục — Cross-reference với 2 tài liệu thiết kế

| Section trong tài liệu này   | Tham chiếu Strict design       | Tham chiếu Heuristic design        |
|-------------------------------|--------------------------------|-------------------------------------|
| §2 Capacity                   | §11 Tham số                    | §14 Tham số                         |
| §3 Monitoring                 | §7 Latency Analysis            | §10 Latency + §10.3 metrics         |
| §4 Runbook                    | §8 Robustness                  | §11 Robustness + §12 DLQ            |
| §5 Hybrid                     | All                            | All                                 |
| §6 Deployment                 | Tất cả tầng failover           | §6 Cold Start + §11.3 Replay        |
| §7 Schema Evolution           | §4 Windowing                   | §4 Windowing                        |
| §8 Network Partition          | §5 Coordinator HA              | §9 Aggregator HA                    |
| §9 Testing                    | All                            | All                                 |
| §11 Pre-Prod Checklist        | All                            | All                                 |

---

**Chúc triển khai thành công.**

Bộ 3 tài liệu (Strict design + Heuristic design + Triển khai này) cung cấp toàn bộ kiến thức cần thiết để đưa hệ thống vào production. Khi gặp vấn đề chưa được cover, tạo Postmortem + cập nhật tài liệu này — đây là tài liệu sống.
