# Out-of-Scope Items

Items found in the spec docs (`refactor/docs/`) that are **not implemented** in the current codebase
because they fall outside the agreed scope: security hardening, large-scale infrastructure, and
external-system tooling that requires multi-week deployment programs.

Each entry records *where the spec mentions it*, *what it requires*, and *why it is out of scope*.

---

## 1. Security / Authentication / Encryption

### 1.1 TLS for HTTP endpoints
**Spec**: `Trien_Khai_He_Thong.md §10.1`  
**Requirement**: All internal HTTP endpoints (coordinator, aggregator, workers) must use TLS 1.2+.
Certificates managed by cert-manager or Vault PKI.  
**Status**: The `run.py` HTTP server wires a `--tls-cert / --tls-key` flag that wraps the stdlib
socket with `ssl.SSLContext`. Certificate *provisioning*, rotation, and mutual TLS are not
implemented — only the socket-wrap shim exists.  
**Why out of scope**: Security hardening; requires a PKI / cert-manager deployment.

### 1.2 Kafka SASL/SCRAM authentication
**Spec**: `Trien_Khai_He_Thong.md §10.2`  
**Requirement**: Kafka producers and consumers must authenticate with SASL/SCRAM-SHA-512.
`KAFKA_SASL_MECHANISM`, `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD` env vars.  
**Status**: Real Kafka client adapter is implemented (`kafka_real.py`), but SASL/SCRAM authentication is skipped as out-of-scope security hardening.  
**Why out of scope**: Security; requires real Kafka cluster with SASL enabled.

### 1.3 mTLS for internal service-to-service communication
**Spec**: `Trien_Khai_He_Thong.md §10.3`  
**Requirement**: Worker → Coordinator and Worker → Aggregator HTTP calls use mutual TLS
(client certificates). Service mesh (Istio/Envoy) or standalone mTLS.  
**Status**: Not implemented; all internal urllib calls are plain HTTP.  
**Why out of scope**: Security; requires a service-mesh or PKI infrastructure.

### 1.4 Encryption at rest — MinIO SSE-KMS
**Spec**: `Trien_Khai_He_Thong.md §10.4`  
**Requirement**: All objects stored in MinIO must be encrypted at rest using server-side
encryption with a KMS key (SSE-KMS). MinIO `x-amz-server-side-encryption` header.  
**Status**: Not implemented. `TieredStorageManager.put_object()` does not pass SSE headers.  
**Why out of scope**: Security; requires KMS (HashiCorp Vault / AWS KMS) integration.

### 1.5 ZooKeeper ACL configuration
**Spec**: `Trien_Khai_He_Thong.md §10.5`  
**Requirement**: ZooKeeper znodes used for Raft leader election must be protected with
`digest` ACL (username/password). Prevents unauthorized clients from acquiring the lock.  
**Status**: Real ZooKeeper leader election lock is implemented (`zk_lock.py` and `RaftCoordinator` ZK election mode), but ACL security configuration is skipped as out-of-scope security hardening.  
**Why out of scope**: Security; requires a real ZooKeeper ensemble with ACL configuration.

### 1.6 MinIO bucket RBAC / access policies
**Spec**: `Trien_Khai_He_Thong.md §10.6`  
**Requirement**: Separate IAM policies for read-only (aggregator) vs. read-write (worker) MinIO
access. Bucket versioning enabled.  
**Status**: Not implemented. `TieredStorageManager` uses a single access-key/secret pair with
no per-role policies.  
**Why out of scope**: Security; requires MinIO admin / IAM policy management.

---

---

## 2. Schema Evolution Infrastructure

### 2.1 Schema Registry & migration tooling
**Spec**: `Trien_Khai_He_Thong.md §7`
**Requirement**: A Schema Registry (Confluent Schema Registry or equivalent)
that versions log schemas, plus a four-step dual-write migration pattern for
breaking changes (dual emit → dual consume → switch primary → drop legacy).
**Status**: Not implemented. `LogEvent` and `WindowResult` are plain
dataclasses with no `schema_version` field, no compatibility checking, and no
versioned consumer parsers. Schema migrations would have to be done by
redeploying both producers and consumers in lockstep.
**Why out of scope**: Requires an external Schema Registry deployment plus a
multi-week dual-write migration pipeline — fits the spec's "schema runtime
changes (cần migration plan)" caveat (Heuristic §15.3 / Strict §12.3).

---

## 3. Large-Scale Infrastructure

### 3.1 Kafka cluster sizing (3+ brokers, RF=3)
**Spec**: `Trien_Khai_He_Thong.md §4.2`  
**Requirement**: Production Kafka cluster with ≥3 brokers, replication factor 3, min-ISR 2.
Topic `events`: 12 partitions. Topics `strict_results`, `heuristic_results`: 4 partitions each.
`late_logs_dlq`: 4 partitions, retention 7 days.  
**Status**: Real Kafka client integration has been **implemented** (`kafka_real.py` using `kafka-python`) and is used by the processors. Docker Compose has been updated with real Kafka container services, though multi-node cluster size itself remains a deployment detail.  
**Why out of scope**: Large-scale infrastructure; requires external Kafka cluster provisioning.

### 3.2 ZooKeeper ensemble sizing
**Spec**: `Trien_Khai_He_Thong.md §4.3`  
**Requirement**: 3-node or 5-node ZooKeeper ensemble for Raft coordinator leader election in
production. Quorum size = (N/2) + 1.  
**Status**: Real ZooKeeper client integration has been **implemented** (`zk_lock.py` using `kazoo` and `RaftCoordinator` ZK election mode) and is used by the processors. Docker Compose has been updated with real ZooKeeper container services, though multi-node ensemble size itself remains a deployment detail.  
**Why out of scope**: Large-scale infrastructure; requires external ZooKeeper cluster.

### 3.3 MinIO distributed mode (erasure coding)
**Spec**: `Trien_Khai_He_Thong.md §4.4`  
**Requirement**: MinIO deployed in distributed mode with erasure coding (EC:4+2 or EC:8+4) for
object storage durability. Minimum 4 drives / 2 servers.  
**Status**: `TieredStorageManager` connects to a single MinIO endpoint; distributed mode is a
deployment concern.  
**Why out of scope**: Large-scale infrastructure; deploy-time concern only.

### 3.4 Multi-region disaster recovery replication
**Spec**: `Trien_Khai_He_Thong.md §8.3`  
**Requirement**: Active MinIO bucket in primary region, passive replica in DR region via
MinIO Site Replication. RTO ≤ 30 min, RPO ≤ 5 min.  
**Status**: `DisasterRecovery` in `strict/disaster_recovery.py` backs up to a single MinIO
endpoint. Cross-region replication is a MinIO cluster configuration.  
**Why out of scope**: Large-scale infrastructure; requires multi-region MinIO deployment.

### 3.5 Auto-scaling rules for workers / aggregators
**Spec**: `Trien_Khai_He_Thong.md §6.1`  
**Requirement**: Kubernetes HPA based on `csdlpt_watermark_lag_seconds` metric. Scale out when
lag > 30 s, scale in when lag < 10 s. Min 2 / max 12 worker replicas.  
**Status**: Prometheus metrics are exported (see `MonitoringManager`) but HPA manifests and
scaling logic are not part of the codebase.  
**Why out of scope**: Large-scale infrastructure; Kubernetes deployment configuration.

### 3.6 Network policies / service mesh
**Spec**: `Trien_Khai_He_Thong.md §10.7`  
**Requirement**: Kubernetes NetworkPolicy to restrict inter-pod traffic. Only workers may reach
the coordinator; only workers may reach the aggregator. Egress to MinIO is restricted by
namespace.  
**Status**: Not implemented; pure application-layer concern.  
**Why out of scope**: Large-scale infrastructure / security; Kubernetes deployment configuration.

### 3.7 Host-level resource metrics (RAM, disk, IOPS)
**Spec**: `Trien_Khai_He_Thong.md §3.1 Tier 3` and
`deploy/prometheus-rules.yml` (WorkerRAMHigh, WorkerDiskHigh)
**Requirement**: Host-level gauges for `csdlpt_worker_ram_usage_bytes`,
`csdlpt_worker_ram_limit_bytes`, `csdlpt_worker_disk_usage_bytes`,
`csdlpt_worker_disk_limit_bytes`, and `shared_volume_iops_used` so the
Tier-3 RAM/disk alerts can fire.
**Status**: Not implemented at the application layer. These are host metrics
typically supplied by `node_exporter`, cAdvisor, or the Kubernetes kubelet —
the stream processor itself does not introspect its container's cgroup or
filesystem.
**Why out of scope**: Infrastructure metric concern. Production deployments
attach `node_exporter` or similar; the application surface remains focused
on stream-processing-specific signals (watermarks, sketch, DLQ, failover).

---

## Summary

| Category | Items | Status |
|----------|-------|--------|
| Security | TLS, mTLS, SSE-KMS, MinIO RBAC (SASL/SCRAM & ZK ACL are skipped) | Not implemented — deployment/ops concern |
| Schema  | Schema Registry, dual-write migration pattern | Not implemented — requires multi-week migration program |
| Scale   | MinIO distributed, multi-region DR, HPA, NetworkPolicy (Real ZK & Kafka clients are implemented) | Not implemented — infrastructure concern |

All functional spec requirements are implemented in the codebase:

- **Strict mode**: punctuation-based watermarks (per-partition empty-token emission),
  bounded priority queue, RocksDB isolated partition instances, 4-state tiered eviction
  state machine with crash recovery, idempotent dedup filter with TTL, partition-level
  checkpointing every 10s, Raft + ZK-style coordinator HA with state replication,
  fencing tokens, even redistribution + cascading failover, 5-step strict failback
  protocol with persistence, replay sub-checkpointing, differentiated eviction for
  recovery partitions, exactly-once output (transactional + idempotent modes), audit
  sink mirror to `audit_results`, and disaster-recovery active-window backups every 5
  minutes.
- **Heuristic mode**: DDSketch with log-scale buckets and bucket-collapse memory bound,
  sliding-window sketch with checkpoint serialization, adaptive percentile (p_normal /
  p_safe) with burst detection, adaptive alpha, cold-start two-condition exit with
  MinIO baseline persistence, negative-lag handler with 4-tier diagnosis and BOO
  fallback, snapshot-based replay rollback, DLQ pipeline with hourly correction
  consumer, three correction-message patterns, per-window-type SLA tracking (normal 1h
  vs burst 15min), DLQ oldest-entry-age metric, 24-hour FINAL reconciliation
  scheduler, aggregator HA (cross-platform file-lock active-standby), and per-window
  loss accounting.
- **Hybrid mode**: priority-based routing between strict and heuristic engines per
  partition with unified loss accounting and shared DLQ.
- **Observability**: Prometheus metrics (50+ gauges/counters/histograms covering
  watermark, lag, skew, sketch quantiles, fencing violations, DLQ, replay, adaptive
  percentile, SLA compliance, RocksDB and tiered-storage size), PagerDuty alerting
  with 13 default rules and per-rule cooldowns, ingestor health monitor with
  W_meta_global meta-metric and 4-tier clock-skew diagnosis.
- **Operational ergonomics**: feature flags (`ENABLE_ADAPTIVE_PERCENTILE`,
  `ENABLE_REPLAY_SUB_CHECKPOINTING`, `ENABLE_TWO_PHASE_EVICTION`,
  `ENABLE_NEGATIVE_LAG_RECALIBRATION`) gate the complex mechanisms per spec §6.4.
