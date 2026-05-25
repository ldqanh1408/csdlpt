# DOCUMENT 05: OPERATIONS AND MONITORING GUIDE

This document details the monitoring matrix, automated alerting rules, and incident runbooks designed to ensure the operational stability of the distributed Stateful Stream Processing system (`refactor/`).

---

## 1. Prometheus Metrics Matrix

Worker and Coordinator processes publish Prometheus-compatible metrics via HTTP endpoints at `/metrics`. The core metrics include:

### 1.1. Watermark & Latency Metrics
* `csdlpt_watermark_global` (Gauge): The current system-wide Global Watermark. Labels: `mode`.
* `csdlpt_watermark_lag_seconds` (Gauge): The time delta between physical Processing-Time and the Global Watermark. An increasing lag indicates processing bottlenecks.
* `csdlpt_node_skew_seconds` (Gauge): The maximum watermark drift observed between workers in the cluster. Labels: `worker_id`. A skew $> 5.0$ seconds indicates a lagging worker node.

### 1.2. Throughput & Processing Metrics
* `csdlpt_events_total` (Counter): Cumulative count of processed log events. Labels: `worker_id`, `partition_id`, `status` (`processed`, `late`, `dropped`).
* `csdlpt_windows_closed_total` (Counter): Cumulative count of closed and materialized time windows. Labels: `worker_id`, `partition_id`.
* `csdlpt_queue_depth` (Gauge): The current count of events buffered in the worker's internal queue. Reaching `500` triggers backpressure.

### 1.3. Heuristic & DDSketch Statistics
* `csdlpt_sketch_quantile` (Gauge): Estimated quantiles of log latencies computed by the local DDSketch. Labels: `worker_id`, `partition_id`, `quantile` (`p50`, `p95`, `p99`, `p999`).

### 1.4. Cluster & Infrastructure Fault Metrics
* `csdlpt_fencing_violations_total` (Counter): Cumulative count of fencing token violations intercepted by workers.
* `csdlpt_aggregator_leader_changes_total` (Counter): Total leader re-elections occurring within the Aggregator HA group.
* `csdlpt_backpressure_pause_total` (Counter): Total times the worker paused Kafka consumption due to buffer saturation.
* `csdlpt_minio_upload_errors_total` (Counter): Cumulative count of state upload failures to MinIO.
* `csdlpt_dlq_backlog` (Gauge): Current count of unprocessed log records pending in the DLQ.

---

## 2. Automated Alerting Rules

The system includes 15 built-in alerting rules evaluated every 10 seconds via `AlertManager`. Active alerts are forwarded to incident notification providers (e.g., PagerDuty, Slack, or local console logs):

| No. | Alert Name | Severity | Activation Condition | Auto-Mitigation Action |
|:---|:---|:---|:---|:---|
| 1 | `watermark_lag_critical` | Critical | $W_{lag} > 60$ seconds | High-severity alert indicating severe stream processing stall. |
| 2 | `watermark_lag_warning` | Warning | $30\text{s} < W_{lag} \le 60\text{s}$ | Warning alert for degrading stream processing performance. |
| 3 | `node_skew_warning` | Warning | $1\text{s} < \text{skew} \le 5\text{s}$ | Warning indicating one worker is lagging behind other cluster nodes. |
| 4 | `node_skew_critical` | Critical | $\text{skew} > 5$ seconds | Critical alert indicating major watermark drift, risking a global watermark block. |
| 5 | `combined_status_critical` | Critical | Error count $\ge 3$ | Worker cluster-wide cascading failure alert. |
| 6 | `fencing_violation` | High | Fencing violation detected | Indicates active split-brain scenario or stale node message injection. |
| 7 | `ingestor_silent` | High | No heartbeat $> 15$ seconds | Indicates Ingestor process offline or network partitioned. |
| 8 | `negative_lag_warning` | Warning | Negative lag rate between 1% and 5% | Indicates moderate clock skew between log producers and workers. |
| 9 | `negative_lag_critical` | Critical | Negative lag rate $> 5\%$ | Indicates extreme system clock skew; triggers urgent NTP synchronization. |
| 10 | `all_workers_idle` | High | All workers idle | No incoming event traffic processed by any worker nodes. |
| 11 | `replay_mode_extended` | Warning | Replay $> 2$ workers for $> 5$ min | Indicates historical data recovery phase is taking longer than expected. |
| 12 | `correction_latency_high`| Warning | DLQ correction delay $> 1$ hour | Downstream Emitter blocked; unable to publish correction messages. |
| 13 | `dlq_backlog_critical` | Critical | DLQ Backlog $> 50,000$ | Late logs accumulating rapidly; DLQ consumer processing bottleneck. |
| 14 | `sketch_drift_high` | Warning | DDSketch Drift $> 0.5$ | Latency estimation model deviates significantly from actual network behavior. |
| 15 | `extreme_lag_detected` | Warning | Latency anomaly $> 1$ hour | Processing logs with highly outdated timestamps (outside sketch scope). |

---

## 3. Incident Runbooks

### Runbook 01: Worker Node Crash
* **Symptoms**: `watermark_lag_warning` triggers. The Prometheus dashboard shows a surge in `csdlpt_node_skew_seconds` for a specific worker node.
* **Mitigation Protocol**:
  1. Identify the crashed worker node via the Coordinator's `/failover` endpoint or partition layout.
  2. Inspect the corresponding container log files for system crash details (e.g., Out Of Memory - OOM events).
  3. Verify that the `FailoverManager` has dynamically reallocated the partitions to healthy workers (if `FAILOVER_ENABLED=true` is set, reassignment completes within $\le 10$ seconds).
  4. If automatic failover is disabled, issue a manual reassignment API request:
     `POST http://coordinator:8000/reassign` with payload `{"worker_id": "target-worker-node", "partitions": [orphaned_partition_id]}`.
  5. Restart the failed worker container. The newly spawned container will automatically restore state files from MinIO checkpoints and resume operations.

### Runbook 02: Fencing Token Violation Alert
* **Symptoms**: `fencing_violation` alerts trigger continuously.
* **Root Cause**: Split-Brain condition. A partitioned Coordinator (Stale Leader) recovers and attempts to issue stale partition assignments to worker nodes.
* **Mitigation Protocol**:
  1. Determine the active Coordinator leader by querying the HTTP endpoint `GET http://localhost:8000/state`.
  2. Check Coordinator logs to trace the consensus or ZooKeeper leader election sequence.
  3. Confirm that the workers successfully rejected commands issued by the stale leader (this is the built-in fencing safety mechanism).
  4. Isolate or restart the stale Coordinator node if it fails to automatically step down as leader.

### Runbook 03: Kafka Partition Lag Accrual
* **Symptoms**: The consumer lag for group `strict-workers` or `heuristic-workers` rises steadily on the Kafka cluster metrics.
* **Mitigation Protocol**:
  1. Query the `/ready` HTTP endpoint on the workers to verify if backpressure has paused consumption (internal event buffer queue depth exceeds 500).
  2. If Backpressure is active: Check if the local SSD storage IOPS capacity is saturated (`rocksdb_write_nanos`).
  3. If Backpressure is inactive but lag persists: The input throughput exceeds worker CPU processing capacity. Scale out the worker group (the system supports scaling up to 12 workers to match the 12 Kafka partitions).

### Runbook 04: MinIO State Upload Failure
* **Symptoms**: Metric `csdlpt_minio_upload_errors_total` increases. Worker state eviction process halts in `UPLOADING` state.
* **Mitigation Protocol**:
  1. Verify network connectivity from worker nodes to the MinIO cluster endpoint.
  2. Check that the MinIO Access Key and Secret Key environment variables on the workers are valid and not expired.
  3. Confirm that the MinIO cluster has sufficient free storage space.
  4. Once connectivity is restored, the `EvictionManager` state machine on the workers will automatically retry the pending uploads without requiring a process restart.
