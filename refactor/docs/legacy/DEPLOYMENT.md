# CSDLPT Stream Processor -- Deployment Guide

This guide covers deploying the csdlpt stateful stream processor with real infrastructure
(Kafka, ZooKeeper, MinIO, Prometheus, Grafana) and how to migrate from the current
simulation-mode stack.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Real Kafka Setup](#2-real-kafka-setup)
3. [ZooKeeper vs File Lock](#3-zookeeper-vs-file-lock)
4. [MinIO Setup](#4-minio-setup)
5. [Prometheus and Grafana](#5-prometheus-and-grafana)
6. [Docker Compose Profiles](#6-docker-compose-profiles)
7. [Code vs Spec Compliance](#7-code-vs-spec-compliance)
8. [Feature Flags](#8-feature-flags)
9. [Known Gaps (Accepted Divergences from Spec)](#9-known-gaps-accepted-divergences-from-spec)
10. [Pre-Production Checklist](#10-pre-production-checklist)

---

## 1. Architecture Overview

The csdlpt stream processor is a distributed stateful stream processing system with two
processing paths: **Strict** (watermark-based, zero data loss) and **Heuristic**
(DDSketch-based, low latency with acceptable loss). Both paths read from a shared Kafka
cluster and produce results to separate output topics.

### 1.1 Component Roles

| Component | Role | HA Mechanism |
|-----------|------|--------------|
| **Ingestor** | Reads raw log files, partitions by `hash(event_id) % 12`, fans out to workers | Single instance (stateless) |
| **Worker** | Processes events within assigned partitions, manages RocksDB windows | 4 workers, 3 partitions each |
| **Coordinator** (Strict) | Synchronizes global watermarks, assigns partitions, manages failover | Raft quorum (3 nodes) |
| **Aggregator** (Heuristic) | Merges per-worker DDSketch quantiles, broadcasts `W_global_h` | Active-Standby pair |
| **DLQ Consumer** (Heuristic) | Replays late-arriving events from `late_logs_dlq` topic | Consumer group scaling |

### 1.2 Simulation Mode vs Real Infrastructure

In **simulation mode** (current default), all external dependencies are replaced with
in-process or file-based alternatives. The code is written against abstract interfaces
so the same application logic works in both modes.

| Dependency | Simulation Mode | Real Infrastructure |
|------------|----------------|---------------------|
| **Messaging** | `kafka_sim.py` -- HTTP-based broker, producer, and consumer running inside the coordinator | Real Kafka cluster with `kafka-python` |
| **Coordinator HA** | 3 HTTP servers implementing Raft over HTTP | Same Raft code, connected over real network interfaces |
| **Aggregator HA** | `fcntl.flock()` on a shared file path | ZooKeeper ephemeral lock via `kazoo` |
| **Tiered Storage** | `TieredStorageManager` with optional MinIO (falls back to local disk) | Real MinIO cluster with bucket policies |
| **Exactly-Once Output** | `OutputManager` idempotent mode (dedup via `window_id`) | Kafka transactions via `MockTransactionalSink` |
| **DLQ Pipeline** | In-memory queue + RocksDB metadata | Real Kafka topic `late_logs_dlq` with consumer group |
| **Monitoring** | Prometheus `/metrics` endpoint on port 8000 | Same endpoints, scraped by external Prometheus |
| **State Persistence** | RocksDB on local disk | Same RocksDB, backed by shared volume for checkpoint portability |

**Switching to production mode** requires:
1. Setting environment variables to point at real infrastructure endpoints.
2. Installing the Python drivers (`kafka-python`, `minio`, `kazoo`).
3. Replacing the simulation transport layer with real driver clients.

---

## 2. Real Kafka Setup

### 2.1 Cluster Sizing

Per the operations spec (`Trien_Khai_He_Thong.md` section 2.4):

- **3 brokers**, replication factor 3
- **128 TB NVMe per broker** (for 100k logs/s baseline)
- `min.insync.replicas=2`, `acks=all` on producers
- Adjust storage based on your measured peak log rate, burst factor, and retention needs

### 2.2 Topic Creation

Create the following topics with exact retention and partition counts:

```bash
# Events topic -- raw log stream, consumed by all workers
kafka-topics.sh --create \
  --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 \
  --topic events \
  --partitions 12 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete \
  --config compression.type=lz4

# Strict results -- finalized windows for billing/audit/compliance
kafka-topics.sh --create \
  --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 \
  --topic strict_results \
  --partitions 12 \
  --replication-factor 3 \
  --config retention.ms=2592000000 \
  --config cleanup.policy=delete \
  --config compression.type=lz4

# Heuristic results -- speculative windows for dashboards/alerting/ML
kafka-topics.sh --create \
  --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 \
  --topic heuristic_results \
  --partitions 12 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete \
  --config compression.type=lz4

# Dead Letter Queue -- late events that missed heuristic windows
kafka-topics.sh --create \
  --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 \
  --topic late_logs_dlq \
  --partitions 12 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete

# Audit results -- immutable audit trail for compliance
kafka-topics.sh --create \
  --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 \
  --topic audit_results \
  --partitions 12 \
  --replication-factor 3 \
  --config retention.ms=7776000000 \
  --config cleanup.policy=delete
```

| Topic | Retention | Purpose |
|-------|-----------|---------|
| `events` | 7 days | Raw log stream from ingestor to workers |
| `strict_results` | 30 days | Finalized windows (billing, audit, compliance) |
| `heuristic_results` | 7 days | Speculative windows (dashboards, alerting, ML) |
| `late_logs_dlq` | 7 days | Late events that missed heuristic windows |
| `audit_results` | 90 days | Immutable audit trail |

### 2.3 Consumer Group IDs

| Consumer Group | Consumes From | Description |
|---------------|---------------|-------------|
| `strict-workers` | `events` | Workers in strict mode |
| `heuristic-workers` | `events` | Workers in heuristic mode |
| `dlq-consumer` | `late_logs_dlq` | DLQ replay consumers |

### 2.4 Partition Key Strategy

```
partition = hash(event_id) % 12
```

The ingestor computes this before fan-out. Each of the 4 workers is assigned 3 partitions
(node0: 0,1,2; node1: 3,4,5; node2: 6,7,8; node3: 9,10,11). This is deterministic and
ensures all events for a given `event_id` land on the same partition.

### 2.5 Switching from kafka_sim to Real kafka-python

The simulation layer (`refactor/common/kafka_sim.py`) provides the full Kafka abstraction
via HTTP. To switch to real Kafka:

**Step 1:** Install the driver.

```bash
pip install kafka-python
```

**Step 2:** Define a real Kafka producer and consumer adapter that implements the same
interface used by the current code. The simulation types in `kafka_sim.py` are:

- `KafkaMessage` (dataclass with `key`, `value`, `partition`, `offset`, `timestamp`)
- `KafkaTopic` (stores messages, manages offset tracking)
- `KafkaProducer` (`send(topic, value, key, partition)` with `acks=all` simulation)
- `KafkaConsumer` (`poll()`, `pause()`, `resume()`, `seek()`, `commit()`)
- `KafkaConsumerGroup` (rebalancing on member join/leave)
- `KafkaBroker` (multi-broker HTTP server)

Create a thin adapter module (e.g., `refactor/common/kafka_real.py`) that wraps
`kafka.KafkaProducer` and `kafka.KafkaConsumer` behind the same interface.

**Step 3:** Set environment variables to switch the transport:

```bash
export KAFKA_BOOTSTRAP_SERVERS="kafka-1:9092,kafka-2:9092,kafka-3:9092"
export KAFKA_ENABLE_REAL="true"
```

**Step 4:** In `run.py` or worker initialization, use a factory that checks
`KAFKA_ENABLE_REAL` and instantiates the appropriate adapter.

---

## 3. ZooKeeper vs File Lock

### 3.1 Current Implementation: File Lock via fcntl

The aggregator HA module (`refactor/heuristic/aggregator_ha.py`) uses `fcntl.flock()` to
acquire an exclusive lock on a shared file (`/tmp/aggregator.lock`). The active aggregator
writes a heartbeat to `/tmp/aggregator-heartbeat` every 1 second. The standby monitors
the heartbeat file; if the active is stale for more than 1.5 seconds, the standby attempts
to acquire the lock and take over.

```python
# Current lock mechanism (aggregator_ha.py)
from refactor.heuristic.aggregator_ha import AggregatorHA, FileLockLeader

lock = FileLockLeader("/tmp/aggregator.lock")
if lock.try_acquire():
    # This instance is the active leader
    ...
```

### 3.2 When File Lock Works

| Scenario | Works? | Notes |
|----------|--------|-------|
| Single machine (all containers on one host) | Yes | All processes share the kernel's lock namespace |
| Multiple machines with NFS shared volume | Yes (with caveats) | NFSv4 supports `fcntl` locks, but lock recovery on NFS server restart is unreliable |
| Multiple machines, no shared filesystem | No | Each host sees its own `/tmp`, locks are independent |
| Kubernetes with ReadWriteMany PVC | Depends | The CSI driver must support `fcntl`; many do not |

### 3.3 When ZooKeeper Is Required

ZooKeeper is required when:

- Aggregator instances run on **different physical machines** without a shared POSIX filesystem.
- The deployment uses **Kubernetes** where pods cannot share a filesystem with `fcntl` support.
- You need **automatic lock release** on process death (ZK ephemeral nodes provide this
  guarantee; file locks may or may not depending on the filesystem).
- You need **multi-node coordination** beyond aggregator HA (e.g., distributed config,
  partition assignment consensus).

### 3.4 Switching to ZooKeeper

Replace the `FileLockLeader` class with a `kazoo`-based implementation:

**Step 1:** Install kazoo.

```bash
pip install kazoo
```

**Step 2:** Create `refactor/common/zk_lock.py`:

```python
"""ZooKeeper-based leader election for Aggregator HA."""

import logging
import os
import time
from kazoo.client import KazooClient
from kazoo.recipe.election import Election

logger = logging.getLogger("zk_lock")


class ZKLeaderElection:
    """Leader election via ZooKeeper ephemeral sequential nodes.

    Uses Kazoo's Election recipe, which creates an ephemeral node under
    a leader-election path. The node with the lowest sequence number is
    the leader. When the leader's session expires (crash, network loss),
    the ephemeral node is automatically removed and the next node takes over.
    """

    def __init__(self, zk_hosts: str, election_path: str = "/csdlpt/aggregator-leader"):
        self.zk_hosts = zk_hosts
        self.election_path = election_path
        self._client: KazooClient | None = None
        self._election: Election | None = None
        self._is_leader = False

    def start(self):
        self._client = KazooClient(hosts=self.zk_hosts)
        self._client.start()
        self._client.ensure_path(self.election_path)
        self._election = self._client.Election(self.election_path)

    def try_acquire(self, timeout: float = 5.0) -> bool:
        """Block until elected leader or timeout."""
        try:
            self._election.run(self._on_leadership, timeout=timeout)
            return self._is_leader
        except Exception:
            return False

    def _on_leadership(self):
        self._is_leader = True
        logger.info("ZK: elected leader (pid=%d)", os.getpid())
        # Block until leadership is lost (session expires or release called)
        while self._is_leader and self._client and self._client.connected:
            time.sleep(1)

    @property
    def is_leader(self) -> bool:
        return self._is_leader

    def release(self):
        self._is_leader = False
        if self._client:
            try:
                self._client.stop()
                self._client.close()
            except Exception:
                pass
```

**Step 3:** Update `AggregatorHA` to accept a pluggable lock backend:

```python
# In aggregator_ha.py, replace hardcoded FileLockLeader:
if os.environ.get("ZK_HOSTS"):
    lock = ZKLeaderElection(
        zk_hosts=os.environ["ZK_HOSTS"],
        election_path=os.environ.get("ZK_ELECTION_PATH", "/csdlpt/aggregator-leader"),
    )
else:
    from refactor.heuristic.aggregator_ha import FileLockLeader
    lock = FileLockLeader(
        lock_path=os.environ.get("AGGREGATOR_LOCK_PATH", "/tmp/aggregator.lock"),
    )
```

### 3.5 ZooKeeper Ensemble Sizing

Per the operations spec, deploy a 3-node ZooKeeper ensemble:

```yaml
# Add to docker-compose.yml (production profile)
zookeeper-1:
  image: confluentinc/cp-zookeeper:7.5.0
  environment:
    ZOOKEEPER_SERVER_ID: 1
    ZOOKEEPER_SERVERS: "zookeeper-1:2888:3888,zookeeper-2:2888:3888,zookeeper-3:2888:3888"

zookeeper-2:
  image: confluentinc/cp-zookeeper:7.5.0
  environment:
    ZOOKEEPER_SERVER_ID: 2
    ZOOKEEPER_SERVERS: "zookeeper-1:2888:3888,zookeeper-2:2888:3888,zookeeper-3:2888:3888"

zookeeper-3:
  image: confluentinc/cp-zookeeper:7.5.0
  environment:
    ZOOKEEPER_SERVER_ID: 3
    ZOOKEEPER_SERVERS: "zookeeper-1:2888:3888,zookeeper-2:2888:3888,zookeeper-3:2888:3888"
```

---

## 4. MinIO Setup

### 4.1 Bucket Creation

The application uses a single bucket, `csdlpt-windows`.

```bash
# Install MinIO client
wget https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x mc

# Configure alias (use your production credentials)
mc alias set csdlpt-prod http://minio:9000 minioadmin minioadmin

# Create the bucket
mc mb csdlpt-prod/csdlpt-windows

# Set retention policy (optional, for compliance)
mc retention set --default COMPLIANCE 90d csdlpt-prod/csdlpt-windows
```

### 4.2 Access Key / Secret Key Configuration

Set the following environment variables on all workers, the coordinator, and the aggregator:

```bash
export MINIO_ENDPOINT="minio:9000"
export MINIO_ACCESS_KEY="minioadmin"
export MINIO_SECRET_KEY="minioadmin"
export MINIO_BUCKET="csdlpt-windows"
export MINIO_SECURE="false"          # Set to "true" for TLS
```

In `docker-compose.yml`, add these to each service:

```yaml
environment:
  MINIO_ENDPOINT: "minio:9000"
  MINIO_ACCESS_KEY: "${MINIO_ACCESS_KEY:-minioadmin}"
  MINIO_SECRET_KEY: "${MINIO_SECRET_KEY:-minioadmin}"
  MINIO_BUCKET: "csdlpt-windows"
  MINIO_SECURE: "false"
```

### 4.3 Tiered Storage Path Structure

The tiered storage module (`refactor/common/tiered_storage.py`) organizes data by
processing mode, partition, and window:

```
csdlpt-windows/
  strict-watermark/
    active_state_backup/
      2026-05-25T12:00:00Z/
        partition_0.json
        partition_1.json
        ...
    historical/
      partition_0/
        window_2026-05-25T10:00:00_2026-05-25T10:00:05.json
        window_2026-05-25T10:00:05_2026-05-25T10:00:10.json
        ...
      partition_1/
        ...
  heuristic-watermark/
    active_state_backup/
      ...
    historical/
      partition_0/
        ...
```

**Path convention:**

- `active_state_backup/` -- Periodic DR snapshots (every 5 minutes by default)
- `historical/{partition_id}/{window_id}.json` -- Closed and evicted windows

### 4.4 Eviction State Machine

The 4-state eviction protocol (`tiered_storage.py`):

```
CLOSED -> UPLOADING -> UPLOADED -> PURGED
   |          |            |          |
   Window     Data         ETag       Local
   closed     being        written    RocksDB
              uploaded     returned   freed
```

- **CLOSED**: Window is finalized, data still in RocksDB
- **UPLOADING**: Async upload to MinIO in progress (max 3 retries)
- **UPLOADED**: Upload succeeded, ETag recorded; data still duplicated locally
- **PURGED**: Local RocksDB data deleted; MinIO is the sole copy

Retry logic: 3 attempts with exponential backoff (1s, 2s, 4s). After 3 failures, the
eviction is abandoned and the failure counter is incremented for monitoring.

---

## 5. Prometheus and Grafana

### 5.1 Prometheus Scrape Configuration

The current `deploy/prometheus.yml` is configured for the simulation environment using
Docker hostnames. For production, update the targets:

```yaml
global:
  scrape_interval: 5s
  scrape_timeout: 4s
  evaluation_interval: 10s

alerting:
  alertmanagers:
    - static_configs:
        - targets:
            - "alertmanager:9093"

rule_files:
  - "prometheus-rules.yml"

scrape_configs:
  - job_name: "csdlpt-coordinator"
    metrics_path: "/metrics"
    static_configs:
      - targets:
          - "coordinator-1:8000"
          - "coordinator-2:8000"
          - "coordinator-3:8000"
        labels:
          role: "coordinator"

  - job_name: "csdlpt-aggregator"
    metrics_path: "/metrics"
    static_configs:
      - targets:
          - "aggregator:8000"
          - "aggregator-standby:8000"
        labels:
          role: "aggregator"

  - job_name: "csdlpt-workers"
    metrics_path: "/metrics"
    static_configs:
      - targets:
          - "node0:8000"
          - "node1:8000"
          - "node2:8000"
          - "node3:8000"
        labels:
          role: "worker"

  - job_name: "csdlpt-ingestor"
    metrics_path: "/metrics"
    static_configs:
      - targets:
          - "ingestor:8000"
        labels:
          role: "ingestor"
```

**Important:** The original `deploy/prometheus.yml` uses `localhost:9xxx` targets which
do not work inside Docker (localhost resolves to the Prometheus container). The targets
above use Docker service hostnames which resolve correctly on the Docker network.

### 5.2 Prometheus Datasource in Grafana

After both Prometheus and Grafana are running:

1. Navigate to Grafana at `http://localhost:3000` (default credentials: `admin` / `admin`).
2. Go to **Configuration > Data Sources > Add data source**.
3. Select **Prometheus**.
4. Set **URL** to `http://prometheus:9090` (Docker service name, not localhost).
5. Click **Save & Test**.

Alternatively, provision the datasource via a config file mounted into Grafana:

```yaml
# deploy/grafana-datasource.yml
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: false
```

Mount this into the Grafana container:

```yaml
grafana:
  image: grafana/grafana:10.3.0
  volumes:
    - ./grafana-datasource.yml:/etc/grafana/provisioning/datasources/datasource.yml:ro
    - ./grafana-dashboard.json:/etc/grafana/provisioning/dashboards/csdlpt.json:ro
```

### 5.3 Dashboard Import

The dashboard file `deploy/grafana-dashboard.json` is already mounted in the docker-compose
file. It contains 5 pages as specified in the operations doc:

| Page | Content |
|------|---------|
| 1 -- Executive Summary | Throughput, watermark lag, worker count |
| 2 -- Per-Partition Detail | LW_i, Skew, Backpressure per partition |
| 3 -- Strict Specific | Node skew, fencing, clock skew, punctuation |
| 4 -- Heuristic Specific | Sketch quantiles, DLQ, negative lag, replay mode |
| 5 -- Resources | RocksDB stats, MinIO latency, eviction states, ingestor RTT |

If the dashboard does not auto-provision, import manually:

1. Go to **Dashboards > Import**.
2. Upload `deploy/grafana-dashboard.json`.
3. Select the Prometheus datasource created in step 5.2.

### 5.4 Alertmanager Integration

Enable Alertmanager by uncommenting the target in `deploy/prometheus.yml`:

```yaml
alerting:
  alertmanagers:
    - static_configs:
        - targets:
            - "alertmanager:9093"
```

Add an Alertmanager service to docker-compose:

```yaml
alertmanager:
  image: prom/alertmanager:v0.26.0
  container_name: refactor-alertmanager
  ports:
    - "9093:9093"
  volumes:
    - ./alertmanager.yml:/etc/alertmanager/alertmanager.yml:ro
  restart: unless-stopped
```

The alert rules file (`deploy/prometheus-rules.yml`) defines severity levels:

| Severity | Channel | Example |
|----------|---------|---------|
| Critical | PagerDuty + phone | `DataLossRateHigh`, all workers down |
| High | PagerDuty | `NodeSkewHigh` > 5000ms |
| Warning | Slack | `DLQLagHigh` > 600s |
| Info | Email digest | Non-monotonic punctuation observed |

For PagerDuty integration, set the routing key:

```bash
export PAGERDUTY_ROUTING_KEY="routing_key_from_pagerduty"
```

This is read by `refactor/common/alerting.py` at startup.

---

## 6. Docker Compose Profiles

The docker-compose file in `refactor/deploy/docker-compose.yml` uses Compose profiles
to control which processing path is active. The `MODE` environment variable (set via
`.env` file or command line) determines the active profile.

### 6.1 Strict Mode (default)

```bash
MODE=strict docker compose --profile strict up -d
```

Starts: coordinator (3 instances), 4 workers, Prometheus, Grafana, MinIO.
Does NOT start: aggregator, aggregator-standby.

### 6.2 Heuristic Mode

```bash
MODE=heuristic docker compose --profile heuristic up -d
```

Starts: aggregator, aggregator-standby, 4 workers (in heuristic mode), Prometheus,
Grafana, MinIO.
Does NOT start: coordinator cluster.

### 6.3 Hybrid Mode (both paths)

```bash
MODE=hybrid docker compose --profile hybrid up -d
```

Starts everything: coordinator cluster, aggregator pair, 4 workers (hybrid-capable),
Prometheus, Grafana, MinIO.

### 6.4 Manual Ingestor

The ingestor is in the `manual` profile and must be started separately after workers
are healthy:

```bash
docker compose --profile manual up ingestor
```

### 6.5 Scaling Workers

Each worker handles 3 partitions by default. To add capacity:

```bash
# Scale to 6 workers, then reassign partitions:
docker compose up --scale node=5 --scale node=6 -d
# Adjust PARTITIONS env var on each new worker accordingly.
```

Partition-to-worker mapping is controlled by the `PARTITIONS` environment variable
on each worker container. The env var accepts comma-separated partition IDs:
`PARTITIONS="0,1,2"`.

---

## 7. Code vs Spec Compliance

The following table documents how each spec requirement is fulfilled in simulation mode
and how it maps to the production implementation.

| Spec Requirement | Simulation Mode | Production Mode |
|-----------------|----------------|-----------------|
| **Kafka messaging** (12 partitions) | `kafka_sim.py` -- HTTP-based broker with in-process topics, producers, and consumers. Supports pause/resume/seek/commit and consumer group rebalancing | Real Kafka cluster with `kafka-python` driver |
| **Coordinator HA** (Raft 3 nodes) | 3 HTTP server instances running Raft consensus over HTTP. Leader election, log replication, and term fencing all work identically to production | Same Raft code, communicating over real TCP. Raft log stored on durable disk |
| **Aggregator HA** (ZK lock) | `fcntl.flock()` file lock on a shared Docker volume (`aggregator_lock`). Heartbeat monitoring at 1s intervals | Replace `FileLockLeader` with `kazoo` ZK client; use ephemeral sequential znodes for leader election |
| **Tiered Storage** (MinIO, 4-state eviction) | `TieredStorageManager` with optional MinIO endpoint. If `MINIO_ENDPOINT` is unset, eviction is skipped and data stays in RocksDB | Point `MINIO_ENDPOINT` at a real MinIO cluster. Same 4-state machine (CLOSED->UPLOADING->UPLOADED->PURGED) with ETag verification |
| **Exactly-once output** | `OutputManager` idempotent mode: dedup key = `window_id`, RocksDB-backed dedup filter with TTL | `OutputManager` transactional mode: `MockTransactionalSink` implements 2PC (begin_tx, pre_commit, commit). Replace with real Kafka transactional producer |
| **DLQ pipeline** | In-memory queue (`collections.deque`) + RocksDB metadata tracking (`csdlpt_dlq_meta` column family) | Real Kafka `late_logs_dlq` topic; `dlq-consumer` consumer group replays with offset tracking |
| **Heartbeat punctuation** | Ingestor sends heartbeat tokens every 1s to the coordinator. Workers forward to advance global watermark | Same mechanism; relies on real network between ingestor and coordinator |
| **Strict failover** | Heartbeat-based failure detection; coordinator reassigns partitions from dead workers. Failback state machine restores from checkpoint + replays | Same code; requires faster heartbeat timeout for real network environments |
| **Heuristic adaptive percentile** | DDSketch percentile auto-adjusts based on loss rate monitoring | Same algorithm; configurable via `ENABLE_ADAPTIVE_PERCENTILE` flag |
| **RocksDB state store** | Per-worker RocksDB instance on local Docker volume. Column families: `windows`, `checkpoints`, `output_dedup`, `dlq_meta` | Same setup on real NVMe storage. Column families and compaction settings unchanged |
| **Prometheus metrics** | `/metrics` endpoint on port 8000, scoped per component | Same endpoints, scraped by external Prometheus. All metrics namespaced under `csdlpt_` |

---

## 8. Feature Flags

Feature flags are defined in `refactor/common/config.py` and controlled via environment
variables. They allow turning complex mechanisms on and off without redeploying.

### 8.1 ENABLE_TWO_PHASE_EVICTION

- **Default:** `true`
- **Env var:** `ENABLE_TWO_PHASE_EVICTION`
- **Gates:** Whether the tiered storage eviction state machine runs the full 4-state
  protocol (CLOSED -> UPLOADING -> UPLOADED -> PURGED) or a simpler 2-phase path that
  skips the UPLOADING verification step.
- **When to disable:** If MinIO is unavailable and you want eviction to complete faster
  using local-only storage. Also useful for testing window lifecycle without network
  dependencies.

### 8.2 ENABLE_ADAPTIVE_PERCENTILE

- **Default:** `true`
- **Env var:** `ENABLE_ADAPTIVE_PERCENTILE`
- **Gates:** The Heuristic engine's automatic percentile adjustment. When the loss rate
  exceeds the target, the system bumps the percentile (e.g., P99 -> P99.5 -> P99.9) to
  reduce late-event drops. When loss rate is below target for an extended period, it
  relaxes back to P99.
- **When to disable:** If you need a fixed-percentile for reproducible benchmarking or
  if the adaptive mechanism causes oscillating latency in stable traffic patterns.

### 8.3 ENABLE_REPLAY_SUB_CHECKPOINTING

- **Default:** `true`
- **Env var:** `ENABLE_REPLAY_SUB_CHECKPOINTING`
- **Gates:** During recovery replay, whether the worker periodically writes incremental
  checkpoints (sub-checkpoints) so that a crash during replay can resume from the last
  sub-checkpoint instead of restarting the entire replay.
- **When to disable:** If the replay volume is small enough that full replay is faster
  than checkpoint I/O. Also useful during debugging to simplify the replay execution path.

### 8.4 ENABLE_NEGATIVE_LAG_RECALIBRATION

- **Default:** `true`
- **Env var:** `ENABLE_NEGATIVE_LAG_RECALIBRATION`
- **Gates:** In the Heuristic engine, whether negative lag events (events that appear to
  arrive before their timestamp suggests) trigger automatic recalibration of the watermark
  reference point. Negative lag typically indicates clock skew between the ingestor and
  the event source.
- **When to disable:** If you have verified that NTP is perfectly synchronized across
  all machines, or if negative lag recalibration causes false-positive watermark jumps
  in your specific deployment.

### 8.5 ENABLE_HYBRID_ROUTING

- **Default:** `false`
- **Env var:** `ENABLE_HYBRID_ROUTING`
- **Gates:** Whether the ingestor can route events to both strict and heuristic paths
  simultaneously. When enabled, each event is duplicated to both processing pipelines
  based on routing rules (e.g., `event_type=billing` goes to strict, `event_type=click`
  goes to heuristic, `event_type=transaction` goes to both).
- **When to enable:** When you need both guaranteed-zero-loss output for critical
  consumers AND low-latency speculative results for dashboards. Costs approximately
  1.7x the resources of a single path.

### 8.6 FAILOVER_ENABLED

- **Default:** `false`
- **Env var:** `FAILOVER_ENABLED`
- **Gates:** Whether the coordinator performs automatic partition reassignment when a
  worker is detected as dead (heartbeat timeout). When disabled, failed workers leave
  their partitions unprocessed until manual intervention.
- **When to enable:** Always in production. Disable only in development when you are
  actively debugging worker behavior and don't want automatic reassignment to interfere.

### 8.7 AGGREGATOR_HA_ENABLED

- **Default:** `false`
- **Env var:** `AGGREGATOR_HA_ENABLED`
- **Gates:** Whether the aggregator runs in Active-Standby HA mode. When enabled, the
  standby monitors the active's heartbeat and takes over if the active is unresponsive
  for more than 1.5 seconds.
- **When to enable:** In production heuristic and hybrid deployments. Not needed for
  strict-only deployments (strict mode uses the coordinator, not the aggregator).

---

## 9. Known Gaps (Accepted Divergences from Spec)

These items are documented in `refactor/docs/out_of_scope1.md` section 5 as intentional
architectural trade-offs. They represent places where the code diverges from the spec
for valid reasons, not bugs.

| Gap ID | Spec Expectation | Current Implementation | Rationale |
|--------|-----------------|----------------------|-----------|
| **5.1** | Aggregator HA via ZooKeeper ephemeral lock | `fcntl.flock()` file-based lock | ZooKeeper adds deployment and operational complexity. File lock works for single-machine and NFS deployments. Multi-machine deployments can switch to ZK (see section 3) |
| **5.2** | Checkpoint serialization in Protobuf | JSON serialization inside RocksDB `meta:checkpoint` key | RocksDB provides atomic writes and crash recovery out of the box. Protobuf requires a compile step and schema registry that was deemed out of scope |
| **5.3** | Windows close at `W_global_h` (server-side) | Heuristic engine uses `local_watermark` for closing decisions | Local closing avoids network round-trips in the hot path. The aggregator's periodic broadcast corrects drift every 200ms, making local decisions converge quickly |
| **5.4** | Cascading failover for heuristic workers | Only strict mode has `failover.py`; heuristic has no automatic failover | Heuristic mode tolerates <= 1% loss by design, making aggressive failover less critical. The cost of implementing cascading redistribution is high relative to the benefit |
| **5.5** | Replay progress as fraction (0.0-1.0) | Percentage integer (40.0 instead of 0.4) | Percentage is more human-readable. A one-line format change if downstream tooling expects fractions |
| **5.6** | Vietnamese quadrant strings for diagnosis | Numeric `combined_status` (0-3) with separate `skew_status` and `lag_status` | Numeric codes are language-agnostic, easier to alert on, and can be remapped to localized strings at the dashboard layer |
| **5.7** | `T_network_ingest` measures poll+decode only | Measures end-to-end `arrival_time - event_time` | End-to-end latency is more actionable for operators. Isolating poll+decode would require instrumenting the Kafka driver at the `poll()` call site |
| **5.8** | Recovery eviction via Tier 2 (shared volume) | Direct upload to Tier 3 (MinIO), bypassing Tier 2 | During recovery, the priority is freeing Tier 1 (RocksDB) space as fast as possible. Direct MinIO upload achieves this with minimal local I/O |
| **5.9** | Persistent `processed_corrections` table | In-memory `set[str]` | Corrections use idempotent incremental updates, so duplicate processing is harmless (same delta applied twice = same result). Persistence is an optimization, not a correctness requirement |
| **5.10** | Correction field name: `correction_timestamp` | `timestamp` | Shorter, consistent with other dataclass fields. A property alias can be added if strict JSON schema compatibility is required |
| **5.11** | Async backpressure queue (`asyncio.Queue`) | Synchronous processing with boolean gate at run.py loop | The run.py-level boolean gate provides equivalent throughput control without the complexity of async queue management |
| **5.12** | Merged sketch cache at 200ms intervals | `SlidingWindowDDSketch.quantile()` merges 60 sub-sketches on every call | The sub-sketch merge is O(60 * 1024) = ~61K operations, fast enough in Python for current throughput. Caching can be added if profiling shows a bottleneck |
| **5.13** | Single Dockerfile | Two Dockerfiles (`Dockerfile` Python 3.12, `deploy/Dockerfile` Python 3.10) | docker-compose references the deploy Dockerfile. Root Dockerfile is unused but retained for local development |

---

## 10. Pre-Production Checklist

Verify each item before going live. References in parentheses point to the operations
spec (`Trien_Khai_He_Thong.md`).

### 10.1 Strict Path

- [ ] Coordinator HA cluster deployed (3 instances) + failover tested (< 5s RTO)
- [ ] Output Exactly-Once verified (transactional sink or idempotent key)
- [ ] Tiered Storage 4-state eviction protocol tested end-to-end
- [ ] Strict failback state machine verified with coordinator leader switch
- [ ] Idempotent filter TTL working correctly
- [ ] Replay sub-checkpointing tested (crash mid-replay, resume from sub-checkpoint)
- [ ] DR procedure drilled (restore from MinIO `active_state_backup`)
- [ ] Heartbeat punctuation health monitoring active
- [ ] Clock skew alert configured and tested with injected skew
- [ ] Watermark lag metric exposed and alerted

### 10.2 Heuristic Path

- [ ] Aggregator HA deployed (2 instances, file lock or ZK lock)
- [ ] Cold start strategy verified with cold restart test
- [ ] DDSketch state size bounded (`max_buckets`, `MAX_LAG_ACCEPTED`)
- [ ] Adaptive percentile triggered and verified in burst test
- [ ] Negative lag handler tested with injected clock skew
- [ ] DLQ pipeline working end-to-end (produce -> consume -> correct)
- [ ] Correction protocol verified with downstream consumer
- [ ] Snapshot manager + rollback tested

### 10.3 Cluster-wide

- [ ] Capacity planning completed, headroom >= 30%
- [ ] All monitoring dashboards live and populated with real data
- [ ] Alert paging configured and tested (PagerDuty/Slack)
- [ ] Runbook reviewed and trained with on-call team
- [ ] All chaos tests passed (kill worker, kill coordinator, network partition, disk full)
- [ ] Replay test: >= 99.9999% match (Strict), >= 99% match (Heuristic)
- [ ] Load test: 5x burst passes with SLA targets
- [ ] Security checklist completed (TLS, auth, encryption at rest)
- [ ] Feature flags configured correctly for each environment
- [ ] Rollback procedure documented and tested
- [ ] Disaster recovery plan documented and drilled

### 10.4 Operational Readiness

- [ ] On-call rotation set up and published
- [ ] Escalation policy documented
- [ ] SLAs agreed with downstream consumer teams
- [ ] Incident communication plan in place
- [ ] Status page configured

---

## Appendix A: Environment Variable Quick Reference

All variables read by `refactor/common/config.py`. Values shown are defaults.

### Core Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `MODE` | `strict` | Processing mode: `strict`, `heuristic`, or `hybrid` |
| `ROLE` | `worker` | Component role: `coordinator`, `aggregator`, `worker`, `ingestor` |
| `PORT` | `8000` | HTTP server port for health, metrics, and Raft RPC |
| `WINDOW_SIZE_S` | `5.0` | Duration of each processing window in seconds |
| `DELTA_BASE_S` | `10.0` | Base watermark delay for strict mode |
| `PARTITIONS` | `0,1,2` | Partition IDs assigned to this worker |
| `TOTAL_PARTITIONS` | `12` | Total partition count across the cluster |
| `NODE_ID` | `0` | Unique worker identifier |

### Infrastructure

| Variable | Default | Description |
|----------|---------|-------------|
| `MINIO_ENDPOINT` | `""` (disabled) | MinIO server address (`host:port`) |
| `MINIO_ACCESS_KEY` | `""` | MinIO access key |
| `MINIO_SECRET_KEY` | `""` | MinIO secret key |
| `MINIO_BUCKET` | `csdlpt-windows` | MinIO bucket name |
| `MINIO_SECURE` | `false` | Use TLS for MinIO |
| `CHECKPOINT_DIR` | `/data/checkpoint` | RocksDB data directory |
| `AGGREGATOR_LOCK_PATH` | `/tmp/aggregator.lock` | File lock path for aggregator HA |
| `AGGREGATOR_HA_ENABLED` | `false` | Enable Active-Standby aggregator |
| `PAGERDUTY_ROUTING_KEY` | `""` | PagerDuty integration routing key |

### Kafka (when using kafka-python)

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | (none) | Comma-separated broker addresses |
| `KAFKA_ENABLE_REAL` | `false` | Switch from kafka_sim to real client |

### Performance

| Variable | Default | Description |
|----------|---------|-------------|
| `BACKPRESSURE_MAX_QUEUE` | `500` | Max queued events before pausing ingest |
| `BACKPRESSURE_RESUME_AT` | `100` | Resume ingest when queue drops below this |
| `HEARTBEAT_TIMEOUT_S` | `10.0` | Seconds before a worker is considered dead |
| `DR_BACKUP_INTERVAL_S` | `300.0` | Interval between DR snapshots to MinIO |

### Heuristic Parameters

| Variable | Default | Description |
|----------|---------|-------------|
| `HEURISTIC_ALPHA` | `0.01` | EMA smoothing factor for percentile tracking |
| `HEURISTIC_P_NORMAL` | `0.99` | Target quantile for normal operation |
| `HEURISTIC_P_SAFE` | `0.999` | Safe quantile used during burst/recovery |
| `HEURISTIC_L_MAX` | `60.0` | Maximum accepted lag in seconds |
| `HEURISTIC_WARMUP_S` | `10.0` | Warmup period for DDSketch |
| `HEURISTIC_WARMUP_SAMPLES` | `1000` | Minimum samples before sketch is considered ready |

### DLQ

| Variable | Default | Description |
|----------|---------|-------------|
| `DLQ_RETENTION_DAYS` | `7` | How long DLQ entries are retained |
| `DLQ_RETRY_BATCH_SIZE` | `100` | Batch size for DLQ retry processing |
| `DLQ_RETRY_INTERVAL_S` | `3600.0` | Interval between DLQ retry attempts |

### Feature Flags

| Variable | Default | Description |
|----------|---------|-------------|
| `ENABLE_TWO_PHASE_EVICTION` | `true` | 4-state eviction protocol |
| `ENABLE_ADAPTIVE_PERCENTILE` | `true` | Auto-adjust Heuristic percentile |
| `ENABLE_REPLAY_SUB_CHECKPOINTING` | `true` | Incremental checkpoints during replay |
| `ENABLE_NEGATIVE_LAG_RECALIBRATION` | `true` | Auto-correct negative lag |
| `ENABLE_HYBRID_ROUTING` | `false` | Dual-path event routing |
| `FAILOVER_ENABLED` | `false` | Auto-reassign partitions on worker failure |

---

## Appendix B: Quickstart (Simulation Mode)

For local development and testing with the simulation stack:

```bash
# Clone and build
cd /path/to/csdlpt
docker compose -f refactor/deploy/docker-compose.yml build

# Start strict mode (default)
MODE=strict docker compose -f refactor/deploy/docker-compose.yml up -d

# Check all services are healthy
docker compose -f refactor/deploy/docker-compose.yml ps

# Access monitoring
# Prometheus: http://localhost:9090
# Grafana:    http://localhost:3000  (admin / admin)
# MinIO:      http://localhost:9001  (minioadmin / minioadmin)

# Stream test data (optional)
docker compose -f refactor/deploy/docker-compose.yml --profile manual up ingestor

# Stop
docker compose -f refactor/deploy/docker-compose.yml down -v
```

---

**Last updated:** 2026-05-25
**Applies to:** csdlpt refactor (branch `rebuild/d1-skeleton`)
