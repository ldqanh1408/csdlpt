# DOCUMENT 04: DEPLOYMENT AND CONFIGURATION GUIDE

This document provides capacity planning guidelines, instructions for configuring production-ready infrastructure services (Kafka, ZooKeeper, MinIO), and a comprehensive directory of environment variables and feature flags for the distributed Stateful Stream Processing system (`refactor/`).

---

## 1. Capacity Planning

The following specifications are based on a production benchmark baseline of **100,000 logs/sec**, an average payload size of **500 bytes/log**, and a peak workload burst factor of **3x**:

### 1.1. Sizing Worker Nodes (4 Concurrent Instances)
Each worker is responsible for processing 3 partitions out of the total 12 Kafka partitions.
* **Estimated RAM Consumption**:
  $$\text{RAM}_{worker} = \text{Queue Buffer} (500 \times \text{log\_size}) + \text{RocksDB Block Cache} (64\text{MB} \times 3) + \text{DDSketch} (100\text{KB} \times 3) + \text{Overhead} (1\text{GB}) \approx 1.3\text{GB}$$
  *Recommendation*: Allocate **4 GB RAM** per worker to accommodate traffic spikes or historic data replay workloads.
* **Local SSD Storage (Tier 1)**: NVMe SSD storage is required to ensure sustained IOPS for RocksDB Write-Ahead Logs (WAL) and SST file operations.
  *Recommendation*: Allocate **50 GB NVMe SSD** per worker node.

### 1.2. Sizing Shared Volume (Tier 2) & Object Storage (Tier 3)
* **Shared Checkpoint Directory**: Stores coordinator cluster configuration state and RocksDB manifests.
  *Recommendation*: **100 GB Network SSD** (e.g., AWS EBS gp3 volume).
* **MinIO Object Storage**: Stores archived state files for closed windows to guarantee durability.
  *Recommendation*: Storage requirements scale with the state retention policy. For instance, assuming a 30-day retention policy:
  $$\text{Storage}_{MinIO} = 100\text{k logs/s} \times 500\text{B} \times 86400\text{s/day} \times 30\text{ days} \approx 130\text{ TB}$$

---

## 2. Production Infrastructure Setup (Kafka, ZooKeeper, MinIO)

### 2.1. Kafka Topic Configuration
Before starting the system in a production environment, initialize the Kafka topics using the following commands and configuration parameters:

```bash
# 1. Topic: events - Consumes raw log events from the Ingestor (12 partitions)
kafka-topics.sh --create --bootstrap-server kafka:9092 --topic events --partitions 12 --replication-factor 3 --config min.insync.replicas=2 --config retention.ms=604800000 --config compression.type=lz4

# 2. Topic: strict_results - Stores finalized aggregation outputs from Strict Mode (30-day retention)
kafka-topics.sh --create --bootstrap-server kafka:9092 --topic strict_results --partitions 12 --replication-factor 3 --config retention.ms=2592000000 --config compression.type=lz4

# 3. Topic: heuristic_results - Stores speculative window outputs from Heuristic Mode (7-day retention)
kafka-topics.sh --create --bootstrap-server kafka:9092 --topic heuristic_results --partitions 12 --replication-factor 3 --config retention.ms=604800000 --config compression.type=lz4

# 4. Topic: late_logs_dlq - Stores late-arriving log events captured by Heuristic workers (7-day retention)
kafka-topics.sh --create --bootstrap-server kafka:9092 --topic late_logs_dlq --partitions 12 --replication-factor 3 --config retention.ms=604800000
```

### 2.2. ZooKeeper Ensemble
* Deploy a minimum 3-node ZooKeeper ensemble to guarantee high availability (HA).
* ZooKeeper manages Aggregator HA active locks, coordinator leader elections, and shared orchestration configurations.

---

## 3. Activating and Transitioning to Production (Real Kafka & ZooKeeper)

The system supports transitioning from simulation-mode adapters (`kafka_sim.py` operating over HTTP and local file locks) to production-grade integrations via native adapters and leader election handlers (`kafka_real.py` and `zk_lock.py`).

### 3.1. Activating the Real Kafka Adapter
To direct the Ingestor and Worker processes to connect to a live Kafka cluster, configure the following environment variables:
* `KAFKA_ENABLE=true`: Enables Kafka event integration.
* `KAFKA_ENABLE_REAL=true`: Activates the production Kafka adapter (`kafka_real.py` using `kafka-python`) instead of the mock simulator.
* `KAFKA_BROKER_URL=localhost:9092` (or the network address of the Kafka cluster).

### 3.2. Activating ZooKeeper Leader Election for Aggregator HA
To configure the Active-Standby Aggregator instances to run high availability leader election via ZooKeeper:
* Set `ZK_HOSTS=localhost:2181` (or the list of ZooKeeper hosts).
* Once the `ZK_HOSTS` variable is detected, the Aggregator HA manager automatically spawns a `ZKLeaderElection` instance using `kazoo` to maintain ephemeral locks.

### 3.3. Activating ZooKeeper Leader Election for Coordinator HA
To configure the Coordinator HA cluster to leverage ZooKeeper instead of the internal HTTP-based Raft simulation:
* Set `ZK_ENSEMBLE=true` and configure `ZK_HOSTS=localhost:2181`.
* The coordinator instances will automatically acquire ephemeral locks at `/csdlpt/coordinator-lock` to determine the Active leader and Standby nodes.

---

## 4. Environment Variables & Feature Flags Directory

All configuration parameters are centralized in `refactor/common/config.py` and can be set using environment variables.

### 4.1. Core Configuration Variables
| Variable Name | Data Type | Default Value | Description |
|:---|:---|:---|:---|
| `MODE` | String | `strict` | Operational mode of the cluster: `strict`, `heuristic`, or `hybrid`. |
| `ROLE` | String | `worker` | Process role: `coordinator`, `aggregator`, `worker`, or `ingestor`. |
| `PORT` | Integer | `8000` | HTTP port for cross-process communication and Prometheus metrics scraping. |
| `NODE_ID` | String | (Auto-gen) | Unique identifier for the cluster node (e.g., `worker-1`). |
| `PARTITIONS` | String | `0,1,2` | Comma-separated list of Kafka partition IDs assigned to the Worker. |
| `TOTAL_PARTITIONS`| Integer | `12` | Total system partitions (default is 12). |
| `COORDINATOR_URL` | String | (Empty) | HTTP URL of the active Coordinator (used by workers to send heartbeats). |
| `AGGREGATOR_URL`  | String | `http://localhost:8001` | HTTP URL of the active Aggregator in Heuristic mode. |

### 4.2. Storage & State Management Configuration
| Variable Name | Data Type | Default Value | Description |
|:---|:---|:---|:---|
| `CHECKPOINT_DIR` | String | `/data/checkpoint` | Local directory path where RocksDB state files are persisted. |
| `DB_ENABLED` | Boolean | `true` | Enables or disables writing state metadata to local RocksDB. |
| `MINIO_ENDPOINT` | String | (Empty) | Network endpoint of the MinIO cluster. If left empty, Tier 3 storage is disabled. |
| `MINIO_ACCESS_KEY`| String | (Empty) | Access key for MinIO authentication. |
| `MINIO_SECRET_KEY`| String | (Empty) | Secret key for MinIO authentication. |
| `MINIO_BUCKET` | String | `csdlpt-windows` | S3 bucket name used to archive completed window states. |

### 4.3. Heuristic Engine & DLQ Configuration
| Variable Name | Data Type | Default Value | Description |
|:---|:---|:---|:---|
| `HEURISTIC_ALPHA` | Float | `0.01` | Relative error accuracy target ($\alpha = 1\%$) for the DDSketch. |
| `HEURISTIC_P_NORMAL`| Float | `0.99` | Lag percentile utilized during normal steady-state operations ($99\%$). |
| `HEURISTIC_P_SAFE` | Float | `0.999` | Safe lag percentile used when network spikes or bursts are detected ($99.9\%$). |
| `HEURISTIC_L_MAX` | Float | `60.0` | Conservative fallback lag duration used during the worker warm-up/cold-start phase. |
| `DLQ_RETENTION_DAYS`| Integer | `7` | Retention period (in days) for late logs stored in the DLQ topic. |
| `DLQ_RETRY_INTERVAL_S`| Float | `3600.0` | Execution interval (in seconds) for the late-log correction scheduler (default 1 hour). |

### 4.4. Feature Flags
| Variable Name | Data Type | Default Value | Description |
|:---|:---|:---|:---|
| `ENABLE_ADAPTIVE_PERCENTILE` | Boolean | `true` | Allows the heuristic engine to dynamically raise the DDSketch target quantile to $99.9\%$ during traffic surges. |
| `ENABLE_REPLAY_SUB_CHECKPOINTING`| Boolean | `true` | Automatically creates sub-checkpoints every 1000 events when replaying historical log data. |
| `ENABLE_TWO_PHASE_EVICTION` | Boolean | `true` | Activates the differentiated state eviction scheduler. |
| `ENABLE_NEGATIVE_LAG_RECALIBRATION`| Boolean | `true` | Enables recalibration of minor clock-skew anomalies (negative lag) to 0. |
| `ENABLE_HYBRID_ROUTING` | Boolean | `false` | Enables hybrid routing logic, separating streams by metadata values and HTTP status codes. |
