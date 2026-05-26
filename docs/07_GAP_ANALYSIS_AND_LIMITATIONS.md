# DOCUMENT 07: GAP ANALYSIS AND LIMITATIONS

This document summarizes the results of the audit between the system specifications (Spec) and the actual codebase, and details the monitoring/routing changes implemented to synchronize theory with practical operations.

---

## 1. Out-of-Scope Items

Certain requirements in the system specification were intentionally excluded to keep the codebase lightweight and independent of specific infrastructure providers:

### 1.1. Security Hardening & Encryption
* **Internal mTLS (Mutual TLS)**: Communication channels between Workers, Coordinators, and Aggregators utilize standard HTTP. While `run.py` supports wrapping sockets with TLS, automated certificate rotation using HashiCorp Vault or cert-manager is out of scope.
* **Kafka SASL/SCRAM Authentication**: The consumer codebase assumes Kafka operates without authentication or encryption mechanisms.
* **Encryption-at-Rest on MinIO**: The storage manager does not transmit HTTP headers requesting MinIO Server-Side Encryption (SSE-KMS) with KMS keys.
* **ZooKeeper ACL Configuration**: Znodes used for coordinator and aggregator leader elections do not configure Access Control Lists (ACLs). Any client connecting to the ZooKeeper ensemble has administrative rights to manipulate the locks.

### 1.2. Schema Evolution Infrastructure
* The codebase utilizes static `LogEvent` and `WindowResult` schemas represented as Python dataclasses. Integration with Confluent Schema Registry and the 4-step schema migration protocol (dual-write/dual-consume) to prevent data pipeline interruptions is not supported.

---

## 2. Docker Compose Configurations

To support different validation and sandbox scenarios, the project maintains two separate Docker Compose manifests:

### 2.1. Local Sandbox (`refactor/docker-compose.yml`)
* **Objective**: Provides a lightweight development environment on a local machine.
* **Characteristics**: Launches a single instance of the Coordinator, 3 Strict Workers, 1 Aggregator, 1 Heuristic Worker, and 1 Ingestor. It does not spin up observability dashboards (Prometheus/Grafana) or persistent cloud storage mocks (MinIO).

### 2.2. Full HA & Observability Stack (`refactor/deploy/docker-compose.yml`)
* **Objective**: Simulates a production-like environment with High Availability (HA) and full monitoring.
* **Characteristics**:
  * Runs a **3-node Coordinator cluster** with ZooKeeper-based leader election (`coordinator-1`, `coordinator-2`, `coordinator-3`).
  * Runs an Active-Standby **2-node Aggregator group** using ZooKeeper-based locks (`aggregator`, `aggregator-standby`).
  * Features integrated services for Kafka (`v7.5.0`), ZooKeeper (`v7.5.0`), Prometheus (`v2.50.0`), Grafana (`v10.3.0` pre-configured with custom dashboard layouts), and MinIO (`RELEASE.2024-01-16T16-07-38Z`).

---

## 3. Audited & Remediated Code Gaps

During codebase audits, several inconsistencies between the codebase implementations and specifications were identified and resolved:

### 3.1. Hybrid Router HTTP Status Resolution
* **Issue**: Previously, `HybridRouter._resolve_priority` only inspected the `priority` field in the event payload, neglecting the specification rule: *"HTTP log events representing system errors (`status >= 500`) must be prioritized and routed through the Strict Path."*
* **Remediation**: Added status code validation checks inside `_resolve_priority` in `refactor/hybrid/router.py`:
  ```python
  if event.status is not None and event.status >= 500:
      return EventPriority.CRITICAL
  ```
* **Verification**: Added `test_routes_status_500_to_critical` inside `refactor/tests/test_hybrid.py` to assert correct routing behavior.

### 3.2. Prometheus Scrape Configuration Targets
* **Issue**: The scrape targets in `refactor/deploy/prometheus.yml` were configured as static strings `"coordinator:8000"` and `"aggregator:8000"`. This caused Prometheus to omit secondary HA coordinators (`coordinator-2`, `coordinator-3`) and the Standby Aggregator node.
* **Remediation**: Updated the Prometheus configuration target endpoints to align with the Docker Compose service identifiers:
  * Coordinator scrape targets: `coordinator-1:8000`, `coordinator-2:8000`, `coordinator-3:8000`
  * Aggregator scrape targets: `aggregator:8000`, `aggregator-standby:8000`

### 3.3. Prometheus Negative Lag Alert Thresholds
* **Issue**: The alert rule definitions in `refactor/deploy/prometheus-rules.yml` configured both `NegativeLagRateHigh` (Warning) and `NegativeLagRateCritical` (Critical) alerts with the same conditional trigger expression: `expr: csdlpt_negative_lag_rate > 0.01` (1%). This defeated the purpose of tiered alerts.
* **Remediation**: Corrected the trigger expression for `NegativeLagRateCritical` to align with the Heuristic Path specifications:
  * Modified the threshold to fire only when `csdlpt_negative_lag_rate > 0.05` (5%).

### 3.4. Prometheus DLQ Critical Backlog Alert
* **Issue**: While the python implementation in `alerting.py` defined `dlq_backlog_critical` at a threshold of `> 50000` records, the corresponding `prometheus-rules.yml` only defined a `DLQBacklogHigh` (Warning) alert at `> 10000`, missing the critical warning level.
* **Remediation**: Appended the `DLQBacklogCritical` alert definition to target thresholds `> 50000` in `refactor/deploy/prometheus-rules.yml`.

### 3.5. ZooKeeper & Kafka Client Adapters Integration
* **Issue**: The initial version of the codebase operated solely on mock HTTP queues for Kafka and local file locks for Aggregator coordination. The Coordinator HA group elected leaders using basic HTTP pings.
* **Remediation**:
  * Implemented `refactor/common/kafka_real.py` (using `kafka-python`) to support production-grade Kafka consumption and production.
  * Implemented `refactor/common/zk_lock.py` (using `kazoo`) to support Active-Standby coordinator locking for Aggregator HA.
  * Embedded ZooKeeper-based leader election inside `refactor/strict/raft_coordinator.py` for Coordinator HA clusters.
  * Introduced the `KAFKA_ENABLE` configuration flag to activate Kafka-native streams throughout the cluster.

### 3.6. Kafka and ZooKeeper Services in Compose Manifest
* **Issue**: The `refactor/deploy/docker-compose.yml` file lacked service definitions for Kafka and ZooKeeper containers, preventing deployment on live containers.
* **Remediation**:
  * Added Kafka and ZooKeeper services to `refactor/deploy/docker-compose.yml`.
  * Injected environment variable overrides (`KAFKA_ENABLE: "true"`, `KAFKA_ENABLE_REAL: "true"`, `ZK_HOSTS: "zookeeper:2181"`) into Coordinator, Aggregator, Worker, and Ingestor service definitions.

### 3.7. In-Band Punctuation Propagation in Kafka Mode
* **Issue**: Originally, the Ingestor sent Punctuation Tokens (`PunctuationToken`) out-of-band via synchronous HTTP POST requests to the Workers. When running with Kafka enabled, this caused a race condition where the punctuation token (which traveled instantly over HTTP) overtook the log events (which traveled with latency over Kafka). As a result, the watermark at the Worker advanced prematurely, causing trailing events to be incorrectly dropped as late, which prevented completeness from reaching 100% on the dashboard UI.
* **Remediation**: Modified the ingestor's `send_punctuation` to publish punctuation tokens in-band directly to the `"events"` Kafka topic (properly partitioned) when Kafka is enabled. Strict workers in `kafka_poll_loop` now intercept these punctuation events, parse them, and call `worker.on_punctuation(token)` in the correct FIFO order. Heuristic workers in `kafka_heuristic_poll_loop` filter out and ignore these punctuation events to avoid validation failures.
* **Verification**: All 157 unit/integration tests continue to pass (using the HTTP fallback in simulation/sandbox mode), and manual runs with the NASA dataset show completeness reaching exactly 100% on the UI.
