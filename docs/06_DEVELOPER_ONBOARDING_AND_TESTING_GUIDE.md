# DOCUMENT 06: DEVELOPER ONBOARDING AND TESTING GUIDE

This document serves as a guide for software developers who wish to build, extend, or maintain the codebase in the `refactor/` directory. It details local development environment setup, the codebase directory structure, testing workflows, and command-line interfaces (CLI) for running system nodes.

---

## 1. Local Development Environment Setup

### System Prerequisites
* **Operating System**: Linux, macOS, or Windows (WSL2 or Git Bash recommended).
* **Python**: Version 3.8 or higher.
* **C++ Compiler**: Required to compile the embedded RocksDB wrapper library (`rocksdict`). On Ubuntu/Debian, install `build-essential`; on macOS, install Xcode Command Line Tools.

### Setup Instructions
1. Create a Python virtual environment to isolate dependencies:
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: .\venv\Scripts\activate
   ```
2. Install the required system dependencies:
   ```bash
   pip install --upgrade pip
   pip install -r refactor/requirements.txt
   ```
3. Install the testing framework libraries:
   ```bash
   pip install pytest pytest-asyncio
   ```

---

## 2. Directory Structure & Module Architecture

All processing logic for the stream processing system is self-contained within the `refactor/` directory:

```
refactor/
├── run.py                 # Main execution entrypoint; handles roles and configuration bootstrapping
├── common/                # Shared utilities and cluster-wide modules
│   ├── types.py           #   Data models, metrics structures, and enumeration definitions
│   ├── config.py          #   Centralized environment variable configuration loader
│   ├── rocks_store.py     #   RocksDB key-value wrapper for local SSD state persistence
│   ├── tiered_storage.py  #   MinIO S3 client integration and differentiated state eviction logic
│   ├── monitoring.py      #   Prometheus metrics exporter and collector registry
│   ├── alerting.py        #   Rule engine containing 15 auto-evaluating alerts
│   ├── kafka_sim.py       #   HTTP-based partition queue and log ingestion simulator
│   ├── kafka_real.py      #   Production Kafka producer/consumer client adapter (using kafka-python)
│   └── zk_lock.py         #   ZooKeeper lock manager for Aggregator High Availability
├── strict/                # Strict Watermark Path Engine
│   ├── engine.py          #   Punctuation-based window lifecycle engine
│   ├── worker.py          #   Multi-partition consumer worker controller
│   ├── coordinator.py     #   Watermark coordinator and cluster manager
│   └── failover.py        #   Heartbeat manager and deterministic partition reassigner
├── heuristic/             # Heuristic Watermark Path Engine
│   ├── engine.py          #   DDSketch quantile calculator and adaptive percentile controller
│   ├── dlq.py             #   Late-data queue, DLQ Kafka producer, and correction publisher
│   └── aggregator.py      #   Active-Standby Aggregator controller for heuristic path
├── hybrid/                # Hybrid Stream Routing Engine
│   └── router.py          #   HTTP status and priority metadata router
└── ddsketch/              # DDSketch Core Algorithm
    └── sketch.py          #   Log-scale DDSketch implementation
```

---

## 3. Testing Strategy

The repository contains an automated test suite composed of 11 test modules located in the `refactor/tests/` directory, organized into three validation layers:

### 3.1. Running the Test Suite
To execute the entire test suite with verbose output, run:
```bash
python3 -m pytest refactor/tests/ -v
```

### 3.2. Unit Tests
These tests validate individual algorithm logic and isolated engine behaviors:
* **DDSketch Math**: Verifies that the DDSketch quantile calculations adhere to the relative error bounds ($\alpha = 1\%$):
  ```bash
  python3 -m pytest refactor/tests/test_ddsketch.py -v
  ```
* **Strict & Heuristic Window Engines**: Tests window grouping, punctuation triggers, and speculative heuristic closures in isolation:
  ```bash
  python3 -m pytest refactor/tests/test_strict.py -v
  python3 -m pytest refactor/tests/test_heuristic.py -v
  ```
* **Deduplication & Backpressure**: Validates Exactly-Once processing logic and verify that workers pause consumer pipelines when internal queues saturate:
  ```bash
  python3 -m pytest refactor/tests/test_output_manager.py -v
  python3 -m pytest refactor/tests/test_backpressure.py -v
  ```

### 3.3. Integration Tests
Simulates end-to-end processing pipelines, starting from ingest ingestion, log partition distribution, state processing, and output delivery:
```bash
python3 -m pytest refactor/tests/test_integration.py -v
```

### 3.4. Chaos & Failover Tests
Simulates operational failures, such as immediate worker terminates, coordinator leader failures, and network partitioning. These tests ensure the system successfully recovers state files and does not suffer from data loss or record duplication:
```bash
python3 -m pytest refactor/tests/test_chaos.py -v
```

---

## 4. CLI Execution Guide

Developers can spin up individual nodes using the command-line interface provided by `run.py`.

### 4.1. Local Strict Mode Cluster (1 Coordinator, 1 Worker, 1 Ingestor)
Open three terminal windows and execute the commands in order:

```bash
# Terminal 1: Launch the Coordinator node
python3 -m refactor.run --role coordinator --mode strict --port 8000

# Terminal 2: Launch a Worker instance assigned to partitions 0, 1, and 2
python3 -m refactor.run --role worker --mode strict --port 8001 --partitions 0,1,2 --coordinator-url http://localhost:8000 --node-id strict-node-1

# Terminal 3: Run the Ingestor node to stream the dataset
python3 -m refactor.run --role ingestor --mode strict --source dataset/data.csv --node-hosts localhost:8001 --coordinator-url http://localhost:8000
```

### 4.2. Securing Services with TLS
To secure control plane communications between cluster components, you can pass certificates directly:
```bash
python3 -m refactor.run --role coordinator --mode strict --tls-cert /path/to/cert.pem --tls-key /path/to/key.pem
```
*Note*: Ensure that the TLS certificates are valid and that worker nodes are configured to trust the certificate authority (CA) signing the coordinator's certificates.
