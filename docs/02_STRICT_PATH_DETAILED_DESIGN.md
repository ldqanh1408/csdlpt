# DOCUMENT 02: STRICT PATH DETAILED DESIGN

This document details the design and specifications of the **Strict Watermark Path** within the distributed Stateful Stream Processing system. The strict path guarantees **0% data loss** and ensures Exactly-Once processing semantics, making it suitable for critical tasks such as revenue reconciliation, billing, and financial auditing.

---

## 1. Punctuation-Based Watermark Principle

The Strict Watermark mechanism relies on stream control signals called **Punctuation Tokens**, which are injected directly by the Ingestor into the log stream.

```
Log Event Stream: [E1] -> [E2] -> [PunctuationToken(T_commit)] -> [E3] -> ...
```

* **PunctuationToken** contains a timestamp value $T_{commit}$ and its corresponding $partition\_id$. It acts as a commit guarantee: *"All events with Event-Time $\le T_{commit}$ on this partition have been fully emitted."*
* When a Worker receives a `PunctuationToken`, it updates the local watermark of that partition:
  $$W_{local} = T_{commit} - \delta_{base}$$
  Here, $\delta_{base}$ represents a configurable base delay offset to account for minor network jitters (defaulting to 10.0 seconds).
* After establishing a new $W_{local}$, the Worker performs the following actions:
  1. Drains the buffer queues containing out-of-order log events.
  2. Updates local tumbling windows.
  3. Speculatively closes and processes all windows where $window\_end \le W_{local}$.

### 1.1. Transmission Channels (In-Band vs Out-of-Band)

Depending on the deployment mode, the Punctuation Token is propagated using different channels to preserve ordering guarantees:
* **Kafka Mode (Production / HA Stack)**: The Punctuation Token is injected **in-band** into the `"events"` Kafka topic, routed to the target partition (`partition_id % 12`). Since Kafka guarantees FIFO ordering within each partition, the worker consumes and processes the punctuation token *after* processing all preceding log events for that partition. This eliminates race conditions and ensures a 100% completeness rate.
* **Simulation Mode (Local Sandbox)**: The Punctuation Token is sent **out-of-band** via a synchronous HTTP `POST /punctuation` request to the target Worker. Since the sandbox uses synchronous HTTP requests sequentially for ingest and punctuation, FIFO is preserved.

---

## 2. Cluster Coordination & Raft HA Protocol

In a distributed environment, the `coordinator` role coordinates partition assignment and aggregates local watermarks to compute the global watermark.

### 2.1. Global Watermark Synchronization ($W_{global}$)
Each worker periodically (every 200ms by default) sends a `WorkerHeartbeat` containing its active partition list and their respective local watermarks ($W_{local}$) to the Coordinator.
The Coordinator aggregates these and computes the Global Watermark $W_{global}$ as follows:
$$W_{global} = \max(W_{global\_prev}, \min_{i \in \text{Active Partitions}} (W_{local\_i}))$$
* **Computation Rule**: All partitions in `ACTIVE`, `STALE`, or `IDLE` states must be included in the minimum ($\min$) search. Only partitions belonging to workers explicitly marked as `FAILED` are excluded from the equation. This prevents the global watermark progress from stalling if a worker node crashes.
* **Monotonicity**: The outer $\max(W_{global\_prev}, ...)$ wrapper guarantees that $W_{global}$ is strictly monotonic and never regresses.

### 2.2. Coordinator HA & Fencing Token (Split-Brain Prevention)
In production deployments, the Coordinator is configured as a High Availability (HA) cluster of 3 nodes executing a Raft-based consensus protocol, or using ZooKeeper-based leader election (utilizing ephemeral nodes and locks).

* **Fencing Token**: Whenever a Coordinator assumes the leader role or the partition layout updates, a monotonically increasing integer called `fencing_token` is generated.
* This token is attached to every control message transmitted from the Coordinator to the Workers.
* If a Worker receives a command from a Coordinator with a `fencing_token` less than the maximum token it has recorded, it rejects the command immediately (`fencing violation`). This eliminates split-brain conditions caused by stale leaders sending delayed messages due to network partitioning or transient stalls.

---

## 3. Tiered State Management

Each worker node maintains window states locally using an embedded **RocksDB** instance on local SSD (Tier 1) and periodically flushes state data to **MinIO Object Storage** (Tier 3) to reclaim local disk storage and guarantee fault tolerance.

### 3.1. RocksDB Key-Value Schemas
Window state keys are segregated by partition to avoid overlaps:
* `ow:{window_start}`: Stores states of active, Open Windows.
* `cw:{window_start}`: Stores states of Closed Windows that have not yet been synchronized with Tier 3 storage.
* `si:{event_id}`: A Seen Set containing processed event IDs to perform event deduplication within a configured Time-To-Live (TTL).
* `meta:W_local`: Stores the current local watermark of the partition.

### 3.2. State Lifecycle (Eviction State Machine)
The worker cleans up local SSD states using a 4-step state machine:

```
[CLOSED] (Window closed locally)
   │
   ▼
[UPLOADING] (Uploading window state JSON to MinIO bucket)
   │
   ├─► Failure: Retry (up to 3 times with exponential backoff)
   ▼
[UPLOADED] (Upload completed successfully)
   │
   ▼
[PURGED] (Purge the corresponding state data from local RocksDB)
```

* **Differentiated Eviction**: For partitions undergoing error recovery, window state data is retained on the local SSD for an extended duration. This optimizes random read operations during recovery before finally evicting the state to MinIO.

---

## 4. Failover & Disaster Recovery Protocol

### 4.1. Worker Failover Protocol
The Coordinator monitors worker status via heartbeat messages.

```
   Worker stops sending heartbeats
                │
                ▼ (Wait for HEARTBEAT_TIMEOUT_S = 10.0s)
      Mark Worker as FAILED
                │
                ▼
      Invoke FailoverManager
                │
                ▼
   Deterministic Reassignment (Compute partition redistribution)
                │
                ▼
    Issue new Fencing Token -> Assign partitions to healthy Workers
                │
                ▼
    New Worker restores state from MinIO -> Resume processing
```

1. **Failure Detection**: If the Coordinator fails to receive a heartbeat from a Worker for 10.0 seconds, that Worker is marked as `FAILED`.
2. **Deterministic Reassignment**: The system redistributes the orphaned partitions using a deterministic partition assignment algorithm. This minimizes unnecessary partition shuffling among the remaining active workers.
3. **State Restoration**: The newly assigned worker fetches the latest partition checkpoint from MinIO, restores the local RocksDB state, and rewinds the Kafka consumer offsets to resume processing without record duplication or data loss.

### 4.2. Disaster Recovery (DR)
The Coordinator periodically backs up the partition assignments and the last recorded global watermark $W_{global}$ to MinIO (defaulting to every 5 minutes).
* **RTO (Recovery Time Objective)**: $\le 30$ minutes to fully rebuild the system state from a catastrophic loss of all local disks.
* **RPO (Recovery Point Objective)**: $\le 5$ minutes of watermark progress lag upon recovery (bound by backup intervals).
