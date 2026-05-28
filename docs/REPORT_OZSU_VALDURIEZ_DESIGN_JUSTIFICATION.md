# A Report Justifying Design Choices Using Özsu and Valduriez Theory

**Project**: Distributed Watermark Tracker / Log Delay Compensator  
**Design sources**:
- `docs/legacy/Thiet_Ke_Strict_Watermark.md`
- `docs/legacy/Thiet_Ke_Heuristic_Watermark.md`
- `docs/legacy/Trien_Khai_He_Thong.md`

---

## 1. Purpose of This Report

This report justifies the major design choices of the project using the theory of distributed database systems described by Özsu and Valduriez. The project is not a traditional relational distributed DBMS, but it has the same core distributed data management problems: data is fragmented across partitions, processed by multiple nodes, coordinated through global metadata, replicated for availability, recovered after failures, and exposed as one logical processing system.

The project contains two watermark designs:

1. **Strict Watermark**: prioritizes correctness, consistency, exactly-once processing, and zero late-data loss.
2. **Heuristic Watermark**: prioritizes low latency, local autonomy, adaptive estimation, and correction of late data through DLQ.

Both designs follow distributed database principles, but they choose different trade-offs between consistency, availability, latency, autonomy, and communication cost.

---

## 2. Özsu and Valduriez Theory Used as Evaluation Criteria

According to Özsu and Valduriez, distributed database design is commonly evaluated through the following concerns:

| Theory concept | Meaning in distributed database systems | Equivalent concern in this project |
|---|---|---|
| **Fragmentation / Partitioning** | Split data into fragments so each site processes only part of the global data. | Kafka partitions split the unbounded log stream across workers. |
| **Allocation** | Decide where fragments should be stored and processed. | Partitions are assigned to worker nodes; failed partitions can be reassigned. |
| **Replication** | Keep copies of data or metadata to improve availability and recovery. | Kafka replication, Coordinator HA, Aggregator HA, checkpoint replicas, Tier 2/Tier 3 state. |
| **Distributed query / processing** | Execute work across multiple sites while combining partial results. | Workers process local windows and watermarks; Coordinator/Aggregator computes global watermark. |
| **Distributed transaction / consistency** | Ensure correctness when multiple sites update shared logical state. | Exactly-once input/output, fencing token, failback state machine, idempotent output. |
| **Reliability and recovery** | Recover from node, storage, and communication failures. | Checkpoint, replay, DLQ, tiered storage, disaster recovery, failover. |
| **Transparency** | Hide distribution details from the user. | The user sees one logical stream-processing pipeline, not individual partitions and nodes. |
| **Communication cost** | Minimize network/control messages while preserving correctness. | Strict accepts higher coordination cost; Heuristic reduces blocking coordination. |
| **Site autonomy** | Allow sites to operate locally where possible. | Heuristic workers estimate watermarks locally with DDSketch. |
| **Performance trade-off** | Balance response time, throughput, consistency, and availability. | Strict favors correctness; Heuristic favors latency and adaptive performance. |

These concepts form the basis for the design justification below.

---

## 3. System Overview

The project solves a distributed stream-processing problem: web server logs arrive continuously, out of order, and sometimes late. The system must group logs into event-time windows and emit correct or near-correct window results.

The main components are:

- **Ingestor layer**: receives log events, assigns event-time, and emits records into Kafka.
- **Kafka cluster**: durable distributed log and partitioning layer.
- **Worker nodes**: process partitions, maintain window state, calculate local watermark, and emit results.
- **Coordinator / Aggregator**: combines local progress into a global watermark.
- **State storage**: RocksDB local state, shared checkpoint volume, and object storage for long-term recovery.
- **Output layer**: emits window results with exactly-once or correction semantics.

From Özsu and Valduriez's perspective, this architecture is a distributed data-management system with fragmented data, replicated metadata/state, distributed processing, recovery protocols, and global consistency requirements.

---

## 4. Strict Watermark Design Justification

### 4.1. Design Goal

The Strict Watermark design prioritizes correctness. A window is emitted only when the system can prove that all relevant partitions have progressed beyond that window. The design target is:

- 0% data loss caused by late arrival.
- Exactly-once input processing.
- Exactly-once or idempotent output.
- Strong recovery after worker/coordinator/storage failure.
- High availability through Coordinator HA and tiered state storage.

This design is justified when the application domain cannot tolerate missing or duplicated results, such as audit logs, billing, compliance reporting, and security monitoring.

### 4.2. Fragmentation and Parallel Processing

Özsu and Valduriez describe fragmentation as a core design step in distributed databases. The project applies this idea by splitting the log stream into Kafka partitions.

| Design choice | Theory justification |
|---|---|
| Kafka uses 12 partitions. | This is horizontal fragmentation of the event stream. Each partition contains a subset of the global log stream. |
| Four workers process assigned partition groups. | This is distributed processing over fragments. Each worker processes local state instead of forcing all logs through one node. |
| Each worker has isolated RocksDB instances per partition. | This preserves fragment independence and avoids lock contention between partitions. |

This choice improves throughput and scalability. It also gives the system a clear unit of ownership: the partition. Partition ownership is essential for recovery, failover, and exactly-once semantics.

### 4.3. Allocation and Reallocation

In distributed database design, allocation decides where each fragment lives. In this project, Kafka partitions are allocated to workers.

Strict Watermark extends allocation with controlled reallocation:

- If a worker fails, its partitions are reassigned.
- A new owner must load the latest checkpoint.
- The worker resumes from the correct Kafka offset.
- The Coordinator controls the transition through a state machine.

This follows the distributed database principle that fragment movement must preserve correctness. A partition must not be processed by two workers at the same time because that would create duplicate state updates and duplicate output.

### 4.4. Global Coordination and Strong Consistency

Strict Watermark requires a global decision: when is a window safe to close? Each worker only knows local progress. The Coordinator combines local watermarks:

```text
W_global = min(LW_i(P_k)) over valid active partitions
```

This is equivalent to distributed metadata coordination. In Özsu and Valduriez's terms, the system needs a global control component because correctness depends on the state of multiple sites.

| Strict component | Distributed DB equivalent |
|---|---|
| Local watermark `LW_i(P_k)` | Local site progress metadata |
| Global watermark `W_global` | Global consistency metadata |
| Coordinator Leader | Distributed transaction/metadata coordinator |
| Worker heartbeat | Site status and progress report |
| Fencing token / term | Protection against stale coordinators and split-brain |

The design deliberately accepts coordination overhead because the correctness requirement is stronger than the latency requirement.

### 4.5. Punctuation Token as Distributed Progress Metadata

Strict Watermark uses upstream Punctuation Tokens to indicate that a partition has logically advanced past a timestamp. Empty Punctuation Tokens are emitted even when a partition is idle.

This design addresses a known distributed-systems problem: absence of data is not proof of progress. Without punctuation, the Coordinator cannot distinguish:

- a partition that is truly idle,
- a partition whose ingestor has failed,
- a partition delayed by network or broker conditions.

Using punctuation is consistent with Özsu and Valduriez's emphasis on explicit metadata and coordination in distributed systems. The system avoids unsafe assumptions and uses explicit progress information to maintain correctness.

### 4.6. Replication and High Availability

Strict Watermark uses replication at several layers:

| Layer | Replication / redundancy mechanism | Purpose |
|---|---|---|
| Kafka | Broker replication | Durable input log and replay source |
| Coordinator | Raft / ZooKeeper-style HA | Avoid Coordinator single point of failure |
| State | Tier 2 shared checkpoints | Fast worker failover |
| Disaster recovery | Tier 3 object storage | Recovery when local/shared storage is lost |
| Output | Transactional or idempotent sink | Prevent duplicate emitted results |

This follows the reliability principle in distributed database systems: important data and metadata must survive site failures. Strict Watermark applies that principle to both processing state and coordination state.

### 4.7. Recovery and Exactly-Once Semantics

Özsu and Valduriez treat recovery as a fundamental requirement of distributed data systems. Strict Watermark implements recovery through:

- checkpointing active window state,
- storing Kafka offsets,
- replaying from offset after failure,
- deduplicating input events,
- fencing stale leaders,
- ensuring only one owner per partition,
- using exactly-once or idempotent output.

The failback state machine is especially important. When a failed worker returns, it cannot independently resume old partitions. The Coordinator must first pause the temporary owner, flush state, persist checkpoint metadata, transfer ownership, and only then allow the recovered worker to resume.

This preserves the invariant:

```text
At any time, each partition has exactly one valid processing owner.
```

That invariant is the foundation of exactly-once processing in this design.

### 4.8. Tiered State Storage

The design uses three storage tiers:

| Tier | Role | Theory justification |
|---|---|---|
| Tier 1: local RocksDB | Hot state for active windows | Data locality and fast local processing |
| Tier 2: shared volume | Warm checkpoint for fast failover | Replicated/recoverable state allocation |
| Tier 3: object storage | Cold archive and disaster recovery | Long-term reliability and storage scalability |

This matches distributed database storage principles: frequently accessed state should stay close to the processing site, while durable replicas should exist outside the failing node.

### 4.9. Communication Cost Trade-off

Strict Watermark requires:

- Worker heartbeats.
- Ingestor heartbeats.
- Punctuation Tokens.
- Coordinator broadcasts.
- State machine transitions.
- Raft/ZooKeeper coordination.

This increases communication cost. However, according to Özsu and Valduriez, communication cost must be evaluated against correctness and reliability goals. Strict Watermark chooses higher communication overhead to guarantee stronger consistency and failure recovery.

### 4.10. Strict Watermark Conclusion

Strict Watermark is justified by Özsu and Valduriez theory because it uses:

- fragmentation for scalability,
- allocation and controlled reallocation for partition ownership,
- replication for reliability,
- distributed coordination for global watermark consistency,
- recovery protocols for fault tolerance,
- exactly-once semantics for correctness,
- transparency to expose one logical pipeline.

The design is therefore appropriate when strong correctness is more important than low latency.

---

## 5. Heuristic Watermark Design Justification

### 5.1. Design Goal

The Heuristic Watermark design prioritizes low latency and adaptability. It does not wait for strict upstream punctuation or heavy global coordination. Instead, workers estimate lateness using DDSketch and produce a heuristic watermark.

The design target is:

- lower end-to-end latency,
- bounded loss or controlled late-data handling,
- adaptive watermark estimation,
- reduced dependency on strict upstream signals,
- late-data correction through DLQ.

This design is justified when the system must serve realtime dashboards, monitoring views, or operational analytics where fast results are more valuable than waiting for perfect completeness.

### 5.2. Fragmentation and Distributed Processing

Like Strict Watermark, Heuristic Watermark uses Kafka partitions and multiple workers. This still follows the fragmentation principle.

| Design choice | Theory justification |
|---|---|
| Kafka partitions split the log stream. | Horizontal fragmentation enables parallelism. |
| Workers process partitions independently. | Distributed processing reduces central bottlenecks. |
| Each worker estimates local lag. | Computation is pushed to the site where data is observed. |

The key difference is that Heuristic Watermark gives workers more autonomy. They do not need to wait for upstream Punctuation Tokens to estimate progress.

### 5.3. Site Autonomy

Özsu and Valduriez discuss site autonomy as an important property in distributed systems. Heuristic Watermark applies this directly:

- Each worker observes local arrival delay.
- Each worker updates its own DDSketch.
- Each worker computes a local heuristic watermark `W_h_i^{P_k}`.
- The Aggregator only merges local watermarks into `W_global_h`.

This reduces the role of the central coordinator. Workers can make progress based on local measurements, which improves responsiveness and reduces blocking.

### 5.4. DDSketch as Distributed Metadata

DDSketch is used to summarize lag distribution. This is not raw business data; it is metadata about stream behavior. In distributed database terms, it is a compact local summary that can be merged or compared across sites.

The choice is justified because DDSketch provides:

- bounded memory,
- efficient updates,
- quantile estimation,
- mergeability,
- suitability for heavy-tail delay distributions.

The system estimates:

```text
lag = arrival_time - T_event
L_eff = P_p(lag history)
W_h = max_event_time - L_eff
```

This is a deliberate approximate metadata design. It reduces coordination cost while still giving a principled estimate of event-time progress.

### 5.5. Aggregator HA and Reduced Coordination

Heuristic Watermark uses an Aggregator rather than the stronger Strict Coordinator. The Aggregator tracks per-partition heuristic watermarks and computes a global heuristic watermark:

```text
W_global_h = min(W_h_i)
```

This still preserves a global control point, but the control is lighter:

- It does not require upstream punctuation.
- It does not coordinate a strict failback state machine for every watermark decision.
- It can run Active-Standby with ZooKeeper lock.
- It focuses on global progress aggregation, not strict correctness proof.

This is justified by Özsu and Valduriez's communication-cost principle. When the application can tolerate correction, the system can reduce blocking coordination and improve latency.

### 5.6. Eventual Correction Instead of Immediate Strong Consistency

Heuristic Watermark may emit a window before all late data has arrived. This weakens immediate consistency. The design compensates with:

- DLQ for late logs,
- correction messages,
- downstream update patterns,
- final reconciliation,
- correction deduplication.

This is similar to eventual consistency and compensating transaction patterns in distributed systems. The result may be speculative at first, but the system records late data and can later correct the output.

| Problem | Heuristic solution |
|---|---|
| A late log arrives after watermark. | Route it to DLQ. |
| A previously emitted window is incomplete. | Compute correction. |
| Downstream receives correction more than once. | Use correction deduplication. |
| The initial result is not final. | Use versioning or final reconciliation. |

The design does not silently lose data. It changes the correctness model from **immediate strong consistency** to **eventual correction**.

### 5.7. Cold Start and Conservative Prior

At startup, DDSketch does not yet have enough samples. If the system trusts a small sample set, watermark may advance too aggressively and classify valid events as late.

The design introduces:

- warm-up phases,
- minimum sample count,
- minimum running time,
- conservative prior,
- baseline history.

This is justified as a reliability guard for approximate distributed metadata. Approximation is acceptable only if the system controls the period where statistics are not representative.

### 5.8. Negative Lag and Clock Skew Handling

Distributed systems often suffer from clock skew. Heuristic Watermark explicitly handles negative lag:

```text
lag = arrival_time - T_event
```

If `lag < 0`, the system identifies clock skew and avoids poisoning the lag estimator. This is consistent with distributed database theory because physical clocks across sites cannot be assumed perfectly synchronized. The design protects metadata quality before using it for global progress decisions.

### 5.9. Replay-Mode Snapshot and Rollback

During replay after failure, old events may appear to have very large lag. If these replay samples are inserted into DDSketch, the estimator becomes inaccurate.

The design handles this with:

- DDSketch snapshots,
- replay-mode detection,
- rollback to clean sketch state,
- pausing sketch updates during replay,
- recalibration after stable conditions return.

This is a recovery protocol for approximate metadata. It preserves estimator correctness across failure recovery, which is necessary because the watermark depends on the estimator.

### 5.10. Communication Cost Trade-off

Compared with Strict Watermark, Heuristic Watermark reduces:

- dependence on upstream Punctuation Tokens,
- blocking coordination,
- strict global proof before each window close,
- coordinator complexity.

The cost is weaker immediate consistency. However, the design compensates with DLQ and correction. This is a valid distributed design trade-off under Özsu and Valduriez theory: lower communication and lower latency can be chosen when the application semantics allow later reconciliation.

### 5.11. Heuristic Watermark Conclusion

Heuristic Watermark is justified by Özsu and Valduriez theory because it uses:

- fragmentation for scalability,
- local site autonomy for low latency,
- distributed processing of partition-local state,
- compact metadata estimation through DDSketch,
- lightweight global aggregation,
- recovery through snapshot and replay-mode handling,
- eventual correction through DLQ and correction messages.

The design is therefore appropriate when low latency and adaptive operation are more important than immediate strong consistency.

---

## 6. Comparative Analysis

| Aspect | Strict Watermark | Heuristic Watermark |
|---|---|---|
| Primary goal | Correctness and strong consistency | Low latency and adaptive performance |
| Watermark source | Upstream Punctuation Token and local watermarks | DDSketch lag estimation |
| Global control | Strong Coordinator | Lightweight Aggregator |
| Consistency model | Emit only when globally safe | Emit earlier, correct later |
| Late data handling | Avoid late-data loss by waiting | Route late data to DLQ and correction |
| Communication cost | Higher | Lower |
| Site autonomy | Lower; workers depend on global coordination | Higher; workers estimate locally |
| Recovery model | Checkpoint, replay, fencing, failback state machine | Snapshot, replay-mode rollback, DLQ reconciliation |
| Best fit | Audit, billing, compliance, security | Realtime dashboard, monitoring, near-realtime analytics |

The two designs are not contradictory. They represent two valid points in the distributed database design space:

- **Strict Watermark** chooses consistency and reliability.
- **Heuristic Watermark** chooses performance and autonomy.

Özsu and Valduriez theory supports both choices because distributed systems are not optimized along a single dimension. They must balance correctness, communication cost, availability, recovery, and performance based on application requirements.

---

## 7. Final Justification

The project's design choices are justified by distributed database theory as follows:

1. **The system uses fragmentation correctly** by splitting the unbounded log stream into Kafka partitions.
2. **The system uses distributed processing correctly** by assigning partitions to workers and keeping state local where possible.
3. **The system uses replication and checkpointing correctly** to survive failures of brokers, workers, coordinators, and storage layers.
4. **The system separates local and global metadata correctly** through local watermarks, global watermarks, DDSketch summaries, and coordinator/aggregator state.
5. **The system handles recovery explicitly** through replay, checkpoint, DLQ, fencing tokens, and failback state machines.
6. **The system acknowledges communication-cost trade-offs** by providing both a strict path and a heuristic path.
7. **The system provides transparency** because users interact with one logical stream-processing service even though the internal execution is distributed.

Therefore, the overall architecture follows the design principles of Özsu and Valduriez. The Strict Watermark path is theoretically justified as a consistency-first distributed design, while the Heuristic Watermark path is theoretically justified as a performance-first distributed design with controlled correction.

---

## 8. Reference

Özsu, M. T., and Valduriez, P. *Principles of Distributed Database Systems*. Springer.
