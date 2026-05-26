# DOCUMENT 03: HEURISTIC PATH DETAILED DESIGN

This document outlines the detailed design and specifications of the **Heuristic Watermark Path** within the distributed Stateful Stream Processing system. The heuristic path is optimized for **low-latency** execution. It leverages empirical statistical analysis of the incoming log stream to speculatively predict safe window closure times.

---

## 1. Heuristic Watermark Principle via Lag Estimation

When control signals like Punctuation Tokens are unavailable (e.g., when consuming from legacy upstream services where source code modifications are restricted), the system estimates watermarks based on local log transmission latency.

* **Log Lag Definition**: For each log event received at physical time `arrival_time` carrying an event timestamp `event_time`, the log latency is computed as:
  $$\text{lag} = \text{arrival\_time} - \text{event\_time}$$
* **Watermark Computation Formula**:
  $$W_h(t) = \max\left(W_h(t-1), \max(T_{event}) - L_{eff}(t)\right)$$
  Where:
  * $\max(T_{event})$ is the maximum Event-Time observed up to time $t$.
  * $L_{eff}(t)$ is the **Effective Lag** estimated from the empirical log delay distribution computed by the local DDSketch.

---

## 2. The DDSketch & Sliding Window DDSketch Algorithms

To estimate the effective lag ($L_{eff}$) accurately without persisting the entire history of log latencies in memory, the system employs **DDSketch** (introduced by Datadog in 2019).

### 2.1. Log-Scale Bucket Allocation in DDSketch
DDSketch divides latency values into log-scale buckets with a guaranteed relative error boundary $\alpha$ (defaulting to $\alpha = 0.01$, meaning a maximum of 1% deviation from the actual value):
* **Multiplier**: $\gamma = \frac{1+\alpha}{1-\alpha} \approx 1.0204$ (for $\alpha = 0.01$).
* **Bucket Index**:
  $$i = \left\lceil \frac{\ln(x / \text{min\_value})}{\ln(\gamma)} \right\rceil$$
  Where $x$ is the latency value and $\text{min\_value}$ is the minimum trackable latency (defaulting to 1ms).
* **Lower Bound**: $\text{value\_lower\_bound}(i) = \text{min\_value} \times \gamma^i$.
* **Complexity**: Inserting a value into the sketch takes $O(1)$ time, and querying a quantile takes $O(1)$ memory, with a strict maximum bound of 1024 active buckets.

### 2.2. SlidingWindowDDSketch (Dynamic Latency Estimation)
To adapt rapidly to transient network fluctuations and changing traffic patterns, the system uses a **SlidingWindowDDSketch** composed of 60 sub-sketches representing the last 60 seconds of processing:
* Each sub-sketch records latency samples for a 1-second interval.
* Every 1 second, the window slides forward, discarding the oldest sub-sketch and initializing a new active sub-sketch.
* Quantile calculations ($L_{eff} = \text{quantile}(p)$) merge the remaining 60 sub-sketches to compute the aggregate latency distribution.

---

## 3. Cold Start Management

Upon worker startup or failover recovery, the local DDSketch does not contain sufficient latency samples to compute a safe and stable $L_{eff}$ estimate. The **ColdStartManager** addresses this:

1. **Conservative Prior**: During the warm-up phase, the system fixes $L_{eff}$ to a conservative maximum lag (`HEURISTIC_L_MAX = 60.0` seconds) to prevent premature window closure and avoid losing out-of-order logs.
2. **Warm-up Exit Criteria**: The system transitions out of the cold start state only when both of the following conditions are met:
   * **Duration**: The elapsed uptime satisfies $\text{elapsed} \ge \text{warmup\_min\_seconds}$ (defaulting to 10.0 seconds).
   * **Sample Count**: The total collected lag samples satisfy $\text{samples} \ge \text{warmup\_min\_samples}$ (defaulting to 1000 samples).
3. **Baseline Restoration**: The system periodically backs up the latency baseline to MinIO. If a baseline exists, the worker can restore the baseline on startup, bypassing the cold start phase.

---

## 4. Clock Skew and Negative Lag Management

Due to clock desynchronization between log sources and the processing workers, some log events may arrive with an `event_time > arrival_time`, resulting in a negative lag ($\text{lag} < 0$). The **NegativeLagHandler** diagnoses and handles these anomalies across four tiers:

| Tier Level | Detection Condition | System Mitigation Action |
|:---|:---|:---|
| **TIER 1 (Minor Clock Skew)** | Negative lag $< 100\text{ms}$ | **Auto-Recalibrate**: Automatically shifts the latency value to 0 to allow standard DDSketch processing. |
| **TIER 2 (Moderate Clock Skew)** | Negative lag between $100\text{ms}$ and $1000\text{ms}$ | **Hold Watermark**: Freezes the local heuristic watermark progress $W_h$ for up to 5 seconds to wait for synchronization. |
| **TIER 3 (Severe Clock Skew)** | Negative lag between $1000\text{ms}$ and $5000\text{ms}$ | **Bypass & Fallback**: Suspends updates to the local DDSketch and falls back to a predefined static latency estimation (BOO fallback). |
| **TIER 4 (Clock Fault)** | Negative lag $> 5000\text{ms}$ | **System Alert**: Raises high-severity alerts indicating that NTP clock synchronization must be verified on the host system. |

---

## 5. Adaptive Percentile Controller

To maintain low latency under stable network conditions while preventing data loss during network spikes or bursts:

* **Steady State**: The system uses a default target percentile of $p = 0.99$ (allowing a theoretical $1\%$ late event loss rate to achieve an emission latency of $\le 3-5$ seconds).
* **Burst Detection**: If the local buffer queue length spikes or the late-data rate rises, the controller automatically triggers a safe fallback mode, raising the target percentile to $p_{safe} = 0.999$ (limiting late data to $0.1\%$).
* **Recovery Phase**: Once latency metrics and queue lengths return below warning thresholds for 3 consecutive monitoring intervals, the controller transitions back to the steady-state percentile ($p = 0.99$).

---

## 6. Dead Letter Queue & Correction Protocol

Any log event that arrives at a worker with an `event_time < W_global_h` (indicating it is late relative to a closed window) is routed to the **Dead Letter Queue (DLQ)**.

```
       Late-Arriving Log Event
                 │
                 ▼
       DLQPipeline.enqueue()
                 │
                 ▼ (Publish to Kafka Topic: late_logs_dlq)
       Periodic Correction Loop (Every 1 hour)
                 │
                 ▼
       compute_corrections()
                 │
                 ▼
       Generate CorrectionMessage
    (delta_count, late_log_ids, window_id)
                 │
                 ▼
       DownstreamEmitter
    (Publish corrections to downstream consumer applications)
                 │
                 ▼
    Run 24h FINAL Reconciliation Scheduler
      (Reconcile daily aggregates and close reports)
```

1. **DLQ Pipeline**: Late events are recorded in local RocksDB metadata before being published to the Kafka topic `late_logs_dlq` (configured with a 7-day retention period).
2. **Correction Protocol**: Every 1 hour (configured via `DLQ_RETRY_INTERVAL_S`), a background worker scans the DLQ logs, aggregates late events by their corresponding window boundaries, and emits `CorrectionMessage` payloads containing:
   * `window_id`: The identifier of the window being corrected.
   * `delta_count`: The number of records to add to the previous window aggregation.
   * `late_log_ids`: A list of the late log identifiers included in this correction.
3. **Downstream Emitter**: Publishes the correction messages to downstream consumers. Downstream consumers can handle corrections in one of two patterns:
   * **Incremental Update**: Add the delta value directly to the existing aggregates.
   * **Upsert/Replace**: Replace the previous window result with the new state, identified by an incremented `version` field.
4. **Reconciliation**: A daily reconciler job runs every 24 hours to compare database records and compile the final, absolute daily aggregates.
