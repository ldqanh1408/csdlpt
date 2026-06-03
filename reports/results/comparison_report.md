# Watermark Comparison Report
Generated: 2026-06-02 18:30:45

## Strict vs Heuristic Watermark

| Strategy | Mechanism | Guarantee | Latency |
|---|---|---|---|
| **Strict** | `W_global = min(LW_i)` via Raft Coordinator | 0% data loss | High (straggler-bound) |
| **Heuristic** | `W_h = max(T_event) - DDSketch.quantile(p)` + DLQ | <=1% immediate, 100% eventual | Low (adaptive) |

## Experiment Results

### Strict: No Docker experiment results (use --docker flag)

### Heuristic: No Docker experiment results (use --docker flag)

## Reference
- Dataset: `D:\dev\csdlpt\dataset\nyc_taxi_events_full.csv` (152 MB)
- Time compression: DIV=60 (31 days -> 12.4 hours)
- Windows: 5s tumbling, 12 partitions, 4 workers