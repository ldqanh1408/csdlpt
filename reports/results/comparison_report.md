# Watermark Comparison Report
Generated: 2026-06-02 18:30:45

## Strict vs Heuristic Watermark

| Strategy | Mechanism | Guarantee | Latency |
|---|---|---|---|
| **Strict** | `W_global = min(LW_i)` via Raft Coordinator | 0% data loss | High (straggler-bound) |
| **Heuristic** | `W_h = max(T_event) - DDSketch.quantile(p)` + DLQ | <=1% immediate, 100% eventual | Low (adaptive) |

## Experiment Results

### Strict (nyc_taxi_events_full.csv):
| DELTA_BASE_S | Wait Time (ms) | L_eff (ms) | Completeness avg % | Completeness final % | Late rate % | Received | Proc p99 (µs) |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.0 | 0 | 0.0 | 83.085 | 83.085 | 16.915 | 2,961,423 | 7752.8 |
| 5.0 | 5000 | 0.0 | 88.153 | 88.153 | 11.847 | 2,961,423 | 4666.4 |
| 10.0 | 10000 | 0.0 | 90.138 | 90.145 | 9.855 | 2,852,314 | 8592.7 |
| 20.0 | 20000 | 0.0 | 91.373 | 91.378 | 8.622 | 2,902,616 | 6443.7 |
| 40.0 | 40000 | 0.0 | 97.565 | 97.565 | 2.435 | 2,961,423 | 5884.8 |
| 60.0 | 60000 | 0.0 | 97.873 | 97.874 | 2.126 | 2,793,032 | 6257.8 |
| 90.0 | 90000 | 0.0 | 99.300 | 99.301 | 0.699 | 2,864,149 | 5912.2 |
| 120.0 | 120000 | 0.0 | 98.738 | 98.739 | 1.261 | 2,922,572 | 5101.1 |

### Heuristic (nyc_taxi_events_half.csv):
| HEURISTIC_P_NORMAL | Wait Time (ms) | L_eff (ms) | Completeness avg % | Completeness final % | Late rate % | Received | Proc p99 (µs) |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.75 | 750 | 0.0 | 91.899 | 91.898 | 8.101 | 614,220 | 0.0 |

## Reference
- Dataset: `D:\dev\csdlpt\dataset\nyc_taxi_events_full.csv` (152 MB)
- Time compression: DIV=60 (31 days -> 12.4 hours)
- Windows: 5s tumbling, 12 partitions, 4 workers