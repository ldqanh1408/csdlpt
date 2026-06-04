# Data Completeness % vs Wait Time (ms) — heuristic mode

- Dataset: `nyc_taxi_events_half.csv`  ·  Punctuation: `max-event-time`  ·  Generated: 20260603_210530
- Swept variable: `HEURISTIC_P_NORMAL`  ·  Wait Time axis = realized effective lag L_eff measured from DDSketch.
- Completeness measured AFTER full dataset processed (EOF + settle), `completeness_avg_pct` = mean of steady-state samples.
- **Overall average completeness across all points: 91.899%**

| HEURISTIC_P_NORMAL | Wait Time (ms) | L_eff (ms) | Completeness avg % | Completeness final % | Late rate % | Received | Proc p99 (µs) |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.75 | 750 | 0.0 | 91.899 | 91.898 | 8.101 | 614,220 | 0.0 |
