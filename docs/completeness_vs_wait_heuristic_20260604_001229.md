# Data Completeness % vs Wait Time (ms) — heuristic mode

- Dataset: `nyc_taxi_events_sliced.csv`  ·  Punctuation: `max-event-time`  ·  Generated: 20260604_001229
- Swept variable: `HEURISTIC_P_NORMAL`  ·  Wait Time axis = realized effective lag L_eff measured from DDSketch.
- Completeness measured AFTER full dataset processed (EOF + settle), `completeness_avg_pct` = mean of steady-state samples.
- **Overall average completeness across all points: 91.019%**

| HEURISTIC_P_NORMAL | Wait Time (ms) | L_eff (ms) | Completeness avg % | Completeness final % | Late rate % | Received | Proc p99 (µs) |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.75 | 56788.6 | 56788.6 | 91.019 | 91.019 | 8.981 | 200,000 | 33803.8 |
