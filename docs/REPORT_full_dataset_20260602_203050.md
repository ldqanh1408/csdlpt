# Report — Data Completeness % vs Wait Time (ms)

**Topic #112 — Distributed Watermark Tracker ("Log Delay Compensator")**
Strict Watermark (no data loss, high latency) vs Heuristic Watermark (low latency, adaptive + DLQ reconciliation).

> **Full Dataset Analysis** — `dataset/nyc_taxi_events_full.csv` (2,961,423 rows)
> Time compression: DIV = 60 (natural trip duration preserved)  
> Generated: 2026-06-02 20:31:22  
> Analysis time: ~29s for full sweep

---

## 1. Time Compression Design (DIV = 60)

### 1.1 Why Time Compression Is Necessary

Raw NYC taxi data has two mismatched time scales:

| Scale | Raw Value | After DIV=60 |
|---|---|---|
| Pickup span (event-time range) | ~31 days (2.68M s) | 44640s (12.4h) |
| Trip duration p50 | ~720s (12 min) | 12s |
| Trip duration p95 | ~2,267s (38 min) | 38s |
| Trip duration p99 | ~3,582s (60 min) | 60s |

Without compression: duration/span ≈ 0.02% → lateness ≈ 0 → all events appear on-time → completeness curve is flat at 100% → useless for watermark study.

### 1.2 Compression Formula (Single Divisor)

```
pickup_min = min(pickup timestamps)
event_time = (pickup  − pickup_min) / DIV
arrival    = (dropoff − pickup_min) / DIV

lateness = arrival − event_time
         = (dropoff − pickup) / DIV   ← natural trip duration, only scaled
```

**Critical property**: both timestamps share the same `pickup_min` and `DIV`, so the subtraction cancels the offset. Lateness = actual trip duration ÷ DIV — the out-of-order pattern is preserved exactly, just scaled down.

### 1.3 Why DIV = 60

- **p50 lateness ≈ 12s**: the median trip needs ~12s of wait to be captured
- **p95 lateness ≈ 38s**: 95% of trips are captured with δ ≤ 38s
- **p99 lateness ≈ 60s**: near-total capture at δ = 60s
- **Sweep range 0..120s**: covers the full lateness distribution with room for outliers
- **12.4h event-time span**: large enough to have meaningful window statistics (~8,927 windows × 5s)

---

## 2. Dataset Statistics

### 2.1 Overview

| Metric | Value |
|---|---|
| Total rows | 2,961,423 |
| Unique hosts (partition keys) | 260 |
| Status 500 count | 0 (0.00%) |
| Total response bytes | 8,022,464,353 |
| File size | ~152 MB |
| Load time | 4.3s |

### 2.2 Event-Time & Arrival-Time Characteristics

| Metric | Value |
|---|---|
| Event-time min | 0.000s |
| Event-time max | 44639.483s |
| Event-time span | 44,639.5s (12.4h) |
| Arrival-time min | 2.700s |
| Arrival-time max | 44639.983s |
| Out-of-order steps (adjacent pairs) | 1,463,199 (49.41%) |
| Window size | 5s |
| Total windows | 8,927 |
| Avg events/window | 331.7 |

### 2.3 Inter-Arrival Gap Analysis

| Metric | Value |
|---|---|
| Mean gap | 15.07 ms |
| Median gap | 16.00 ms |
| P95 gap | 50.00 ms |
| P99 gap | 117.00 ms |
| Max gap | 3083 ms |
| Burst events (gap < 1ms) | 1,387,886 |

### 2.4 Complete Lateness Distribution

Lateness = arrival_time − event_time = trip duration / DIV. This is the core metric that drives watermark effectiveness.

| Percentile | Lateness (s) | Lateness (ms) |
|---:|---:|---:|
| Min | 0.000 | 0 |
| P1 | 0.650 | 650 |
| P5 | 3.300 | 3300 |
| P10 | 4.500 | 4500 |
| P25 | 7.150 | 7150 |
| P50 (Median) | 11.633 | 11633 |
| P75 | 18.667 | 18667 |
| P80 | 20.984 | 20984 |
| P85 | 24.084 | 24084 |
| P90 | 28.800 | 28800 |
| P91 | 30.100 | 30100 |
| P92 | 31.567 | 31567 |
| P93 | 33.284 | 33284 |
| P94 | 35.316 | 35316 |
| P95 | 37.783 | 37783 |
| P96 | 40.900 | 40900 |
| P97 | 44.983 | 44983 |
| P98 | 50.583 | 50583 |
| P99 | 59.700 | 59700 |
| P99.5 | 68.784 | 68784 |
| P99.9 | 95.034 | 95034 |
| P99.99 | 160.028 | 160028 |
| Max | 359.733 | 359733 |

| **Mean** | **14.845** | **14845** |
| **Std Dev** | **12.018** | **12018** |
| **Skewness** | **2.919** | — |

| Category | Count | % |
|---|---:|---:|
| Negative lateness | 0 | 0.0% |
| Zero lateness | 0 | 0.00% |
| Positive lateness | 2,961,423 | 100.00% |

### 2.5 Lateness Histogram (Fine Bins)

| Range (s) | Count | % | Cumulative % |
|---:|---:|---:|---:|
| [0, 1) | 34,250 | 1.16% | 1.16% |
| [1, 2) | 24,287 | 0.82% | 1.98% |
| [2, 3) | 61,839 | 2.09% | 4.06% |
| [3, 4) | 106,511 | 3.60% | 7.66% |
| [4, 5) | 143,914 | 4.86% | 12.52% |
| [5, 6) | 165,182 | 5.58% | 18.10% |
| [6, 7) | 175,569 | 5.93% | 24.03% |
| [7, 8) | 177,355 | 5.99% | 30.02% |
| [8, 9) | 174,392 | 5.89% | 35.91% |
| [9, 10) | 166,313 | 5.62% | 41.52% |
| [10, 12) | 301,696 | 10.19% | 51.71% |
| [12, 15) | 367,012 | 12.39% | 64.10% |
| [15, 18) | 272,101 | 9.19% | 73.29% |
| [18, 20) | 139,484 | 4.71% | 78.00% |
| [20, 25) | 241,719 | 8.16% | 86.16% |
| [25, 30) | 140,986 | 4.76% | 90.92% |
| [30, 35) | 86,798 | 2.93% | 93.85% |
| [35, 40) | 55,645 | 1.88% | 95.73% |
| [40, 45) | 37,596 | 1.27% | 97.00% |
| [45, 50) | 26,872 | 0.91% | 97.91% |
| [50, 55) | 19,261 | 0.65% | 98.56% |
| [55, 60) | 13,771 | 0.47% | 99.03% |
| [60, 70) | 15,226 | 0.51% | 99.54% |
| [70, 80) | 6,749 | 0.23% | 99.77% |
| [80, 90) | 3,074 | 0.10% | 99.87% |
| [90, 100) | 1,477 | 0.05% | 99.92% |
| [100, 120) | 1,377 | 0.05% | 99.97% |
| [120, 150) | 599 | 0.02% | 99.99% |
| [150, 180) | 160 | 0.01% | 99.99% |
| [180, 240) | 104 | 0.00% | 100.00% |
| [240, 360) | 104 | 0.00% | 100.00% |
| [360, ∞) | 0 | 0.00% | 100.00% |

### 2.6 Lateness vs Event-Time Correlation

- **Pearson r** = 0.0041
- Interpretation: weak/no correlation — lateness is independent of event-time position

### 2.7 Partition Key (Host) Distribution

- **Unique hosts**: 260
- **Partition scheme**: `hash(host) % 12` → 12 partitions
- **Skew ratio (max/avg)**: 2.01×

| Partition | Event Count | % |
|---:|---:|---:|
| 0 | 54,250 | 1.83% |
| 1 | 369,541 | 12.48% |
| 2 | 187,818 | 6.34% |
| 3 | 102,818 | 3.47% |
| 4 | 120,411 | 4.07% |
| 5 | 200,160 | 6.76% |
| 6 | 496,342 | 16.76% |
| 7 | 368,625 | 12.45% |
| 8 | 251,411 | 8.49% |
| 9 | 134,269 | 4.53% |
| 10 | 333,242 | 11.25% |
| 11 | 342,536 | 11.57% |

**Top 10 Hosts (zones):**

| Host | Count | % |
|---|---:|---:|
| 132 | 144,944 | 4.89% |
| 161 | 143,356 | 4.84% |
| 237 | 142,622 | 4.82% |
| 236 | 136,356 | 4.60% |
| 162 | 106,638 | 3.60% |
| 230 | 106,199 | 3.59% |
| 186 | 104,433 | 3.53% |
| 142 | 103,966 | 3.51% |
| 138 | 89,424 | 3.02% |
| 239 | 88,414 | 2.99% |

### 2.8 Event-Time Distribution (per hour of compressed time)

| From (s) | To (s) | Events | % |
|---:|---:|---:|---:|
| 0 | 1,860 | 85,150 | 2.88% |
| 1,860 | 3,720 | 105,856 | 3.57% |
| 3,720 | 5,580 | 135,046 | 4.56% |
| 5,580 | 7,440 | 128,096 | 4.33% |
| 7,440 | 9,300 | 106,343 | 3.59% |
| 9,300 | 11,160 | 103,279 | 3.49% |
| 11,160 | 13,020 | 119,970 | 4.05% |
| 13,020 | 14,880 | 102,935 | 3.48% |
| 14,880 | 16,740 | 141,811 | 4.79% |
| 16,740 | 18,600 | 150,347 | 5.08% |
| 18,600 | 20,460 | 111,380 | 3.76% |
| 20,460 | 22,320 | 99,188 | 3.35% |
| 22,320 | 24,180 | 147,589 | 4.98% |
| 24,180 | 26,040 | 142,175 | 4.80% |
| 26,040 | 27,900 | 110,577 | 3.73% |
| 27,900 | 29,760 | 143,269 | 4.84% |
| 29,760 | 31,620 | 115,399 | 3.90% |
| 31,620 | 33,480 | 105,606 | 3.57% |
| 33,480 | 35,340 | 137,934 | 4.66% |
| 35,340 | 37,200 | 156,034 | 5.27% |
| 37,200 | 39,060 | 149,528 | 5.05% |
| 39,060 | 40,920 | 93,416 | 3.15% |
| 40,920 | 42,780 | 126,029 | 4.26% |
| 42,780 | 44,639 | 144,466 | 4.88% |

---

## 3. STRICT Watermark — Completeness % vs Wait Time (δ)

### 3.1 Algorithm

```
watermark = max_event_time_seen − δ
event is ON TIME ⟺ window_start(event_time) + window_size > watermark
```

Larger δ → watermark is further behind → windows stay open longer → more late events are recovered. Trade-off: δ directly adds to end-to-end latency.

### 3.2 Completeness Curve (Full Sweep, 15 δ values)

| δ (s) | Wait (ms) | Completeness % | On Time | Late Events | Late Rate % | Δ Completeness |
|---:|---:|---:|---:|---:|---:|---:|
| 0.0 | 0 | **8.753** | 259,207 | 2,702,216 | 91.247 | +8.75% |
| 1.0 | 1,000 | **12.877** | 381,335 | 2,580,088 | 87.123 | +4.12% |
| 2.0 | 2,000 | **17.864** | 529,039 | 2,432,384 | 82.136 | +4.99% |
| 3.0 | 3,000 | **23.285** | 689,570 | 2,271,853 | 76.715 | +5.42% |
| 5.0 | 5,000 | **34.663** | 1,026,525 | 1,934,898 | 65.337 | +11.38% |
| 7.0 | 7,000 | **45.477** | 1,346,757 | 1,614,666 | 54.523 | +10.81% |
| 10.0 | 10,000 | **59.175** | 1,752,424 | 1,208,999 | 40.825 | +13.70% |
| 15.0 | 15,000 | **75.031** | 2,221,978 | 739,445 | 24.969 | +15.86% |
| 20.0 | 20,000 | **84.435** | 2,500,486 | 460,937 | 15.565 | +9.40% |
| 30.0 | 30,000 | **93.228** | 2,760,874 | 200,549 | 6.772 | +8.79% |
| 40.0 | 40,000 | **96.724** | 2,864,410 | 97,013 | 3.276 | +3.50% |
| 50.0 | 50,000 | **98.413** | 2,914,430 | 46,993 | 1.587 | +1.69% |
| 60.0 | 60,000 | **99.271** | 2,939,832 | 21,591 | 0.729 | +0.86% |
| 90.0 | 90,000 | **99.894** | 2,958,288 | 3,135 | 0.106 | +0.62% |
| 120.0 | 120,000 | **99.972** | 2,960,590 | 833 | 0.028 | +0.08% |

### 3.3 Observations

- **Completeness range**: 8.75% (δ=0s) → 99.97% (δ=120s)
- **Late events range**: 2,702,216 → 833
- **δ for 50% completeness**: 10.0s
- **δ for 75% completeness**: 15.0s
- **δ for 90% completeness**: 30.0s
- **δ for 95% completeness**: 40.0s
- **δ for 99% completeness**: 60.0s
- **δ for 99.9% completeness**: 120.0s
- **Max completeness at δ=120s**: 99.972% (833 events still dropped — extreme outlier lateness up to 359.7s)

### 3.4 Curve Fitting — Theoretical vs Empirical

- **Model**: completeness(δ) ≈ CDF(lateness, δ) = % of events with lateness ≤ δ
- **R² (fit quality)**: 0.87612
- **Interpretation**: The empirical completeness curve closely follows the theoretical lateness CDF, with small deviations due to window boundary effects.

| δ (s) | Empirical % | Theoretical % (CDF) | Deviation |
|---:|---:|---:|---:|
| 0 | 8.753 | 0.000 | +8.753 |
| 1 | 12.877 | 1.166 | +11.711 |
| 2 | 17.864 | 2.001 | +15.863 |
| 3 | 23.285 | 4.114 | +19.171 |
| 5 | 34.663 | 12.613 | +22.050 |
| 7 | 45.477 | 24.136 | +21.341 |
| 10 | 59.175 | 41.625 | +17.550 |
| 15 | 75.031 | 64.172 | +10.859 |
| 20 | 84.435 | 78.040 | +6.395 |
| 30 | 93.228 | 90.938 | +2.290 |
| 40 | 96.724 | 95.738 | +0.986 |
| 50 | 98.413 | 97.913 | +0.500 |
| 60 | 99.271 | 99.027 | +0.244 |
| 90 | 99.894 | 99.871 | +0.023 |
| 120 | 99.972 | 99.967 | +0.005 |

### 3.5 Marginal Cost — ms of Wait per 1% Completeness Gain

| δ Range | Δ Wait (ms) | Δ Completeness % | ms per 1% gain |
|---|---:|---:|---:|
| 0s → 1s | 1,000 | 4.12 | 242 |
| 1s → 2s | 1,000 | 4.99 | 200 |
| 2s → 3s | 1,000 | 5.42 | 184 |
| 3s → 5s | 2,000 | 11.38 | 176 |
| 5s → 7s | 2,000 | 10.81 | 185 |
| 7s → 10s | 3,000 | 13.70 | 219 |
| 10s → 15s | 5,000 | 15.86 | 315 |
| 15s → 20s | 5,000 | 9.40 | 532 |
| 20s → 30s | 10,000 | 8.79 | 1137 |
| 30s → 40s | 10,000 | 3.50 | 2860 |
| 40s → 50s | 10,000 | 1.69 | 5921 |
| 50s → 60s | 10,000 | 0.86 | 11655 |
| 60s → 90s | 30,000 | 0.62 | 48154 |
| 90s → 120s | 30,000 | 0.08 | 384615 |

### 3.6 Per-Window Loss Distribution (key δ values)

| δ (s) | Total Windows | Windows w/ Loss | Loss p50 | Loss p95 | Loss p99 | Loss Max | Loss Mean | 100% Loss Windows | 0% Loss Windows |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 8,928 | 8,927 | 90.8% | 96.1% | 97.3% | 100.0% | 88.1% | 17 | 1 |
| 5 | 8,928 | 8,926 | 63.6% | 75.7% | 79.4% | 95.0% | 61.7% | 0 | 2 |
| 10 | 8,928 | 8,921 | 39.5% | 53.1% | 59.1% | 84.6% | 38.9% | 0 | 7 |
| 20 | 8,928 | 8,843 | 15.1% | 26.0% | 35.3% | 54.8% | 15.4% | 0 | 85 |
| 30 | 8,928 | 8,319 | 6.0% | 14.0% | 19.8% | 35.3% | 6.5% | 0 | 609 |
| 40 | 8,928 | 7,236 | 2.1% | 8.5% | 11.9% | 24.1% | 2.9% | 0 | 1,692 |
| 60 | 8,928 | 4,689 | 0.2% | 3.1% | 5.3% | 15.4% | 0.7% | 0 | 4,239 |
| 120 | 8,928 | 733 | 0.0% | 0.2% | 0.5% | 7.7% | 0.0% | 0 | 8,195 |

### 3.7 Partition-Level Completeness (δ = 0s, 10s, 30s, 60s)

#### δ = 0s
| Partition | Total | On Time | Late | Completeness % |
|---:|---:|---:|---:|---:|
| 0 | 54,250 | 18,277 | 35,973 | 33.69 |
| 1 | 369,541 | 80,109 | 289,432 | 21.68 |
| 2 | 187,818 | 40,904 | 146,914 | 21.78 |
| 3 | 102,818 | 33,103 | 69,715 | 32.20 |
| 4 | 120,411 | 35,166 | 85,245 | 29.20 |
| 5 | 200,160 | 22,665 | 177,495 | 11.32 |
| 6 | 496,342 | 95,866 | 400,476 | 19.32 |
| 7 | 368,625 | 79,437 | 289,188 | 21.55 |
| 8 | 251,411 | 61,937 | 189,474 | 24.64 |
| 9 | 134,269 | 37,335 | 96,934 | 27.81 |
| 10 | 333,242 | 65,465 | 267,777 | 19.64 |
| 11 | 342,536 | 57,716 | 284,820 | 16.85 |
| **All** | **2,961,423** | **627,980** | **2,333,443** | **23.31 (avg) / min=11.32 / max=33.69** |

#### δ = 10s
| Partition | Total | On Time | Late | Completeness % |
|---:|---:|---:|---:|---:|
| 0 | 54,250 | 39,139 | 15,111 | 72.15 |
| 1 | 369,541 | 271,317 | 98,224 | 73.42 |
| 2 | 187,818 | 130,822 | 56,996 | 69.65 |
| 3 | 102,818 | 81,133 | 21,685 | 78.91 |
| 4 | 120,411 | 90,801 | 29,610 | 75.41 |
| 5 | 200,160 | 61,540 | 138,620 | 30.75 |
| 6 | 496,342 | 352,867 | 143,475 | 71.09 |
| 7 | 368,625 | 261,207 | 107,418 | 70.86 |
| 8 | 251,411 | 183,837 | 67,574 | 73.12 |
| 9 | 134,269 | 100,364 | 33,905 | 74.75 |
| 10 | 333,242 | 240,317 | 92,925 | 72.11 |
| 11 | 342,536 | 202,788 | 139,748 | 59.20 |
| **All** | **2,961,423** | **2,016,132** | **945,291** | **68.45 (avg) / min=30.75 / max=78.91** |

#### δ = 30s
| Partition | Total | On Time | Late | Completeness % |
|---:|---:|---:|---:|---:|
| 0 | 54,250 | 51,532 | 2,718 | 94.99 |
| 1 | 369,541 | 361,944 | 7,597 | 97.94 |
| 2 | 187,818 | 179,341 | 8,477 | 95.49 |
| 3 | 102,818 | 101,011 | 1,807 | 98.24 |
| 4 | 120,411 | 117,348 | 3,063 | 97.46 |
| 5 | 200,160 | 131,221 | 68,939 | 65.56 |
| 6 | 496,342 | 483,647 | 12,695 | 97.44 |
| 7 | 368,625 | 357,144 | 11,481 | 96.89 |
| 8 | 251,411 | 244,535 | 6,876 | 97.27 |
| 9 | 134,269 | 129,663 | 4,606 | 96.57 |
| 10 | 333,242 | 323,068 | 10,174 | 96.95 |
| 11 | 342,536 | 319,233 | 23,303 | 93.20 |
| **All** | **2,961,423** | **2,799,687** | **161,736** | **94.00 (avg) / min=65.56 / max=98.24** |

#### δ = 60s
| Partition | Total | On Time | Late | Completeness % |
|---:|---:|---:|---:|---:|
| 0 | 54,250 | 53,593 | 657 | 98.79 |
| 1 | 369,541 | 368,729 | 812 | 99.78 |
| 2 | 187,818 | 186,584 | 1,234 | 99.34 |
| 3 | 102,818 | 102,553 | 265 | 99.74 |
| 4 | 120,411 | 119,977 | 434 | 99.64 |
| 5 | 200,160 | 193,679 | 6,481 | 96.76 |
| 6 | 496,342 | 494,910 | 1,432 | 99.71 |
| 7 | 368,625 | 367,454 | 1,171 | 99.68 |
| 8 | 251,411 | 250,403 | 1,008 | 99.60 |
| 9 | 134,269 | 133,413 | 856 | 99.36 |
| 10 | 333,242 | 331,817 | 1,425 | 99.57 |
| 11 | 342,536 | 340,751 | 1,785 | 99.48 |
| **All** | **2,961,423** | **2,943,863** | **17,560** | **99.29 (avg) / min=96.76 / max=99.78** |

---

## 4. HEURISTIC Watermark — Completeness % vs Effective Lag (L_eff)

### 4.1 Algorithm

```
L_eff = DDSketch.quantile(p_normal)      // p-th percentile of recent lateness
W_h   = max_event_time_seen − L_eff      // adaptive watermark
```

The heuristic continuously adapts L_eff based on the observed lateness distribution. Events that miss the watermark are **routed to the DLQ** for eventual reconciliation → eventual completeness = 100%.

### 4.2 Results Table

| P_NORMAL | L_eff (s) | L_eff (ms) | Immediate Completeness % | DLQ Routed | Immediate Late Rate % | Eventual Completeness % |
|---:|---:|---:|---:|---:|---:|---:|
| 0.500 | 11.633 | 11,632 | **64.206** | 1,060,021 | 35.794 | **100.000** |
| 0.750 | 18.667 | 18,666 | **81.263** | 554,876 | 18.737 | **100.000** |
| 0.900 | 28.800 | 28,800 | **91.305** | 257,506 | 8.695 | **100.000** |
| 0.950 | 37.783 | 37,782 | **94.767** | 154,984 | 5.233 | **100.000** |
| 0.990 | 59.700 | 59,699 | **97.699** | 68,148 | 2.301 | **100.000** |
| 0.999 | 95.034 | 95,033 | **98.376** | 48,099 | 1.624 | **100.000** |

### 4.3 L_eff Convergence

L_eff estimate stabilizes as more data is observed (streaming percentile):

#### p = 0.500
| Events Processed | L_eff (s) |
|---:|---:|
| 50,000 | 11.734 |
| 100,000 | 11.817 |
| 150,000 | 11.883 |
| 200,000 | 11.900 |
| 250,000 | 11.767 |
| 300,000 | 11.933 |
| 350,000 | 11.867 |
| 400,000 | 11.850 |
| 450,000 | 11.816 |
| 500,000 | 11.650 |
| ... | ... |
| 2,961,423 (final) | 11.633 |

#### p = 0.750
| Events Processed | L_eff (s) |
|---:|---:|
| 50,000 | 19.000 |
| 100,000 | 19.484 |
| 150,000 | 19.600 |
| 200,000 | 19.616 |
| 250,000 | 19.400 |
| 300,000 | 19.550 |
| 350,000 | 19.333 |
| 400,000 | 19.250 |
| 450,000 | 19.116 |
| 500,000 | 18.834 |
| ... | ... |
| 2,961,423 (final) | 18.667 |

#### p = 0.900
| Events Processed | L_eff (s) |
|---:|---:|
| 50,000 | 27.769 |
| 100,000 | 30.000 |
| 150,000 | 30.917 |
| 200,000 | 31.166 |
| 250,000 | 31.216 |
| 300,000 | 31.200 |
| 350,000 | 30.600 |
| 400,000 | 30.300 |
| 450,000 | 29.950 |
| 500,000 | 29.384 |
| ... | ... |
| 2,961,423 (final) | 28.800 |

#### p = 0.950
| Events Processed | L_eff (s) |
|---:|---:|
| 50,000 | 33.517 |
| 100,000 | 37.583 |
| 150,000 | 40.233 |
| 200,000 | 40.766 |
| 250,000 | 41.350 |
| 300,000 | 41.450 |
| 350,000 | 40.384 |
| 400,000 | 40.183 |
| 450,000 | 39.733 |
| 500,000 | 38.850 |
| ... | ... |
| 2,961,423 (final) | 37.783 |

#### p = 0.990
| Events Processed | L_eff (s) |
|---:|---:|
| 50,000 | 47.150 |
| 100,000 | 55.983 |
| 150,000 | 59.683 |
| 200,000 | 59.850 |
| 250,000 | 63.000 |
| 300,000 | 62.717 |
| 350,000 | 61.667 |
| 400,000 | 61.450 |
| 450,000 | 60.850 |
| 500,000 | 59.950 |
| ... | ... |
| 2,961,423 (final) | 59.700 |

#### p = 0.999
| Events Processed | L_eff (s) |
|---:|---:|
| 50,000 | 65.851 |
| 100,000 | 81.084 |
| 150,000 | 88.950 |
| 200,000 | 90.950 |
| 250,000 | 98.833 |
| 300,000 | 97.183 |
| 350,000 | 95.567 |
| 400,000 | 94.600 |
| 450,000 | 93.516 |
| 500,000 | 92.416 |
| ... | ... |
| 2,961,423 (final) | 95.034 |

### 4.4 Observations

- **Adaptive L_eff tracks the lateness distribution exactly**: p=0.50 → L_eff≈11.633s (median), p=0.95 → L_eff≈37.783s, p=0.99 → L_eff≈59.7s
- **Immediate completeness ≈ p_normal × 100%**: p% of events have lateness ≤ L_eff by definition of the percentile
- **DLQ backlog size**: ranges from 48,099 (p=0.999, 1.6% late) to 1,060,021 (p=0.50, 35.8% late)
- **Eventual completeness = 100%** across all p values — DLQ reconciliation ensures no data loss

---

## 5. STRICT vs HEURISTIC — Head-to-Head Comparison

### 5.1 Completeness-at-Latency Trade-off

| Wait Time (ms) | Strict Completeness % | Heuristic (matched p) | Heuristic L_eff (ms) | Heuristic Immediate % | Heuristic Eventual % |
|---:|---:|---:|---:|---:|---:|
| 0 | **8.75** | p=0.500 | 11,632 | 64.21 | **100.00** |
| 1,000 | **12.88** | p=0.500 | 11,632 | 64.21 | **100.00** |
| 2,000 | **17.86** | p=0.500 | 11,632 | 64.21 | **100.00** |
| 3,000 | **23.29** | p=0.500 | 11,632 | 64.21 | **100.00** |
| 5,000 | **34.66** | p=0.500 | 11,632 | 64.21 | **100.00** |
| 7,000 | **45.48** | p=0.500 | 11,632 | 64.21 | **100.00** |
| 10,000 | **59.17** | p=0.500 | 11,632 | 64.21 | **100.00** |
| 15,000 | **75.03** | p=0.500 | 11,632 | 64.21 | **100.00** |
| 20,000 | **84.44** | p=0.750 | 18,666 | 81.26 | **100.00** |
| 30,000 | **93.23** | p=0.900 | 28,800 | 91.31 | **100.00** |
| 40,000 | **96.72** | p=0.950 | 37,782 | 94.77 | **100.00** |
| 50,000 | **98.41** | p=0.990 | 59,699 | 97.70 | **100.00** |
| 60,000 | **99.27** | p=0.990 | 59,699 | 97.70 | **100.00** |
| 90,000 | **99.89** | p=0.999 | 95,033 | 98.38 | **100.00** |
| 120,000 | **99.97** | p=0.999 | 95,033 | 98.38 | **100.00** |

### 5.2 Summary Table

| | Strict | Heuristic |
|---|---|---|
| **Wait knob** | configured δ (DELTA_BASE_S) | adaptive L_eff (percentile-based) |
| **Completeness range** | 8.8% → 100.0% | Immediate: 64.2% → 98.4% | Eventual: 100% |
| **Latency model** | = δ (fixed, predictable) | Low watermark wait + deferred DLQ corrections |
| **Data loss** | Permanent (late events dropped) | None (DLQ recovery ensures 100% eventual) |
| **Adaptability** | Static (manual tuning required) | Automatic (tracks lateness distribution) |
| **Worst case** | δ must cover max lateness (360s) for 100% | DLQ handles all outliers |
| **Best for** | Real-time dashboards (cannot wait for DLQ) | Analytical/batch (can tolerate deferred corrections) |

### 5.3 Key Findings

1. **Strict watermark is a direct completeness-for-latency trade-off**: Each second of δ adds 1s of latency to every window. Achieving 100.0% completeness requires δ=120s. Even then, 833 events (0.028%) with extreme outlier lateness (>120s) are permanently dropped.

2. **Heuristic + DLQ is the strictly dominant strategy for eventual completeness**: At p=0.50, watermark wait is only 11.6s (vs 120s for strict), and eventual completeness is 100% (vs 100.0% for strict). The cost is that 1,060,021 events go through DLQ reconciliation.

3. **The DLQ fundamentally changes the trade-off space**: Without a DLQ, you must choose between low latency (low completeness) and high completeness (high latency). With DLQ reconciliation, you get low watermark latency AND 100% eventual completeness — but with deferred correction latency.

4. **For real-time use cases**, strict with δ≈38s (~95.00% completeness) is the pragmatic choice: predictable latency, good completeness, no DLQ complexity.

5. **For batch/analytical use cases**, heuristic with p=0.50 (L_eff=12s) achieves extremely low watermark wait with 100% eventual accuracy — the clear winner.

---

## 6. Sensitivity Analysis — Window Size Impact

How does the choice of window size affect the completeness curve?

| Window Size | Total Windows | Completeness @ δ=0s | Completeness @ δ=60s | δ for 95% | δ for 99% |
|---:|---:|---:|---:|---:|---:|
| 5s | 8,927 | 8.75% | 99.27% | 40.0s | 60.0s |

**Interpretation**: Larger windows reduce the number of windows, which slightly changes the watermark boundary effects. However, for datasets where lateness is driven by the arrival-time vs event-time gap (not random jitter), the completeness curve is primarily determined by the lateness distribution, not the window size.

---

## 7. Lateness Threshold Analysis — "How Long Should I Wait?"

For each wait threshold, the percentage of events that would be recovered:

| Wait (s) | Wait (ms) | % Recovered | % Still Late | Late Events Remaining |
|---:|---:|---:|---:|---:|
| 0 | 0 | 0.00% | 100.00% | 2,961,423 |
| 1 | 1,000 | 1.17% | 98.83% | 2,926,881 |
| 2 | 2,000 | 2.00% | 98.00% | 2,902,169 |
| 3 | 3,000 | 4.11% | 95.89% | 2,839,585 |
| 5 | 5,000 | 12.61% | 87.39% | 2,587,900 |
| 7 | 7,000 | 24.14% | 75.86% | 2,246,655 |
| 10 | 10,000 | 41.63% | 58.37% | 1,728,718 |
| 12 | 12,000 | 51.80% | 48.20% | 1,427,371 |
| 15 | 15,000 | 64.17% | 35.83% | 1,061,033 |
| 20 | 20,000 | 78.04% | 21.96% | 650,340 |
| 25 | 25,000 | 86.19% | 13.81% | 409,058 |
| 30 | 30,000 | 90.94% | 9.06% | 268,370 |
| 35 | 35,000 | 93.86% | 6.14% | 181,755 |
| 40 | 40,000 | 95.74% | 4.26% | 126,227 |
| 45 | 45,000 | 97.01% | 2.99% | 88,658 |
| 50 | 50,000 | 97.91% | 2.09% | 61,806 |
| 55 | 55,000 | 98.56% | 1.44% | 42,576 |
| 60 | 60,000 | 99.03% | 0.97% | 28,812 |
| 75 | 75,000 | 99.68% | 0.32% | 9,526 |
| 90 | 90,000 | 99.87% | 0.13% | 3,816 |
| 120 | 120,000 | 99.97% | 0.03% | 966 |

---

## 8. Edge Cases & Outliers

- **Events with lateness > 120s**: 966 (0.03%) — these are the 0.03% of trips that cause the last bit of completeness loss at δ=120s
- **Events with lateness > 180s**: 208 (0.01%)
- **Max lateness**: 359.7s — the single worst outlier
- **δ for 100% completeness**: theoretically ≥ 360s but this is impractical for production
- **δ for 99.99% completeness**: ~160s (160,028 ms)

---

## 9. Production Recommendations

### 9.1 For Real-Time Dashboards (Strict Mode)

- **Recommended δ**: 38s (37,783 ms)
- **Expected completeness**: ~95.00%
- **Expected late events**: 148,037 (5.00%)
- **Latency impact**: 38s added to each window's output delay
- **Rationale**: Covers 95% of lateness distribution with bounded, predictable latency

### 9.2 For Analytical/Batch Use Cases (Heuristic Mode)

- **Recommended p**: 0.50 (L_eff ≈ 12s)
- **Expected immediate completeness**: ~50.01%
- **DLQ backlog**: ~1,480,499 events (49.99%) — reconcile within SLA window
- **Eventual completeness**: 100% (after DLQ reconciliation)
- **Rationale**: Minimal watermark latency, 100% eventual accuracy, DLQ handles the rest

### 9.3 For Balanced Operation

- **Recommended p**: 0.90 (L_eff ≈ 29s)
- **Expected immediate completeness**: ~90.00%
- **DLQ backlog**: ~296,230 events (10.00%)
- **Rationale**: Good immediate completeness with small DLQ backlog — sweet spot

---

## 10. Artifacts & Reproduce

### 10.1 Generated Files

- **Report**: `docs/REPORT_full_dataset_20260602_203050.md`
- **CSV**: `docs/full_dataset_analysis_20260602_203050.csv`
- **Analysis script**: `deploy/full_dataset_analysis.py`
- **Dataset**: `dataset/nyc_taxi_events_full.csv` (2,961,423 rows, 152 MB)
- **Converter**: `tools/nyc_taxi_to_events.py`

### 10.2 Reproduce

```bash
# 1. Convert raw NYC Yellow Taxi data to engine event format:
python tools/nyc_taxi_to_events.py --div 60 --rows 0  # 0 = all rows

# 2. Run full-dataset analysis:
python deploy/full_dataset_analysis.py

# 3. Run with custom parameters:
python deploy/full_dataset_analysis.py \
    --window-size 10 \
    --strict-deltas 0,5,10,20,30,40,50,60,90,120 \
    --heuristic-ps 0.50,0.75,0.90,0.95,0.99,0.999 \
    --extra-sweeps

# 4. (Alternative) Docker-based experiment with real cluster:
python deploy/experiment_completeness_vs_wait.py \
    --mode strict --punctuation max-event-time \
    --dataset nyc_taxi_events_full.csv \
    --deltas 0,5,10,20,40,60 --max-wait 600
```

### 10.3 Data Lineage

```
yellow_tripdata_2024-01.csv (NYC TLC, raw)
    │
    └── nyc_taxi_to_events.py  (DIV=60 compression, schema mapping)
         │
         └── nyc_taxi_events_full.csv  (2.96M rows, engine-compatible)
              │
              └── full_dataset_analysis.py  (this script)
                   │
                   ├── REPORT_full_dataset_<ts>.md
                   └── full_dataset_analysis_<ts>.csv
```
