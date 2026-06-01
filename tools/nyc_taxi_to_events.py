#!/usr/bin/env python3
"""NYC Taxi (yellow_tripdata) -> engine-schema CSV converter.

Supports both CSV and Parquet input (auto-detected by file extension).

Chuẩn hóa dataset/yellow_tripdata_2024-01.parquet (hoặc .csv, 19 cột thô) sang schema engine:
    ,host,time,method,url,response,bytes,arrival

Ánh xạ cột:
    host     <- PULocationID          (partition key: hash(host)%12)
    time     <- pickup  / DIV         (event-time, epoch giây sau nén)
    method   = "GET"                 (cosmetic)
    url      <- "/zone/{DO}"          (cosmetic)
    response = 200                   (không có ý nghĩa trong topic taxi)
    bytes    <- int(total_amount*100) (cents, dương)
    arrival  <- dropoff / DIV         (arrival-time thật, out-of-order vs event-time)

Nén thời gian — 1 hệ số DIV duy nhất:
    pickup_min = min(pickup)
    event_time = (pickup  - pickup_min) / DIV
    arrival    = (dropoff - pickup_min) / DIV
    lateness   = arrival - event_time = (dropoff-pickup)/DIV = duration/DIV
    DIV=60 -> lateness p50~12s / p95~38s / p99~60s  -> delta sweep 0..60s

Thứ tự dòng file: sắp theo arrival tăng dần (= thứ tự đến thật), out-of-order so với
event_time vì chuyến dài kết thúc muộn hơn chuyến ngắn bắt đầu sau.

Usage:
    python tools/nyc_taxi_to_events.py --rows 150000 --div 60
    python tools/nyc_taxi_to_events.py                       # toàn bộ dataset (auto-detect parquet/csv)
    python tools/nyc_taxi_to_events.py --late-target-s 40    # tự tính DIV từ p95
"""
from __future__ import annotations
import argparse
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).parent.parent
DEFAULT_SRC = ROOT / "dataset" / "yellow_tripdata_2024-01.parquet"
DEFAULT_OUT = ROOT / "dataset" / "nyc_taxi_events_full.csv"


def _read_input(src: Path, cols: list, nrows: int | None) -> pd.DataFrame:
    """Read CSV or Parquet, auto-detected by file extension."""
    suffix = src.suffix.lower()
    if suffix in (".parquet", ".pq"):
        df = pd.read_parquet(src, columns=cols)
        if nrows:
            df = df.head(nrows)
        # Parquet columns may already be datetime64 — coerce to naive
        for c in ["tpep_pickup_datetime", "tpep_dropoff_datetime"]:
            if c in df.columns and hasattr(df[c].dtype, "tz") and df[c].dtype.tz is not None:
                df[c] = df[c].dt.tz_localize(None)
    else:
        df = pd.read_csv(src, usecols=cols, nrows=nrows,
                         parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"])
    return df


def main():
    ap = argparse.ArgumentParser(description="NYC Taxi -> engine event CSV")
    ap.add_argument("--src", default=str(DEFAULT_SRC))
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    ap.add_argument("--rows", type=int, default=0, help="Max rows (0 = all 2.96M)")
    ap.add_argument("--div", type=float, default=60.0,
                    help="Time compression divisor (default 60)")
    ap.add_argument("--late-target-s", type=float, default=0.0,
                    help="If >0, auto-compute DIV = duration_p95 / this")
    args = ap.parse_args()

    t0 = time.time()
    src_path = Path(args.src)
    print(f"[converter] reading {args.src} ({src_path.suffix}) ...", flush=True)
    cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime",
            "PULocationID", "DOLocationID", "total_amount"]
    nrows = args.rows if args.rows > 0 else None
    df = _read_input(src_path, cols, nrows)
    print(f"[converter]  read {len(df):,} rows ({time.time()-t0:.1f}s)")

    # -- Step 1: Clean -------------------------------------------------------
    df = df.dropna(subset=cols)
    dur = (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds()
    df = df[(dur > 0) & (dur <= 6 * 3600)].copy()

    # Keep within Jan 2024 (drop stray Dec-2023 / Feb-2024 records)
    def _naive(s):
        return s.dt.tz_localize(None) if s.dt.tz is not None else s
    pu = _naive(df["tpep_pickup_datetime"])
    do = _naive(df["tpep_dropoff_datetime"])
    jan_start = pd.Timestamp("2024-01-01")
    jan_end = pd.Timestamp("2024-02-01")
    mask = (pu >= jan_start) & (do < jan_end)
    df = df[mask].reset_index(drop=True)
    pu = _naive(df["tpep_pickup_datetime"])
    do = _naive(df["tpep_dropoff_datetime"])
    dur = (do - pu).dt.total_seconds()
    print(f"[converter]  after clean: {len(df):,} rows")
    if len(df) == 0:
        raise SystemExit("[converter] no rows left after cleaning")

    # -- Step 2: DIV ---------------------------------------------------------
    dur_p50 = float(np.percentile(dur, 50))
    dur_p95 = float(np.percentile(dur, 95))
    dur_p99 = float(np.percentile(dur, 99))
    div = (dur_p95 / args.late_target_s) if args.late_target_s > 0 else args.div
    div = max(div, 1e-6)
    print(f"[converter]  duration p50={dur_p50:.0f}s p95={dur_p95:.0f}s p99={dur_p99:.0f}s")
    print(f"[converter]  DIV={div:.2f} -> lateness p50={dur_p50/div:.1f}s "
          f"p95={dur_p95/div:.1f}s p99={dur_p99/div:.1f}s")

    # -- Step 3: Compress (single divisor on both timestamps) ----------------
    pmin_ts = pu.min()
    event_time_c = (pu - pmin_ts).dt.total_seconds() / div   # = (pickup - pickup_min)/DIV
    arrival_c = (do - pmin_ts).dt.total_seconds() / div      # = (dropoff - pickup_min)/DIV

    # -- Step 4: Build + sort by arrival -------------------------------------
    out_df = pd.DataFrame({
        "host": df["PULocationID"].astype(int).astype(str),
        "time": event_time_c.round(3),
        "method": "GET",
        "url": "/zone/" + df["DOLocationID"].astype(int).astype(str),
        "response": 200,
        "bytes": (df["total_amount"] * 100).clip(lower=0).astype(int),
        "arrival": arrival_c.round(3),
    })
    out_df = out_df.sort_values("arrival", kind="mergesort").reset_index(drop=True)

    # -- Step 5: Stats -------------------------------------------------------
    lateness = out_df["arrival"] - out_df["time"]
    oo = int((out_df["time"].diff() < 0).sum())
    print(f"[converter]  event-time span: 0 .. {out_df['time'].max():.1f}s")
    print(f"[converter]  lateness p50={lateness.quantile(.50):.1f}s "
          f"p95={lateness.quantile(.95):.1f}s p99={lateness.quantile(.99):.1f}s")
    print(f"[converter]  out-of-order steps: {oo:,}/{len(out_df):,} "
          f"({100*oo/max(len(out_df),1):.1f}%)")

    # -- Step 6: Write -------------------------------------------------------
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out, index=True, index_label="")
    print(f"[converter]  written {len(out_df):,} rows -> {out} "
          f"({out.stat().st_size/1024/1024:.1f} MB, {time.time()-t0:.1f}s)")
    print("[converter] run experiment with:")
    print(f"  python deploy/experiment_completeness_vs_wait.py --mode strict "
          f"--punctuation max-event-time --dataset {args.out.name} "
          f"--deltas 0,5,10,20,40,60 --max-wait 200")


if __name__ == "__main__":
    main()
