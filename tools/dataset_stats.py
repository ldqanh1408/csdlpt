#!/usr/bin/env python3
"""Comprehensive statistics report for a NYC-taxi trip CSV.

Prints EVERY useful number (row counts, datetime range, duration distribution,
out-of-order analysis, per-column describe, top zones, payment/amount dist) so
the full report can be kept verbatim. Default target: the full Jan-2024 file.

Usage:
    python tools/dataset_stats.py
    python tools/dataset_stats.py --src dataset/yellow_tripdata_2024-01.csv
"""
from __future__ import annotations
import argparse
import time
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).parent.parent
DEFAULT_SRC = ROOT / "dataset" / "yellow_tripdata_2024-01.csv"

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)
pd.set_option("display.float_format", lambda x: f"{x:,.4f}")


def hr(t):
    print("\n" + "=" * 78 + f"\n{t}\n" + "=" * 78, flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default=str(DEFAULT_SRC))
    args = ap.parse_args()
    t0 = time.time()

    hr(f"DATASET: {args.src}")
    df = pd.read_csv(args.src,
                     parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"])
    print(f"rows (raw)        : {len(df):,}")
    print(f"columns ({len(df.columns)}): {list(df.columns)}")
    print(f"read time         : {time.time()-t0:.1f}s")
    print(f"memory            : {df.memory_usage(deep=True).sum()/1024/1024:.1f} MB")

    pu = df["tpep_pickup_datetime"]
    do = df["tpep_dropoff_datetime"]

    hr("DATETIME RANGE")
    print(f"pickup  min : {pu.min()}")
    print(f"pickup  max : {pu.max()}")
    print(f"dropoff min : {do.min()}")
    print(f"dropoff max : {do.max()}")
    span_s = (pu.max() - pu.min()).total_seconds()
    print(f"pickup span : {span_s:,.0f}s  = {span_s/86400:.1f} days")
    in_jan = ((pu >= "2024-01-01") & (do < "2024-02-01")).sum()
    print(f"within Jan-2024 (pickup>=01/01 & dropoff<02/01): {in_jan:,} "
          f"({100*in_jan/len(df):.2f}%)")

    hr("TRIP DURATION (dropoff - pickup), seconds")
    dur = (do - pu).dt.total_seconds()
    print(f"count        : {dur.notna().sum():,}")
    print(f"<= 0 (bad)   : {(dur <= 0).sum():,}")
    print(f"> 6h (bad)   : {(dur > 6*3600).sum():,}")
    valid = dur[(dur > 0) & (dur <= 6*3600)]
    print(f"valid (0,6h] : {len(valid):,}")
    for p in [1, 5, 25, 50, 75, 90, 95, 99, 99.9]:
        print(f"  p{p:<4} : {np.percentile(valid, p):>10,.1f}s  ({np.percentile(valid, p)/60:.1f} min)")
    print(f"  mean : {valid.mean():>10,.1f}s   max : {valid.max():,.1f}s   min : {valid.min():,.1f}s")

    hr("LATENESS AFTER COMPRESSION (duration / DIV)")
    for div in [30, 60, 120]:
        l = valid / div
        print(f"DIV={div:<4}: p50={l.quantile(.50):6.1f}s p95={l.quantile(.95):6.1f}s "
              f"p99={l.quantile(.99):6.1f}s max={l.max():7.1f}s | "
              f"event-span={(span_s/div):,.0f}s ({span_s/div/3600:.1f}h)")

    hr("OUT-OF-ORDER ANALYSIS (event=pickup, arrival=dropoff order)")
    clean = df[(dur > 0) & (dur <= 6*3600)].copy()
    clean = clean[(clean["tpep_pickup_datetime"] >= "2024-01-01") &
                  (clean["tpep_dropoff_datetime"] < "2024-02-01")]
    # arrival order = sort by dropoff; out-of-order = pickup decreases
    arr = clean.sort_values("tpep_dropoff_datetime")
    pk = arr["tpep_pickup_datetime"].values
    oo = int((np.diff(pk.astype("int64")) < 0).sum())
    print(f"clean rows               : {len(clean):,}")
    print(f"out-of-order steps       : {oo:,} ({100*oo/max(len(clean)-1,1):.1f}%)")

    hr("PULocationID (partition key = host)")
    vc = clean["PULocationID"].value_counts()
    print(f"distinct zones : {clean['PULocationID'].nunique()}")
    print(f"top 10 zones by trips:")
    print(vc.head(10).to_string())
    # partition skew: hash%12 distribution (approx via PU%12 as proxy is not hash; show zone spread)
    print(f"max/avg zone load ratio : {vc.max()/vc.mean():.1f}x")

    hr("total_amount ($)")
    ta = clean["total_amount"]
    print(ta.describe().to_string())
    print(f"<= 0 (anomalous): {(ta <= 0).sum():,} ({100*(ta<=0).sum()/len(clean):.2f}%)")

    if "payment_type" in clean.columns:
        hr("payment_type distribution")
        print(clean["payment_type"].value_counts().to_string())

    hr("NUMERIC COLUMNS describe()")
    print(df.describe(include=[np.number]).to_string())

    hr(f"DONE ({time.time()-t0:.1f}s total)")


if __name__ == "__main__":
    main()
