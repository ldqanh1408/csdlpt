#!/usr/bin/env python3
"""Generate a dataset with CONTROLLED out-of-order lateness for the
Completeness-vs-Wait-Time study (topic #112).

The raw web log is (mostly) time-sorted, so ingested in file order it shows almost
no lateness and strict completeness stays ~100% at every wait time — a flat curve.
To exercise the watermark trade-off we inject a known lateness distribution:

  - keep each row's event-time (`time` column) unchanged,
  - give a fraction `p_late` of rows an arrival delay L ~ Uniform(0, max_late_s),
  - re-sort rows by (event_time + L) → that becomes the ARRIVAL (file) order.

An event then arrives "late" for wait δ roughly when L > δ, so sweeping δ from 0 to
max_late_s yields completeness rising from ~(1 - p_late) toward 100%.

Usage:
    python deploy/make_out_of_order.py --rows 150000 --p-late 0.30 --max-late-s 20 \
        --out dataset/oo_sample.csv
"""
import argparse
import csv
import random
from pathlib import Path

ROOT = Path(__file__).parent.parent
SRC = ROOT / "dataset" / "data.csv"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=150000)
    ap.add_argument("--p-late", type=float, default=0.30, help="fraction of rows that arrive late")
    ap.add_argument("--max-late-s", type=float, default=20.0, help="max arrival delay (event-time seconds)")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--src", default=str(SRC))
    ap.add_argument("--out", default=str(ROOT / "dataset" / "oo_sample.csv"))
    ap.add_argument("--compress-span-s", type=float, default=0.0,
                    help="if >0, linearly remap event-time into a [0, N] second window so the "
                         "seconds-scale wait time actually matters (raw data spans ~62 days, which "
                         "otherwise dwarfs the wait and flattens the curve)")
    args = ap.parse_args()

    random.seed(args.seed)
    src = Path(args.src)
    out = Path(args.out)

    # Pass 1: read rows + raw event-times.
    raw = []
    with src.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        ti = header.index("time")
        for i, row in enumerate(reader):
            if i >= args.rows:
                break
            try:
                t = float(row[ti])
            except (ValueError, IndexError):
                continue
            raw.append((t, row))

    if not raw:
        print("no rows read")
        return

    tmin = min(t for t, _ in raw)
    tmax = max(t for t, _ in raw)
    span = (tmax - tmin) or 1.0

    rows = []
    late_count = 0
    for t, row in raw:
        # Optional time compression: 62-day span -> [0, compress_span_s].
        if args.compress_span_s > 0:
            t = (t - tmin) / span * args.compress_span_s
            row = list(row)
            row[ti] = f"{t:.3f}"
        # Inject out-of-order lateness: a fraction of rows arrive L seconds late.
        if random.random() < args.p_late:
            L = random.uniform(0.0, args.max_late_s)
            late_count += 1
        else:
            L = 0.0
        rows.append((t + L, row))

    # Arrival (file) order = sort by (event_time + injected delay).
    rows.sort(key=lambda x: x[0])

    with out.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        for _, row in rows:
            w.writerow(row)

    print(f"wrote {len(rows):,} rows -> {out}")
    if args.compress_span_s > 0:
        print(f"event-time compressed: {span:.0f}s -> {args.compress_span_s:.0f}s span")
    print(f"injected lateness: p_late={args.p_late}, max_late_s={args.max_late_s}, "
          f"late_rows={late_count:,} ({100.0*late_count/max(len(rows),1):.1f}%)")
    print(f"expected completeness curve: ~{100*(1-args.p_late):.0f}% at wait=0  ->  ~100% at wait>={args.max_late_s:.0f}s")


if __name__ == "__main__":
    main()
