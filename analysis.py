"""
analysis.py — CLI mỏng cho deliverable "Data Completeness % vs Wait Time".

Toàn bộ logic nằm trong package `wm` (engine, sweep, demos, data loaders).
Script này chỉ làm: parse args -> load dataset -> sweep -> write report
-> chạy recovery + backpressure demo.

Chạy:
  python analysis.py                                             # synthetic
  python analysis.py --source real --csv dataset/data.csv -n 200000
"""
import time
import argparse

from wm.sweep import load_dataset, sweep, write_report
from wm.demos import crash_recovery_demo, backpressure_demo


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["synthetic", "real"], default="synthetic",
                    help="Nguồn dữ liệu: synthetic (mặc định) hoặc real (NASA CSV).")
    ap.add_argument("--csv", default="dataset/data.csv",
                    help="Đường dẫn CSV NASA-HTTP khi --source=real.")
    ap.add_argument("-n", "--n-events", type=int, default=5000,
                    help="Số event đọc/sinh (real khuyến nghị 100000+).")
    args = ap.parse_args()

    t = time.perf_counter()
    df = load_dataset(args.source, args.n_events, args.csv)
    rows = sweep(df)
    write_report(rows)
    crash_recovery_demo(df)
    backpressure_demo(df)
    print(f"\nTotal runtime: {time.perf_counter() - t:.2f}s")


if __name__ == "__main__":
    main()
