"""
distributed_sweep.py — CLI mỏng cho cluster mô phỏng (N nodes, partition theo host).

Logic ở `wm.partition`. Script này chỉ làm: parse args -> load dataset
-> chạy `run_cluster` cho từng wait time -> in bảng kết quả.

Chạy:
  python distributed_sweep.py --nodes 4 -n 100000              # real data
  python distributed_sweep.py --nodes 4 -n 5000 --source synthetic
"""
import argparse
import time

from wm.data.synthetic import generate_logs
from wm.data.nasa import load_nasa_csv
from wm.partition import run_cluster

WAIT_TIMES_MS = [0, 250, 1000, 2000, 4000, 8000]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=4)
    ap.add_argument("--source", choices=["synthetic", "real"], default="real")
    ap.add_argument("--csv", default="dataset/data.csv")
    ap.add_argument("-n", "--n-events", type=int, default=100000)
    args = ap.parse_args()

    t = time.perf_counter()
    if args.source == "real":
        df = load_nasa_csv(args.csv, limit=args.n_events)
    else:
        df = generate_logs(n_events=args.n_events)
        # synthetic không có cột 'host' -> dùng endpoint làm khoá phân vùng.
        df = df.assign(host=df["endpoint"])

    print(f"\n=== DISTRIBUTED CLUSTER: {args.nodes} nodes, "
          f"{len(df)} events ({args.source}) ===\n")

    print(f"{'Wait(ms)':>9} | {'Completeness%':>13} | {'LateDrop':>9} | "
          f"{'Windows':>8} | per-node event counts")
    print("-" * 90)
    first = True
    for wt in WAIT_TIMES_MS:
        r = run_cluster(df, wt, args.nodes)
        print(f"{wt:>9} | {r['completeness_pct']:>13} | "
              f"{r['late_dropped']:>9} | {r['windows']:>8} | "
              f"{r['per_node_counts']}")
        if first:
            counts = r["per_node_counts"]
            mx, mn = max(counts), min(counts)
            skew_pct = 100.0 * (mx - mn) / max(sum(counts), 1)
            print(f"  -> Hot-key skew (max-min)/total = {skew_pct:.2f}% "
                  f"[DESIGN.md §3.1 cảnh báo điều này khi partition theo host]")
            first = False

    print(f"\nTotal runtime: {time.perf_counter() - t:.2f}s")


if __name__ == "__main__":
    main()
