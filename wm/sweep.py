"""wm.sweep — chạy sweep Wait Time vs Completeness và xuất deliverable.

Sinh `tradeoff.csv` + `tradeoff.png` ở thư mục đang chạy. Engine config
mặc định: tumbling 10s, danh sách wait time chuẩn của báo cáo.
"""
import csv
import os
import tempfile

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from wm.engine import WatermarkEngine
from wm.data.synthetic import generate_logs
from wm.data.nasa import load_nasa_csv

WINDOW_S = 10.0
WAIT_TIMES_MS = [0, 100, 250, 500, 1000, 2000, 4000, 6000, 8000]


def load_dataset(source: str, n_events: int, csv_path: str):
    """Chọn nguồn dữ liệu: 'real' = NASA-HTTP từ data.csv; 'synthetic' = sinh giả."""
    if source == "real":
        df = load_nasa_csv(csv_path, limit=n_events)
        print(f"[dataset] REAL NASA-HTTP: {len(df)} rows, "
              f"{df['event_id'].nunique()} unique events")
        return df
    df = generate_logs(n_events=n_events)
    print(f"[dataset] SYNTHETIC: {len(df)} rows, "
          f"{df['event_id'].nunique()} unique events")
    return df


def run_once(df, allowed_lateness_ms, max_queue=10_000_000):
    """Sweep chính: KHÔNG bật backpressure (queue_len=0) để cô lập hiệu ứng
    Wait Time lên completeness. Backpressure được test riêng (wm.demos)."""
    eng = WatermarkEngine(
        window_size_s=WINDOW_S,
        allowed_lateness_s=allowed_lateness_ms / 1000.0,
        checkpoint_interval=2000,
        checkpoint_path=os.path.join(tempfile.gettempdir(),
                                     f"ckpt_{allowed_lateness_ms}.json"),
        max_queue=max_queue,
    )
    for row in df.itertuples(index=False):
        ev = {"event_id": row.event_id, "event_time": row.event_time,
              "status": row.status}
        eng.process(ev, queue_len=0)
    eng.flush()
    return eng.summary()


def sweep(df):
    print(f"{'Wait(ms)':>9} | {'Completeness%':>13} | "
          f"{'LateDropped':>11} | {'ResultLat(ms)':>13} | {'p99(us)':>8}")
    print("-" * 66)
    rows = []
    for wt in WAIT_TIMES_MS:
        s = run_once(df, wt)
        rows.append(s)
        print(f"{wt:>9} | {s['data_completeness_pct']:>13} | "
              f"{s['late_dropped']:>11} | {s['avg_result_latency_ms']:>13} | "
              f"{s['proc_latency_p99_us']:>8}")
    return rows


def write_report(rows, csv_path="tradeoff.csv", png_path="tradeoff.png"):
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    xs = [r["allowed_lateness_ms"] for r in rows]
    comp = [r["data_completeness_pct"] for r in rows]
    lat = [r["avg_result_latency_ms"] for r in rows]

    fig, ax1 = plt.subplots(figsize=(9, 5.2))
    ax1.plot(xs, comp, "o-", color="#1f77b4", lw=2, label="Data Completeness %")
    ax1.set_xlabel("Wait Time / allowed lateness (ms)")
    ax1.set_ylabel("Data Completeness (%)", color="#1f77b4")
    ax1.tick_params(axis="y", labelcolor="#1f77b4")
    ax1.set_ylim(min(comp) - 2, 101)
    ax1.grid(alpha=0.3)

    ax2 = ax1.twinx()
    ax2.plot(xs, lat, "s--", color="#d62728", lw=2,
             label="Avg result latency (ms)")
    ax2.set_ylabel("Result Latency (ms)", color="#d62728")
    ax2.tick_params(axis="y", labelcolor="#d62728")

    ax1.axvspan(0, 400, color="orange", alpha=0.10)
    ax1.axvspan(3500, max(xs), color="green", alpha=0.10)
    ax1.annotate("Heuristic\n(low latency,\nsome loss)", xy=(150, min(comp)),
                 fontsize=9, ha="center", color="#a05a00")
    ax1.annotate("Strict\n(no loss,\nhigh latency)",
                 xy=(max(xs) - 1500, 99), fontsize=9, ha="center",
                 color="#1a5a1a")

    fig.suptitle("Watermark Trade-off: Data Completeness vs Wait Time",
                 fontsize=13, fontweight="bold")
    lines = ax1.get_lines() + ax2.get_lines()
    ax1.legend(lines, [l.get_label() for l in lines], loc="center right")
    fig.tight_layout()
    fig.savefig(png_path, dpi=130)
    print(f"\n[OK] Wrote {csv_path} và {png_path}")
