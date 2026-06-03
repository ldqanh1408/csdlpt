import subprocess
import glob
import shutil
import os
import sys

# Ensure UTF-8 output and prevent buffering
os.environ["PYTHONUTF8"] = "1"
os.environ["PYTHONUNBUFFERED"] = "1"

# --- Warmup fix (topic #112) ---------------------------------------------
# The default warmup (10s AND 50,000 samples/partition = 600K events across 12
# partitions) freezes the heuristic watermark at L_max for nearly the entire
# measured run. While frozen no window closes => every event is on_time =>
# completeness is a flat 100% at every percentile and late=0.0% — the sweep
# cannot produce a trade-off curve. Lower the warmup so the heuristic watermark
# governs almost the whole 2.96M-row run. compose interpolates these via
# ${HEURISTIC_WARMUP_*}; run_experiment.py copies os.environ into `docker
# compose up`, so setting them here reaches the worker containers.
os.environ.setdefault("HEURISTIC_WARMUP_SAMPLES", "2000")
os.environ.setdefault("HEURISTIC_WARMUP_S", "5.0")

# --- Local-watermark close (topic #112, ROOT CAUSE of the flat 100% curve) ---
# Heuristic normally closes windows on W_global_h = min(per-partition W_h), which
# lags the arrival frontier far more than L_eff (dragged down by the slowest /
# least-warmed partition), so every event lands on_time and completeness is a
# flat 100% at every percentile. This flag makes _close_windows use the LOCAL
# per-partition watermark (max_event_time - L_eff), matching the offline model so
# the percentile sweep produces the real immediate-completeness trade-off curve.
os.environ.setdefault("HEURISTIC_LOCAL_WATERMARK_CLOSE", "true")
os.environ.setdefault("INGESTOR_REPLAY", "arrival")
os.environ.setdefault("REPLAY_SPEED", "50")

print("[run_heuristic] Starting heuristic sweep...")
print(f"[run_heuristic] HEURISTIC_WARMUP_SAMPLES={os.environ['HEURISTIC_WARMUP_SAMPLES']} "
      f"HEURISTIC_WARMUP_S={os.environ['HEURISTIC_WARMUP_S']}")

cmd = [
    sys.executable, "reports/run_experiment.py",
    "--mode", "heuristic",
    "--punctuation", "max-event-time",
    "--dataset", "nyc_taxi_events_full.csv",
    "--ps", "0.10,0.20,0.30,0.40,0.50,0.75,0.90,0.95,0.99,0.999,0.9999",
    "--max-wait", "2000",
    "--settle", "30"
]

print(f"[run_heuristic] Command: {' '.join(cmd)}")
result = subprocess.run(cmd)

if result.returncode != 0:
    print(f"[run_heuristic] Error: experiment runner returned {result.returncode}")
    sys.exit(result.returncode)

# Find latest generated files in docs/
docs_dir = "docs"
csv_files = glob.glob(os.path.join(docs_dir, "completeness_vs_wait_heuristic_*.csv"))
md_files = glob.glob(os.path.join(docs_dir, "completeness_vs_wait_heuristic_*.md"))

if not csv_files or not md_files:
    print("[run_heuristic] Error: no output files found in docs/")
    sys.exit(1)

latest_csv = sorted(csv_files, key=os.path.getmtime)[-1]
latest_md = sorted(md_files, key=os.path.getmtime)[-1]

dest_dir = "reports/results"
os.makedirs(dest_dir, exist_ok=True)

shutil.copy2(latest_csv, os.path.join(dest_dir, os.path.basename(latest_csv)))
shutil.copy2(latest_md, os.path.join(dest_dir, os.path.basename(latest_md)))

print(f"[run_heuristic] Success! Copied heuristic reports to {dest_dir}:")
print(f"  - {os.path.basename(latest_csv)}")
print(f"  - {os.path.basename(latest_md)}")
