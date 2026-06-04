"""
Script bọc sẵn để chạy sweep heuristic trên full dataset.

Nó đặt biến môi trường cho warmup, local-watermark close và paced replay, gọi `reports/run_experiment.py`, rồi copy báo cáo heuristic mới nhất sang `reports/results`.
"""

import subprocess
import glob
import shutil
import os
import sys

# Bảo đảm output UTF-8 và tắt buffering để log hiện ngay khi chạy sweep.
os.environ["PYTHONUTF8"] = "1"
os.environ["PYTHONUNBUFFERED"] = "1"

# --- Sửa warmup (topic #112) ----------------------------------------------
# Warmup mặc định (10s và 50.000 mẫu/partition = 600K event trên 12 partition)
# giữ heuristic watermark ở L_max gần như suốt lần đo. Khi watermark bị đóng băng
# thì không cửa sổ nào đóng, mọi event đều on_time, completeness phẳng 100% và
# sweep không tạo được đường cong trade-off. Hạ warmup để watermark heuristic
# chi phối gần như toàn bộ 2.96M dòng; docker compose nội suy các biến này qua
# ${HEURISTIC_WARMUP_*}, còn run_experiment.py chuyển env vào worker container.
os.environ.setdefault("HEURISTIC_WARMUP_SAMPLES", "2000")
os.environ.setdefault("HEURISTIC_WARMUP_S", "5.0")

# --- Đóng cửa sổ bằng local watermark (nguyên nhân chính của đường 100%) ----
# Heuristic mặc định đóng cửa sổ bằng W_global_h = min(W_h từng partition).
# Giá trị này thường tụt xa hơn L_eff vì bị partition chậm nhất kéo xuống, nên
# event vẫn on_time và completeness tiếp tục phẳng 100%. Flag dưới đây buộc
# _close_windows dùng watermark cục bộ (max_event_time - L_eff), khớp mô hình
# offline để sweep percentile tạo đúng đường trade-off immediate-completeness.
os.environ.setdefault("HEURISTIC_LOCAL_WATERMARK_CLOSE", "true")
os.environ.setdefault("INGESTOR_REPLAY", "arrival")
os.environ.setdefault("REPLAY_SPEED", "150")

# Allow passing dataset as first argument, default to nyc_taxi_events_full.csv
dataset = sys.argv[1] if len(sys.argv) > 1 else "nyc_taxi_events_full.csv"

print(f"[run_heuristic] Starting heuristic sweep on dataset: {dataset}...")
print(f"[run_heuristic] HEURISTIC_WARMUP_SAMPLES={os.environ['HEURISTIC_WARMUP_SAMPLES']} "
      f"HEURISTIC_WARMUP_S={os.environ['HEURISTIC_WARMUP_S']} "
      f"REPLAY_SPEED={os.environ['REPLAY_SPEED']}")

cmd = [
    sys.executable, "reports/run_experiment.py",
    "--mode", "heuristic",
    "--punctuation", "max-event-time",
    "--dataset", dataset,
    "--ps", "0.10,0.20,0.30,0.40,0.50,0.75,0.90,0.95,0.99,0.999,0.9999",
    "--max-wait", "2000",
    "--settle", "30"
]

print(f"[run_heuristic] Command: {' '.join(cmd)}")
result = subprocess.run(cmd)

if result.returncode != 0:
    print(f"[run_heuristic] Error: experiment runner returned {result.returncode}")
    sys.exit(result.returncode)

# Tìm bộ file mới nhất sinh trong docs/ rồi copy sang reports/results.
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
