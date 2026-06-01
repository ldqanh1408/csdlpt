#!/usr/bin/env python3
"""Auto-run strict + heuristic experiments and generate comparison report.

One-command report generation. Place your NYC taxi dataset in dataset/ then run:

    python reports/run_all.py                        # offline analysis only
    python reports/run_all.py --docker               # full Docker experiment (needs Docker)
    python reports/run_all.py --docker --rows 150000 # quick test with 150K rows

Flow:
  1. Checks dataset exists (nyc_taxi_events_full.csv or generates from yellow_tripdata)
  2. Runs offline analysis (analyze_dataset.py) — always
  3. (--docker) Runs strict mode Docker experiment at multiple delta values
  4. (--docker) Runs heuristic mode Docker experiment at multiple percentile values
  5. Generates comparison report -> reports/artifacts/comparison_report.md

Output:
    reports/artifacts/
        analysis_report.md          — Full offline numerical analysis
        completeness_vs_wait_strict.csv / .md     — Strict Docker experiment
        completeness_vs_wait_heuristic.csv / .md  — Heuristic Docker experiment
        comparison_report.md        — Combined summary with completeness curves
"""
from __future__ import annotations
import argparse
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

REPORTS_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = REPORTS_DIR.parent
ARTIFACTS_DIR = REPORTS_DIR / "artifacts"
DATASET_DIR = PROJECT_ROOT / "dataset"
DATASET_READY = DATASET_DIR / "nyc_taxi_events_full.csv"
DATASET_RAW = DATASET_DIR / "yellow_tripdata_2024-01.parquet"
DATASET_RAW_CSV = DATASET_DIR / "yellow_tripdata_2024-01.csv"

ANALYZE_SCRIPT = REPORTS_DIR / "analyze_dataset.py"
EXPERIMENT_SCRIPT = REPORTS_DIR / "run_experiment.py"
CONVERTER_SCRIPT = PROJECT_ROOT / "tools" / "nyc_taxi_to_events.py"


def _run(cmd: list[str], cwd=None, timeout=3600) -> int:
    """Run a command, streaming output to stdout."""
    label = " ".join(str(c) for c in cmd)
    print(f"\n{'='*60}\n[run_all] {label}\n{'='*60}", flush=True)
    result = subprocess.run(cmd, cwd=cwd or str(PROJECT_ROOT), timeout=timeout)
    if result.returncode != 0:
        print(f"[run_all] WARNING: command returned {result.returncode}", flush=True)
    return result.returncode


def ensure_dataset() -> bool:
    """Ensure the prepared dataset exists; offer to generate it if needed."""
    if DATASET_READY.exists():
        size_mb = DATASET_READY.stat().st_size / 1024 / 1024
        print(f"[run_all] Dataset ready: {DATASET_READY} ({size_mb:.0f} MB)")
        return True

    # Check for raw source
    raw_src = None
    if DATASET_RAW.exists():
        raw_src = DATASET_RAW
    elif DATASET_RAW_CSV.exists():
        raw_src = DATASET_RAW_CSV

    if raw_src:
        print(f"[run_all] Raw dataset found: {raw_src}")
        print("[run_all] Converting to engine format...")
        cmd = [sys.executable, str(CONVERTER_SCRIPT), "--src", str(raw_src),
               "--out", str(DATASET_READY)]
        rc = _run(cmd)
        if rc == 0 and DATASET_READY.exists():
            return True

    print(f"[run_all] ERROR: No dataset found.")
    print(f"[run_all] Download the dataset first:")
    print(f"  wget -P dataset/ https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet")
    print(f"[run_all] Then re-run: python reports/run_all.py")
    return False


def run_offline_analysis() -> Path | None:
    """Run offline numerical analysis. Returns path to report or None."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

    print("\n[run_all] Step 1/3: Offline dataset analysis (no Docker)...")
    cmd = [sys.executable, str(ANALYZE_SCRIPT)]
    rc = _run(cmd)

    # analyze_dataset.py outputs to docs/ — find the latest report
    docs_dir = PROJECT_ROOT / "docs"
    reports_md = sorted(docs_dir.glob("REPORT_full_dataset_*.md"), key=os.path.getmtime, reverse=True)
    reports_csv = sorted(docs_dir.glob("full_dataset_analysis_*.csv"), key=os.path.getmtime, reverse=True)

    if reports_md:
        # Copy to artifacts
        import shutil
        dest_md = ARTIFACTS_DIR / "analysis_report.md"
        shutil.copy2(reports_md[0], dest_md)
        print(f"[run_all] Analysis report -> {dest_md}")
        return reports_md[0]
    return None


def run_docker_experiment(mode: str, deltas_or_ps: str, max_wait: int = 600) -> Path | None:
    """Run one Docker experiment sweep. Returns path to report or None."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    cmd = [
        sys.executable, str(EXPERIMENT_SCRIPT),
        "--mode", mode,
        "--dataset", DATASET_READY.name,
        "--max-wait", str(max_wait),
        "--settle", "30",
    ]
    if mode == "strict":
        cmd += ["--deltas", deltas_or_ps]
    else:
        cmd += ["--ps", deltas_or_ps]

    rc = _run(cmd)

    # experiment outputs CSV+MD to docs/
    docs_dir = PROJECT_ROOT / "docs"
    pattern = f"completeness_vs_wait_{mode}_*"
    reports = sorted(docs_dir.glob(f"{pattern}*"), key=os.path.getmtime, reverse=True)
    if reports:
        return reports[0]
    return None


def generate_comparison(strict_report: Path | None, heuristic_report: Path | None) -> Path:
    """Generate a combined comparison markdown report."""
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    out = ARTIFACTS_DIR / "comparison_report.md"

    lines = [
        "# Watermark Comparison Report",
        f"Generated: {ts}",
        "",
        "## Strict vs Heuristic Watermark",
        "",
        "| Strategy | Mechanism | Guarantee | Latency |",
        "|---|---|---|---|",
        "| **Strict** | `W_global = min(LW_i)` via Raft Coordinator | 0% data loss | High (straggler-bound) |",
        "| **Heuristic** | `W_h = max(T_event) - DDSketch.quantile(p)` + DLQ | <=1% immediate, 100% eventual | Low (adaptive) |",
        "",
        "## Experiment Results",
        "",
    ]

    if strict_report and strict_report.exists():
        lines.append(f"### Strict Watermark")
        lines.append(f"Report: `{strict_report}`")
        lines.append("")
        # Try to extract key numbers
        try:
            text = strict_report.read_text()
            lines.append(text[:3000])
        except Exception:
            lines.append("(see full report)")
    else:
        lines.append("### Strict: No Docker experiment results (use --docker flag)")

    lines.append("")

    if heuristic_report and heuristic_report.exists():
        lines.append(f"### Heuristic Watermark")
        lines.append(f"Report: `{heuristic_report}`")
        lines.append("")
        try:
            text = heuristic_report.read_text()
            lines.append(text[:3000])
        except Exception:
            lines.append("(see full report)")
    else:
        lines.append("### Heuristic: No Docker experiment results (use --docker flag)")

    lines.append("")
    lines.append("## Reference")
    lines.append(f"- Dataset: `{DATASET_READY}` ({DATASET_READY.stat().st_size/1024/1024:.0f} MB)")
    lines.append(f"- Time compression: DIV=60 (31 days -> 12.4 hours)")
    lines.append(f"- Windows: 5s tumbling, 12 partitions, 4 workers")

    out.write_text("\n".join(lines))
    print(f"[run_all] Comparison report -> {out}")
    return out


def main():
    ap = argparse.ArgumentParser(description="Auto-run all experiments and generate reports")
    ap.add_argument("--docker", action="store_true",
                    help="Also run Docker-based experiments (requires Docker)")
    ap.add_argument("--strict-deltas", default="0,5,10,20,40,60,90,120",
                    help="Comma-separated delta values for strict mode (seconds)")
    ap.add_argument("--heuristic-ps", default="0.50,0.75,0.90,0.95,0.99,0.999",
                    help="Comma-separated percentile values for heuristic mode")
    ap.add_argument("--max-wait", type=int, default=600,
                    help="Max wait seconds for Docker experiments")
    ap.add_argument("--rows", type=int, default=0,
                    help="Max rows for converter (0=all, 150000 for quick test)")
    args = ap.parse_args()

    t0 = time.time()

    # 1. Ensure dataset
    if not ensure_dataset():
        sys.exit(1)

    # 2. Offline analysis
    analysis_report = run_offline_analysis()

    strict_report = None
    heuristic_report = None

    # 3. Docker experiments
    if args.docker:
        print("\n[run_all] Step 2/3: Strict mode Docker experiment...")
        strict_report = run_docker_experiment("strict", args.strict_deltas, args.max_wait)

        print("\n[run_all] Step 3/3: Heuristic mode Docker experiment...")
        heuristic_report = run_docker_experiment("heuristic", args.heuristic_ps, args.max_wait)

    # 4. Generate comparison
    comparison = generate_comparison(strict_report, heuristic_report)

    elapsed = time.time() - t0
    print(f"\n[run_all] DONE ({elapsed:.0f}s)")
    print(f"[run_all] Reports in: {ARTIFACTS_DIR}/")
    for f in sorted(ARTIFACTS_DIR.iterdir()):
        print(f"  {f.name} ({f.stat().st_size/1024:.0f} bytes)")


if __name__ == "__main__":
    main()
