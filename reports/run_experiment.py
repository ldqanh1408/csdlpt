#!/usr/bin/env python3
"""Experiment runner — Data Completeness % vs Wait Time (ms)  [topic #112 deliverable].

For each Wait Time (DELTA_BASE_S, seconds) this script:
  1. Recreates the cluster (deploy/docker-compose.yml ONLY) with that wait time,
  2. Runs the WHOLE dataset until the ingestor reaches EOF and workers drain,
  3. Measures the FINAL completeness AND the steady-state AVERAGE completeness
     (mean of several samples after EOF) across all workers,
  4. Writes a CSV + Markdown report and prints the table.

This is NOT a manual mid-run "capture" — completeness is read after the dataset
has been fully processed at each wait time, then averaged.

Punctuation mode and the trade-off curve:
  The strict engine sets  local_watermark = T_commit - delta_base  and closes a
  window when  window_end <= watermark  (strict/engine.py). So a LARGER wait (δ)
  lowers the watermark, keeps windows open longer, and recovers more late data
  (higher completeness); a SMALLER δ closes windows sooner (more late drops).
  * data-driven (DEFAULT, recommended for this historical web-log dataset):
    T_commit tracks event-time, so δ is compared against the data's out-of-order
    delay (seconds). Sweeping δ 0->10s yields the classic rising curve that
    plateaus at 100% once δ exceeds the lateness spread.
  * wall-clock: T_commit = now - δ (real-time streaming). For a dataset whose
    event-times span many days but is replayed in seconds, δ of a few seconds is
    negligible, so completeness is roughly flat — not useful for the curve.

Usage (from repo root):
    python reports/run_experiment.py \
        --mode strict --punctuation wall-clock \
        --dataset nyc_taxi_events_full.csv \
        --deltas 0,2,5,10,20 --repeats 1 \
        --max-wait 300 --settle 25

    # Full dataset (slow, ~10 min/point):
    python reports/run_experiment.py --dataset nyc_taxi_events_full.csv --deltas 0,5,10,20,40
"""
from __future__ import annotations
import os
os.environ["PYTHONUTF8"] = "1"
import argparse
import csv
import importlib.util
import json
import statistics
import subprocess
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path

REPORTS_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = REPORTS_DIR.parent
DEPLOY_DIR = PROJECT_ROOT / "deploy"
COMPOSE_FILE = str(DEPLOY_DIR / "docker-compose.yml")
SHARED_VOL = DEPLOY_DIR / "checkpoint" / "shared"
WORKER_PORTS = [9101, 9102, 9103, 9104]
ALL_PROFILES = ["strict", "heuristic"]

# Reuse the dashboard's metric helpers so completeness is computed identically to the UI.
_spec = importlib.util.spec_from_file_location("_dash", str(DEPLOY_DIR / "dashboard.py"))
_dash = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_dash)


def _compose(*args, env=None, timeout=900):
    cmd = ["docker", "compose", "-f", COMPOSE_FILE, *args]
    return subprocess.run(cmd, cwd=str(DEPLOY_DIR), env=env, capture_output=True,
                          text=True, encoding="utf-8", errors="replace", timeout=timeout)


def _down():
    cmd = ["docker", "compose", "-f", COMPOSE_FILE]
    for p in ALL_PROFILES:
        cmd += ["--profile", p]
    cmd += ["down", "--remove-orphans"]
    subprocess.run(cmd, cwd=str(DEPLOY_DIR), capture_output=True, text=True,
                   encoding="utf-8", errors="replace", timeout=180)


def _clear_shared():
    import shutil
    try:
        shutil.rmtree(SHARED_VOL)
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"  [warn] could not clear {SHARED_VOL}: {e}")
    SHARED_VOL.mkdir(parents=True, exist_ok=True)


def _up(mode: str, env: dict, build: bool):
    profiles = ["strict"] if mode == "strict" else ["heuristic"]
    cmd = ["docker", "compose", "-f", COMPOSE_FILE]
    for p in profiles:
        cmd += ["--profile", p]
    cmd += ["up", "-d"]
    if build:
        cmd += ["--build"]
    return subprocess.run(cmd, cwd=str(DEPLOY_DIR), env=env, capture_output=True,
                          text=True, encoding="utf-8", errors="replace", timeout=900)


def _ingestor_eof() -> bool:
    r = _compose("logs", "--tail", "6", "--no-color", "ingestor", timeout=30)
    return "eof=True" in (r.stdout or "")


def _read_worker(port: int):
    try:
        with urllib.request.urlopen(f"http://localhost:{port}/api/metrics", timeout=6) as r:
            return json.load(r)
    except Exception:
        return None


def _agg(mode: str, retries: int = 4) -> dict:
    """Robust aggregate read: query each worker /api/metrics DIRECTLY with a
    generous timeout (sequential, not the dashboard's 1.5s parallel fetch which
    drops slow workers under load and produces spurious dips). Completeness is
    computed identically to the dashboard: on_time / (received - duplicates)."""
    blank = {"total_received": 0, "on_time": 0, "late_dropped": 0,
             "data_completeness_pct": -1.0, "late_arrival_rate_pct": 0.0,
             "wm_lag_max_s": 0.0, "proc_lat_p99_us": 0.0, "l_eff_ms": 0.0, "_workers": 0}
    for _ in range(retries):
        ms = [_read_worker(p) for p in WORKER_PORTS]
        if all(m is not None for m in ms):
            tot = on = late = dup = 0
            p99 = 0.0
            leff_vals = []
            for m in ms:
                dicts = []
                if "total_received" in m and (m.get("total_received") or "partitions" not in m):
                    dicts = [m]
                elif "partitions" in m:
                    dicts = [pd for pd in m["partitions"].values() if isinstance(pd, dict)]
                else:
                    dicts = [m]
                for d in dicts:
                    tot += d.get("total_received", 0)
                    on += d.get("on_time", 0)
                    late += d.get("late_dropped", 0)
                    dup += d.get("duplicates", 0)
                    p99 = max(p99, float(d.get("proc_latency_p99_us", 0) or 0))
                    le = d.get("L_eff_s")
                    if le is not None and float(le) > 0:
                        leff_vals.append(float(le))
            if tot == 0:
                return blank  # no data yet → sentinel completeness = -1
            uniq = max(tot - dup, 1)
            leff_ms = round(1000.0 * sum(leff_vals) / len(leff_vals), 1) if leff_vals else 0.0
            return {"total_received": tot, "on_time": on, "late_dropped": late,
                    "data_completeness_pct": round(100.0 * on / uniq, 3),
                    "late_arrival_rate_pct": round(100.0 * late / max(tot, 1), 3),
                    "wm_lag_max_s": 0.0, "proc_lat_p99_us": round(p99, 1),
                    "l_eff_ms": leff_ms, "_workers": 4}
        time.sleep(1)
    return blank


def _wait_healthy(timeout_s: int = 120) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        ok = 0
        for p in WORKER_PORTS:
            try:
                with urllib.request.urlopen(f"http://localhost:{p}/health", timeout=2) as resp:
                    if resp.status == 200:
                        ok += 1
            except Exception:
                pass
        if ok == len(WORKER_PORTS):
            return True
        time.sleep(3)
    return False


def _dataset_rows(dataset: str) -> int:
    """Count data rows (excluding header) of dataset/<file>, or 0 if unknown."""
    p = PROJECT_ROOT / "dataset" / dataset
    try:
        with p.open("r", encoding="utf-8", errors="replace") as f:
            return max(sum(1 for _ in f) - 1, 0)
    except Exception:
        return 0


def run_point(mode: str, punctuation: str, dataset: str, sweep_var: str, sweep_value: float,
              max_wait: int, settle: int, log_level: str) -> dict:
    print(f"\n=== {sweep_var} = {sweep_value} | mode={mode} | punct={punctuation} ===")
    target_rows = _dataset_rows(dataset)
    if target_rows:
        print(f"  dataset rows = {target_rows:,} (will wait until ~all consumed)")
    _down()
    _clear_shared()

    import os
    env = os.environ.copy()
    env.update({
        "MODE": mode,
        "DATASET_FILE": dataset,
        "PUNCTUATION_MODE": punctuation,
        "LOG_LEVEL": log_level,
        # Moderate-high backpressure room so the small experiment dataset drains
        # without an OOM crash (very high limits) or a pause-without-resume stall
        # (default limits). We isolate the WATERMARK effect on completeness.
        "BP_PAUSE_THRESHOLD": "100000",
        "BP_RESUME_THRESHOLD": "5000",
        "STRICT_HARD_QUEUE_LIMIT": "300000",
    })
    # Forward arrival-paced replay knobs if set in the launcher's environment
    # (INGESTOR_REPLAY=arrival REPLAY_SPEED=50). Paced replay makes max_event_time
    # advance gradually → cleaner completeness-vs-wait curve (vs fast-send which
    # makes the watermark aggressive).
    for k in ("INGESTOR_REPLAY", "REPLAY_SPEED", "INGESTOR_SLEEP_S", "PUNCTUATION_INTERVAL_S", "HEURISTIC_WARMUP_SAMPLES", "HEURISTIC_WARMUP_S", "HEURISTIC_LOCAL_WATERMARK_CLOSE"):
        if os.environ.get(k):
            env[k] = os.environ[k]
    # The swept independent variable (DELTA_BASE_S for strict, HEURISTIC_P_NORMAL
    # for heuristic). docker-compose.yml interpolates both into the workers.
    env[sweep_var] = str(sweep_value)
    print("  bringing cluster up...")
    up = _up(mode, env, build=run_point._first)
    run_point._first = False
    if up.returncode != 0:
        print("  [error] compose up failed:\n", (up.stdout or "") + (up.stderr or ""))
        return {}

    if not _wait_healthy(150):
        print("  [warn] not all workers became healthy; continuing anyway")

    t0 = time.time()
    last_total = -1
    stable = 0
    eof_seen = False
    print("  running dataset to EOF...")
    while time.time() - t0 < max_wait:
        time.sleep(5)
        a = _agg(mode)
        total = a.get("total_received", 0)
        comp = a.get("data_completeness_pct", -1.0)
        if not eof_seen and _ingestor_eof():
            eof_seen = True
            print(f"  [eof] ingestor reached EOF at t={int(time.time()-t0)}s (received={total:,})")
        if total == last_total:
            stable += 1
        else:
            stable = 0
            last_total = total
        consumed_ok = (target_rows == 0) or (total >= 0.9999 * target_rows)
        comp_str = f"{comp:6.2f}%" if comp >= 0 else "     N/A"
        print(f"    t={int(time.time()-t0):>4}s received={total:>9,}"
              f"{('/'+format(target_rows,',')) if target_rows else ''} "
              f"completeness={comp_str} eof={eof_seen} consumed_ok={consumed_ok} stable={stable}")
        # Done only when: ingestor EOF, ~all rows consumed, AND the count has
        # held steady (drained). consumed_ok guards against a backpressure lull
        # being mistaken for completion.
        if eof_seen and consumed_ok and stable >= 3:
            break

    # Settle: let final windows close, then sample completeness for the average.
    # Skip samples where total_received == 0 — completeness is meaningless when
    # no data has arrived (would be 0% and skew the average downward).
    print(f"  settling {settle}s and sampling steady-state completeness...")
    samples = []
    s_end = time.time() + settle
    final = _agg(mode)
    while time.time() < s_end:
        a = _agg(mode)
        if a.get("total_received", 0) > 0:
            samples.append(a.get("data_completeness_pct", 0.0))
            final = a
        time.sleep(5)

    comp_avg = round(statistics.mean(samples), 3) if samples else (
        final.get("data_completeness_pct", 0.0) if final.get("total_received", 0) > 0 else 0.0)
    leff_ms = round(final.get("l_eff_ms", 0.0), 1)
    # Wait-time axis: strict uses the CONFIGURED δ (DELTA_BASE_S); heuristic uses
    # the REALIZED effective lag L_eff measured from the DDSketch.
    if mode == "strict":
        wait_ms = int(sweep_value * 1000)
    else:
        wait_ms = leff_ms if leff_ms > 0 else int(sweep_value * 1000)
    row = {
        "mode": mode,
        "punctuation": punctuation,
        "sweep_var": sweep_var,
        "sweep_value": sweep_value,
        "wait_time_ms": wait_ms,
        "wait_time_s": round(wait_ms / 1000.0, 3),
        "l_eff_ms": leff_ms,
        "completeness_final_pct": round(final.get("data_completeness_pct", 0.0), 3),
        "completeness_avg_pct": comp_avg,
        "late_rate_pct": round(final.get("late_arrival_rate_pct", 0.0), 3),
        "total_received": final.get("total_received", 0),
        "on_time": final.get("on_time", 0),
        "late_dropped": final.get("late_dropped", 0),
        "proc_lat_p99_us": round(final.get("proc_lat_p99_us", 0.0), 1),
        "run_seconds": int(time.time() - t0),
        "samples": len(samples),
    }
    print(f"  -> {sweep_var}={sweep_value}  wait~{wait_ms}ms  "
          f"completeness final={row['completeness_final_pct']}% avg={row['completeness_avg_pct']}%  "
          f"late={row['late_rate_pct']}%  received={row['total_received']:,}")
    return row


run_point._first = True


def write_reports(rows: list[dict], out_dir: Path, mode: str, dataset: str, punctuation: str):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = out_dir / f"completeness_vs_wait_{mode}_{ts}.csv"
    md_path = out_dir / f"completeness_vs_wait_{mode}_{ts}.md"

    cols = ["mode", "punctuation", "sweep_var", "sweep_value", "wait_time_ms", "wait_time_s",
            "l_eff_ms", "completeness_final_pct", "completeness_avg_pct", "late_rate_pct",
            "total_received", "on_time", "late_dropped", "proc_lat_p99_us", "run_seconds", "samples"]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in cols})

    overall_avg = round(statistics.mean([r["completeness_avg_pct"] for r in rows]), 3) if rows else 0.0
    knob = rows[0].get("sweep_var", "wait") if rows else "wait"
    wait_note = ("configured δ (DELTA_BASE_S)" if mode == "strict"
                 else "realized effective lag L_eff measured from DDSketch")
    lines = [
        f"# Data Completeness % vs Wait Time (ms) — {mode} mode",
        "",
        f"- Dataset: `{dataset}`  ·  Punctuation: `{punctuation}`  ·  Generated: {ts}",
        f"- Swept variable: `{knob}`  ·  Wait Time axis = {wait_note}.",
        f"- Completeness measured AFTER full dataset processed (EOF + settle), "
        f"`completeness_avg_pct` = mean of steady-state samples.",
        f"- **Overall average completeness across all points: {overall_avg}%**",
        "",
        f"| {knob} | Wait Time (ms) | L_eff (ms) | Completeness avg % | Completeness final % | Late rate % | Received | Proc p99 (µs) |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for r in sorted(rows, key=lambda x: x["wait_time_ms"]):
        lines.append(
            f"| {r.get('sweep_value')} | {r['wait_time_ms']} | {r.get('l_eff_ms', 0)} | "
            f"{r['completeness_avg_pct']} | {r['completeness_final_pct']} | {r['late_rate_pct']} | "
            f"{r['total_received']:,} | {r['proc_lat_p99_us']} |")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return csv_path, md_path, overall_avg


def main():
    ap = argparse.ArgumentParser(description="Completeness % vs Wait Time experiment runner")
    ap.add_argument("--mode", default="strict", choices=["strict", "heuristic"])
    ap.add_argument("--punctuation", default="data-driven",
                    choices=["wall-clock", "data-driven", "max-event-time"])
    ap.add_argument("--dataset", default="nyc_taxi_events_full.csv", help="DATASET_FILE relative to dataset/")
    ap.add_argument("--deltas", default="0,2,5,10,20",
                    help="strict: comma-separated wait times in SECONDS (DELTA_BASE_S)")
    ap.add_argument("--ps", default="0.5,0.8,0.95,0.99,0.999",
                    help="heuristic: comma-separated HEURISTIC_P_NORMAL percentiles to sweep")
    ap.add_argument("--repeats", type=int, default=1)
    ap.add_argument("--max-wait", type=int, default=300, help="max seconds to wait for EOF per run")
    ap.add_argument("--settle", type=int, default=25, help="seconds to sample steady-state after EOF")
    ap.add_argument("--log-level", default="info")
    ap.add_argument("--keep-up", action="store_true", help="leave the cluster running at the end")
    args = ap.parse_args()

    # Pick the swept independent variable per mode.
    if args.mode == "strict":
        sweep_var = "DELTA_BASE_S"
        values = [float(x) for x in args.deltas.split(",") if x.strip() != ""]
    else:
        sweep_var = "HEURISTIC_P_NORMAL"
        values = [float(x) for x in args.ps.split(",") if x.strip() != ""]
    out_dir = PROJECT_ROOT / "docs"
    out_dir.mkdir(exist_ok=True)

    print(f"Experiment: mode={args.mode} punct={args.punctuation} dataset={args.dataset} "
          f"sweep {sweep_var}={values} repeats={args.repeats}")
    rows = []
    try:
        for d in values:
            reps = []
            for r in range(args.repeats):
                if args.repeats > 1:
                    print(f"  [repeat {r+1}/{args.repeats}]")
                row = run_point(args.mode, args.punctuation, args.dataset, sweep_var, d,
                                args.max_wait, args.settle, args.log_level)
                if row:
                    reps.append(row)
            if not reps:
                continue
            if len(reps) == 1:
                rows.append(reps[0])
            else:
                merged = dict(reps[0])
                merged["completeness_avg_pct"] = round(statistics.mean(r["completeness_avg_pct"] for r in reps), 3)
                merged["completeness_final_pct"] = round(statistics.mean(r["completeness_final_pct"] for r in reps), 3)
                merged["late_rate_pct"] = round(statistics.mean(r["late_rate_pct"] for r in reps), 3)
                merged["repeats"] = len(reps)
                rows.append(merged)
    finally:
        if not args.keep_up:
            print("\nTearing down cluster...")
            _down()

    if rows:
        csv_path, md_path, overall = write_reports(rows, out_dir, args.mode, args.dataset, args.punctuation)
        print("\n================ REPORT ================")
        print(f"Overall average completeness: {overall}%")
        print(f"CSV : {csv_path}")
        print(f"MD  : {md_path}")
        for r in sorted(rows, key=lambda x: x["wait_time_s"]):
            print(f"  wait={r['wait_time_ms']:>6} ms | completeness avg={r['completeness_avg_pct']:>6.2f}% "
                  f"final={r['completeness_final_pct']:>6.2f}% | late={r['late_rate_pct']:>5.2f}% | "
                  f"recv={r['total_received']:,}")
    else:
        print("No data points collected.")
        sys.exit(1)


if __name__ == "__main__":
    main()
