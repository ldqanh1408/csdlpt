"""Streamlit Dashboard for CSDLPT Watermark System.

Drives both docker-compose.yml (full) and docker-compose.sim.yml (simulation).

Run:
    pip install -r deploy/requirements-dashboard.txt
    streamlit run deploy/dashboard.py
"""

import streamlit as st
import subprocess
import requests
import time
import json
import os
import glob
import pandas as pd
import concurrent.futures
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

DEPLOY_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = DEPLOY_DIR.parent
COMPOSE_FILE = str(DEPLOY_DIR / "docker-compose.yml")
COMPOSE_SIM_FILE = str(DEPLOY_DIR / "docker-compose.sim.yml")
DATASET_DIR = str(PROJECT_ROOT / "dataset")

# ---------------------------------------------------------------------------
# Service topology
# ---------------------------------------------------------------------------

WORKERS_FULL = {
    "node0": {"port": 9101, "partitions": "0,1,2"},
    "node1": {"port": 9102, "partitions": "3,4,5"},
    "node2": {"port": 9103, "partitions": "6,7,8"},
    "node3": {"port": 9104, "partitions": "9,10,11"},
}

WORKERS_SIM = {
    "strict-worker":  {"port": 9101, "partitions": "0..11", "profile": "strict"},
    "heuristic-worker": {"port": 9111, "partitions": "0..11", "profile": "heuristic"},
}

INFRA_FULL = {
    "Kafka":      {"port": 29092, "health": None},
    "ZooKeeper":  {"port": 2181, "health": None},
    "Prometheus": {"port": 9090, "health": "http://localhost:9090/-/healthy"},
    "Grafana":    {"port": 3000, "health": "http://localhost:3000/api/health"},
    "MinIO":      {"port": 9002, "health": "http://localhost:9002/minio/health/live"},
}

INFRA_SIM = {}

COORDINATORS_FULL = {
    "coordinator-1": 9000,
    "coordinator-2": 9003,
    "coordinator-3": 9004,
}

COORDINATORS_SIM = {
    "strict-coordinator": 9000,
}

AGGREGATOR_PORT_FULL = 9007
AGGREGATOR_PORT_SIM = 9017

MAX_HISTORY = 500
REFRESH_INTERVAL_S = 3


# ---------------------------------------------------------------------------
# Mode helpers
# ---------------------------------------------------------------------------

def _is_sim() -> bool:
    return st.session_state.get("deploy_mode", "full") == "sim"


def _compose_file() -> str:
    return COMPOSE_SIM_FILE if _is_sim() else COMPOSE_FILE


def _workers() -> dict:
    if _is_sim():
        mode = st.session_state.get("mode", "strict")
        return {k: v for k, v in WORKERS_SIM.items() if v.get("profile") == mode}
    return WORKERS_FULL


def _infra() -> dict:
    return INFRA_SIM if _is_sim() else INFRA_FULL


def _coordinators() -> dict:
    return COORDINATORS_SIM if _is_sim() else COORDINATORS_FULL


def _aggregator_port() -> int:
    return AGGREGATOR_PORT_SIM if _is_sim() else AGGREGATOR_PORT_FULL


# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------

def init_state():
    defaults = {
        "running": False,
        "mode": "strict",
        "deploy_mode": "full",
        "auto_refresh": True,
        "metrics_history": [],
        "start_time": None,
        "last_metrics": None,
        "container_statuses": {},
        "run_results": {},
        "log_level": "info",
        "punctuation_mode": "data-driven",
        "log_filter": "",
        "log_auto_scroll": True,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------

def get_datasets() -> dict[str, str]:
    csvs = glob.glob(os.path.join(DATASET_DIR, "**/*.csv"), recursive=True)
    return {os.path.relpath(f, DATASET_DIR).replace("\\", "/"): f for f in csvs}


@st.cache_data
def count_csv_rows(path: str) -> int:
    try:
        with open(path) as f:
            return sum(1 for _ in f) - 1
    except Exception:
        return -1


# ---------------------------------------------------------------------------
# Docker Compose
# ---------------------------------------------------------------------------

def _compose_base() -> list[str]:
    return ["docker", "compose", "-f", _compose_file()]


def _make_env(mode: str) -> dict:
    env = os.environ.copy()
    env["MODE"] = mode
    env["DATASET_FILE"] = st.session_state.get("dataset_file", "data.csv")
    env["LOG_LEVEL"] = st.session_state.get("log_level", "info")
    env["PUNCTUATION_MODE"] = st.session_state.get("punctuation_mode", "data-driven")
    return env


def compose_up(mode: str, dataset_file: str) -> tuple[bool, str]:
    env = _make_env(mode)
    env["DATASET_FILE"] = dataset_file

    if _is_sim():
        profiles = [mode]
    else:
        profiles = [mode]
        if mode == "hybrid":
            profiles.extend(["heuristic"])

    cmd = _compose_base()
    for p in profiles:
        cmd.extend(["--profile", p])
    cmd.extend(["up", "-d", "--build"])

    try:
        r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8",
                           errors="replace", env=env, cwd=str(DEPLOY_DIR), timeout=600)
        return r.returncode == 0, r.stdout + r.stderr
    except subprocess.TimeoutExpired:
        return False, "Build timed out (10 min)"
    except Exception as e:
        return False, str(e)


def compose_down() -> tuple[bool, str]:
    if _is_sim():
        profiles = ["strict", "heuristic"]
    else:
        profiles = ["strict", "heuristic", "hybrid"]

    cmd = _compose_base()
    for p in profiles:
        cmd.extend(["--profile", p])
    cmd.extend(["down", "-v", "--remove-orphans"])
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8",
                           errors="replace", cwd=str(DEPLOY_DIR), timeout=120)
        return r.returncode == 0, r.stdout + r.stderr
    except Exception as e:
        return False, str(e)


def compose_logs(service: str = "", tail: int = 150) -> str:
    if _is_sim():
        profiles = ["strict", "heuristic"]
    else:
        profiles = ["strict", "heuristic", "hybrid"]

    cmd = _compose_base()
    for p in profiles:
        cmd.extend(["--profile", p])
    cmd.extend(["logs", "--tail", str(tail), "--no-color"])
    if service:
        cmd.append(service)
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8",
                           errors="replace", cwd=str(DEPLOY_DIR), timeout=15)
        return r.stdout + r.stderr
    except Exception:
        return "(could not fetch logs)"


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def format_timestamp(val) -> str:
    """Format an epoch float to 'YYYY-MM-DD HH:MM:SS' if it looks like a Unix timestamp.
    Returns a dash for None/inf/-inf, or the raw value as string otherwise.
    """
    if val is None:
        return "—"
    try:
        v = float(val)
    except (TypeError, ValueError):
        return str(val)
    if v in (float("inf"), float("-inf")) or v != v:  # nan check
        return "—"
    if v > 1e8:  # looks like Unix epoch (after year 1973)
        try:
            return datetime.fromtimestamp(v).strftime("%Y-%m-%d %H:%M:%S")
        except (OSError, ValueError, OverflowError):
            return f"{v:.1f}"
    return f"{v:.3f}"


# ---------------------------------------------------------------------------
# Metrics fetching
# ---------------------------------------------------------------------------

def _get_json(url: str, timeout: float = 2.0):
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None


def is_healthy(port: int) -> bool:
    try:
        r = requests.get(f"http://localhost:{port}/health", timeout=1.5)
        return r.status_code == 200
    except Exception:
        return False


def check_infra_health(name: str) -> bool:
    info = _infra()[name]
    if info.get("health"):
        try:
            r = requests.get(info["health"], timeout=2)
            return r.status_code == 200
        except Exception:
            return False
    try:
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1.5)
        s.connect(("localhost", info["port"]))
        s.close()
        return True
    except Exception:
        return False


def _infra_key(name: str) -> str | None:
    """Return the configured infra display key for a docker service name."""
    lname = name.lower()
    for key in _infra():
        if key.lower() == lname:
            return key
    return None


def fetch_all_metrics(mode: str) -> dict:
    result = {"timestamp": time.time(), "mode": mode, "workers": {}}

    urls_to_fetch = {}

    # 1. Workers
    for name, info in _workers().items():
        port = info["port"] if isinstance(info, dict) else info
        urls_to_fetch[f"w_m_{name}"] = (f"http://localhost:{port}/api/metrics", 1.5)
        urls_to_fetch[f"w_s_{name}"] = (f"http://localhost:{port}/state", 1.5)

    # 2. Coordinators
    if mode in ("strict", "hybrid"):
        for cname, cport in _coordinators().items():
            urls_to_fetch[f"c_{cname}"] = (f"http://localhost:{cport}/state", 1.5)

    # 3. Aggregator
    if mode in ("heuristic", "hybrid"):
        agg_port = _aggregator_port()
        urls_to_fetch["aggregator"] = (f"http://localhost:{agg_port}/state", 1.5)

    # Fetch in parallel
    fetched = {}
    if urls_to_fetch:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(urls_to_fetch)) as executor:
            future_to_key = {
                executor.submit(_get_json, url, timeout): key
                for key, (url, timeout) in urls_to_fetch.items()
            }
            for future in concurrent.futures.as_completed(future_to_key):
                key = future_to_key[future]
                try:
                    fetched[key] = future.result()
                except Exception:
                    fetched[key] = None

    # Assemble workers
    for name, info in _workers().items():
        port = info["port"] if isinstance(info, dict) else info
        metrics = fetched.get(f"w_m_{name}")
        state = fetched.get(f"w_s_{name}")
        if metrics or state:
            result["workers"][name] = {"metrics": metrics, "state": state, "port": port}

    # Assemble coordinator
    best_coord = None
    best_cname = None
    for cname, cport in _coordinators().items():
        coord = fetched.get(f"c_{cname}")
        if coord is None:
            continue
        wg = coord.get("W_global")
        if wg is not None and wg != float("-inf") and wg != float("inf"):
            result["coordinator"] = coord
            result["coordinator_name"] = cname
            best_coord = coord
            best_cname = cname
            break
        if best_coord is None:
            best_coord = coord
            best_cname = cname

    if best_coord:
        result["coordinator"] = best_coord
        result["coordinator_name"] = best_cname

    # Assemble aggregator
    if mode in ("heuristic", "hybrid"):
        agg = fetched.get("aggregator")
        if agg:
            result["aggregator"] = agg

    return result


def aggregate_worker_metrics(metrics: dict) -> dict:
    """Aggregate metrics from all workers into a single summary including latency percentiles."""
    total_recv = 0
    total_on_time = 0
    total_late = 0
    total_dupes = 0
    total_bp = 0
    total_non_mono = 0
    total_dlq = 0
    total_sketch = 0
    total_extreme_lag = 0

    # Latency collection across all workers/partitions
    proc_p50_vals, proc_p95_vals, proc_p99_vals = [], [], []
    sketch_p50_vals, sketch_p95_vals, sketch_p99_vals = [], [], []
    wm_lag_vals = []
    poll_decode_p95_vals, dedup_p95_vals, state_write_p95_vals = [], [], []
    sketch_update_p95_vals, sketch_query_p95_vals = [], []

    def _collect_latency_from(d: dict):
        """Extract latency fields from a partition or engine metrics dict."""
        if not isinstance(d, dict):
            return

        def _get_float(key: str) -> float:
            val = d.get(key)
            if val is None:
                return 0.0
            try:
                return float(val)
            except (TypeError, ValueError):
                return 0.0

        p50 = _get_float("proc_latency_p50_us")
        p95 = _get_float("proc_latency_p95_us")
        p99 = _get_float("proc_latency_p99_us")
        if p50 > 0: proc_p50_vals.append(p50)
        if p95 > 0: proc_p95_vals.append(p95)
        if p99 > 0: proc_p99_vals.append(p99)

        sp50 = _get_float("sketch_quantile_p50_ms")
        sp95 = _get_float("sketch_quantile_p95_ms")
        sp99 = _get_float("sketch_quantile_p99_ms")
        if sp50 > 0: sketch_p50_vals.append(sp50)
        if sp95 > 0: sketch_p95_vals.append(sp95)
        if sp99 > 0: sketch_p99_vals.append(sp99)

        wl = _get_float("watermark_lag_s")
        if wl > 0:
            wm_lag_vals.append(wl)

        for field, target in (
            ("poll_decode_latency_p95_us", poll_decode_p95_vals),
            ("dedup_latency_p95_us", dedup_p95_vals),
            ("state_write_latency_p95_us", state_write_p95_vals),
            ("sketch_update_latency_p95_us", sketch_update_p95_vals),
            ("sketch_query_latency_p95_us", sketch_query_p95_vals),
        ):
            val = _get_float(field)
            if val > 0:
                target.append(val)

    for wname, wdata in metrics.get("workers", {}).items():
        m = wdata.get("metrics")
        if m is None:
            continue
        if "total_received" in m:
            total_recv += m.get("total_received", 0)
            total_on_time += m.get("on_time", 0)
            total_late += m.get("late_dropped", 0)
            total_dupes += m.get("duplicates", 0)
            total_bp += m.get("backpressure_drops", 0)
            total_non_mono += m.get("non_monotonic_punctuation", 0)
            total_dlq += m.get("dlq_backlog", 0)
            total_sketch += m.get("sketch_total_count", 0)
            total_extreme_lag += m.get("extreme_lag_count", 0)
            _collect_latency_from(m)
            if not any(m.get(f, 0.0) for f in (
                "proc_latency_p95_us",
                "sketch_update_latency_p95_us",
                "dedup_latency_p95_us",
            )):
                for pdata in m.get("partitions", {}).values():
                    _collect_latency_from(pdata)
        elif "partitions" in m:
            for pid, pdata in m.get("partitions", {}).items():
                if not isinstance(pdata, dict):
                    continue
                total_recv += pdata.get("total_received", 0)
                total_on_time += pdata.get("on_time", 0)
                total_late += pdata.get("late_dropped", 0)
                total_dupes += pdata.get("duplicates", 0)
                total_bp += pdata.get("backpressure_drops", 0)
                total_non_mono += pdata.get("non_monotonic_punctuation", 0)
                total_dlq += pdata.get("dlq_backlog", 0)
                total_sketch += pdata.get("sketch_total_count", 0)
                total_extreme_lag += pdata.get("extreme_lag_count", 0)
                _collect_latency_from(pdata)

    unique = max(total_recv - total_dupes, 1)
    completeness = 100.0 * total_on_time / unique
    late_rate = 100.0 * total_late / max(total_recv, 1)

    def _avg(lst): return round(sum(lst) / len(lst), 2) if lst else 0.0
    def _max(lst): return round(max(lst), 2) if lst else 0.0

    return {
        "total_received": total_recv,
        "on_time": total_on_time,
        "late_dropped": total_late,
        "duplicates": total_dupes,
        "backpressure_drops": total_bp,
        "non_monotonic_punctuation": total_non_mono,
        "data_completeness_pct": round(completeness, 3),
        "late_arrival_rate_pct": round(late_rate, 3),
        "dlq_backlog": total_dlq,
        "sketch_total_count": total_sketch,
        "extreme_lag_count": total_extreme_lag,
        # Processing latency (microseconds)
        "proc_lat_p50_us": _avg(proc_p50_vals),
        "proc_lat_p95_us": _avg(proc_p95_vals),
        "proc_lat_p99_us": _avg(proc_p99_vals),
        "proc_lat_p99_max_us": _max(proc_p99_vals),
        "poll_decode_p95_us": _avg(poll_decode_p95_vals),
        "dedup_p95_us": _avg(dedup_p95_vals),
        "state_write_p95_us": _avg(state_write_p95_vals),
        "sketch_update_p95_us": _avg(sketch_update_p95_vals),
        "sketch_query_p95_us": _avg(sketch_query_p95_vals),
        # Event-time lag from DDSketch (milliseconds)
        "sketch_p50_ms": _avg(sketch_p50_vals),
        "sketch_p95_ms": _avg(sketch_p95_vals),
        "sketch_p99_ms": _avg(sketch_p99_vals),
        # Watermark lag
        "wm_lag_avg_s": _avg(wm_lag_vals),
        "wm_lag_max_s": _max(wm_lag_vals),
    }


# ---------------------------------------------------------------------------
# Heuristic-specific helpers
# ---------------------------------------------------------------------------

def _collect_partition_data(metrics: dict):
    """Extract per-partition engine summaries from all workers."""
    rows = []
    for wname, wdata in metrics.get("workers", {}).items():
        m = wdata.get("metrics") or {}
        partitions = m.get("partitions", {})
        for pid, pdata in partitions.items():
            if not isinstance(pdata, dict):
                continue
            row = {
                "worker": wname,
                "partition": str(pid),
                "W_h": pdata.get("watermark", pdata.get("W_h", float("-inf"))),
                "L_eff_s": round(pdata.get("L_eff_s", 0), 3),
                "p_current": pdata.get("p_current", 0),
                "sketch_n": pdata.get("sketch_total_count", 0),
                "received": pdata.get("total_received", 0),
                "on_time": pdata.get("on_time", 0),
                "late": pdata.get("late_dropped", 0),
                "completeness": round(pdata.get("data_completeness_pct", 0), 2),
                "dlq": pdata.get("dlq_backlog", 0),
                "dupes": pdata.get("duplicates_filtered", pdata.get("duplicates", 0)),
                "bp_drops": pdata.get("backpressure_drops", 0),
                "cold_start": pdata.get("cold_start", {}),
                "negative_lag": pdata.get("negative_lag", {}),
                "extreme_lag": pdata.get("extreme_lag_count", 0),
                "replay": pdata.get("replay_mode", pdata.get("replay_mode_active", False)),
                "sketch_p50": pdata.get("sketch_quantile_p50_ms", 0.0),
                "sketch_p95": pdata.get("sketch_quantile_p95_ms", 0.0),
                "sketch_p99": pdata.get("sketch_quantile_p99_ms", 0.0),
                "proc_p50": pdata.get("proc_latency_p50_us", 0.0),
                "proc_p95": pdata.get("proc_latency_p95_us", 0.0),
                "proc_p99": pdata.get("proc_latency_p99_us", 0.0),
                "poll_decode_p95": pdata.get("poll_decode_latency_p95_us", 0.0),
                "dedup_p95": pdata.get("dedup_latency_p95_us", 0.0),
                "state_write_p95": pdata.get("state_write_latency_p95_us", 0.0),
                "sketch_update_p95": pdata.get("sketch_update_latency_p95_us", 0.0),
                "sketch_query_p95": pdata.get("sketch_query_latency_p95_us", 0.0),
                "neg_lag_rate": pdata.get("negative_lag_rate", 0.0),
                "est_drift": pdata.get("estimator_drift", 0.0),
                "open_windows": pdata.get("open_windows", 0),
                "closed_windows": pdata.get("closed_windows", 0),
                "dedup_ttl": pdata.get("dedup_ttl_entries", 0),
                "fencing_violations": pdata.get("fencing_token_violations", 0),
                "non_monotonic_punctuation": pdata.get("non_monotonic_punctuation", 0),
            }
            rows.append(row)
    return rows


def _collect_per_window_loss(metrics: dict):
    """Collect per_window_loss from all partitions."""
    entries = []
    for wname, wdata in metrics.get("workers", {}).items():
        m = wdata.get("metrics") or {}
        partitions = m.get("partitions", {})
        for pid, pdata in partitions.items():
            if not isinstance(pdata, dict):
                continue
            for wl in pdata.get("per_window_loss", []):
                entries.append({
                    "Worker": wname,
                    "Partition": str(pid),
                    "Window": wl.get("window_id", "?"),
                    "Late": wl.get("late", 0),
                    "Total": wl.get("total", 0),
                    "Loss Rate %": round(wl.get("loss_rate", 0) * 100, 2),
                })
            overall = pdata.get("overall_loss_rate")
            if overall is not None:
                entries.append({
                    "Worker": wname,
                    "Partition": str(pid),
                    "Window": "OVERALL",
                    "Late": pdata.get("late_dropped", 0),
                    "Total": pdata.get("total_received", 0),
                    "Loss Rate %": round(overall * 100, 2),
                })
    return entries


def _heuristic_kpi_row(metrics: dict, agg: dict):
    """Render heuristic-specific KPI row below the main KPIs."""
    parts = _collect_partition_data(metrics)
    if not parts:
        return

    wh_vals = [p["W_h"] for p in parts if p["W_h"] != float("-inf")]
    leff_vals = [p["L_eff_s"] for p in parts if p["L_eff_s"] > 0]

    st.markdown("---")
    st.markdown("#### Heuristic-Specific KPIs")
    c1, c2, c3, c4, c5, c6, c7, c8 = st.columns(8)
    c1.metric("DLQ Backlog", f"{agg.get('dlq_backlog', 0):,}")
    c2.metric("Sketch Samples", f"{agg.get('sketch_total_count', 0):,}")
    c3.metric("Extreme Lag Events", f"{agg.get('extreme_lag_count', 0):,}")

    agg_state = metrics.get("aggregator", {})
    wgh = agg_state.get("W_global_h")
    if wgh is not None and wgh != float("-inf"):
        try:
            wgh_str = datetime.fromtimestamp(wgh).strftime("%Y-%m-%d %H:%M:%S")
        except (OSError, ValueError, OverflowError):
            wgh_str = f"{wgh:.1f}"
        c4.metric("W_global_h", wgh_str)
    else:
        c4.metric("W_global_h", "—")

    if wh_vals:
        try:
            max_wh_str = datetime.fromtimestamp(max(wh_vals)).strftime("%Y-%m-%d %H:%M:%S")
        except (OSError, ValueError, OverflowError):
            max_wh_str = f"{max(wh_vals):.1f}"
        try:
            min_wh_str = datetime.fromtimestamp(min(wh_vals)).strftime("%Y-%m-%d %H:%M:%S")
        except (OSError, ValueError, OverflowError):
            min_wh_str = f"{min(wh_vals):.1f}"
        c5.metric("Max W_h", max_wh_str)
        c6.metric("Min W_h", min_wh_str)
        c7.metric("W_h Span (s)", f"{max(wh_vals) - min(wh_vals):.1f}")
    else:
        c5.metric("Max W_h", "—")
        c6.metric("Min W_h", "—")
        c7.metric("W_h Span (s)", "—")

    if leff_vals:
        c8.metric("Avg L_eff (s)", f"{sum(leff_vals) / len(leff_vals):.3f}")
    else:
        c8.metric("Avg L_eff (s)", "—")

    # Cold start / negative lag status
    cold_phases = {}
    neg_tiers = {}
    for p in parts:
        cs = p.get("cold_start", {})
        if cs:
            phase = cs.get("phase", "?")
            cold_phases[phase] = cold_phases.get(phase, 0) + 1
        nl = p.get("negative_lag", {})
        if nl:
            tier = nl.get("tier", "?")
            neg_tiers[tier] = neg_tiers.get(tier, 0) + 1

    if cold_phases or neg_tiers:
        st.markdown("---")
        ccol1, ccol2 = st.columns(2)
        with ccol1:
            if cold_phases:
                st.caption(f"Cold Start: {', '.join(f'{k}:{v}' for k, v in sorted(cold_phases.items()))}")
        with ccol2:
            if neg_tiers:
                st.caption(f"Negative Lag: {', '.join(f'{k}:{v}' for k, v in sorted(neg_tiers.items()))}")


# ---------------------------------------------------------------------------
# UI — Sidebar
# ---------------------------------------------------------------------------

def render_sidebar():
    st.sidebar.markdown("## CSDLPT Watermark")
    st.sidebar.markdown("---")

    # Deploy mode
    deploy_mode = st.sidebar.radio(
        "Deploy Mode",
        options=["full", "sim"],
        format_func=lambda m: {
            "full": "Full (Kafka, ZK, 4 workers, HA)",
            "sim": "Sim (HTTP-only, 1 worker, no infra)",
        }[m],
        index=0 if st.session_state.deploy_mode == "full" else 1,
        disabled=st.session_state.running,
    )
    st.session_state.deploy_mode = deploy_mode

    st.sidebar.markdown("---")

    mode = st.sidebar.radio(
        "Watermark Mode",
        options=["strict", "heuristic"],
        format_func=lambda m: {
            "strict": "Strict (0% loss, ~15s latency)",
            "heuristic": "Heuristic (<=1% loss, ~5s latency)",
        }[m],
        index=0 if st.session_state.mode == "strict" else 1,
        disabled=st.session_state.running,
    )
    st.session_state.mode = mode

    st.sidebar.markdown("---")
    datasets = get_datasets()
    if not datasets:
        st.sidebar.warning("No CSV in dataset/")
        selected_ds = "data.csv"
    else:
        selected_ds = st.sidebar.selectbox(
            "Dataset", list(datasets.keys()), disabled=st.session_state.running,
        )
        rc = count_csv_rows(datasets[selected_ds])
        if rc >= 0:
            st.sidebar.caption(f"{rc:,} rows")

    st.sidebar.markdown("---")
    st.sidebar.markdown("**Pipeline Settings**")
    st.session_state.log_level = st.sidebar.selectbox(
        "Log Level",
        options=["info", "debug", "trace"],
        index=["info", "debug", "trace"].index(st.session_state.log_level),
        disabled=st.session_state.running,
        help="info=summary every 5s | debug/trace=per-event flow through containers",
    )
    st.session_state.punctuation_mode = st.sidebar.selectbox(
        "Punctuation Mode",
        options=["data-driven", "wall-clock"],
        index=0 if st.session_state.punctuation_mode == "data-driven" else 1,
        disabled=st.session_state.running,
        help="data-driven: T_commit tracks CSV timestamps (100% completeness). wall-clock: T_commit=now (real-time streaming).",
    )
    st.sidebar.markdown("---")
    c1, c2 = st.sidebar.columns(2)
    with c1:
        start = st.button("Start", disabled=st.session_state.running,
                          width="stretch", type="primary")
    with c2:
        stop = st.button("Stop", disabled=not st.session_state.running,
                         width="stretch")

    if start and not st.session_state.running:
        with st.sidebar.status("Building & starting...", expanded=True) as s:
            st.write(f"Deploy: **{deploy_mode}** | Mode: **{mode}** | Dataset: **{selected_ds}**")
            st.write(f"Log: **{st.session_state.log_level}** | Punctuation: **{st.session_state.punctuation_mode}**")
            ok, out = compose_up(mode, selected_ds)
            if ok:
                s.update(label="All services started!", state="complete")
                st.session_state.running = True
                st.session_state.start_time = time.time()
                st.session_state.metrics_history = []
                st.session_state.last_metrics = None
            else:
                s.update(label="Start failed", state="error")
                st.sidebar.error(out[-800:] if len(out) > 800 else out)

    if stop and st.session_state.running:
        with st.sidebar.status("Stopping all containers...") as s:
            if st.session_state.last_metrics:
                agg = aggregate_worker_metrics(st.session_state.last_metrics)
                st.session_state.run_results[mode] = {
                    "agg": agg, "raw": st.session_state.last_metrics,
                    "duration_s": time.time() - (st.session_state.start_time or time.time()),
                    "timestamp": datetime.now().strftime("%H:%M:%S"),
                    "deploy_mode": deploy_mode,
                }
            compose_down()
            st.session_state.running = False
            s.update(label="Stopped", state="complete")

    st.sidebar.markdown("---")
    st.sidebar.caption("🟢 **Real-time Auto-refresh enabled.** Stats update every 3 seconds while running.")

    # Service status
    if st.session_state.running:
        st.sidebar.markdown("---")

        infra = _infra()
        if infra:
            st.sidebar.markdown("**Infrastructure**")
            for name in infra:
                ok = check_infra_health(name)
                st.sidebar.markdown(f"{'🟢' if ok else '🔴'} {name}")

        coords = _coordinators()
        if coords:
            st.sidebar.markdown("**Coordinators**")
            for cname, cport in coords.items():
                ok = is_healthy(cport)
                st.sidebar.markdown(f"{'🟢' if ok else '🔴'} {cname} (:{cport})")

        if mode in ("heuristic", "hybrid"):
            st.sidebar.markdown("**Aggregator**")
            ok = is_healthy(_aggregator_port())
            st.sidebar.markdown(f"{'🟢' if ok else '🔴'} aggregator (:{_aggregator_port()})")

        st.sidebar.markdown("**Workers**")
        for wname, winfo in _workers().items():
            port = winfo["port"] if isinstance(winfo, dict) else winfo
            ok = is_healthy(port)
            st.sidebar.markdown(f"{'🟢' if ok else '🔴'} {wname} (:{port})")

        if st.session_state.start_time:
            elapsed = time.time() - st.session_state.start_time
            m, s_ = divmod(int(elapsed), 60)
            st.sidebar.caption(f"Running {m}m {s_}s")

        if not _is_sim():
            st.sidebar.markdown("---")
            st.sidebar.markdown("**Links**")
            st.sidebar.markdown("- [Grafana](http://localhost:3000) (admin/admin)")
            st.sidebar.markdown("- [Prometheus](http://localhost:9090)")
            st.sidebar.markdown("- [MinIO Console](http://localhost:9001)")


# ---------------------------------------------------------------------------
# UI — Dashboard
# ---------------------------------------------------------------------------

def render_dashboard(mode: str, metrics: dict):
    agg = aggregate_worker_metrics(metrics)

    is_sim = _is_sim()
    label = f"{'Sim' if is_sim else 'Full'} — {'Strict' if mode == 'strict' else 'Heuristic'} Watermark"
    worker_count = len(_workers())
    if is_sim:
        st.markdown(f"### {label} (1 worker, 12 partitions)")
    else:
        st.markdown(f"### {label} — Aggregated Metrics ({worker_count} workers, 12 partitions)")

    # Top-level KPIs
    c1, c2, c3, c4, c5, c6, c7, c8 = st.columns(8)
    comp_val = agg['data_completeness_pct']
    comp_delta = None
    if len(st.session_state.metrics_history) >= 2:
        prev_comp = st.session_state.metrics_history[-2]["completeness"]
        if prev_comp > 0:
            comp_delta = f"{comp_val - prev_comp:+.2f}%"
    c1.metric("Completeness", f"{comp_val:.2f}%", delta=comp_delta)
    c2.metric("Total Received", f"{agg['total_received']:,}")
    c3.metric("On-Time", f"{agg['on_time']:,}")
    c4.metric("Late Dropped", f"{agg['late_dropped']:,}")
    c5.metric("Late Rate", f"{agg['late_arrival_rate_pct']:.2f}%")
    c6.metric("Duplicates", f"{agg['duplicates']:,}")
    c7.metric("BP Drops", f"{agg['backpressure_drops']:,}")
    c8.metric("Non-Mono Punct", f"{agg['non_monotonic_punctuation']:,}")

    # Heuristic-specific KPIs
    if mode == "heuristic":
        _heuristic_kpi_row(metrics, agg)

    # Performance Latency KPIs (both strict & heuristic)
    p50 = agg.get("proc_lat_p50_us", 0.0)
    p95 = agg.get("proc_lat_p95_us", 0.0)
    p99 = agg.get("proc_lat_p99_us", 0.0)
    p99mx = agg.get("proc_lat_p99_max_us", 0.0)
    sp50 = agg.get("sketch_p50_ms", 0.0)
    sp95 = agg.get("sketch_p95_ms", 0.0)
    sp99 = agg.get("sketch_p99_ms", 0.0)
    wlag_avg = agg.get("wm_lag_avg_s", 0.0)
    wlag_max = agg.get("wm_lag_max_s", 0.0)
    if metrics.get("workers"):
        st.markdown("---")
        st.markdown("#### Processing Latency & Lag")
        lc1, lc2, lc3, lc4, lc5, lc6, lc7 = st.columns(7)
        lc1.metric("Proc p50", f"{p50:.0f} µs" if p50 else "—")
        lc2.metric("Proc p95", f"{p95:.0f} µs" if p95 else "—")
        lc3.metric("Proc p99", f"{p99:.0f} µs" if p99 else "—")
        lc4.metric("Proc p99 (max)", f"{p99mx:.0f} µs" if p99mx else "—")
        lc5.metric("Event-Lag p50", f"{sp50:.1f} ms" if sp50 else "—")
        lc6.metric("Event-Lag p99", f"{sp99:.1f} ms" if sp99 else "—")
        lc7.metric("WM Lag (avg/max)", f"{wlag_avg:.1f}s / {wlag_max:.1f}s" if wlag_avg else "—")
        if mode == "strict":
            sl1, sl2, sl3 = st.columns(3)
            sl1.metric("Poll Decode p95", f"{agg.get('poll_decode_p95_us', 0.0):.0f} us" if agg.get("poll_decode_p95_us", 0.0) else "—")
            sl2.metric("Dedup p95", f"{agg.get('dedup_p95_us', 0.0):.0f} us" if agg.get("dedup_p95_us", 0.0) else "—")
            sl3.metric("State Write p95", f"{agg.get('state_write_p95_us', 0.0):.0f} us" if agg.get("state_write_p95_us", 0.0) else "—")
        else:
            sl1, sl2, sl3 = st.columns(3)
            sl1.metric("Event-Lag p95", f"{sp95:.1f} ms" if sp95 else "—")
            sl2.metric("Sketch Update p95", f"{agg.get('sketch_update_p95_us', 0.0):.0f} us" if agg.get("sketch_update_p95_us", 0.0) else "—")
            sl3.metric("Sketch Query p95", f"{agg.get('sketch_query_p95_us', 0.0):.0f} us" if agg.get("sketch_query_p95_us", 0.0) else "—")

    st.markdown("---")

    # Control plane + per-worker breakdown
    col_ctrl, col_workers = st.columns([1, 2])

    with col_ctrl:
        if mode == "strict":
            st.markdown("#### Coordinator")
            coord = metrics.get("coordinator", {})
            wg = coord.get("W_global")
            if wg is not None and wg != float("-inf"):
                try:
                    wm_str = datetime.fromtimestamp(wg).strftime("%Y-%m-%d %H:%M:%S")
                except (OSError, ValueError, OverflowError):
                    wm_str = f"{wg:.1f}"
                st.metric("W_global", wm_str)
            else:
                st.metric("W_global", "initializing...")
            st.caption(f"Leader: {metrics.get('coordinator_name', '?')}")
            st.caption(f"Term: {coord.get('term', 0)}")
            st.caption(f"Partitions: {coord.get('partition_count', 0)} | Workers: {coord.get('active_workers', 0)}")
            skew_ms = coord.get("node_skew_max_ms", 0)
            lag_s = coord.get("watermark_lag_s", 0)
            diag = coord.get("combined_diagnosis", "?")
            skew_status = coord.get("skew_status", "OK")
            lag_status = coord.get("lag_status", "OK")
            combined = coord.get("combined_status", "?")
            st.caption(f"Skew: {skew_ms:.0f}ms ({skew_status}) | Lag: {lag_s:.1f}s ({lag_status})")
            status_color = {"Healthy": "green", "Degraded": "orange", "Warning": "orange", "Critical": "red"}
            color = status_color.get(combined, "violet")
            st.markdown(f"**Status:** :{color}[{combined}] — *{diag}*")
            fv = coord.get("fencing_violations", 0)
            if fv > 0:
                st.caption(f"Fencing violations: {fv}")
        else:
            st.markdown("#### Aggregator")
            agg_state = metrics.get("aggregator", {})
            wgh = agg_state.get("W_global_h")
            if wgh is not None and wgh != float("-inf"):
                try:
                    wm_str = datetime.fromtimestamp(wgh).strftime("%Y-%m-%d %H:%M:%S")
                except (OSError, ValueError, OverflowError):
                    wm_str = f"{wgh:.1f}"
                st.metric("W_global_h", wm_str)
            else:
                st.metric("W_global_h", "initializing...")
            st.caption(f"Active partitions: {agg_state.get('active_count', '?')}")
            st.caption(f"HA failovers: {agg_state.get('ha_failover_count', 0)}")
            wlag = agg_state.get("watermark_lag_s", 0)
            if wlag and wlag != float("inf"):
                st.caption(f"Watermark lag: {wlag:.1f}s")

    with col_workers:
        st.markdown("#### Per-Worker Breakdown")
        rows = []
        for wname, wdata in metrics.get("workers", {}).items():
            m = wdata.get("metrics")
            if m is None:
                continue
            part_values = [
                p for p in m.get("partitions", {}).values()
                if isinstance(p, dict)
            ]

            def _avg_part(field: str) -> float:
                vals = [p.get(field, 0.0) for p in part_values if p.get(field, 0.0)]
                return round(sum(vals) / len(vals), 2) if vals else 0.0

            if "total_received" in m:
                rows.append({
                    "Worker": wname,
                    "Port": wdata["port"],
                    "Received": m.get("total_received", 0),
                    "On-Time": m.get("on_time", 0),
                    "Late": m.get("late_dropped", 0),
                    "Completeness %": round(m.get("data_completeness_pct", 0), 2),
                    "DLQ": m.get("dlq_backlog", 0),
                    "Sketch N": m.get("sketch_total_count", 0),
                    "BP Drops": m.get("backpressure_drops", 0),
                    "Proc p95 us": m.get("proc_latency_p95_us", _avg_part("proc_latency_p95_us")),
                    "Proc p99 us": m.get("proc_latency_p99_us", _avg_part("proc_latency_p99_us")),
                })
            elif "partitions" in m and "node_id" in m:
                t_recv = sum(p.get("total_received", 0) for p in m["partitions"].values() if isinstance(p, dict))
                t_on = sum(p.get("on_time", 0) for p in m["partitions"].values() if isinstance(p, dict))
                t_late = sum(p.get("late_dropped", 0) for p in m["partitions"].values() if isinstance(p, dict))
                uniq = max(t_recv, 1)
                rows.append({
                    "Worker": wname,
                    "Port": wdata["port"],
                    "Received": t_recv,
                    "On-Time": t_on,
                    "Late": t_late,
                    "Completeness %": round(100.0 * t_on / uniq, 2),
                    "DLQ": m.get("dlq_backlog", 0),
                    "Sketch N": m.get("sketch_total_count", 0),
                    "BP Drops": 0,
                    "Proc p95 us": _avg_part("proc_latency_p95_us"),
                    "Proc p99 us": _avg_part("proc_latency_p99_us"),
                })
        if rows:
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        else:
            st.info("Waiting for worker data...")

    # Trend charts
    history = st.session_state.metrics_history
    if len(history) >= 2:
        st.markdown("---")
        st.markdown("#### Trends")
        df = pd.DataFrame(history)
        df["time_s"] = df["elapsed_s"].round(0)
        df = df.groupby("time_s").mean().reset_index()

        ch1, ch2 = st.columns(2)
        with ch1:
            st.markdown("**Completeness %**")
            st.line_chart(df.set_index("time_s")["completeness"], height=220)
        with ch2:
            st.markdown("**Total Events Processed**")
            st.line_chart(df.set_index("time_s")["total_received"], height=220)

        ch3, ch4 = st.columns(2)
        with ch3:
            st.markdown("**Late Dropped (cumulative)**")
            st.line_chart(df.set_index("time_s")["late_dropped"], height=220)
        with ch4:
            st.markdown("**Late Arrival Rate %**")
            st.line_chart(df.set_index("time_s")["late_rate"], height=220)

        lat_cols = [c for c in ("proc_p95_us", "proc_p99_us") if c in df.columns and df[c].max() > 0]
        lag_cols = [c for c in ("wm_lag_max_s", "event_lag_p95_ms") if c in df.columns and df[c].max() > 0]
        if lat_cols or lag_cols:
            ch5, ch6 = st.columns(2)
            if lat_cols:
                with ch5:
                    st.markdown("**Processing Latency (us)**")
                    st.line_chart(df.set_index("time_s")[lat_cols], height=220)
            if lag_cols:
                with ch6:
                    st.markdown("**Watermark / Event Lag**")
                    st.line_chart(df.set_index("time_s")[lag_cols], height=220)

    # Per-partition details (expandable)
    with st.expander("Per-Partition Details", expanded=False):
        part_rows = []
        for wname, wdata in metrics.get("workers", {}).items():
            m = wdata.get("metrics") or {}
            partitions = m.get("partitions", {})
            for pid, pdata in partitions.items():
                if not isinstance(pdata, dict):
                    continue
                row = {"Worker": wname, "Partition": str(pid)}
                if mode == "strict":
                    row["Watermark"] = format_timestamp(pdata.get("watermark", 0))
                    row["Local WM"] = format_timestamp(pdata.get("local_watermark", 0))
                    row["Open Win"] = pdata.get("open_windows", 0)
                    row["Closed Win"] = pdata.get("closed_windows", 0)
                    row["Completeness %"] = round(pdata.get("data_completeness_pct", 0), 2)
                    row["Late"] = pdata.get("late_dropped", 0)
                    row["Dupes"] = pdata.get("duplicates_filtered", 0)
                    row["BP Drops"] = pdata.get("backpressure_drops", 0)
                    row["Poll Decode p95 us"] = pdata.get("poll_decode_latency_p95_us", 0)
                    row["Dedup p95 us"] = pdata.get("dedup_latency_p95_us", 0)
                    row["State Write p95 us"] = pdata.get("state_write_latency_p95_us", 0)
                    row["p50 (µs)"] = pdata.get("proc_latency_p50_us", 0)
                    row["p99 (µs)"] = pdata.get("proc_latency_p99_us", 0)
                else:
                    wh = pdata.get("watermark", pdata.get("W_h", float("-inf")))
                    row["W_h"] = format_timestamp(wh) if wh != float("-inf") else "—"
                    row["L_eff (s)"] = round(pdata.get("L_eff_s", 0), 3)
                    row["p"] = pdata.get("p_current", 0)
                    row["Sketch N"] = pdata.get("sketch_total_count", 0)
                    row["Completeness %"] = round(pdata.get("data_completeness_pct", 0), 2)
                    row["On-Time"] = pdata.get("on_time", 0)
                    row["Late"] = pdata.get("late_dropped", 0)
                    row["DLQ"] = pdata.get("dlq_backlog", 0)
                    row["Extreme"] = pdata.get("extreme_lag_count", 0)
                    row["Lag p50 (ms)"] = round(pdata.get("sketch_quantile_p50_ms", 0), 2)
                    row["Lag p99 (ms)"] = round(pdata.get("sketch_quantile_p99_ms", 0), 2)
                    row["Proc p50 (µs)"] = pdata.get("proc_latency_p50_us", 0)
                    row["Proc p99 (µs)"] = pdata.get("proc_latency_p99_us", 0)
                    row["Sketch Update p95 us"] = pdata.get("sketch_update_latency_p95_us", 0)
                    row["Sketch Query p95 us"] = pdata.get("sketch_query_latency_p95_us", 0)
                    cs = pdata.get("cold_start", {})
                    row["Cold Phase"] = cs.get("phase", "?") if cs else "?"
                    nl = pdata.get("negative_lag", {})
                    row["NegLag Tier"] = nl.get("tier", "?") if nl else "?"
                part_rows.append(row)
        if part_rows:
            st.dataframe(pd.DataFrame(part_rows), use_container_width=True, hide_index=True)
        else:
            st.info("No partition data yet.")

    # Per-window loss accounting (heuristic only)
    if mode == "heuristic":
        loss_entries = _collect_per_window_loss(metrics)
        if loss_entries:
            with st.expander("Per-Window Loss Accounting", expanded=False):
                st.caption("Top windows by loss rate from each partition + overall loss rate.")
                st.dataframe(pd.DataFrame(loss_entries), use_container_width=True, hide_index=True)

    # Coordinator per-partition watermarks (strict mode)
    if mode == "strict":
        coord = metrics.get("coordinator", {})
        partition_types = coord.get("partition_types", {})
        recovery_info = coord.get("recovery_info", {})
        if partition_types or recovery_info:
            with st.expander("Coordinator Partition Map", expanded=False):
                coord_rows = []
                for pid_str, ptype in sorted(partition_types.items(), key=lambda x: int(x[0])):
                    pid = int(pid_str)
                    row = {"Partition": pid, "Type": ptype}
                    if pid_str in recovery_info:
                        ri = recovery_info[pid_str]
                        row["Original Owner"] = ri.get("original_owner", "?")
                        row["Current Owner"] = ri.get("current_owner", "?")
                        row["Reassigned At"] = ri.get("reassigned_at", "?")
                    coord_rows.append(row)
                if coord_rows:
                    st.dataframe(pd.DataFrame(coord_rows), use_container_width=True, hide_index=True)
                else:
                    st.caption("All partitions normal — no reassignments.")


# ---------------------------------------------------------------------------
# UI — Simulation Statistics
# ---------------------------------------------------------------------------

def render_sim_stats(mode: str, metrics: dict):
    st.markdown("### Simulation Statistics")
    st.markdown("Detailed dataset replay progress, partition metrics, and fault-tolerance status.")

    parts = _collect_partition_data(metrics)
    if not parts:
        st.info("Waiting for partition data...")
        return

    total_recv = sum(p["received"] for p in parts)
    total_on = sum(p["on_time"] for p in parts)
    total_late = sum(p["late"] for p in parts)
    total_dupes = sum(p["dupes"] for p in parts)
    total_bp = sum(p["bp_drops"] for p in parts)
    total_dlq = sum(p["dlq"] for p in parts)
    total_non_mono = sum(p["non_monotonic_punctuation"] for p in parts)
    total_fencing = sum(p["fencing_violations"] for p in parts)

    # Dataset progress & ETA
    dataset_file = st.session_state.get("dataset_file", "data.csv")
    total_rows = 0
    datasets = get_datasets()
    ds_path = datasets.get(dataset_file, "")
    if ds_path:
        total_rows = count_csv_rows(ds_path)

    # Event rate calculation
    if st.session_state.start_time and total_recv > 0:
        elapsed = time.time() - st.session_state.start_time
        rate = total_recv / max(elapsed, 1)
        m_elapsed, s_elapsed = divmod(int(elapsed), 60)
        elapsed_str = f"{m_elapsed}m {s_elapsed}s"
    else:
        rate = 0
        elapsed_str = "0s"

    # Row 1: Replay Progress KPIs
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Elapsed Time", elapsed_str)
    c2.metric("Event Rate", f"{rate:.1f} ev/s")
    c3.metric("Total Processed", f"{total_recv:,}")
    if total_rows > 0:
        pct = 100.0 * total_recv / total_rows
        c4.metric("Dataset Progress", f"{pct:.1f}%")
        if rate > 0:
            remaining = (total_rows - total_recv) / rate
            m, s_ = divmod(int(remaining), 60)
            c5.metric("ETA", f"{m}m {s_}s")
        else:
            c5.metric("ETA", "—")
    else:
        c4.metric("Dataset Progress", "?")
        c5.metric("ETA", "?")

    # Row 2: Quality & Compliance KPIs
    st.markdown("#### Quality & Compliance KPIs")
    q1, q2, q3, q4, q5, q6 = st.columns(6)

    unique_events = max(total_recv - total_dupes, 1)
    completeness_pct = 100.0 * total_on / unique_events
    late_rate_pct = 100.0 * total_late / max(total_recv, 1)

    q1.metric("Completeness %", f"{completeness_pct:.2f}%")
    q2.metric("Late Arrival %", f"{late_rate_pct:.2f}%")
    q3.metric("Duplicates", f"{total_dupes:,}")
    q4.metric("Backpressure Drops", f"{total_bp:,}")
    q5.metric("DLQ Backlog", f"{total_dlq:,}")

    if mode == "strict":
        q6.metric("Fencing Violations", f"{total_fencing:,}")
    else:
        q6.metric("Non-Mono Punctuation", f"{total_non_mono:,}")

    st.markdown("---")

    # Interactive tabs inside stats
    stats_tab1, stats_tab2, stats_tab3 = st.tabs(["Partition Metrics", "DDSketch & Latency (Heuristic)", "Load Balance & Skew"])

    with stats_tab1:
        st.markdown("#### Per-Partition Detailed Statistics")
        tbl = []
        for p in sorted(parts, key=lambda x: int(x["partition"])):
            w_val = p["W_h"]
            if isinstance(w_val, (int, float)) and w_val != float("-inf"):
                try:
                    w_str = datetime.fromtimestamp(w_val).strftime("%Y-%m-%d %H:%M:%S")
                except (OSError, ValueError, OverflowError):
                    w_str = f"{w_val:.1f}"
            else:
                w_str = "—"
            row = {
                "Partition": p["partition"],
                "Worker": p["worker"],
                "Received": p["received"],
                "On-Time": p["on_time"],
                "Late": p["late"],
                "Completeness %": p["completeness"],
                "Duplicates": p["dupes"],
                "BP Drops": p["bp_drops"],
                "DLQ": p["dlq"],
                "Watermark": w_str,
                "Open Windows": p["open_windows"],
                "Closed Windows": p["closed_windows"],
            }
            if mode == "strict":
                row["Dedup TTL"] = p["dedup_ttl"]
                row["Fencing Violations"] = p["fencing_violations"]
            else:
                row["Replay Mode"] = "Active" if p["replay"] else "Normal"
            tbl.append(row)
        st.dataframe(pd.DataFrame(tbl), use_container_width=True, hide_index=True)

    with stats_tab2:
        if mode == "heuristic":
            st.markdown("#### Heuristic Latency & Profiling")
            ds_tbl = []
            for p in sorted(parts, key=lambda x: int(x["partition"])):
                ds_tbl.append({
                    "Partition": p["partition"],
                    "Worker": p["worker"],
                    "Effective Lag (s)": p["L_eff_s"],
                    "Percentile (p)": p["p_current"],
                    "Lag p50 (ms)": round(p["sketch_p50"], 1),
                    "Lag p99 (ms)": round(p["sketch_p99"], 1),
                    "Proc p50 (\u03bcs)": round(p["proc_p50"], 1),
                    "Proc p99 (\u03bcs)": round(p["proc_p99"], 1),
                    "Sketch Update p95 (us)": round(p["sketch_update_p95"], 1),
                    "Sketch Query p95 (us)": round(p["sketch_query_p95"], 1),
                    "Negative Lag Rate": f"{p['neg_lag_rate'] * 100:.3f}%",
                    "Estimator Drift": p["est_drift"],
                })
            st.dataframe(pd.DataFrame(ds_tbl), use_container_width=True, hide_index=True)

            # Show summary metrics
            p50s = [p["sketch_p50"] for p in parts if p["sketch_p50"] > 0]
            p95s = [p["sketch_p95"] for p in parts if p["sketch_p95"] > 0]
            proc_p50s = [p["proc_p50"] for p in parts if p["proc_p50"] > 0]

            col_l1, col_l2, col_l3 = st.columns(3)
            col_l1.metric("Max Lag p95", f"{max(p95s):.1f} ms" if p95s else "—")
            col_l2.metric("Avg Lag p50", f"{sum(p50s)/len(p50s):.1f} ms" if p50s else "—")
            col_l3.metric("Avg Proc p50", f"{sum(proc_p50s)/len(proc_p50s):.1f} \u03bcs" if proc_p50s else "—")
        else:
            st.markdown("#### Strict Profiling & Event Processing Latency")
            strict_tbl = []
            for p in sorted(parts, key=lambda x: int(x["partition"])):
                strict_tbl.append({
                    "Partition": p["partition"],
                    "Worker": p["worker"],
                    "Proc p50 (\u03bcs)": round(p["proc_p50"], 1),
                    "Proc p95 (\u03bcs)": round(p["proc_p95"], 1),
                    "Proc p99 (\u03bcs)": round(p["proc_p99"], 1),
                    "Poll Decode p95 (us)": round(p["poll_decode_p95"], 1),
                    "Dedup p95 (us)": round(p["dedup_p95"], 1),
                    "State Write p95 (us)": round(p["state_write_p95"], 1),
                    "Dedup TTL Entries": p["dedup_ttl"],
                    "Fencing Violations": p["fencing_violations"],
                })
            st.dataframe(pd.DataFrame(strict_tbl), use_container_width=True, hide_index=True)

            proc_p50s = [p["proc_p50"] for p in parts if p["proc_p50"] > 0]
            proc_p99s = [p["proc_p99"] for p in parts if p["proc_p99"] > 0]
            fencing_viols = [p["fencing_violations"] for p in parts]

            col_l1, col_l2, col_l3 = st.columns(3)
            col_l1.metric("Avg Proc p50", f"{sum(proc_p50s)/len(proc_p50s):.1f} \u03bcs" if proc_p50s else "—")
            col_l2.metric("Max Proc p99", f"{max(proc_p99s):.1f} \u03bcs" if proc_p99s else "—")
            col_l3.metric("Total Fencing Violations", f"{sum(fencing_viols)}" if fencing_viols else "0")

    with stats_tab3:
        st.markdown("#### Event Load Balance & Skew")
        col_chart, col_skew = st.columns([3, 2])
        with col_chart:
            chart_data = pd.DataFrame([
                {"Partition": p["partition"], "On-Time": p["on_time"], "Late": p["late"]}
                for p in parts
            ])
            st.bar_chart(chart_data.set_index("Partition")[["On-Time", "Late"]], height=300)

        with col_skew:
            recv_vals = [p["received"] for p in parts]
            if recv_vals and max(recv_vals) > 0:
                skew_ratio = max(recv_vals) / max(sum(recv_vals) / len(recv_vals), 1)
                st.metric("Max/Avg Load Ratio", f"{skew_ratio:.2f}x")
                st.markdown(f"""
                - **Min events per partition:** {min(recv_vals):,}
                - **Max events per partition:** {max(recv_vals):,}
                - **Standard Deviation:** {pd.Series(recv_vals).std():.1f}
                """)

                # Chunk boundary effects
                parts_sorted = sorted(parts, key=lambda x: int(x["partition"]))
                min_recv_parts = [p for p in parts_sorted if p["received"] == min(recv_vals)]
                max_recv_parts = [p for p in parts_sorted if p["received"] == max(recv_vals)]
                st.caption(f"Least-loaded partitions: {', '.join(p['partition'] for p in min_recv_parts)} ({min(recv_vals):,} events)")
                st.caption(f"Most-loaded partitions: {', '.join(p['partition'] for p in max_recv_parts)} ({max(recv_vals):,} events)")
                spread = (max(recv_vals) - min(recv_vals)) / max(recv_vals) * 100
                st.caption(f"Load Spread: {spread:.1f}% (caused by hash rounding at chunk boundaries)")


# ---------------------------------------------------------------------------
# UI — Logs
# ---------------------------------------------------------------------------

def render_logs():
    st.markdown("### Docker Logs")

    is_sim = _is_sim()
    if is_sim:
        services = [
            "(all)", "strict-coordinator", "strict-worker", "strict-ingestor",
            "heuristic-aggregator", "heuristic-worker", "heuristic-ingestor",
        ]
    else:
        services = [
            "(all)", "coordinator-1", "coordinator-2", "coordinator-3",
            "aggregator", "aggregator-standby",
            "node0", "node1", "node2", "node3",
            "ingestor", "kafka", "zookeeper", "prometheus", "grafana", "minio",
        ]

    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
    with col1:
        service = st.selectbox("Service", services)
    with col2:
        tail = st.selectbox("Lines", [50, 100, 200, 500, 1000], index=1)
    with col3:
        st.session_state.log_filter = st.text_input(
            "Filter", value=st.session_state.log_filter,
            placeholder="e.g. completeness, late, ingest",
            help="Filter log lines containing this text (case-insensitive)",
        )
    with col4:
        st.session_state.log_auto_scroll = st.checkbox(
            "Auto-refresh", value=st.session_state.log_auto_scroll,
        )
        if st.button("Refresh Now", width="stretch"):
            st.rerun()

    if st.session_state.running:
        svc = "" if service == "(all)" else service
        logs = compose_logs(service=svc, tail=tail)
        if st.session_state.log_filter:
            filt = st.session_state.log_filter.lower()
            lines = logs.split("\n")
            logs = "\n".join(line for line in lines if filt in line.lower())
        if logs.strip():
            st.code(logs, language="log")
        else:
            st.info("No logs yet — containers may still be starting, or filter excludes everything.")
    else:
        st.info("Start the system to see logs.")


# ---------------------------------------------------------------------------
# UI — Compare
# ---------------------------------------------------------------------------

def render_comparison():
    st.markdown("### Mode Comparison")
    results = st.session_state.run_results

    if not results:
        st.info("Run at least one mode to see results here. Run both to compare.")
        return

    cols = st.columns(max(len(results), 2))
    for i, (mode, data) in enumerate(results.items()):
        a = data["agg"]
        dur = data.get("duration_s", 0)
        dm = data.get("deploy_mode", "full")
        with cols[i]:
            label = f"{'Sim' if dm == 'sim' else 'Full'} — {'Strict' if mode == 'strict' else 'Heuristic'}"
            st.markdown(f"#### {label}")
            st.caption(f"Run: {data.get('timestamp', '?')} | Duration: {int(dur)}s")
            st.metric("Completeness", f"{a['data_completeness_pct']:.2f}%")
            st.metric("Total Events", f"{a['total_received']:,}")
            st.metric("On-Time", f"{a['on_time']:,}")
            st.metric("Late Dropped", f"{a['late_dropped']:,}")
            st.metric("Late Rate", f"{a['late_arrival_rate_pct']:.2f}%")
            st.metric("Duplicates", f"{a['duplicates']:,}")
            st.metric("BP Drops", f"{a['backpressure_drops']:,}")

    if len(results) == 2 and "strict" in results and "heuristic" in results:
        st.markdown("---")
        st.markdown("#### Side-by-Side")
        sa = results["strict"]["agg"]
        ha = results["heuristic"]["agg"]
        df = pd.DataFrame({
            "Metric": ["Completeness %", "Late Rate %", "Events Received",
                       "On-Time", "Late Dropped", "Duplicates", "BP Drops",
                       "Non-Mono Punctuation"],
            "Strict": [sa["data_completeness_pct"], sa["late_arrival_rate_pct"],
                       sa["total_received"], sa["on_time"], sa["late_dropped"],
                       sa["duplicates"], sa["backpressure_drops"],
                       sa.get("non_monotonic_punctuation", 0)],
            "Heuristic": [ha["data_completeness_pct"], ha["late_arrival_rate_pct"],
                          ha["total_received"], ha["on_time"], ha["late_dropped"],
                          ha["duplicates"], ha["backpressure_drops"],
                          ha.get("non_monotonic_punctuation", 0)],
        })
        st.dataframe(df, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# UI — Raw JSON
# ---------------------------------------------------------------------------

def render_raw(metrics: dict):
    st.markdown("### Raw Metrics")
    if not metrics or not metrics.get("workers"):
        st.info("No metrics available.")
        return
    for section in ["coordinator", "aggregator"]:
        if section in metrics:
            with st.expander(section.title(), expanded=False):
                st.json(metrics[section])
    for wname, wdata in metrics.get("workers", {}).items():
        with st.expander(f"Worker: {wname}", expanded=False):
            if wdata.get("metrics"):
                st.json(wdata["metrics"])
            if wdata.get("state"):
                st.json(wdata["state"])


# ---------------------------------------------------------------------------
# Node Control & Fault Injection
# ---------------------------------------------------------------------------

def fetch_all_container_statuses(mode: str) -> dict[str, str]:
    """Fetch the status of all containers in a single docker compose ps call.
    Returns a dict mapping docker service name to status ('running', 'stopped', 'unknown').
    """
    statuses = {}
    cmd = _compose_base() + [
        "--profile", "strict",
        "--profile", "heuristic",
        "--profile", "hybrid",
        "ps", "--format", "json"
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8",
                           errors="replace", cwd=str(DEPLOY_DIR), timeout=8)
        if r.returncode == 0 and r.stdout.strip():
            lines = r.stdout.strip().split("\n")
            for line in lines:
                try:
                    data = json.loads(line)
                    service = data.get("Service")
                    if not service:
                        name = data.get("Name", "")
                        if name.startswith("refactor-"):
                            service = name[len("refactor-"):]
                    if service:
                        state = data.get("State", data.get("Status", "")).lower()
                        if "up" in state or "running" in state:
                            statuses[service] = "running"
                        elif "exit" in state or "stop" in state:
                            statuses[service] = "stopped"
                        else:
                            statuses[service] = "unknown"
                except Exception:
                    pass
    except Exception:
        pass
    return statuses


def get_container_status_docker(service: str) -> str:
    """Check the container status using cached status map or fallback to docker compose ps."""
    cache = st.session_state.get("container_statuses")
    if cache is not None and service in cache:
        return cache[service]

    cmd = _compose_base() + ["ps", "--format", "json", service]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8",
                           errors="replace", cwd=str(DEPLOY_DIR), timeout=5)
        if r.returncode == 0 and r.stdout.strip():
            lines = r.stdout.strip().split("\n")
            for line in lines:
                try:
                    data = json.loads(line)
                    state = data.get("State", data.get("Status", "")).lower()
                    if "up" in state or "running" in state:
                        return "running"
                    if "exit" in state or "stop" in state:
                        return "stopped"
                except Exception:
                    pass
    except Exception:
        pass
    return "unknown"


def check_node_status(service: str, port: int = None) -> str:
    """Combined health status using Docker state lookup and network check fallback."""
    # 1. Check Docker state first (fast cache lookup)
    docker_status = get_container_status_docker(service)
    if docker_status == "stopped":
        return "stopped"

    # 2. If it is running in Docker, verify with network health check
    infra_key = _infra_key(service)
    if infra_key is not None:
        if check_infra_health(infra_key):
            return "running"
        return docker_status if docker_status != "unknown" else "stopped"

    if port:
        if is_healthy(port):
            return "running"

    return docker_status if docker_status != "unknown" else "stopped"


def _container_id(service: str) -> str:
    """Return the container id for a compose service, or '' if not found."""
    cmd = _compose_base() + ["ps", "-aq", service]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8",
                           errors="replace", cwd=str(DEPLOY_DIR), timeout=5)
        if r.returncode == 0:
            return r.stdout.strip().split("\n")[0].strip()
    except Exception:
        pass
    return ""


def _set_restart_policy(service: str, policy: str) -> None:
    """Override a container's restart policy (e.g. 'no' or 'unless-stopped').

    Required for fault injection: the compose services declare
    `restart: unless-stopped`, so a plain `docker kill` would be auto-revived by
    the daemon within ~1s. Setting the policy to 'no' before killing keeps the
    node down until it is manually restarted.
    """
    cid = _container_id(service)
    if not cid:
        return
    try:
        subprocess.run(["docker", "update", "--restart", policy, cid],
                       capture_output=True, text=True, encoding="utf-8",
                       errors="replace", timeout=15)
    except Exception:
        pass


def compose_start_service(service: str) -> tuple[bool, str]:
    cmd = _compose_base() + ["start", service]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8",
                           errors="replace", cwd=str(DEPLOY_DIR), timeout=30)
        # Restore auto-restart so the node behaves normally after a manual restart.
        if r.returncode == 0:
            _set_restart_policy(service, "unless-stopped")
        return r.returncode == 0, r.stdout + r.stderr
    except Exception as e:
        return False, str(e)


def compose_stop_service(service: str) -> tuple[bool, str]:
    cmd = _compose_base() + ["stop", service]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8",
                           errors="replace", cwd=str(DEPLOY_DIR), timeout=30)
        return r.returncode == 0, r.stdout + r.stderr
    except Exception as e:
        return False, str(e)


def compose_kill_service(service: str) -> tuple[bool, str]:
    # Disable auto-restart first; otherwise `restart: unless-stopped` makes the
    # daemon immediately revive the killed container, defeating fault injection.
    _set_restart_policy(service, "no")
    cmd = _compose_base() + ["kill", service]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8",
                           errors="replace", cwd=str(DEPLOY_DIR), timeout=30)
        return r.returncode == 0, r.stdout + r.stderr
    except Exception as e:
        return False, str(e)


def _node_control_groups(mode: str) -> list[tuple[str, list[dict]]]:
    """Single source of truth for the controllable services in the current
    deploy/watermark mode. Returns [(group_header, [node, ...]), ...] where each
    node is {service, display, port, role}."""
    def n(service, display, port, role):
        return {"service": service, "display": display, "port": port, "role": role}

    if _is_sim():
        if mode == "strict":
            return [("Simulation Cluster", [
                n("strict-coordinator", "Strict Coordinator", 9000, "Coordinator / Raft Leader"),
                n("strict-worker", "Strict Worker", 9101, "Worker / Partitions 0..11"),
                n("strict-ingestor", "Strict Ingestor", 9200, "Ingestor"),
            ])]
        return [("Simulation Cluster", [
            n("heuristic-aggregator", "Heuristic Aggregator", 9017, "Aggregator"),
            n("heuristic-worker", "Heuristic Worker", 9111, "Worker / Partitions 0..11"),
            n("heuristic-ingestor", "Heuristic Ingestor", 9210, "Ingestor"),
        ])]

    if mode == "strict":
        core = ("Core Cluster", [
            n("coordinator-1", "Coordinator 1", 9000, "Coordinator Leader / Term"),
            n("coordinator-2", "Coordinator 2", 9003, "Coordinator Peer"),
            n("coordinator-3", "Coordinator 3", 9004, "Coordinator Peer"),
        ])
    else:
        core = ("Core Cluster", [
            n("aggregator", "Aggregator Primary", 9007, "Aggregator Leader"),
            n("aggregator-standby", "Aggregator Standby", 9005, "Aggregator Standby"),
        ])
    return [
        core,
        ("Workers", [
            n("node0", "Node 0 (Worker)", 9101, "Worker (Partitions 0,1,2)"),
            n("node1", "Node 1 (Worker)", 9102, "Worker (Partitions 3,4,5)"),
            n("node2", "Node 2 (Worker)", 9103, "Worker (Partitions 6,7,8)"),
            n("node3", "Node 3 (Worker)", 9104, "Worker (Partitions 9,10,11)"),
        ]),
        ("Ingestion Layer", [
            n("ingestor", "Ingestor Service", None, "Dataset Ingestion"),
        ]),
        ("Infrastructure", [
            n("kafka", "Kafka Broker", 29092, "Message Queue"),
            n("zookeeper", "ZooKeeper", 2181, "Coordination Ensemble"),
            n("prometheus", "Prometheus", 9090, "Metrics Server"),
            n("grafana", "Grafana", 3000, "Visualization Server"),
            n("minio", "MinIO Object Storage", 9002, "Tiered Storage Store"),
        ]),
    ]


# action -> (compose fn, gerund label, past-tense label, icon)
_NODE_ACTIONS = {
    "kill": (compose_kill_service, "Killing", "Killed", "💥"),
    "start": (compose_start_service, "Starting", "Started", "🟢"),
    "stop": (compose_stop_service, "Stopping", "Stopped", "🔴"),
}


def _exec_node_action(action: str, service: str, display: str):
    """Run a kill/start/stop against a service and refresh the page on success."""
    print(f"[dashboard] EXECUTING NODE ACTION: action={action} service={service} display={display}", flush=True)
    fn, gerund, past, icon = _NODE_ACTIONS[action]
    with st.spinner(f"{gerund} {display}..."):
        ok, err = fn(service)
    if ok:
        print(f"[dashboard] NODE ACTION SUCCESS: {past} {display}!", flush=True)
        st.toast(f"{past} {display}!", icon=icon)
        time.sleep(1)
        st.rerun()
    else:
        print(f"[dashboard] NODE ACTION FAILED: {action} {display}! Error: {err}", flush=True)
        st.error(f"Failed to {action} {display}: {err}")


def render_quick_fault_injection(mode: str):
    """Prominent one-click Kill / Recover panel for the selected node."""
    flat = [node for _, nodes in _node_control_groups(mode) for node in nodes]
    labels = [f"{x['display']}  ·  {x['role']}" for x in flat]

    st.markdown("#### ⚡ Quick Fault Injection")
    sel = st.selectbox("Target node", labels, key="nc_quick_sel")
    target = flat[labels.index(sel)]
    status = check_node_status(target["service"], target["port"])

    badge = "🟢 :green[Running]" if status == "running" else (
        "🔴 :red[Stopped]" if status == "stopped" else "⚪ :gray[Unknown]")
    st.markdown(f"**{target['display']}** — {badge}"
                + (f"  (:{target['port']})" if target["port"] else ""))

    b_kill, b_recover, b_refresh = st.columns(3)
    if b_kill.button("💥 Kill node", key="nc_quick_kill", type="primary",
                     disabled=(status != "running"), width="stretch"):
        _exec_node_action("kill", target["service"], target["display"])
    if b_recover.button("♻️ Recover node", key="nc_quick_recover",
                        disabled=(status == "running"), width="stretch"):
        _exec_node_action("start", target["service"], target["display"])
    if b_refresh.button("🔄 Refresh status", key="nc_quick_refresh", width="stretch"):
        st.rerun()

    st.caption("Kill a worker → watch the Dashboard tab react → Recover it. "
               "Killed nodes stay down (restart policy disabled) until you recover them.")


def render_node_control_row(service_name: str, display_name: str, port: int = None, role: str = ""):
    # Only probe Docker for live status when the cluster is up; otherwise skip
    # the (slow) subprocess calls and show the controls in a disabled preview.
    controls_enabled = bool(st.session_state.get("running", False))
    status = check_node_status(service_name, port) if controls_enabled else "unknown"

    col1, col2, col3, col4, col5, col6 = st.columns([2, 1.5, 2.5, 1, 1, 1])
    col1.markdown(f"**{display_name}**")

    if status == "running":
        col2.markdown("🟢 :green[Running]")
    elif status == "stopped":
        col2.markdown("🔴 :red[Stopped]")
    else:
        col2.markdown("⚪ :gray[—]")

    col3.caption(f"{role} (:{port})" if port else role)

    start_btn = col4.button("Start", key=f"start_{service_name}", disabled=(not controls_enabled) or (status == "running"), width="stretch")
    stop_btn = col5.button("Stop", key=f"stop_{service_name}", disabled=(not controls_enabled) or (status == "stopped"), width="stretch")
    kill_btn = col6.button("Kill", key=f"kill_{service_name}", disabled=(not controls_enabled) or (status == "stopped"), type="secondary", width="stretch")

    if start_btn:
        _exec_node_action("start", service_name, display_name)
    if stop_btn:
        _exec_node_action("stop", service_name, display_name)
    if kill_btn:
        _exec_node_action("kill", service_name, display_name)


def render_failover_events_section():
    metrics = st.session_state.get("last_metrics") or {}
    coord = metrics.get("coordinator", {})
    failover = coord.get("failover", {})
    events = failover.get("events", [])
    
    st.markdown("#### 📜 Recent Coordination & Failover Events")
    if not events:
        st.info("No failover events recorded yet.")
        return
        
    event_rows = []
    for ev in reversed(events):
        t_str = datetime.fromtimestamp(ev["time"]).strftime("%H:%M:%S")
        parts_str = ", ".join(str(p) for p in ev["partitions"]) if ev["partitions"] else "—"
        event_rows.append({
            "Time": t_str,
            "Event": ev["event"].upper(),
            "Worker": ev["worker"],
            "Partitions": parts_str,
            "Details": ev["details"],
        })
    st.dataframe(pd.DataFrame(event_rows), use_container_width=True, hide_index=True)


def render_node_control(mode: str):
    st.markdown("### Node Control / Fault Injection")
    st.markdown("Abruptly kill, gracefully stop, or start individual system services to verify fault-tolerance.")

    if not st.session_state.running:
        st.warning("⚠️ Press **Start** in the sidebar to launch the cluster first. "
                   "The Kill / Recover buttons activate once containers are running.")
    else:
        render_quick_fault_injection(mode)
        st.divider()
        render_failover_events_section()
        st.divider()
        st.caption("Status is read on demand (this tab does not auto-refresh, so buttons stay stable).")

    with st.expander("All services (advanced per-node controls)", expanded=not st.session_state.running):
        for header, nodes in _node_control_groups(mode):
            st.markdown(f"#### {header}")
            for nd in nodes:
                render_node_control_row(nd["service"], nd["display"], nd["port"], nd["role"])
            st.markdown("---")


# -----------------------------------------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------------------------------------

def main():
    st.set_page_config(
        page_title="CSDLPT Watermark System",
        page_icon="🌊",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.markdown("""<style>
    .block-container { padding-top: 1rem; }
    /* Metric cards — subtle border instead of filled background for theme compatibility */
    [data-testid="stMetric"] {
        border: 1px solid rgba(128, 128, 128, 0.3);
        border-radius: 8px; padding: 10px;
    }
    /* Table text legibility */
    .stDataFrame { font-size: 0.9rem; }
    /* Metric value emphasis */
    [data-testid="stMetricValue"] { font-weight: 600 !important; }
    </style>""", unsafe_allow_html=True)

    init_state()
    if st.session_state.running and not st.session_state.container_statuses:
        st.session_state.container_statuses = fetch_all_container_statuses(st.session_state.mode)
    render_sidebar()

    mode = st.session_state.mode
    is_sim = _is_sim()

    # No page-level auto-refresh. Each live-metrics tab is an isolated fragment
    # with its own "Refresh data" button, so a refresh re-renders ONLY that data
    # block — never the sidebar, tabs, or control panels.
    def _render_refresh_bar(key: str = None):
        st.caption(f"🟢 **Real-time Auto-refresh (every 3s) active** · Last update: {datetime.now().strftime('%H:%M:%S')}")

    def _refresh_metrics() -> dict:
        """Fetch live metrics once and append a history sample. Throttled to max once per 2 seconds."""
        metrics = st.session_state.get("last_metrics") or {}
        if st.session_state.running:
            now = time.time()
            last_fetch = st.session_state.get("last_fetch_time", 0.0)
            if now - last_fetch >= 2.0:
                metrics = fetch_all_metrics(mode) or {}
                st.session_state.last_metrics = metrics
                st.session_state.last_fetch_time = now
                st.session_state.container_statuses = fetch_all_container_statuses(mode)
                
                # Coordination tracking: detect changes and log to console
                if metrics.get("coordinator"):
                    coord_data = metrics["coordinator"]
                    leader = metrics.get("coordinator_name")
                    term = coord_data.get("term", 0)
                    
                    # Check leader change
                    prev_leader = st.session_state.get("last_leader")
                    if prev_leader is not None and prev_leader != leader:
                        print(f"[dashboard] COORDINATOR LEADER CHANGE DETECTED: {prev_leader} -> {leader} (term={term})", flush=True)
                    st.session_state.last_leader = leader

                    # Check active workers change
                    active_workers = coord_data.get("active_workers", 0)
                    prev_active = st.session_state.get("last_active_workers")
                    if prev_active is not None and prev_active != active_workers:
                        print(f"[dashboard] ACTIVE WORKERS COUNT CHANGED: {prev_active} -> {active_workers}", flush=True)
                    st.session_state.last_active_workers = active_workers

                    # Check partition reassignments (rebalancing)
                    if "recovery_info" in coord_data:
                        recovery_info = coord_data["recovery_info"]
                        prev_recovery_info = st.session_state.get("last_recovery_info", {})
                        if recovery_info != prev_recovery_info:
                            for pid_str, info in recovery_info.items():
                                pid = int(pid_str)
                                prev_info = prev_recovery_info.get(pid_str)
                                if prev_info != info:
                                    print(f"[dashboard] COORDINATION PARTITION REASSIGNMENT: "
                                          f"partition={pid} original_owner={info.get('original_owner')} "
                                          f"current_owner={info.get('current_owner')} "
                                          f"reassigned_at={format_timestamp(info.get('reassigned_at'))}", flush=True)
                            st.session_state.last_recovery_info = recovery_info

                if metrics.get("workers"):
                    agg = aggregate_worker_metrics(metrics)
                    elapsed = time.time() - (st.session_state.start_time or time.time())
                    st.session_state.metrics_history.append({
                        "elapsed_s": elapsed,
                        "completeness": agg["data_completeness_pct"],
                        "total_received": agg["total_received"],
                        "on_time": agg["on_time"],
                        "late_dropped": agg["late_dropped"],
                        "late_rate": agg["late_arrival_rate_pct"],
                        "proc_p95_us": agg.get("proc_lat_p95_us", 0.0),
                        "proc_p99_us": agg.get("proc_lat_p99_us", 0.0),
                        "wm_lag_max_s": agg.get("wm_lag_max_s", 0.0),
                        "event_lag_p95_ms": agg.get("sketch_p95_ms", 0.0),
                    })
                    if len(st.session_state.metrics_history) > MAX_HISTORY:
                        st.session_state.metrics_history = st.session_state.metrics_history[-MAX_HISTORY:]
            else:
                # Use cached metrics if queried within the throttle window
                metrics = st.session_state.last_metrics or {}
        return metrics or {}

    # Build the tab bar once. Node Control sits right after Dashboard and carries
    # a distinct icon so the kill/recover controls are easy to find.
    TAB_DASH = "📊 Dashboard"
    TAB_NODE = "🛑 Node Control · Kill / Recover"
    TAB_SIM = "🧪 Sim Stats"
    TAB_LOGS = "📜 Logs"
    TAB_CMP = "⚖️ Compare"
    TAB_RAW = "🧩 Raw JSON"

    tab_names = [TAB_DASH, TAB_NODE]
    if is_sim:
        tab_names.append(TAB_SIM)
    tab_names.extend([TAB_LOGS, TAB_CMP, TAB_RAW])
    tabs = st.tabs(tab_names)
    tab_idx = {name: i for i, name in enumerate(tab_names)}

    # Tab: Dashboard — isolated fragment; auto-refreshes every 3 seconds when running.
    with tabs[tab_idx[TAB_DASH]]:
        @st.fragment(run_every=3.0 if st.session_state.running else None)
        def _dashboard_fragment():
            if st.session_state.running:
                st.info("🛑 To **kill / recover a node**, open the **Node Control · Kill / Recover** tab above.")
                _render_refresh_bar("dash_refresh")
            metrics = _refresh_metrics()
            if st.session_state.running and metrics.get("workers"):
                render_dashboard(mode, metrics)
            elif st.session_state.running:
                st.info("Waiting for services to start...")
                st.caption("This may take 30-60 seconds on first run.")
            else:
                st.markdown("### CSDLPT Watermark Processing System")

                if is_sim:
                    st.markdown(f"""
**Deploy Mode: Simulation** — HTTP-only, no Kafka/ZK/MinIO. Single worker handling all 12 partitions.

**How to use:**
1. Select **Deploy Mode** (`sim`) and **Watermark Mode** (`strict` or `heuristic`) in the sidebar
2. Choose a **dataset** CSV file
3. Click **Start** to build & launch the simulation containers
4. Watch live metrics on the Dashboard tab, dataset replay progress on Sim Stats
5. **Kill / recover any node** in the **🛑 Node Control · Kill / Recover** tab to test fault-tolerance
6. Click **Stop** when done — results are saved for comparison

**Modes:**
- **Strict**: Punctuation-based watermark via coordinator. 0% data loss, ~15s latency.
- **Heuristic**: DDSketch lag estimation via aggregator. <=1% loss, ~5s latency.

**Sim containers:** strict-coordinator / strict-worker / strict-ingestor or heuristic-aggregator / heuristic-worker / heuristic-ingestor
""")
                else:
                    st.markdown("""
**Deploy Mode: Full** — 3 Coordinators (HA) + 4 Workers (12 partitions) + Kafka + ZooKeeper + Prometheus + Grafana + MinIO

**How to use:**
1. Select **Deploy Mode** (`full`) and **Watermark Mode** (`strict` or `heuristic`) in the sidebar
2. Choose a **dataset** CSV file
3. Configure **Log Level** and **Punctuation Mode** (see below)
4. Click **Start** to build & launch all Docker containers
5. Watch aggregated metrics from all 4 workers (click **Refresh data** to update)
6. **Kill / recover any node** in the **🛑 Node Control · Kill / Recover** tab to test fault-tolerance
7. Click **Stop** when done — results are saved for comparison

**Pipeline Settings:**
- **Log Level**: `info` (5s summary) | `debug`/`trace` (per-event flow through each container)
- **Punctuation Mode**: `data-driven` (T_commit tracks CSV timestamps — 100% completeness) | `wall-clock` (real-time streaming mode)

**Modes:**
- **Strict**: Punctuation-based watermark via coordinator. 0% data loss guarantee, ~15s latency.
- **Heuristic**: DDSketch lag estimation via aggregator. <=1% loss target, ~5s latency.

**External dashboards (when running):**
- [Grafana](http://localhost:3000) — Full monitoring dashboard (admin/admin)
- [Prometheus](http://localhost:9090) — Raw metrics queries
- [MinIO Console](http://localhost:9001) — Tiered storage (minioadmin/minioadmin)
""")
        _dashboard_fragment()

    # Tab: Sim Stats — isolated fragment; auto-refreshes.
    if is_sim:
        with tabs[tab_idx[TAB_SIM]]:
            @st.fragment(run_every=3.0 if st.session_state.running else None)
            def _sim_fragment():
                if st.session_state.running:
                    _render_refresh_bar("sim_refresh")
                metrics = _refresh_metrics() if st.session_state.running else {}
                if st.session_state.running and metrics.get("workers"):
                    render_sim_stats(mode, metrics)
                elif st.session_state.running:
                    st.info("Waiting for partition data...")
                else:
                    st.info("Start the simulation to see replay statistics.")
            _sim_fragment()

    # Tab: Node Control — isolated fragment; auto-refreshes.
    with tabs[tab_idx[TAB_NODE]]:
        @st.fragment(run_every=3.0 if st.session_state.running else None)
        def _node_control_fragment():
            # Trigger a silent metrics fetch to update the events display
            _ = _refresh_metrics()
            render_node_control(mode)
        _node_control_fragment()

    # Tab: Logs — isolated fragment; auto-refreshes if checked.
    with tabs[tab_idx[TAB_LOGS]]:
        @st.fragment(run_every=3.0 if (st.session_state.running and st.session_state.get("log_auto_scroll", False)) else None)
        def _logs_fragment():
            render_logs()
        _logs_fragment()

    # Tab: Compare — static.
    with tabs[tab_idx[TAB_CMP]]:
        render_comparison()

    # Tab: Raw JSON — static snapshot of the last fetched metrics.
    with tabs[tab_idx[TAB_RAW]]:
        render_raw(st.session_state.get("last_metrics") or {})


if __name__ == "__main__":
    main()
