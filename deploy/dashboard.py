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
    "Kafka":      {"port": 9092, "health": None},
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
    return WORKERS_SIM if _is_sim() else WORKERS_FULL


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


def fetch_all_metrics(mode: str) -> dict:
    result = {"timestamp": time.time(), "mode": mode, "workers": {}}

    for name, info in _workers().items():
        port = info["port"] if isinstance(info, dict) else info
        metrics = _get_json(f"http://localhost:{port}/api/metrics")
        state = _get_json(f"http://localhost:{port}/state")
        if metrics or state:
            result["workers"][name] = {"metrics": metrics, "state": state, "port": port}

    # Try all coordinators, pick the one with valid W_global (leader)
    best_coord = None
    for cname, cport in _coordinators().items():
        coord = _get_json(f"http://localhost:{cport}/state", timeout=3.0)
        if coord is None:
            continue
        wg = coord.get("W_global")
        if wg is not None and wg != float("-inf") and wg != float("inf"):
            result["coordinator"] = coord
            result["coordinator_name"] = cname
            best_coord = coord
            break
        if best_coord is None:
            best_coord = coord
            result["coordinator"] = coord
            result["coordinator_name"] = cname
    # Fallback to any responding coordinator
    if best_coord is None:
        for cname, cport in _coordinators().items():
            coord = _get_json(f"http://localhost:{cport}/state", timeout=1.5)
            if coord:
                result["coordinator"] = coord
                result["coordinator_name"] = cname
                break

    if mode in ("heuristic", "hybrid"):
        agg = _get_json(f"http://localhost:{_aggregator_port()}/state")
        if agg:
            result["aggregator"] = agg

    return result


def aggregate_worker_metrics(metrics: dict) -> dict:
    """Aggregate metrics from all workers into a single summary."""
    total_recv = 0
    total_on_time = 0
    total_late = 0
    total_dupes = 0
    total_bp = 0
    total_non_mono = 0
    total_dlq = 0
    total_sketch = 0
    total_extreme_lag = 0

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

    unique = max(total_recv - total_dupes, 1)
    completeness = 100.0 * total_on_time / unique
    late_rate = 100.0 * total_late / max(total_recv, 1)

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
                "dupes": pdata.get("duplicates", 0),
                "bp_drops": pdata.get("backpressure_drops", 0),
                "cold_start": pdata.get("cold_start", {}),
                "negative_lag": pdata.get("negative_lag", {}),
                "extreme_lag": pdata.get("extreme_lag_count", 0),
                "replay": pdata.get("replay_mode", pdata.get("replay_mode_active", False)),
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
            wgh_str = datetime.fromtimestamp(wgh).strftime("%H:%M:%S")
        except (OSError, ValueError, OverflowError):
            wgh_str = f"{wgh:.1f}"
        c4.metric("W_global_h", wgh_str)
    else:
        c4.metric("W_global_h", "—")

    if wh_vals:
        c5.metric("Max W_h", f"{max(wh_vals):.1f}")
        c6.metric("Min W_h", f"{min(wh_vals):.1f}")
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
                          use_container_width=True, type="primary")
    with c2:
        stop = st.button("Stop", disabled=not st.session_state.running,
                         use_container_width=True)

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
    st.session_state.auto_refresh = st.sidebar.toggle(
        "Auto-refresh (3s)", value=st.session_state.auto_refresh,
    )

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
                    wm_str = datetime.fromtimestamp(wg).strftime("%H:%M:%S")
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
                    row["Watermark"] = round(pdata.get("watermark", 0), 2)
                    row["Local WM"] = round(pdata.get("local_watermark", 0), 2)
                    row["Open Win"] = pdata.get("open_windows", 0)
                    row["Closed Win"] = pdata.get("closed_windows", 0)
                    row["Completeness %"] = round(pdata.get("data_completeness_pct", 0), 2)
                    row["Late"] = pdata.get("late_dropped", 0)
                    row["Dupes"] = pdata.get("duplicates_filtered", 0)
                    row["BP Drops"] = pdata.get("backpressure_drops", 0)
                else:
                    wh = pdata.get("watermark", pdata.get("W_h", float("-inf")))
                    row["W_h"] = round(wh, 2) if wh != float("-inf") else "—"
                    row["L_eff (s)"] = round(pdata.get("L_eff_s", 0), 3)
                    row["p"] = pdata.get("p_current", 0)
                    row["Sketch N"] = pdata.get("sketch_total_count", 0)
                    row["Completeness %"] = round(pdata.get("data_completeness_pct", 0), 2)
                    row["On-Time"] = pdata.get("on_time", 0)
                    row["Late"] = pdata.get("late_dropped", 0)
                    row["DLQ"] = pdata.get("dlq_backlog", 0)
                    row["Extreme"] = pdata.get("extreme_lag_count", 0)
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
    st.markdown("Dataset replay progress, event rates, and per-partition distribution.")

    parts = _collect_partition_data(metrics)
    if not parts:
        st.info("Waiting for partition data...")
        return

    total_recv = sum(p["received"] for p in parts)
    total_on = sum(p["on_time"] for p in parts)
    total_late = sum(p["late"] for p in parts)

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
    else:
        rate = 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Event Rate", f"{rate:.1f} ev/s")
    c2.metric("Total Processed", f"{total_recv:,}")
    if total_rows > 0:
        pct = 100.0 * total_recv / total_rows
        c3.metric("Dataset Progress", f"{pct:.1f}%")
        if rate > 0:
            remaining = (total_rows - total_recv) / rate
            m, s_ = divmod(int(remaining), 60)
            c4.metric("ETA", f"{m}m {s_}s")
        else:
            c4.metric("ETA", "—")
    else:
        c3.metric("Dataset Progress", "?")
        c4.metric("ETA", "?")

    st.markdown("---")

    # Per-partition event distribution
    col_chart, col_table = st.columns([2, 1])
    with col_chart:
        st.markdown("**Events per Partition**")
        if parts:
            chart_data = pd.DataFrame([
                {"Partition": p["partition"], "Received": p["received"], "On-Time": p["on_time"], "Late": p["late"]}
                for p in parts
            ])
            st.bar_chart(chart_data.set_index("Partition")[["On-Time", "Late"]], height=300)
    with col_table:
        st.markdown("**Partition Stats**")
        tbl = []
        for p in sorted(parts, key=lambda x: int(x["partition"])):
            tbl.append({
                "P": p["partition"],
                "Recv": p["received"],
                "On-Time": p["on_time"],
                "Late": p["late"],
                "Comp %": p["completeness"],
                "W_h": round(p["W_h"], 1) if isinstance(p["W_h"], (int, float)) and p["W_h"] != float("-inf") else "—",
            })
        st.dataframe(pd.DataFrame(tbl), use_container_width=True, hide_index=True)

    # Distribution summary
    st.markdown("---")
    st.markdown("**Event Distribution Skew**")
    recv_vals = [p["received"] for p in parts]
    if recv_vals and max(recv_vals) > 0:
        skew_ratio = max(recv_vals) / max(sum(recv_vals) / len(recv_vals), 1)
        st.caption(f"Max/avg ratio: {skew_ratio:.2f}x | "
                   f"Min: {min(recv_vals):,} | Max: {max(recv_vals):,} | "
                   f"StdDev: {pd.Series(recv_vals).std():.0f}")

    # Chunk boundary effects
    if total_rows > 0 and total_recv > 0:
        st.markdown("**Chunk Boundary Effects**")
        parts_sorted = sorted(parts, key=lambda x: int(x["partition"]))
        min_recv_parts = [p for p in parts_sorted if p["received"] == min(recv_vals)]
        max_recv_parts = [p for p in parts_sorted if p["received"] == max(recv_vals)]
        st.caption(f"Least-loaded partitions: {', '.join(p['partition'] for p in min_recv_parts)} "
                   f"({min(recv_vals):,} events)")
        st.caption(f"Most-loaded partitions: {', '.join(p['partition'] for p in max_recv_parts)} "
                   f"({max(recv_vals):,} events)")
        if max(recv_vals) > 0:
            st.caption(f"Spread: {(max(recv_vals) - min(recv_vals)) / max(recv_vals) * 100:.1f}% "
                       f"of max — caused by hash(chunk_host) % 12 rounding at chunk boundaries")


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
        if st.button("Refresh Now", use_container_width=True):
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
# Main
# ---------------------------------------------------------------------------

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
    render_sidebar()

    mode = st.session_state.mode
    is_sim = _is_sim()

    # Fetch live metrics
    metrics = {}
    if st.session_state.running:
        metrics = fetch_all_metrics(mode)
        st.session_state.last_metrics = metrics

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
            })
            if len(st.session_state.metrics_history) > MAX_HISTORY:
                st.session_state.metrics_history = st.session_state.metrics_history[-MAX_HISTORY:]

    # Tabs — sim mode adds "Sim Stats" tab
    if is_sim:
        tab_dash, tab_sim, tab_logs, tab_compare, tab_raw = st.tabs(
            ["Dashboard", "Sim Stats", "Logs", "Compare", "Raw JSON"]
        )
    else:
        tab_dash, tab_logs, tab_compare, tab_raw = st.tabs(
            ["Dashboard", "Logs", "Compare", "Raw JSON"]
        )

    with tab_dash:
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
5. Click **Stop** when done — results are saved for comparison

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
5. Watch aggregated metrics from all 4 workers auto-refresh
6. Click **Stop** when done — results are saved for comparison

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

    if is_sim:
        with tab_sim:
            if st.session_state.running and metrics.get("workers"):
                render_sim_stats(mode, metrics)
            elif st.session_state.running:
                st.info("Waiting for partition data...")
            else:
                st.info("Start the simulation to see replay statistics.")

    with tab_logs:
        render_logs()

    with tab_compare:
        render_comparison()

    with tab_raw:
        render_raw(metrics)

    # Auto-refresh
    if st.session_state.running and st.session_state.auto_refresh:
        time.sleep(REFRESH_INTERVAL_S)
        st.rerun()


if __name__ == "__main__":
    main()
