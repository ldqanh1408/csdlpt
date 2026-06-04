"""
Dashboard Streamlit điều khiển và quan sát cụm CSDLPT Watermark.

Dashboard chỉ chạy `deploy/docker-compose.yml` cho triển khai đầy đủ: 3 coordinator HA, 4 worker, Kafka, ZooKeeper, MinIO, Prometheus và Grafana; cung cấp start/stop/logs, health, metrics, bottleneck và thí nghiệm failover/completeness.
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
import threading
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------------------------
# Đường dẫn quan trọng của dashboard và project.
# ---------------------------------------------------------------------------

DEPLOY_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = DEPLOY_DIR.parent
COMPOSE_FILE = str(DEPLOY_DIR / "docker-compose.yml")
COMPOSE_SIM_FILE = str(DEPLOY_DIR / "docker-compose.sim.yml")
DATASET_DIR = str(PROJECT_ROOT / "dataset")

# ---------------------------------------------------------------------------
# Topology service trong compose đầy đủ và cấu hình mô phỏng cũ.
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
# Heuristic aggregator HA standby (host port → container :8000). The active vs
# standby role is decided at runtime by AggregatorHA, so it must be probed live
# (see _detect_active_aggregator) rather than assumed.
AGGREGATOR_STANDBY_PORT_FULL = 9005

MAX_HISTORY = 500
REFRESH_INTERVAL_S = 3
HEURISTIC_PERCENTILE_OPTIONS = {
    "p50 - median, very low latency": 0.50,
    "p60 - low latency": 0.60,
    "p70 - low latency, fewer drops": 0.70,
    "p75 - quartile baseline": 0.75,
    "p80 - balanced latency": 0.80,
    "p85 - balanced safety": 0.85,
    "p90 - safer low latency": 0.90,
    "p95 - demo-safe": 0.95,
    "p97 - conservative": 0.97,
    "p98 - high completeness": 0.98,
    "p99 - default loss target": 0.99,
    "p99.5 - very conservative": 0.995,
    "p99.9 - burst-safe": 0.999,
    "p99.99 - maximum safety": 0.9999,
}

EXPERIMENT_RUNTIME_ENV = {
    "BP_PAUSE_THRESHOLD": "100000",
    "BP_RESUME_THRESHOLD": "5000",
    "STRICT_HARD_QUEUE_LIMIT": "300000",
}

DASHBOARD_PUNCTUATION_MODE = "max-event-time"
HEURISTIC_DEFAULT_P_NORMAL = 0.75
HEURISTIC_DEFAULT_P_SAFE = 0.999

HEURISTIC_RUNTIME_ENV = {
    "INGESTOR_REPLAY": "arrival",
    "REPLAY_SPEED": "150",
    "HEURISTIC_LOCAL_WATERMARK_CLOSE": "true",
    "HEURISTIC_WARMUP_SAMPLES": "2000",
    "HEURISTIC_WARMUP_S": "5.0",
    "PYTHONUNBUFFERED": "1",
}

HEURISTIC_RUNTIME_ENV_KEYS = {
    "INGESTOR_REPLAY": "heuristic_ingestor_replay",
    "REPLAY_SPEED": "heuristic_replay_speed",
    "HEURISTIC_LOCAL_WATERMARK_CLOSE": "heuristic_local_watermark_close",
    "HEURISTIC_WARMUP_SAMPLES": "heuristic_warmup_samples",
    "HEURISTIC_WARMUP_S": "heuristic_warmup_s",
    "PYTHONUNBUFFERED": "heuristic_python_unbuffered",
}


# ---------------------------------------------------------------------------
# Helper chọn mode triển khai.
# ---------------------------------------------------------------------------

def _is_sim(deploy_mode: str = None) -> bool:
    # Dashboard này chỉ điều khiển deploy/docker-compose.yml cho cụm đầy đủ.
    # Compose mô phỏng không được khởi chạy từ UI, nên sim mode luôn tắt dù
    # session_state trước đó lưu giá trị nào.
    """Hàm `_is_sim` thực hiện phần xử lý liên quan đến is sim."""
    return False


def _compose_file(deploy_mode: str = None) -> str:
    """Hàm `_compose_file` thực hiện phần xử lý liên quan đến compose file."""
    return COMPOSE_SIM_FILE if _is_sim(deploy_mode) else COMPOSE_FILE


def _workers(deploy_mode: str = None, mode: str = None) -> dict:
    """Hàm `_workers` thực hiện phần xử lý liên quan đến workers."""
    if _is_sim(deploy_mode):
        if mode is None:
            try:
                mode = st.session_state.get("mode", "strict")
            except Exception:
                mode = "strict"
        return {k: v for k, v in WORKERS_SIM.items() if v.get("profile") == mode}
    return WORKERS_FULL


def _infra(deploy_mode: str = None) -> dict:
    """Hàm `_infra` thực hiện phần xử lý liên quan đến infra."""
    return INFRA_SIM if _is_sim(deploy_mode) else INFRA_FULL


def _coordinators(deploy_mode: str = None) -> dict:
    """Hàm `_coordinators` thực hiện phần xử lý liên quan đến coordinators."""
    return COORDINATORS_SIM if _is_sim(deploy_mode) else COORDINATORS_FULL


def _aggregator_port(deploy_mode: str = None) -> int:
    """Hàm `_aggregator_port` thực hiện phần xử lý liên quan đến aggregator port."""
    return AGGREGATOR_PORT_SIM if _is_sim(deploy_mode) else AGGREGATOR_PORT_FULL


# ---------------------------------------------------------------------------
# Trạng thái session Streamlit.
# ---------------------------------------------------------------------------

def init_state():
    """Hàm `init_state` thực hiện phần xử lý liên quan đến init state."""
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
        "dataset_file": "nyc_taxi_events_full.csv",
        "log_level": "info",
        "punctuation_mode": DASHBOARD_PUNCTUATION_MODE,
        "delta_base_s": 10.0,
        "heuristic_p_normal": HEURISTIC_DEFAULT_P_NORMAL,
        "heuristic_p_safe": HEURISTIC_DEFAULT_P_SAFE,
        "heuristic_ingestor_replay": "arrival",
        "heuristic_replay_speed": 150,
        "heuristic_local_watermark_close": True,
        "heuristic_warmup_samples": 2000,
        "heuristic_warmup_s": 5.0,
        "heuristic_python_unbuffered": True,
        "log_filter": "",
        "log_auto_scroll": True,
        "lab_points": [],
        "failover_test_log": [],
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v
            
    if "fetch_shared_state" not in st.session_state:
        st.session_state.fetch_shared_state = {
            "metrics": None,
            "container_statuses": {},
            "node_statuses": {},
            "last_fetch_time": 0.0,
            "fetch_in_progress": False
        }
    if "last_processed_fetch_time" not in st.session_state:
        st.session_state.last_processed_fetch_time = 0.0
    if "node_statuses" not in st.session_state:
        st.session_state.node_statuses = {}


# ---------------------------------------------------------------------------
# Dataset đầu vào trong thư mục dataset/.
# ---------------------------------------------------------------------------

def get_datasets() -> dict[str, str]:
    """Trả về thông tin `datasets` từ trạng thái hiện tại."""
    csvs = glob.glob(os.path.join(DATASET_DIR, "**/*.csv"), recursive=True)
    return {os.path.relpath(f, DATASET_DIR).replace("\\", "/"): f for f in csvs}


@st.cache_data
def count_csv_rows(path: str) -> int:
    """Đếm số lượng `csv rows` theo nguồn dữ liệu hiện tại."""
    try:
        with open(path) as f:
            return sum(1 for _ in f) - 1
    except Exception:
        return -1


# ---------------------------------------------------------------------------
# Lệnh điều khiển Docker Compose.
# ---------------------------------------------------------------------------

def _compose_base(deploy_mode: str = None) -> list[str]:
    """Hàm `_compose_base` thực hiện phần xử lý liên quan đến compose base."""
    return ["docker", "compose", "-f", _compose_file(deploy_mode)]


def _heuristic_runtime_env_from_state() -> dict:
    """Return heuristic runtime env values configured from the dashboard."""
    env = {}
    for env_key, state_key in HEURISTIC_RUNTIME_ENV_KEYS.items():
        value = st.session_state.get(state_key, HEURISTIC_RUNTIME_ENV[env_key])
        if env_key == "HEURISTIC_LOCAL_WATERMARK_CLOSE":
            value = "true" if bool(value) else "false"
        elif env_key == "PYTHONUNBUFFERED":
            value = "1" if bool(value) else "0"
        else:
            value = str(value)
        env[env_key] = value
    return env


def _apply_heuristic_recommended_defaults():
    """Apply the same heuristic defaults used by the Docker experiment runner."""
    st.session_state.punctuation_mode = DASHBOARD_PUNCTUATION_MODE
    st.session_state.heuristic_p_normal = HEURISTIC_DEFAULT_P_NORMAL
    st.session_state.heuristic_p_safe = HEURISTIC_DEFAULT_P_SAFE
    st.session_state.heuristic_ingestor_replay = HEURISTIC_RUNTIME_ENV["INGESTOR_REPLAY"]
    st.session_state.heuristic_replay_speed = int(HEURISTIC_RUNTIME_ENV["REPLAY_SPEED"])
    st.session_state.heuristic_local_watermark_close = (
        HEURISTIC_RUNTIME_ENV["HEURISTIC_LOCAL_WATERMARK_CLOSE"].lower() == "true"
    )
    st.session_state.heuristic_warmup_samples = int(HEURISTIC_RUNTIME_ENV["HEURISTIC_WARMUP_SAMPLES"])
    st.session_state.heuristic_warmup_s = float(HEURISTIC_RUNTIME_ENV["HEURISTIC_WARMUP_S"])
    st.session_state.heuristic_python_unbuffered = HEURISTIC_RUNTIME_ENV["PYTHONUNBUFFERED"] == "1"


def _make_env(mode: str) -> dict:
    """Hàm `_make_env` thực hiện phần xử lý liên quan đến make env."""
    env = os.environ.copy()
    env["MODE"] = mode
    env["DATASET_FILE"] = st.session_state.get("dataset_file", "nyc_taxi_events_full.csv")
    env["LOG_LEVEL"] = st.session_state.get("log_level", "info")
    st.session_state.punctuation_mode = DASHBOARD_PUNCTUATION_MODE
    env["PUNCTUATION_MODE"] = DASHBOARD_PUNCTUATION_MODE
    env.update(EXPERIMENT_RUNTIME_ENV)
    p_normal = float(st.session_state.get("heuristic_p_normal", 0.99))
    p_safe = float(st.session_state.get("heuristic_p_safe", max(p_normal, 0.999)))
    env["HEURISTIC_P_NORMAL"] = f"{p_normal:.6g}"
    env["HEURISTIC_P_SAFE"] = f"{p_safe:.6g}"
    if mode == "heuristic":
        env.update(_heuristic_runtime_env_from_state())
    # Wait Time (delta_base) — independent variable for the completeness-vs-wait
    # study. docker-compose.yml interpolates ${DELTA_BASE_S} into coordinators
    # and workers.
    env["DELTA_BASE_S"] = str(st.session_state.get("delta_base_s", 10.0))
    return env


def compose_up(mode: str, dataset_file: str) -> tuple[bool, str]:
    """Hàm `compose_up` thực hiện phần xử lý liên quan đến compose up."""
    env = _make_env(mode)
    env["DATASET_FILE"] = dataset_file

    profiles = [mode]

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
    """Hàm `compose_down` thực hiện phần xử lý liên quan đến compose down."""
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
    """Hàm `compose_logs` thực hiện phần xử lý liên quan đến compose logs."""
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
    """Định dạng giá trị `format timestamp` để hiển thị hoặc ghi báo cáo.
    
    Ghi chú gốc:
    Format an epoch float to 'YYYY-MM-DD HH:MM:SS' if it looks like a Unix timestamp.
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


def _is_finite_number(value) -> bool:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return False
    return v == v and v not in (float("inf"), float("-inf"))


def _fmt_watermark(value, empty: str = "initializing") -> str:
    """Format W_global/W_h/local watermark values consistently."""
    if not _is_finite_number(value):
        return empty
    return format_timestamp(value)


def _fmt_bytes(value) -> str:
    """Format a byte count with binary units."""
    value = _positive_float(value)
    if value <= 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    idx = 0
    while value >= 1024 and idx < len(units) - 1:
        value /= 1024.0
        idx += 1
    if idx == 0:
        return f"{int(value)} {units[idx]}"
    return f"{value:.2f} {units[idx]}"


def _fmt_ratio(value: float, suffix: str = "x") -> str:
    value = _positive_float(value)
    if value <= 0:
        return "-"
    return f"{value:.2f}{suffix}"


# ---------------------------------------------------------------------------
# Metrics fetching
# ---------------------------------------------------------------------------

def _get_json(url: str, timeout: float = 2.0):
    """Hàm `_get_json` thực hiện phần xử lý liên quan đến get json."""
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None


def is_healthy(port: int) -> bool:
    """Kiểm tra điều kiện `is healthy` và trả về boolean."""
    try:
        r = requests.get(f"http://localhost:{port}/health", timeout=1.5)
        return r.status_code == 200
    except Exception:
        return False


def check_infra_health(name: str, deploy_mode: str = None) -> bool:
    """Kiểm tra điều kiện `check infra health` và trả về kết quả đánh giá."""
    info = _infra(deploy_mode)[name]
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


def _infra_key(name: str, deploy_mode: str = None) -> str | None:
    """Hàm `_infra_key` thực hiện phần xử lý liên quan đến infra key.
    
    Ghi chú gốc:
    Return the configured infra display key for a docker service name.
    """
    lname = name.lower()
    for key in _infra(deploy_mode):
        if key.lower() == lname:
            return key
    return None


def fetch_all_metrics(mode: str, deploy_mode: str = None) -> dict:
    """Lấy dữ liệu `fetch all metrics` từ service, cache hoặc nguồn bên ngoài."""
    result = {"timestamp": time.time(), "mode": mode, "workers": {}}

    urls_to_fetch = {}

    # 1. Workers
    for name, info in _workers(deploy_mode, mode).items():
        port = info["port"] if isinstance(info, dict) else info
        urls_to_fetch[f"w_m_{name}"] = (f"http://localhost:{port}/api/metrics", 1.5)
        urls_to_fetch[f"w_s_{name}"] = (f"http://localhost:{port}/state", 1.5)

    # 2. Mode-specific control plane. Strict mode uses Raft coordinators;
    # heuristic mode uses the DDSketch aggregator HA pair. Fetching only the
    # active mode keeps refresh latency low and avoids showing stale role data.
    fetch_coordinators = mode == "strict" or mode not in ("strict", "heuristic")
    fetch_aggregator = mode == "heuristic" or mode not in ("strict", "heuristic")
    if fetch_coordinators:
        for cname, cport in _coordinators(deploy_mode).items():
            urls_to_fetch[f"c_{cname}"] = (f"http://localhost:{cport}/state", 1.5)
            urls_to_fetch[f"c_ih_{cname}"] = (f"http://localhost:{cport}/ingestor-health", 1.5)

    if fetch_aggregator:
        agg_port = _aggregator_port(deploy_mode)
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
    for name, info in _workers(deploy_mode, mode).items():
        port = info["port"] if isinstance(info, dict) else info
        metrics = fetched.get(f"w_m_{name}")
        state = fetched.get(f"w_s_{name}")
        if metrics or state:
            result["workers"][name] = {"metrics": metrics, "state": state, "port": port}

    # Assemble coordinator
    if fetch_coordinators:
        best_coord = None
        best_cname = None
        for cname, cport in _coordinators(deploy_mode).items():
            coord = fetched.get(f"c_{cname}")
            if coord is None:
                continue
            ih = fetched.get(f"c_ih_{cname}")
            if ih:
                coord["ingestor_health"] = ih
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
    if fetch_aggregator:
        agg = fetched.get("aggregator")
        if agg:
            result["aggregator"] = agg

    return result


def aggregate_worker_metrics(metrics: dict) -> dict:
    """Tổng hợp dữ liệu `aggregate worker metrics` thành một kết quả chung.
    
    Ghi chú gốc:
    Aggregate metrics from all workers into a single summary including latency percentiles.
    """
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
    timer_vals = {
        "network_ingest": {"p50": [], "p95": [], "p99": []},
        "poll_decode": {"p50": [], "p95": [], "p99": []},
        "dedup": {"p50": [], "p95": [], "p99": []},
        "state_write": {"p50": [], "p95": [], "p99": []},
        "sketch_update": {"p50": [], "p95": [], "p99": []},
        "sketch_query": {"p50": [], "p95": [], "p99": []},
    }

    def _collect_latency_from(d: dict):
        """Thu thập dữ liệu `collect latency from` từ nhiều nguồn nội bộ.
        
        Ghi chú gốc:
        Extract latency fields from a partition or engine metrics dict.
        """
        if not isinstance(d, dict):
            return

        def _get_float(key: str) -> float:
            """Hàm `_get_float` thực hiện phần xử lý liên quan đến get float."""
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

        for stage, percentiles in timer_vals.items():
            for pct, target in percentiles.items():
                val = _get_float(f"{stage}_latency_{pct}_us")
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
            partition_metrics = [
                pdata for pdata in m.get("partitions", {}).values()
                if isinstance(pdata, dict)
            ]
            if partition_metrics:
                for pdata in partition_metrics:
                    _collect_latency_from(pdata)
            else:
                _collect_latency_from(m)
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

    def _avg(lst):
        """Tính giá trị trung bình đã làm tròn cho danh sách metric."""
        return round(sum(lst) / len(lst), 2) if lst else 0.0

    def _max(lst):
        """Tính giá trị lớn nhất đã làm tròn cho danh sách metric."""
        return round(max(lst), 2) if lst else 0.0

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
        "network_ingest_p50_us": _avg(timer_vals["network_ingest"]["p50"]),
        "network_ingest_p95_us": _avg(timer_vals["network_ingest"]["p95"]),
        "network_ingest_p99_us": _avg(timer_vals["network_ingest"]["p99"]),
        "network_ingest_p95_max_us": _max(timer_vals["network_ingest"]["p95"]),
        "network_ingest_p99_max_us": _max(timer_vals["network_ingest"]["p99"]),
        "poll_decode_p50_us": _avg(timer_vals["poll_decode"]["p50"]),
        "poll_decode_p95_us": _avg(timer_vals["poll_decode"]["p95"]),
        "poll_decode_p99_us": _avg(timer_vals["poll_decode"]["p99"]),
        "poll_decode_p95_max_us": _max(timer_vals["poll_decode"]["p95"]),
        "poll_decode_p99_max_us": _max(timer_vals["poll_decode"]["p99"]),
        "dedup_p50_us": _avg(timer_vals["dedup"]["p50"]),
        "dedup_p95_us": _avg(timer_vals["dedup"]["p95"]),
        "dedup_p99_us": _avg(timer_vals["dedup"]["p99"]),
        "dedup_p95_max_us": _max(timer_vals["dedup"]["p95"]),
        "dedup_p99_max_us": _max(timer_vals["dedup"]["p99"]),
        "state_write_p50_us": _avg(timer_vals["state_write"]["p50"]),
        "state_write_p95_us": _avg(timer_vals["state_write"]["p95"]),
        "state_write_p99_us": _avg(timer_vals["state_write"]["p99"]),
        "state_write_p95_max_us": _max(timer_vals["state_write"]["p95"]),
        "state_write_p99_max_us": _max(timer_vals["state_write"]["p99"]),
        "sketch_update_p50_us": _avg(timer_vals["sketch_update"]["p50"]),
        "sketch_update_p95_us": _avg(timer_vals["sketch_update"]["p95"]),
        "sketch_update_p99_us": _avg(timer_vals["sketch_update"]["p99"]),
        "sketch_update_p95_max_us": _max(timer_vals["sketch_update"]["p95"]),
        "sketch_update_p99_max_us": _max(timer_vals["sketch_update"]["p99"]),
        "sketch_query_p50_us": _avg(timer_vals["sketch_query"]["p50"]),
        "sketch_query_p95_us": _avg(timer_vals["sketch_query"]["p95"]),
        "sketch_query_p99_us": _avg(timer_vals["sketch_query"]["p99"]),
        "sketch_query_p95_max_us": _max(timer_vals["sketch_query"]["p95"]),
        "sketch_query_p99_max_us": _max(timer_vals["sketch_query"]["p99"]),
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
    """Thu thập dữ liệu `collect partition data` từ nhiều nguồn nội bộ.
    
    Ghi chú gốc:
    Extract per-partition engine summaries from all workers.
    """
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
                "network_ingest_p50": pdata.get("network_ingest_latency_p50_us", 0.0),
                "network_ingest_p95": pdata.get("network_ingest_latency_p95_us", 0.0),
                "network_ingest_p99": pdata.get("network_ingest_latency_p99_us", 0.0),
                "poll_decode_p50": pdata.get("poll_decode_latency_p50_us", 0.0),
                "poll_decode_p95": pdata.get("poll_decode_latency_p95_us", 0.0),
                "poll_decode_p99": pdata.get("poll_decode_latency_p99_us", 0.0),
                "dedup_p50": pdata.get("dedup_latency_p50_us", 0.0),
                "dedup_p95": pdata.get("dedup_latency_p95_us", 0.0),
                "dedup_p99": pdata.get("dedup_latency_p99_us", 0.0),
                "state_write_p50": pdata.get("state_write_latency_p50_us", 0.0),
                "state_write_p95": pdata.get("state_write_latency_p95_us", 0.0),
                "state_write_p99": pdata.get("state_write_latency_p99_us", 0.0),
                "sketch_update_p50": pdata.get("sketch_update_latency_p50_us", 0.0),
                "sketch_update_p95": pdata.get("sketch_update_latency_p95_us", 0.0),
                "sketch_update_p99": pdata.get("sketch_update_latency_p99_us", 0.0),
                "sketch_query_p50": pdata.get("sketch_query_latency_p50_us", 0.0),
                "sketch_query_p95": pdata.get("sketch_query_latency_p95_us", 0.0),
                "sketch_query_p99": pdata.get("sketch_query_latency_p99_us", 0.0),
                "clock_skew_ms": pdata.get("clock_skew_ms", pdata.get("node_skew_ms", 0.0)),
                "punctuation_total": pdata.get("punctuation_total", pdata.get("punctuation_count", 0)),
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
    """Thu thập dữ liệu `collect per window loss` từ nhiều nguồn nội bộ.
    
    Ghi chú gốc:
    Collect per_window_loss from all partitions.
    """
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
    """Hàm `_heuristic_kpi_row` thực hiện phần xử lý liên quan đến heuristic kpi row.
    
    Ghi chú gốc:
    Render heuristic-specific KPI row below the main KPIs.
    """
    parts = _collect_partition_data(metrics)
    if not parts:
        return

    wh_vals = [p["W_h"] for p in parts if p["W_h"] != float("-inf")]
    leff_vals = [p["L_eff_s"] for p in parts if p["L_eff_s"] > 0]
    p_vals = [float(p["p_current"]) for p in parts if p.get("p_current")]

    st.markdown("---")
    st.markdown("#### Heuristic-Specific KPIs")
    c1, c2, c3, c4, c5, c6, c7, c8 = st.columns(8)
    c1.metric("DLQ Backlog", _fmt_count(agg.get("dlq_backlog", 0)))
    c2.metric("Sketch Samples", _fmt_count(agg.get("sketch_total_count", 0)))
    c3.metric("Extreme Lag Events", _fmt_count(agg.get("extreme_lag_count", 0)))

    agg_state = metrics.get("aggregator", {})
    wgh = agg_state.get("W_global_h")
    c4.metric("W_global_h", _fmt_watermark(wgh, "-"))

    if wh_vals:
        c5.metric("Max W_h", _fmt_watermark(max(wh_vals), "-"))
        c6.metric("Min W_h", _fmt_watermark(min(wh_vals), "-"))
        c7.metric("W_h Span", _fmt_dur_s(max(wh_vals) - min(wh_vals)))
    else:
        c5.metric("Max W_h", "-")
        c6.metric("Min W_h", "-")
        c7.metric("W_h Span", "-")

    if p_vals:
        c8.metric("Percentile p", f"{max(p_vals):.3f}")
    elif leff_vals:
        c8.metric("Avg L_eff", _fmt_dur_s(sum(leff_vals) / len(leff_vals)))
    else:
        c8.metric("Percentile p", "-")

    if leff_vals:
        st.caption(f"Avg L_eff: {_fmt_dur_s(sum(leff_vals) / len(leff_vals))}")

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
# Profiling / bottleneck helpers
# ---------------------------------------------------------------------------

PROFILE_STAGE_SPECS = [
    ("network_ingest", "T_network_ingest_ns", "Network/Kafka inbound", "Network and Kafka receive delay"),
    ("poll_decode", "T_poll_decode_ns", "Poll/decode", "Kafka poll + JSON/schema decode"),
    ("dedup", "T_deduplication_ns", "Deduplication", "ID filter and RocksDB dedup log check"),
    ("state_write", "T_state_write_ns", "State write", "Window/RocksDB state update"),
    ("sketch_update", "T_sketch_update_ns", "DDSketch update", "Insert observed lag into DDSketch"),
    ("sketch_query", "T_sketch_query_ns", "DDSketch query", "Estimate effective watermark lag"),
]


def _positive_float(value) -> float:
    """Hàm `_positive_float` thực hiện phần xử lý liên quan đến positive float."""
    try:
        f = float(value)
    except (TypeError, ValueError):
        return 0.0
    if f != f or f in (float("inf"), float("-inf")):
        return 0.0
    return max(0.0, f)


def _fmt_us(value: float) -> str:
    """Định dạng giá trị `fmt us` để hiển thị hoặc ghi báo cáo."""
    value = _positive_float(value)
    if value <= 0:
        return "—"
    if value >= 1000:
        return f"{value / 1000.0:.2f} ms"
    return f"{value:.0f} µs"


# Unit-consistent KPI formatters. Used across the main KPI rows so every metric
# reads with the same scale/precision rules (auto-promote µs→ms, ms→s, s→min;
# thousands separators for counts) instead of ad-hoc per-call f-strings.

def _fmt_ms(value: float) -> str:
    """Format a millisecond value, promoting to seconds when large."""
    value = _positive_float(value)
    if value <= 0:
        return "—"
    if value >= 1000:
        return f"{value / 1000.0:.2f} s"
    return f"{value:.1f} ms"


def _fmt_dur_s(value: float) -> str:
    """Format a duration given in seconds, auto-scaling ms/s/min."""
    value = _positive_float(value)
    if value <= 0:
        return "—"
    if value < 1.0:
        return f"{value * 1000.0:.0f} ms"
    if value >= 60.0:
        return f"{value / 60.0:.1f} min"
    return f"{value:.1f} s"


def _fmt_count(value) -> str:
    """Format an integer count with thousands separators (— for empty)."""
    try:
        v = int(value)
    except (TypeError, ValueError):
        return "—"
    return f"{v:,}"


def _fmt_rate(value: float, suffix: str = "/s") -> str:
    """Format a per-second rate, using K/M suffixes for large throughput."""
    value = _positive_float(value)
    if value <= 0:
        return "—"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M{suffix}"
    if value >= 1_000:
        return f"{value / 1_000:.1f}K{suffix}"
    return f"{value:.1f}{suffix}"


def _fmt_pct(value: float, digits: int = 2) -> str:
    """Format a percentage value with fixed precision."""
    try:
        return f"{float(value):.{digits}f}%"
    except (TypeError, ValueError):
        return "—"


def _partition_bottleneck(partition_row: dict) -> tuple[str, float, str]:
    """Hàm `_partition_bottleneck` thực hiện phần xử lý liên quan đến partition bottleneck."""
    candidates = []
    for key, _raw_metric, label, hint in PROFILE_STAGE_SPECS:
        p99 = _positive_float(partition_row.get(f"{key}_p99"))
        p95 = _positive_float(partition_row.get(f"{key}_p95"))
        val = p99 if p99 > 0 else p95
        if val > 0:
            candidates.append((label, val, hint))
    if not candidates:
        proc = _positive_float(partition_row.get("proc_p95"))
        if proc > 0:
            return "Overall processing", proc, "End-to-end event handling"
        return "—", 0.0, "Waiting for timer samples"
    return max(candidates, key=lambda item: item[1])


def _build_bottleneck_profile(metrics: dict, mode: str, agg: dict) -> tuple[list[dict], list[dict]]:
    """Xây dựng cấu trúc dữ liệu hoặc payload `build bottleneck profile`."""
    parts = _collect_partition_data(metrics)
    stage_rows = []

    for key, raw_metric, label, hint in PROFILE_STAGE_SPECS:
        p50_values = [_positive_float(p.get(f"{key}_p50")) for p in parts]
        p95_values = [_positive_float(p.get(f"{key}_p95")) for p in parts]
        p99_values = [_positive_float(p.get(f"{key}_p99")) for p in parts]
        p50_values = [v for v in p50_values if v > 0]
        p95_values = [v for v in p95_values if v > 0]
        p99_values = [v for v in p99_values if v > 0]
        score_values = p99_values or p95_values

        if not score_values:
            avg_p50 = _positive_float(agg.get(f"{key}_p50_us"))
            avg_p95 = _positive_float(agg.get(f"{key}_p95_us"))
            avg_p99 = _positive_float(agg.get(f"{key}_p99_us"))
            max_p95 = _positive_float(agg.get(f"{key}_p95_max_us"))
            max_p99 = _positive_float(agg.get(f"{key}_p99_max_us"))
            score = max_p99 if max_p99 > 0 else max_p95
            if avg_p50 <= 0 and avg_p95 <= 0 and avg_p99 <= 0 and score <= 0:
                continue
            stage_rows.append({
                "Stage": label,
                "Raw Timer": raw_metric,
                "Avg p50 (µs)": round(avg_p50, 1),
                "Avg p95 (µs)": round(avg_p95, 1),
                "Avg p99 (µs)": round(avg_p99, 1),
                "Max p99 (µs)": round(max(score, avg_p99), 1),
                "Slowest Worker": "—",
                "Slowest Partition": "—",
                "Diagnosis": hint,
            })
            continue

        slowest = max(parts, key=lambda p: _positive_float(p.get(f"{key}_p99")) or _positive_float(p.get(f"{key}_p95")))
        max_p99 = max(p99_values) if p99_values else max(p95_values)
        stage_rows.append({
            "Stage": label,
            "Raw Timer": raw_metric,
            "Avg p50 (µs)": round(sum(p50_values) / len(p50_values), 1) if p50_values else 0.0,
            "Avg p95 (µs)": round(sum(p95_values) / len(p95_values), 1) if p95_values else 0.0,
            "Avg p99 (µs)": round(sum(p99_values) / len(p99_values), 1) if p99_values else 0.0,
            "Max p99 (µs)": round(max_p99, 1),
            "Slowest Worker": slowest.get("worker", "—"),
            "Slowest Partition": slowest.get("partition", "—"),
            "Diagnosis": hint,
        })

    if not stage_rows:
        proc_vals = [_positive_float(p.get("proc_p95")) for p in parts]
        proc_vals = [v for v in proc_vals if v > 0]
        if proc_vals:
            slowest = max(parts, key=lambda p: _positive_float(p.get("proc_p95")))
            stage_rows.append({
                "Stage": "Overall processing",
                "Raw Timer": "process() elapsed",
                "Avg p50 (µs)": 0.0,
                "Avg p95 (µs)": round(sum(proc_vals) / len(proc_vals), 1),
                "Avg p99 (µs)": 0.0,
                "Max p99 (µs)": round(max(proc_vals), 1),
                "Slowest Worker": slowest.get("worker", "—"),
                "Slowest Partition": slowest.get("partition", "—"),
                "Diagnosis": "End-to-end event handling",
            })

    total_max = sum(_positive_float(r.get("Max p99 (µs)")) for r in stage_rows)
    for row in stage_rows:
        max_p99 = _positive_float(row.get("Max p99 (µs)"))
        share = (100.0 * max_p99 / total_max) if total_max > 0 else 0.0
        row["Bottleneck Share"] = f"{share:.1f}%"
        if share >= 45:
            row["Impact"] = "Dominant"
        elif share >= 25:
            row["Impact"] = "High"
        else:
            row["Impact"] = "Watch"

    top_partitions = []
    for p in parts:
        stage, val, hint = _partition_bottleneck(p)
        if val <= 0:
            continue
        top_partitions.append({
            "Worker": p.get("worker", "—"),
            "Partition": p.get("partition", "—"),
            "Bottleneck": stage,
            "Stage p99/p95 (µs)": round(val, 1),
            "Proc p95 (µs)": round(_positive_float(p.get("proc_p95")), 1),
            "Received": p.get("received", 0),
            "Diagnosis": hint,
        })
    top_partitions.sort(key=lambda row: row["Stage p99/p95 (µs)"], reverse=True)

    stage_rows.sort(key=lambda row: _positive_float(row.get("Max p99 (µs)")), reverse=True)
    return stage_rows, top_partitions[:8]


def _current_bottleneck_snapshot(metrics: dict, mode: str, agg: dict) -> dict:
    """Hàm `_current_bottleneck_snapshot` thực hiện phần xử lý liên quan đến current bottleneck snapshot."""
    stage_rows, top_partitions = _build_bottleneck_profile(metrics, mode, agg)
    top_stage = stage_rows[0] if stage_rows else {}
    top_part = top_partitions[0] if top_partitions else {}
    return {
        "stage": top_stage.get("Stage", "—"),
        "stage_p95_us": _positive_float(top_stage.get("Avg p95 (µs)")),
        "stage_p99_us": _positive_float(top_stage.get("Max p99 (µs)")),
        "partition": top_part.get("Partition", "—"),
        "worker": top_part.get("Worker", "—"),
    }


def _format_timer_display_rows(rows: list[dict]) -> list[dict]:
    formatted = []
    for row in rows:
        display = dict(row)
        for key, value in list(display.items()):
            if key.startswith(("Avg p", "Max p", "Stage p99/p95", "Proc p95")):
                display[key] = _fmt_us(value)
            elif key in ("Received",):
                display[key] = _fmt_count(value)
        formatted.append(display)
    return formatted


def render_high_res_timer_profile(mode: str, metrics: dict, agg: dict, compact: bool = False):
    """Render phần giao diện `render high res timer profile` lên dashboard hoặc báo cáo."""
    stage_rows, top_partitions = _build_bottleneck_profile(metrics, mode, agg)
    bottleneck = _current_bottleneck_snapshot(metrics, mode, agg)
    proc_p95 = agg.get("proc_lat_p95_us", 0.0)
    proc_p99 = agg.get("proc_lat_p99_us", 0.0)

    st.markdown("##### ⏱️ High-Resolution Timers & Bottlenecks")
    st.caption(
        "Worker stages are measured with `time.perf_counter_ns()` and shown as p95/p99 latency. "
        "The dashboard ranks stages and partitions by p95 to identify bottlenecks."
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Timer Source", "perf_counter_ns")
    c2.metric("Overall p95", _fmt_us(proc_p95))
    c3.metric("Overall p99", _fmt_us(proc_p99))
    c4.metric("Top Bottleneck", bottleneck["stage"])

    if not stage_rows:
        st.info("Waiting for high-resolution timer samples. They appear after workers process events.")
        return

    if compact:
        st.caption(
            f"Slowest stage: **{bottleneck['stage']}** at **{_fmt_us(bottleneck['stage_p99_us'])} p99**"
            + (
                f" on {bottleneck['worker']} / partition {bottleneck['partition']}."
                if bottleneck["worker"] != "—" else "."
            )
        )
        return

    if bottleneck["stage"] == "State write":
        st.warning(
            "State write is the current bottleneck. On Windows Docker hosts, high "
            "`state_write_latency_p99_us` often points to RocksDB writes through a "
            "host-mounted NTFS/WSL2 path; use the `/tmp` tmpfs checkpoint workspace "
            "to reduce WAL/SST write overhead."
        )

    st.dataframe(pd.DataFrame(_format_timer_display_rows(stage_rows)), width="stretch", hide_index=True)
    if top_partitions:
        st.markdown("##### Slowest Partitions")
        st.dataframe(pd.DataFrame(_format_timer_display_rows(top_partitions)), width="stretch", hide_index=True)

    with st.expander("Metric Reference"):
        st.markdown("""
- `T_network_ingest_ns`: network/Kafka inbound delay.
- `T_poll_decode_ns`: Kafka poll and JSON/schema decode.
- `T_deduplication_ns`: duplicate filter and RocksDB dedup log check.
- `T_state_write_ns`: local window/RocksDB write.
- `T_sketch_update_ns`: DDSketch update.
- `T_sketch_query_ns`: effective watermark lag query.
""")


def render_dashboard_fault_injection(mode: str):
    """Render phần giao diện `render dashboard fault injection` lên dashboard hoặc báo cáo."""
    st.markdown("##### 🛑 Kill Node / Recover")
    st.caption("Fault-injection control is available here and in the dedicated **Kill Node / Recover** tab.")
    render_quick_fault_injection(
        mode,
        key_prefix="dash_nc",
        title="",
        caption_text="Choose a node, kill it, watch the metrics/failover response, then recover it.",
    )


def render_welcome_intro():
    """Render phần giao diện `render welcome intro` lên dashboard hoặc báo cáo."""
    st.markdown("## CSDLPT Watermark Processing System")
    st.markdown("""
Hệ thống mô phỏng và giám sát xử lý dòng dữ liệu phân tán theo **event-time watermark**.
Dashboard này điều khiển triển khai Full bằng `deploy/docker-compose.yml`: 3 coordinator
HA, 4 worker xử lý 12 partition, Kafka, ZooKeeper, Prometheus, Grafana và MinIO.

**Mục tiêu chính**
- Theo dõi độ đầy đủ dữ liệu, late arrival, throughput, watermark lag và DLQ.
- So sánh hai chiến lược watermark: Strict bảo toàn dữ liệu và Heuristic giảm latency.
- Kiểm thử chịu lỗi bằng thao tác Kill/Recover node ngay trên Dashboard.
- Phân tích hiệu năng bằng high-resolution timers và tự chỉ ra stage bottleneck.
""")

    arch_rows = [
        {"Layer": "Ingestor", "Role": "Đọc dataset CSV, đẩy sự kiện vào Kafka theo 12 partition."},
        {"Layer": "Workers", "Role": "Xử lý partition, đóng window, ghi state RocksDB, phát metrics."},
        {"Layer": "Strict Coordinators", "Role": "Tính W_global theo min-of-min và điều phối failover."},
        {"Layer": "Heuristic Aggregator", "Role": "Tổng hợp W_global_h từ DDSketch latency quantiles."},
        {"Layer": "Observability", "Role": "Dashboard, Prometheus, Grafana, raw JSON và export CSV/JSON."},
    ]
    st.dataframe(pd.DataFrame(arch_rows), width="stretch", hide_index=True)

    st.markdown("""
**Cách chạy nhanh**
1. Chọn **Watermark Mode** ở sidebar: `Strict` hoặc `Heuristic`.
2. Chọn dataset CSV, log level và punctuation mode.
3. Nếu chọn `Heuristic`, chọn thêm **Percentile p for L_eff**. Giá trị này được truyền xuống worker qua `HEURISTIC_P_NORMAL`.
4. Bấm **Start** để build và chạy toàn bộ Docker cluster.
5. Xem live metrics trong **Dashboard**, kiểm thử lỗi trong **Kill Node / Recover**, và xem profiler trong **Timers & Bottlenecks**.

**Ý nghĩa các mode**
- **Strict**: dùng punctuation/coordinator để bảo toàn dữ liệu tốt hơn, đổi lại watermark lag thường cao hơn.
- **Heuristic**: dùng DDSketch để lấy `L_eff = quantile(p)`, giảm độ trễ bằng cách chọn phân vị phù hợp. p thấp hơn giảm latency nhưng tăng DLQ; p cao hơn an toàn hơn nhưng chờ lâu hơn.

**Heuristic percentile gợi ý**
- `p50` đến `p85`: thử nghiệm low-latency, phù hợp quan sát trade-off với DLQ.
- `p90` đến `p98`: cân bằng latency và completeness.
- `p99` đến `p99.99`: ưu tiên an toàn, phù hợp demo cần ít late drop hơn.
""")

    st.info(
        "Sau khi Start, panel Kill Node / Recover sẽ xuất hiện ngay trong Dashboard. "
        "Tab Timers & Bottlenecks sẽ dùng `time.perf_counter_ns()` để hiển thị p50/p95/p99 "
        "và chỉ ra stage chậm nhất như network, decode, dedup, state write hoặc DDSketch."
    )


# ---------------------------------------------------------------------------
# UI — Sidebar
# ---------------------------------------------------------------------------

def render_sidebar():
    """Render phần giao diện `render sidebar` lên dashboard hoặc báo cáo."""
    st.sidebar.markdown("## CSDLPT Watermark")
    st.sidebar.markdown("---")

    # Deploy mode is fixed to the full docker-compose.yml deployment.
    deploy_mode = "full"
    st.session_state.deploy_mode = deploy_mode
    st.sidebar.caption("Deploy: **Full** — docker-compose.yml (3 coordinators HA · 4 workers · Kafka/ZK/MinIO)")

    st.sidebar.markdown("---")

    previous_mode = st.session_state.get("mode", "strict")
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
    if mode == "heuristic" and previous_mode != "heuristic" and not st.session_state.running:
        _apply_heuristic_recommended_defaults()
    st.session_state.mode = mode

    st.sidebar.markdown("---")
    datasets = get_datasets()
    if not datasets:
        st.sidebar.warning("No CSV in dataset/")
        selected_ds = st.session_state.get("dataset_file", "nyc_taxi_events_full.csv")
    else:
        dataset_names = list(datasets.keys())
        current_dataset = st.session_state.get("dataset_file", dataset_names[0])
        dataset_index = dataset_names.index(current_dataset) if current_dataset in dataset_names else 0
        selected_ds = st.sidebar.selectbox(
            "Dataset", dataset_names, index=dataset_index, disabled=st.session_state.running,
        )
        st.session_state.dataset_file = selected_ds
        rc = count_csv_rows(datasets[selected_ds])
        if rc >= 0:
            st.sidebar.caption(f"{_fmt_count(rc)} rows")

    st.sidebar.markdown("---")
    st.sidebar.markdown("**Pipeline Settings**")
    st.session_state.log_level = st.sidebar.selectbox(
        "Log Level",
        options=["info", "debug", "trace"],
        index=["info", "debug", "trace"].index(st.session_state.log_level),
        disabled=st.session_state.running,
        help="info=summary every 5s | debug/trace=per-event flow through containers",
    )
    st.session_state.punctuation_mode = DASHBOARD_PUNCTUATION_MODE
    st.sidebar.caption("Punctuation Mode: **max-event-time**")
    # Wait Time (delta_base) — the watermark delay. This is the independent
    # variable for the "Data Completeness % vs Wait Time" deliverable: larger
    # delta waits longer for late data (higher completeness, higher latency).
    if mode != "heuristic":
        st.session_state.delta_base_s = st.sidebar.number_input(
            "Wait Time δ (s)",
            min_value=0.0, max_value=120.0, step=1.0,
            value=float(st.session_state.get("delta_base_s", 10.0)),
            disabled=st.session_state.running,
            help="Watermark wait delay (DELTA_BASE_S). Strict: how long to wait for "
                 "late data before closing a window. Sweep this value across runs and "
                 "record points in the 'Completeness vs Wait' tab.",
        )

    if mode == "heuristic":
        st.sidebar.markdown("**Heuristic DDSketch**")
        if st.sidebar.button(
            "Use recommended defaults",
            disabled=st.session_state.running,
            width="stretch",
        ):
            _apply_heuristic_recommended_defaults()
            st.rerun()
        current_p = float(st.session_state.get("heuristic_p_normal", 0.99))
        option_labels = list(HEURISTIC_PERCENTILE_OPTIONS.keys())
        option_values = list(HEURISTIC_PERCENTILE_OPTIONS.values())
        default_idx = min(
            range(len(option_values)),
            key=lambda i: abs(option_values[i] - current_p),
        )
        selected_percentile = st.sidebar.selectbox(
            "Percentile p for L_eff",
            options=option_labels,
            index=default_idx,
            disabled=st.session_state.running,
            help="Heuristic computes L_eff = DDSketch.quantile(p). Lower p closes "
                 "windows sooner with lower latency but routes more late events to DLQ.",
        )
        st.session_state.heuristic_p_normal = HEURISTIC_PERCENTILE_OPTIONS[selected_percentile]
        st.session_state.heuristic_p_safe = max(
            float(st.session_state.heuristic_p_normal),
            0.999,
        )
        st.sidebar.caption(
            f"Using p={st.session_state.heuristic_p_normal:.3f}; burst-safe "
            f"p={st.session_state.heuristic_p_safe:.3f}."
        )
        with st.sidebar.expander("Heuristic runtime env", expanded=True):
            st.text_input(
                "INGESTOR_REPLAY",
                key="heuristic_ingestor_replay",
                disabled=st.session_state.running,
                help="Use 'arrival' for paced replay, or leave blank to disable replay pacing.",
            )
            st.number_input(
                "REPLAY_SPEED",
                min_value=1, max_value=10000, step=10,
                key="heuristic_replay_speed",
                disabled=st.session_state.running,
            )
            st.checkbox(
                "HEURISTIC_LOCAL_WATERMARK_CLOSE",
                key="heuristic_local_watermark_close",
                disabled=st.session_state.running,
            )
            st.number_input(
                "HEURISTIC_WARMUP_SAMPLES",
                min_value=0, max_value=1000000, step=100,
                key="heuristic_warmup_samples",
                disabled=st.session_state.running,
            )
            st.number_input(
                "HEURISTIC_WARMUP_S",
                min_value=0.0, max_value=3600.0, step=0.5,
                key="heuristic_warmup_s",
                disabled=st.session_state.running,
            )
            st.checkbox(
                "PYTHONUNBUFFERED",
                key="heuristic_python_unbuffered",
                disabled=st.session_state.running,
            )
            env_preview = {
                "PUNCTUATION_MODE": st.session_state.punctuation_mode,
                "HEURISTIC_P_NORMAL": f"{float(st.session_state.heuristic_p_normal):.6g}",
                "HEURISTIC_P_SAFE": f"{float(st.session_state.heuristic_p_safe):.6g}",
                **EXPERIMENT_RUNTIME_ENV,
                **_heuristic_runtime_env_from_state(),
            }
            st.caption("Applied on Start:")
            st.code("\n".join(f"{k}={v}" for k, v in env_preview.items()), language="text")
    else:
        st.sidebar.caption("Heuristic percentile p is available after choosing Heuristic mode.")

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
            if mode == "heuristic":
                st.write(
                    f"Heuristic percentile: **p={st.session_state.heuristic_p_normal:.3f}** "
                    f"(safe p={st.session_state.heuristic_p_safe:.3f})"
                )
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
                    "heuristic_p_normal": float(st.session_state.get("heuristic_p_normal", 0.99)),
                }
            compose_down()
            st.session_state.running = False
            s.update(label="Stopped", state="complete")

    st.sidebar.markdown("---")
    if st.session_state.running:
        st.sidebar.info("Kill Node: use the Dashboard panel or the 🛑 Kill Node / Recover tab.")
    st.sidebar.caption("🟢 **Real-time Auto-refresh enabled.** Stats update every 3 seconds while running.")

    # Service status
    if st.session_state.running:
        st.sidebar.markdown("---")

        infra = _infra()
        if infra:
            st.sidebar.markdown("**Infrastructure**")
            for name in infra:
                ok = (check_node_status(name) == "running")
                st.sidebar.markdown(f"{'🟢' if ok else '🔴'} {name}")

        coords = _coordinators()
        if coords:
            st.sidebar.markdown("**Coordinators**")
            for cname, cport in coords.items():
                ok = (check_node_status(cname, cport) == "running")
                st.sidebar.markdown(f"{'🟢' if ok else '🔴'} {cname} (:{cport})")

            st.sidebar.markdown("**Aggregator**")
            agg_service = "heuristic-aggregator" if _is_sim() else "aggregator"
            ok = (check_node_status(agg_service, _aggregator_port()) == "running")
            st.sidebar.markdown(f"{'🟢' if ok else '🔴'} aggregator (:{_aggregator_port()})")

        st.sidebar.markdown("**Workers**")
        for wname, winfo in _workers().items():
            port = winfo["port"] if isinstance(winfo, dict) else winfo
            ok = (check_node_status(wname, port) == "running")
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
    """Render phần giao diện `render dashboard` lên dashboard hoặc báo cáo."""
    agg = aggregate_worker_metrics(metrics)

    is_sim = _is_sim()
    label = f"{'Sim' if is_sim else 'Full'} — {'Strict' if mode == 'strict' else 'Heuristic'} Watermark"
    worker_count = len(_workers())
    if is_sim:
        st.markdown(f"### {label} (1 worker, 12 partitions)")
    else:
        st.markdown(f"### {label} — Aggregated Metrics ({worker_count} workers, 12 partitions)")

    # Dashboard sub-pages. Keep labels concise so first-time users can scan them.
    tab_names = [
        "📈 Overview",
        "⏱️ Timers & Bottlenecks",
        "🔀 Partitions",
        "🔒 Strict Mode",
        "⚡ Heuristic Mode",
        "🖥️ Resources",
    ]
    selected_page = st.radio(
        "Select Dashboard Page",
        tab_names,
        horizontal=True,
        label_visibility="collapsed",
        key="dashboard_sub_page"
    )

    # -----------------------------------------------------------------------
    # Page 1: Executive Summary
    # -----------------------------------------------------------------------
    if selected_page == "📈 Overview":
        st.markdown("#### System Executive Summary")
        render_dashboard_fault_injection(mode)
        st.markdown("---")
        render_export_panel(mode, metrics, agg)
        st.markdown("---")
        c1, c2, c3, c4, c5, c6, c7, c8 = st.columns(8)
        
        # Calculate completeness delta
        comp_val = agg['data_completeness_pct']
        comp_delta = None
        if len(st.session_state.metrics_history) >= 2:
            prev_comp = st.session_state.metrics_history[-2]["completeness"]
            if prev_comp > 0:
                comp_delta = f"{comp_val - prev_comp:+.2f}%"

        # Calculate throughput
        history = st.session_state.metrics_history
        throughput = 0.0
        if len(history) >= 2:
            time_diff = history[-1]["elapsed_s"] - history[-2]["elapsed_s"]
            recv_diff = history[-1]["total_received"] - history[-2]["total_received"]
            if time_diff > 0:
                throughput = max(0.0, recv_diff / time_diff)

        # Worker alive count
        worker_alive = 0
        if mode == "strict":
            coord = metrics.get("coordinator", {})
            worker_alive = coord.get("active_workers", 0)
        else:
            agg_state = metrics.get("aggregator", {})
            worker_alive = len(metrics.get("workers", {}))

        wlag_max = agg.get("wm_lag_max_s", 0.0)
        data_loss_rate = max(0.0, 100.0 - comp_val)

        c1.metric("Completeness", _fmt_pct(comp_val), delta=comp_delta)
        c2.metric("Data Loss Rate", _fmt_pct(data_loss_rate))
        c3.metric("Throughput", _fmt_rate(throughput, " evt/s"))
        c4.metric("Watermark Lag (max)", _fmt_dur_s(wlag_max))
        c5.metric("Worker Alive", _fmt_count(worker_alive))
        c6.metric("Total Received", _fmt_count(agg['total_received']))
        c7.metric("On-Time", _fmt_count(agg['on_time']))
        c8.metric("Late Dropped", _fmt_count(agg['late_dropped']))

        # Latency Metrics sub-row
        st.markdown("---")
        st.markdown("##### Latency & Lag Details")
        lc1, lc2, lc3, lc4, lc5, lc6 = st.columns(6)
        p50 = agg.get("proc_lat_p50_us", 0.0)
        p95 = agg.get("proc_lat_p95_us", 0.0)
        p99 = agg.get("proc_lat_p99_us", 0.0)
        sp50 = agg.get("sketch_p50_ms", 0.0)
        sp99 = agg.get("sketch_p99_ms", 0.0)
        wlag_avg = agg.get("wm_lag_avg_s", 0.0)

        lc1.metric("Proc Latency p50", _fmt_us(p50))
        lc2.metric("Proc Latency p95", _fmt_us(p95))
        lc3.metric("Proc Latency p99", _fmt_us(p99))
        lc4.metric("Event Lag p50", _fmt_ms(sp50))
        lc5.metric("Event Lag p99", _fmt_ms(sp99))
        lc6.metric("WM Lag (avg)", _fmt_dur_s(wlag_avg))

        if mode == "heuristic":
            _heuristic_kpi_row(metrics, agg)

        st.markdown("---")
        render_high_res_timer_profile(mode, metrics, agg, compact=True)

        # Trend charts
        if len(history) >= 2:
            st.markdown("---")
            st.markdown("##### Aggregated Trends")
            df = pd.DataFrame(history)
            df["time_s"] = df["elapsed_s"].round(0)
            numeric_cols = [c for c in df.select_dtypes(include="number").columns if c != "time_s"]
            df = df.groupby("time_s")[numeric_cols].mean().reset_index()

            ch1, ch2 = st.columns(2)
            with ch1:
                st.markdown("**Completeness %**")
                st.line_chart(df.set_index("time_s")["completeness"], height=200)
            with ch2:
                st.markdown("**Total Events Processed**")
                st.line_chart(df.set_index("time_s")["total_received"], height=200)

            ch3, ch4 = st.columns(2)
            with ch3:
                st.markdown("**Late Dropped (cumulative)**")
                st.line_chart(df.set_index("time_s")["late_dropped"], height=200)
            with ch4:
                st.markdown("**Throughput Trend (events/s)**")
                df_tp = df.copy()
                df_tp["throughput_s"] = df_tp["total_received"].diff() / df_tp["elapsed_s"].diff()
                df_tp["throughput_s"] = df_tp["throughput_s"].fillna(0.0).clip(lower=0.0)
                st.line_chart(df_tp.set_index("time_s")["throughput_s"], height=200)

            if {"proc_p95_us", "proc_p99_us"}.issubset(df.columns):
                st.markdown("**High-resolution processing latency (µs)**")
                latency_cols = ["proc_p95_us", "proc_p99_us"]
                if "bottleneck_p99_us" in df.columns:
                    latency_cols.append("bottleneck_p99_us")
                if "bottleneck_p95_us" in df.columns:
                    latency_cols.append("bottleneck_p95_us")
                st.line_chart(df.set_index("time_s")[latency_cols], height=220)

    # -----------------------------------------------------------------------
    # Page 2: Timers & Bottlenecks
    # -----------------------------------------------------------------------
    elif selected_page == "⏱️ Timers & Bottlenecks":
        st.markdown("#### High-Resolution Timer Profiling")
        render_high_res_timer_profile(mode, metrics, agg, compact=False)

    # -----------------------------------------------------------------------
    # Page 3: Per-Partition Detail
    # -----------------------------------------------------------------------
    elif selected_page == "🔀 Partitions":
        st.markdown("#### Per-Partition Detailed Statistics")
        
        parts = _collect_partition_data(metrics)
        active_parts_count = len(parts)
        total_punctuations = sum(p.get("punctuation_total", 0) for p in parts)
        
        # Calculate watermark global reference
        w_ref = float("-inf")
        if mode == "strict":
            w_ref = metrics.get("coordinator", {}).get("W_global", float("-inf"))
        else:
            w_ref = metrics.get("aggregator", {}).get("W_global_h", float("-inf"))

        pc1, pc2, pc3 = st.columns(3)
        pc1.metric("Active Partitions", _fmt_count(active_parts_count))
        pc2.metric("Total Punctuations", _fmt_count(total_punctuations))

        part_rows = []
        max_skew_ms = 0.0
        for p in sorted(parts, key=lambda x: int(x["partition"])):
            w_val = p["W_h"]
            
            # Watermark skew calculation (local watermark - W_global)
            wm_skew_ms = 0.0
            if w_val != float("-inf") and w_ref != float("-inf"):
                wm_skew_ms = max(0.0, (w_val - w_ref) * 1000.0)
                if wm_skew_ms > max_skew_ms:
                    max_skew_ms = wm_skew_ms

            clk_skew = p.get("clock_skew_ms", 0.0)
            
            # Formulating status
            p_status = "ACTIVE"
            if p.get("replay"):
                p_status = "REPLAY"
            elif p.get("bp_drops", 0) > 0:
                p_status = "BACKPRESSURE"
            elif p.get("negative_lag", {}).get("tier") == "critical":
                p_status = "CRITICAL_LAG"
            bottleneck_stage, bottleneck_p95, _ = _partition_bottleneck(p)

            row = {
                "Partition": p["partition"],
                "Worker": p["worker"],
                "Status": p_status,
                "LW_i (Local WM)": _fmt_watermark(w_val, "-"),
                "WM Skew": _fmt_ms(wm_skew_ms),
                "Clock Skew": _fmt_ms(clk_skew),
                "Proc p95": _fmt_us(p.get("proc_p95", 0.0)),
                "Bottleneck": bottleneck_stage,
                "Bottleneck p99/p95": _fmt_us(bottleneck_p95),
                "BP Drops": _fmt_count(p["bp_drops"]),
                "Punctuation Total": _fmt_count(p.get("punctuation_total", 0)),
                "Received": _fmt_count(p["received"]),
                "On-Time": _fmt_count(p["on_time"]),
                "Late": _fmt_count(p["late"]),
                "Completeness": _fmt_pct(p["completeness"]),
            }
            part_rows.append(row)

        pc3.metric("Max Watermark Skew", _fmt_ms(max_skew_ms))

        if part_rows:
            st.dataframe(pd.DataFrame(part_rows), width="stretch", hide_index=True)
        else:
            st.info("No partition data yet.")

        # Show load imbalance chart
        if parts:
            st.markdown("---")
            st.markdown("##### Partition Load Balancing")
            chart_data = pd.DataFrame([
                {"Partition": p["partition"], "On-Time": p["on_time"], "Late": p["late"]}
                for p in parts
            ])
            st.bar_chart(chart_data.set_index("Partition")[["On-Time", "Late"]], height=240)

    # -----------------------------------------------------------------------
    # Page 4: Strict Specific
    # -----------------------------------------------------------------------
    elif selected_page == "🔒 Strict Mode":
        st.markdown("#### Strict Mode Operational Specifics")
        if mode == "strict":
            sc1, sc2 = st.columns([1, 2])
            with sc1:
                st.markdown("##### Coordinator HA Status")
                coord = metrics.get("coordinator", {})
                wg = coord.get("W_global")
                st.metric("W_global", _fmt_watermark(wg))
                
                st.caption(f"Leader Node: {metrics.get('coordinator_name', '?')}")
                st.caption(f"Raft Term: {_fmt_count(coord.get('term', 0))}")
                st.caption(
                    f"Partitions: {_fmt_count(coord.get('partition_count', 0))} | "
                    f"Workers: {_fmt_count(coord.get('active_workers', 0))}"
                )
                
                skew_ms = coord.get("node_skew_max_ms", 0)
                lag_s = coord.get("watermark_lag_s", 0)
                diag = coord.get("combined_diagnosis", "?")
                skew_status = coord.get("skew_status", "OK")
                lag_status = coord.get("lag_status", "OK")
                combined = coord.get("combined_status", "?")
                st.caption(
                    f"Skew: {_fmt_ms(skew_ms)} ({skew_status}) | "
                    f"Lag: {_fmt_dur_s(lag_s)} ({lag_status})"
                )
                
                status_color = {"Healthy": "green", "Degraded": "orange", "Warning": "orange", "Critical": "red"}
                color = status_color.get(combined, "violet")
                st.markdown(f"**Status:** :{color}[{combined}] — *{diag}*")
                
                fv = coord.get("fencing_violations", 0)
                if fv > 0:
                    st.caption(f"Fencing Violations: {_fmt_count(fv)}")

            with sc2:
                st.markdown("##### MinIO Tiered Storage Eviction States")
                
                # Fetch eviction state distribution and totals
                ts_objs = 0
                ts_bytes = 0
                total_ev_errors = 0
                eviction_counts = {0: 0, 1: 0, 2: 0, 3: 0} # CLOSED, UPLOADING, UPLOADED, PURGED
                
                for wname, wdata in metrics.get("workers", {}).items():
                    m = wdata.get("metrics") or {}
                    total_ev_errors += m.get("tiered_eviction_failure_total", 0)
                    for pdata in m.get("partitions", {}).values():
                        if isinstance(pdata, dict):
                            ev_st = pdata.get("eviction_state", 0)
                            eviction_counts[ev_st] = eviction_counts.get(ev_st, 0) + 1
                            if "tier_storage" in pdata:
                                ts_stats = pdata["tier_storage"]
                                ts_objs += ts_stats.get("objects", 0)
                                ts_bytes += ts_stats.get("bytes", 0)

                tc1, tc2, tc3 = st.columns(3)
                tc1.metric("Objects in MinIO", _fmt_count(ts_objs))
                tc2.metric("Size in MinIO", _fmt_bytes(ts_bytes))
                tc3.metric("Upload Errors", _fmt_count(total_ev_errors))

                # Eviction state distribution table
                state_labels = {0: "CLOSED", 1: "UPLOADING", 2: "UPLOADED", 3: "PURGED"}
                dist_rows = [
                    {"Eviction State": state_labels[k], "Window Count": v}
                    for k, v in eviction_counts.items()
                ]
                st.dataframe(pd.DataFrame(dist_rows), width="stretch", hide_index=True)

                # Show partition map reassignments
                st.markdown("##### Partition Maps & Recovery Details")
                partition_types = coord.get("partition_types", {})
                recovery_info = coord.get("recovery_info", {})
                if partition_types or recovery_info:
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
                    st.dataframe(pd.DataFrame(coord_rows), width="stretch", hide_index=True)
                else:
                    st.caption("No partition reassignments or recovery events.")
        else:
            st.info("Strict-specific metrics are only available in Strict mode.")

    # -----------------------------------------------------------------------
    # Page 5: Heuristic Specific
    # -----------------------------------------------------------------------
    elif selected_page == "⚡ Heuristic Mode":
        st.markdown("#### Heuristic Mode Operational Specifics")
        if mode == "heuristic":
            hc1, hc2 = st.columns([1, 2])
            with hc1:
                st.markdown("##### Aggregator HA Status")
                agg_state = metrics.get("aggregator", {})
                wgh = agg_state.get("W_global_h")
                st.metric("W_global_h", _fmt_watermark(wgh))

                parts = _collect_partition_data(metrics)
                p_vals = [float(p["p_current"]) for p in parts if p.get("p_current")]
                leff_vals = [float(p["L_eff_s"]) for p in parts if p.get("L_eff_s", 0) > 0]
                st.metric("Percentile p", f"{max(p_vals):.3f}" if p_vals else "-")
                st.metric("Avg L_eff", _fmt_dur_s(sum(leff_vals) / len(leff_vals)) if leff_vals else "-")
                
                st.caption(f"Active partitions: {_fmt_count(agg_state.get('active_count', 0))}")
                st.caption(f"Active Aggregator: {agg_state.get('ha_active', True)}")
                st.caption(f"Standby Takeovers (Failovers): {_fmt_count(agg_state.get('ha_failover_count', 0))}")
                wlag = agg_state.get("watermark_lag_s", 0)
                if wlag and wlag != float("inf"):
                    st.caption(f"Watermark lag: {_fmt_dur_s(wlag)}")
                
                # Ingestor clock skews from leader health monitor
                st.markdown("##### Ingestor Health & Skew")
                ing_data = []
                coord = metrics.get("coordinator", {})
                ih = coord.get("ingestor_health", {}) if isinstance(coord, dict) else {}
                ing_dict = ih.get("ingestors", {}) if isinstance(ih, dict) else {}
                for ing_id, ing_val in ing_dict.items():
                    ing_data.append({
                        "Ingestor": ing_id,
                        "Status": ing_val.get("status", "unknown"),
                        "Clock Skew": _fmt_ms(ing_val.get("clock_skew_ms", 0.0)),
                        "Heartbeat Lag": _fmt_dur_s(ing_val.get("last_heartbeat_s", 0.0)),
                    })
                if ing_data:
                    st.dataframe(pd.DataFrame(ing_data), width="stretch", hide_index=True)
                else:
                    st.caption("No ingestor health data available.")

            with hc2:
                st.markdown("##### DLQ Pipeline & DDSketch Details")
                
                # Fetch DLQ status and quantiles
                dlq_backlog = agg.get("dlq_backlog", 0)
                extreme_lag = agg.get("extreme_lag_count", 0)
                
                # Calculate SLA compliance from worker metrics
                sla_compliant = 100.0
                for wname, wdata in metrics.get("workers", {}).items():
                    m = wdata.get("metrics") or {}
                    if "sla_compliant_pct" in m:
                        sla_compliant = min(sla_compliant, m.get("sla_compliant_pct", 100.0))

                dc1, dc2, dc3 = st.columns(3)
                dc1.metric("DLQ Backlog", _fmt_count(dlq_backlog))
                dc2.metric("Extreme Lag Count", _fmt_count(extreme_lag))
                dc3.metric("SLA Compliance", _fmt_pct(sla_compliant))

                # Quantiles
                st.markdown("##### DDSketch Event Lag Quantiles")
                qc1, qc2, qc3 = st.columns(3)
                qc1.metric("Lag p50", _fmt_ms(agg.get("sketch_p50_ms", 0.0)))
                qc2.metric("Lag p95", _fmt_ms(agg.get("sketch_p95_ms", 0.0)))
                qc3.metric("Lag p99", _fmt_ms(agg.get("sketch_p99_ms", 0.0)))

                # Show per-window loss accounting
                loss_entries = _collect_per_window_loss(metrics)
                if loss_entries:
                    st.markdown("##### Per-Window Loss Accounting")
                    st.dataframe(pd.DataFrame(loss_entries), width="stretch", hide_index=True)
                else:
                    st.info("No window loss records yet.")
        else:
            st.info("Heuristic-specific metrics are only available in Heuristic mode.")

    # -----------------------------------------------------------------------
    # Page 6: Resources
    # -----------------------------------------------------------------------
    elif selected_page == "🖥️ Resources":
        st.markdown("#### Infrastructure & Node System Resources")
        
        resource_rows = []
        for wname, wdata in metrics.get("workers", {}).items():
            m = wdata.get("metrics") or {}
            res = m.get("resources", {})
            if res:
                ram_used = res.get("ram_used_bytes", 0)
                ram_total = res.get("ram_total_bytes", 0)
                disk_used = res.get("disk_used_bytes", 0)
                disk_total = res.get("disk_total_bytes", 0)
                
                ram_pct = (ram_used / ram_total * 100.0) if ram_total else 0.0
                disk_pct = (disk_used / disk_total * 100.0) if disk_total else 0.0
                
                resource_rows.append({
                    "Worker Node": wname,
                    "RAM Used": _fmt_bytes(ram_used),
                    "RAM Total": _fmt_bytes(ram_total),
                    "RAM Usage": _fmt_pct(ram_pct, 1),
                    "Disk Used": _fmt_bytes(disk_used),
                    "Disk Total": _fmt_bytes(disk_total),
                    "Disk Usage": _fmt_pct(disk_pct, 1),
                })
        if resource_rows:
            st.dataframe(pd.DataFrame(resource_rows), width="stretch", hide_index=True)
        else:
            st.info("No system resource metrics available from worker nodes.")


# ---------------------------------------------------------------------------
# UI — Simulation Statistics
# ---------------------------------------------------------------------------

def render_sim_stats(mode: str, metrics: dict):
    """Render phần giao diện `render sim stats` lên dashboard hoặc báo cáo."""
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
    dataset_file = st.session_state.get("dataset_file", "nyc_taxi_events_full.csv")
    total_rows = 0
    datasets = get_datasets()
    ds_path = datasets.get(dataset_file, "")
    if ds_path:
        total_rows = count_csv_rows(ds_path)

    # Event rate calculation
    if st.session_state.start_time and total_recv > 0:
        elapsed = time.time() - st.session_state.start_time
        rate = total_recv / max(elapsed, 1)
        elapsed_str = _fmt_dur_s(elapsed)
    else:
        rate = 0
        elapsed_str = "0s"

    # Row 1: Replay Progress KPIs
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Elapsed Time", elapsed_str)
    c2.metric("Event Rate", _fmt_rate(rate, " ev/s"))
    c3.metric("Total Processed", _fmt_count(total_recv))
    if total_rows > 0:
        pct = 100.0 * total_recv / total_rows
        c4.metric("Dataset Progress", _fmt_pct(pct, 1))
        if rate > 0:
            remaining = (total_rows - total_recv) / rate
            c5.metric("ETA", _fmt_dur_s(remaining))
        else:
            c5.metric("ETA", "-")
    else:
        c4.metric("Dataset Progress", "?")
        c5.metric("ETA", "?")

    # Row 2: Quality & Compliance KPIs
    st.markdown("#### Quality & Compliance KPIs")
    q1, q2, q3, q4, q5, q6 = st.columns(6)

    unique_events = max(total_recv - total_dupes, 1)
    completeness_pct = 100.0 * total_on / unique_events
    late_rate_pct = 100.0 * total_late / max(total_recv, 1)

    q1.metric("Completeness", _fmt_pct(completeness_pct))
    q2.metric("Late Arrival", _fmt_pct(late_rate_pct))
    q3.metric("Duplicates", _fmt_count(total_dupes))
    q4.metric("Backpressure Drops", _fmt_count(total_bp))
    q5.metric("DLQ Backlog", _fmt_count(total_dlq))

    if mode == "strict":
        q6.metric("Fencing Violations", _fmt_count(total_fencing))
    else:
        q6.metric("Non-Mono Punctuation", _fmt_count(total_non_mono))

    st.markdown("---")

    # Interactive tabs inside stats
    tab_names = ["Partition Metrics", "High-Res Timers & Bottlenecks", "Load Balance & Skew"]
    selected_tab = st.radio(
        "Select Stat Tab",
        tab_names,
        horizontal=True,
        label_visibility="collapsed",
        key="sim_stats_active_tab"
    )

    if selected_tab == "Partition Metrics":
        st.markdown("#### Per-Partition Detailed Statistics")
        tbl = []
        for p in sorted(parts, key=lambda x: int(x["partition"])):
            w_val = p["W_h"]
            row = {
                "Partition": p["partition"],
                "Worker": p["worker"],
                "Received": _fmt_count(p["received"]),
                "On-Time": _fmt_count(p["on_time"]),
                "Late": _fmt_count(p["late"]),
                "Completeness": _fmt_pct(p["completeness"]),
                "Duplicates": _fmt_count(p["dupes"]),
                "BP Drops": _fmt_count(p["bp_drops"]),
                "DLQ": _fmt_count(p["dlq"]),
                "Watermark": _fmt_watermark(w_val, "-"),
                "Open Windows": _fmt_count(p["open_windows"]),
                "Closed Windows": _fmt_count(p["closed_windows"]),
            }
            if mode == "strict":
                row["Dedup TTL"] = _fmt_count(p["dedup_ttl"])
                row["Fencing Violations"] = _fmt_count(p["fencing_violations"])
            else:
                row["Replay Mode"] = "Active" if p["replay"] else "Normal"
            tbl.append(row)
        st.dataframe(pd.DataFrame(tbl), width="stretch", hide_index=True)

    elif selected_tab == "High-Res Timers & Bottlenecks":
        st.markdown("#### Per-Partition High-Resolution Timers")
        st.caption("Raw timers are stored in nanoseconds and summarized here as microsecond p50/p95/p99 values.")
        timer_tbl = []
        for p in sorted(parts, key=lambda x: int(x["partition"])):
            bottleneck_stage, bottleneck_value, bottleneck_hint = _partition_bottleneck(p)
            row = {
                "Partition": p["partition"],
                "Worker": p["worker"],
                "Bottleneck": bottleneck_stage,
                "Bottleneck p99/p95": _fmt_us(bottleneck_value),
                "Diagnosis": bottleneck_hint,
                "Proc p50": _fmt_us(p["proc_p50"]),
                "Proc p95": _fmt_us(p["proc_p95"]),
                "Proc p99": _fmt_us(p["proc_p99"]),
                "Network p99": _fmt_us(p["network_ingest_p99"]),
                "Poll Decode p99": _fmt_us(p["poll_decode_p99"]),
                "Dedup p99": _fmt_us(p["dedup_p99"]),
                "State Write p99": _fmt_us(p["state_write_p99"]),
                "Sketch Update p99": _fmt_us(p["sketch_update_p99"]),
                "Sketch Query p99": _fmt_us(p["sketch_query_p99"]),
            }
            if mode == "heuristic":
                row["Lag p99"] = _fmt_ms(p["sketch_p99"])
                row["Negative Lag Rate"] = _fmt_pct(p["neg_lag_rate"] * 100, 3)
            else:
                row["Dedup TTL Entries"] = _fmt_count(p["dedup_ttl"])
                row["Fencing Violations"] = _fmt_count(p["fencing_violations"])
            timer_tbl.append(row)
        st.dataframe(pd.DataFrame(timer_tbl), width="stretch", hide_index=True)

    elif selected_tab == "Load Balance & Skew":
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
                st.metric("Max/Avg Load Ratio", _fmt_ratio(skew_ratio))
                st.markdown(f"""
                - **Min events per partition:** {_fmt_count(min(recv_vals))}
                - **Max events per partition:** {_fmt_count(max(recv_vals))}
                - **Standard Deviation:** {_fmt_count(round(pd.Series(recv_vals).std(), 0))}
                """)

                # Chunk boundary effects
                parts_sorted = sorted(parts, key=lambda x: int(x["partition"]))
                min_recv_parts = [p for p in parts_sorted if p["received"] == min(recv_vals)]
                max_recv_parts = [p for p in parts_sorted if p["received"] == max(recv_vals)]
                st.caption(f"Least-loaded partitions: {', '.join(p['partition'] for p in min_recv_parts)} ({_fmt_count(min(recv_vals))} events)")
                st.caption(f"Most-loaded partitions: {', '.join(p['partition'] for p in max_recv_parts)} ({_fmt_count(max(recv_vals))} events)")
                spread = (max(recv_vals) - min(recv_vals)) / max(recv_vals) * 100
                st.caption(f"Load Spread: {_fmt_pct(spread, 1)} (caused by hash rounding at chunk boundaries)")


# ---------------------------------------------------------------------------
# Export / download helpers
# ---------------------------------------------------------------------------

def _ts_suffix() -> str:
    """Hàm `_ts_suffix` thực hiện phần xử lý liên quan đến ts suffix."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def render_export_panel(mode: str, metrics: dict, agg: dict):
    """Render phần giao diện `render export panel` lên dashboard hoặc báo cáo.
    
    Ghi chú gốc:
    Download buttons for the current snapshot: aggregated KPIs (JSON/CSV),
        per-partition table (CSV), and the time-series history (CSV).
    """
    st.markdown("##### 💾 Export current snapshot")
    e1, e2, e3, e4 = st.columns(4)

    # Aggregated KPIs
    agg_export = dict(agg)
    bottleneck = _current_bottleneck_snapshot(metrics, mode, agg)
    parts = _collect_partition_data(metrics)
    p_vals = [float(p["p_current"]) for p in parts if p.get("p_current")]
    leff_vals = [float(p["L_eff_s"]) for p in parts if p.get("L_eff_s", 0) > 0]
    agg_export.update({
        "mode": mode,
        "exported_at": datetime.now().isoformat(),
        "heuristic_p_normal": float(st.session_state.get("heuristic_p_normal", 0.99)),
        "heuristic_p_current": max(p_vals) if p_vals else None,
        "heuristic_l_eff_avg_s": round(sum(leff_vals) / len(leff_vals), 3) if leff_vals else None,
        "timer_source": "time.perf_counter_ns",
        "bottleneck_stage": bottleneck["stage"],
        "bottleneck_stage_p95_us": bottleneck["stage_p95_us"],
        "bottleneck_stage_p99_us": bottleneck["stage_p99_us"],
        "bottleneck_worker": bottleneck["worker"],
        "bottleneck_partition": bottleneck["partition"],
    })
    e1.download_button(
        "Aggregated KPIs (JSON)",
        data=json.dumps(agg_export, indent=2, default=str),
        file_name=f"kpi_{mode}_{_ts_suffix()}.json",
        mime="application/json", width="stretch", key="dl_agg_json",
    )
    e2.download_button(
        "Aggregated KPIs (CSV)",
        data=pd.DataFrame([agg_export]).to_csv(index=False),
        file_name=f"kpi_{mode}_{_ts_suffix()}.csv",
        mime="text/csv", width="stretch", key="dl_agg_csv",
    )

    # Per-partition table
    if parts:
        pdf = pd.DataFrame([
            {k: v for k, v in p.items() if not isinstance(v, dict)}
            for p in parts
        ])
        e3.download_button(
            "Per-Partition (CSV)",
            data=pdf.to_csv(index=False),
            file_name=f"partitions_{mode}_{_ts_suffix()}.csv",
            mime="text/csv", width="stretch", key="dl_parts_csv",
        )
    else:
        e3.button("Per-Partition (CSV)", disabled=True, width="stretch", key="dl_parts_disabled")

    # Time-series history
    history = st.session_state.get("metrics_history", [])
    if history:
        e4.download_button(
            "Time-Series History (CSV)",
            data=pd.DataFrame(history).to_csv(index=False),
            file_name=f"history_{mode}_{_ts_suffix()}.csv",
            mime="text/csv", width="stretch", key="dl_hist_csv",
        )
    else:
        e4.button("Time-Series History (CSV)", disabled=True, width="stretch", key="dl_hist_disabled")


# ---------------------------------------------------------------------------
# UI — Completeness % vs Wait Time (deliverable §112)
# ---------------------------------------------------------------------------

def _record_lab_point(mode: str, metrics: dict, agg: dict, note: str = ""):
    """Hàm `_record_lab_point` thực hiện phần xử lý liên quan đến record lab point.
    
    Ghi chú gốc:
    Append the current achieved (wait_time, completeness, latency) as a data
        point for the Completeness-vs-Wait-Time deliverable.
    """
    parts = _collect_partition_data(metrics)
    p_vals = [float(p["p_current"]) for p in parts if p.get("p_current")]
    leff_vals = [float(p["L_eff_s"]) for p in parts if p.get("L_eff_s", 0) > 0]
    point = {
        "mode": mode,
        "wait_time_s": float(st.session_state.get("delta_base_s", 10.0)),
        "heuristic_p_config": float(st.session_state.get("heuristic_p_normal", 0.99)) if mode == "heuristic" else 0.0,
        "heuristic_p_current": max(p_vals) if p_vals else 0.0,
        "heuristic_l_eff_avg_s": round(sum(leff_vals) / len(leff_vals), 3) if leff_vals else 0.0,
        "window_size_s": 5.0,
        "completeness_pct": round(agg.get("data_completeness_pct", 0.0), 3),
        "late_rate_pct": round(agg.get("late_arrival_rate_pct", 0.0), 3),
        "total_received": agg.get("total_received", 0),
        "on_time": agg.get("on_time", 0),
        "late_dropped": agg.get("late_dropped", 0),
        "wm_lag_max_s": round(agg.get("wm_lag_max_s", 0.0), 3),
        "proc_lat_p99_us": round(agg.get("proc_lat_p99_us", 0.0), 1),
        "event_lag_p95_ms": round(agg.get("sketch_p95_ms", 0.0), 1),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "note": note,
    }
    st.session_state.lab_points.append(point)


def render_completeness_vs_wait():
    """Render phần giao diện `render completeness vs wait` lên dashboard hoặc báo cáo."""
    st.markdown("### 📐 Data Completeness % vs Wait Time (ms)")
    st.caption(
        "Core deliverable (topic #112): the trade-off between **Wait Time** "
        "(watermark delay δ) and **Data Completeness %**. Set a Wait Time δ in the "
        "sidebar, run the pipeline, let it stabilise, then record a data point. "
        "Sweep δ across several runs (strict vs heuristic) to build the curve."
    )

    mode = st.session_state.mode
    metrics = st.session_state.get("last_metrics") or {}
    agg = aggregate_worker_metrics(metrics) if metrics.get("workers") else {}

    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        if st.session_state.running and agg:
            heuristic_bits = ""
            if mode == "heuristic":
                parts = _collect_partition_data(metrics)
                p_vals = [float(p["p_current"]) for p in parts if p.get("p_current")]
                p_current = max(p_vals) if p_vals else float(st.session_state.get("heuristic_p_normal", 0.99))
                heuristic_bits = f" · p={p_current:.3f}"
            st.success(
                f"Current run — δ={st.session_state.get('delta_base_s', 10.0):.0f}s · "
                f"mode={mode}{heuristic_bits} · completeness={agg.get('data_completeness_pct', 0.0):.2f}% · "
                f"late={agg.get('late_arrival_rate_pct', 0.0):.2f}%"
            )
        else:
            st.info("Start a run to capture a live data point (or add a manual one).")
    with c2:
        note = st.text_input("Note (optional)", key="lab_note", placeholder="e.g. burst load")
    with c3:
        st.markdown("&nbsp;")
        if st.button("➕ Record data point", type="primary", width="stretch",
                     disabled=not (st.session_state.running and agg)):
            _record_lab_point(mode, metrics, agg, note)
            st.toast("Data point recorded.", icon="📐")

    # Manual entry (for offline / external runs)
    with st.expander("Add a manual data point"):
        m1, m2, m3, m4 = st.columns(4)
        man_mode = m1.selectbox("Mode", ["strict", "heuristic"], key="lab_man_mode")
        man_wait = m2.number_input("Wait Time δ (s)", min_value=0.0, value=10.0, step=1.0, key="lab_man_wait")
        man_comp = m3.number_input("Completeness %", min_value=0.0, max_value=100.0, value=100.0, step=0.1, key="lab_man_comp")
        man_late = m4.number_input("Late rate %", min_value=0.0, max_value=100.0, value=0.0, step=0.1, key="lab_man_late")
        if st.button("Add manual point", key="lab_man_add"):
            st.session_state.lab_points.append({
                "mode": man_mode, "wait_time_s": man_wait, "window_size_s": 5.0,
                "completeness_pct": man_comp, "late_rate_pct": man_late,
                "total_received": 0, "on_time": 0, "late_dropped": 0,
                "wm_lag_max_s": 0.0, "proc_lat_p99_us": 0.0, "event_lag_p95_ms": 0.0,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "note": "manual",
            })
            st.toast("Manual point added.", icon="✍️")

    points = st.session_state.get("lab_points", [])
    if not points:
        st.info("No data points yet. Record at least two (varying δ) to see the trade-off curve.")
        return

    df = pd.DataFrame(points)
    st.markdown("#### Recorded data points")
    st.dataframe(df, width="stretch", hide_index=True)

    dl1, dl2 = st.columns([1, 4])
    dl1.download_button(
        "⬇️ Download CSV", data=df.to_csv(index=False),
        file_name=f"completeness_vs_wait_{_ts_suffix()}.csv",
        mime="text/csv", width="stretch", key="dl_lab_csv",
    )
    if dl2.button("🗑️ Clear all points", key="lab_clear"):
        st.session_state.lab_points = []
        st.rerun()

    # Trade-off curve: completeness vs wait time, one line per mode
    st.markdown("#### Trade-off curve")
    try:
        chart_df = (
            df.groupby(["mode", "wait_time_s"])["completeness_pct"]
            .mean().reset_index()
            .pivot(index="wait_time_s", columns="mode", values="completeness_pct")
            .sort_index()
        )
        cc1, cc2 = st.columns(2)
        with cc1:
            st.markdown("**Completeness % vs Wait Time (s)**")
            st.line_chart(chart_df, height=260)
        with cc2:
            lat_df = (
                df.groupby(["mode", "wait_time_s"])["late_rate_pct"]
                .mean().reset_index()
                .pivot(index="wait_time_s", columns="mode", values="late_rate_pct")
                .sort_index()
            )
            st.markdown("**Late Rate % vs Wait Time (s)**")
            st.line_chart(lat_df, height=260)
    except Exception as e:
        st.warning(f"Need more varied data points to plot the curve. ({e})")


# ---------------------------------------------------------------------------
# UI — Logs
# ---------------------------------------------------------------------------

def render_logs():
    """Render phần giao diện `render logs` lên dashboard hoặc báo cáo."""
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
            st.download_button(
                "⬇️ Download logs (.log)",
                data=logs,
                file_name=f"logs_{service or 'all'}_{_ts_suffix()}.log",
                mime="text/plain", key="dl_logs",
            )
            st.code(logs, language="log")
        else:
            st.info("No logs yet — containers may still be starting, or filter excludes everything.")
    else:
        st.info("Start the system to see logs.")


# ---------------------------------------------------------------------------
# UI — Compare
# ---------------------------------------------------------------------------

def render_comparison():
    """Render phần giao diện `render comparison` lên dashboard hoặc báo cáo."""
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
            st.caption(f"Run: {data.get('timestamp', '?')} | Duration: {_fmt_dur_s(dur)}")
            if mode == "heuristic":
                st.caption(f"Percentile p: {data.get('heuristic_p_normal', 0.99):.3f}")
            st.metric("Completeness", _fmt_pct(a["data_completeness_pct"]))
            st.metric("Total Events", _fmt_count(a["total_received"]))
            st.metric("On-Time", _fmt_count(a["on_time"]))
            st.metric("Late Dropped", _fmt_count(a["late_dropped"]))
            st.metric("Late Rate", _fmt_pct(a["late_arrival_rate_pct"]))
            st.metric("Duplicates", _fmt_count(a["duplicates"]))
            st.metric("BP Drops", _fmt_count(a["backpressure_drops"]))

    if len(results) == 2 and "strict" in results and "heuristic" in results:
        st.markdown("---")
        st.markdown("#### Side-by-Side")
        sa = results["strict"]["agg"]
        ha = results["heuristic"]["agg"]
        df = pd.DataFrame({
            "Metric": ["Completeness %", "Late Rate %", "Events Received",
                       "On-Time", "Late Dropped", "Duplicates", "BP Drops",
                       "Non-Mono Punctuation"],
            "Strict": [_fmt_pct(sa["data_completeness_pct"]), _fmt_pct(sa["late_arrival_rate_pct"]),
                       _fmt_count(sa["total_received"]), _fmt_count(sa["on_time"]), _fmt_count(sa["late_dropped"]),
                       _fmt_count(sa["duplicates"]), _fmt_count(sa["backpressure_drops"]),
                       _fmt_count(sa.get("non_monotonic_punctuation", 0))],
            "Heuristic": [_fmt_pct(ha["data_completeness_pct"]), _fmt_pct(ha["late_arrival_rate_pct"]),
                          _fmt_count(ha["total_received"]), _fmt_count(ha["on_time"]), _fmt_count(ha["late_dropped"]),
                          _fmt_count(ha["duplicates"]), _fmt_count(ha["backpressure_drops"]),
                          _fmt_count(ha.get("non_monotonic_punctuation", 0))],
        })
        st.dataframe(df, width="stretch", hide_index=True)


# ---------------------------------------------------------------------------
# UI — Raw JSON
# ---------------------------------------------------------------------------

def render_raw(metrics: dict):
    """Render phần giao diện `render raw` lên dashboard hoặc báo cáo."""
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

def fetch_all_container_statuses(mode: str, deploy_mode: str = None) -> dict[str, str]:
    """Lấy dữ liệu `fetch all container statuses` từ service, cache hoặc nguồn bên ngoài.
    
    Ghi chú gốc:
    Fetch the status of all containers in a single docker compose ps call.
        Returns a dict mapping docker service name to status ('running', 'stopped', 'unknown').
    """
    statuses = {}
    cmd = _compose_base(deploy_mode) + [
        "--profile", "strict",
        "--profile", "heuristic",
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
    """Trả về thông tin `container status docker` từ trạng thái hiện tại.
    
    Ghi chú gốc:
    Check the container status using cached status map or fallback to docker compose ps.
    """
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
    """Kiểm tra điều kiện `check node status` và trả về kết quả đánh giá.
    
    Ghi chú gốc:
    Combined health status using Docker state lookup and network check fallback.
    """
    # Check pre-calculated background cache first
    cached_statuses = st.session_state.get("node_statuses")
    if cached_statuses and service in cached_statuses:
        return cached_statuses[service]

    # Fallback to live check if cache is not available
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
    """Hàm `_container_id` thực hiện phần xử lý liên quan đến container id.
    
    Ghi chú gốc:
    Return the container id for a compose service, or '' if not found.
    """
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
    """Hàm `_set_restart_policy` thực hiện phần xử lý liên quan đến set restart policy.
    
    Ghi chú gốc:
    Override a container's restart policy (e.g. 'no' or 'unless-stopped').
    
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
    """Hàm `compose_start_service` thực hiện phần xử lý liên quan đến compose start service."""
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
    """Hàm `compose_stop_service` thực hiện phần xử lý liên quan đến compose stop service."""
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
    """Hàm `compose_kill_service` thực hiện phần xử lý liên quan đến compose kill service."""
    _set_restart_policy(service, "no")
    cmd = _compose_base() + ["kill", service]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8",
                           errors="replace", cwd=str(DEPLOY_DIR), timeout=30)
        return r.returncode == 0, r.stdout + r.stderr
    except Exception as e:
        return False, str(e)


# ---------------------------------------------------------------------------
# Live leader / active-node detection
# ---------------------------------------------------------------------------
# The Raft coordinator leader and the heuristic aggregator's active node are
# decided at runtime, not by container name. Killing "coordinator-1" assuming it
# is the leader can hit a follower and produce a misleading failover test (the
# cluster keeps a real leader, so nothing visibly fails over). These helpers
# probe the live /state endpoints so the UI labels — and therefore the kill
# target the operator picks — track the real leader/active node.
#
# Results are cached briefly in a module-level dict (thread-safe) so the probe
# runs at most once per _ROLE_CACHE_TTL_S even though it is consulted from both
# the Streamlit main thread and the background metrics-fetch thread.

_ROLE_CACHE_TTL_S = 2.0
_ROLE_CACHE_LOCK = threading.Lock()
_ROLE_CACHE = {
    "strict": {"t": 0.0, "roles": {}, "term": None},
    "agg": {"t": 0.0, "active": None, "reachable": {}},
}


def _detect_strict_roles(deploy_mode: str = None) -> tuple[dict, object]:
    """Probe each coordinator's /state and return ({service: raft_role}, term).

    raft_role is one of 'leader' / 'follower' / 'candidate' / 'down'. When no
    coordinator self-reports 'leader' but a peer names one via `raft_leader`
    (``coordinator-N:8000``), that node is marked leader as a fallback.
    """
    now = time.time()
    with _ROLE_CACHE_LOCK:
        c = _ROLE_CACHE["strict"]
        if now - c["t"] < _ROLE_CACHE_TTL_S and c["roles"]:
            return dict(c["roles"]), c["term"]

    coords = _coordinators(deploy_mode)
    states = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(coords))) as ex:
        futs = {
            ex.submit(_get_json, f"http://localhost:{cport}/state", 1.0): cname
            for cname, cport in coords.items()
        }
        for fut in concurrent.futures.as_completed(futs):
            states[futs[fut]] = (fut.result() if not fut.exception() else None)

    roles = {}
    term = None
    leader_hint = None
    for cname in coords:
        state = states.get(cname)
        if not state:
            roles[cname] = "down"
            continue
        roles[cname] = str(state.get("raft_role", "") or "follower").lower()
        if state.get("term") is not None:
            term = state.get("term")
        rl = state.get("raft_leader") or ""
        if rl:
            leader_hint = str(rl).split(":")[0]

    if "leader" not in roles.values() and leader_hint in roles and roles.get(leader_hint) != "down":
        roles[leader_hint] = "leader"

    with _ROLE_CACHE_LOCK:
        _ROLE_CACHE["strict"] = {"t": now, "roles": dict(roles), "term": term}
    return roles, term


def _detect_active_aggregator(deploy_mode: str = None) -> object:
    """Probe both aggregators' /state and return the service name of the ACTIVE
    (leader) node, or None if it cannot be determined.

    AggregatorHA reports ``ha_active=true`` on whichever node currently holds the
    lock; after a failover the standby container becomes active, so the role
    cannot be inferred from the container name.
    """
    now = time.time()
    with _ROLE_CACHE_LOCK:
        a = _ROLE_CACHE["agg"]
        if now - a["t"] < _ROLE_CACHE_TTL_S and a["reachable"]:
            return a["active"]

    ports = {
        "aggregator": _aggregator_port(deploy_mode),
        "aggregator-standby": AGGREGATOR_STANDBY_PORT_FULL,
    }
    states = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
        futs = {
            ex.submit(_get_json, f"http://localhost:{port}/state", 1.0): svc
            for svc, port in ports.items()
        }
        for fut in concurrent.futures.as_completed(futs):
            states[futs[fut]] = (fut.result() if not fut.exception() else None)

    active = None
    reachable = {}
    for svc in ports:
        state = states.get(svc)
        reachable[svc] = state is not None
        if state and state.get("ha_active"):
            active = svc
    # Fallback: if exactly one is reachable, it is necessarily carrying the load.
    if active is None:
        up = [s for s, ok in reachable.items() if ok]
        if len(up) == 1:
            active = up[0]

    with _ROLE_CACHE_LOCK:
        _ROLE_CACHE["agg"] = {"t": now, "active": active, "reachable": reachable}
    return active


def _node_control_groups(mode: str, deploy_mode: str = None,
                         detect_roles: bool = True) -> list[tuple[str, list[dict]]]:
    """Hàm `_node_control_groups` thực hiện phần xử lý liên quan đến node control groups.
    
    Ghi chú gốc:
    Single source of truth for the controllable services in the current
        deploy/watermark mode. Returns [(group_header, [node, ...]), ...] where each
        node is {service, display, port, role}.
    """
    def n(service, display, port, role):
        """Hàm `n` thực hiện phần xử lý liên quan đến n."""
        return {"service": service, "display": display, "port": port, "role": role}

    if _is_sim(deploy_mode):
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
        roles, term = ({}, None)
        if detect_roles:
            try:
                roles, term = _detect_strict_roles(deploy_mode)
            except Exception:
                roles, term = {}, None

        def _coord_role(cname: str) -> str:
            """Label a coordinator by its LIVE raft role so the operator can kill
            the real leader instead of assuming coordinator-1."""
            r = roles.get(cname)
            if r == "leader":
                return f"Coordinator Leader · term {term}" if term is not None else "Coordinator Leader"
            if r == "candidate":
                return "Coordinator Candidate (election in progress)"
            if r == "follower":
                return "Coordinator Follower"
            if r == "down":
                return "Coordinator (unreachable)"
            return "Coordinator"

        core = ("Core Cluster", [
            n("coordinator-1", "Coordinator 1", 9000, _coord_role("coordinator-1")),
            n("coordinator-2", "Coordinator 2", 9003, _coord_role("coordinator-2")),
            n("coordinator-3", "Coordinator 3", 9004, _coord_role("coordinator-3")),
        ])
    else:
        active = None
        if detect_roles:
            try:
                active = _detect_active_aggregator(deploy_mode)
            except Exception:
                active = None

        def _agg_role(svc: str, fallback: str) -> str:
            """Label aggregators by LIVE HA state (ha_active) rather than name, so
            killing the 'leader' targets the node actually carrying the load."""
            if active is None:
                return fallback
            return "Aggregator Active (Leader)" if svc == active else "Aggregator Standby"

        core = ("Core Cluster", [
            n("aggregator", "Aggregator Primary", 9007, _agg_role("aggregator", "Aggregator Leader")),
            n("aggregator-standby", "Aggregator Standby", 9005, _agg_role("aggregator-standby", "Aggregator Standby")),
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


def fetch_node_health_statuses(mode: str, deploy_mode: str = None, container_statuses: dict[str, str] = None) -> dict[str, str]:
    """Lấy dữ liệu `fetch node health statuses` từ service, cache hoặc nguồn bên ngoài.
    
    Ghi chú gốc:
    Calculate the health status ('running', 'stopped', 'unknown') of all services.
        Runs HTTP/TCP checks in parallel using a ThreadPoolExecutor.
    """
    if container_statuses is None:
        container_statuses = {}
    node_statuses = {}
    
    # Single source of truth for controllable services. Role labels are not
    # needed for health checks, so skip the live leader probe here.
    groups = _node_control_groups(mode, deploy_mode, detect_roles=False)
    flat_nodes = [node for _, nodes in groups for node in nodes]
    
    def check_node(node):
        """Kiểm tra điều kiện `check node` và trả về kết quả đánh giá."""
        service = node["service"]
        port = node["port"]
        
        # 1. Get docker status from container_statuses dict
        docker_status = container_statuses.get(service, "unknown")
        if docker_status == "stopped":
            return service, "stopped"
            
        # 2. Network/health check fallback if running in docker
        infra_key = _infra_key(service, deploy_mode)
        if infra_key is not None:
            if check_infra_health(infra_key, deploy_mode):
                return service, "running"
            return service, docker_status if docker_status != "unknown" else "stopped"
            
        if port:
            if is_healthy(port):
                return service, "running"
            return service, "stopped"
            
        return service, docker_status

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(flat_nodes))) as executor:
        results = executor.map(check_node, flat_nodes)
        for service, status in results:
            node_statuses[service] = status
            
    return node_statuses


def bg_fetch_job(shared_state, mode, deploy_mode):
    """Hàm `bg_fetch_job` thực hiện phần xử lý liên quan đến bg fetch job."""
    try:
        metrics = fetch_all_metrics(mode, deploy_mode)
        container_statuses = fetch_all_container_statuses(mode, deploy_mode)
        node_statuses = fetch_node_health_statuses(mode, deploy_mode, container_statuses)
        shared_state["metrics"] = metrics
        shared_state["container_statuses"] = container_statuses
        shared_state["node_statuses"] = node_statuses
        shared_state["last_fetch_time"] = time.time()
    except Exception as e:
        print(f"[dashboard] Error in background metrics fetch thread: {e}", flush=True)
    finally:
        shared_state["fetch_in_progress"] = False


def _exec_node_action(action: str, service: str, display: str):
    """Hàm `_exec_node_action` thực hiện phần xử lý liên quan đến exec node action.
    
    Ghi chú gốc:
    Run a kill/start/stop against a service and refresh the page on success.
    """
    print(f"[dashboard] EXECUTING NODE ACTION: action={action} service={service} display={display}", flush=True)
    fn, gerund, past, icon = _NODE_ACTIONS[action]
    with st.spinner(f"{gerund} {display}..."):
        ok, err = fn(service)
    if ok:
        print(f"[dashboard] NODE ACTION SUCCESS: {past} {display}!", flush=True)
        st.toast(f"{past} {display}!", icon=icon)
        
        # Optimistically update the status cache locally
        new_status = "stopped" if action in ("kill", "stop") else "running"
        if "node_statuses" in st.session_state:
            st.session_state.node_statuses[service] = new_status
        if "container_statuses" in st.session_state:
            st.session_state.container_statuses[service] = new_status
            
        # Reset last fetch time to force an immediate background refresh on next render
        if "fetch_shared_state" in st.session_state:
            st.session_state.fetch_shared_state["last_fetch_time"] = 0.0
            
        time.sleep(1)
        st.rerun()
    else:
        print(f"[dashboard] NODE ACTION FAILED: {action} {display}! Error: {err}", flush=True)
        st.error(f"Failed to {action} {display}: {err}")



def render_quick_fault_injection(
    mode: str,
    key_prefix: str = "nc",
    title: str = "#### ⚡ Quick Fault Injection",
    caption_text: str = None,
):
    """Render phần giao diện `render quick fault injection` lên dashboard hoặc báo cáo.
    
    Ghi chú gốc:
    Prominent one-click Kill / Recover panel for the selected node.
    """
    controls_enabled = bool(st.session_state.get("running", False))
    # Only probe live roles when the cluster is up (avoids slow timeouts when down).
    flat = [node for _, nodes in _node_control_groups(mode, detect_roles=controls_enabled)
            for node in nodes]
    labels = [f"{x['display']}  ·  {x['role']}" for x in flat]

    if title:
        st.markdown(title)
    sel = st.selectbox("Target node", labels, key=f"{key_prefix}_quick_sel")
    target = flat[labels.index(sel)]
    status = check_node_status(target["service"], target["port"]) if controls_enabled else "unknown"

    badge = "🟢 :green[Running]" if status == "running" else (
        "🔴 :red[Stopped]" if status == "stopped" else "⚪ :gray[Unknown]")
    st.markdown(f"**{target['display']}** — {badge}  ·  _{target['role']}_"
                + (f"  (:{target['port']})" if target["port"] else ""))

    # Surface the live leader/active node so the operator kills the correct one
    # (the container name is NOT the leader — it is elected at runtime).
    if controls_enabled:
        if mode == "strict":
            roles, _term = _detect_strict_roles()
            leaders = [c for c, r in roles.items() if r == "leader"]
            if leaders:
                st.caption(f"🧭 Live Raft leader: **{', '.join(leaders)}**. "
                           "Kill the leader to trigger a re-election; kill a follower to test redundancy.")
            else:
                st.caption("🧭 No Raft leader currently detected (election in progress or coordinators down).")
        else:
            active = _detect_active_aggregator()
            if active:
                st.caption(f"🧭 Live active aggregator: **{active}**. "
                           "Kill it to force HA takeover by the standby.")

    is_live_leader = (
        (mode == "strict" and target["role"].startswith("Coordinator Leader"))
        or (mode != "strict" and "Active" in target["role"])
    )
    if controls_enabled and is_live_leader and status == "running":
        st.warning(f"⚠️ **{target['display']}** is the current leader. Killing it forces a "
                   "failover/re-election — expect a brief watermark stall while a new leader takes over.")

    if not controls_enabled:
        st.info("Start the cluster from the sidebar to enable Kill / Recover controls.")

    b_kill, b_recover, b_refresh = st.columns(3)
    if b_kill.button("💥 Kill node", key=f"{key_prefix}_quick_kill", type="primary",
                     disabled=(not controls_enabled) or (status != "running"), width="stretch"):
        _exec_node_action("kill", target["service"], target["display"])
    if b_recover.button("♻️ Recover node", key=f"{key_prefix}_quick_recover",
                        disabled=(not controls_enabled) or (status == "running"), width="stretch"):
        _exec_node_action("start", target["service"], target["display"])
    if b_refresh.button("🔄 Refresh status", key=f"{key_prefix}_quick_refresh",
                        disabled=not controls_enabled, width="stretch"):
        st.rerun()

    st.caption(caption_text or "Kill a worker → watch the Dashboard tab react → Recover it. "
               "Killed nodes stay down (restart policy disabled) until you recover them.")


def render_node_control_row(service_name: str, display_name: str, port: int = None, role: str = ""):
    # Only probe Docker for live status when the cluster is up; otherwise skip
    # the (slow) subprocess calls and show the controls in a disabled preview.
    """Render phần giao diện `render node control row` lên dashboard hoặc báo cáo."""
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
    """Render phần giao diện `render failover events section` lên dashboard hoặc báo cáo."""
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
    st.dataframe(pd.DataFrame(event_rows), width="stretch", hide_index=True)


def _failover_snapshot(mode: str, phase: str) -> dict:
    """Hàm `_failover_snapshot` thực hiện phần xử lý liên quan đến failover snapshot.
    
    Ghi chú gốc:
    Capture a compact metrics snapshot for the failover-test timeline.
    """
    metrics = fetch_all_metrics(mode, "full")
    agg = aggregate_worker_metrics(metrics)
    coord = metrics.get("coordinator", {}) if mode == "strict" else {}
    agg_state = metrics.get("aggregator", {}) if mode != "strict" else {}
    return {
        "phase": phase,
        "time": datetime.now().strftime("%H:%M:%S"),
        "active_workers": coord.get("active_workers", len(metrics.get("workers", {}))),
        "workers_responding": len(metrics.get("workers", {})),
        "completeness_pct": round(agg.get("data_completeness_pct", 0.0), 2),
        "total_received": agg.get("total_received", 0),
        "late_dropped": agg.get("late_dropped", 0),
        "W_global": format_timestamp(coord.get("W_global")) if mode == "strict" else format_timestamp(agg_state.get("W_global_h")),
        "reassignments": len(coord.get("recovery_info", {})) if mode == "strict" else agg_state.get("ha_failover_count", 0),
        "term": coord.get("term", "—") if mode == "strict" else "—",
    }


def run_automated_failover_test(target_service: str, display: str, mode: str, observe_s: int = 20):
    """Chạy luồng xử lý `run automated failover test` theo cấu hình hiện tại.
    
    Ghi chú gốc:
    Kill a node, observe recovery while it is down, then recover it —
        recording a before/during/after timeline for the fault-tolerance deliverable.
    """
    timeline = []
    with st.status(f"Failover test on {display}...", expanded=True) as s:
        st.write("1/5 · Capturing baseline (healthy cluster)...")
        timeline.append(_failover_snapshot(mode, "before_kill"))

        st.write(f"2/5 · 💥 Killing {display} (auto-restart disabled)...")
        ok, err = compose_kill_service(target_service)
        if not ok:
            s.update(label="Failover test failed at kill step", state="error")
            st.error(err)
            return
        timeline.append(_failover_snapshot(mode, "just_killed"))

        st.write(f"3/5 · Observing failover for {observe_s}s (partitions should reassign)...")
        time.sleep(observe_s)
        timeline.append(_failover_snapshot(mode, "during_outage"))

        st.write(f"4/5 · ♻️ Recovering {display}...")
        ok, err = compose_start_service(target_service)
        if not ok:
            s.update(label="Failover test: node killed but recovery failed", state="error")
            st.error(err)
        st.write(f"5/5 · Observing failback for {observe_s}s...")
        time.sleep(observe_s)
        timeline.append(_failover_snapshot(mode, "after_recover"))

        s.update(label=f"Failover test complete for {display}", state="complete")

    record = {
        "target": display, "service": target_service, "mode": mode,
        "observe_s": observe_s, "ran_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "timeline": timeline,
    }
    st.session_state.failover_test_log.append(record)


def render_failover_test(mode: str):
    """Render phần giao diện `render failover test` lên dashboard hoặc báo cáo.
    
    Ghi chú gốc:
    Guided, one-click automated failover test with a recorded timeline.
    """
    st.markdown("#### 🧪 Automated Failover Test")
    st.caption(
        "Kills a worker, watches the coordinator reassign its partitions while it "
        "is down, then recovers it — recording a before/during/after timeline you "
        "can download as evidence of fault tolerance."
    )

    workers = [("node0", "Node 0 (P0,1,2)"), ("node1", "Node 1 (P3,4,5)"),
               ("node2", "Node 2 (P6,7,8)"), ("node3", "Node 3 (P9,10,11)")]
    f1, f2, f3 = st.columns([2, 1, 1])
    with f1:
        labels = [d for _, d in workers]
        sel = st.selectbox("Worker to fail", labels, key="fo_test_target")
        target = workers[labels.index(sel)][0]
    with f2:
        observe_s = st.number_input("Observe window (s)", min_value=5, max_value=120, value=20, step=5, key="fo_observe")
    with f3:
        st.markdown("&nbsp;")
        run = st.button("▶️ Run failover test", type="primary", width="stretch",
                        disabled=not st.session_state.running, key="fo_run")

    if run:
        run_automated_failover_test(target, sel, mode, int(observe_s))
        st.rerun()

    logs = st.session_state.get("failover_test_log", [])
    if not logs:
        st.info("No failover tests run yet.")
        return

    latest = logs[-1]
    st.markdown(f"**Last test:** {latest['target']} · mode={latest['mode']} · {latest['ran_at']}")
    tdf = pd.DataFrame(latest["timeline"])
    st.dataframe(tdf, width="stretch", hide_index=True)

    flat = []
    for rec in logs:
        for row in rec["timeline"]:
            flat.append({"ran_at": rec["ran_at"], "target": rec["target"], "mode": rec["mode"], **row})
    st.download_button(
        "⬇️ Download failover timeline (CSV)",
        data=pd.DataFrame(flat).to_csv(index=False),
        file_name=f"failover_test_{_ts_suffix()}.csv",
        mime="text/csv", key="dl_failover_csv",
    )


def render_node_control(mode: str):
    """Render phần giao diện `render node control` lên dashboard hoặc báo cáo."""
    st.markdown("### Kill Node / Recover")
    st.markdown("Choose a target service, kill or stop it, then recover it to verify fault-tolerance.")

    render_quick_fault_injection(
        mode,
        key_prefix="node_nc",
        title="#### Quick Kill / Recover",
    )

    if not st.session_state.running:
        st.warning("⚠️ Press **Start** in the sidebar to launch the cluster first. "
                   "The Kill / Recover buttons activate once containers are running.")
    else:
        st.divider()
        render_failover_test(mode)
        st.divider()
        render_failover_events_section()
        st.divider()
        st.caption("Status is read on demand (this tab does not auto-refresh, so buttons stay stable).")

    with st.expander("All services (advanced per-node controls)", expanded=not st.session_state.running):
        for header, nodes in _node_control_groups(mode, detect_roles=st.session_state.running):
            st.markdown(f"#### {header}")
            for nd in nodes:
                render_node_control_row(nd["service"], nd["display"], nd["port"], nd["role"])
            st.markdown("---")


# -----------------------------------------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------------------------------------

def main():
    """Điểm vào CLI của script, đọc tham số và điều phối các bước xử lý."""
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

    # No page-level auto-refresh. Each live-metrics tab is an isolated fragment
    # with its own "Refresh data" button, so a refresh re-renders ONLY that data
    # block — never the sidebar, tabs, or control panels.
    def _render_refresh_bar(key: str = None):
        """Hàm `_render_refresh_bar` thực hiện phần xử lý liên quan đến render refresh bar."""
        st.caption(f"🟢 **Real-time Auto-refresh (every 3s) active** · Last update: {datetime.now().strftime('%H:%M:%S')}")

    def _refresh_metrics() -> dict:
        """Hàm `_refresh_metrics` thực hiện phần xử lý liên quan đến refresh metrics.
        
        Ghi chú gốc:
        Fetch live metrics once and append a history sample. Throttled to max once per 2 seconds.
        """
        metrics = st.session_state.get("last_metrics") or {}
        if st.session_state.running:
            # 1. Trigger background fetch if not already in progress and 2 seconds elapsed
            shared = st.session_state.fetch_shared_state
            now = time.time()
            if not shared["fetch_in_progress"]:
                if now - shared["last_fetch_time"] >= 2.0:
                    shared["fetch_in_progress"] = True
                    thread = threading.Thread(
                        target=bg_fetch_job,
                        args=(shared, mode, st.session_state.deploy_mode),
                        daemon=True
                    )
                    thread.start()

            # 2. Consume any new background fetch results
            if shared["last_fetch_time"] > st.session_state.get("last_processed_fetch_time", 0.0):
                new_metrics = shared["metrics"] or {}
                new_statuses = shared["container_statuses"] or {}
                new_node_statuses = shared["node_statuses"] or {}
                
                st.session_state.last_metrics = new_metrics
                st.session_state.container_statuses = new_statuses
                st.session_state.node_statuses = new_node_statuses
                st.session_state.last_processed_fetch_time = shared["last_fetch_time"]
                
                # Coordination tracking: detect changes and log to console
                if new_metrics.get("coordinator"):
                    coord_data = new_metrics["coordinator"]
                    leader = new_metrics.get("coordinator_name")
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

                if new_metrics.get("workers"):
                    agg = aggregate_worker_metrics(new_metrics)
                    bottleneck = _current_bottleneck_snapshot(new_metrics, mode, agg)
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
                        "bottleneck_p95_us": bottleneck.get("stage_p95_us", 0.0),
                        "bottleneck_p99_us": bottleneck.get("stage_p99_us", 0.0),
                        "wm_lag_max_s": agg.get("wm_lag_max_s", 0.0),
                        "event_lag_p95_ms": agg.get("sketch_p95_ms", 0.0),
                    })
                    if len(st.session_state.metrics_history) > MAX_HISTORY:
                        st.session_state.metrics_history = st.session_state.metrics_history[-MAX_HISTORY:]

            # Always return the cached metrics
            metrics = st.session_state.last_metrics or {}
        return metrics or {}

    # Build the tab bar once. Kill Node sits right after Dashboard and carries
    # a distinct label so the fault-injection controls are easy to find.
    TAB_DASH = "📊 Dashboard"
    TAB_NODE = "🛑 Kill Node / Recover"
    TAB_LAB = "📐 Completeness vs Wait"
    TAB_LOGS = "📜 Logs"
    TAB_CMP = "⚖️ Compare"
    TAB_RAW = "🧩 Raw JSON"

    tab_names = [TAB_DASH, TAB_NODE, TAB_LAB, TAB_LOGS, TAB_CMP, TAB_RAW]
    tabs = st.tabs(tab_names)
    tab_idx = {name: i for i, name in enumerate(tab_names)}

    # Tab: Dashboard — isolated fragment; auto-refreshes every 3 seconds when running.
    with tabs[tab_idx[TAB_DASH]]:
        @st.fragment(run_every=3.0 if st.session_state.running else None)
        def _dashboard_fragment():
            """Hàm `_dashboard_fragment` thực hiện phần xử lý liên quan đến dashboard fragment."""
            if st.session_state.running:
                st.info("🛑 Kill / Recover controls are at the top of this Dashboard and in the **Kill Node / Recover** tab above.")
                _render_refresh_bar("dash_refresh")
            metrics = _refresh_metrics()
            if st.session_state.running and metrics.get("workers"):
                render_dashboard(mode, metrics)
            elif st.session_state.running:
                st.info("Waiting for services to start...")
                st.caption("This may take 30-60 seconds on first run.")
            else:
                render_welcome_intro()
        _dashboard_fragment()

    # Tab: Completeness vs Wait Time — deliverable lab. Refresh keeps the live
    # "current run" line fresh; recorded points persist across reruns.
    with tabs[tab_idx[TAB_LAB]]:
        @st.fragment(run_every=5.0 if st.session_state.running else None)
        def _lab_fragment():
            """Hàm `_lab_fragment` thực hiện phần xử lý liên quan đến lab fragment."""
            _ = _refresh_metrics()
            render_completeness_vs_wait()
        _lab_fragment()

    # Tab: Node Control — isolated fragment; auto-refreshes.
    with tabs[tab_idx[TAB_NODE]]:
        @st.fragment(run_every=3.0 if st.session_state.running else None)
        def _node_control_fragment():
            # Trigger a silent metrics fetch to update the events display
            """Hàm `_node_control_fragment` thực hiện phần xử lý liên quan đến node control fragment."""
            _ = _refresh_metrics()
            render_node_control(mode)
        _node_control_fragment()

    # Tab: Logs — isolated fragment; auto-refreshes if checked.
    with tabs[tab_idx[TAB_LOGS]]:
        @st.fragment(run_every=3.0 if (st.session_state.running and st.session_state.get("log_auto_scroll", False)) else None)
        def _logs_fragment():
            """Hàm `_logs_fragment` thực hiện phần xử lý liên quan đến logs fragment."""
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
