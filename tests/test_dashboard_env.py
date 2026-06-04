from types import SimpleNamespace
import time

from deploy import dashboard
from streamlit.testing.v1 import AppTest


class State(dict):
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as exc:
            raise AttributeError(key) from exc

    def __setattr__(self, key, value):
        self[key] = value


def test_dashboard_heuristic_defaults_match_runner(monkeypatch):
    state = State()
    monkeypatch.setattr(dashboard.st, "session_state", state)

    dashboard._apply_heuristic_recommended_defaults()
    env = dashboard._make_env("heuristic")

    assert state.punctuation_mode == "max-event-time"
    assert state.heuristic_p_normal == 0.75
    assert env["PUNCTUATION_MODE"] == "max-event-time"
    assert env["HEURISTIC_P_NORMAL"] == "0.75"
    assert env["HEURISTIC_P_SAFE"] == "0.999"
    assert env["INGESTOR_REPLAY"] == "arrival"
    assert env["REPLAY_SPEED"] == "150"
    assert env["HEURISTIC_LOCAL_WATERMARK_CLOSE"] == "true"
    assert env["HEURISTIC_WARMUP_SAMPLES"] == "2000"
    assert env["HEURISTIC_WARMUP_S"] == "5.0"
    assert env["PYTHONUNBUFFERED"] == "1"
    assert env["BP_PAUSE_THRESHOLD"] == "100000"
    assert env["BP_RESUME_THRESHOLD"] == "5000"
    assert env["STRICT_HARD_QUEUE_LIMIT"] == "300000"


def test_dashboard_init_state_tracks_dataset(monkeypatch):
    state = State()
    monkeypatch.setattr(dashboard.st, "session_state", state)

    dashboard.init_state()

    assert state.dataset_file == "nyc_taxi_events_full.csv"
    assert state.punctuation_mode == "max-event-time"


def _compose_state(**overrides):
    state = State(
        dataset_file="nyc_taxi_events_sliced.csv",
        log_level="info",
        punctuation_mode="max-event-time",
        heuristic_p_normal=0.75,
        heuristic_p_safe=0.999,
        heuristic_ingestor_replay="arrival",
        heuristic_replay_speed=150,
        heuristic_local_watermark_close=True,
        heuristic_warmup_samples=2000,
        heuristic_warmup_s=5.0,
        heuristic_python_unbuffered=True,
        delta_base_s=10.0,
    )
    state.update(overrides)
    return state


def test_dashboard_compose_up_uses_only_heuristic_profile(monkeypatch):
    state = _compose_state()
    captured = {}

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        captured["env"] = kwargs["env"]
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr(dashboard.st, "session_state", state)
    monkeypatch.setattr(dashboard.subprocess, "run", fake_run)

    ok, out = dashboard.compose_up("heuristic", "nyc_taxi_events_sliced.csv")

    assert ok
    assert out == "ok"
    assert captured["cmd"].count("--profile") == 1
    assert captured["cmd"][captured["cmd"].index("--profile") + 1] == "heuristic"
    assert "strict" not in captured["cmd"]
    assert captured["env"]["DATASET_FILE"] == "nyc_taxi_events_sliced.csv"
    assert captured["env"]["PUNCTUATION_MODE"] == "max-event-time"


def test_dashboard_compose_up_uses_only_strict_profile(monkeypatch):
    state = _compose_state(punctuation_mode="data-driven")
    captured = {}

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        captured["env"] = kwargs["env"]
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr(dashboard.st, "session_state", state)
    monkeypatch.setattr(dashboard.subprocess, "run", fake_run)

    ok, out = dashboard.compose_up("strict", "nyc_taxi_events_sliced.csv")

    assert ok
    assert out == "ok"
    assert captured["cmd"].count("--profile") == 1
    assert captured["cmd"][captured["cmd"].index("--profile") + 1] == "strict"
    assert "heuristic" not in captured["cmd"]
    assert captured["env"]["MODE"] == "strict"
    assert captured["env"]["DATASET_FILE"] == "nyc_taxi_events_sliced.csv"
    assert captured["env"]["PUNCTUATION_MODE"] == "max-event-time"
    assert state.punctuation_mode == "max-event-time"


def test_dashboard_metric_formatters_are_readable():
    assert dashboard._fmt_watermark(1_700_000_000).startswith("2023-")
    assert dashboard._fmt_watermark(float("-inf"), "-") == "-"
    assert dashboard._fmt_ms(12.345) == "12.3 ms"
    assert dashboard._fmt_ms(2500) == "2.50 s"
    assert dashboard._fmt_dur_s(0.25) == "250 ms"
    assert dashboard._fmt_count(1234567) == "1,234,567"
    assert dashboard._fmt_bytes(1_073_741_824) == "1.00 GB"
    assert dashboard._fmt_pct(91.0189) == "91.02%"


def test_fetch_all_metrics_strict_collects_workers_and_coordinators(monkeypatch):
    requested = []
    payloads = {
        "http://localhost:9101/api/metrics": {"total_received": 10},
        "http://localhost:9101/state": {"worker": "ok"},
        "http://localhost:9000/state": {"W_global": 1_700_000_000},
        "http://localhost:9000/ingestor-health": {"ingestors": {"ing-1": {"status": "ok"}}},
    }

    def fake_get_json(url, timeout=2.0):
        requested.append(url)
        return payloads.get(url)

    monkeypatch.setattr(dashboard, "_workers", lambda deploy_mode=None, mode=None: {"node0": {"port": 9101}})
    monkeypatch.setattr(dashboard, "_coordinators", lambda deploy_mode=None: {"coordinator-1": 9000})
    monkeypatch.setattr(dashboard, "_aggregator_port", lambda deploy_mode=None: 9007)
    monkeypatch.setattr(dashboard, "_get_json", fake_get_json)

    metrics = dashboard.fetch_all_metrics("strict")

    assert set(requested) == set(payloads)
    assert metrics["workers"]["node0"]["metrics"]["total_received"] == 10
    assert metrics["coordinator"]["W_global"] == 1_700_000_000
    assert metrics["coordinator"]["ingestor_health"]["ingestors"]["ing-1"]["status"] == "ok"
    assert "aggregator" not in metrics


def test_fetch_all_metrics_heuristic_collects_workers_and_aggregator(monkeypatch):
    requested = []
    payloads = {
        "http://localhost:9101/api/metrics": {"total_received": 10},
        "http://localhost:9101/state": {"worker": "ok"},
        "http://localhost:9007/state": {"W_global_h": 1_700_000_001},
    }

    def fake_get_json(url, timeout=2.0):
        requested.append(url)
        return payloads.get(url)

    monkeypatch.setattr(dashboard, "_workers", lambda deploy_mode=None, mode=None: {"node0": {"port": 9101}})
    monkeypatch.setattr(dashboard, "_coordinators", lambda deploy_mode=None: {"coordinator-1": 9000})
    monkeypatch.setattr(dashboard, "_aggregator_port", lambda deploy_mode=None: 9007)
    monkeypatch.setattr(dashboard, "_get_json", fake_get_json)

    metrics = dashboard.fetch_all_metrics("heuristic")

    assert set(requested) == set(payloads)
    assert metrics["workers"]["node0"]["metrics"]["total_received"] == 10
    assert "coordinator" not in metrics
    assert metrics["aggregator"]["W_global_h"] == 1_700_000_001


def test_dashboard_initial_streamlit_render_has_no_exceptions():
    app = AppTest.from_file("deploy/dashboard.py", default_timeout=20)

    app.run(timeout=20)

    assert not app.exception
    assert [button.label for button in app.sidebar.button] == ["Start", "Stop"]
    assert [select.label for select in app.sidebar.selectbox] == ["Dataset", "Log Level"]


def test_dashboard_heuristic_mode_switch_applies_recommended_controls():
    app = AppTest.from_file("deploy/dashboard.py", default_timeout=20)
    app.run(timeout=20)

    app.sidebar.radio[0].set_value("heuristic")
    app.run(timeout=20)

    assert not app.exception
    assert app.sidebar.radio[0].value == "heuristic"
    sidebar_selects = {select.label: select.value for select in app.sidebar.selectbox}
    assert sidebar_selects["Percentile p for L_eff"] == "p75 - quartile baseline"
    assert "Punctuation Mode" not in sidebar_selects
    assert app.session_state["punctuation_mode"] == "max-event-time"


def _sample_dashboard_metrics(mode: str):
    partition = {
        "total_received": 10,
        "on_time": 9,
        "late_dropped": 1,
        "duplicates": 0,
        "watermark": 1_700_000_000,
        "data_completeness_pct": 90.0,
        "proc_latency_p95_us": 120.0,
        "proc_latency_p99_us": 240.0,
        "watermark_lag_s": 0.5,
    }
    metrics = {
        "timestamp": time.time(),
        "mode": mode,
        "workers": {
            "node0": {
                "port": 9101,
                "metrics": {
                    "total_received": 10,
                    "on_time": 9,
                    "late_dropped": 1,
                    "duplicates": 0,
                    "partitions": {"0": partition},
                },
                "state": {},
            }
        },
    }
    if mode == "strict":
        metrics["coordinator"] = {
            "W_global": 1_700_000_000,
            "active_workers": 1,
            "partition_count": 1,
        }
        metrics["coordinator_name"] = "coordinator-1"
    else:
        metrics["aggregator"] = {
            "W_global_h": 1_700_000_000,
            "active_count": 1,
            "ha_active": True,
            "ha_failover_count": 0,
            "watermark_lag_s": 0.5,
        }
    return metrics


def test_dashboard_running_modes_render_without_exceptions():
    for mode in ("strict", "heuristic"):
        metrics = _sample_dashboard_metrics(mode)
        app = AppTest.from_file("deploy/dashboard.py", default_timeout=20)
        app.session_state["running"] = True
        app.session_state["mode"] = mode
        app.session_state["last_metrics"] = metrics
        app.session_state["metrics_history"] = []
        app.session_state["fetch_shared_state"] = {
            "metrics": metrics,
            "container_statuses": {},
            "node_statuses": {},
            "last_fetch_time": time.time(),
            "fetch_in_progress": False,
        }
        app.session_state["last_processed_fetch_time"] = 0.0

        app.run(timeout=20)

        assert not app.exception
