from run import _stable_partition


def test_stable_partition_is_process_independent():
    assert _stable_partition("48", 12) == _stable_partition("48", 12)
    assert _stable_partition("140", 12) == 0
    assert _stable_partition("48", 12) == 10


def test_run_experiment_agg_prefers_partition_metrics(monkeypatch):
    from reports import run_experiment

    payloads = {
        1: {
            "total_received": 100,
            "on_time": 100,
            "late_dropped": 0,
            "partitions": {
                "0": {"total_received": 10, "on_time": 10, "late_dropped": 0},
                "1": {"total_received": 10, "on_time": 5, "late_dropped": 5},
            },
        },
        2: {
            "total_received": 100,
            "on_time": 100,
            "late_dropped": 0,
            "partitions": {
                "2": {"total_received": 10, "on_time": 0, "late_dropped": 10},
            },
        },
    }

    monkeypatch.setattr(run_experiment, "WORKER_PORTS", [1, 2])
    monkeypatch.setattr(run_experiment, "_read_worker", lambda port: payloads[port])

    agg = run_experiment._agg("heuristic", retries=1)

    assert agg["total_received"] == 30
    assert agg["on_time"] == 15
    assert agg["late_dropped"] == 15
    assert agg["data_completeness_pct"] == 50.0
