"""
E2E docker-compose test runner for both Strict and Heuristic watermark modes.
Captures 100-line logs per service and verifies watermark advancement.
"""

import subprocess
import time
import requests
import json
import os
import sys

DEPLOY_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "deploy"))
COMPOSE_FILE = os.path.join(DEPLOY_DIR, "docker-compose.yml")


def run_cmd(cmd, env=None, cwd=DEPLOY_DIR, check=True, timeout=600):
    print(f"Running: {' '.join(cmd)}")
    res = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8",
                         errors="replace", env=env, cwd=cwd, timeout=timeout)
    if check and res.returncode != 0:
        print(f"FAILED returncode={res.returncode}")
        print("STDOUT:", res.stdout[-3000:])
        print("STDERR:", res.stderr[-3000:])
        raise RuntimeError(f"Command failed: {cmd}")
    return res


def cleanup():
    print("\n=== Cleaning up ===")
    cmd = ["docker", "compose", "-f", COMPOSE_FILE,
           "--profile", "strict", "--profile", "heuristic", "--profile", "hybrid",
           "down", "-v", "--remove-orphans"]
    run_cmd(cmd, check=False)


def wait_for_healthy(services_with_ports, timeout=120):
    print(f"=== Waiting for {list(services_with_ports.keys())} to be healthy ===")
    start = time.time()
    pending = list(services_with_ports.items())
    while pending and (time.time() - start) < timeout:
        elapsed = int(time.time() - start)
        still_pending = []
        for name, port in pending:
            try:
                r = requests.get(f"http://127.0.0.1:{port}/health", timeout=2)
                if r.status_code == 200:
                    print(f"  [OK] {name} healthy")
                    continue
            except Exception as e:
                if elapsed % 15 == 0:
                    print(f"  [FAIL] {name}:{port} failed: {e}")
            still_pending.append((name, port))
        pending = still_pending
        if pending:
            print(f"  [{elapsed}s] Waiting for: {[n for n, _ in pending]}")
            time.sleep(3)
    if pending:
        raise TimeoutError(f"Services failed to become healthy: {[n for n, _ in pending]}")


def get_state(port):
    try:
        r = requests.get(f"http://127.0.0.1:{port}/state", timeout=3)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None


def get_worker_state(port):
    try:
        r = requests.get(f"http://127.0.0.1:{port}/api/metrics", timeout=3)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None


def get_ingestor_logs(tail=30):
    res = run_cmd(["docker", "compose", "-f", COMPOSE_FILE,
                   "logs", "--tail", str(tail), "--no-color", "ingestor"],
                  check=False, timeout=15)
    return res.stdout + res.stderr


def monitor_run(coordinator_or_aggregator_port, is_strict, timeout=300):
    print("=== Monitoring progress ===")
    start = time.time()
    eof_reached = False
    last_wm = None

    while (time.time() - start) < timeout:
        state = get_state(coordinator_or_aggregator_port)
        elapsed = int(time.time() - start)

        if state:
            if is_strict:
                w_global = state.get("W_global", float("-inf"))
                partitions = state.get("partitions", {})
                term = state.get("term", 0)
                print(f"  [{elapsed}s] Strict W_global={w_global} term={term} partitions={len(partitions)}")
                if w_global is not None and w_global != float("-inf"):
                    last_wm = w_global
                    if not eof_reached:
                        print("  -> Watermark advancing, waiting for EOF...")
            else:
                w_global_h = state.get("W_global_h", float("-inf"))
                workers = state.get("workers", {})
                print(f"  [{elapsed}s] Heuristic W_global_h={w_global_h} workers={len(workers)}")
                if w_global_h is not None and w_global_h != float("-inf"):
                    last_wm = w_global_h
        else:
            print(f"  [{elapsed}s] state endpoint not responding")

        # Worker node states
        worker_ports = {"node0": 9101, "node1": 9102, "node2": 9103, "node3": 9104}
        wm_summary = []
        for wname, wport in worker_ports.items():
            ws = get_worker_state(wport)
            if ws:
                p_states = ws.get("partitions", {})
                for pid, ps in p_states.items():
                    wm = ps.get("watermark", ps.get("W_h", "?"))
                    wm_summary.append(f"{wname}:p{pid}:{wm:.1f}" if isinstance(wm, float) else f"{wname}:p{pid}:{wm}")
        if wm_summary and elapsed % 30 == 0:
            print(f"  Workers: {' '.join(wm_summary[:6])}")

        # Check EOF in ingestor logs
        logs = get_ingestor_logs(50)
        if "CSV EOF" in logs or "switching to idle loop" in logs:
            if not eof_reached:
                print(f"  [{elapsed}s] Ingestor CSV EOF detected!")
                eof_reached = True

        # After EOF, wait for watermarks to propagate (up to 60s more)
        if eof_reached and state is not None:
            wm_val = state.get("W_global") if is_strict else state.get("W_global_h")
            if wm_val is not None and wm_val != float("-inf"):
                print(f"  [{elapsed}s] Watermark propagated! W={wm_val}")
                # Wait a bit more for windows to close
                time.sleep(20)
                break
            # Keep waiting up to 60s after EOF
            if elapsed > (time.time() - start) + 60:
                print("  Timed out waiting for watermark after EOF")
                break

        time.sleep(5)

    # Wait another 10s for final flush
    if eof_reached:
        print("  Waiting 10s for final window flush...")
        time.sleep(10)

    final = get_state(coordinator_or_aggregator_port)
    print("\n=== Final State ===")
    if final:
        print(json.dumps(final, indent=2, default=str))
    else:
        print("(no state)")
    return final


def capture_logs(mode_name, services):
    print(f"\n=== Capturing 100-line logs for {mode_name} ===")
    log_dir = os.path.join(DEPLOY_DIR, "test_logs", mode_name)
    os.makedirs(log_dir, exist_ok=True)
    for s in services:
        res = run_cmd(["docker", "compose", "-f", COMPOSE_FILE,
                       "logs", "--tail", "100", "--no-color", s],
                      check=False, timeout=15)
        content = res.stdout + res.stderr
        path = os.path.join(log_dir, f"{s}.log")
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        lines = content.strip().split("\n")
        print(f"  {s}: {len(lines)} lines -> {path}")
        # Print last 10 lines as preview
        for line in lines[-10:]:
            print(f"    {line}")
        print()


def run_test_strict():
    print("\n" + "=" * 60)
    print("STRICT WATERMARK E2E TEST")
    print("=" * 60)
    cleanup()
    time.sleep(3)

    env = os.environ.copy()
    env["MODE"] = "strict"
    env["DATASET_FILE"] = "access.log/access_sample.csv"
    env["LOG_LEVEL"] = "info"
    env["PUNCTUATION_MODE"] = "data-driven"

    run_cmd(["docker", "compose", "-f", COMPOSE_FILE, "--profile", "strict", "up", "-d", "--build"], env=env)

    wait_for_healthy({
        "coordinator-1": 9000,
        "coordinator-2": 9003,
        "coordinator-3": 9004,
        "node0": 9101,
        "node1": 9102,
        "node2": 9103,
        "node3": 9104,
    })

    final = monitor_run(9000, is_strict=True)

    capture_logs("strict", [
        "ingestor", "node0", "node1", "node2", "node3",
        "coordinator-1", "coordinator-2", "coordinator-3",
        "kafka", "zookeeper",
    ])

    cleanup()
    return final


def run_test_heuristic():
    print("\n" + "=" * 60)
    print("HEURISTIC WATERMARK E2E TEST")
    print("=" * 60)
    cleanup()
    time.sleep(3)

    env = os.environ.copy()
    env["MODE"] = "heuristic"
    env["DATASET_FILE"] = "access.log/access_sample.csv"
    env["LOG_LEVEL"] = "info"
    env["PUNCTUATION_MODE"] = "data-driven"

    run_cmd(["docker", "compose", "-f", COMPOSE_FILE, "--profile", "heuristic", "up", "-d", "--build"], env=env)

    wait_for_healthy({
        "aggregator": 9007,
        "aggregator-standby": 9005,
        "node0": 9101,
        "node1": 9102,
        "node2": 9103,
        "node3": 9104,
    })

    final = monitor_run(9007, is_strict=False)

    capture_logs("heuristic", [
        "ingestor", "node0", "node1", "node2", "node3",
        "aggregator", "aggregator-standby",
        "kafka", "zookeeper",
    ])

    cleanup()
    return final


if __name__ == "__main__":
    strict_ok = False
    heuristic_ok = False

    try:
        s = run_test_strict()
        if s:
            w = s.get("W_global")
            strict_ok = w is not None and w != float("-inf")
            print(f"\nStrict W_global = {w}")
    except Exception as e:
        print(f"Strict test error: {e}")
        cleanup()

    try:
        h = run_test_heuristic()
        if h:
            w = h.get("W_global_h")
            heuristic_ok = w is not None and w != float("-inf")
            print(f"\nHeuristic W_global_h = {w}")
    except Exception as e:
        print(f"Heuristic test error: {e}")
        cleanup()

    print("\n" + "=" * 60)
    print("TEST RESULTS")
    print("=" * 60)
    print(f"  Strict Mode:    {'PASSED' if strict_ok else 'FAILED'}")
    print(f"  Heuristic Mode: {'PASSED' if heuristic_ok else 'FAILED'}")
    sys.exit(0 if (strict_ok and heuristic_ok) else 1)
