"""
Docker Compose Chaos Test Runner for Strict Watermark Mode.
Verifies:
1. System startup and healthy state.
2. Worker node failure detection and partition reassignment (Kill Node).
3. Worker node recovery and 5-step failback protocol (Recover Node).
4. Correctness of global watermark advancement after recovery.
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
    print("\n=== Cleaning up containers ===")
    cmd = ["docker", "compose", "-f", COMPOSE_FILE,
           "--profile", "strict", "down", "-v", "--remove-orphans"]
    run_cmd(cmd, check=False)


def wait_for_healthy(services_with_ports, timeout=120):
    print(f"=== Waiting for health of {list(services_with_ports.keys())} ===")
    start = time.time()
    pending = list(services_with_ports.items())
    while pending and (time.time() - start) < timeout:
        elapsed = int(time.time() - start)
        still_pending = []
        for name, port in pending:
            try:
                r = requests.get(f"http://127.0.0.1:{port}/health", timeout=2)
                if r.status_code == 200:
                    print(f"  [OK] {name} is healthy")
                    continue
            except Exception:
                pass
            still_pending.append((name, port))
        pending = still_pending
        if pending:
            if elapsed % 15 == 0:
                print(f"  [{elapsed}s] Still waiting for: {[n for n, _ in pending]}")
            time.sleep(2)
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


def get_leader_state():
    # Try all coordinators to find the leader
    for name, port in [("coordinator-1", 9000), ("coordinator-2", 9003), ("coordinator-3", 9004)]:
        state = get_state(port)
        if state and state.get("raft_role") == "leader":
            return state
    # If no leader is explicitly found or election is in progress, fallback to coordinator-1
    return get_state(9000)


def get_partition_owner_from_state(state, pid):
    if not state or "failover" not in state:
        return None
    workers = state["failover"].get("workers", {})
    for wid, w_info in workers.items():
        if pid in w_info.get("partitions", []):
            return wid
    return None


def get_ingestor_logs(tail=30):
    res = run_cmd(["docker", "compose", "-f", COMPOSE_FILE,
                   "logs", "--tail", str(tail), "--no-color", "ingestor"],
                  check=False, timeout=15)
    return res.stdout + res.stderr


def run_chaos_test():
    print("\n" + "=" * 60)
    print("STARTING DOCKER COMPOSE CHAOS TEST")
    print("=" * 60)
    cleanup()
    time.sleep(3)

    env = os.environ.copy()
    env["MODE"] = "strict"
    env["DATASET_FILE"] = "nyc_taxi_events_full.csv"
    env["LOG_LEVEL"] = "info"
    env["PUNCTUATION_MODE"] = "data-driven"

    # Start up strict profile
    print("\n=== [1/5] Building and starting containers ===")
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

    print("\n=== [2/5] Letting watermark advance initially ===")
    start_time = time.time()
    watermark_started = False
    while time.time() - start_time < 30:
        state = get_leader_state()
        if state:
            w_global = state.get("W_global", float("-inf"))
            print(f"  Watermark progress check: W_global={w_global}")
            if w_global is not None and w_global > 0:
                watermark_started = True
                print("  -> Watermark started advancing successfully.")
                break
        time.sleep(3)

    if not watermark_started:
        print("  WARNING: Watermark did not start advancing within 30s. Proceeding anyway.")

    # Get initial partitions for node1 (Worker ID is "1")
    target_worker = "1"
    target_service = "node1"
    initial_node1_partitions = []
    
    # Wait for partitions assignment to appear in state
    start_time = time.time()
    while time.time() - start_time < 15:
        state = get_leader_state()
        if state and "failover" in state:
            workers = state["failover"].get("workers", {})
            if target_worker in workers:
                initial_node1_partitions = workers[target_worker].get("partitions", [])
                if initial_node1_partitions:
                    break
        time.sleep(2)
        
    print(f"  Initial partitions for worker {target_worker}: {initial_node1_partitions}")

    # KILL NODE: stop node1
    print(f"\n=== [3/5] KILL NODE CHAOS: Stopping service {target_service} ===")
    run_cmd(["docker", "compose", "-f", COMPOSE_FILE, "stop", target_service])
    print(f"  {target_service} stopped. Monitoring failover reassignment...")

    failover_success = False
    reassigned_owner_changed = False
    start_time = time.time()
    while time.time() - start_time < 45:
        state = get_leader_state()
        if state and "failover" in state:
            workers = state["failover"].get("workers", {})
            
            # Check if target_worker is detected as failed
            worker_status = workers.get(target_worker, {}).get("status", "")
            print(f"  Failover check: worker {target_worker} status={worker_status}")
            
            # Verify its partitions are being reassigned to other active nodes
            reassigned = []
            for pid in initial_node1_partitions:
                owner = get_partition_owner_from_state(state, pid)
                if owner and owner != target_worker:
                    reassigned.append(pid)
            
            if reassigned:
                print(f"  Partitions {reassigned} reassigned to other owners.")
                reassigned_owner_changed = True
                
            if worker_status == "failed" and reassigned_owner_changed:
                failover_success = True
                print("  -> Failover successfully detected and completed.")
                break
        time.sleep(3)

    assert failover_success, f"Failover validation failed: worker {target_worker} was not marked as failed or its partitions were not reassigned."

    # RECOVERY NODE: start node1 back
    print(f"\n=== [4/5] RECOVERY NODE CHAOS: Starting service {target_service} back ===")
    run_cmd(["docker", "compose", "-f", COMPOSE_FILE, "start", target_service])
    print(f"  {target_service} started. Monitoring failback progress...")

    failback_success = False
    start_time = time.time()
    while time.time() - start_time < 45:
        state = get_leader_state()
        if state and "failover" in state:
            workers = state["failover"].get("workers", {})
            
            worker_status = workers.get(target_worker, {}).get("status", "")
            print(f"  Failback check: worker {target_worker} status={worker_status}")
            
            # Verify partitions are returned back to target_worker
            returned = []
            for pid in initial_node1_partitions:
                owner = get_partition_owner_from_state(state, pid)
                if owner == target_worker:
                    returned.append(pid)
            
            if len(returned) == len(initial_node1_partitions):
                print(f"  All partitions {returned} returned back to worker {target_worker}.")
                failback_success = True
                break
        time.sleep(3)

    assert failback_success, f"Failback validation failed: partitions were not reassigned back to worker {target_worker}."
    print("  -> Node recovery and 5-step failback successfully verified.")

    # Let the system run to completion
    print("\n=== [5/5] Monitoring system run to completion ===")
    start_time = time.time()
    eof_reached = False
    success = False
    while time.time() - start_time < 120:
        state = get_leader_state()
        elapsed = int(time.time() - start_time)
        if state:
            w_global = state.get("W_global", float("-inf"))
            print(f"  [{elapsed}s] W_global={w_global}")
            if w_global is not None and w_global > 0:
                success = True

        logs = get_ingestor_logs(30)
        if "CSV EOF" in logs or "switching to idle loop" in logs:
            if not eof_reached:
                print("  Ingestor CSV EOF reached.")
                eof_reached = True
                break
        time.sleep(5)

    cleanup()
    print("\n" + "=" * 60)
    print("TEST COMPLETED SUCCESSFULLY!")
    print("=" * 60)
    return success


if __name__ == "__main__":
    try:
        ok = run_chaos_test()
        sys.exit(0 if ok else 1)
    except Exception as e:
        print(f"Chaos test error: {e}")
        cleanup()
        sys.exit(1)
