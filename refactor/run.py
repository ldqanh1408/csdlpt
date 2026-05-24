"""Unified entrypoint for the refactor stream processing system.

Roles: coordinator | aggregator | worker | ingestor
Modes:  strict | heuristic | hybrid

Usage:
  python3 -m refactor.run --role coordinator --mode strict
  python3 -m refactor.run --role worker --mode heuristic
  python3 -m refactor.run --role ingestor --source /data/events.jsonl
"""

import argparse
import json
import logging
import os
import signal
import sys
import time
import threading
import random
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs


def _create_tiered_storage():
    """Initialize TieredStorageManager from env vars. Returns None if not configured."""
    endpoint = os.environ.get("MINIO_ENDPOINT", "")
    if not endpoint:
        return None

    from refactor.common.tiered_storage import TieredStorageManager

    logging.info("TieredStorage: initializing with endpoint=%s", endpoint)
    return TieredStorageManager(
        endpoint=endpoint,
        access_key=os.environ.get("MINIO_ACCESS_KEY", ""),
        secret_key=os.environ.get("MINIO_SECRET_KEY", ""),
        bucket_name=os.environ.get("MINIO_BUCKET", "csdlpt-windows"),
        secure=os.environ.get("MINIO_SECURE", "false").lower() == "true",
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description="Refactor Stream Processor")
    p.add_argument("--role", required=True,
                   choices=["coordinator", "aggregator", "worker", "ingestor"])
    p.add_argument("--mode", default="strict",
                   choices=["strict", "heuristic", "hybrid"])
    p.add_argument("--port", type=int, default=8000)
    p.add_argument("--node-id", default=None)
    p.add_argument("--partitions", default="0,1,2")
    p.add_argument("--source", default=None)
    # Raft HA
    p.add_argument("--coordinator-id", default=None, help="Raft coordinator node ID")
    p.add_argument("--coordinator-peers", default=None, help="Comma-separated peer host:port list")
    # TLS
    p.add_argument("--tls-cert", default=None, help="Path to TLS certificate file")
    p.add_argument("--tls-key", default=None, help="Path to TLS private key file")
    # Output
    p.add_argument("--output-mode", default="idempotent", choices=["idempotent", "transactional"])
    # DR
    p.add_argument("--backup-interval", type=int, default=300, help="DR backup interval in seconds")
    p.add_argument("--disable-failover", action="store_true")
    p.add_argument("--disable-dr", action="store_true")
    # Kafka simulation
    p.add_argument("--enable-kafka", action="store_true", help="Enable Kafka simulation layer")
    p.add_argument("--kafka-port", type=int, default=9092, help="Kafka broker HTTP port")
    p.add_argument("--kafka-broker-url", default=None, help="Kafka broker URL (for producers/consumers)")
    return p.parse_args()


# ---------------------------------------------------------------------------
# HTTP health + metrics server (stdlib, no frameworks)
# ---------------------------------------------------------------------------

class HealthHandler(BaseHTTPRequestHandler):
    server_state: dict = {}

    def log_message(self, *args):
        pass

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        if path == "/health":
            self._json(200, {"status": "ok", "role": self.server_state.get("role", "unknown")})
        elif path == "/metrics":
            self._serve_prometheus_metrics()
        elif path == "/api/metrics":
            self._json(200, self._build_metrics_json())
        elif path == "/ready":
            ready = self.server_state.get("ready", False)
            self._json(200 if ready else 503, {"ready": ready})
        elif path == "/state":
            self._json(200, self._build_state())
        elif path == "/failover":
            fm = self.server_state.get("failover_manager")
            if fm:
                self._json(200, fm.summary())
            else:
                self._json(503, {"error": "failover disabled"})
        elif path == "/backpressure":
            bp = self.server_state.get("backpressure_controller")
            if bp:
                self._json(200, bp.summary())
            else:
                self._json(503, {"error": "backpressure not available"})
        elif path == "/ingestor-health":
            hm = self.server_state.get("health_monitor")
            if hm:
                self._json(200, {"W_meta_global": hm.W_meta_global, "ingestors": hm.summary(), "alerts": hm.alerts})
            else:
                self._json(503, {"error": "no health monitor"})
        else:
            self._json(404, {"error": "not found"})

    def do_POST(self):
        path = urlparse(self.path).path
        content_len = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_len) if content_len else b"{}"
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            self._json(400, {"error": "invalid json"})
            return

        if path == "/ingest":
            handler = self.server_state.get("ingest_handler")
            if handler:
                events = data if isinstance(data, list) else [data]
                count = handler(events)
                self._json(200, {"ingested": count})
            else:
                self._json(503, {"error": "no ingest handler registered"})
        elif path == "/punctuation":
            handler = self.server_state.get("punctuation_handler")
            if handler:
                handler(data)
                self._json(200, {"ok": True})
            else:
                self._json(503, {"error": "no punctuation handler registered"})
        elif path == "/ingestor-heartbeat":
            hm = self.server_state.get("health_monitor")
            if hm:
                hm.receive_heartbeat(
                    ingestor_id=data.get("ingestor_id", ""),
                    partitions=data.get("partitions", []),
                    last_T_commit=float(data.get("last_T_commit", 0.0)),
                    ingestor_clock=float(data.get("ingestor_clock", time.time())),
                    offsets=data.get("offsets", {}),
                )
                self._json(200, {"ok": True})
            else:
                self._json(503, {"error": "no health monitor"})
        elif path == "/reassign":
            handler = self.server_state.get("reassign_handler")
            if handler:
                result = handler(data)
                self._json(200, result)
            else:
                self._json(503, {"error": "reassign not available"})
        elif path == "/raft-state":
            handler = self.server_state.get("raft_handler")
            if handler:
                result = handler(data)
                self._json(200, result)
            else:
                self._json(503, {"error": "raft not enabled"})
        elif path == "/backpressure":
            handler = self.server_state.get("backpressure_handler")
            if handler:
                result = handler(data)
                self._json(200, result)
            else:
                self._json(503, {"error": "backpressure not available"})
        elif path == "/zk-vote":
            handler = self.server_state.get("zk_handler")
            if handler:
                result = handler(data)
                self._json(200, result)
            else:
                self._json(503, {"error": "zk not enabled"})
        elif path == "/zk-state":
            handler = self.server_state.get("zk_state_handler")
            if handler:
                result = handler(data)
                self._json(200, result)
            else:
                self._json(503, {"error": "zk not enabled"})
        elif path == "/raft-vote":
            handler = self.server_state.get("raft_vote_handler")
            if handler:
                result = handler(data)
                self._json(200, result)
            else:
                self._json(503, {"error": "raft not enabled"})
        else:
            self._json(404, {"error": "not found"})

    def _json(self, code, data):
        body = json.dumps(data, default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_prometheus_metrics(self):
        """Serve metrics in Prometheus text format from MonitoringManager."""
        mon_mgr = self.server_state.get("monitoring_manager")
        if mon_mgr is not None:
            body = mon_mgr.generate_metrics()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self._json(503, {"error": "monitoring not initialized"})

    def _build_metrics_json(self):
        """Return metrics as JSON (legacy /api/metrics endpoint)."""
        comp = self.server_state.get("component")
        if comp is None:
            return {"status": "initializing"}
        if hasattr(comp, "metrics") and comp.metrics is not None:
            return comp.metrics.summary()
        if hasattr(comp, "summary"):
            return comp.summary()
        return {"status": "no metrics available"}

    def _build_state(self):
        comp = self.server_state.get("component")
        if comp is None:
            return {"status": "initializing"}
        result = {}
        if hasattr(comp, "broadcast"):
            result.update(comp.broadcast())
        elif hasattr(comp, "summary"):
            result.update(comp.summary())
        else:
            result["status"] = "no state available"
        # Include failover state if available
        fm = self.server_state.get("failover_manager")
        if fm:
            result["failover"] = fm.summary()
        return result


def start_http_server(port: int, state: dict) -> HTTPServer:
    HealthHandler.server_state = state
    srv = HTTPServer(("0.0.0.0", port), HealthHandler)

    # TLS support: wrap socket if cert_file and key_file are provided
    cert_file = state.get("tls_cert")
    key_file = state.get("tls_key")
    if cert_file and key_file:
        import ssl
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(cert_file, key_file)
        srv.socket = ctx.wrap_socket(srv.socket, server_side=True)
        logging.info("TLS enabled: cert=%s key=%s", cert_file, key_file)

    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    return srv


# ---------------------------------------------------------------------------
# Monitoring + Alerting helpers
# ---------------------------------------------------------------------------

def _create_monitoring() -> "MonitoringManager":
    """Create a MonitoringManager instance.

    Returns None if prometheus_client is not available.
    """
    try:
        from refactor.common.monitoring import MonitoringManager
        return MonitoringManager()
    except ImportError:
        print("[monitoring] prometheus_client not available, metrics disabled", file=sys.stderr)
        return None


def _start_alerting_thread(mon_mgr, stop_event):
    """Start periodic alert evaluation if PagerDuty routing key is configured.

    Parameters
    ----------
    mon_mgr : MonitoringManager | None
    stop_event : threading.Event
    """
    if mon_mgr is None:
        return
    routing_key = os.environ.get("PAGERDUTY_ROUTING_KEY", "")
    if not routing_key:
        routing_key = None  # normalize empty string to None

    try:
        from refactor.common.alerting import AlertManager, alert_evaluation_loop
        alert_mgr = AlertManager(pagerduty_routing_key=routing_key)
        if routing_key:
            print("[alerting] PagerDuty integration enabled", file=sys.stderr)
        else:
            print("[alerting] PagerDuty not configured, alerts logged to stderr", file=sys.stderr)

        t = threading.Thread(
            target=alert_evaluation_loop,
            args=(alert_mgr, mon_mgr, 10.0, stop_event),
            daemon=True,
        )
        t.start()
    except ImportError:
        pass


def _update_monitoring_from_component(mon_mgr, state: dict) -> None:
    """Push current component state into MonitoringManager.

    Called periodically by each role's background loop.
    """
    if mon_mgr is None:
        return

    comp = state.get("component")
    if comp is None:
        return

    # Coordinator broadcast
    if hasattr(comp, "broadcast"):
        try:
            broadcast = comp.broadcast()
            mon_mgr.update_from_coordinator(broadcast)
        except Exception:
            pass

    # Per-engine summaries (worker role)
    if hasattr(comp, "engines"):
        engines = comp.engines
        for pid, eng in engines.items():
            try:
                summary = eng.metrics.summary() if hasattr(eng, "metrics") else eng.summary()
                worker_id = getattr(comp, "worker_id", "unknown")
                mon_mgr.update_from_engine(summary, worker_id, pid)
            except Exception:
                pass

    # Heuristic worker proxy
    if hasattr(comp, "engines") and hasattr(comp, "node_id"):
        engines = comp.engines
        for pid, eng in engines.items():
            try:
                summary = eng.metrics.summary() if hasattr(eng, "metrics") else eng.summary()
                mon_mgr.update_from_engine(summary, comp.node_id, pid)
            except Exception:
                pass

    # Kafka broker: report partition lag for all consumer groups
    kb = state.get("kafka_broker")
    if kb is not None:
        try:
            for gid, cg in kb._consumer_groups.items():
                for cid, member in cg._members.items():
                    for pid in member.assigned_partitions:
                        lag_val = kb.partition_lag("events", gid, cid, pid)
                        mon_mgr.update_kafka_lag("events", gid, cid, pid, lag_val)
        except Exception:
            pass

    # Health monitor
    hm = state.get("health_monitor")
    if hm and hasattr(hm, "evaluate"):
        try:
            hb = state.get("component")
            wg = hb.W_global if hasattr(hb, "W_global") else 0.0
            eval_result = hm.evaluate(wg)
            mon_mgr.update_from_health_monitor(eval_result)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Coordinator role (Strict mode)
# ---------------------------------------------------------------------------

def run_coordinator(args):
    from refactor.strict.coordinator import StrictCoordinator
    from refactor.strict.ingestor_health import IngestorHealthMonitor
    from refactor.strict.failover import FailoverManager
    from refactor.common.types import WorkerHeartbeat

    ckpt_dir = os.environ.get("CHECKPOINT_DIR", "/data/checkpoint")

    # Core coordinator
    coord = StrictCoordinator(
        delta_base_s=float(os.environ.get("DELTA_BASE_S", "10.0")),
        state_path=ckpt_dir + "/coordinator.json",
        db_path=ckpt_dir + "/rocksdb-coordinator",
    )
    coord.load_state()

    # Optional Raft HA wrapper (or ZK election if ZK_ENSEMBLE is set)
    raft_peers = []
    if args.coordinator_id:
        peers_str = args.coordinator_peers or os.environ.get("COORDINATOR_PEERS", "")
        raft_peers = [p.strip() for p in peers_str.split(",") if p.strip()]
        zk_ensemble = os.environ.get("ZK_ENSEMBLE", "").lower() in ("1", "true", "yes")
        from refactor.strict.raft_coordinator import RaftCoordinator
        coord = RaftCoordinator(
            args.coordinator_id, raft_peers,
            delta_base_s=float(os.environ.get("DELTA_BASE_S", "10.0")),
            state_path=ckpt_dir + "/coordinator.json",
            db_path=ckpt_dir + "/rocksdb-coordinator",
            zk_ensemble=zk_ensemble,
        )

    # Failover manager
    fm = None
    if not args.disable_failover:
        fm = FailoverManager(
            heartbeat_timeout_s=float(os.environ.get("HEARTBEAT_TIMEOUT_S", "10.0")),
        )
        fm.on_partition_type_change = lambda pid, ptype: logging.info(
            "partition_type_change: pid=%d type=%s", pid, ptype
        )
        coord.set_failover_manager(fm)

    # Disaster recovery (backup W_global + partition state to MinIO)
    dr = None
    if not args.disable_dr:
        tiered = _create_tiered_storage()
        if tiered is not None:
            from refactor.strict.disaster_recovery import DisasterRecovery
            dr = DisasterRecovery(
                tiered_storage=tiered,
                backup_interval_s=float(os.environ.get("DR_BACKUP_INTERVAL_S", str(args.backup_interval))),
            )
            dr.start_background_backup(coord)

    health_mon = IngestorHealthMonitor(
        silent_timeout_s=15.0,
        stuck_timeout_s=5.0,
        clock_skew_critical_ms=2000.0,
        w_meta_deviation_s=10.0,
    )

    mon_mgr = _create_monitoring()

    def heartbeat_handler(data):
        hb = WorkerHeartbeat(
            worker_id=data.get("worker_id", ""),
            partitions={int(k): float(v) for k, v in data.get("partitions", {}).items()},
            max_event_time=data.get("max_event_time", 0.0),
            timestamp=data.get("timestamp", time.time()),
        )
        coord.receive_heartbeat(hb)
        if "ingestor_id" in data:
            health_mon.receive_heartbeat(
                ingestor_id=data.get("ingestor_id", "unknown"),
                partitions=data.get("partitions_assigned", []),
                last_T_commit=float(data.get("T_commit", data.get("max_event_time", 0.0))),
                ingestor_clock=float(data.get("ingestor_clock", time.time())),
                offsets=data.get("offsets", {}),
            )
        # Failover: heartbeat with kafka offsets + detection + reassignment
        if fm is not None:
            kafka_offsets_raw = data.get("kafka_offsets", {})
            kafka_offsets = {int(k): int(v) for k, v in kafka_offsets_raw.items()} if kafka_offsets_raw else None
            fm.heartbeat(hb.worker_id, list(hb.partitions.keys()), offsets=kafka_offsets)
            failed = fm.detect_failures()
            if failed:
                fm.reassign_failed_partitions()

    def reassign_handler(data):
        """POST /reassign — manual or automated partition reassignment."""
        if fm is None:
            return {"error": "failover disabled"}
        worker_id = data.get("worker_id", "")
        if worker_id:
            fm.register_worker(worker_id, data.get("partitions", []))
        failed = fm.detect_failures()
        reassigned = fm.reassign_failed_partitions() if failed else {}
        return {"failed": list(failed), "reassigned": reassigned}

    def raft_handler(data):
        """POST /raft-state — receive replicated state from leader."""
        if hasattr(coord, "receive_state"):
            coord.receive_state(data)
            return {"ok": True}
        return {"error": "raft not enabled"}

    def raft_vote_handler(data):
        """POST /raft-vote — handle Raft vote requests from peers."""
        if hasattr(coord, "handle_vote_request"):
            return coord.handle_vote_request(
                term=data.get("term", 0),
                candidate_id=data.get("candidate_id", ""),
                W_global=data.get("W_global", float("-inf")),
            )
        return {"error": "raft not enabled"}

    def zk_handler(data):
        """POST /zk-vote — handle ZK ping/vote requests from peers."""
        if hasattr(coord, "handle_zk_vote"):
            return coord.handle_zk_vote(data)
        return {"error": "zk not enabled"}

    def zk_state_handler(data):
        """POST /zk-state — receive ZK-replicated state from leader."""
        if hasattr(coord, "handle_zk_state"):
            return coord.handle_zk_state(data)
        return {"error": "zk not enabled"}

    state = {
        "role": "coordinator",
        "ready": True,
        "component": coord,
        "ingest_handler": lambda events: 0,
        "punctuation_handler": heartbeat_handler,
        "reassign_handler": reassign_handler,
        "raft_handler": raft_handler,
        "raft_vote_handler": raft_vote_handler,
        "zk_handler": zk_handler,
        "zk_state_handler": zk_state_handler,
        "health_monitor": health_mon,
        "failover_manager": fm,
        "monitoring_manager": mon_mgr,
        "tls_cert": args.tls_cert,
        "tls_key": args.tls_key,
    }

    port = int(os.environ.get("PORT", args.port))
    srv = start_http_server(port, state)

    stop = threading.Event()

    # Alerting
    _start_alerting_thread(mon_mgr, stop)

    def save_loop():
        while not stop.is_set():
            time.sleep(2.0)
            coord.save_state()
            # Failover persistence
            if fm is not None and hasattr(fm, "_store") and fm._store is not None:
                fm._store.flush()
    threading.Thread(target=save_loop, daemon=True).start()

    # §11 Proactive 200ms broadcast loop: push W_global + partition state
    def proactive_broadcast_loop():
        while not stop.is_set():
            time.sleep(0.2)
            if hasattr(coord, "broadcast"):
                broadcast = coord.broadcast()
                mon_mgr.update_from_coordinator(broadcast)
                state["last_broadcast"] = broadcast
    threading.Thread(target=proactive_broadcast_loop, daemon=True).start()

    # Monitoring push loop
    def monitoring_loop():
        while not stop.is_set():
            time.sleep(5.0)
            _update_monitoring_from_component(mon_mgr, state)
    threading.Thread(target=monitoring_loop, daemon=True).start()

    print(f"[coordinator] listening on :{port}, W_global={coord.W_global}")

    # Kafka broker — start embedded when --enable-kafka
    kafka_broker = None
    if args.enable_kafka:
        from refactor.common.kafka_sim import KafkaBroker
        kafka_port = int(os.environ.get("KAFKA_PORT", str(args.kafka_port)))
        kafka_broker = KafkaBroker()
        kafka_broker.start_http_server(kafka_port)
        state["kafka_broker"] = kafka_broker
        print(f"[coordinator] kafka broker on :{kafka_port}")

    _wait_shutdown(stop, srv, state)


# ---------------------------------------------------------------------------
# Aggregator role (Heuristic mode)
# ---------------------------------------------------------------------------

def run_aggregator(args):
    from refactor.heuristic.aggregator import HeuristicAggregator

    ckpt_dir = os.environ.get("CHECKPOINT_DIR", "/data/checkpoint")
    os.makedirs(ckpt_dir, exist_ok=True)
    port = int(os.environ.get("PORT", args.port))

    agg_base = HeuristicAggregator(
        state_path=os.path.join(ckpt_dir, "aggregator.json"),
        db_path=os.path.join(ckpt_dir, "rocksdb-aggregator"),
    )
    agg_base.load_state()

    # §9.4 Active-Standby HA via file lock
    lock_path = f"/tmp/aggregator-{port}.lock"
    heartbeat_path = f"/tmp/aggregator-{port}-heartbeat"
    try:
        from refactor.heuristic.aggregator_ha import AggregatorHA
        agg = AggregatorHA(agg_base, lock_path=lock_path, heartbeat_path=heartbeat_path)
        agg.start()
        _agg_ha = agg  # keep reference for shutdown
    except Exception:
        logging.warning("AggregatorHA: file lock failed, running in standby mode")
        agg = agg_base
        _agg_ha = None

    mon_mgr = _create_monitoring()

    def wm_handler(data):
        agg_base.receive_worker_watermark(
            worker_id=data.get("worker_id", ""),
            partition_id=int(data.get("partition_id", 0)),
            W_h=float(data.get("W_h", 0.0)),
        )

    state = {
        "role": "aggregator",
        "ready": True,
        "component": agg,
        "ingest_handler": lambda events: 0,
        "punctuation_handler": wm_handler,
        "monitoring_manager": mon_mgr,
        "tls_cert": args.tls_cert,
        "tls_key": args.tls_key,
    }

    srv = start_http_server(port, state)

    stop = threading.Event()

    # Alerting
    _start_alerting_thread(mon_mgr, stop)

    def save_loop():
        while not stop.is_set():
            time.sleep(2.0)
            agg_base.save_state()
    threading.Thread(target=save_loop, daemon=True).start()

    # §9.3 Proactive 500ms broadcast loop: recompute W_global_h even without new worker messages
    def broadcast_loop():
        while not stop.is_set():
            time.sleep(0.5)
            agg_base._update_statuses()
            agg_base._compute_global()
    threading.Thread(target=broadcast_loop, daemon=True).start()

    # Monitoring push loop
    def monitoring_loop():
        while not stop.is_set():
            time.sleep(5.0)
            _update_monitoring_from_component(mon_mgr, state)
    threading.Thread(target=monitoring_loop, daemon=True).start()

    print(f"[aggregator] listening on :{port}, W_global_h={agg_base.W_global_h}")
    _wait_shutdown(stop, srv, state)

    # HA shutdown
    if _agg_ha is not None:
        _agg_ha.shutdown()


# ---------------------------------------------------------------------------
# Worker role (Strict or Heuristic based on mode)
# ---------------------------------------------------------------------------

def run_worker(args):
    mode = os.environ.get("MODE", args.mode)
    node_id = args.node_id or os.environ.get("NODE_ID", "0")
    parts = [int(x.strip()) for x in os.environ.get("PARTITIONS", args.partitions).split(",") if x.strip()]

    if mode == "strict":
        _run_strict_worker(node_id, parts, args)
    elif mode == "hybrid":
        _run_hybrid_worker(node_id, parts, args)
    else:
        _run_heuristic_worker(node_id, parts, args)


def _run_strict_worker(node_id, parts, args):
    from refactor.strict.worker import (
        StrictWorker, BACKPRESSURE_MAX_QUEUE, BACKPRESSURE_RESUME_AT,
    )
    from refactor.strict.backpressure import BackpressureController
    from refactor.strict.output_manager import OutputManager
    from refactor.strict.replay_checkpoint import ReplayCheckpointManager
    from refactor.common.types import LogEvent, PunctuationToken

    tiered_storage = _create_tiered_storage()
    mon_mgr = _create_monitoring()
    ckpt_dir = os.environ.get("CHECKPOINT_DIR", "/data/checkpoint")

    output_mode = os.environ.get("OUTPUT_MODE", args.output_mode)
    kafka_consumer = None
    kafka_producer = None
    bp = BackpressureController(
        pause_threshold=int(os.environ.get("BP_PAUSE_THRESHOLD", "500")),
        resume_threshold=int(os.environ.get("BP_RESUME_THRESHOLD", "100")),
    )
    replay_mgr = ReplayCheckpointManager(
        db_path=f"{ckpt_dir}/rocksdb-replay-{node_id}",
        checkpoint_interval=int(os.environ.get("REPLAY_CKPT_INTERVAL", "1000")),
    )

    # Differentiated eviction: partition-type-aware tiered storage eviction
    diff_eviction = None
    if tiered_storage is not None:
        from refactor.common.differentiated_eviction import DifferentiatedEvictionManager
        diff_eviction = DifferentiatedEvictionManager(storage=tiered_storage)

    worker = StrictWorker(
        worker_id=node_id,
        partition_ids=parts,
        window_size_s=float(os.environ.get("WINDOW_SIZE_S", "5.0")),
        delta_base_s=float(os.environ.get("DELTA_BASE_S", "10.0")),
        tiered_storage=tiered_storage,
        db_path=f"{ckpt_dir}/rocksdb-strict-{node_id}",
        output_mode=output_mode,
        kafka_producer=kafka_producer,
        kafka_results_topic="strict_results",
        diff_eviction=diff_eviction,
    )

    # Kafka consumer/producer setup — pull-based event ingestion +
    # window results emission to strict_results topic
    kafka_broker_url = args.kafka_broker_url or os.environ.get("KAFKA_BROKER_URL", "")
    if args.enable_kafka and kafka_broker_url:
        from refactor.common.kafka_sim import KafkaConsumer, KafkaProducer
        kafka_consumer = KafkaConsumer(broker_url=kafka_broker_url,
                                       group_id="strict-workers",
                                       client_id=f"strict-{node_id}")
        assigned = kafka_consumer.subscribe(["events"])
        print(f"[worker-strict:{node_id}] Kafka consumer assigned={assigned}", file=sys.stderr)
        kafka_producer = KafkaProducer(broker_url=kafka_broker_url, acks="all",
                                       client_id=f"strict-producer-{node_id}")

    def ingest_handler(events):
        count = 0
        for ev in events:
            pid = ev.get("partition_id", parts[0])
            le = LogEvent(
                event_id=str(ev.get("event_id", f"ev-{time.time_ns()}")),
                event_time=float(ev.get("event_time", time.time())),
                status=int(ev.get("status", 200)),
                arrival_time=time.time(),
            )
            # Check replay mode before processing
            if replay_mgr.detect_replay_mode(le):
                replay_mgr.record_event(le)
            worker.process(le, pid)
            count += 1
        return count

    def punctuation_handler(data):
        token = PunctuationToken(
            T_commit=float(data.get("T_commit", time.time())),
            partition_id=int(data.get("partition_id", parts[0])),
            ingestor_id=str(data.get("ingestor_id", "http")),
        )
        worker.on_punctuation(token)

    def backpressure_handler(data):
        """POST /backpressure — worker reports buffer size, receives pause/resume."""
        pid = int(data.get("partition_id", 0))
        size = int(data.get("buffer_size", 0))
        signal = bp.report_buffer(node_id, pid, size)
        return {"partition_id": pid, "paused": bp.is_paused(pid), "signal": signal}

    state = {
        "role": "worker",
        "ready": True,
        "component": worker,
        "ingest_handler": ingest_handler,
        "punctuation_handler": punctuation_handler,
        "backpressure_handler": backpressure_handler,
        "backpressure_controller": bp,
        "monitoring_manager": mon_mgr,
        "tls_cert": args.tls_cert,
        "tls_key": args.tls_key,
    }

    port = int(os.environ.get("PORT", args.port))
    srv = start_http_server(port, state)

    stop = threading.Event()

    # Alerting
    _start_alerting_thread(mon_mgr, stop)

    def drain_loop():
        while not stop.is_set():
            time.sleep(0.5)
            for pid, buf in worker.buffers.items():
                # Report buffer size for backpressure
                bp.report_buffer(node_id, pid, len(buf))
                ready = buf.pop_ready()
                if ready:
                    worker._process_event(ready, pid)
    threading.Thread(target=drain_loop, daemon=True).start()

    # Kafka consumer poll loop — consumer.poll() simulation
    if kafka_consumer is not None:
        def kafka_poll_loop():
            committed_offsets: dict[int, int] = {}
            last_lag_report = 0.0
            while not stop.is_set():
                try:
                    # Poll from Kafka broker
                    polled = kafka_consumer.poll(timeout_ms=500, max_messages=100)
                    for pid, msgs in polled.items():
                        for msg in msgs:
                            value = json.loads(msg["value"]) if isinstance(msg["value"], str) else msg["value"]
                            ev = value if isinstance(value, dict) else {"payload": str(value)}
                            le = LogEvent(
                                event_id=str(ev.get("event_id", f"kafka-{msg['offset']}")),
                                event_time=float(ev.get("event_time", time.time())),
                                status=int(ev.get("status", 200)),
                                arrival_time=time.time(),
                            )
                            worker.process(le, pid)
                            committed_offsets[pid] = msg["offset"] + 1
                        # Kafka backpressure: consumer.pause() when buffer full
                        buf_len = len(worker.buffers.get(pid, []))
                        if buf_len >= BACKPRESSURE_MAX_QUEUE:
                            kafka_consumer.pause([pid])
                        elif buf_len < BACKPRESSURE_RESUME_AT:
                            kafka_consumer.resume([pid])
                    # commit offsets
                    if committed_offsets:
                        kafka_consumer.commit(committed_offsets)
                        committed_offsets.clear()
                    # Report Kafka partition lag every 5s
                    now_t = time.time()
                    if now_t - last_lag_report >= 5.0 and mon_mgr is not None:
                        for pid in kafka_consumer.assigned_partitions():
                            lag = kafka_consumer.lag("events", pid)
                            mon_mgr.update_kafka_lag("events", "strict-workers",
                                                     f"strict-{node_id}", pid, lag)
                        last_lag_report = now_t
                except Exception:
                    time.sleep(0.1)
        threading.Thread(target=kafka_poll_loop, daemon=True).start()

    # Coordinator health check — auto-reconnect on coordinator failure
    coordinator_url = os.environ.get("COORDINATOR_URL", "")
    coordinator_peers_str = os.environ.get("COORDINATOR_PEERS", "")
    coordinator_peers = [p.strip() for p in coordinator_peers_str.split(",") if p.strip()]
    coord_state = {"url": coordinator_url, "failures": 0}

    def coordinator_health_check_loop():
        if not coord_state["url"]:
            return
        import urllib.request
        while not stop.is_set():
            time.sleep(5.0)
            try:
                req = urllib.request.Request(coord_state["url"] + "/health")
                urllib.request.urlopen(req, timeout=2)
                coord_state["failures"] = 0
            except Exception:
                coord_state["failures"] += 1
                if coord_state["failures"] >= 3 and coordinator_peers:
                    old_url = coord_state["url"]
                    coord_state["url"] = f"http://{coordinator_peers[coord_state['failures'] % len(coordinator_peers)]}"
                    print(f"[worker-strict:{node_id}] coordinator switched: {old_url} -> {coord_state['url']} (failures={coord_state['failures']})",
                          file=sys.stderr)
                    coord_state["failures"] = 0
    threading.Thread(target=coordinator_health_check_loop, daemon=True).start()

    # Monitoring push loop
    def monitoring_loop():
        while not stop.is_set():
            time.sleep(5.0)
            _update_monitoring_from_component(mon_mgr, state)
    threading.Thread(target=monitoring_loop, daemon=True).start()

    print(f"[worker-strict:{node_id}] listening on :{port}, partitions={parts}")
    _wait_shutdown(stop, srv, state)


def _run_heuristic_worker(node_id, parts, args):
    from refactor.heuristic.engine import HeuristicWatermarkEngine
    from refactor.heuristic.dlq import DLQPipeline
    from refactor.common.types import LogEvent
    from refactor.common.rocks_store import RocksStore
    from refactor.strict.backpressure import BackpressureController

    ckpt_dir = os.environ.get("CHECKPOINT_DIR", "/data/checkpoint")
    tiered_storage = _create_tiered_storage()
    engines = {
        pid: HeuristicWatermarkEngine(
            partition_id=pid, worker_id=node_id,
            db_path=f"{ckpt_dir}/rocksdb-heuristic-{node_id}-p{pid}",
            tiered_storage=tiered_storage,
        )
        for pid in parts
    }

    # §12 DLQ pipeline — persists late events; mandatory, must not drop silently
    _dlq_store = RocksStore(os.path.join(ckpt_dir, f"rocksdb-dlq-{node_id}"))
    dlq = DLQPipeline(
        dlq_path=os.path.join(ckpt_dir, f"dlq-{node_id}.json"),
        retention_days=int(os.environ.get("DLQ_RETENTION_DAYS", "7")),
        store=_dlq_store,
    )

    # DownstreamEmitter for DLQ correction delivery
    from refactor.heuristic.downstream_emitter import DownstreamEmitter
    downstream_emitter = DownstreamEmitter()

    aggregator_url = os.environ.get("AGGREGATOR_URL", "http://localhost:8001")
    aggregator_standby_url = os.environ.get("AGGREGATOR_STANDBY_URL", aggregator_url)
    mon_mgr = _create_monitoring()
    bp = BackpressureController(
        pause_threshold=int(os.environ.get("BP_PAUSE_THRESHOLD", "500")),
        resume_threshold=int(os.environ.get("BP_RESUME_THRESHOLD", "100")),
    )

    # Kafka consumer setup
    kafka_consumer = None
    kafka_producer = None
    kafka_results_topic = "heuristic_results"
    kafka_broker_url = args.kafka_broker_url or os.environ.get("KAFKA_BROKER_URL", "")
    if args.enable_kafka and kafka_broker_url:
        from refactor.common.kafka_sim import KafkaConsumer, KafkaProducer
        kafka_consumer = KafkaConsumer(broker_url=kafka_broker_url,
                                       group_id="heuristic-workers",
                                       client_id=f"heuristic-{node_id}")
        assigned = kafka_consumer.subscribe(["events"])
        print(f"[worker-heuristic:{node_id}] Kafka consumer assigned={assigned}", file=sys.stderr)
        kafka_producer = KafkaProducer(broker_url=kafka_broker_url, acks="all",
                                       client_id=f"heuristic-producer-{node_id}")
        print(f"[worker-heuristic:{node_id}] Kafka results producer created, topic={kafka_results_topic}", file=sys.stderr)

    def ingest_handler(events):
        count = 0
        for ev in events:
            pid = ev.get("partition_id", parts[0])
            if pid not in engines:
                continue
            le = LogEvent(
                event_id=str(ev.get("event_id", f"ev-{time.time_ns()}")),
                event_time=float(ev.get("event_time", time.time())),
                status=int(ev.get("status", 200)),
                arrival_time=time.time(),
            )
            engines[pid].process(le)
            count += 1
        return count

    class HeuristicWorkerProxy:
        def __init__(self, engines, node_id, tiered_storage=None,
                     kafka_producer=None, kafka_results_topic=None):
            self.engines = engines
            self.node_id = node_id
            self.tiered_storage = tiered_storage
            self.kafka_producer = kafka_producer
            self.kafka_results_topic = kafka_results_topic

        def summary(self):
            result = {"node_id": self.node_id, "partitions": {}}
            for pid, eng in self.engines.items():
                result["partitions"][pid] = eng.summary()
            return result

        def broadcast(self):
            result = {"node_id": self.node_id, "partitions": {}}
            for pid, eng in self.engines.items():
                result["partitions"][pid] = {"W_h": eng.W_h, "L_eff": eng.L_eff}
            return result

        def checkpoint(self):
            """Persist engine metadata to RocksDB across all partitions."""
            for eng in self.engines.values():
                eng.checkpoint()

        def flush(self):
            """Flush open windows and save cold-start baseline for graceful shutdown."""
            for pid, eng in self.engines.items():
                eng.flush()
                # Gap 3: save_baseline on graceful shutdown
                if self.tiered_storage is not None:
                    try:
                        eng.cold_start.save_baseline(
                            self.tiered_storage, pid, eng.sketch.to_dict())
                    except Exception:
                        pass

        def close(self):
            """Close all engine RocksDB stores."""
            for eng in self.engines.values():
                eng.close()

    proxy = HeuristicWorkerProxy(engines, node_id, tiered_storage=tiered_storage,
                                 kafka_producer=kafka_producer,
                                 kafka_results_topic=kafka_results_topic)

    state = {
        "role": "worker",
        "ready": True,
        "component": proxy,
        "ingest_handler": ingest_handler,
        "monitoring_manager": mon_mgr,
        "tls_cert": args.tls_cert,
        "tls_key": args.tls_key,
    }

    port = int(os.environ.get("PORT", args.port))
    srv = start_http_server(port, state)

    stop = threading.Event()

    # Alerting
    _start_alerting_thread(mon_mgr, stop)

    # Gap 2: 24h FINAL reconciliation — one scheduler per engine partition
    for pid, eng in engines.items():
        downstream_emitter.schedule_final_reconciliation(stop, eng)

    # Kafka consumer poll loop for heuristic worker
    if kafka_consumer is not None:
        def kafka_heuristic_poll_loop():
            committed_offsets: dict[int, int] = {}
            last_lag_report = 0.0
            while not stop.is_set():
                try:
                    # Backpressure: skip poll if all partitions are paused (spec §11.1)
                    if bp.paused_partitions() and all(bp.is_paused(pid) for pid in parts):
                        time.sleep(0.1)
                        continue
                    polled = kafka_consumer.poll(timeout_ms=500, max_messages=100)
                    for pid, msgs in polled.items():
                        if pid not in engines or bp.is_paused(pid):
                            continue
                        eng = engines[pid]
                        for msg in msgs:
                            value = json.loads(msg["value"]) if isinstance(msg["value"], str) else msg["value"]
                            ev = value if isinstance(value, dict) else {"payload": str(value)}
                            le = LogEvent(
                                event_id=str(ev.get("event_id", f"kafka-{msg['offset']}")),
                                event_time=float(ev.get("event_time", time.time())),
                                status=int(ev.get("status", 200)),
                                arrival_time=time.time(),
                            )
                            eng.process(le)
                            committed_offsets[pid] = msg["offset"] + 1
                        # Gap 1: Drain closed window results and produce to heuristic_results topic
                        if kafka_producer is not None:
                            results = eng.drain_results()
                            for result in results:
                                try:
                                    kafka_producer.send(kafka_results_topic, {
                                        "window_id": result.window_id,
                                        "partition_id": result.partition_id,
                                        "window_start": result.window_start,
                                        "window_end": result.window_end,
                                        "count": result.count,
                                        "status_500": result.status_500,
                                        "is_speculative": result.is_speculative,
                                        "version": result.version,
                                    })
                                except Exception:
                                    pass
                        # Report buffer depth for backpressure tracking
                        bp.report_buffer(node_id, pid, len(eng.open_windows) + len(eng.late_events))
                    if committed_offsets:
                        kafka_consumer.commit(committed_offsets)
                        committed_offsets.clear()
                    # Report Kafka partition lag every 5s
                    now_t = time.time()
                    if now_t - last_lag_report >= 5.0 and mon_mgr is not None:
                        for pid in kafka_consumer.assigned_partitions():
                            lag = kafka_consumer.lag("events", pid)
                            mon_mgr.update_kafka_lag("events", "heuristic-workers",
                                                     f"heuristic-{node_id}", pid, lag)
                        last_lag_report = now_t
                except Exception:
                    time.sleep(0.1)
        threading.Thread(target=kafka_heuristic_poll_loop, daemon=True).start()

    def report_loop():
        import urllib.request
        while not stop.is_set():
            time.sleep(0.2)
            for pid, eng in engines.items():
                payload = json.dumps({
                    "worker_id": node_id,
                    "partition_id": pid,
                    "W_h": eng.W_h,
                }).encode()
                headers = {"Content-Type": "application/json"}
                # Push to active aggregator
                try:
                    req = urllib.request.Request(
                        aggregator_url + "/punctuation",
                        data=payload, headers=headers,
                    )
                    urllib.request.urlopen(req, timeout=1)
                except Exception:
                    pass
                # Push to standby aggregator (best-effort, fire-and-forget)
                if aggregator_standby_url != aggregator_url:
                    try:
                        req = urllib.request.Request(
                            aggregator_standby_url + "/punctuation",
                            data=payload, headers=headers,
                        )
                        urllib.request.urlopen(req, timeout=1)
                    except Exception:
                        pass
    threading.Thread(target=report_loop, daemon=True).start()

    # Monitoring push loop
    def monitoring_loop():
        while not stop.is_set():
            time.sleep(5.0)
            _update_monitoring_from_component(mon_mgr, state)
    threading.Thread(target=monitoring_loop, daemon=True).start()

    # §12 DLQ drain loop — move late_events from engines into DLQ pipeline every 1s
    def dlq_drain_loop():
        while not stop.is_set():
            time.sleep(1.0)
            for eng in engines.values():
                if eng.late_events:
                    batch = eng.late_events[:]
                    eng.late_events.clear()
                    for ev in batch:
                        dlq.enqueue(ev)
            if mon_mgr is not None:
                mon_mgr.dlq_backlog.labels(worker_id=node_id).set(dlq.backlog)
    threading.Thread(target=dlq_drain_loop, daemon=True).start()

    # §12.5 DLQ hourly correction scheduler — drain DLQ, compute corrections, emit
    def dlq_correction_loop():
        while not stop.is_set():
            time.sleep(3600.0)
            if dlq.backlog == 0:
                continue
            try:
                batch = dlq.drain(batch_size=500)
                if batch:
                    corrections = dlq.compute_corrections(batch)
                    for corr in corrections:
                        downstream_emitter.enqueue(corr)
                    # Drain emitted corrections
                    drained = downstream_emitter.drain(batch_size=500)
                    # Gap 4: SLA check after correction drain
                    sla = downstream_emitter.check_sla()
                    if sla["normal_window_violations"] > 0 or sla["burst_window_violations"] > 0:
                        print(f"[worker-heuristic:{node_id}] SLA violations: "
                              f"normal={sla['normal_window_violations']} "
                              f"burst={sla['burst_window_violations']} "
                              f"compliant={sla['sla_compliant_pct']}%",
                              file=sys.stderr)
                    # Report SLA metrics to monitoring
                    if mon_mgr is not None:
                        mon_mgr.sla_compliant_pct.labels(worker_id=node_id).set(
                            sla["sla_compliant_pct"])
                    # Report correction metrics to monitoring
                    if mon_mgr is not None:
                        emitter_summary = downstream_emitter.summary()
                        mon_mgr.dlq_backlog.labels(worker_id=node_id).set(dlq.backlog)
                        print(f"[worker-heuristic:{node_id}] dlq correction: processed={len(batch)} "
                              f"corrections={len(corrections)} emitted={len(drained)} "
                              f"backlog={dlq.backlog} emitter_depth={emitter_summary['queue_depth']}",
                              file=sys.stderr)
            except Exception as exc:
                print(f"[worker-heuristic:{node_id}] dlq correction error: {exc}", file=sys.stderr)
    threading.Thread(target=dlq_correction_loop, daemon=True).start()

    print(f"[worker-heuristic:{node_id}] listening on :{port}, partitions={parts}")
    _wait_shutdown(stop, srv, state)


def _run_hybrid_worker(node_id, parts, args):
    """Hybrid worker: routes CRITICAL events to strict engine, STANDARD to heuristic engine.

    Uses HybridRouter per partition for unified window results and loss accounting.
    """
    from refactor.hybrid.router import HybridRouter, EventPriority
    from refactor.common.types import LogEvent, PunctuationToken
    from refactor.strict.backpressure import BackpressureController

    ckpt_dir = os.environ.get("CHECKPOINT_DIR", "/data/checkpoint")
    mon_mgr = _create_monitoring()
    bp = BackpressureController(
        pause_threshold=int(os.environ.get("BP_PAUSE_THRESHOLD", "500")),
        resume_threshold=int(os.environ.get("BP_RESUME_THRESHOLD", "100")),
    )

    routers = {
        pid: HybridRouter(
            partition_id=pid,
            window_size_s=float(os.environ.get("WINDOW_SIZE_S", "5.0")),
            delta_base_s=float(os.environ.get("DELTA_BASE_S", "10.0")),
            checkpoint_dir=os.path.join(ckpt_dir, f"hybrid-{node_id}-p{pid}"),
        )
        for pid in parts
    }

    def ingest_handler(events):
        count = 0
        for ev in events:
            pid = ev.get("partition_id", parts[0])
            if pid not in routers:
                continue
            router = routers[pid]
            priority_val = ev.get("priority", None)
            if priority_val is None:
                status = int(ev.get("status", 200))
                priority_val = "critical" if status >= 500 else "standard"
            le = LogEvent(
                event_id=str(ev.get("event_id", f"ev-{time.time_ns()}")),
                event_time=float(ev.get("event_time", time.time())),
                status=int(ev.get("status", 200)),
                arrival_time=time.time(),
            )
            le.payload = {"priority": priority_val}
            router.process(le)
            count += 1
        return count

    def punctuation_handler(data):
        token = PunctuationToken(
            T_commit=float(data.get("T_commit", time.time())),
            partition_id=int(data.get("partition_id", parts[0])),
            ingestor_id=str(data.get("ingestor_id", "http")),
        )
        pid = token.partition_id
        if pid in routers:
            routers[pid].on_punctuation(token)

    class HybridWorkerProxy:
        def __init__(self, routers, node_id):
            self.routers = routers
            self.node_id = node_id

        def summary(self):
            result = {"node_id": self.node_id, "mode": "hybrid", "partitions": {}}
            for pid, router in self.routers.items():
                result["partitions"][pid] = router.summary()
            return result

        def broadcast(self):
            result = {"node_id": self.node_id, "mode": "hybrid", "partitions": {}}
            for pid, router in self.routers.items():
                result["partitions"][pid] = {
                    "strict_W": getattr(router.strict_engine, "W_local", 0.0),
                    "heuristic_W": getattr(router.heuristic_engine, "W_h", 0.0),
                    "route_distribution": router.summary().get("route_distribution", {}),
                }
            return result

        @property
        def engines(self):
            return {pid: router for pid, router in self.routers.items()}

    proxy = HybridWorkerProxy(routers, node_id)

    state = {
        "role": "worker",
        "ready": True,
        "component": proxy,
        "ingest_handler": ingest_handler,
        "punctuation_handler": punctuation_handler,
        "backpressure_controller": bp,
        "monitoring_manager": mon_mgr,
        "tls_cert": args.tls_cert,
        "tls_key": args.tls_key,
    }

    port = int(os.environ.get("PORT", args.port))
    srv = start_http_server(port, state)

    stop = threading.Event()

    # Alerting
    _start_alerting_thread(mon_mgr, stop)

    # Flush closed windows periodically
    def window_flush_loop():
        while not stop.is_set():
            time.sleep(0.5)
            for router in routers.values():
                try:
                    router.close_windows()
                except Exception:
                    pass
    threading.Thread(target=window_flush_loop, daemon=True).start()

    # DLQ drain loop
    def dlq_drain_loop():
        while not stop.is_set():
            time.sleep(1.0)
            for router in routers.values():
                try:
                    router._drain_heuristic_dlq()
                except Exception:
                    pass
    threading.Thread(target=dlq_drain_loop, daemon=True).start()

    # Monitoring push loop
    def monitoring_loop():
        while not stop.is_set():
            time.sleep(5.0)
            _update_monitoring_from_component(mon_mgr, state)
    threading.Thread(target=monitoring_loop, daemon=True).start()

    print(f"[worker-hybrid:{node_id}] listening on :{port}, partitions={parts}")
    _wait_shutdown(stop, srv, state)

    # Flush on shutdown
    for router in routers.values():
        try:
            router.flush()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Ingestor role
# ---------------------------------------------------------------------------

def run_ingestor(args):
    node_hosts = [h.strip() for h in os.environ.get("NODE_HOSTS", "localhost:9101").split(",")]
    mode = os.environ.get("MODE", args.mode)
    source = args.source or os.environ.get("SOURCE", "")

    import urllib.request

    events = []
    if source and os.path.exists(source):
        with open(source) as f:
            for line in f:
                line = line.strip()
                if line:
                    events.append(json.loads(line))
    else:
        base = time.time()
        for i in range(1000):
            events.append({
                "event_id": f"syn-{i}",
                "event_time": base - random.uniform(0, 5),
                "status": random.choice([200, 200, 200, 200, 500]),
                "partition_id": i % 12,
            })

    # Kafka producer mode
    kafka_producer = None
    kafka_broker_url = args.kafka_broker_url or os.environ.get("KAFKA_BROKER_URL", "")
    if args.enable_kafka and kafka_broker_url:
        from refactor.common.kafka_sim import KafkaProducer
        kafka_producer = KafkaProducer(broker_url=kafka_broker_url)
        print(f"[ingestor] using Kafka producer -> {kafka_broker_url}", file=sys.stderr)

    node_for_partition = {}
    for i, host in enumerate(node_hosts):
        for p in range(i * 3, min((i + 1) * 3, 12)):
            node_for_partition[p] = host

    ingested = [0]
    last_punctuation = [time.time()]
    punctuation_interval = 1.0
    events_sent_since_punctuation = [0]
    last_log_offsets: dict[int, int] = {}

    def send_event_kafka(ev):
        """Send event via KafkaProducer (acks=all)."""
        result = kafka_producer.send("events", ev, key=ev.get("event_id", ""),
                                     partition=ev.get("partition_id", 0) % 12)
        if "error" not in result:
            ingested[0] += 1
            events_sent_since_punctuation[0] += 1

    def send_event(ev, host):
        try:
            data = json.dumps([ev]).encode()
            req = urllib.request.Request(
                f"http://{host}/ingest",
                data=data,
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=1)
            ingested[0] += 1
            events_sent_since_punctuation[0] += 1
        except Exception:
            pass

    def send_punctuation():
        is_empty = events_sent_since_punctuation[0] == 0
        events_sent_since_punctuation[0] = 0
        for host in node_hosts:
            try:
                data = json.dumps({
                    "T_commit": time.time() - 10.0,
                    "partition_id": 0,
                    "ingestor_id": "ingestor-main",
                    "is_empty": is_empty,
                }).encode()
                req = urllib.request.Request(
                    f"http://{host}/punctuation",
                    data=data,
                    headers={"Content-Type": "application/json"},
                )
                urllib.request.urlopen(req, timeout=1)
            except Exception:
                pass

    stop = threading.Event()

    def loop():
        idx = 0
        while not stop.is_set():
            ev = events[idx % len(events)]
            pid = ev.get("partition_id", 0)
            if kafka_producer is not None:
                send_event_kafka(ev)
            else:
                host = node_for_partition.get(pid, node_hosts[0])
                send_event(ev, host)
            last_log_offsets[pid] = idx
            idx += 1
            time.sleep(0.01)

            if mode == "strict" and (time.time() - last_punctuation[0]) > punctuation_interval:
                send_punctuation()
                last_punctuation[0] = time.time()

    threading.Thread(target=loop, daemon=True).start()

    coordinator_url = os.environ.get("COORDINATOR_URL", "")

    def heartbeat_loop():
        import urllib.request as _req
        while not stop.is_set():
            time.sleep(5.0)
            if not coordinator_url:
                continue
            try:
                payload = json.dumps({
                    "ingestor_id": "ingestor-main",
                    "T_commit": time.time() - 10.0,
                    "timestamp": time.time(),
                    "partitions_assigned": list(last_log_offsets.keys()),
                    "last_log_offset": max(last_log_offsets.values()) if last_log_offsets else 0,
                    "ingestor_clock": time.time(),
                    "offsets": dict(last_log_offsets),
                }).encode()
                r = _req.Request(coordinator_url + "/ingestor-heartbeat",
                                 data=payload, headers={"Content-Type": "application/json"})
                _req.urlopen(r, timeout=1)
            except Exception:
                pass
    threading.Thread(target=heartbeat_loop, daemon=True).start()

    mon_mgr = _create_monitoring()

    state = {
        "role": "ingestor",
        "ready": True,
        "component": None,
        "ingest_handler": lambda events: len(events),
        "monitoring_manager": mon_mgr,
        "tls_cert": args.tls_cert,
        "tls_key": args.tls_key,
    }

    port = int(os.environ.get("PORT", str(args.port))) if args.port != 8000 else 8100
    srv = start_http_server(port, state)

    # Alerting
    _start_alerting_thread(mon_mgr, stop)

    # Monitoring push loop
    def monitoring_loop():
        while not stop.is_set():
            time.sleep(5.0)
            if mon_mgr:
                mon_mgr.events_total.labels(
                    worker_id="ingestor-main", partition_id="0", status="ingested"
                ).inc(ingested[0])
                ingested[0] = 0
    threading.Thread(target=monitoring_loop, daemon=True).start()

    print(f"[ingestor] listening on :{port}, source={source or 'synthetic ({})'.format(len(events))}")
    _wait_shutdown(stop, srv, state)


# ---------------------------------------------------------------------------
# Shutdown
# ---------------------------------------------------------------------------

def _cleanup_components(state: dict) -> None:
    """Call checkpoint/flush/close on all components during graceful shutdown."""
    if state is None:
        return
    comp = state.get("component")
    if comp is not None:
        for attr_name in ("checkpoint", "flush", "close"):
            if hasattr(comp, attr_name) and callable(getattr(comp, attr_name)):
                try:
                    print(f"[shutdown] calling {attr_name}() on component", file=sys.stderr)
                    getattr(comp, attr_name)()
                except Exception as exc:
                    print(f"[shutdown] {attr_name}() failed: {exc}", file=sys.stderr)


def _wait_shutdown(stop_event, server, state: dict = None):
    def _handle(signum, frame):
        print(f"\n[shutdown] signal {signum} received, stopping...")
        stop_event.set()
        _cleanup_components(state)
        server.shutdown()
        sys.exit(0)
    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        stop_event.set()
        _cleanup_components(state)
        server.shutdown()


# ---------------------------------------------------------------------------
# Main dispatch
# ---------------------------------------------------------------------------

ROLE_DISPATCH = {
    "coordinator": run_coordinator,
    "aggregator": run_aggregator,
    "worker": run_worker,
    "ingestor": run_ingestor,
}


if __name__ == "__main__":
    _args = parse_args()
    _fn = ROLE_DISPATCH.get(_args.role)
    if _fn is None:
        print(f"Unknown role: {_args.role}", file=sys.stderr)
        sys.exit(1)
    _fn(_args)
