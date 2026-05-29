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
import traceback
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn

def dump_tracebacks(signum, frame):
    print("=== DUMPING TRACEBACKS ===", file=sys.stderr)
    for thread_id, stack in sys._current_frames().items():
        print(f"\nThread {thread_id}:", file=sys.stderr)
        traceback.print_stack(stack, file=sys.stderr)
    print("=== END OF DUMP ===", file=sys.stderr)

if hasattr(signal, "SIGUSR1"):
    signal.signal(signal.SIGUSR1, dump_tracebacks)

# Ensure sibling packages (common, heuristic, strict, etc.) are importable
# when run.py lives inside a refactor/ package directory.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common import csdlpt_pb2


class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True
from urllib.parse import urlparse, parse_qs
import urllib.request
import ssl

try:
    import grpc
    from common import csdlpt_pb2_grpc
except ImportError:
    grpc = None

_grpc_channels = {}

def get_grpc_target(url_or_peer):
    if not url_or_peer:
        return None
    if url_or_peer.startswith("http://") or url_or_peer.startswith("https://"):
        parsed = urlparse(url_or_peer)
        netloc = parsed.netloc
    else:
        netloc = url_or_peer
    if ":" in netloc:
        host, port_str = netloc.rsplit(":", 1)
        try:
            grpc_port = int(port_str) + 50
            return f"{host}:{grpc_port}"
        except ValueError:
            pass
    return None

def get_grpc_stub(url_or_peer):
    if grpc is None:
        return None
    target = get_grpc_target(url_or_peer)
    if not target:
        return None
    if target not in _grpc_channels:
        try:
            channel = grpc.insecure_channel(target)
            stub = csdlpt_pb2_grpc.CoordinatorServiceStub(channel)
            _grpc_channels[target] = (channel, stub)
        except Exception:
            return None
    return _grpc_channels[target][1]

def get_grpc_aggregator_stub(url_or_peer):
    if grpc is None:
        return None
    target = get_grpc_target(url_or_peer)
    if not target:
        return None
    cache_key = f"agg_{target}"
    if cache_key not in _grpc_channels:
        try:
            channel = grpc.insecure_channel(target)
            stub = csdlpt_pb2_grpc.AggregatorServiceStub(channel)
            _grpc_channels[cache_key] = (channel, stub)
        except Exception:
            return None
    return _grpc_channels[cache_key][1]



_original_urlopen = urllib.request.urlopen

def _injected_urlopen(*args, **kwargs):
    if "context" not in kwargs or kwargs["context"] is None:
        cert_file = os.environ.get("TLS_CLIENT_CERT") or os.environ.get("TLS_CERT")
        key_file = os.environ.get("TLS_CLIENT_KEY") or os.environ.get("TLS_KEY")
        ca_file = os.environ.get("TLS_CA_FILE")
        insecure = os.environ.get("TLS_INSECURE", "false").lower() in ("true", "1", "yes")

        if cert_file or key_file or ca_file or insecure:
            try:
                ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
                if insecure:
                    ctx.check_hostname = False
                    ctx.verify_mode = ssl.CERT_NONE
                if cert_file and key_file:
                    ctx.load_cert_chain(cert_file, key_file)
                if ca_file:
                    ctx.load_verify_locations(cafile=ca_file)
                kwargs["context"] = ctx
            except Exception:
                pass
    return _original_urlopen(*args, **kwargs)

urllib.request.urlopen = _injected_urlopen



def parse_and_deduplicate_event(ev: dict, seen_ids_cache: set) -> dict | None:
    """Validate, parse and deduplicate event according to schema version and migration step."""
    if not isinstance(ev, dict):
        return ev

    event_id = ev.get("event_id")
    schema_ver = ev.get("schema_version", 1)

    # 1. Deduplication (important in dual_consume step where both V1 & V2 are received)
    if event_id:
        if event_id in seen_ids_cache:
            return None
        seen_ids_cache.add(event_id)
        if len(seen_ids_cache) > 20000:
            try:
                seen_ids_cache.remove(next(iter(seen_ids_cache)))
            except (StopIteration, KeyError):
                pass

    # 2. Schema Registry Validation
    from common.schema_registry import validate_json_schema, LOG_EVENT_V1_SCHEMA, LOG_EVENT_V2_SCHEMA
    schema = LOG_EVENT_V2_SCHEMA if schema_ver == 2 else LOG_EVENT_V1_SCHEMA
    validate_json_schema(ev, schema)

    # 3. Transform / Backward Compatibility mapping
    if schema_ver == 2:
        if "status" not in ev:
            ev["status"] = ev.get("http_status", 200)
    elif schema_ver == 1:
        if "http_status" not in ev:
            ev["http_status"] = ev.get("status", 200)
        if "service_name" not in ev:
            ev["service_name"] = "legacy-service"

    return ev



def _create_tiered_storage():
    """Initialize TieredStorageManager from env vars. Returns None if not configured."""
    endpoint = os.environ.get("MINIO_ENDPOINT", "")
    if not endpoint:
        return None

    from common.tiered_storage import TieredStorageManager

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
        elif path.startswith("/schemas/subjects/") and "/versions/" in path:
            parts = path.split("/")
            if len(parts) >= 6:
                subject = parts[3]
                try:
                    version = int(parts[5])
                    from common.schema_registry import _global_registry
                    schema = _global_registry.get_version(subject, version)
                    if schema:
                        self._json(200, {"subject": subject, "version": version, "schema": schema})
                        return
                except ValueError:
                    pass
            self._json(404, {"error": "schema version not found"})
        elif path.startswith("/schemas/ids/"):
            parts = path.split("/")
            if len(parts) >= 4:
                try:
                    schema_id = int(parts[3])
                    from common.schema_registry import _global_registry
                    schema = _global_registry.get_by_id(schema_id)
                    if schema:
                        self._json(200, {"schema": schema})
                        return
                except ValueError:
                    pass
            self._json(404, {"error": "schema id not found"})
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
                # Strict §10.1 wire format: ingestor sends `T_commit` and
                # `partitions_assigned`. Accept the spec field names directly;
                # also keep `last_T_commit`/`partitions` for back-compat with
                # any older clients (the receive_heartbeat parameter name
                # happens to be `last_T_commit` for historical reasons).
                hm.receive_heartbeat(
                    ingestor_id=data.get("ingestor_id", ""),
                    partitions=data.get("partitions_assigned",
                                        data.get("partitions", [])),
                    last_T_commit=float(data.get("T_commit",
                                                 data.get("last_T_commit", 0.0))),
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
        elif path.startswith("/schemas/subjects/") and path.endswith("/versions"):
            parts = path.split("/")
            if len(parts) >= 5:
                subject = parts[3]
                schema = data.get("schema")
                if schema:
                    from common.schema_registry import _global_registry
                    version = _global_registry.register(subject, schema)
                    self._json(200, {"version": version})
                    return
            self._json(400, {"error": "bad request or missing schema"})
        elif path.startswith("/compatibility/subjects/") and "/versions/" in path:
            parts = path.split("/")
            if len(parts) >= 6:
                subject = parts[3]
                try:
                    version = int(parts[5])
                    schema = data.get("schema")
                    if schema:
                        from common.schema_registry import _global_registry, check_backward_compatibility
                        old = _global_registry.get_version(subject, version)
                        compatible = True
                        if old:
                            compatible = check_backward_compatibility(old, schema)
                        self._json(200, {"compatible": compatible})
                        return
                except ValueError:
                    pass
            self._json(400, {"error": "bad request or missing schema/version"})
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
        # Return fast-path cached state if available (avoids lock contention)
        cached = self.server_state.get("cached_state")
        if cached is not None:
            return cached
        comp = self.server_state.get("component")
        if comp is None:
            return {"status": "initializing"}
        result = {}
        try:
            if hasattr(comp, "broadcast"):
                result.update(comp.broadcast())
            elif hasattr(comp, "summary"):
                result.update(comp.summary())
            else:
                result["status"] = "no state available"
            fm = self.server_state.get("failover_manager")
            if fm:
                result["failover"] = fm.summary()
        except Exception:
            result.setdefault("status", "state build error")
        return result


def start_http_server(port: int, state: dict) -> HTTPServer:
    HealthHandler.server_state = state
    srv = ThreadingHTTPServer(("0.0.0.0", port), HealthHandler)

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
        from common.monitoring import MonitoringManager
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
        from common.alerting import AlertManager, alert_evaluation_loop
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
            # Aggregator HA failover counter (mirrors leader-change rule)
            ha_count = broadcast.get("ha_failover_count")
            if ha_count is not None:
                prev = mon_mgr._snapshot.get("_agg_ha_failover_prev", 0)
                if ha_count > prev:
                    mon_mgr.aggregator_leader_changes.inc(ha_count - prev)
                    mon_mgr.aggregator_failover_total.inc(ha_count - prev)
                mon_mgr._snapshot["_agg_ha_failover_prev"] = ha_count
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

    # Trien_Khai §3 — push BackpressureController and TieredStorage status
    bp = state.get("backpressure_controller")
    if bp is not None and hasattr(bp, "_pause_count"):
        worker_id = state.get("worker_id", "unknown")
        try:
            prev = mon_mgr._snapshot.get(f"_bp_pause_{worker_id}", 0)
            cur = bp._pause_count
            if cur > prev:
                mon_mgr.backpressure_pause_count.labels(worker_id=worker_id).inc(cur - prev)
            mon_mgr._snapshot[f"_bp_pause_{worker_id}"] = cur
        except Exception:
            pass

    ts_mgr = state.get("tiered_storage")
    if ts_mgr is not None:
        try:
            connected = 1 if getattr(ts_mgr, "client", None) is not None else 0
            mon_mgr.tier_storage_status.set(connected)
            # MinIO upload failure counter — inc by diff so Prometheus rate()
            # works against `csdlpt_minio_upload_errors_total`.
            err_count = getattr(ts_mgr, "upload_error_count", 0)
            prev = mon_mgr._snapshot.get("_minio_upload_errors_prev", 0)
            if err_count > prev:
                mon_mgr.minio_upload_errors_total.inc(err_count - prev)
            mon_mgr._snapshot["_minio_upload_errors_prev"] = err_count
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
    from strict.coordinator import StrictCoordinator
    from strict.ingestor_health import IngestorHealthMonitor
    from strict.failover import FailoverManager
    from common.types import WorkerHeartbeat

    ckpt_dir = os.environ.get("CHECKPOINT_DIR", "/data/checkpoint")

    # Optional Raft HA wrapper (or ZK election if ZK_ENSEMBLE is set)
    # Auto-detect coordinator_id from env if not provided as CLI arg
    coordinator_id = args.coordinator_id or os.environ.get("COORDINATOR_ID", "")

    if coordinator_id:
        # RaftCoordinator wraps its own StrictCoordinator internally — do NOT create StrictCoordinator first
        # to avoid RocksDB double-open lock collision (each Rdict call holds an exclusive LOCK file)
        peers_str = args.coordinator_peers or os.environ.get("COORDINATOR_PEERS", "") or os.environ.get("RAFT_PEERS", "")
        raft_peers = [p.strip() for p in peers_str.split(",") if p.strip() and p.strip() != coordinator_id]
        zk_ensemble = os.environ.get("ZK_ENSEMBLE", "").lower() in ("1", "true", "yes")
        from strict.raft_coordinator import RaftCoordinator
        coord = RaftCoordinator(
            coordinator_id, raft_peers,
            delta_base_s=float(os.environ.get("DELTA_BASE_S", "10.0")),
            state_path=ckpt_dir + "/coordinator.json",
            db_path=ckpt_dir + "/rocksdb-coordinator",
            zk_ensemble=zk_ensemble,
        )
        # load_state is already called by RaftCoordinator.__init__ -> StrictCoordinator.__init__
    else:
        # Standalone (no Raft/ZK): plain StrictCoordinator
        from strict.coordinator import StrictCoordinator
        coord = StrictCoordinator(
            delta_base_s=float(os.environ.get("DELTA_BASE_S", "10.0")),
            state_path=ckpt_dir + "/coordinator.json",
            db_path=ckpt_dir + "/rocksdb-coordinator",
        )
        coord.load_state()

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
            from strict.disaster_recovery import DisasterRecovery
            dr = DisasterRecovery(
                storage=tiered,
                backup_interval_s=float(os.environ.get("DR_BACKUP_INTERVAL_S", str(args.backup_interval))),
            )
            # Coordinator has no per-partition engines; pass empty dict + coordinator
            # broadcast as state callback so W_global and term are captured (§6.6)
            dr.start_background_backup({}, get_state_callback=coord.broadcast)

    health_mon = IngestorHealthMonitor(
        silent_timeout_s=15.0,
        stuck_timeout_s=5.0,
        clock_skew_critical_ms=2000.0,
        w_meta_deviation_s=10.0,
    )
    coord.set_ingestor_health(health_mon)

    mon_mgr = _create_monitoring()

    _coord_hb_count = [0]
    _coord_last_hb_log = [0.0]
    _coord_last_wglobal = [float("-inf")]
    _coord_last_cache_update = [0.0]

    def heartbeat_handler(data):
        hb = WorkerHeartbeat(
            worker_id=data.get("worker_id", ""),
            partitions={int(k): float(v) for k, v in data.get("partitions", {}).items()},
            max_event_time=data.get("max_event_time", 0.0),
            timestamp=data.get("timestamp", time.time()),
        )
        old_wg = coord.W_global
        coord.receive_heartbeat(hb)
        _coord_hb_count[0] += 1
        now = time.time()

        # Refresh cached state before acquiring fm lock (broadcast() needs fm._lock too)
        if now - _coord_last_cache_update[0] >= 1.0:
            try:
                _cached = coord.broadcast() if hasattr(coord, "broadcast") else {}
                if fm is not None:
                    _cached["failover"] = fm.summary()
                _cached["timestamp"] = now
                state["cached_state"] = _cached
                _coord_last_cache_update[0] = now
            except Exception:
                pass

        if now - _coord_last_hb_log[0] >= 5.0:
            wg = coord.W_global
            wg_str = f"{wg:.1f}" if wg != float("-inf") else "-inf"
            adv = "advancing" if wg > _coord_last_wglobal[0] else "stable"
            n_workers = len(set(getattr(fm, '_worker_partitions', {}).keys())) if fm else "?"
            part_watermarks = " ".join(
                f"p{pid}:{info.local_watermark:.1f}"
                for pid, info in sorted(coord.partitions.items())
            ) if coord.partitions else "none"
            print(f"[coordinator] W_global={wg_str} ({adv}) "
                  f"delta={coord.delta_base}s "
                  f"term={getattr(coord, 'term', 0)} "
                  f"heartbeats={_coord_hb_count[0]} "
                  f"workers={n_workers} "
                  f"skew={coord._node_skew_max_ms:.0f}ms "
                  f"lag={coord._watermark_lag_s:.1f}s "
                  f"diag={coord._combined_diagnosis} "
                  f"partitions=[{part_watermarks}] "
                  f"from={hb.worker_id}",
                  file=sys.stderr)
            _coord_last_hb_log[0] = now
            _coord_last_wglobal[0] = wg
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
        "cached_state": {"status": "initializing", "timestamp": time.time()},
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

    grpc_server = None
    grpc_port = port + 50
    if grpc is not None:
        from concurrent import futures
        
        class CoordinatorServicer(csdlpt_pb2_grpc.CoordinatorServiceServicer):
            def __init__(self, coord, fm, health_mon):
                self.coord = coord
                self.fm = fm
                self.health_mon = health_mon
                
            def WorkerHeartbeat(self, request, context):
                hb = WorkerHeartbeat(
                    worker_id=request.worker_id,
                    partitions={int(k): float(v) for k, v in request.partitions.items()},
                    max_event_time=request.max_event_time,
                    timestamp=request.timestamp,
                )
                self.coord.receive_heartbeat(hb)
                _coord_hb_count[0] += 1
                if self.fm is not None:
                    kafka_offsets = {int(k): int(v) for k, v in request.kafka_offsets.items()} if request.kafka_offsets else None
                    self.fm.heartbeat(hb.worker_id, list(hb.partitions.keys()), offsets=kafka_offsets)
                    failed = self.fm.detect_failures()
                    if failed:
                        self.fm.reassign_failed_partitions()
                # ── Fix: refresh cached_state so /state returns live data ──
                now = time.time()
                if now - _coord_last_cache_update[0] >= 1.0:
                    try:
                        _cached = self.coord.broadcast() if hasattr(self.coord, "broadcast") else {}
                        if self.fm is not None:
                            _cached["failover"] = self.fm.summary()
                        _cached["timestamp"] = now
                        state["cached_state"] = _cached
                        _coord_last_cache_update[0] = now
                    except Exception:
                        pass
                return csdlpt_pb2.EmptyReply(ok=True)
                
            def IngestorHeartbeat(self, request, context):
                self.health_mon.receive_heartbeat(
                    ingestor_id=request.ingestor_id,
                    partitions=list(request.partitions_assigned),
                    last_T_commit=request.T_commit,
                    ingestor_clock=request.ingestor_clock,
                    offsets={int(k): int(v) for k, v in request.offsets.items()},
                )
                return csdlpt_pb2.EmptyReply(ok=True)
                
            def GetGlobalState(self, request, context):
                broadcast = self.coord.broadcast()
                partition_types = {}
                if self.fm is not None:
                    partition_types = {int(k): str(v) for k, v in self.fm.get_partition_types().items()}
                return csdlpt_pb2.StateReply(
                    W_global=self.coord.W_global,
                    term=getattr(self.coord, "term", 0),
                    partition_types=partition_types
                )
                
            def RaftVote(self, request, context):
                if hasattr(self.coord, "handle_vote_request"):
                    r = self.coord.handle_vote_request(
                        term=request.term,
                        candidate_id=request.candidate_id,
                        W_global=request.W_global
                    )
                    return csdlpt_pb2.RaftVoteReply(granted=r.get("granted", False), term=r.get("term", 0))
                return csdlpt_pb2.RaftVoteReply(granted=False, term=0)
                
            def RaftState(self, request, context):
                if hasattr(self.coord, "receive_state"):
                    data = json.loads(request.json_state) if request.json_state else {}
                    data["term"] = request.term
                    data["leader_id"] = request.leader_id
                    data["W_global"] = request.W_global
                    self.coord.receive_state(data)
                    return csdlpt_pb2.EmptyReply(ok=True)
                return csdlpt_pb2.EmptyReply(ok=False)
                
            def ZkVote(self, request, context):
                if hasattr(self.coord, "handle_zk_vote"):
                    r = self.coord.handle_zk_vote({"action": request.action})
                    return csdlpt_pb2.ZkVoteReply(ok=r.get("ok", False), coordinator_id=r.get("coordinator_id", ""))
                return csdlpt_pb2.ZkVoteReply(ok=False, coordinator_id="")
                
            def ZkState(self, request, context):
                if hasattr(self.coord, "handle_zk_state"):
                    data = json.loads(request.json_state) if request.json_state else {}
                    data["leader_id"] = request.leader_id
                    data["mode"] = request.mode
                    self.coord.handle_zk_state(data)
                    return csdlpt_pb2.EmptyReply(ok=True)
                return csdlpt_pb2.EmptyReply(ok=False)

        grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        csdlpt_pb2_grpc.add_CoordinatorServiceServicer_to_server(
            CoordinatorServicer(coord, fm, health_mon), grpc_server
        )
        grpc_server.add_insecure_port(f"0.0.0.0:{grpc_port}")
        grpc_server.start()
        print(f"[coordinator] gRPC server listening on :{grpc_port}", file=sys.stderr)
        state["grpc_server"] = grpc_server

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

    # Failover monitoring loop: proactively detect worker failures and trigger rebalancing
    def failover_monitoring_loop():
        while not stop.is_set():
            time.sleep(1.0)
            if fm is not None:
                try:
                    failed = fm.detect_failures()
                    if failed:
                        reassigned = fm.reassign_failed_partitions()
                        if reassigned:
                            print(f"[coordinator] FAILOVER DETECTED: failed={failed} reassigned={reassigned}", file=sys.stderr, flush=True)
                except Exception as e:
                    print(f"[coordinator] Failover detection loop error: {e}", file=sys.stderr, flush=True)
    threading.Thread(target=failover_monitoring_loop, daemon=True).start()

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
    
    enable_kafka = args.enable_kafka or os.environ.get("KAFKA_ENABLE", "false").lower() in ("1", "true", "yes", "on")
    # Kafka broker — start embedded when --enable-kafka and not real Kafka
    kafka_broker = None
    if enable_kafka and not os.environ.get("KAFKA_ENABLE_REAL", "false").lower() in ("1", "true", "yes", "on"):
        from common.kafka_sim import KafkaBroker
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
    from heuristic.aggregator import HeuristicAggregator

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
        from heuristic.aggregator_ha import AggregatorHA
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

    grpc_server = None
    grpc_port = port + 50
    if grpc is not None:
        from concurrent import futures
        
        class AggregatorServicer(csdlpt_pb2_grpc.AggregatorServiceServicer):
            def __init__(self, agg_base):
                self.agg_base = agg_base
                
            def SendWorkerWatermark(self, request, context):
                self.agg_base.receive_worker_watermark(
                    worker_id=request.worker_id,
                    partition_id=request.partition_id,
                    W_h=request.W_h,
                )
                return csdlpt_pb2.EmptyReply(ok=True)
                
        grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        csdlpt_pb2_grpc.add_AggregatorServiceServicer_to_server(
            AggregatorServicer(agg_base), grpc_server
        )
        grpc_server.add_insecure_port(f"0.0.0.0:{grpc_port}")
        grpc_server.start()
        print(f"[aggregator] gRPC server listening on :{grpc_port}", file=sys.stderr)
        state["grpc_server"] = grpc_server


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
    from strict.worker import (
        StrictWorker, BACKPRESSURE_MAX_QUEUE, BACKPRESSURE_RESUME_AT,
    )
    from strict.backpressure import BackpressureController
    from strict.output_manager import OutputManager
    from strict.replay_checkpoint import ReplayCheckpointManager
    from common.types import LogEvent, PunctuationToken

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
        from common.differentiated_eviction import DifferentiatedEvictionManager
        diff_eviction = DifferentiatedEvictionManager(storage=tiered_storage)

    kafka_audit_producer = None
    worker = StrictWorker(
        worker_id=node_id,
        partition_ids=parts,
        window_size_s=float(os.environ.get("WINDOW_SIZE_S", "5.0")),
        delta_base_s=float(os.environ.get("DELTA_BASE_S", "10.0")),
        max_queue=int(os.environ.get("BP_PAUSE_THRESHOLD", "500")),
        tiered_storage=tiered_storage,
        db_path=f"{ckpt_dir}/rocksdb-strict-{node_id}",
        output_mode=output_mode,
        kafka_producer=kafka_producer,
        kafka_results_topic="strict_results",
        diff_eviction=diff_eviction,
        kafka_audit_producer=kafka_audit_producer,
        kafka_audit_topic="audit_results",
    )

    # Log level for tracing
    _worker_log_level = os.environ.get("LOG_LEVEL", "info").lower()
    _worker_trace = _worker_log_level in ("debug", "trace")

    kafka_broker_url = args.kafka_broker_url or os.environ.get("KAFKA_BROKER_URL", "")
    enable_kafka = args.enable_kafka or os.environ.get("KAFKA_ENABLE", "false").lower() in ("1", "true", "yes", "on")
    if enable_kafka and kafka_broker_url:
        if os.environ.get("KAFKA_ENABLE_REAL", "false").lower() in ("1", "true", "yes", "on"):
            from common.kafka_real import KafkaConsumer, KafkaProducer, ensure_topics
            ensure_topics(kafka_broker_url, ["events", "strict_results", "audit_results"], num_partitions=12)
        else:
            from common.kafka_sim import KafkaConsumer, KafkaProducer
        kafka_consumer = KafkaConsumer(broker_url=kafka_broker_url,
                                       group_id="strict-workers",
                                       client_id=f"strict-{node_id}")
        assigned = kafka_consumer.subscribe(["events"])
        print(f"[worker-strict:{node_id}] Kafka consumer assigned={assigned}", file=sys.stderr)
        kafka_producer = KafkaProducer(broker_url=kafka_broker_url, acks="all",
                                       client_id=f"strict-producer-{node_id}")
        # §3 Architecture: Critical Audit Sink — mirror committed windows
        # to an audit topic so compliance/billing consumers can replay
        # independently of the primary strict_results stream.
        kafka_audit_producer = KafkaProducer(broker_url=kafka_broker_url, acks="all",
                                             client_id=f"strict-audit-{node_id}")

    seen_event_ids = set()

    def ingest_handler(events):
        count = 0
        for ev_raw in events:
            ev = parse_and_deduplicate_event(ev_raw, seen_event_ids)
            if ev is None:
                continue
            pid = ev.get("partition_id", parts[0])
            le = LogEvent(
                event_id=str(ev.get("event_id", f"ev-{time.time_ns()}")),
                event_time=float(ev.get("event_time", time.time())),
                status=int(ev.get("status", 200)),
                arrival_time=float(ev.get("arrival_time", time.time())),
                poll_received_at=time.time(),
                schema_version=int(ev.get("schema_version", 1)),
            )
            # Check replay mode before processing (§8.7 sub-checkpointing).
            # §6.4 feature flag: ENABLE_REPLAY_SUB_CHECKPOINTING gates the
            # sub-checkpoint bookkeeping; processing always runs.
            if os.environ.get("ENABLE_REPLAY_SUB_CHECKPOINTING", "true").lower() in ("1", "true", "yes", "on"):
                _eng = worker.engines.get(pid)
                _wm = _eng.local_watermark if _eng is not None else float("-inf")
                _delta = float(os.environ.get("DELTA_BASE_S", "10.0"))
                if replay_mgr.detect_replay_mode(le, watermark=_wm, delta_base_s=_delta):
                    replay_mgr.record_event(pid)
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
        "worker_id": node_id,
        "tiered_storage": tiered_storage,
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

    strict_drain_sleep = float(os.environ.get("STRICT_DRAIN_SLEEP_S", "0.1"))
    strict_drain_batch = int(os.environ.get("STRICT_DRAIN_BATCH_SIZE", "200"))

    def drain_loop():
        while not stop.is_set():
            time.sleep(strict_drain_sleep)
            for pid in worker.buffers:
                bp.report_buffer(node_id, pid, worker.buffer_size(pid))
                worker.drain_ready(pid, batch_size=strict_drain_batch)
    threading.Thread(target=drain_loop, daemon=True).start()

    worker_kafka_offsets = {}

    # Kafka consumer poll loop — consumer.poll() simulation
    if kafka_consumer is not None:
        def kafka_poll_loop():
            committed_offsets: dict[int, int] = {}
            last_lag_report = 0.0
            last_progress_log = 0.0
            poll_total = [0]
            while not stop.is_set():
                try:
                    # Poll from Kafka broker
                    polled = kafka_consumer.poll(timeout_ms=500, max_messages=100)
                    for pid, msgs in polled.items():
                        for msg in msgs:
                            value = json.loads(msg["value"]) if isinstance(msg["value"], str) else msg["value"]
                            if isinstance(value, dict) and value.get("is_punctuation"):
                                token = PunctuationToken(
                                    T_commit=float(value["T_commit"]),
                                    partition_id=int(value["partition_id"]),
                                    ingestor_id=value["ingestor_id"],
                                )
                                worker.on_punctuation(token)
                                committed_offsets[pid] = msg["offset"] + 1
                                worker_kafka_offsets[pid] = msg["offset"] + 1
                                poll_total[0] += 1
                                continue
                            ev_raw = value if isinstance(value, dict) else {"payload": str(value)}
                            ev = parse_and_deduplicate_event(ev_raw, seen_event_ids)
                            if ev is None:
                                continue
                            le = LogEvent(
                                event_id=str(ev.get("event_id", f"kafka-{msg['offset']}")),
                                event_time=float(ev.get("event_time", time.time())),
                                status=int(ev.get("status", 200)),
                                arrival_time=float(ev.get("arrival_time", time.time())),
                                poll_received_at=time.time(),
                                offset=msg["offset"],
                                schema_version=int(ev.get("schema_version", 1)),
                            )
                            eng = worker.engines.get(pid)
                            wm_before = eng.watermark if eng else float("-inf")
                            result = worker.process(le, pid)
                            if _worker_trace:
                                latency_us = (result / 1000.0) if result else 0.0
                                wm_after = eng.watermark if eng else float("-inf")
                                verdict = "ON_TIME" if wm_before < (le.event_time + float(os.environ.get("WINDOW_SIZE_S", "5.0"))) else "LATE"
                                print(f"[worker-strict:{node_id}] p{pid} eid={le.event_id} "
                                      f"et={le.event_time:.3f} ws={le.event_time - (le.event_time % 5.0):.1f} "
                                      f"wm={wm_after:.1f} verdict={verdict} "
                                      f"lat={latency_us:.0f}us buf={len(worker.buffers.get(pid, []))}",
                                      file=sys.stderr)
                            committed_offsets[pid] = msg["offset"] + 1
                            worker_kafka_offsets[pid] = msg["offset"] + 1
                            poll_total[0] += 1
                    # Kafka backpressure: check all assigned partitions
                    bp_pause_val = int(os.environ.get("BP_PAUSE_THRESHOLD", str(BACKPRESSURE_MAX_QUEUE)))
                    bp_resume_val = int(os.environ.get("BP_RESUME_THRESHOLD", str(BACKPRESSURE_RESUME_AT)))
                    for pid in kafka_consumer.assigned_partitions():
                        buf_len = len(worker.buffers.get(pid, []))
                        if buf_len >= bp_pause_val:
                            kafka_consumer.pause([pid])
                        elif buf_len < bp_resume_val:
                            kafka_consumer.resume([pid])
                    # commit offsets
                    if committed_offsets:
                        kafka_consumer.commit(committed_offsets)
                        committed_offsets.clear()
                    now_t = time.time()
                    # Progress log every 5s
                    if now_t - last_progress_log >= 5.0 and poll_total[0] > 0:
                        s = worker.summary()
                        buf_info = " ".join(f"p{p}={len(b)}q" for p, b in worker.buffers.items())
                        wm_info = " ".join(f"p{p}:W={eng.watermark:.1f}" for p, eng in worker.engines.items())
                        print(f"[worker-strict:{node_id}] "
                              f"received={s['total_received']} "
                              f"on_time={s['on_time']} "
                              f"late={s['late_dropped']} "
                              f"dupes={s['duplicates']} "
                              f"bp_drops={s['backpressure_drops']} "
                              f"completeness={s['data_completeness_pct']:.1f}% "
                              f"late_rate={s['late_arrival_rate_pct']:.1f}% "
                              f"open_windows={sum(p.get('open_windows',0) for p in s['partitions'].values() if isinstance(p,dict))} "
                              f"closed_windows={sum(p.get('closed_windows',0) for p in s['partitions'].values() if isinstance(p,dict))} "
                              f"buf=[{buf_info}] "
                              f"watermarks=[{wm_info}]",
                              file=sys.stderr)
                        last_progress_log = now_t
                    # Report Kafka partition lag every 5s
                    if now_t - last_lag_report >= 5.0 and mon_mgr is not None:
                        for pid in kafka_consumer.assigned_partitions():
                            lag = kafka_consumer.lag("events", pid)
                            mon_mgr.update_kafka_lag("events", "strict-workers",
                                                     f"strict-{node_id}", pid, lag)
                        last_lag_report = now_t
                except Exception:
                    import traceback
                    traceback.print_exc()
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

    # ── Fix 1: Heartbeat loop — send LW to coordinator every 200ms ──
    def heartbeat_loop():
        import dataclasses
        import urllib.request
        while not stop.is_set():
            time.sleep(0.2)
            hb = None
            try:
                hb = worker.heartbeat()
            except Exception:
                pass
            if hb is not None:
                sent = False
                if grpc is not None:
                    try:
                        stub = get_grpc_stub(coord_state["url"])
                        if stub is not None:
                            msg = csdlpt_pb2.WorkerHeartbeatMsg(
                                worker_id=hb.worker_id,
                                partitions=hb.partitions,
                                max_event_time=hb.max_event_time,
                                timestamp=hb.timestamp,
                                fencing_token=hb.fencing_token,
                                idle_partitions=hb.idle_partitions,
                                backpressure_partitions=hb.backpressure_partitions,
                                kafka_offsets=worker_kafka_offsets,
                            )
                            stub.WorkerHeartbeat(msg, timeout=2)
                            sent = True
                    except Exception as e:
                        print(f"[worker-strict:{node_id}] grpc heartbeat failed: {e}", file=sys.stderr)
                    if not sent and coordinator_peers:
                        for peer in coordinator_peers:
                            try:
                                stub = get_grpc_stub(peer)
                                if stub is not None:
                                    msg = csdlpt_pb2.WorkerHeartbeatMsg(
                                        worker_id=hb.worker_id,
                                        partitions=hb.partitions,
                                        max_event_time=hb.max_event_time,
                                        timestamp=hb.timestamp,
                                        fencing_token=hb.fencing_token,
                                        idle_partitions=hb.idle_partitions,
                                        backpressure_partitions=hb.backpressure_partitions,
                                        kafka_offsets=worker_kafka_offsets,
                                    )
                                    stub.WorkerHeartbeat(msg, timeout=1)
                                    sent = True
                                    break
                            except Exception:
                                continue
                if not sent:
                    hb_dict = dataclasses.asdict(hb)
                    if worker_kafka_offsets:
                        hb_dict["kafka_offsets"] = worker_kafka_offsets
                    data_bytes = json.dumps(hb_dict).encode()
                    try:
                        req = urllib.request.Request(
                            coord_state["url"] + "/punctuation",
                            data=data_bytes,
                            headers={"Content-Type": "application/json"},
                        )
                        urllib.request.urlopen(req, timeout=2)
                        sent = True
                    except Exception as e:
                        print(f"[worker-strict:{node_id}] http heartbeat failed: {e}", file=sys.stderr)
                    if not sent and coordinator_peers:
                        for peer in coordinator_peers:
                            try:
                                peer_url = f"http://{peer}"
                                req = urllib.request.Request(
                                    peer_url + "/punctuation",
                                    data=data_bytes,
                                    headers={"Content-Type": "application/json"},
                                )
                                urllib.request.urlopen(req, timeout=1)
                                break
                            except Exception:
                                continue
            for pid in worker.buffers:
                try:
                    bp.report_buffer(node_id, pid, len(worker.buffers[pid]))
                except Exception:
                    pass
    threading.Thread(target=heartbeat_loop, daemon=True).start()

    # ── Fix 2: W_global fetch loop — pull global watermark + partition types every 500ms ──
    def wglobal_fetch_loop():
        import urllib.request
        from common.differentiated_eviction import PartitionEvictionType
        while not stop.is_set():
            time.sleep(0.5)
            state_data = None
            # Always try HTTP /state first to get the full JSON state including recovery_info
            try:
                req = urllib.request.Request(coord_state["url"] + "/state")
                with urllib.request.urlopen(req, timeout=2) as resp:
                    state_data = json.loads(resp.read())
            except Exception:
                # If HTTP fails, fall back to gRPC if available
                if grpc is not None:
                    try:
                        stub = get_grpc_stub(coord_state["url"])
                        if stub is not None:
                            reply = stub.GetGlobalState(csdlpt_pb2.StateRequest(worker_id=node_id), timeout=2)
                            state_data = {
                                "W_global": reply.W_global,
                                "term": reply.term,
                                "partition_types": {str(k): v for k, v in reply.partition_types.items()},
                            }
                    except Exception:
                        pass
            if state_data is not None:
                try:
                    W_global = float(state_data.get("W_global", float("-inf")))
                    term = int(state_data.get("term", 0))
                    worker.update_global_watermark(W_global, term)
                    # ── Fix 3: Propagate partition_type changes to DifferentiatedEvictionManager ──
                    if diff_eviction is not None:
                        partition_types = state_data.get("partition_types", {})
                        for pid_str, ptype_str in partition_types.items():
                            pid = int(pid_str)
                            ptype = (
                                PartitionEvictionType.RECOVERY
                                if ptype_str == "recovery"
                                else PartitionEvictionType.NORMAL
                             )
                            diff_eviction.set_partition_type(pid, ptype)

                    # Dynamic partition reassignment update
                    if kafka_consumer is not None and hasattr(kafka_consumer, "update_assignment"):
                        recovery_info = state_data.get("recovery_info", {})
                        active_pids = set(parts)  # original partitions configured for this node
                        for pid_str, info in recovery_info.items():
                            pid = int(pid_str)
                            curr_owner = info.get("current_owner")
                            orig_owner = info.get("original_owner")
                            if curr_owner == node_id:
                                active_pids.add(pid)
                            elif orig_owner == node_id:
                                active_pids.discard(pid)
                        
                        current_assigned = set(kafka_consumer.assigned_partitions())
                        if active_pids != current_assigned:
                            print(f"[worker-strict:{node_id}] DYNAMIC REBALANCE: partition assignment changed from {sorted(current_assigned)} to {sorted(active_pids)}", file=sys.stderr, flush=True)
                            kafka_consumer.update_assignment(list(active_pids))
                except Exception:
                    pass
    threading.Thread(target=wglobal_fetch_loop, daemon=True).start()

    print(f"[worker-strict:{node_id}] listening on :{port}, partitions={parts}")
    _wait_shutdown(stop, srv, state)


def _run_heuristic_worker(node_id, parts, args):
    from heuristic.engine import HeuristicWatermarkEngine
    from heuristic.dlq import DLQPipeline
    from common.types import LogEvent
    from common.rocks_store import RocksStore
    from strict.backpressure import BackpressureController

    ckpt_dir = os.environ.get("CHECKPOINT_DIR", "/data/checkpoint")
    tiered_storage = _create_tiered_storage()
    engines = {
        pid: HeuristicWatermarkEngine(
            partition_id=pid, worker_id=node_id,
            db_path=f"{ckpt_dir}/rocksdb-heuristic-{node_id}-p{pid}",
            tiered_storage=tiered_storage,
            checkpoint_dir=ckpt_dir,
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
    from heuristic.downstream_emitter import DownstreamEmitter
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
    enable_kafka = args.enable_kafka or os.environ.get("KAFKA_ENABLE", "false").lower() in ("1", "true", "yes", "on")
    if enable_kafka and kafka_broker_url:
        if os.environ.get("KAFKA_ENABLE_REAL", "false").lower() in ("1", "true", "yes", "on"):
            from common.kafka_real import KafkaConsumer, KafkaProducer, ensure_topics
            ensure_topics(kafka_broker_url, ["events", "heuristic_results"], num_partitions=12)
        else:
            from common.kafka_sim import KafkaConsumer, KafkaProducer
        kafka_consumer = KafkaConsumer(broker_url=kafka_broker_url,
                                       group_id="heuristic-workers",
                                       client_id=f"heuristic-{node_id}")
        assigned = kafka_consumer.subscribe(["events"])
        print(f"[worker-heuristic:{node_id}] Kafka consumer assigned={assigned}", file=sys.stderr)
        kafka_producer = KafkaProducer(broker_url=kafka_broker_url, acks="all",
                                       client_id=f"heuristic-producer-{node_id}")
        print(f"[worker-heuristic:{node_id}] Kafka results producer created, topic={kafka_results_topic}", file=sys.stderr)

    seen_event_ids = set()

    def ingest_handler(events):
        count = 0
        for ev_raw in events:
            ev = parse_and_deduplicate_event(ev_raw, seen_event_ids)
            if ev is None:
                continue
            pid = ev.get("partition_id", parts[0])
            if pid not in engines:
                from heuristic.engine import HeuristicWatermarkEngine
                engines[pid] = HeuristicWatermarkEngine(
                    partition_id=pid, worker_id=node_id,
                    db_path=f"{ckpt_dir}/rocksdb-heuristic-{node_id}-p{pid}",
                    tiered_storage=tiered_storage,
                    checkpoint_dir=ckpt_dir,
                )
                downstream_emitter.schedule_final_reconciliation(stop, engines[pid])
            le = LogEvent(
                event_id=str(ev.get("event_id", f"ev-{time.time_ns()}")),
                event_time=float(ev.get("event_time", time.time())),
                status=int(ev.get("status", 200)),
                arrival_time=float(ev.get("arrival_time", time.time())),
                poll_received_at=time.time(),
                schema_version=int(ev.get("schema_version", 1)),
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
            total_recv = 0
            total_on = 0
            total_late = 0
            total_dupes = 0
            total_bp = 0
            total_non_mono = 0
            total_dlq = 0
            total_sketch = 0
            for pid, eng in list(self.engines.items()):
                es = eng.summary()
                result["partitions"][pid] = es
                total_recv += es.get("total_received", 0)
                total_on += es.get("on_time", 0)
                total_late += es.get("late_dropped", 0)
                total_dupes += es.get("duplicates", 0)
                total_bp += es.get("backpressure_drops", 0)
                total_non_mono += es.get("non_monotonic_punctuation", 0)
                total_dlq += es.get("dlq_backlog", 0)
                total_sketch += es.get("sketch_total_count", 0)
            uniq = max(total_recv - total_dupes, 1)
            result["total_received"] = total_recv
            result["on_time"] = total_on
            result["late_dropped"] = total_late
            result["duplicates"] = total_dupes
            result["backpressure_drops"] = total_bp
            result["non_monotonic_punctuation"] = total_non_mono
            result["data_completeness_pct"] = round(100.0 * total_on / uniq, 3)
            result["late_arrival_rate_pct"] = round(100.0 * total_late / max(total_recv, 1), 3)
            result["dlq_backlog"] = total_dlq
            result["sketch_total_count"] = total_sketch
            return result

        def broadcast(self):
            result = {"node_id": self.node_id, "partitions": {}}
            for pid, eng in list(self.engines.items()):
                result["partitions"][pid] = {"W_h": eng.W_h, "L_eff": eng.L_eff}
            return result

        def checkpoint(self):
            """Persist engine metadata to RocksDB across all partitions."""
            for eng in list(self.engines.values()):
                eng.checkpoint()

        def flush(self):
            """Flush open windows and save cold-start baseline for graceful shutdown."""
            for pid, eng in list(self.engines.items()):
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
            for eng in list(self.engines.values()):
                eng.close()

    proxy = HeuristicWorkerProxy(engines, node_id, tiered_storage=tiered_storage,
                                 kafka_producer=kafka_producer,
                                 kafka_results_topic=kafka_results_topic)

    state = {
        "role": "worker",
        "ready": True,
        "component": proxy,
        "worker_id": node_id,
        "tiered_storage": tiered_storage,
        "backpressure_controller": bp,
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
    for pid, eng in list(engines.items()):
        downstream_emitter.schedule_final_reconciliation(stop, eng)

    # Kafka consumer poll loop for heuristic worker
    if kafka_consumer is not None:
        def kafka_heuristic_poll_loop():
            committed_offsets: dict[int, int] = {}
            last_lag_report = 0.0
            last_progress_log = 0.0
            poll_total = [0]
            while not stop.is_set():
                try:
                    # Backpressure: skip poll if all partitions are paused (spec §11.1)
                    if bp.paused_partitions() and all(bp.is_paused(pid) for pid in parts):
                        time.sleep(0.1)
                        continue
                    polled = kafka_consumer.poll(timeout_ms=500, max_messages=100)
                    for pid, msgs in polled.items():
                        if pid not in engines:
                            # Dynamically initialize heuristic engine
                            from heuristic.engine import HeuristicWatermarkEngine
                            engines[pid] = HeuristicWatermarkEngine(
                                partition_id=pid, worker_id=node_id,
                                db_path=f"{ckpt_dir}/rocksdb-heuristic-{node_id}-p{pid}",
                                tiered_storage=tiered_storage,
                                checkpoint_dir=ckpt_dir,
                            )
                            downstream_emitter.schedule_final_reconciliation(stop, engines[pid])
                        if bp.is_paused(pid):
                            continue
                        eng = engines[pid]
                        for msg in msgs:
                            value = json.loads(msg["value"]) if isinstance(msg["value"], str) else msg["value"]
                            if isinstance(value, dict) and value.get("is_punctuation"):
                                committed_offsets[pid] = msg["offset"] + 1
                                poll_total[0] += 1
                                continue
                            ev_raw = value if isinstance(value, dict) else {"payload": str(value)}
                            ev = parse_and_deduplicate_event(ev_raw, seen_event_ids)
                            if ev is None:
                                continue
                            le = LogEvent(
                                event_id=str(ev.get("event_id", f"kafka-{msg['offset']}")),
                                event_time=float(ev.get("event_time", time.time())),
                                status=int(ev.get("status", 200)),
                                arrival_time=float(ev.get("arrival_time", time.time())),
                                poll_received_at=time.time(),
                                offset=msg["offset"],
                                schema_version=int(ev.get("schema_version", 1)),
                            )
                            eng.process(le)
                            committed_offsets[pid] = msg["offset"] + 1
                            poll_total[0] += 1
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
                        # Report buffer depth for backpressure tracking.
                        # In heuristic mode open_windows accumulates across
                        # the entire event-time range (can be millions of
                        # 5-second windows). Using that as the buffer metric
                        # causes an immediate, permanent backpressure deadlock.
                        # Instead, report the actual inbound queue depth.
                        bp.report_buffer(node_id, pid, eng.queue_size)
                    if committed_offsets:
                        kafka_consumer.commit(committed_offsets)
                        committed_offsets.clear()
                    now_t = time.time()
                    # Progress log every 5s
                    if now_t - last_progress_log >= 5.0 and poll_total[0] > 0:
                        total_recv = sum(e.metrics.total_received for e in engines.values())
                        total_on = sum(e.metrics.on_time for e in engines.values())
                        total_late = sum(e.metrics.late_dropped for e in engines.values())
                        total_dlq = sum(len(e.late_events) for e in engines.values())
                        uniq = max(total_recv, 1)
                        comp = 100.0 * total_on / uniq
                        part_info = " ".join(f"p{p}:W_h={e.W_h:.1f}" for p, e in engines.items())
                        print(f"[worker-heuristic:{node_id}] processed={total_recv} "
                              f"on_time={total_on} late={total_late} "
                              f"completeness={comp:.1f}% "
                              f"dlq_backlog={total_dlq} "
                              f"[{part_info}]", file=sys.stderr)
                        last_progress_log = now_t
                    # Report Kafka partition lag every 5s
                    if now_t - last_lag_report >= 5.0 and mon_mgr is not None:
                        for pid in kafka_consumer.assigned_partitions():
                            lag = kafka_consumer.lag("events", pid)
                            mon_mgr.update_kafka_lag("events", "heuristic-workers",
                                                     f"heuristic-{node_id}", pid, lag)
                        last_lag_report = now_t
                except Exception:
                    import traceback
                    traceback.print_exc()
                    time.sleep(0.1)
        threading.Thread(target=kafka_heuristic_poll_loop, daemon=True).start()

    def report_loop():
        import urllib.request
        while not stop.is_set():
            time.sleep(0.2)
            for pid, eng in engines.items():
                sent = False
                if grpc is not None:
                    try:
                        stub = get_grpc_aggregator_stub(aggregator_url)
                        if stub is not None:
                            stub.SendWorkerWatermark(
                                csdlpt_pb2.WorkerWatermarkMsg(
                                    worker_id=node_id,
                                    partition_id=pid,
                                    W_h=eng.W_h,
                                ),
                                timeout=1
                            )
                            sent = True
                    except Exception:
                        pass
                    if aggregator_standby_url != aggregator_url:
                        try:
                            stub = get_grpc_aggregator_stub(aggregator_standby_url)
                            if stub is not None:
                                stub.SendWorkerWatermark(
                                    csdlpt_pb2.WorkerWatermarkMsg(
                                        worker_id=node_id,
                                        partition_id=pid,
                                        W_h=eng.W_h,
                                    ),
                                    timeout=1
                                )
                        except Exception:
                            pass
                if not sent:
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
                # §12 oldest-entry age — surfaces stalled DLQ consumers before
                # the hourly correction loop next fires.
                mon_mgr.dlq_oldest_entry_age_s.labels(worker_id=node_id).set(
                    dlq.oldest_entry_age_s())
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
                    # §12.6 SLA: a window emitted while the owning engine was
                    # in adaptive/burst mode gets a tighter 15-min correction
                    # deadline. Look up the partition's current burst state and
                    # classify per-correction so DownstreamEmitter.check_sla
                    # can apply the right threshold.
                    for corr in corrections:
                        burst = False
                        try:
                            pid = int(corr.window_id.split("_", 1)[0])
                            eng = engines.get(pid)
                            if eng is not None and getattr(eng, "in_burst", False):
                                burst = True
                        except (ValueError, IndexError):
                            pass
                        downstream_emitter.enqueue(
                            corr, window_type=("burst" if burst else "normal"))
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

    # Periodic checkpoint thread — persist engine metadata every 10s
    def checkpoint_loop():
        while not stop.is_set():
            time.sleep(10.0)
            try:
                for eng in engines.values():
                    eng.checkpoint()
            except Exception:
                pass
    threading.Thread(target=checkpoint_loop, daemon=True).start()

    # Progress log loop (HTTP-only / non-Kafka path)
    def progress_log_loop():
        while not stop.is_set():
            time.sleep(5.0)
            total_recv = sum(e.metrics.total_received for e in engines.values())
            if total_recv == 0:
                continue
            total_on = sum(e.metrics.on_time for e in engines.values())
            total_late = sum(e.metrics.late_dropped for e in engines.values())
            total_dlq = sum(len(e.late_events) for e in engines.values())
            uniq = max(total_recv, 1)
            comp = 100.0 * total_on / uniq
            part_info = " ".join(f"p{p}:W_h={e.W_h:.1f}" for p, e in sorted(engines.items()))
            print(f"[worker-heuristic:{node_id}] processed={total_recv} "
                  f"on_time={total_on} late={total_late} "
                  f"completeness={comp:.1f}% "
                  f"dlq_backlog={total_dlq} "
                  f"[{part_info}]", file=sys.stderr)
    threading.Thread(target=progress_log_loop, daemon=True).start()

    # W_global_h fetch loop — pull global watermark from aggregator every 500ms
    def wglobal_fetch_loop():
        import urllib.request
        while not stop.is_set():
            time.sleep(0.5)
            W_global_h = None
            try:
                req = urllib.request.Request(aggregator_url + "/state")
                with urllib.request.urlopen(req, timeout=2) as resp:
                    data = json.loads(resp.read().decode())
                    if data.get("ha_active", True) or aggregator_url == aggregator_standby_url:
                        W_global_h = float(data["W_global_h"])
            except Exception:
                pass

            if W_global_h is None and aggregator_standby_url != aggregator_url:
                try:
                    req = urllib.request.Request(aggregator_standby_url + "/state")
                    with urllib.request.urlopen(req, timeout=2) as resp:
                        data = json.loads(resp.read().decode())
                        if data.get("ha_active", True):
                            W_global_h = float(data["W_global_h"])
                except Exception:
                    pass

            if W_global_h is not None and W_global_h > float("-inf"):
                for eng in engines.values():
                    eng.update_global_watermark(W_global_h)
    threading.Thread(target=wglobal_fetch_loop, daemon=True).start()

    print(f"[worker-heuristic:{node_id}] listening on :{port}, partitions={parts}")
    _wait_shutdown(stop, srv, state)


def _run_hybrid_worker(node_id, parts, args):
    """Hybrid worker: routes CRITICAL events to strict engine, STANDARD to heuristic engine.

    Uses HybridRouter per partition for unified window results and loss accounting.
    """
    from common.config import Config
    cfg = Config()
    if not cfg.enable_hybrid_routing:
        print("[worker-hybrid] ENABLE_HYBRID_ROUTING is disabled. Set ENABLE_HYBRID_ROUTING=true to enable hybrid mode.")
        return

    from hybrid.router import HybridRouter, EventPriority
    from common.types import LogEvent, PunctuationToken
    from strict.backpressure import BackpressureController

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

    seen_event_ids = set()

    def ingest_handler(events):
        count = 0
        for ev_raw in events:
            ev = parse_and_deduplicate_event(ev_raw, seen_event_ids)
            if ev is None:
                continue
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
                arrival_time=float(ev.get("arrival_time", time.time())),
                poll_received_at=time.time(),
                schema_version=int(ev.get("schema_version", 1)),
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

    csv_reader = None
    csv_file = None
    csv_first_time = None
    csv_wall_base = time.time()
    last_simulated_arrival_time = [csv_wall_base]
    events = []
    if source and os.path.exists(source):
        if source.endswith('.csv'):
            import csv as _csv
            csv_file = open(source, encoding='utf-8')
            csv_reader = _csv.DictReader(csv_file)

            # Detect CSV format and set up column mapping + time parser
            csv_col_map = None
            csv_time_parser = None
            if csv_reader.fieldnames:
                fields = set(csv_reader.fieldnames)
                if 'remote_addr' in fields and 'timestamp' in fields:
                    from datetime import datetime, timezone, timedelta
                    csv_col_map = {
                        'host': 'remote_addr',
                        'time': 'timestamp',
                        'response': 'status',
                        'method': 'method',
                        'url': 'path',
                        'bytes': 'body_bytes_sent',
                    }
                    def _parse_access_log_time(ts_str):
                        dt = datetime.strptime(ts_str, "%d/%b/%Y:%H:%M:%S %z")
                        return dt.timestamp()
                    csv_time_parser = _parse_access_log_time
                    print(f"[ingestor] detected access-log CSV format, mapping columns",
                          file=sys.stderr)

            def _map_row(row):
                if csv_col_map is None:
                    return row
                mapped = dict(row)
                for std_col, log_col in csv_col_map.items():
                    val = row.get(log_col, '')
                    if std_col == 'time' and csv_time_parser and val:
                        try:
                            mapped['time'] = str(csv_time_parser(val))
                        except (ValueError, OSError):
                            mapped['time'] = val
                        continue
                    mapped[std_col] = val
                return mapped

            try:
                first_row = _map_row(next(csv_reader))
                csv_first_time = float(first_row.get('time', '0'))
            except StopIteration:
                pass

            csv_file.seek(0)
            csv_reader = _csv.DictReader(csv_file)
        else:
            with open(source) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        events.append(json.loads(line))
    if not csv_reader and not events:
        base = time.time()
        for i in range(1000):
            events.append({
                "event_id": f"syn-{i}",
                "event_time": base - random.uniform(0, 5),
                "status": random.choice([200, 200, 200, 200, 500]),
                "partition_id": i % 12,
            })

    csv_row_counter = [0]

    def _next_csv_event():
        """Read next row from CSV, normalize timestamp, return event dict or None on EOF."""
        nonlocal csv_first_time
        try:
            row = _map_row(next(csv_reader))
        except StopIteration:
            return None
        host = row.get('host', '')
        csv_time = float(row.get('time', '0'))
        if csv_first_time is None:
            csv_first_time = csv_time
        offset = csv_time - csv_first_time
        event_time = csv_wall_base + offset
        sim_lag = float(os.environ.get("SIMULATED_LAG_S", "2.0"))
        next_arr = max(last_simulated_arrival_time[0] + 0.001, event_time + sim_lag)
        last_simulated_arrival_time[0] = next_arr
        csv_row_counter[0] += 1
        return {
            "event_id": f"csv-{csv_row_counter[0]}",
            "event_time": event_time,
            "arrival_time": next_arr,
            "status": int(row.get('response', '200')),
            "partition_id": hash(host) % 12,
            "payload": {
                "host": host,
                "method": row.get('method', ''),
                "url": row.get('url', ''),
                "bytes": int(row.get('bytes', '0')),
            },
        }

    # Kafka producer mode
    kafka_producer = None
    kafka_broker_url = args.kafka_broker_url or os.environ.get("KAFKA_BROKER_URL", "")
    enable_kafka = args.enable_kafka or os.environ.get("KAFKA_ENABLE", "false").lower() in ("1", "true", "yes", "on")
    if enable_kafka and kafka_broker_url:
        if os.environ.get("KAFKA_ENABLE_REAL", "false").lower() in ("1", "true", "yes", "on"):
            from common.kafka_real import KafkaProducer, ensure_topics
            ensure_topics(kafka_broker_url, ["events", "strict_results", "heuristic_results", "audit_results"], num_partitions=12)
        else:
            from common.kafka_sim import KafkaProducer
        kafka_producer = KafkaProducer(broker_url=kafka_broker_url)
        print(f"[ingestor] using Kafka producer -> {kafka_broker_url}", file=sys.stderr)

    total_parts = int(os.environ.get("TOTAL_PARTITIONS", "12"))
    node_for_partition = {}
    parts_per_host = max(1, total_parts // max(1, len(node_hosts)))
    for i, host in enumerate(node_hosts):
        start = i * parts_per_host
        end = min((i + 1) * parts_per_host, total_parts)
        for p in range(start, end):
            node_for_partition[p] = host

    ingested = [0]
    ingested_total = [0]
    last_punctuation = [time.time()]
    punctuation_interval = 1.0
    events_sent_since_punctuation = [0]
    last_log_offsets: dict[int, int] = {}
    eof_reached = [False]  # shared with send_punctuation for EOF flush

    # Data-driven punctuation: track max event_time sent globally and per-partition.
    # In simulation/CSV mode, T_commit must be driven by the actual data timestamps,
    # not wall clock, otherwise the watermark advances faster than event_time and
    # events are incorrectly marked as late (Completeness < 100%).
    #
    # Key insight: T_commit = max_event_time_sent causes the watermark to sit at
    # max_et - delta_base. If the CSV spans > delta_base + window_size (25s with
    # defaults), older events get window_end <= watermark → LATE.
    #
    # Fix: track min_event_time_sent and set T_commit = min_et, so the watermark
    # stays behind ALL events in the CSV. In simulation, all events are "historical"
    # from the system's perspective, so none should be late.
    max_event_time_sent: float = 0.0
    # In replay mode the CSV can be heavily out of order. Keep the watermark
    # below all real event timestamps until EOF, then jump it forward to flush.
    min_event_time_sent: float = 0.0
    max_event_time_per_part: dict[int, float] = {}
    punctuation_mode = os.environ.get("PUNCTUATION_MODE", "data-driven")
    # Log level for tracing event flow through the pipeline
    log_level = os.environ.get("LOG_LEVEL", "info").lower()
    _log_trace = log_level in ("debug", "trace")

    def _update_max_event_time(ev):
        nonlocal max_event_time_sent, min_event_time_sent
        et = float(ev.get("event_time", 0.0))
        if et > max_event_time_sent:
            max_event_time_sent = et
        if et < min_event_time_sent:
            min_event_time_sent = et
        pid = ev.get("partition_id", 0)
        if et > max_event_time_per_part.get(pid, 0.0):
            max_event_time_per_part[pid] = et

    def send_event_kafka(ev):
        """Send event via KafkaProducer (acks=all)."""
        _update_max_event_time(ev)
        result = kafka_producer.send("events", ev, key=ev.get("event_id", ""),
                                     partition=ev.get("partition_id", 0) % 12,
                                     sync=False)
        if "error" not in result:
            ingested[0] += 1
            ingested_total[0] += 1
            events_sent_since_punctuation[0] += 1
            if _log_trace:
                print(f"[ingestor→kafka] eid={ev.get('event_id','?')} "
                      f"et={ev.get('event_time',0):.3f} pid={ev.get('partition_id',0)} "
                      f"status={ev.get('status',0)} offset={result.get('offset','?')}",
                      file=sys.stderr)

    def send_event(ev, host):
        _update_max_event_time(ev)
        try:
            data = json.dumps([ev]).encode()
            req = urllib.request.Request(
                f"http://{host}/ingest",
                data=data,
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=1)
            ingested[0] += 1
            ingested_total[0] += 1
            events_sent_since_punctuation[0] += 1
            if _log_trace:
                print(f"[ingestor→{host}] eid={ev.get('event_id','?')} "
                      f"et={ev.get('event_time',0):.3f} pid={ev.get('partition_id',0)} "
                      f"status={ev.get('status',200)}",
                      file=sys.stderr)
        except Exception as e:
            import traceback
            print(f"[ingestor] send_event to {host} failed: {e}", file=sys.stderr)
            if _log_trace:
                traceback.print_exc(file=sys.stderr)

    def send_punctuation():
        """Emit per-partition punctuation tokens.

        In data-driven mode (default for CSV/simulation):
          - While ingesting: T_commit = min_event_time_sent keeps the watermark
            behind ALL events, guaranteeing 100% completeness regardless of
            CSV time span. Windows accumulate but don't close prematurely.
          - After EOF: T_commit = max_event_time_sent + delta_base_s flushes
            all accumulated windows at once, simulating a clean shutdown.

        In wall-clock mode: T_commit = now - 10s (real-time streaming).
        """
        is_empty = events_sent_since_punctuation[0] == 0
        events_sent_since_punctuation[0] = 0

        if punctuation_mode == "wall-clock":
            T_commit = time.time() - 10.0
        elif punctuation_mode == "max-event-time":
            # Driven by max_event_time_sent for sorted datasets to progress watermark dynamically
            if not eof_reached[0] and max_event_time_sent > 0:
                T_commit = max_event_time_sent
            elif eof_reached[0] and max_event_time_sent > 0:
                T_commit = max_event_time_sent + float(os.environ.get("DELTA_BASE_S", "10.0"))
            else:
                T_commit = time.time()
        else:
            # Data-driven (default/min-event-time): anchor watermark at the oldest event so nothing is
            # ever late. After EOF, jump to max_et + delta_base to close all windows in one shot.
            if not eof_reached[0] and min_event_time_sent != float("inf"):
                # Ingesting phase: hold watermark behind all events
                T_commit = min_event_time_sent
            elif eof_reached[0] and max_event_time_sent > 0:
                # EOF phase: flush all windows
                T_commit = max_event_time_sent + float(os.environ.get("DELTA_BASE_S", "10.0"))
            else:
                T_commit = time.time()

        print(f"[ingestor] punctuation T_commit={T_commit:.3f} "
              f"min_et={min_event_time_sent:.3f} max_et={max_event_time_sent:.3f} "
              f"eof={eof_reached[0]} mode={punctuation_mode} is_empty={is_empty}",
              file=sys.stderr)

        for pid, host in node_for_partition.items():
            try:
                pid_T_commit = T_commit
                if kafka_producer is not None:
                    kafka_producer.send("events", {
                        "is_punctuation": True,
                        "T_commit": pid_T_commit,
                        "partition_id": pid,
                        "ingestor_id": "ingestor-main",
                        "is_empty": is_empty,
                    }, key=f"punct-{pid}-{pid_T_commit}", partition=pid % 12)
                else:
                    data = json.dumps({
                        "T_commit": pid_T_commit,
                        "partition_id": pid,
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
                pass  # KAFKA_ENABLE=false: punctuation over HTTP best-effort

    from common.schema_registry import validate_json_schema, LOG_EVENT_V1_SCHEMA, LOG_EVENT_V2_SCHEMA

    migration_step = os.environ.get("SCHEMA_MIGRATION_STEP", "v1_only").lower()

    def create_v1_event(ev):
        ev_v1 = dict(ev)
        ev_v1["schema_version"] = 1
        if "status" not in ev_v1:
            ev_v1["status"] = 200
        return ev_v1

    def create_v2_event(ev):
        ev_v2 = dict(ev)
        ev_v2["schema_version"] = 2
        status = ev_v2.pop("status", 200)
        ev_v2["http_status"] = status
        host = ev.get("payload", {}).get("host", "") if isinstance(ev.get("payload"), dict) else ""
        ev_v2["service_name"] = host or "synthetic-service"
        return ev_v2

    stop = threading.Event()

    def loop():
        idx = 0
        eof = False
        last_report = time.time()
        while not stop.is_set():
            if csv_reader is not None:
                if not eof:
                    ev = _next_csv_event()
                    if ev is None:
                        eof = True
                        eof_reached[0] = True
                        print(f"[ingestor] CSV EOF at event {idx}, switching to idle loop", file=sys.stderr)
                        if kafka_producer is not None:
                            kafka_producer.flush()
                        continue
                else:
                    # EOF reached, just send punctuations on idle loop
                    time.sleep(0.1)
                    if mode == "strict" and (time.time() - last_punctuation[0]) > punctuation_interval:
                        send_punctuation()
                        last_punctuation[0] = time.time()
                    continue
            else:
                ev = events[idx % len(events)]
            if "arrival_time" not in ev:
                ev["arrival_time"] = ev.get("event_time", time.time()) + 2.0
            pid = ev.get("partition_id", 0)

            to_send = []
            if migration_step == "drop_legacy":
                to_send.append(create_v2_event(ev))
            elif migration_step in ("dual_emit", "dual_consume", "switch_primary"):
                to_send.append(create_v1_event(ev))
                to_send.append(create_v2_event(ev))
            else:
                to_send.append(create_v1_event(ev))

            for msg_ev in to_send:
                sch = LOG_EVENT_V2_SCHEMA if msg_ev.get("schema_version") == 2 else LOG_EVENT_V1_SCHEMA
                validate_json_schema(msg_ev, sch)

                if kafka_producer is not None:
                    send_event_kafka(msg_ev)
                else:
                    host = node_for_partition.get(pid, node_hosts[0])
                    send_event(msg_ev, host)

            last_log_offsets[pid] = idx
            idx += 1
            
            sleep_s = float(os.environ.get("INGESTOR_SLEEP_S", "0.01"))
            if sleep_s > 0:
                time.sleep(sleep_s)

            if mode == "strict" and (time.time() - last_punctuation[0]) > punctuation_interval:
                send_punctuation()
                last_punctuation[0] = time.time()

            # Progress report every 5s
            now = time.time()
            if now - last_report >= 5.0:
                rate = ingested[0] / max(now - last_report, 0.1)
                print(f"[ingestor] progress: {idx} CSV rows read, "
                      f"{ingested_total[0]} total sent to Kafka, "
                      f"rate={rate:.0f} ev/s, "
                      f"et_range=[{min_event_time_sent:.3f}..{max_event_time_sent:.3f}] "
                      f"span={max_event_time_sent - min_event_time_sent:.1f}s "
                      f"partitions_active={len(last_log_offsets)} "
                      f"eof={eof_reached[0]}",
                      file=sys.stderr)
                ingested[0] = 0
                last_report = now

    threading.Thread(target=loop, daemon=True).start()

    coordinator_url = os.environ.get("COORDINATOR_URL", "")

    def heartbeat_loop():
        import urllib.request as _req
        while not stop.is_set():
            time.sleep(5.0)
            if not coordinator_url:
                continue
            partitions_assigned = list(last_log_offsets.keys())
            T_commit = time.time() - 10.0
            timestamp = time.time()
            last_log_offset = max(last_log_offsets.values()) if last_log_offsets else 0
            ingestor_clock = time.time()
            offsets = dict(last_log_offsets)
            
            sent = False
            if grpc is not None:
                try:
                    stub = get_grpc_stub(coordinator_url)
                    if stub is not None:
                        msg = csdlpt_pb2.IngestorHeartbeatMsg(
                            ingestor_id="ingestor-main",
                            T_commit=T_commit,
                            timestamp=timestamp,
                            partitions_assigned=partitions_assigned,
                            last_log_offset=last_log_offset,
                            ingestor_clock=ingestor_clock,
                            offsets=offsets,
                        )
                        stub.IngestorHeartbeat(msg, timeout=2)
                        sent = True
                except Exception:
                    pass
            
            if not sent:
                try:
                    payload = json.dumps({
                        "ingestor_id": "ingestor-main",
                        "T_commit": T_commit,
                        "timestamp": timestamp,
                        "partitions_assigned": partitions_assigned,
                        "last_log_offset": last_log_offset,
                        "ingestor_clock": ingestor_clock,
                        "offsets": offsets,
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
    grpc_srv = state.get("grpc_server")
    if grpc_srv is not None:
        try:
            print("[shutdown] stopping gRPC server...", file=sys.stderr)
            grpc_srv.stop(0)
        except Exception as exc:
            print(f"[shutdown] stopping gRPC server failed: {exc}", file=sys.stderr)
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
    # Configure standard logging format and level
    _log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    _numeric_level = getattr(logging, _log_level, logging.INFO)
    logging.basicConfig(
        level=_numeric_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stderr)]
    )

    _args = parse_args()
    _fn = ROLE_DISPATCH.get(_args.role)
    if _fn is None:
        print(f"Unknown role: {_args.role}", file=sys.stderr)
        sys.exit(1)
    _fn(_args)
