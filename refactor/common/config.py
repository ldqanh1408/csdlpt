"""Centralized configuration — reads all parameters from environment variables.

Single source of truth replacing scattered os.environ.get() calls across run.py,
coordinator, worker, and engines. All values have spec-defined defaults.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)))
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    val = os.environ.get(name, str(default)).lower()
    return val in ("1", "true", "yes", "on")


def _env_list(name: str, default: list[int]) -> list[int]:
    raw = os.environ.get(name, "")
    if not raw:
        return list(default)
    return [int(x.strip()) for x in raw.split(",") if x.strip()]


@dataclass
class Config:
    """System-wide configuration with spec-defined defaults."""

    mode: str = os.environ.get("MODE", "strict")
    role: str = os.environ.get("ROLE", "worker")
    port: int = _env_int("PORT", 8000)
    node_hosts: list[str] = field(default_factory=lambda:
        [h.strip() for h in os.environ.get("NODE_HOSTS", "localhost:9101").split(",")])
    window_size_s: float = _env_float("WINDOW_SIZE_S", 5.0)
    delta_base_s: float = _env_float("DELTA_BASE_S", 10.0)
    partition_ids: list[int] = field(default_factory=lambda: _env_list("PARTITIONS", [0, 1, 2]))
    total_partitions: int = _env_int("TOTAL_PARTITIONS", 12)
    node_id: str = os.environ.get("NODE_ID", "0")
    worker_id: str = os.environ.get("WORKER_ID", os.environ.get("NODE_ID", "0"))
    aggregator_url: str = os.environ.get("AGGREGATOR_URL", "http://localhost:8001")
    checkpoint_dir: str = os.environ.get("CHECKPOINT_DIR", "/data/checkpoint")
    db_enabled: bool = _env_bool("DB_ENABLED", True)
    minio_endpoint: str = os.environ.get("MINIO_ENDPOINT", "")
    minio_access_key: str = os.environ.get("MINIO_ACCESS_KEY", "")
    minio_secret_key: str = os.environ.get("MINIO_SECRET_KEY", "")
    minio_bucket: str = os.environ.get("MINIO_BUCKET", "csdlpt-windows")
    minio_secure: bool = _env_bool("MINIO_SECURE", False)
    pagerduty_routing_key: str = os.environ.get("PAGERDUTY_ROUTING_KEY", "")
    backpressure_max_queue: int = _env_int("BACKPRESSURE_MAX_QUEUE", 500)
    backpressure_resume_at: int = _env_int("BACKPRESSURE_RESUME_AT", 100)
    heartbeat_timeout_s: float = _env_float("HEARTBEAT_TIMEOUT_S", 10.0)
    failover_enabled: bool = _env_bool("FAILOVER_ENABLED", False)
    coordinator_id: str = os.environ.get("COORDINATOR_ID", "")
    coordinator_peers: list[str] = field(default_factory=lambda:
        [h.strip() for h in os.environ.get("COORDINATOR_PEERS", "").split(",") if h.strip()])
    dr_backup_interval_s: float = _env_float("DR_BACKUP_INTERVAL_S", 300.0)
    heuristic_alpha: float = _env_float("HEURISTIC_ALPHA", 0.01)
    heuristic_p_normal: float = _env_float("HEURISTIC_P_NORMAL", 0.99)
    heuristic_p_safe: float = _env_float("HEURISTIC_P_SAFE", 0.999)
    heuristic_L_max: float = _env_float("HEURISTIC_L_MAX", 60.0)
    heuristic_warmup_s: float = _env_float("HEURISTIC_WARMUP_S", 10.0)
    heuristic_warmup_samples: int = _env_int("HEURISTIC_WARMUP_SAMPLES", 1000)
    dlq_retention_days: int = _env_int("DLQ_RETENTION_DAYS", 7)
    dlq_retry_batch_size: int = _env_int("DLQ_RETRY_BATCH_SIZE", 100)
    dlq_retry_interval_s: float = _env_float("DLQ_RETRY_INTERVAL_S", 3600.0)
    aggregator_lock_path: str = os.environ.get("AGGREGATOR_LOCK_PATH", "/tmp/aggregator.lock")
    aggregator_ha_enabled: bool = _env_bool("AGGREGATOR_HA_ENABLED", False)
    tls_cert_file: str = os.environ.get("TLS_CERT_FILE", "")
    tls_key_file: str = os.environ.get("TLS_KEY_FILE", "")
    metrics_enabled: bool = _env_bool("METRICS_ENABLED", True)
    alert_interval_s: float = _env_float("ALERT_INTERVAL_S", 10.0)
    source_path: str = os.environ.get("SOURCE", "")
    punctuation_interval_s: float = _env_float("PUNCTUATION_INTERVAL_S", 1.0)
    wm_emit_interval_s: float = _env_float("WM_EMIT_INTERVAL_S", 0.2)
    agg_emit_interval_s: float = _env_float("AGG_EMIT_INTERVAL_S", 0.5)
    # Feature flags (§6.4)
    enable_adaptive_percentile: bool = _env_bool("ENABLE_ADAPTIVE_PERCENTILE", True)
    enable_replay_sub_checkpointing: bool = _env_bool("ENABLE_REPLAY_SUB_CHECKPOINTING", True)
    enable_two_phase_eviction: bool = _env_bool("ENABLE_TWO_PHASE_EVICTION", True)
    enable_negative_lag_recalibration: bool = _env_bool("ENABLE_NEGATIVE_LAG_RECALIBRATION", True)
    enable_hybrid_routing: bool = _env_bool("ENABLE_HYBRID_ROUTING", False)

    @property
    def minio_configured(self) -> bool:
        return bool(self.minio_endpoint)

    @property
    def db_path(self) -> str:
        if not self.db_enabled:
            return ""
        return f"{self.checkpoint_dir}/rocksdb-{self.mode}-{self.node_id}"
