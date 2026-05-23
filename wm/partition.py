"""wm.partition — horizontal fragmentation (DESIGN.md §3.1).

Phân vùng stream qua N engines bằng hash(host) % N. Mỗi engine = 1 "node"
độc lập có state + checkpoint riêng. Aggregator gộp metrics cuối stream.
"""
import hashlib
import os
import tempfile

from wm.engine import WatermarkEngine
from wm.config import EngineConfig

CONFIG = EngineConfig()
TMP = tempfile.gettempdir()


def partition_key(host: str, n_nodes: int) -> int:
    """Hash(host) % N — horizontal fragmentation theo host (DESIGN.md §3.1)."""
    h = hashlib.md5(str(host).encode()).digest()
    return int.from_bytes(h[:4], "little") % n_nodes


def run_cluster(df, allowed_lateness_ms: float, n_nodes: int):
    """Chạy stream qua N engine, mỗi engine xử lý partition riêng.
    Trả về tổng hợp metrics toàn cluster (completeness, late_dropped,
    windows) cùng phân phối event theo node để đo hot-key skew."""
    engines = [
        WatermarkEngine(
            window_size_s=CONFIG.window_size_s,
            allowed_lateness_s=allowed_lateness_ms / 1000.0,
            checkpoint_interval=5000,
            checkpoint_path=os.path.join(
                TMP, f"cluster_node{i}_{int(allowed_lateness_ms)}.json"),
            max_queue=10_000_000,
        )
        for i in range(n_nodes)
    ]

    per_node_counts = [0] * n_nodes
    for row in df.itertuples(index=False):
        host = getattr(row, "host", "unknown")
        nid = partition_key(host, n_nodes)
        per_node_counts[nid] += 1
        engines[nid].process(
            {"event_id": row.event_id, "event_time": row.event_time,
             "status": row.status},
            queue_len=0)

    for eng in engines:
        eng.flush()

    agg = {"total": 0, "unique": 0, "duplicates": 0, "on_time": 0,
           "late_dropped": 0, "backpressure_drops": 0, "windows": 0}
    for eng in engines:
        for k in ("total", "unique", "duplicates", "on_time",
                  "late_dropped", "backpressure_drops"):
            agg[k] += eng.metrics[k]
        agg["windows"] += len(eng.closed_windows)

    completeness = 100.0 * agg["on_time"] / max(agg["unique"], 1)
    return {
        "allowed_lateness_ms": allowed_lateness_ms,
        "completeness_pct": round(completeness, 3),
        "late_dropped": agg["late_dropped"],
        "duplicates": agg["duplicates"],
        "windows": agg["windows"],
        "per_node_counts": per_node_counts,
    }
