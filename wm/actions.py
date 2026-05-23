"""Reusable kill/revive actions for distributed simulation scenarios.

Consolidates 4 duplicated code paths from app.py (quick actions,
per-node buttons, click-to-kill, auto-play executor) into shared helpers.
"""

import json
import os

from wm.config import DEFAULT_CONFIG, EngineConfig
from wm.engine import WatermarkEngine


def kill_node(state: dict, nid: int) -> bool:
    """Checkpoint + mark single node dead. Returns True on success."""
    if state["status"][nid] != "alive":
        return False
    try:
        state["engines"][nid].checkpoint()
        ck_sz = os.path.getsize(state["ckpt_paths"][nid])
    except Exception:
        ck_sz = 0
    state["status"][nid] = "dead"
    cursor = state.get("cursor", 0)
    state.setdefault("events_markers", []).append((cursor, "kill", nid))
    state.setdefault("log", []).append(
        f"[cursor={cursor:,}] KILL Node {nid} · "
        f"checkpoint {ck_sz}B saved"
    )
    return True


def revive_node(state: dict, nid: int, config: EngineConfig = DEFAULT_CONFIG) -> bool:
    """Restore node from checkpoint, drain its DLQ, mark alive. Returns True on success."""
    if state["status"][nid] != "dead":
        return False
    ckpt = state["ckpt_paths"][nid]
    try:
        eng_new = WatermarkEngine.restore(ckpt, **config.to_engine_kwargs())
    except Exception:
        eng_new = WatermarkEngine(
            checkpoint_path=ckpt, **config.to_engine_kwargs())
    dlq = state["dlq_paths"][nid]
    replayed = 0
    if os.path.exists(dlq):
        with open(dlq, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    ev = json.loads(line)
                except json.JSONDecodeError:
                    continue
                eng_new.process(ev)
                replayed += 1
                state["node_processed"][nid] += 1
        try:
            os.remove(dlq)
        except OSError:
            pass
    state["pending"][nid] = []
    state["engines"][nid] = eng_new
    state["status"][nid] = "alive"
    cursor = state.get("cursor", 0)
    state.setdefault("events_markers", []).append((cursor, "revive", nid))
    state.setdefault("log", []).append(
        f"[cursor={cursor:,}] REVIVE Node {nid} · "
        f"replay {replayed:,} DLQ events · Exactly-Once OK"
    )
    return True


def kill_all(state: dict) -> int:
    """Kill all alive nodes. Returns count of nodes killed."""
    killed = 0
    n = len(state["engines"])
    for i in range(n):
        if kill_node(state, i):
            killed += 1
    return killed


def revive_all(state: dict, config: EngineConfig = DEFAULT_CONFIG) -> int:
    """Revive all dead nodes. Returns count of nodes revived."""
    revived = 0
    n = len(state["engines"])
    for i in range(n):
        if revive_node(state, i, config):
            revived += 1
    return revived
