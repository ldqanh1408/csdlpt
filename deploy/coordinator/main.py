"""Coordinator service — §8.13 barrier with diagnosis + integrity check.

Endpoints
  POST /api/run_started   ingestor announces RUN_ID + scatter_total
  POST /api/heartbeat     node liveness (5s interval)
  POST /api/completed     node final EOS report (after flush)
  GET  /api/wait?timeout  block until ALL_DONE or TIMEOUT with diagnosis
  GET  /health            stats
  GET  /metrics           Prometheus text exposition
"""
from __future__ import annotations

import asyncio
import os
import time
import uuid
from typing import Any

from fastapi import FastAPI, Response
from pydantic import BaseModel

TOTAL_NODES = int(os.environ.get("TOTAL_NODES", 4))
ALLOWED_LATENESS_S = float(os.environ.get("ALLOWED_LATENESS_S", 2.0))

RUN_ID: str = ""  # set on first /api/run_started
completed: dict[int, dict] = {}
last_hb: dict[int, float] = {}
node_max_event_time: dict[int, float] = {}  # per-node max event-time → global watermark
scatter_total: int | None = None
done_event = asyncio.Event()
lock = asyncio.Lock()


def _compute_global_watermark() -> float:
    vals = [v for v in node_max_event_time.values() if v > float("-inf")]
    if len(vals) < TOTAL_NODES:
        return float("-inf")
    return min(vals) - ALLOWED_LATENESS_S


class Report(BaseModel):
    run_id: str
    node_id: int
    events_processed: int
    watermark_final: float
    completeness: float
    dropped_late: int = 0
    duplicates: int = 0
    backpressure_drops: int = 0
    flush_ts: float
    flush_duration_ms: float
    schema_version: int = 1


class Heartbeat(BaseModel):
    run_id: str | None = None
    node_id: int
    phase: str
    ts: float
    max_event_time: float = float("-inf")


class RunStarted(BaseModel):
    run_id: str
    scatter_total: int  # -1 if not yet known


app = FastAPI(title="csdlpt-coordinator")


@app.post("/api/run_started")
async def run_started(r: RunStarted) -> dict:
    global RUN_ID, scatter_total, completed, done_event
    if not RUN_ID:
        RUN_ID = r.run_id
    if r.run_id != RUN_ID:
        return {"ack": False, "reason": "stale_run_id", "expected": RUN_ID}
    if r.scatter_total >= 0:
        scatter_total = r.scatter_total
    return {"ack": True, "run_id": RUN_ID}


@app.post("/api/heartbeat")
async def heartbeat(hb: Heartbeat) -> dict:
    if hb.run_id and hb.run_id != RUN_ID:
        return {"ack": False, "reason": "stale_run_id"}
    last_hb[hb.node_id] = hb.ts
    if hb.max_event_time > float("-inf"):
        node_max_event_time[hb.node_id] = max(
            node_max_event_time.get(hb.node_id, float("-inf")),
            hb.max_event_time,
        )
    return {"ack": True, "global_watermark": _compute_global_watermark()}


@app.post("/api/completed")
async def node_completed(r: Report) -> dict:
    if r.run_id != RUN_ID:  # C3
        return {"ack": False, "reason": "stale_run_id", "expected": RUN_ID}
    async with lock:
        completed[r.node_id] = r.model_dump()  # C4 idempotent
        if len(completed) == TOTAL_NODES:
            done_event.set()
    return {"ack": True, "received": len(completed), "expected": TOTAL_NODES}


@app.get("/api/wait")
async def wait(timeout: int = 60) -> dict:
    try:
        await asyncio.wait_for(done_event.wait(), timeout)
    except asyncio.TimeoutError:
        now = time.time()
        missing = sorted(set(range(TOTAL_NODES)) - set(completed.keys()))
        diagnosis: dict[int, str] = {}
        for nid in missing:
            if nid not in last_hb:
                diagnosis[nid] = "NEVER_SEEN"
            else:
                gap = now - last_hb[nid]
                diagnosis[nid] = (
                    "DEAD" if gap > 15 else f"SLOW({gap:.1f}s_since_hb)"
                )
        return {
            "status": "TIMEOUT",
            "run_id": RUN_ID,
            "received": sorted(completed.keys()),
            "missing": missing,
            "diagnosis": diagnosis,
        }

    # All done — C11 integrity validation
    received_events = sum(r["events_processed"] for r in completed.values())
    integrity_ok = scatter_total is None or received_events == scatter_total
    avg_completeness = (
        sum(r["completeness"] for r in completed.values()) / TOTAL_NODES
        if TOTAL_NODES
        else 0.0
    )
    return {
        "status": "ALL_DONE" if integrity_ok else "DATA_LOSS",
        "run_id": RUN_ID,
        "events_received": received_events,
        "scatter_total": scatter_total,
        "missing_events": (scatter_total or 0) - received_events,
        "avg_completeness": avg_completeness,
        "results": completed,
    }


@app.get("/health")
def health() -> dict:
    return {
        "service": "coordinator",
        "run_id": RUN_ID,
        "received": len(completed),
        "expected": TOTAL_NODES,
        "heartbeats_seen": len(last_hb),
        "scatter_total": scatter_total,
        "global_watermark": _compute_global_watermark(),
        "node_max_event_times": {
            str(k): v for k, v in node_max_event_time.items()
        },
    }


@app.get("/metrics")
def metrics() -> Response:
    gw = _compute_global_watermark()
    body = (
        f"coord_received {len(completed)}\n"
        f"coord_expected {TOTAL_NODES}\n"
        f"coord_heartbeats_seen {len(last_hb)}\n"
        f"coord_scatter_total {scatter_total if scatter_total is not None else -1}\n"
        f'coord_done{{run_id="{RUN_ID}"}} {1 if done_event.is_set() else 0}\n'
        f'coord_global_watermark {gw if gw > float("-inf") else -1}\n'
    )
    return Response(content=body, media_type="text/plain")
