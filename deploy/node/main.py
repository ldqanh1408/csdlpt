"""Node service — wraps WatermarkEngine with §8.13 EOS protocol.

Endpoints
  POST /ingest      data event or EOS marker (in-band, with run_id)
  POST /heartbeat   internal (not used — node sends, doesn't receive)
  GET  /health      liveness
  GET  /metrics     Prometheus text exposition
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, Response
from pydantic import BaseModel

from wm.engine import WatermarkEngine

NODE_ID = int(os.environ.get("NODE_ID", -1))
COORDINATOR = os.environ.get("COORDINATOR_URL", "http://coordinator:8000")
WINDOW_SIZE_S = float(os.environ.get("WINDOW_SIZE_S", 10.0))
ALLOWED_LATENESS_S = float(os.environ.get("ALLOWED_LATENESS_S", 2.0))
DLQ_PATH = Path(os.environ.get("DLQ_PATH", f"/var/lib/csdlpt/dlq/node-{NODE_ID}.jsonl"))
CHECKPOINT_PATH = os.environ.get("CHECKPOINT_PATH", f"/var/lib/csdlpt/dlq/ckpt-{NODE_ID}.json")

engine = WatermarkEngine(
    window_size_s=WINDOW_SIZE_S,
    allowed_lateness_s=ALLOWED_LATENESS_S,
    checkpoint_path=CHECKPOINT_PATH,
)

state: dict[str, Any] = {
    "phase": "INIT",
    "run_id": None,
    "eos_acked": False,
    "queue_len": 0,
}


def log_phase(phase: str, **extra: Any) -> None:
    state["phase"] = phase
    print(
        json.dumps(
            {
                "ts": time.time(),
                "node_id": NODE_ID,
                "run_id": state["run_id"],
                "phase": phase,
                **extra,
            }
        ),
        flush=True,
    )


class Event(BaseModel):
    type: str  # "data" | "EOS"
    run_id: str | None = None
    event_id: str | None = None
    event_time: float | None = None
    status: int | None = None
    endpoint: str | None = None


async def send_with_ack(client: httpx.AsyncClient, report: dict) -> bool:
    """C5 — only return True on body.ack == true."""
    try:
        r = await client.post(f"{COORDINATOR}/api/completed", json=report)
        return r.status_code == 200 and r.json().get("ack") is True
    except (httpx.ConnectError, httpx.TimeoutException, httpx.HTTPError):
        return False


def persist_dlq(report: dict) -> None:
    """C7 — append-only DLQ, never crashes node."""
    DLQ_PATH.parent.mkdir(parents=True, exist_ok=True)
    with DLQ_PATH.open("a") as f:
        f.write(json.dumps({"queued_at": time.time(), **report}) + "\n")


async def replay_dlq(client: httpx.AsyncClient) -> None:
    if not DLQ_PATH.exists():
        return
    pending = [json.loads(line) for line in DLQ_PATH.read_text().splitlines() if line]
    kept: list[dict] = []
    for rpt in pending:
        if not await send_with_ack(client, rpt):
            kept.append(rpt)
    DLQ_PATH.write_text(
        "\n".join(json.dumps(r) for r in kept) + ("\n" if kept else "")
    )
    log_phase("DLQ_REPLAY", pending=len(pending), kept=len(kept))


async def heartbeat_loop() -> None:
    """C8 — separate channel, lifecycle independent of EOS.
    Also carries max_event_time → coordinator computes global watermark."""
    async with httpx.AsyncClient(timeout=2) as c:
        while not state["eos_acked"]:
            try:
                r = await c.post(
                    f"{COORDINATOR}/api/heartbeat",
                    json={
                        "run_id": state["run_id"],
                        "node_id": NODE_ID,
                        "phase": state["phase"],
                        "ts": time.time(),
                        "max_event_time": (
                            engine.max_event_time
                            if engine.max_event_time > float("-inf")
                            else float("-inf")
                        ),
                    },
                )
                data = r.json()
                gw = data.get("global_watermark")  # None when coordinator returns null
                if gw is not None and gw > float("-inf"):
                    engine.update_global_watermark(gw)
            except Exception:
                pass
            await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(_: FastAPI):
    log_phase("READY")
    hb_task = asyncio.create_task(heartbeat_loop())
    async with httpx.AsyncClient(timeout=5) as c:
        await replay_dlq(c)
    yield
    hb_task.cancel()


app = FastAPI(title="csdlpt-node", lifespan=lifespan)


async def _report_eos(report: dict) -> None:
    """Background task — report to coordinator with bounded retries (C6)."""
    async with httpx.AsyncClient(timeout=5) as client:
        for attempt in range(5):
            if await send_with_ack(client, report):
                state["eos_acked"] = True
                log_phase("ACKED")
                return
            await asyncio.sleep(min(2**attempt, 10))
    persist_dlq(report)
    log_phase("DEGRADED", reason="ack_timeout_after_5_retries")


@app.post("/ingest")
async def ingest(event: Event) -> dict:
    if event.type == "EOS":
        state["run_id"] = event.run_id
        log_phase("EOS_RECEIVED")

        # C1 — flush BLOCKING before any report
        log_phase("FLUSHING")
        t0 = time.time()
        engine.flush()
        flush_ms = (time.time() - t0) * 1000.0
        log_phase("FLUSHED", duration_ms=flush_ms, events=engine.metrics["total"])

        completeness_info = engine.summary()
        report = {
            "run_id": state["run_id"],
            "node_id": NODE_ID,
            "events_processed": engine.metrics["total"],
            "watermark_final": engine.watermark if engine.watermark != float("-inf") else 0.0,
            "completeness": completeness_info.get("data_completeness_pct", 0.0) / 100.0,
            "dropped_late": engine.metrics.get("late_dropped", 0),
            "duplicates": engine.metrics.get("duplicates", 0),
            "backpressure_drops": engine.metrics.get("backpressure_drops", 0),
            "flush_ts": time.time(),
            "flush_duration_ms": flush_ms,
            "schema_version": 1,
        }

        log_phase("REPORTING")
        asyncio.create_task(_report_eos(report))  # fire-and-forget background
        return {"status": "EOS_PROCESSING"}

    # Data event
    data = event.model_dump(exclude_none=True)
    data.pop("type", None)
    data.pop("run_id", None)
    state["queue_len"] += 1
    try:
        engine.process(data, queue_len=state["queue_len"])
    finally:
        state["queue_len"] = max(0, state["queue_len"] - 1)
    return {"ok": True}


@app.get("/health")
def health() -> dict:
    return {
        "service": "node",
        "node_id": NODE_ID,
        "phase": state["phase"],
        "run_id": state["run_id"],
        "watermark": engine.watermark if engine.watermark != float("-inf") else None,
        "global_watermark": engine._global_watermark if engine._global_watermark > float("-inf") else None,
        "processed": engine.metrics["total"],
    }


@app.get("/metrics")
def metrics() -> Response:
    wm_val = engine.watermark if engine.watermark != float("-inf") else 0.0
    gw_val = engine._global_watermark if engine._global_watermark > float("-inf") else 0.0
    s = engine.summary()
    completeness = s.get("data_completeness_pct", 0.0) / 100.0
    body = (
        f'wm_watermark{{node="{NODE_ID}"}} {wm_val}\n'
        f'wm_global_watermark{{node="{NODE_ID}"}} {gw_val}\n'
        f'wm_events_total{{node="{NODE_ID}"}} {engine.metrics["total"]}\n'
        f'wm_events_unique{{node="{NODE_ID}"}} {engine.metrics["unique"]}\n'
        f'wm_completeness{{node="{NODE_ID}"}} {completeness}\n'
        f'wm_dropped_late{{node="{NODE_ID}"}} {engine.metrics.get("late_dropped",0)}\n'
        f'wm_duplicates{{node="{NODE_ID}"}} {engine.metrics.get("duplicates",0)}\n'
        f'wm_backpressure{{node="{NODE_ID}"}} {engine.metrics.get("backpressure_drops",0)}\n'
    )
    return Response(content=body, media_type="text/plain")
