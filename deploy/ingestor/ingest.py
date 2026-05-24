"""Ingestor — §8.13 scatter with in-band RUN_ID EOS markers and integrity announce.

Usage
  python ingest.py [--csv PATH] [--n 10000] [--seed 42]
                   [--drop-fraction 0.0]    # for test_10_data_loss

Env
  NODE_HOSTS        comma-separated host:port list (e.g. node0:8000,node1:8000)
  COORDINATOR_URL   http://coordinator:8000
  RUN_ID            optional override; auto-generated if absent
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import os
import random
import sys
import time
import uuid

import httpx

NODE_HOSTS = os.environ.get(
    "NODE_HOSTS", "node0:8000,node1:8000,node2:8000,node3:8000"
).split(",")
COORDINATOR = os.environ.get("COORDINATOR_URL", "http://coordinator:8000")
N = len(NODE_HOSTS)
RUN_ID = os.environ.get("RUN_ID") or f"run-{int(time.time())}-{uuid.uuid4().hex[:6]}"


def route(key: str) -> str:
    h = int(hashlib.md5(key.encode()).hexdigest(), 16)
    return NODE_HOSTS[h % N]


def _gen_synthetic(n_events: int, seed: int = 42) -> list[dict]:
    """Stand-alone generator — no wm/ import needed inside container."""
    rng = random.Random(seed)
    base = float(int(time.time())) - 300.0
    events = []
    for i in range(n_events):
        event_time = base + rng.uniform(0, 300.0)
        events.append(
            {
                "event_id": f"evt-{i}",
                "event_time": event_time,
                "status": rng.choices([200, 404, 500], weights=[0.9, 0.07, 0.03])[0],
                "endpoint": rng.choice(["/", "/login", "/api", "/static", "/checkout"]),
            }
        )
    # 2% duplicates to verify C4 idempotency through engine
    for e in list(events):
        if rng.random() < 0.02:
            events.append(dict(e))
    rng.shuffle(events)
    return events


def _read_csv(path: str) -> list[dict]:
    out: list[dict] = []
    with open(path) as f:
        for row in csv.DictReader(f):
            out.append(
                {
                    "event_id": row.get("event_id") or row.get("id"),
                    "event_time": float(row["event_time"]),
                    "status": int(row.get("status", 200)),
                    "endpoint": row.get("endpoint", "/"),
                }
            )
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default=None)
    parser.add_argument("--n", type=int, default=10000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--drop-fraction",
        type=float,
        default=0.0,
        help="Drop fraction of events before scattering (for test_10_data_loss)",
    )
    args = parser.parse_args()

    events = _read_csv(args.csv) if args.csv else _gen_synthetic(args.n, args.seed)
    print(f"ingestor: {len(events)} events generated, RUN_ID={RUN_ID}, N={N}")

    if args.drop_fraction > 0:
        rng = random.Random(args.seed + 1)
        kept = [e for e in events if rng.random() > args.drop_fraction]
        print(f"  dropped {len(events) - len(kept)} events (drop_fraction={args.drop_fraction})")
        events = kept

    sent = 0
    with httpx.Client(timeout=3) as client:
        # Announce run start so coordinator can validate later (C11)
        try:
            client.post(
                f"{COORDINATOR}/api/run_started",
                json={"run_id": RUN_ID, "scatter_total": -1},
            )
        except Exception as e:
            print(f"  warn: /api/run_started initial call failed: {e}", file=sys.stderr)

        for ev in events:
            target = route(ev["event_id"])
            payload = {"type": "data", "run_id": RUN_ID, **ev}
            try:
                client.post(f"http://{target}/ingest", json=payload)
                sent += 1
            except Exception as e:
                print(f"  warn: drop event {ev['event_id']}: {e}", file=sys.stderr)
            if sent % 5000 == 0:
                print(f"  sent {sent}/{len(events)}")

        # In-band EOS marker with RUN_ID → fence to all nodes (C2)
        for host in NODE_HOSTS:
            try:
                client.post(
                    f"http://{host}/ingest",
                    json={"type": "EOS", "run_id": RUN_ID},
                )
            except Exception as e:
                print(f"  warn: EOS to {host} failed: {e}", file=sys.stderr)

        # Final scatter_total for C11 integrity check
        try:
            client.post(
                f"{COORDINATOR}/api/run_started",
                json={"run_id": RUN_ID, "scatter_total": sent},
            )
        except Exception as e:
            print(f"  warn: final scatter_total post failed: {e}", file=sys.stderr)

    print(f"ingestor done. RUN_ID={RUN_ID} sent={sent}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
