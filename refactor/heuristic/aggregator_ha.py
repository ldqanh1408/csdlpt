"""Aggregator HA — Active-Standby failover via file lock (Spec §9.4)."""

import fcntl
import json
import logging
import os
import threading
import time

from refactor.heuristic.aggregator import HeuristicAggregator

logger = logging.getLogger("aggregator_ha")


class FileLockLeader:
    """Leader election via fcntl exclusive lock on a shared file."""

    def __init__(self, lock_path: str = "/tmp/aggregator.lock"):
        self.lock_path = lock_path
        self._lock_file = None
        self._active = False

    def try_acquire(self) -> bool:
        try:
            os.makedirs(os.path.dirname(self.lock_path) or "/tmp", exist_ok=True)
            self._lock_file = open(self.lock_path, "w")
            fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._active = True
            self._lock_file.write(str(os.getpid()))
            self._lock_file.flush()
            return True
        except (IOError, OSError):
            if self._lock_file:
                self._lock_file.close()
                self._lock_file = None
            return False

    def release(self):
        if self._lock_file:
            try:
                fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)
                self._lock_file.close()
            except Exception:
                pass
            self._lock_file = None
        self._active = False

    @property
    def is_active(self) -> bool:
        return self._active


class AggregatorHA:
    """Wraps HeuristicAggregator with Active-Standby HA.

    Active holds file lock + writes heartbeat every 1s.
    Standby monitors heartbeat file, takes over if active stale > 3s.
    """

    def __init__(self, aggregator: HeuristicAggregator, lock_path: str = "/tmp/aggregator.lock",
                 heartbeat_path: str = "/tmp/aggregator-heartbeat"):
        self.aggregator = aggregator
        self._leader = FileLockLeader(lock_path)
        self.heartbeat_path = heartbeat_path
        self._stop = threading.Event()
        self._active = False
        self._failover_count = 0

    def start(self):
        if self._leader.try_acquire():
            self._active = True
            self.aggregator._is_active = True
            logger.info("Aggregator HA: ACTIVE (pid=%d)", os.getpid())
            self._start_heartbeat_writer()
        else:
            logger.info("Aggregator HA: STANDBY (pid=%d)", os.getpid())
            self._start_heartbeat_monitor()

    def _start_heartbeat_writer(self):
        def _write():
            while not self._stop.is_set() and self._active:
                try:
                    os.makedirs(os.path.dirname(self.heartbeat_path) or "/tmp", exist_ok=True)
                    with open(self.heartbeat_path, "w") as f:
                        json.dump({"pid": os.getpid(), "timestamp": time.time(),
                                    "W_global_h": self.aggregator.W_global_h}, f)
                except Exception:
                    pass
                time.sleep(1.0)
        threading.Thread(target=_write, daemon=True).start()

    def _start_heartbeat_monitor(self):
        def _monitor():
            while not self._stop.is_set():
                try:
                    if os.path.exists(self.heartbeat_path):
                        if time.time() - os.path.getmtime(self.heartbeat_path) > 3.0:
                            logger.warning("Aggregator HA: active stale, taking over")
                            self._attempt_takeover()
                except Exception:
                    pass
                time.sleep(1.0)
        threading.Thread(target=_monitor, daemon=True).start()

    def _attempt_takeover(self):
        if self._leader.try_acquire():
            self._active = True
            self.aggregator._is_active = True
            self._failover_count += 1
            self.aggregator.load_state()
            logger.warning("Aggregator HA: TAKEOVER (failover #%d)", self._failover_count)
            self._start_heartbeat_writer()

    def broadcast(self) -> dict:
        r = self.aggregator.broadcast()
        r["ha_active"] = self._active
        r["ha_failover_count"] = self._failover_count
        return r

    def shutdown(self):
        self._stop.set()
        self._leader.release()
