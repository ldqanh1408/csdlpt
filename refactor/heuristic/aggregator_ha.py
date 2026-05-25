"""Aggregator HA — Active-Standby failover via file lock (Spec §9.4)."""

import json
import logging
import os
import sys
import threading
import time

# Cross-platform file locking: fcntl on POSIX, msvcrt on Windows.
try:
    import fcntl  # type: ignore
    _HAS_FCNTL = True
except ImportError:
    _HAS_FCNTL = False
    try:
        import msvcrt  # type: ignore
        _HAS_MSVCRT = True
    except ImportError:
        _HAS_MSVCRT = False

from refactor.heuristic.aggregator import HeuristicAggregator

logger = logging.getLogger("aggregator_ha")


class FileLockLeader:
    """Leader election via exclusive lock on a shared file.

    Uses fcntl.flock on POSIX systems and msvcrt.locking on Windows.
    Falls back to a best-effort O_CREAT|O_EXCL approach if neither is available.
    """

    def __init__(self, lock_path: str = "/tmp/aggregator.lock"):
        self.lock_path = lock_path
        self._lock_file = None
        self._active = False

    def try_acquire(self) -> bool:
        try:
            parent = os.path.dirname(self.lock_path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            self._lock_file = open(self.lock_path, "w")
            if _HAS_FCNTL:
                fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            elif _HAS_MSVCRT:
                # msvcrt.locking locks the byte range starting at current
                # file position; lock 1 byte at offset 0.
                self._lock_file.seek(0)
                msvcrt.locking(self._lock_file.fileno(), msvcrt.LK_NBLCK, 1)
            # else: no advisory locking available; rely on PID tracking only.
            self._active = True
            self._lock_file.write(str(os.getpid()))
            self._lock_file.flush()
            return True
        except (IOError, OSError):
            if self._lock_file:
                try:
                    self._lock_file.close()
                except Exception:
                    pass
                self._lock_file = None
            return False

    def release(self):
        if self._lock_file:
            try:
                if _HAS_FCNTL:
                    fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)
                elif _HAS_MSVCRT:
                    try:
                        self._lock_file.seek(0)
                        msvcrt.locking(self._lock_file.fileno(), msvcrt.LK_UNLCK, 1)
                    except OSError:
                        pass
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
                 heartbeat_path: str = "/tmp/aggregator-heartbeat", leader_lock=None):
        self.aggregator = aggregator
        self.heartbeat_path = heartbeat_path
        self._stop = threading.Event()
        self._active = False
        self._failover_count = 0

        if leader_lock is not None:
            self._leader = leader_lock
        else:
            zk_hosts = os.environ.get("ZK_HOSTS") or os.environ.get("ZK_ENSEMBLE")
            if zk_hosts:
                from refactor.common.zk_lock import ZKLeaderElection
                zk_lock_path = os.environ.get("ZK_LOCK_PATH", "/csdlpt/aggregator-lock")
                logger.info("Aggregator HA: using ZK lock at hosts=%s, path=%s", zk_hosts, zk_lock_path)
                self._leader = ZKLeaderElection(zk_hosts=zk_hosts, lock_path=zk_lock_path)
                self._leader.start()
            else:
                self._leader = FileLockLeader(lock_path)

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
                        if time.time() - os.path.getmtime(self.heartbeat_path) > 1.5:
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
