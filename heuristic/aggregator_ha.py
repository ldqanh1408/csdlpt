"""
Cơ chế High Availability cho Heuristic Aggregator bằng file lock.

Một process giữ vai trò primary, process còn lại standby; khi primary mất lock hoặc dừng, standby takeover để tiếp tục nhận watermark từ worker.
"""

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

from heuristic.aggregator import HeuristicAggregator

logger = logging.getLogger("aggregator_ha")


class FileLockLeader:
    """Lớp `FileLockLeader` gom dữ liệu và hành vi liên quan đến FileLockLeader.
    
    Ghi chú gốc:
    Leader election via exclusive lock on a shared file.
    
        Uses fcntl.flock on POSIX systems and msvcrt.locking on Windows.
        Falls back to a best-effort O_CREAT|O_EXCL approach if neither is available.
    """

    def __init__(self, lock_path: str = "/tmp/aggregator.lock"):
        """Khởi tạo đối tượng của `FileLockLeader` và thiết lập trạng thái ban đầu."""
        self.lock_path = lock_path
        self._lock_file = None
        self._active = False

    def try_acquire(self) -> bool:
        """Hàm `try_acquire` thực hiện phần xử lý liên quan đến try acquire của `FileLockLeader`."""
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
        """Hàm `release` thực hiện phần xử lý liên quan đến release của `FileLockLeader`."""
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
        """Kiểm tra điều kiện `is active` và trả về boolean."""
        return self._active


class AggregatorHA:
    """Lớp `AggregatorHA` gom dữ liệu và hành vi liên quan đến AggregatorHA.
    
    Ghi chú gốc:
    Wraps HeuristicAggregator with Active-Standby HA.
    
        Active holds file lock + writes heartbeat every 1s.
        Standby monitors heartbeat file, takes over if active stale > 3s.
    """

    def __init__(self, aggregator: HeuristicAggregator, lock_path: str = "/tmp/aggregator.lock",
                 heartbeat_path: str = "/tmp/aggregator-heartbeat", leader_lock=None):
        """Khởi tạo đối tượng của `AggregatorHA` và thiết lập trạng thái ban đầu."""
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
                from common.zk_lock import ZKLeaderElection
                zk_lock_path = os.environ.get("ZK_LOCK_PATH", "/csdlpt/aggregator-lock")
                logger.info("Aggregator HA: using ZK lock at hosts=%s, path=%s", zk_hosts, zk_lock_path)
                self._leader = ZKLeaderElection(zk_hosts=zk_hosts, lock_path=zk_lock_path)
                self._leader.start()
            else:
                self._leader = FileLockLeader(lock_path)

    def start(self):
        """Hàm `start` thực hiện phần xử lý liên quan đến start của `AggregatorHA`."""
        if self._leader.try_acquire():
            self._active = True
            self.aggregator._is_active = True
            logger.info("Aggregator HA: ACTIVE (pid=%d)", os.getpid())
            self._start_heartbeat_writer()
        else:
            # A revived old-active starts here: the current active still holds
            # the shared lock, so we come up as STANDBY. Explicitly clear the
            # active flag (its default is True) so state is consistent and we
            # never advertise ourselves as active until we win the lock.
            self._active = False
            self.aggregator._is_active = False
            logger.info("Aggregator HA: STANDBY (pid=%d)", os.getpid())
            self._start_heartbeat_monitor()

    def _start_heartbeat_writer(self):
        """Khởi động tiến trình, server hoặc vòng nền `start heartbeat writer`."""
        def _write():
            """Hàm `_write` thực hiện phần xử lý liên quan đến write của `AggregatorHA`."""
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
        """Khởi động tiến trình, server hoặc vòng nền `start heartbeat monitor`."""
        def _monitor():
            """Hàm `_monitor` thực hiện phần xử lý liên quan đến monitor của `AggregatorHA`."""
            while not self._stop.is_set():
                try:
                    # The standby must take over whenever it can no longer see a
                    # FRESH active heartbeat. That includes the heartbeat file
                    # being stale *and* the file being absent entirely (active
                    # crashed before writing, or the path is not shared). The
                    # shared leader lock (try_acquire) is the real arbiter, so
                    # attempting takeover on a missing heartbeat is safe and
                    # cannot cause split-brain.
                    fresh = (
                        os.path.exists(self.heartbeat_path)
                        and (time.time() - os.path.getmtime(self.heartbeat_path)) <= 1.5
                    )
                    if not fresh:
                        logger.warning(
                            "Aggregator HA: no fresh active heartbeat, attempting takeover")
                        self._attempt_takeover()
                        if self._active:
                            # Promoted; heartbeat writer is now running.
                            break
                except Exception:
                    pass
                time.sleep(1.0)
        threading.Thread(target=_monitor, daemon=True).start()

    def _attempt_takeover(self):
        """Hàm `_attempt_takeover` thực hiện phần xử lý liên quan đến attempt takeover của `AggregatorHA`."""
        if self._leader.try_acquire():
            self._active = True
            self.aggregator._is_active = True
            self._failover_count += 1
            self.aggregator.load_state()
            logger.warning("Aggregator HA: TAKEOVER (failover #%d)", self._failover_count)
            self._start_heartbeat_writer()

    def broadcast(self) -> dict:
        """Hàm `broadcast` thực hiện phần xử lý liên quan đến broadcast của `AggregatorHA`."""
        r = self.aggregator.broadcast()
        r["ha_active"] = self._active
        r["ha_failover_count"] = self._failover_count
        return r

    def shutdown(self):
        """Hàm `shutdown` thực hiện phần xử lý liên quan đến shutdown của `AggregatorHA`."""
        self._stop.set()
        self._leader.release()
