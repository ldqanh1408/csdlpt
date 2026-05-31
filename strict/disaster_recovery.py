"""Disaster Recovery — periodic active window backup to MinIO.

Spec §6.6: Backup active windows every 5 min. RTO <= 30min, RPO <= 5min.
"""

import io
import json
import logging
import threading
import time
from dataclasses import dataclass, field

from common.tiered_storage import TieredStorageManager

logger = logging.getLogger("disaster_recovery")

BACKUP_PREFIX = "strict-watermark/active_state_backup"


@dataclass
class DisasterRecovery:
    """Periodically backs up engine state to MinIO and restores on cold start."""

    storage: TieredStorageManager
    backup_interval_s: float = 300.0
    _last_backup: float = 0.0
    _stop: threading.Event = field(default_factory=threading.Event)

    def start_background_backup(self, engines: dict, get_state_callback=None):
        def _loop():
            while not self._stop.is_set():
                time.sleep(self.backup_interval_s)
                try:
                    self.backup(engines, get_state_callback)
                except Exception as e:
                    logger.error("DR backup failed: %s", e)
        threading.Thread(target=_loop, daemon=True).start()

    def backup(self, engines: dict, get_state_callback=None) -> str:
        if self.storage is None or self.storage.client is None:
            return ""
        unix_ts = int(time.time())
        self._last_backup = time.time()

        for pid, eng in engines.items():
            state = {
                "partition_id": pid,
                "local_watermark": eng.local_watermark,
                "max_event_time": eng.max_event_time,
                "watermark": eng.watermark,
                "last_T_commit": eng.last_T_commit,
                "open_windows": {
                    str(k): {"count": v.count, "status_500": v.status_500}
                    for k, v in eng.open_windows.items()
                },
                "seen_ids": list(eng._seen_ids_ttl.keys()),
            }
            key = f"{BACKUP_PREFIX}/partition_{pid}/{unix_ts}_active.json"
            data = json.dumps(state).encode()
            try:
                self.storage.client.put_object(
                    self.storage.bucket, key, data=io.BytesIO(data), length=len(data))
            except Exception as e:
                logger.error("DR backup partition %d failed: %s", pid, e)
                return ""

        if get_state_callback:
            key = f"{BACKUP_PREFIX}/coordinator/{unix_ts}_active.json"
            data = json.dumps(get_state_callback()).encode()
            try:
                self.storage.client.put_object(
                    self.storage.bucket, key, data=io.BytesIO(data), length=len(data))
            except Exception as e:
                logger.error("DR backup coordinator failed: %s", e)

        logger.info("DR backup: %d (partitions=%d)", unix_ts, len(engines))
        self._prune_old_backups(list(engines.keys()), max_keep=24)
        return str(unix_ts)

    def _prune_old_backups(self, partition_ids: list, max_keep: int = 24) -> None:
        """Keep only the max_keep most recent backups per partition (spec §6.6)."""
        if self.storage is None or self.storage.client is None:
            return
        for pid in partition_ids:
            timestamps = self.list_backups(partition_id=pid)
            for old_ts in timestamps[max_keep:]:
                key = f"{BACKUP_PREFIX}/partition_{pid}/{old_ts}_active.json"
                try:
                    self.storage.client.remove_object(self.storage.bucket, key)
                except Exception:
                    pass

    def list_backups(self, partition_id: int = None) -> list[str]:
        """List backup timestamps for a partition (or all partitions if None)."""
        if self.storage is None or self.storage.client is None:
            return []
        try:
            if partition_id is not None:
                prefix = f"{BACKUP_PREFIX}/partition_{partition_id}/"
            else:
                prefix = f"{BACKUP_PREFIX}/"
            objects = self.storage.client.list_objects(self.storage.bucket, prefix=prefix)
            timestamps = set()
            for obj in objects:
                parts = obj.object_name.split("/")
                # key: strict-watermark/active_state_backup/partition_{pid}/{ts}_active.json
                if len(parts) >= 4:
                    fname = parts[-1]
                    if fname.endswith("_active.json"):
                        ts = fname.replace("_active.json", "")
                        timestamps.add(ts)
            return sorted(timestamps, reverse=True)
        except Exception as e:
            logger.warning("DR list_backups failed: %s", e)
            return []

    def restore_latest(self, engines: dict) -> bool:
        restored = 0
        for pid, eng in engines.items():
            timestamps = self.list_backups(partition_id=pid)
            if not timestamps:
                continue
            if self._restore_partition(pid, eng, timestamps[0]):
                restored += 1
        return restored > 0

    def restore(self, engines: dict, timestamp: str) -> bool:
        if self.storage is None or self.storage.client is None:
            return False
        restored = 0
        for pid, eng in engines.items():
            if self._restore_partition(pid, eng, timestamp):
                restored += 1
        logger.info("DR restore: %d/%d partitions from %s", restored, len(engines), timestamp)
        return restored > 0

    def _restore_partition(self, pid: int, eng, timestamp: str) -> bool:
        key = f"{BACKUP_PREFIX}/partition_{pid}/{timestamp}_active.json"
        try:
            response = self.storage.client.get_object(self.storage.bucket, key)
            data = json.loads(response.read())
            response.close()
            response.release_conn()
            eng.local_watermark = data.get("local_watermark", eng.local_watermark)
            eng.max_event_time = data.get("max_event_time", eng.max_event_time)
            eng.watermark = data.get("watermark", eng.watermark)
            eng.last_T_commit = data.get("last_T_commit", eng.last_T_commit)
            from strict.engine import WindowState
            for k, v in data.get("open_windows", {}).items():
                eng.open_windows[float(k)] = WindowState(count=v["count"], status_500=v["status_500"])
            for eid in data.get("seen_ids", []):
                eng._seen_ids_ttl[eid] = time.time()
            return True
        except Exception as e:
            logger.warning("DR restore partition %d failed: %s", pid, e)
            return False

    def stop(self):
        self._stop.set()
