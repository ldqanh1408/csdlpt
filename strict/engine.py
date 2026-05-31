"""Strict Watermark Engine - 0% data loss via punctuation-based watermarks."""

import dataclasses
import logging
import os
import json
import tempfile
import threading
import time
import pickle
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

from common.types import (
    LogEvent, PunctuationToken, WindowResult, WindowStatus, EvictionState,
)
from common.window import TumblingWindow
from common.metrics import HighResTimer, SystemMetrics
from common.rocks_store import RocksStore


# RocksDB key prefixes
_PFX_OPEN = "ow:"
_PFX_CLOSED = "cw:"
_PFX_SEEN = "si:"
_PFX_META = "meta:"


@dataclass
class WindowState:
    count: int = 0
    status_500: int = 0
    eviction: EvictionState = EvictionState.CLOSED


class StrictWatermarkEngine:

    def __init__(
        self,
        window_size_s: float = 5.0,
        delta_base_s: float = 10.0,
        max_queue: int = 500,
        checkpoint_dir: str = "/tmp/strict-checkpoint",
        tiered_storage=None,
        db_path: Optional[str] = None,
        output_manager=None,
        diff_eviction=None,
        store: Optional[RocksStore] = None,
    ):
        self.tumbling = TumblingWindow(window_size_s)
        self.delta_base = delta_base_s
        self.max_queue = max_queue
        self.checkpoint_dir = checkpoint_dir
        self.tiered_storage = tiered_storage
        self.output_manager = output_manager
        self.diff_eviction = diff_eviction

        # Partition identity (default 0, no Raft term tracking)
        self.partition_id: int = 0
        self.raft_term: int = 0

        self.last_T_commit: float = float("-inf")
        self.local_watermark: float = float("-inf")
        self.max_event_time: float = float("-inf")
        self.watermark: float = float("-inf")

        # Kafka offset tracking per partition (spec §6.3)
        self.kafka_committed_offset: int = 0
        self.kafka_current_offset: int = 0
        self.punctuation_total: int = 0

        self.open_windows: dict[float, WindowState] = defaultdict(WindowState)
        self.closed_windows: dict[float, WindowResult] = {}

        # Dedup with TTL (60s window per spec §6.5)
        self._seen_ids_ttl: dict[str, float] = {}
        self._dedup_ttl_s: float = 60.0

        # Time-based checkpoint (spec §6.3: every 10s)
        self._last_checkpoint_time: float = time.time()
        self._checkpoint_interval_s: float = 10.0
        self._checkpoint_lock = threading.RLock()

        self.metrics = SystemMetrics()
        self.proc_latencies_ns: list[float] = []

        # RocksDB persistent storage (None = in-memory-only, backward compatible).
        # §6.4 feature flag: ENABLE_TWO_PHASE_EVICTION gates the on-restart
        # eviction-state recovery sweep (CLOSED/UPLOADING/UPLOADED check). When
        # disabled, restart only restores in-memory state; closed windows
        # remain in whatever state they were persisted in.
        self._store: Optional[RocksStore] = store
        if self._store is None and db_path is not None:
            self._store = RocksStore(db_path)
        if self._store is not None:
            has_metadata = False
            try:
                has_metadata = self._store.get(f"{_PFX_META}checkpoint") is not None
            except Exception:
                pass

            if has_metadata:
                self._restore_from_store()
            else:
                # No RocksDB metadata (e.g. fresh database directory because of takeover/rebalance).
                # Load fallback from checkpoint.json.
                path = os.path.join(checkpoint_dir, "checkpoint.json")
                if os.path.exists(path):
                    logger.info("StrictWatermarkEngine: No RocksDB metadata found. Falling back to JSON checkpoint at %s", path)
                    try:
                        with open(path) as f:
                            snap = json.load(f)
                        self.last_T_commit = snap.get("last_T_commit", float("-inf"))
                        self.local_watermark = snap.get("local_watermark", float("-inf"))
                        self.max_event_time = snap.get("max_event_time", float("-inf"))
                        self.watermark = snap.get("watermark", float("-inf"))
                        self.kafka_committed_offset = snap.get("kafka_committed_offset", 0)
                        self.kafka_current_offset = self.kafka_committed_offset
                        self.partition_id = snap.get("partition_id", 0)
                        
                        # Open windows
                        open_wins = snap.get("open_windows", {})
                        for k, v in open_wins.items():
                            ws = float(k)
                            ws_state = WindowState(
                                count=v.get("count", 0),
                                status_500=v.get("status_500", 0)
                            )
                            self.open_windows[ws] = ws_state
                            self._persist_open_window(ws)
                        
                        # Seen IDs
                        seen_ids = snap.get("seen_ids", [])
                        ckpt_time = snap.get("last_checkpoint_time", time.time())
                        for eid in seen_ids:
                            self._seen_ids_ttl[eid] = ckpt_time
                            self._persist_seen_id(eid)
                            
                        self._last_checkpoint_time = ckpt_time
                        
                        # Save metadata to store
                        meta = {
                            "last_T_commit": self.last_T_commit,
                            "local_watermark": self.local_watermark,
                            "max_event_time": self.max_event_time,
                            "watermark": self.watermark,
                            "last_checkpoint_time": self._last_checkpoint_time,
                            "rocksdb_checkpoint_path": None,
                            "partition_id": self.partition_id,
                            "active_windows": [str(w) for w in sorted(self.open_windows.keys())],
                            "sst_files_manifest": [],
                            "term_at_checkpoint": self.raft_term,
                        }
                        self._store.put(f"{_PFX_META}checkpoint", meta)
                        self._store.flush()
                    except Exception as e:
                        logger.error("StrictWatermarkEngine: Failed to restore state from JSON checkpoint: %s", e)

            if os.environ.get("ENABLE_TWO_PHASE_EVICTION", "true").lower() in ("1", "true", "yes", "on"):
                self._recover_eviction_states()

        try:
            os.makedirs(checkpoint_dir, exist_ok=True)
        except PermissionError:
            checkpoint_dir = os.path.join(tempfile.gettempdir(), "csdlpt-checkpoint")
            self.checkpoint_dir = checkpoint_dir
            os.makedirs(checkpoint_dir, exist_ok=True)

        # Restore emitted set for duplicate prevention after restart
        if self.output_manager is not None:
            emitted_path = os.path.join(self.checkpoint_dir, "emitted.json")
            self.output_manager.load_emitted(emitted_path)

    # ------------------------------------------------------------------
    # RocksDB persistence helpers
    # ------------------------------------------------------------------

    def _persist_open_window(self, ws: float) -> None:
        if self._store is None:
            return
        st = self.open_windows.get(ws)
        if st is not None:
            self._store.put(f"{_PFX_OPEN}{ws}", st)

    def _delete_open_window(self, ws: float) -> None:
        if self._store is None:
            return
        self._store.delete(f"{_PFX_OPEN}{ws}")

    def _persist_closed_window(self, ws: float, result: WindowResult,
                                eviction: EvictionState = EvictionState.UPLOADING) -> None:
        if self._store is None:
            return
        self._store.put(f"{_PFX_CLOSED}{ws}", {
            "result": result,
            "eviction": eviction.value,
        })

    def _purge_window_from_store(self, ws: float) -> None:
        """Delete a closed window entry from RocksDB after successful upload and emit.

        Called once the window has been uploaded to tiered storage (MinIO) and
        emitted downstream. After purging, only the tier-3 copy remains.
        """
        if self._store is None:
            return
        self._store.delete(f"{_PFX_CLOSED}{ws}")

    def _persist_seen_id(self, event_id: str) -> None:
        if self._store is None:
            return
        ts = self._seen_ids_ttl.get(event_id)
        if ts is not None:
            self._store.put(f"{_PFX_SEEN}{event_id}", ts)

    def _delete_seen_id(self, event_id: str) -> None:
        if self._store is None:
            return
        self._store.delete(f"{_PFX_SEEN}{event_id}")

    def _restore_from_store(self) -> None:
        """Populate in-memory state from RocksDB on startup."""
        if self._store is None:
            return

        for key, val in self._store.items(prefix=_PFX_OPEN):
            ws = float(key[len(_PFX_OPEN):])
            if isinstance(val, dict):
                self.open_windows[ws] = WindowState(
                    count=val.get("count", 0),
                    status_500=val.get("status_500", 0),
                )
            else:
                self.open_windows[ws] = val

        # Closed windows: new format is {"result": ..., "eviction": "uploading"|...}
        # Backward-compat: old format is raw WindowResult object
        for key, val in self._store.items(prefix=_PFX_CLOSED):
            ws = float(key[len(_PFX_CLOSED):])
            if isinstance(val, dict) and "result" in val:
                self.closed_windows[ws] = val["result"]
            else:
                self.closed_windows[ws] = val

        for key, val in self._store.items(prefix=_PFX_SEEN):
            eid = key[len(_PFX_SEEN):]
            self._seen_ids_ttl[eid] = float(val) if not isinstance(val, float) else val

        # Restore metadata
        meta = self._store.get(f"{_PFX_META}checkpoint")
        if meta is not None:
            self.last_T_commit = meta.get("last_T_commit", float("-inf"))
            self.local_watermark = meta.get("local_watermark", float("-inf"))
            self.max_event_time = meta.get("max_event_time", float("-inf"))
            self.watermark = meta.get("watermark", float("-inf"))
            self._last_checkpoint_time = meta.get("last_checkpoint_time", time.time())

    def _recover_eviction_states(self) -> None:
        """Recover eviction state for closed windows after a crash.

        For each closed window in RocksDB:
          - If UPLOADING: check MinIO for the window file
            - If file exists: advance to UPLOADED (upload completed before crash)
            - If file missing: re-upload
          - If UPLOADED: verify file exists, re-upload if missing
          - If UPLOADED and no tiered_storage: no-op (already durable)

        Backward-compat: old-format closed windows (raw WindowResult without
        eviction field) are treated as UPLOADING and recovered.
        """
        if self._store is None:
            return

        recovered_count = 0
        reupload_count = 0

        for key, val in self._store.items(prefix=_PFX_CLOSED):
            ws = float(key[len(_PFX_CLOSED):])
            result = self.closed_windows.get(ws)
            if result is None:
                continue

            # Extract eviction state (backward-compat: old format = no eviction field)
            if isinstance(val, dict) and "eviction" in val:
                eviction_str = val["eviction"]
            else:
                eviction_str = EvictionState.UPLOADING.value

            window_id = result.window_id
            partition_id = result.partition_id

            if self.tiered_storage is None:
                # No external storage — mark as UPLOADED
                if eviction_str != EvictionState.UPLOADED.value:
                    self._persist_closed_window(ws, result, EvictionState.UPLOADED)
                    recovered_count += 1
                continue

            # Check if window file exists in MinIO
            file_exists = self._check_tiered_window_exists(window_id, partition_id)

            if eviction_str in (EvictionState.UPLOADING.value,):
                if file_exists:
                    # Upload succeeded before crash
                    self._persist_closed_window(ws, result, EvictionState.UPLOADED)
                    recovered_count += 1
                    logger.info("RECOVER eviction: %s already uploaded, advanced to UPLOADED", window_id)
                else:
                    # Upload was in progress, re-upload
                    self._upload_to_tiered_storage(ws, result)
                    self._persist_closed_window(ws, result, EvictionState.UPLOADING)
                    reupload_count += 1
                    logger.info("RECOVER eviction: %s re-uploading", window_id)
                # Re-emit if upload completed but emission was lost before crash
                if self.output_manager is not None and not self.output_manager.is_emitted(window_id):
                    self.output_manager.emit(result)
                    logger.info("RECOVER eviction: %s re-emitted after crash", window_id)

            elif eviction_str == EvictionState.UPLOADED.value:
                if not file_exists:
                    # File lost, re-upload
                    self._upload_to_tiered_storage(ws, result)
                    self._persist_closed_window(ws, result, EvictionState.UPLOADING)
                    reupload_count += 1
                    logger.warning("RECOVER eviction: %s marked UPLOADED but file missing, re-uploading", window_id)
                # Re-emit if output was persisted but not emitted before crash
                if self.output_manager is not None and not self.output_manager.is_emitted(window_id):
                    self.output_manager.emit(result)
                    logger.info("RECOVER eviction: %s re-emitted after crash", window_id)

        if recovered_count or reupload_count:
            logger.info("RECOVER eviction complete: %d advanced, %d re-uploaded",
                        recovered_count, reupload_count)

    def _check_tiered_window_exists(self, window_id: str, partition_id: int) -> bool:
        """Check if a window file exists in tiered storage (MinIO)."""
        if self.tiered_storage is None:
            return False
        try:
            # download_window returns None if file not found
            return self.tiered_storage.download_window(window_id, partition_id) is not None
        except Exception:
            return False

    def on_punctuation(self, token: PunctuationToken) -> None:
        self.punctuation_total += 1
        if token.T_commit > self.last_T_commit:
            self.last_T_commit = token.T_commit
            self.local_watermark = token.T_commit - self.delta_base
            self._advance_watermark()
        else:
            self.metrics.non_monotonic_punctuation += 1

    def _advance_watermark(self) -> None:
        self.watermark = self.local_watermark
        to_close = [
            w for w in list(self.open_windows)
            if w + self.tumbling.size <= self.watermark
        ]
        for w in sorted(to_close):
            st = self.open_windows.pop(w)
            self._delete_open_window(w)
            st.eviction = EvictionState.UPLOADING
            result = WindowResult(
                window_id=self.tumbling.window_id(self.partition_id, w),
                partition_id=self.partition_id,
                window_start=w,
                window_end=w + self.tumbling.size,
                count=st.count,
                status_500=st.status_500,
                is_speculative=False,
                version=1,
            )
            self.closed_windows[w] = result
            self._persist_closed_window(w, result, EvictionState.UPLOADING)
            self._upload_to_tiered_storage(w, result, sync=True)
            # Emit BEFORE persisting UPLOADED so crash between emission and
            # persist is safe: _recover_eviction_states sees UPLOADING and
            # re-emits, the idempotent sink deduplicates by window_id.
            if self.output_manager is not None:
                self.output_manager.emit(result)
            st.eviction = EvictionState.UPLOADED
            self._persist_closed_window(w, result, EvictionState.UPLOADED)
            # Transition to PURGED only after upload is confirmed complete
            self._purge_window_from_store(w)
            st.eviction = EvictionState.PURGED

    def _purge_seen_ids(self) -> None:
        """Remove dedup entries older than TTL (60s spec §6.5).

        Uses watermark-based cutoff (W_global - dedupe_window) per the spec,
        NOT wall clock. Stored values are T_event (the log's event_time), so
        entries are purged when T_event < W_global - 60s — i.e. only after the
        global watermark has safely passed the event's time + the dedup window.

        In data-driven mode with -inf watermark during ingestion, falls back to
        max_event_time as the reference point for TTL calculation, preventing
        unbounded memory growth from the seen-IDs cache.

        RocksDB bulk cleanup uses efficient clear_prefix every 5 minutes
        (replacing the per-key-delete sweep for better compaction performance).
        The in-memory dict is the authoritative source; RocksDB is for crash
        recovery only, so bulk clearing is safe.
        """
        if self.watermark == float("-inf"):
            # Data-driven mode during ingestion: watermark not yet advanced.
            # Use max_event_time as reference — events older than
            # max_et - 60s are safe to purge from the dedup cache.
            if self.max_event_time == float("-inf"):
                return  # No events processed yet
            cutoff = self.max_event_time - self._dedup_ttl_s
        else:
            cutoff = self.watermark - self._dedup_ttl_s

        # Fast in-memory sweep — compare stored T_event against watermark-based cutoff
        expired = [eid for eid, ts in list(self._seen_ids_ttl.items()) if ts < cutoff]
        for eid in expired:
            del self._seen_ids_ttl[eid]
            self._delete_seen_id(eid)

        # Periodic bulk cleanup: clear all seen-id entries from RocksDB every 5 min.
        # The in-memory set is authoritative; on restart, recently-seen IDs that
        # were cleared from RocksDB will get a fresh TTL window — an acceptable
        # trade-off for avoiding per-key iteration on every purge cycle.
        if self._store is None:
            return
        now = time.time()
        last_bulk = getattr(self, "_last_dedup_bulk_cleanup", 0.0)
        if now - last_bulk < 300.0:  # 5 minutes
            return
        self._last_dedup_bulk_cleanup = now

        # After clearing, repopulate RocksDB with current in-memory state
        # so crash recovery still has the active set.
        self._store.clear_prefix(_PFX_SEEN)
        for eid, ts in list(self._seen_ids_ttl.items()):
            self._store.put(f"{_PFX_SEEN}{eid}", ts)

    def process(self, event: LogEvent, queue_len: int = 0) -> Optional[float]:
        t0 = HighResTimer.now_ns()
        self.metrics.total_received += 1

        # Track highest committed offset for checkpoint snapshots (§6.3)
        if event.offset > self.kafka_committed_offset:
            self.kafka_committed_offset = event.offset

        # T_network_ingest: end-to-end latency (event_time → engine arrival).
        # In production with real Kafka, this should measure poll()+deserialize()
        # duration. When poll_received_at is available, split into:
        #   - T_network_ingest: network latency (event_time → HTTP/Kafka receive)
        #   - T_poll_decode: poll+decode time (HTTP/Kafka receive → engine arrival)
        if event.arrival_time > 0:
            if event.poll_received_at > 0:
                self.metrics.T_network_ingest_ns.append(
                    (event.poll_received_at - event.event_time) * 1_000_000_000
                )
                self.metrics.T_poll_decode_ns.append(
                    (event.arrival_time - event.poll_received_at) * 1_000_000_000
                )
            else:
                self.metrics.T_network_ingest_ns.append(
                    (event.arrival_time - event.event_time) * 1_000_000_000
                )

        if queue_len > self.max_queue:
            self.metrics.backpressure_drops += 1
            return None

        et = event.event_time
        ws = self.tumbling.window_start(et)
        window_end = ws + self.tumbling.size

        # §8.4 + §6.5 — outer Watermark Filter runs BEFORE the inner Hash
        # Filter. The TTL guarantee on the seen-IDs set (60s window) holds
        # only because anything past `self.watermark` is dropped here first,
        # so the hash table never needs to remember events older than
        # δ_base + replay_safety_margin.
        t_dedup = HighResTimer.now_ns()
        if window_end <= self.watermark or ws in self.closed_windows:
            self.metrics.late_dropped += 1
            self.metrics.T_deduplication_ns.append(HighResTimer.now_ns() - t_dedup)
            return None

        # Inner Hash Filter — drop duplicate event_id within the TTL window.
        if event.event_id in self._seen_ids_ttl:
            self.metrics.duplicates += 1
            self.metrics.T_deduplication_ns.append(HighResTimer.now_ns() - t_dedup)
            return None
        self._seen_ids_ttl[event.event_id] = event.event_time
        self._persist_seen_id(event.event_id)
        self.metrics.T_deduplication_ns.append(HighResTimer.now_ns() - t_dedup)

        self.max_event_time = max(self.max_event_time, et)

        t_state = HighResTimer.now_ns()
        st = self.open_windows[ws]
        st.count += 1
        if event.status == 500:
            st.status_500 += 1
        self._persist_open_window(ws)
        self.metrics.on_time += 1
        self.metrics.T_state_write_ns.append(HighResTimer.now_ns() - t_state)

        self._advance_watermark()

        # Time-based checkpoint (spec §6.3: every 10s)
        if time.time() - self._last_checkpoint_time >= self._checkpoint_interval_s:
            self.checkpoint()
            self._last_checkpoint_time = time.time()

        lat_ns = HighResTimer.now_ns() - t0
        self.proc_latencies_ns.append(lat_ns)
        if len(self.proc_latencies_ns) > 10000:
            self.proc_latencies_ns.pop(0)
        return lat_ns

    def _list_sst_files(self, ckpt_dir: str) -> list[str]:
        """List SST files in a RocksDB checkpoint directory."""
        import glob
        sst_files = []
        for root, _dirs, files in os.walk(ckpt_dir):
            for f in files:
                if f.endswith(".sst"):
                    sst_files.append(os.path.relpath(os.path.join(root, f), ckpt_dir))
        return sorted(sst_files)

    def checkpoint(self) -> None:
        with self._checkpoint_lock:
            return self._checkpoint_impl()

    def _checkpoint_impl(self) -> None:
        # Purge stale dedup entries before checkpoint (spec §6.3 + §6.5)
        self._purge_seen_ids()

        # Create RocksDB checkpoint first so we can list actual SST files
        rocksdb_ckpt_path = self._rocksdb_checkpoint()
        sst_manifest = self._list_sst_files(rocksdb_ckpt_path) if rocksdb_ckpt_path else []

        # JSON checkpoint (always created as fallback)
        path = os.path.join(self.checkpoint_dir, "checkpoint.json")
        snap = {
            "last_T_commit": self.last_T_commit,
            "local_watermark": self.local_watermark,
            "max_event_time": self.max_event_time,
            "watermark": self.watermark,
            "kafka_committed_offset": self.kafka_committed_offset,
            "open_windows": {
                str(k): {"count": v.count, "status_500": v.status_500}
                for k, v in list(self.open_windows.items())
            },
            "closed_count": len(self.closed_windows),
            "seen_ids": sorted(self._seen_ids_ttl.keys()),
            "last_checkpoint_time": self._last_checkpoint_time,
            "partition_id": self.partition_id,
            "active_windows": [str(w) for w in sorted(self.open_windows.keys())],
            "sst_files_manifest": sst_manifest,
            "term_at_checkpoint": self.raft_term,
        }
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        tmp = None
        for attempt in range(5):
            try:
                with tempfile.NamedTemporaryFile(
                    "w",
                    dir=self.checkpoint_dir,
                    prefix="checkpoint.",
                    suffix=".tmp",
                    delete=False,
                ) as f:
                    tmp = f.name
                    json.dump(snap, f)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp, path)
                tmp = None
                break
            except FileNotFoundError:
                if attempt == 4:
                    raise
                os.makedirs(self.checkpoint_dir, exist_ok=True)
                time.sleep(0.05)
            finally:
                if tmp and os.path.exists(tmp):
                    try:
                        os.unlink(tmp)
                    except OSError:
                        pass


        # RocksDB checkpoint metadata (durable alongside JSON)
        if self._store is not None:
            meta = {
                "last_T_commit": self.last_T_commit,
                "local_watermark": self.local_watermark,
                "max_event_time": self.max_event_time,
                "watermark": self.watermark,
                "last_checkpoint_time": self._last_checkpoint_time,
                "rocksdb_checkpoint_path": rocksdb_ckpt_path,
                "partition_id": self.partition_id,
                "active_windows": [str(w) for w in sorted(self.open_windows.keys())],
                "sst_files_manifest": sst_manifest,
                "term_at_checkpoint": self.raft_term,
            }
            self._store.put(f"{_PFX_META}checkpoint", meta)
            self._store.flush()

        # Persist OutputManager emitted set for crash recovery
        if self.output_manager is not None:
            emitted_path = os.path.join(self.checkpoint_dir, "emitted.json")
            self.output_manager.save_emitted(emitted_path)

    def _rocksdb_checkpoint(self) -> Optional[str]:
        """Create an incremental RocksDB SST checkpoint via rocksdict.Checkpoint.

        Returns the checkpoint directory path on success, or None on failure.
        JSON checkpoint is always kept as a fallback for backward compatibility.
        """
        if self._store is None or not self._store.is_open:
            return None
        try:
            from rocksdict import Checkpoint

            ckpt_dir = os.path.join(self.checkpoint_dir, "rocksdb_checkpoints")
            os.makedirs(ckpt_dir, exist_ok=True)
            ckpt_path = os.path.join(ckpt_dir, f"ckpt-{int(time.time() * 1000)}")

            ckpt = Checkpoint(self._store._db)
            ckpt.create_checkpoint(ckpt_path)

            # Clean up old checkpoints (keep last 3)
            existing = sorted(
                [d for d in os.listdir(ckpt_dir) if d.startswith("ckpt-")],
                reverse=True,
            )
            for old in existing[3:]:
                old_path = os.path.join(ckpt_dir, old)
                try:
                    import shutil
                    shutil.rmtree(old_path)
                except Exception:
                    logger.debug("Failed to clean old RocksDB checkpoint: %s", old_path)

            logger.info("RocksDB checkpoint created: %s", ckpt_path)
            return ckpt_path
        except ImportError:
            logger.debug("rocksdict.Checkpoint not available, skipping SST checkpoint")
            return None
        except Exception as e:
            logger.warning("RocksDB checkpoint failed: %s", e)
            return None

    @classmethod
    def restore(cls, checkpoint_dir: str, db_path: Optional[str] = None, **kwargs) -> "StrictWatermarkEngine":
        eng = cls(checkpoint_dir=checkpoint_dir, db_path=db_path, **kwargs)

        # If RocksDB is available, state was already restored in constructor.
        if eng._store is not None:
            return eng

        # Fall back to JSON file.
        path = os.path.join(checkpoint_dir, "checkpoint.json")
        if os.path.exists(path):
            with open(path) as f:
                snap = json.load(f)
            eng.last_T_commit = snap["last_T_commit"]
            eng.local_watermark = snap["local_watermark"]
            eng.max_event_time = snap["max_event_time"]
            eng.watermark = snap["watermark"]
            eng.kafka_committed_offset = snap.get("kafka_committed_offset", 0)
            for k, v in snap["open_windows"].items():
                eng.open_windows[float(k)] = WindowState(
                    count=v["count"], status_500=v["status_500"]
                )
            for eid in snap.get("seen_ids", []):
                eng._seen_ids_ttl[eid] = snap.get("last_checkpoint_time", time.time())
            eng._last_checkpoint_time = snap.get("last_checkpoint_time", time.time())
        return eng

    def flush(self) -> None:
        for w in sorted(self.open_windows):
            st = self.open_windows[w]
            st.eviction = EvictionState.UPLOADING
            result = WindowResult(
                window_id=self.tumbling.window_id(self.partition_id, w),
                partition_id=self.partition_id,
                window_start=w,
                window_end=w + self.tumbling.size,
                count=st.count,
                status_500=st.status_500,
                is_speculative=False,
                version=1,
            )
            self.closed_windows[w] = result
            self._persist_closed_window(w, result, EvictionState.UPLOADING)
            self._upload_to_tiered_storage(w, result)
            # If no external tiered storage, advance to UPLOADED immediately
            if self.tiered_storage is None:
                self._persist_closed_window(w, result, EvictionState.UPLOADED)
            st.eviction = EvictionState.UPLOADED
            if self.output_manager is not None:
                self.output_manager.emit(result)
            # Transition to PURGED: delete local RocksDB data
            self._purge_window_from_store(w)
            st.eviction = EvictionState.PURGED

        # Batch-delete all open windows from RocksDB
        if self._store is not None:
            self._store.clear_prefix(_PFX_OPEN)
            self._store.flush()
        self.open_windows.clear()

    def _upload_to_tiered_storage(self, window_start: float, window_result: WindowResult,
                                   sync: bool = False) -> bool:
        """Upload window to tiered storage. Returns True if upload succeeded."""
        if self.tiered_storage is None:
            return True
        if self.diff_eviction is not None:
            self.diff_eviction.evict_window(
                window_result.partition_id,
                window_result.window_id,
                dataclasses.asdict(window_result),
            )
            return True
        else:
            return self.tiered_storage.upload_window(
                window_result.window_id,
                dataclasses.asdict(window_result),
                window_result.partition_id,
                sync=sync,
            )

    def close(self) -> None:
        """Close the RocksDB store if open."""
        if self._store is not None:
            self._store.close()
            self._store = None

    def summary(self) -> dict:
        open_count = len(self.open_windows)
        closed_count = (
            self._store.count(prefix=_PFX_CLOSED)
            if self._store is not None
            else len(self.closed_windows)
        )
        seen_count = (
            self._store.count(prefix=_PFX_SEEN)
            if self._store is not None
            else len(self._seen_ids_ttl)
        )
        latencies = self.proc_latencies_ns
        p50 = 0.0
        p95 = 0.0
        p99 = 0.0
        if latencies:
            sorted_l = sorted(latencies)
            n = len(sorted_l)
            p50 = sorted_l[int(n * 0.5)] / 1000.0
            p95 = sorted_l[int(n * 0.95)] / 1000.0
            p99 = sorted_l[int(n * 0.99)] / 1000.0

        def _lat_us(values: list[float], percentile: float) -> float:
            values = [v for v in values if v >= 0]
            if not values:
                return 0.0
            sorted_v = sorted(values)
            idx = min(int(len(sorted_v) * percentile), len(sorted_v) - 1)
            return round(sorted_v[idx] / 1000.0, 2)

        # Eviction state detection
        ev_state = 0
        win_id = f"w_{self.partition_id}_none"
        if self.tiered_storage and getattr(self.tiered_storage, "eviction", None):
            closed_win_ids = [self.tumbling.window_id(self.partition_id, w) for w in self.closed_windows]
            if closed_win_ids:
                win_id = closed_win_ids[-1]
                st = self.tiered_storage.eviction.get_state(win_id)
                if st:
                    val = st.value if hasattr(st, "value") else str(st)
                    ev_state = {"closed": 0, "uploading": 1, "uploaded": 2, "purged": 3}.get(val, 0)

        result = {
            "mode": "strict",
            "watermark": self.watermark,
            "local_watermark": self.local_watermark,  # useful to show local watermark in detail
            "delta_base_s": self.delta_base,
            "open_windows": open_count,
            "closed_windows": closed_count,
            "total_received": self.metrics.total_received,
            "on_time": self.metrics.on_time,
            "data_completeness_pct": self.metrics.data_completeness(),
            "late_dropped": self.metrics.late_dropped,
            "duplicates_filtered": self.metrics.duplicates,
            "backpressure_drops": self.metrics.backpressure_drops,
            "non_monotonic_punctuation": self.metrics.non_monotonic_punctuation,
            "dedup_ttl_entries": seen_count,
            "proc_latency_p50_us": round(p50, 2),
            "proc_latency_p95_us": round(p95, 2),
            "proc_latency_p99_us": round(p99, 2),
            "poll_decode_latency_p50_us": _lat_us(self.metrics.T_poll_decode_ns, 0.50),
            "poll_decode_latency_p95_us": _lat_us(self.metrics.T_poll_decode_ns, 0.95),
            "poll_decode_latency_p99_us": _lat_us(self.metrics.T_poll_decode_ns, 0.99),
            "dedup_latency_p50_us": _lat_us(self.metrics.T_deduplication_ns, 0.50),
            "dedup_latency_p95_us": _lat_us(self.metrics.T_deduplication_ns, 0.95),
            "dedup_latency_p99_us": _lat_us(self.metrics.T_deduplication_ns, 0.99),
            "state_write_latency_p50_us": _lat_us(self.metrics.T_state_write_ns, 0.50),
            "state_write_latency_p95_us": _lat_us(self.metrics.T_state_write_ns, 0.95),
            "state_write_latency_p99_us": _lat_us(self.metrics.T_state_write_ns, 0.99),
            # Gap 3 metrics
            "active_partitions": getattr(self, "active_partitions", 1),
            "clock_skew_ms": getattr(self, "clock_skew_ms", 0.0),
            "punctuation_total": self.punctuation_total,
            "eviction_state": ev_state,
            "window_id": win_id,
            "ingestor_id": f"ingestor_{self.partition_id}",
            "ingestor_health_rtt_ms": 0.0,
            "ingestor_network_rtt_seconds": 0.0,
        }
        if self.tiered_storage:
            result["tier_storage"] = self.tiered_storage.get_storage_stats()
        return result
