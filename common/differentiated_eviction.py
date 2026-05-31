"""Differentiated Tiered Eviction — per-partition-type eviction strategies.

Spec §8.6: Normal partitions flush straight to Tier 3 (MinIO), purge Tier 1.
Recovery partitions: aggressive flush to Tier 2 (local checkpoint file) first,
then upload to Tier 3 (MinIO). Tier 2 file serves as fallback if Tier 3 fails.
"""

import json
import logging
import os
from enum import Enum

from common.tiered_storage import TieredStorageManager
from common.types import EvictionState

logger = logging.getLogger("differentiated_eviction")


class PartitionEvictionType(Enum):
    NORMAL = "normal"
    RECOVERY = "recovery"


class DifferentiatedEvictionManager:
    """Wraps TieredStorageManager with partition-type-aware eviction strategies."""

    def __init__(self, storage: TieredStorageManager):
        self.storage = storage
        self.eviction = storage.eviction
        self._partition_types: dict[int, PartitionEvictionType] = {}

    def set_partition_type(self, partition_id: int, ptype: PartitionEvictionType) -> None:
        self._partition_types[partition_id] = ptype

    def get_partition_type(self, partition_id: int) -> PartitionEvictionType:
        return self._partition_types.get(partition_id, PartitionEvictionType.NORMAL)

    def is_recovery(self, partition_id: int) -> bool:
        return self.get_partition_type(partition_id) == PartitionEvictionType.RECOVERY

    def evict_window(self, partition_id: int, window_id: str, window_data: dict) -> bool:
        ptype = self.get_partition_type(partition_id)
        if ptype == PartitionEvictionType.NORMAL:
            return self._evict_normal(partition_id, window_id, window_data)
        else:
            return self._evict_recovery(partition_id, window_id, window_data)

    def _evict_normal(self, partition_id: int, window_id: str, window_data: dict) -> bool:
        logger.debug("Normal eviction: %s -> Tier 3", window_id)
        ok = self.storage.upload_window(window_id, window_data, partition_id)
        if ok:
            self.eviction.set_state(window_id, EvictionState.UPLOADED)
        return ok

    def _evict_recovery(self, partition_id: int, window_id: str, window_data: dict) -> bool:
        logger.debug("Recovery eviction: %s -> Tier 2/3 (aggressive)", window_id)
        self.eviction.set_state(window_id, EvictionState.UPLOADING)

        # Tier 2: flush to local checkpoint file first
        checkpoint_dir = os.environ.get("CHECKPOINT_DIR", "/data/checkpoint")
        tier2_dir = os.path.join(checkpoint_dir, "tier2_recovery")
        tier2_file = os.path.join(tier2_dir, f"{window_id}.json")

        try:
            os.makedirs(tier2_dir, exist_ok=True)
            with open(tier2_file, "w") as f:
                json.dump(window_data, f)
            logger.debug("Recovery Tier 2 saved: %s", tier2_file)
        except OSError as exc:
            logger.error("Failed to save recovery Tier 2 checkpoint for %s: %s", window_id, exc)
            return False

        # Tier 3: upload to MinIO
        ok = self.storage.upload_window(window_id, window_data, partition_id)
        if ok:
            self.eviction.set_state(window_id, EvictionState.UPLOADED)
            self.storage.purge_window(window_id, partition_id)
            # Cleanup Tier 2 after successful Tier 3 upload
            try:
                os.remove(tier2_file)
                logger.debug("Recovery Tier 2 cleaned up after Tier 3 success: %s", tier2_file)
            except OSError:
                logger.warning("Failed to clean up Tier 2 file: %s", tier2_file)
        else:
            # Tier 3 failed — keep Tier 2 file as local fallback
            logger.warning(
                "Recovery Tier 3 upload failed for %s; retaining Tier 2 fallback: %s",
                window_id, tier2_file,
            )

        return ok

    def purge_completed(self, partition_id: int) -> list[str]:
        purged = []
        ptype = self.get_partition_type(partition_id)
        if ptype == PartitionEvictionType.NORMAL:
            objs = self.storage.list_windows(partition_id)
            for obj_name in objs:
                window_id = obj_name.split("/")[-1].replace(".json", "")
                state = self.eviction.get_state(window_id)
                if state == EvictionState.UPLOADED:
                    if self.storage.purge_window(window_id, partition_id):
                        purged.append(window_id)
        return purged

    def summary(self) -> dict:
        s = self.eviction.summary()
        s["partition_types"] = {str(k): v.value for k, v in self._partition_types.items()}
        return s
