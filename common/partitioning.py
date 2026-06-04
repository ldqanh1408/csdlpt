"""Stable partition assignment helpers."""

from __future__ import annotations

import hashlib


def stable_partition(key, total_partitions: int = 12) -> int:
    """Map a partition key to the same partition in every Python process."""
    total = max(int(total_partitions), 1)
    raw = str(key).encode("utf-8", errors="replace")
    digest = hashlib.blake2b(raw, digest_size=8).digest()
    return int.from_bytes(digest, "big") % total
