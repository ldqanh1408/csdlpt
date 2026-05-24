"""RocksDB persistent key-value store wrapper using rocksdict.

Provides dict-like interface with pickle serialization, write-batch support,
prefix-based iteration/cleanup, and context-manager protocol.
"""

import os
import pickle
from typing import Any, Iterator, Optional

import rocksdict

_BLOCK_CACHE_BYTES = 64 * 1024 * 1024  # 64 MB per instance (spec §11)


class RocksStore:
    """RocksDB-backed persistent store.

    All keys are strings (encoded as UTF-8 bytes internally).
    All values are pickled Python objects.

    Usage:
        store = RocksStore("/data/mystore")
        store.put("key1", {"count": 5})
        val = store.get("key1")
        store.close()

        # Context manager
        with RocksStore("/data/mystore") as store:
            store.put("x", 42)
    """

    def __init__(self, db_path: str, create_if_missing: bool = True) -> None:
        self.db_path: str = db_path
        self._db: Optional[rocksdict.Rdict] = None
        self._opts = rocksdict.Options()
        if create_if_missing:
            self._opts.create_if_missing(True)
        block_opts = rocksdict.BlockBasedOptions()
        block_opts.set_block_cache(rocksdict.Cache(_BLOCK_CACHE_BYTES))
        self._opts.set_block_based_table_factory(block_opts)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_connected(self) -> None:
        if self._db is None:
            os.makedirs(self.db_path, exist_ok=True)
            self._db = rocksdict.Rdict(self.db_path, self._opts)

    @staticmethod
    def _encode_key(key: str) -> bytes:
        return key.encode("utf-8")

    @staticmethod
    def _decode_key(raw: bytes) -> str:
        return raw.decode("utf-8")

    @staticmethod
    def _encode_value(value: Any) -> bytes:
        return pickle.dumps(value)

    @staticmethod
    def _decode_value(raw: bytes) -> Any:
        return pickle.loads(raw)

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    def put(self, key: str, value: Any) -> None:
        """Store a value under the given key."""
        self._ensure_connected()
        self._db[self._encode_key(key)] = self._encode_value(value)

    def get(self, key: str) -> Optional[Any]:
        """Retrieve a value by key.  Returns None when missing."""
        self._ensure_connected()
        try:
            return self._decode_value(self._db[self._encode_key(key)])
        except KeyError:
            return None

    def delete(self, key: str) -> None:
        """Remove a key (no-op if absent)."""
        self._ensure_connected()
        try:
            del self._db[self._encode_key(key)]
        except KeyError:
            pass

    def contains(self, key: str) -> bool:
        """Return True if the key exists in the store."""
        self._ensure_connected()
        return self._encode_key(key) in self._db

    # ------------------------------------------------------------------
    # Bulk / iteration
    # ------------------------------------------------------------------

    def items(self, prefix: str = "") -> Iterator[tuple[str, Any]]:
        """Iterate over all (key, value) pairs, optionally filtered by prefix."""
        self._ensure_connected()
        prefix_bytes = prefix.encode("utf-8") if prefix else b""
        for k_raw, v_raw in self._db.items():
            if prefix_bytes and not k_raw.startswith(prefix_bytes):
                continue
            yield self._decode_key(k_raw), self._decode_value(v_raw)

    def keys(self, prefix: str = "") -> Iterator[str]:
        """Iterate over all keys, optionally filtered by prefix."""
        self._ensure_connected()
        prefix_bytes = prefix.encode("utf-8") if prefix else b""
        for k_raw in self._db.keys():
            if prefix_bytes and not k_raw.startswith(prefix_bytes):
                continue
            yield self._decode_key(k_raw)

    def count(self, prefix: str = "") -> int:
        """Count keys matching the given prefix."""
        self._ensure_connected()
        prefix_bytes = prefix.encode("utf-8") if prefix else b""
        n = 0
        for k_raw in self._db.keys():
            if prefix_bytes and not k_raw.startswith(prefix_bytes):
                continue
            n += 1
        return n

    def clear_prefix(self, prefix: str) -> int:
        """Delete all keys that start with *prefix*.  Returns number deleted.

        Uses RocksDB range-delete when available; falls back to batched
        iteration otherwise.
        """
        self._ensure_connected()
        prefix_bytes = prefix.encode("utf-8")
        deleted = 0

        # Attempt efficient range-delete first.
        try:
            end_bytes = prefix_bytes + b"\xff" * 8
            batch = rocksdict.WriteBatch()
            batch.delete_range(prefix_bytes, end_bytes)
            self._db.write(batch)
            return -1  # range-delete gives no count
        except (AttributeError, TypeError):
            # Fall-back: iterate and batch-delete.
            batch = rocksdict.WriteBatch()
            for k_raw in self._db.keys():
                if k_raw.startswith(prefix_bytes):
                    batch.delete(k_raw)
                    deleted += 1
                    if deleted % 1000 == 0:
                        self._db.write(batch)
                        batch = rocksdict.WriteBatch()
            if deleted > 0 and deleted % 1000 != 0:
                self._db.write(batch)
            return deleted

    # ------------------------------------------------------------------
    # Write batch  (atomic multi-key writes)
    # ------------------------------------------------------------------

    def write_batch(self, operations: list[tuple[str, str, Any]]) -> None:
        """Execute an atomic batch of operations.

        Each element is a 3-tuple:
            ("put",    key: str, value: Any)
            ("delete", key: str, _dummy)

        Example:
            store.write_batch([
                ("put", "a", 1),
                ("put", "b", 2.5),
                ("delete", "old", None),
            ])
        """
        self._ensure_connected()
        batch = rocksdict.WriteBatch()
        for op, key, val in operations:
            key_bytes = self._encode_key(key)
            if op == "put":
                batch.put(key_bytes, self._encode_value(val))
            elif op == "delete":
                batch.delete(key_bytes)
            else:
                raise ValueError(f"Unknown batch operation: {op!r}")
        self._db.write(batch)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def flush(self) -> None:
        """Flush pending writes to disk for durability."""
        if self._db is not None:
            self._db.flush()

    def close(self) -> None:
        """Close the database cleanly."""
        if self._db is not None:
            try:
                self._db.close()
            except Exception:
                pass
            finally:
                self._db = None

    def __enter__(self) -> "RocksStore":
        self._ensure_connected()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.close()
        return False

    @property
    def is_open(self) -> bool:
        return self._db is not None
