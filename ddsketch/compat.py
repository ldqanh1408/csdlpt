"""
ddsketch/compat.py — Compatibility wrapper around the official ``ddsketch`` library.

Wraps the official ``ddsketch`` v3.0.1 and presents our legacy API so that
existing callers require zero changes.

Key API mappings
----------------
  Legacy (our custom)              Official ddsketch library
  ───────────────────────────────  ───────────────────────────────
  DDSketch(alpha=0.01)            ddsketch.DDSketch(relative_accuracy=0.01)
  s.add(value)                    s.add(value)
  s.quantile(q)                   s.get_quantile_value(q)
  s.total_count  (int)            s.count  (float)
  s.merge(other) -> new           s.merge(other)  (in-place)
  s.to_dict() -> dict             s.to_proto() -> bytes
  DDSketch.from_dict(d)           DDSketch.from_proto(bytes)
"""

from __future__ import annotations

import base64
import math
import pickle
import time
from typing import Dict, List, Optional, Tuple

import ddsketch as _ddsketch


# ---------------------------------------------------------------------------
# DDSketch — compatibility wrapper
# ---------------------------------------------------------------------------

class DDSketch:
    """Wrapper around the official ``ddsketch.DDSketch`` presenting our legacy API.

    Parameters
    ----------
    alpha : float
        Relative error guarantee (default 0.01 = 1%).  Passed through to the
        official library as ``relative_accuracy``.
    max_buckets : int
        Stored for backward compatibility.  The official library manages its
        own bin limits internally based on *alpha*.
    min_value : float
        Smallest representable value.  Inputs below this are clamped up.
        Negative values are silently dropped (legacy behaviour).
    max_value : float
        Largest representable value.  Inputs above this are clamped down.
    """

    def __init__(
        self,
        alpha: float = 0.01,
        max_buckets: int = 1024,
        min_value: float = 1e-3,
        max_value: float = 3600.0,
    ) -> None:
        if alpha <= 0 or alpha >= 1:
            raise ValueError("alpha must be in (0, 1)")
        if max_buckets < 2:
            raise ValueError("max_buckets must be >= 2")
        if min_value <= 0:
            raise ValueError("min_value must be > 0")
        if max_value <= min_value:
            raise ValueError("max_value must be > min_value")

        self.alpha = alpha
        self.max_buckets = max_buckets
        self.min_value = min_value
        self.max_value = max_value

        # The official library treats *relative_accuracy* identically to
        # *alpha* in the DDSketch algorithm: gamma = (1+alpha)/(1-alpha).
        self._sketch = _ddsketch.DDSketch(alpha=alpha)

        # Track total count separately because the official library accepts
        # negative values but our legacy API silently drops them.  We need
        # ``total_count`` to reflect only the values we actually forwarded.
        self._total_count: int = 0

    # --- public properties --------------------------------------------------

    @property
    def total_count(self) -> int:
        """Total number of (non-negative, in-range) values inserted."""
        return self._total_count

    @property
    def bucket_count(self) -> int:
        """Approximate number of non-empty bins in the underlying store.

        The official library does not expose bucket count directly.  We
        read the internal store's total count of populated bins.
        """
        store = getattr(self._sketch, "_store", None)
        if store is None:
            return 0
        # DenseStore exposes ``count`` (total count across bins) and
        # ``bins`` (the raw bin array).  We approximate non-empty bins
        # by counting non-zero entries.
        try:
            bins = store.bins
            return sum(1 for b in bins if b > 0)
        except (TypeError, AttributeError):
            pass
        return 0

    # --- insert -------------------------------------------------------------

    def add(self, value: float) -> None:
        """Add a single value to the sketch.

        Values outside [min_value, max_value] are capped to the nearest bound.
        Negative values are silently dropped (legacy behaviour).
        """
        if value < 0:
            return
        capped = max(self.min_value, min(self.max_value, value))
        self._sketch.add(capped)
        self._total_count += 1

    def add_many(self, values: List[float]) -> None:
        """Batch-insert a list of values."""
        for v in values:
            self.add(v)

    # --- quantile -----------------------------------------------------------

    def quantile(self, q: float) -> float:
        """Return the estimated *q*-quantile (0 <= q <= 1).

        Returns 0.0 when the sketch is empty.

        Raises
        ------
        ValueError
            If *q* is outside [0, 1].
        """
        if not (0.0 <= q <= 1.0):
            raise ValueError(f"q must be in [0, 1], got {q}")
        if self._total_count == 0:
            return 0.0
        return self._sketch.get_quantile_value(q)

    # --- merge --------------------------------------------------------------

    def merge(self, other: DDSketch) -> DDSketch:
        """Merge *other* into a **new** sketch (legacy API: returns new object)."""
        merged = self.copy()
        merged.merge_into(other)
        return merged

    def merge_into(self, other: DDSketch) -> None:
        """Merge *other* into **this** sketch in-place."""
        self._total_count += other._total_count
        self._sketch.merge(other._sketch)

    def copy(self) -> DDSketch:
        """Return a deep copy with identical configuration and state."""
        cp = DDSketch(
            alpha=self.alpha,
            max_buckets=self.max_buckets,
            min_value=self.min_value,
            max_value=self.max_value,
        )
        cp._total_count = self._total_count
        # Use the library's internal _copy method to clone the sketch state.
        cp._sketch._copy(self._sketch)
        return cp

    # --- serialisation ------------------------------------------------------

    def to_protobuf(self) -> bytes:
        """Serialize to protobuf-equivalent bytes (pickle-based for checkpoint).

        The official ddsketch library does not expose a public protobuf
        serialization API.  We use pickle as a stable wire format that
        preserves full sketch state for checkpoint/restore.
        """
        return pickle.dumps({
            "alpha": self.alpha,
            "max_buckets": self.max_buckets,
            "min_value": self.min_value,
            "max_value": self.max_value,
            "total_count": self._total_count,
            "sketch": self._sketch,
        })

    @classmethod
    def from_protobuf(cls, data: bytes) -> "DDSketch":
        """Deserialize from bytes produced by ``to_protobuf``."""
        d = pickle.loads(data)
        sketch = cls(
            alpha=d["alpha"],
            max_buckets=d.get("max_buckets", 1024),
            min_value=d.get("min_value", 1e-3),
            max_value=d.get("max_value", 3600.0),
        )
        sketch._total_count = d.get("total_count", 0)
        sketch._sketch = d["sketch"]
        return sketch

    def to_dict(self) -> dict:
        """Serialize to a plain dict (suitable for JSON).

        Uses pickle internally, encoded as base64 for JSON compatibility.
        The official ``ddsketch`` library does not provide a public
        serialization API, so we rely on pickle to capture full state.
        """
        pickled_bytes = pickle.dumps(self._sketch)
        return {
            "v": 1,  # format version — future-proofing
            "alpha": self.alpha,
            "max_buckets": self.max_buckets,
            "min_value": self.min_value,
            "max_value": self.max_value,
            "total_count": self._total_count,
            "pickle": base64.b64encode(pickled_bytes).decode("ascii"),
        }

    @classmethod
    def from_dict(cls, d: dict) -> DDSketch:
        """Deserialize from a dict previously produced by ``to_dict``."""
        sketch = cls(
            alpha=d["alpha"],
            max_buckets=d.get("max_buckets", 1024),
            min_value=d.get("min_value", 1e-3),
            max_value=d.get("max_value", 3600.0),
        )
        sketch._total_count = d.get("total_count", 0)
        pickled_bytes = base64.b64decode(d["pickle"].encode("ascii"))
        sketch._sketch = pickle.loads(pickled_bytes)
        return sketch

    # --- repr ---------------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"DDSketch(alpha={self.alpha}, count={self._total_count})"
        )


# ---------------------------------------------------------------------------
# SlidingWindowDDSketch — compatibility wrapper
# ---------------------------------------------------------------------------

class SlidingWindowDDSketch:
    """Quantile sketch over a sliding time window.

    Maintains *N* sub-sketches, each covering a fixed time granularity
    (default 1 second).  The oldest sub-sketch is dropped when it falls
    outside the window, and new sub-sketches are created automatically as
    time advances.

    Each sub-sketch is a :class:`DDSketch` (the compat wrapper), which
    in turn delegates to the official ``ddsketch`` library.

    Parameters
    ----------
    window_seconds : float
        Total width of the sliding window in seconds (default 60).
    sub_sketch_granularity : float
        Duration each sub-sketch covers in seconds (default 1).
        The number of sub-sketches is ``window_seconds / sub_sketch_granularity``.
    alpha : float
        Relative error passed to each underlying ``DDSketch``.
    max_buckets : int
        Bucket cap passed to each underlying ``DDSketch``.
    min_value : float
        Minimum representable value for each sub-sketch.
    max_value : float
        Maximum representable value for each sub-sketch.
    """

    def __init__(
        self,
        window_seconds: float = 60.0,
        sub_sketch_granularity: float = 1.0,
        alpha: float = 0.01,
        max_buckets: int = 1024,
        min_value: float = 1e-3,
        max_value: float = 3600.0,
    ) -> None:
        if window_seconds <= 0:
            raise ValueError("window_seconds must be > 0")
        if sub_sketch_granularity <= 0:
            raise ValueError("sub_sketch_granularity must be > 0")
        if sub_sketch_granularity > window_seconds:
            raise ValueError(
                "sub_sketch_granularity must be <= window_seconds"
            )

        self.window_seconds = window_seconds
        self.sub_sketch_granularity = sub_sketch_granularity
        self.alpha = alpha
        self.max_buckets = max_buckets
        self.min_value = min_value
        self.max_value = max_value

        self._sketches: Dict[float, DDSketch] = {}  # start_ts -> sub-sketch
        self._latest_time: Optional[float] = None

        # Merged-sketch cache: quantile() can be called up to 4 times per
        # event in the hot path.  Rebuilding a merged sketch from ~60
        # sub-sketches each time is expensive (O(sub_sketches * bins)).
        # Cache the merged result for 200ms so all quantile calls within
        # one event-processing cycle share a single merge.
        self._cached_quantiles: dict[float, float] = {}
        self._cache_ts: float = 0.0
        self._cache_ttl_s: float = 0.2

    # --- helpers ------------------------------------------------------------

    def _sub_sketch_start(self, timestamp: float) -> float:
        """Return the start boundary of the granularity bucket for *timestamp*."""
        return (
            math.floor(timestamp / self.sub_sketch_granularity)
            * self.sub_sketch_granularity
        )

    def _make_sub_sketch(self) -> DDSketch:
        """Create an empty sub-sketch with this window's parameters."""
        return DDSketch(
            alpha=self.alpha,
            max_buckets=self.max_buckets,
            min_value=self.min_value,
            max_value=self.max_value,
        )

    def _prune(self) -> None:
        """Remove sub-sketches whose start time has fallen outside the window."""
        if self._latest_time is None:
            return
        cutoff = self._latest_time - self.window_seconds
        expired = [ts for ts in self._sketches if ts < cutoff]
        for ts in expired:
            del self._sketches[ts]
        # Expired sub-sketches invalidate the merged-sketch cache.
        if expired:
            self._cache_ts = 0.0

    # --- insert -------------------------------------------------------------

    def add(self, value: float, timestamp: Optional[float] = None) -> None:
        """Add *value* to the sub-sketch covering *timestamp*.

        If *timestamp* is ``None``, ``time.time()`` is used.  Values with a
        timestamp older than the current window are silently dropped.
        Negative values are silently dropped (delegated to the underlying
        ``DDSketch.add``).
        """
        if timestamp is None:
            timestamp = time.time()

        # Initialise the window on first insertion.
        if self._latest_time is None:
            self._latest_time = timestamp

        # Extend the window forward if the timestamp is ahead.
        if timestamp > self._latest_time:
            self._latest_time = timestamp
            self._prune()

        # Drop values that arrived too late for the current window.
        if timestamp < self._latest_time - self.window_seconds:
            return

        start_ts = self._sub_sketch_start(timestamp)
        if start_ts not in self._sketches:
            self._sketches[start_ts] = self._make_sub_sketch()

        self._sketches[start_ts].add(value)
        # New data invalidates the merged-sketch cache.
        self._cache_ts = 0.0

    def add_many(
        self, values: List[Tuple[float, float]]
    ) -> None:
        """Batch-insert (value, timestamp) pairs."""
        for value, ts in values:
            self.add(value, ts)

    # --- window management --------------------------------------------------

    def advance(self, current_time: float) -> None:
        """Rotate the window so *current_time* becomes the leading edge.

        Sub-sketches whose start time is before
        ``current_time - window_seconds`` are dropped.
        """
        if self._latest_time is None or current_time > self._latest_time:
            self._latest_time = current_time
        self._prune()

    # --- quantile -----------------------------------------------------------

    def quantile(self, q: float) -> float:
        """Return the estimated *q*-quantile across all active sub-sketches.

        Returns 0.0 when no data is present in the window.

        Uses a 200ms merged-sketch cache because this method can be called
        up to 4 times per event in the hot path (L_eff update + 3x metrics
        quantile queries).  Re-merging ~60 sub-sketches each time is
        expensive; the cache amortises that cost over a single processing
        cycle.
        """
        if not self._sketches:
            return 0.0

        # Fast path: return cached quantile if still fresh.
        if q in self._cached_quantiles and time.time() - self._cache_ts < self._cache_ttl_s:
            return self._cached_quantiles[q]

        # Slow path: rebuild merged sketch and precompute common quantiles.
        sketches = list(self._sketches.values())
        merged = sketches[0].copy()
        for sk in sketches[1:]:
            merged.merge_into(sk)

        COMMON_QS = (0.50, 0.95, 0.99, 0.999)
        self._cached_quantiles = {cq: merged.quantile(cq) for cq in COMMON_QS}
        # Also cache the requested q in case it is not one of the common set.
        self._cached_quantiles[q] = merged.quantile(q)
        self._cache_ts = time.time()

        return self._cached_quantiles[q]

    # --- properties ---------------------------------------------------------

    @property
    def total_count(self) -> int:
        """Total number of samples across all active sub-sketches."""
        return sum(sk.total_count for sk in self._sketches.values())

    @property
    def active_sub_sketches(self) -> int:
        """Number of sub-sketches currently within the window."""
        return len(self._sketches)

    # --- serialisation ------------------------------------------------------

    def to_dict(self) -> dict:
        """Serialize to a plain dict (suitable for JSON)."""
        return {
            "window_seconds": self.window_seconds,
            "sub_sketch_granularity": self.sub_sketch_granularity,
            "alpha": self.alpha,
            "max_buckets": self.max_buckets,
            "min_value": self.min_value,
            "max_value": self.max_value,
            "latest_time": self._latest_time,
            "sub_sketches": [
                [ts, sk.to_dict()]
                for ts, sk in sorted(self._sketches.items())
            ],
        }

    @classmethod
    def from_dict(cls, d: dict) -> SlidingWindowDDSketch:
        """Deserialize from a dict previously produced by ``to_dict``."""
        window = cls(
            window_seconds=d["window_seconds"],
            sub_sketch_granularity=d["sub_sketch_granularity"],
            alpha=d["alpha"],
            max_buckets=d["max_buckets"],
            min_value=d.get("min_value", 1e-3),
            max_value=d.get("max_value", 3600.0),
        )
        window._latest_time = d.get("latest_time")
        window._sketches = {
            ts: DDSketch.from_dict(sk_dict)
            for ts, sk_dict in d["sub_sketches"]
        }
        return window

    # --- repr ---------------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"SlidingWindowDDSketch(window={self.window_seconds}s, "
            f"granularity={self.sub_sketch_granularity}s, "
            f"sub_sketches={len(self._sketches)}, "
            f"count={self.total_count})"
        )
