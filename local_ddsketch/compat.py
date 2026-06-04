"""Lớp tương thích DDSketch cho API gần giống thư viện chuẩn.

Trước đây module này từng cố bọc package `ddsketch` cài từ pip. Trong Docker,
việc shadow path có thể làm `sys.modules['ddsketch']` trỏ ngược về package local
và gây đệ quy import. Cách hiện tại an toàn hơn: luôn ủy quyền sang implementation
thuần Python trong `sketch.py`, không cần import package ngoài.

API chính được giữ nguyên cho caller cũ:
  - `DDSketch(alpha=0.01)`
  - `add(value)`, `add_many(values)`, `quantile(q)`
  - `merge(other)`, `merge_into(other)`, `copy()`
  - `to_dict()` / `from_dict()`
  - `to_protobuf()` / `from_protobuf()`
  - `SlidingWindowDDSketch(window_seconds=60.0, ...)`
  - `advance(current_time)`, `total_count`, `active_sub_sketches`
"""

from __future__ import annotations

import base64
import math
import pickle
import time
from typing import Dict, List, Optional, Tuple

# Always use our own pure-Python implementation — no pip-package dependency,
# no sys.path / sys.modules shadowing issues.
from local_ddsketch.sketch import DDSketch as _PureDDSketch, SlidingWindowDDSketch as _PureSlidingWindow


# ---------------------------------------------------------------------------
# DDSketch — thin compatibility shim over sketch.DDSketch
# ---------------------------------------------------------------------------

class DDSketch:
    """Lớp `DDSketch` gom dữ liệu và hành vi liên quan đến DDSketch.
    
    Ghi chú gốc:
    Compatibility wrapper around ``sketch.DDSketch`` with extended API.
    
        Parameters
        ----------
        alpha : float
            Relative error guarantee (default 0.01 = 1%).
        max_buckets : int
            Hard cap on non-empty buckets (default 1024).
        min_value : float
            Smallest representable value (inputs below are clamped).
            Negative values are silently dropped (legacy behaviour).
        max_value : float
            Largest representable value (inputs above are clamped).
    """

    def __init__(
        self,
        alpha: float = 0.01,
        max_buckets: int = 1024,
        min_value: float = 1e-3,
        max_value: float = 3600.0,
    ) -> None:
        """Khởi tạo đối tượng của `DDSketch` và thiết lập trạng thái ban đầu."""
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

        self._sketch = _PureDDSketch(
            alpha=alpha,
            max_buckets=max_buckets,
            min_value=min_value,
            max_value=max_value,
        )

    # --- public properties --------------------------------------------------

    @property
    def total_count(self) -> int:
        """Hàm `total_count` thực hiện phần xử lý liên quan đến total count của `DDSketch`.
        
        Ghi chú gốc:
        Total number of values inserted (excluding negative / dropped).
        """
        return self._sketch.total_count

    @property
    def bucket_count(self) -> int:
        """Hàm `bucket_count` thực hiện phần xử lý liên quan đến bucket count của `DDSketch`.
        
        Ghi chú gốc:
        Approximate number of non-empty bins in the underlying store.
        """
        try:
            return len([c for c in self._sketch._buckets.values() if c > 0])
        except AttributeError:
            pass
        return 0

    # --- insert -------------------------------------------------------------

    def add(self, value: float) -> None:
        """Hàm `add` thực hiện phần xử lý liên quan đến add của `DDSketch`.
        
        Ghi chú gốc:
        Add a single value to the sketch.
        
                Values outside [min_value, max_value] are capped to the nearest bound.
                Negative values are silently dropped (legacy behaviour).
        """
        self._sketch.add(value)

    def add_many(self, values: List[float]) -> None:
        """Hàm `add_many` thực hiện phần xử lý liên quan đến add many của `DDSketch`.
        
        Ghi chú gốc:
        Batch-insert a list of values.
        """
        for v in values:
            self.add(v)

    # --- quantile -----------------------------------------------------------

    def quantile(self, q: float) -> float:
        """Hàm `quantile` thực hiện phần xử lý liên quan đến quantile của `DDSketch`.
        
        Ghi chú gốc:
        Return the estimated *q*-quantile (0 <= q <= 1).
        
                Returns 0.0 when the sketch is empty.
        """
        if not (0.0 <= q <= 1.0):
            raise ValueError(f"q must be in [0, 1], got {q}")
        return self._sketch.quantile(q)

    # --- merge --------------------------------------------------------------

    def merge(self, other: "DDSketch") -> "DDSketch":
        """Hàm `merge` thực hiện phần xử lý liên quan đến merge của `DDSketch`.
        
        Ghi chú gốc:
        Merge *other* into a **new** sketch (legacy API: returns new object).
        """
        merged = self.copy()
        merged.merge_into(other)
        return merged

    def merge_into(self, other: "DDSketch") -> None:
        """Hàm `merge_into` thực hiện phần xử lý liên quan đến merge into của `DDSketch`.
        
        Ghi chú gốc:
        Merge *other* into **this** sketch in-place.
        """
        self._sketch.merge_into(other._sketch)

    def copy(self) -> "DDSketch":
        """Hàm `copy` thực hiện phần xử lý liên quan đến copy của `DDSketch`.
        
        Ghi chú gốc:
        Return a deep copy with identical configuration and state.
        """
        cp = DDSketch(
            alpha=self.alpha,
            max_buckets=self.max_buckets,
            min_value=self.min_value,
            max_value=self.max_value,
        )
        # Use merge_into on the underlying pure sketch to clone state.
        cp._sketch.merge_into(self._sketch)
        return cp

    # --- serialisation ------------------------------------------------------

    def to_protobuf(self) -> bytes:
        """Hàm `to_protobuf` thực hiện phần xử lý liên quan đến to protobuf của `DDSketch`.
        
        Ghi chú gốc:
        Serialize to bytes (pickle-based for checkpoint compatibility).
        """
        return pickle.dumps({
            "alpha": self.alpha,
            "max_buckets": self.max_buckets,
            "min_value": self.min_value,
            "max_value": self.max_value,
            "total_count": self.total_count,
            "sketch": self._sketch,
        })

    @classmethod
    def from_protobuf(cls, data: bytes) -> "DDSketch":
        """Hàm `from_protobuf` thực hiện phần xử lý liên quan đến from protobuf của `DDSketch`.
        
        Ghi chú gốc:
        Deserialize from bytes produced by ``to_protobuf``.
        """
        d = pickle.loads(data)
        sketch = cls(
            alpha=d["alpha"],
            max_buckets=d.get("max_buckets", 1024),
            min_value=d.get("min_value", 1e-3),
            max_value=d.get("max_value", 3600.0),
        )
        sketch._sketch = d["sketch"]
        return sketch

    def to_dict(self) -> dict:
        """Hàm `to_dict` thực hiện phần xử lý liên quan đến to dict của `DDSketch`.
        
        Ghi chú gốc:
        Serialize to a plain dict (suitable for JSON via base64 pickle).
        """
        pickled_bytes = pickle.dumps(self._sketch)
        return {
            "v": 2,  # format version
            "alpha": self.alpha,
            "max_buckets": self.max_buckets,
            "min_value": self.min_value,
            "max_value": self.max_value,
            "total_count": self.total_count,
            "pickle": base64.b64encode(pickled_bytes).decode("ascii"),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "DDSketch":
        """Hàm `from_dict` thực hiện phần xử lý liên quan đến from dict của `DDSketch`.
        
        Ghi chú gốc:
        Deserialize from a dict previously produced by ``to_dict``.
        """
        sketch = cls(
            alpha=d["alpha"],
            max_buckets=d.get("max_buckets", 1024),
            min_value=d.get("min_value", 1e-3),
            max_value=d.get("max_value", 3600.0),
        )
        pickled_bytes = base64.b64decode(d["pickle"].encode("ascii"))
        sketch._sketch = pickle.loads(pickled_bytes)
        return sketch

    # --- repr ---------------------------------------------------------------

    def __repr__(self) -> str:
        """Hàm `__repr__` thực hiện phần xử lý liên quan đến repr của `DDSketch`."""
        return (
            f"DDSketch(alpha={self.alpha}, count={self.total_count})"
        )


# ---------------------------------------------------------------------------
# SlidingWindowDDSketch — thin shim over sketch.SlidingWindowDDSketch
# ---------------------------------------------------------------------------

class SlidingWindowDDSketch:
    """Lớp `SlidingWindowDDSketch` gom dữ liệu và hành vi liên quan đến SlidingWindowDDSketch.
    
    Ghi chú gốc:
    Quantile sketch over a sliding time window.
    
        Parameters
        ----------
        window_seconds : float
            Total width of the sliding window in seconds (default 60).
        sub_sketch_granularity : float
            Duration each sub-sketch covers in seconds (default 1).
        alpha : float
            Relative error passed to each underlying DDSketch.
        max_buckets : int
            Bucket cap passed to each underlying DDSketch.
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
        """Khởi tạo đối tượng của `SlidingWindowDDSketch` và thiết lập trạng thái ban đầu."""
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

        # Merged-sketch cache (200ms TTL) to avoid re-merging on hot-path.
        self._cached_quantiles: dict = {}
        self._cache_ts: float = 0.0
        self._cache_ttl_s: float = 0.2

    # --- helpers ------------------------------------------------------------

    def _sub_sketch_start(self, timestamp: float) -> float:
        """Hàm `_sub_sketch_start` thực hiện phần xử lý liên quan đến sub sketch start của `SlidingWindowDDSketch`.
        
        Ghi chú gốc:
        Return the start boundary of the granularity bucket for *timestamp*.
        """
        return (
            math.floor(timestamp / self.sub_sketch_granularity)
            * self.sub_sketch_granularity
        )

    def _make_sub_sketch(self) -> DDSketch:
        """Hàm `_make_sub_sketch` thực hiện phần xử lý liên quan đến make sub sketch của `SlidingWindowDDSketch`.
        
        Ghi chú gốc:
        Create an empty sub-sketch with this window's parameters.
        """
        return DDSketch(
            alpha=self.alpha,
            max_buckets=self.max_buckets,
            min_value=self.min_value,
            max_value=self.max_value,
        )

    def _prune(self) -> None:
        """Hàm `_prune` thực hiện phần xử lý liên quan đến prune của `SlidingWindowDDSketch`.
        
        Ghi chú gốc:
        Remove sub-sketches whose start time has fallen outside the window.
        """
        if self._latest_time is None:
            return
        cutoff = self._latest_time - self.window_seconds
        expired = [ts for ts in self._sketches if ts < cutoff]
        for ts in expired:
            del self._sketches[ts]
        if expired:
            self._cache_ts = 0.0

    # --- insert -------------------------------------------------------------

    def add(self, value: float, timestamp: Optional[float] = None) -> None:
        """Hàm `add` thực hiện phần xử lý liên quan đến add của `SlidingWindowDDSketch`.
        
        Ghi chú gốc:
        Add *value* to the sub-sketch covering *timestamp*.
        
                If *timestamp* is ``None``, ``time.time()`` is used.  Values with a
                timestamp older than the current window are silently dropped.
                Negative values are silently dropped (delegated to DDSketch.add).
        """
        if timestamp is None:
            timestamp = time.time()

        if self._latest_time is None:
            self._latest_time = timestamp

        if timestamp > self._latest_time:
            self._latest_time = timestamp
            self._prune()

        if timestamp < self._latest_time - self.window_seconds:
            return

        start_ts = self._sub_sketch_start(timestamp)
        if start_ts not in self._sketches:
            self._sketches[start_ts] = self._make_sub_sketch()

        self._sketches[start_ts].add(value)
        self._cache_ts = 0.0

    def add_many(
        self, values: List[Tuple[float, float]]
    ) -> None:
        """Hàm `add_many` thực hiện phần xử lý liên quan đến add many của `SlidingWindowDDSketch`.
        
        Ghi chú gốc:
        Batch-insert (value, timestamp) pairs.
        """
        for value, ts in values:
            self.add(value, ts)

    # --- window management --------------------------------------------------

    def advance(self, current_time: float) -> None:
        """Hàm `advance` thực hiện phần xử lý liên quan đến advance của `SlidingWindowDDSketch`.
        
        Ghi chú gốc:
        Rotate the window so *current_time* becomes the leading edge.
        """
        if self._latest_time is None or current_time > self._latest_time:
            self._latest_time = current_time
        self._prune()

    # --- quantile -----------------------------------------------------------

    def quantile(self, q: float) -> float:
        """Hàm `quantile` thực hiện phần xử lý liên quan đến quantile của `SlidingWindowDDSketch`.
        
        Ghi chú gốc:
        Return the estimated *q*-quantile across all active sub-sketches.
        
                Returns 0.0 when no data is present in the window.
                Uses a 200ms merged-sketch cache to amortise merge cost.
        """
        if not self._sketches:
            return 0.0

        if q in self._cached_quantiles and time.time() - self._cache_ts < self._cache_ttl_s:
            return self._cached_quantiles[q]

        sketches = list(self._sketches.values())
        merged = sketches[0].copy()
        for sk in sketches[1:]:
            merged.merge_into(sk)

        COMMON_QS = (0.50, 0.95, 0.99, 0.999)
        self._cached_quantiles = {cq: merged.quantile(cq) for cq in COMMON_QS}
        self._cached_quantiles[q] = merged.quantile(q)
        self._cache_ts = time.time()

        return self._cached_quantiles[q]

    # --- properties ---------------------------------------------------------

    @property
    def total_count(self) -> int:
        """Hàm `total_count` thực hiện phần xử lý liên quan đến total count của `SlidingWindowDDSketch`.
        
        Ghi chú gốc:
        Total number of samples across all active sub-sketches.
        """
        return sum(sk.total_count for sk in self._sketches.values())

    @property
    def active_sub_sketches(self) -> int:
        """Hàm `active_sub_sketches` thực hiện phần xử lý liên quan đến active sub sketches của `SlidingWindowDDSketch`.
        
        Ghi chú gốc:
        Number of sub-sketches currently within the window.
        """
        return len(self._sketches)

    # --- serialisation ------------------------------------------------------

    def to_dict(self) -> dict:
        """Hàm `to_dict` thực hiện phần xử lý liên quan đến to dict của `SlidingWindowDDSketch`.
        
        Ghi chú gốc:
        Serialize to a plain dict (suitable for JSON).
        """
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
    def from_dict(cls, d: dict) -> "SlidingWindowDDSketch":
        """Hàm `from_dict` thực hiện phần xử lý liên quan đến from dict của `SlidingWindowDDSketch`.
        
        Ghi chú gốc:
        Deserialize from a dict previously produced by ``to_dict``.
        """
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
        """Hàm `__repr__` thực hiện phần xử lý liên quan đến repr của `SlidingWindowDDSketch`."""
        return (
            f"SlidingWindowDDSketch(window={self.window_seconds}s, "
            f"granularity={self.sub_sketch_granularity}s, "
            f"sub_sketches={len(self._sketches)}, "
            f"count={self.total_count})"
        )
