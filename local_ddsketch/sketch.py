"""Cài đặt DDSketch và SlidingWindowDDSketch cho hệ thống watermark.

DDSketch là cấu trúc dữ liệu ước lượng quantile có thể merge, dùng sai số tương
đối alpha. Giá trị dương được ánh xạ vào bucket logarithmic; mỗi bucket chỉ lưu
counter. Khi hỏi quantile, sketch duyệt bucket tăng dần tới khi tổng tích lũy
đạt rank mục tiêu và trả về cận dưới của bucket đó.

Thuật toán:
  - Với sai số tương đối alpha, đặt `gamma = (1 + alpha) / (1 - alpha)`.
  - Với giá trị x trong `[min_value, max_value]`:
    `bucket(x) = ceil(log_gamma(x / min_value))`.
  - Cận dưới bucket i là `min_value * gamma**i`.
  - Sai số tương đối được khống chế vì mọi x trong bucket i nằm trong khoảng
    log tương ứng của gamma.

Khả năng merge:
  - Hai DDSketch cùng alpha/min/max có thể merge bằng cách cộng counter từng
    bucket. Merge không làm tăng sai số, nên phù hợp với hệ phân tán: mỗi worker
    giữ sketch cục bộ, aggregator/coordinator có thể cộng lại khi cần.

Giới hạn bộ nhớ:
  - `max_buckets` giới hạn số bucket không rỗng.
  - Khi vượt giới hạn, hai bucket thấp nhất được gộp lại.
  - Giá trị âm bị bỏ qua; giá trị quá nhỏ/quá lớn được clamp theo min/max.

SlidingWindowDDSketch:
  - Giữ nhiều sub-sketch theo lát thời gian, mặc định mỗi lát 1 giây.
  - Khi `advance()` tới thời điểm mới, sub-sketch hết hạn bị loại bỏ.
  - Query quantile sẽ merge các sub-sketch còn active.
"""

from __future__ import annotations

import math
import time
from typing import Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# DDSketch
# ---------------------------------------------------------------------------

class DDSketch:
    """Lớp `DDSketch` gom dữ liệu và hành vi liên quan đến DDSketch.
    
    Ghi chú gốc:
    Fixed-memory quantile sketch with relative error guarantee.
    
        Parameters
        ----------
        alpha : float
            Relative error guarantee (default 0.01 = 1%).
        max_buckets : int
            Hard cap on the number of non-empty buckets (default 1024).
        min_value : float
            Smallest representable value. Inputs below this are clamped up.
        max_value : float
            Largest representable value. Inputs above this are clamped down
            (the caller is expected to route genuine overflow events to a DLQ).
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

        self._gamma: float = (1.0 + alpha) / (1.0 - alpha)
        self._log_gamma: float = math.log(self._gamma)
        self._buckets: Dict[int, int] = {}
        self._total_count: int = 0

    # --- public properties --------------------------------------------------

    @property
    def total_count(self) -> int:
        """Hàm `total_count` thực hiện phần xử lý liên quan đến total count của `DDSketch`.
        
        Ghi chú gốc:
        Total number of (non-negative, in-range) values inserted.
        """
        return self._total_count

    @property
    def bucket_count(self) -> int:
        """Hàm `bucket_count` thực hiện phần xử lý liên quan đến bucket count của `DDSketch`.
        
        Ghi chú gốc:
        Number of non-empty buckets currently stored.
        """
        return len(self._buckets)

    # --- bucket index helpers -----------------------------------------------

    def _bucket_index(self, value: float) -> int:
        """Hàm `_bucket_index` thực hiện phần xử lý liên quan đến bucket index của `DDSketch`.
        
        Ghi chú gốc:
        Compute the bucket index for a positive value.
        """
        ratio = value / self.min_value
        return int(math.ceil(math.log(ratio) / self._log_gamma))

    def _value_lower_bound(self, bucket_idx: int) -> float:
        """Hàm `_value_lower_bound` thực hiện phần xử lý liên quan đến value lower bound của `DDSketch`.
        
        Ghi chú gốc:
        Return the smallest value that falls into the given bucket.
        """
        return self.min_value * (self._gamma ** bucket_idx)

    # --- insert -------------------------------------------------------------

    def add(self, value: float) -> None:
        """Hàm `add` thực hiện phần xử lý liên quan đến add của `DDSketch`.
        
        Ghi chú gốc:
        Add a single value to the sketch.  O(1) amortised.
        
                Values outside [min_value, max_value] are capped to the nearest bound.
                Negative values are silently dropped.
        """
        if value < 0:
            return
        capped = max(self.min_value, min(self.max_value, value))

        idx = self._bucket_index(capped)
        self._buckets[idx] = self._buckets.get(idx, 0) + 1
        self._total_count += 1

        self._maybe_collapse()

    def add_many(self, values: List[float]) -> None:
        """Hàm `add_many` thực hiện phần xử lý liên quan đến add many của `DDSketch`.
        
        Ghi chú gốc:
        Batch-insert a list of values.
        """
        for v in values:
            self.add(v)

    # --- collapse -----------------------------------------------------------

    def _maybe_collapse(self) -> None:
        """Hàm `_maybe_collapse` thực hiện phần xử lý liên quan đến maybe collapse của `DDSketch`.
        
        Ghi chú gốc:
        Collapse the two lowest-index buckets when over capacity.
        """
        while len(self._buckets) > self.max_buckets:
            sorted_indices = sorted(self._buckets.keys())
            if len(sorted_indices) < 2:
                break
            lo = sorted_indices[0]
            hi = sorted_indices[1]
            self._buckets[hi] += self._buckets[lo]
            del self._buckets[lo]

    # --- quantile -----------------------------------------------------------

    def quantile(self, q: float) -> float:
        """Hàm `quantile` thực hiện phần xử lý liên quan đến quantile của `DDSketch`.
        
        Ghi chú gốc:
        Return the estimated *q*-quantile (0 <= q <= 1).
        
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

        rank = int(math.ceil(q * self._total_count))
        if rank <= 0:
            rank = 1

        cumulative = 0
        for idx in sorted(self._buckets):
            cumulative += self._buckets[idx]
            if cumulative >= rank:
                return self._value_lower_bound(idx)

        # Fallback (should not be reached).
        last_idx = max(self._buckets)
        return self._value_lower_bound(last_idx)

    # --- merge --------------------------------------------------------------

    def merge(self, other: DDSketch) -> DDSketch:
        """Hàm `merge` thực hiện phần xử lý liên quan đến merge của `DDSketch`.
        
        Ghi chú gốc:
        Merge *other* into a **new** sketch.  Exact -- no accuracy loss.
        
                The two sketches must share the same *alpha*, *max_buckets*,
                *min_value*, and *max_value*.
        """
        self._validate_merge_compatible(other)

        merged = DDSketch(
            alpha=self.alpha,
            max_buckets=self.max_buckets,
            min_value=self.min_value,
            max_value=self.max_value,
        )
        merged._total_count = self._total_count + other._total_count

        for idx, cnt in self._buckets.items():
            merged._buckets[idx] = merged._buckets.get(idx, 0) + cnt
        for idx, cnt in other._buckets.items():
            merged._buckets[idx] = merged._buckets.get(idx, 0) + cnt

        merged._maybe_collapse()
        return merged

    def merge_into(self, other: DDSketch) -> None:
        """Hàm `merge_into` thực hiện phần xử lý liên quan đến merge into của `DDSketch`.
        
        Ghi chú gốc:
        Merge *other* into **this** sketch in-place.
        """
        self._validate_merge_compatible(other)

        self._total_count += other._total_count
        for idx, cnt in other._buckets.items():
            self._buckets[idx] = self._buckets.get(idx, 0) + cnt

        self._maybe_collapse()

    def _validate_merge_compatible(self, other: DDSketch) -> None:
        """Hàm `_validate_merge_compatible` thực hiện phần xử lý liên quan đến validate merge compatible của `DDSketch`.
        
        Ghi chú gốc:
        Raise if *other* is incompatible for merging.
        """
        if self.alpha != other.alpha:
            raise ValueError(
                f"alpha mismatch: {self.alpha} vs {other.alpha}"
            )
        if self.max_buckets != other.max_buckets:
            raise ValueError(
                f"max_buckets mismatch: {self.max_buckets} vs {other.max_buckets}"
            )
        if self.min_value != other.min_value:
            raise ValueError(
                f"min_value mismatch: {self.min_value} vs {other.min_value}"
            )
        if self.max_value != other.max_value:
            raise ValueError(
                f"max_value mismatch: {self.max_value} vs {other.max_value}"
            )

    def copy(self) -> DDSketch:
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
        cp._total_count = self._total_count
        cp._buckets = dict(self._buckets)
        return cp

    # --- serialisation ------------------------------------------------------

    def to_dict(self) -> dict:
        """Hàm `to_dict` thực hiện phần xử lý liên quan đến to dict của `DDSketch`.
        
        Ghi chú gốc:
        Serialize to a plain dict (suitable for JSON).
        """
        return {
            "alpha": self.alpha,
            "max_buckets": self.max_buckets,
            "min_value": self.min_value,
            "max_value": self.max_value,
            "total_count": self._total_count,
            "buckets": {str(k): v for k, v in self._buckets.items()},
        }

    @classmethod
    def from_dict(cls, d: dict) -> DDSketch:
        """Hàm `from_dict` thực hiện phần xử lý liên quan đến from dict của `DDSketch`.
        
        Ghi chú gốc:
        Deserialize from a dict previously produced by ``to_dict``.
        """
        sketch = cls(
            alpha=d["alpha"],
            max_buckets=d["max_buckets"],
            min_value=d["min_value"],
            max_value=d["max_value"],
        )
        sketch._total_count = d["total_count"]
        sketch._buckets = {int(k): v for k, v in d["buckets"].items()}
        return sketch

    # --- repr ---------------------------------------------------------------

    def __repr__(self) -> str:
        """Hàm `__repr__` thực hiện phần xử lý liên quan đến repr của `DDSketch`."""
        return (
            f"DDSketch(alpha={self.alpha}, buckets={len(self._buckets)}/"
            f"{self.max_buckets}, count={self._total_count})"
        )


# ---------------------------------------------------------------------------
# SlidingWindowDDSketch
# ---------------------------------------------------------------------------

class SlidingWindowDDSketch:
    """Lớp `SlidingWindowDDSketch` gom dữ liệu và hành vi liên quan đến SlidingWindowDDSketch.
    
    Ghi chú gốc:
    Quantile sketch over a sliding time window.
    
        Maintains *N* sub-sketches, each covering a fixed time granularity
        (default 1 second).  The oldest sub-sketch is dropped when it falls
        outside the window, and new sub-sketches are created automatically as
        time advances.
    
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

    # --- insert -------------------------------------------------------------

    def add(self, value: float, timestamp: Optional[float] = None) -> None:
        """Hàm `add` thực hiện phần xử lý liên quan đến add của `SlidingWindowDDSketch`.
        
        Ghi chú gốc:
        Add *value* to the sub-sketch covering *timestamp*.
        
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
        
                Sub-sketches whose start time is before
                ``current_time - window_seconds`` are dropped.
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
        """
        if not self._sketches:
            return 0.0

        sketches = list(self._sketches.values())
        merged = sketches[0].copy()
        for sk in sketches[1:]:
            merged.merge_into(sk)

        return merged.quantile(q)

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
    def from_dict(cls, d: dict) -> SlidingWindowDDSketch:
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
