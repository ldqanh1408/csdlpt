"""
Phát hiện và xử lý độ trễ âm do lệch đồng hồ hoặc dữ liệu bất thường.

Module phân loại lag âm theo mức độ, thống kê tần suất và cung cấp tín hiệu để engine không để clock skew làm méo DDSketch hoặc watermark.
"""

import time
from collections import deque
from enum import Enum


class LagTier(Enum):
    """Lớp `LagTier` định nghĩa các trạng thái/hằng số dùng trong luồng xử lý."""
    NORMAL = 0
    WARNING = 1
    ALERT = 2
    CRITICAL = 3


class NegativeLagHandler:
    """Lớp `NegativeLagHandler` xử lý request hoặc tình huống runtime liên quan."""
    def __init__(self, window_seconds: float = 60.0):
        """Khởi tạo đối tượng của `NegativeLagHandler` và thiết lập trạng thái ban đầu."""
        self.window_seconds = window_seconds
        self._observations: deque[tuple[float, bool]] = deque()
        self._neg_values: list[float] = []
        self.last_tier: LagTier = LagTier.NORMAL
        self.degraded_to_boo: bool = False
        self._degrade_time: float = 0.0

    def observe(self, lag: float) -> None:
        """Hàm `observe` thực hiện phần xử lý liên quan đến observe của `NegativeLagHandler`."""
        now = time.time()
        self._observations.append((now, lag < 0))
        if lag < 0:
            self._neg_values.append(lag)
            if len(self._neg_values) > 1000:
                self._neg_values.pop(0)
        self._prune(now)

    def _prune(self, now: float) -> None:
        """Hàm `_prune` thực hiện phần xử lý liên quan đến prune của `NegativeLagHandler`."""
        cutoff = now - self.window_seconds
        while self._observations and self._observations[0][0] < cutoff:
            self._observations.popleft()

    @property
    def rate(self) -> float:
        """Hàm `rate` thực hiện phần xử lý liên quan đến rate của `NegativeLagHandler`."""
        total = len(self._observations)
        if total == 0:
            return 0.0
        neg = sum(1 for _, is_neg in list(self._observations) if is_neg)
        return neg / total

    def evaluate(self) -> LagTier:
        """Hàm `evaluate` thực hiện phần xử lý liên quan đến evaluate của `NegativeLagHandler`."""
        r = self.rate
        if r < 0.001:
            self.last_tier = LagTier.NORMAL
        elif r < 0.01:
            self.last_tier = LagTier.WARNING
        elif r < 0.05:
            self.last_tier = LagTier.ALERT
        else:
            self.last_tier = LagTier.CRITICAL
            if not self.degraded_to_boo:
                self.degraded_to_boo = True
                self._degrade_time = time.time()
        return self.last_tier

    def median_neg_lag(self) -> float:
        """Hàm `median_neg_lag` thực hiện phần xử lý liên quan đến median neg lag của `NegativeLagHandler`."""
        if not self._neg_values:
            return 0.0
        sorted_vals = sorted(self._neg_values)
        mid = len(sorted_vals) // 2
        return sorted_vals[mid]

    def adjust_lag(self, lag: float) -> float:
        """Hàm `adjust_lag` thực hiện phần xử lý liên quan đến adjust lag của `NegativeLagHandler`."""
        if self.last_tier in (LagTier.ALERT, LagTier.CRITICAL) and lag < 0:
            return lag + abs(self.median_neg_lag())
        return lag

    def should_degrade_to_boo(self) -> bool:
        """Quyết định có nên thực hiện `degrade to boo` theo trạng thái hiện tại hay không."""
        return self.degraded_to_boo

    def check_recovery(self, stable_minutes: float = 5.0) -> bool:
        """Kiểm tra điều kiện `check recovery` và trả về kết quả đánh giá."""
        if not self.degraded_to_boo:
            return False
        if self.rate < 0.01 and (time.time() - self._degrade_time) > stable_minutes * 60:
            self.degraded_to_boo = False
            return True
        return False

    def reset(self) -> None:
        """Hàm `reset` thực hiện phần xử lý liên quan đến reset của `NegativeLagHandler`."""
        self._observations.clear()
        self._neg_values.clear()
        self.degraded_to_boo = False
        self.last_tier = LagTier.NORMAL

    def status(self) -> dict:
        """Hàm `status` thực hiện phần xử lý liên quan đến status của `NegativeLagHandler`."""
        return {
            "negative_lag_rate": round(self.rate, 5),
            "total_in_window": len(self._observations),
            "tier": self.last_tier.name,
            "degraded_to_boo": self.degraded_to_boo,
            "median_neg_lag_s": round(self.median_neg_lag(), 6),
        }
