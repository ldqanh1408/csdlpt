"""Negative Lag Handler - detects and handles clock skew (lag < 0).

Tier actions:
  < 0.1%:  Normal jitter, skip
  0.1-1%:  Clock skew light, skip + log warning
  1-5%:    Clock skew clear, alert + recalibration
  > 5%:    Clock chaos, alert critical + degrade to BOO fallback
"""

import time
from collections import deque
from enum import Enum


class LagTier(Enum):
    NORMAL = 0
    WARNING = 1
    ALERT = 2
    CRITICAL = 3


class NegativeLagHandler:
    def __init__(self, window_seconds: float = 60.0):
        self.window_seconds = window_seconds
        self._observations: deque[tuple[float, bool]] = deque()
        self._neg_values: list[float] = []
        self.last_tier: LagTier = LagTier.NORMAL
        self.degraded_to_boo: bool = False
        self._degrade_time: float = 0.0

    def observe(self, lag: float) -> None:
        now = time.time()
        self._observations.append((now, lag < 0))
        if lag < 0:
            self._neg_values.append(lag)
            if len(self._neg_values) > 1000:
                self._neg_values.pop(0)
        self._prune(now)

    def _prune(self, now: float) -> None:
        cutoff = now - self.window_seconds
        while self._observations and self._observations[0][0] < cutoff:
            self._observations.popleft()

    @property
    def rate(self) -> float:
        total = len(self._observations)
        if total == 0:
            return 0.0
        neg = sum(1 for _, is_neg in self._observations if is_neg)
        return neg / total

    def evaluate(self) -> LagTier:
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
        if not self._neg_values:
            return 0.0
        sorted_vals = sorted(self._neg_values)
        mid = len(sorted_vals) // 2
        return sorted_vals[mid]

    def adjust_lag(self, lag: float) -> float:
        if self.last_tier in (LagTier.ALERT, LagTier.CRITICAL) and lag < 0:
            return lag + abs(self.median_neg_lag())
        return lag

    def should_degrade_to_boo(self) -> bool:
        return self.degraded_to_boo

    def check_recovery(self, stable_minutes: float = 5.0) -> bool:
        if not self.degraded_to_boo:
            return False
        if self.rate < 0.01 and (time.time() - self._degrade_time) > stable_minutes * 60:
            self.degraded_to_boo = False
            return True
        return False

    def reset(self) -> None:
        self._observations.clear()
        self._neg_values.clear()
        self.degraded_to_boo = False
        self.last_tier = LagTier.NORMAL

    def status(self) -> dict:
        return {
            "negative_lag_rate": round(self.rate, 5),
            "total_in_window": len(self._observations),
            "tier": self.last_tier.name,
            "degraded_to_boo": self.degraded_to_boo,
            "median_neg_lag_s": round(self.median_neg_lag(), 6),
        }
