"""Tumbling window logic on the event-time axis."""

import math


class TumblingWindow:
    """Fixed-size tumbling windows aligned to epoch."""

    def __init__(self, size_s: float = 5.0):
        if size_s <= 0:
            raise ValueError(f"Window size must be positive, got {size_s}")
        self.size = size_s

    def window_start(self, event_time: float) -> float:
        return math.floor(event_time / self.size) * self.size

    def window_end(self, event_time: float) -> float:
        return self.window_start(event_time) + self.size

    def window_id(self, partition_id: int, event_time: float) -> str:
        ws = self.window_start(event_time)
        we = ws + self.size
        return f"{partition_id}_{ws}-{we}"

    def windows_between(self, start: float, end: float) -> list[float]:
        ws = self.window_start(start)
        result = []
        while ws < end:
            result.append(ws)
            ws += self.size
        return result

    def windows_overlapping(
        self, from_time: float, to_time: float
    ) -> list[tuple[float, float]]:
        ws = self.window_start(from_time) - self.size
        result = []
        while ws < to_time:
            we = ws + self.size
            if we > from_time:
                result.append((ws, we))
            ws += self.size
        return result
