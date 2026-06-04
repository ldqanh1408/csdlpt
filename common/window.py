"""
Logic tumbling window theo trục event-time.

Module tính window_start/window_end từ timestamp sự kiện và kiểm tra event có thuộc cửa sổ đang xét hay không.
"""

import math


class TumblingWindow:
    """Lớp `TumblingWindow` gom dữ liệu và hành vi liên quan đến TumblingWindow.
    
    Ghi chú gốc:
    Fixed-size tumbling windows aligned to epoch.
    """

    def __init__(self, size_s: float = 5.0):
        """Khởi tạo đối tượng của `TumblingWindow` và thiết lập trạng thái ban đầu."""
        if size_s <= 0:
            raise ValueError(f"Window size must be positive, got {size_s}")
        self.size = size_s

    def window_start(self, event_time: float) -> float:
        """Hàm `window_start` thực hiện phần xử lý liên quan đến window start của `TumblingWindow`."""
        return math.floor(event_time / self.size) * self.size

    def window_end(self, event_time: float) -> float:
        """Hàm `window_end` thực hiện phần xử lý liên quan đến window end của `TumblingWindow`."""
        return self.window_start(event_time) + self.size

    def window_id(self, partition_id: int, event_time: float) -> str:
        """Hàm `window_id` thực hiện phần xử lý liên quan đến window id của `TumblingWindow`."""
        ws = self.window_start(event_time)
        we = ws + self.size
        return f"{partition_id}_{ws}-{we}"

    def windows_between(self, start: float, end: float) -> list[float]:
        """Hàm `windows_between` thực hiện phần xử lý liên quan đến windows between của `TumblingWindow`."""
        ws = self.window_start(start)
        result = []
        while ws < end:
            result.append(ws)
            ws += self.size
        return result

    def windows_overlapping(
        self, from_time: float, to_time: float
    ) -> list[tuple[float, float]]:
        """Hàm `windows_overlapping` thực hiện phần xử lý liên quan đến windows overlapping của `TumblingWindow`.
        """
        ws = self.window_start(from_time) - self.size
        result = []
        while ws < to_time:
            we = ws + self.size
            if we > from_time:
                result.append((ws, we))
            ws += self.size
        return result
