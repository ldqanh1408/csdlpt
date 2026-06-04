"""Package DDSketch nội bộ dùng cho ước lượng phân vị lateness.

Cung cấp hai class chính:

    DDSketch
        Quantile sketch dùng bộ nhớ hữu hạn, có sai số tương đối cấu hình bằng
        alpha. Hai sketch cùng alpha có thể merge bằng cách cộng bucket counter
        mà không làm tăng sai số.

    SlidingWindowDDSketch
        Bọc nhiều DDSketch con theo từng lát thời gian để ước lượng quantile
        trong một sliding window.

Ghi chú triển khai:
  - API public nằm trong `compat.py` để giữ tương thích với code cũ.
  - Cài đặt thuần Python nằm trong `sketch.py`.
  - Nhánh heuristic dùng package này để tính p50/p95/p99 của lateness.

Ví dụ:
    from local_ddsketch import DDSketch

    sketch = DDSketch(alpha=0.01)
    for lag in latencies:
        sketch.add(lag)
    print(sketch.quantile(0.50))
    print(sketch.quantile(0.99))
"""

from .compat import DDSketch, SlidingWindowDDSketch

__all__ = ["DDSketch", "SlidingWindowDDSketch"]
