"""wm — Distributed Watermark Tracker package.

Re-export public API ở mức package để code gọi `from wm import ...` cho gọn.
"""
from wm.engine import WatermarkEngine, WindowState
from wm.data.synthetic import generate_logs
from wm.data.nasa import load_nasa, load_nasa_csv

__all__ = [
    "WatermarkEngine", "WindowState",
    "generate_logs", "load_nasa", "load_nasa_csv",
]
