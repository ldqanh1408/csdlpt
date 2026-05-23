"""wm — Distributed Watermark Tracker package.

Re-export public API at package level for clean `from wm import ...` usage.
"""
from wm.engine import WatermarkEngine, WindowState
from wm.data.synthetic import generate_logs
from wm.data.nasa import load_nasa, load_nasa_csv
from wm.config import EngineConfig, DEFAULT_CONFIG
from wm.scenario import Scenario, ScenarioAction
from wm.actions import kill_node, revive_node, kill_all, revive_all
from wm.presets import PRESETS

__all__ = [
    "WatermarkEngine", "WindowState",
    "generate_logs", "load_nasa", "load_nasa_csv",
    "EngineConfig", "DEFAULT_CONFIG",
    "Scenario", "ScenarioAction",
    "kill_node", "revive_node", "kill_all", "revive_all",
    "PRESETS",
]
