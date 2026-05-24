"""wm — Distributed Watermark Tracker package.

Re-export public API at package level for clean `from wm import ...` usage.
Lazy imports: data/synthetic/nasa are only loaded on first access so the engine
can be imported without pandas in containerised deployments.
"""
from wm.engine import WatermarkEngine, WindowState
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


def __getattr__(name):
    if name == "generate_logs":
        from wm.data.synthetic import generate_logs as _fn
        return _fn
    if name == "load_nasa":
        from wm.data.nasa import load_nasa as _fn
        return _fn
    if name == "load_nasa_csv":
        from wm.data.nasa import load_nasa_csv as _fn
        return _fn
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
