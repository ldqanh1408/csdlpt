"""Shared configuration for Watermark Engine and simulation scenarios."""

from dataclasses import dataclass


@dataclass
class EngineConfig:
    """Immutable engine configuration used across all modules.

    Replaces the 11 hardcoded copies of (window_size_s=10.0,
    allowed_lateness_s=2.0, checkpoint_interval=200, max_queue=10_000_000)
    that were scattered across app.py, sweep.py, demos.py, and partition.py.
    """

    window_size_s: float = 10.0
    allowed_lateness_s: float = 2.0
    checkpoint_interval: int = 200
    max_queue: int = 10_000_000
    dedup_mode: str = "set"  # "set" | future: "bloom"

    def to_engine_kwargs(self) -> dict:
        return {
            "window_size_s": self.window_size_s,
            "allowed_lateness_s": self.allowed_lateness_s,
            "checkpoint_interval": self.checkpoint_interval,
            "max_queue": self.max_queue,
        }


DEFAULT_CONFIG = EngineConfig()
