"""Scenario DSL for composable distributed simulation timelines.

Replaces the ad-hoc list[(action, node_id, at_cursor)] tuple schedule
in app.py with typed, serializable, composable scenario definitions.
"""

from dataclasses import dataclass, field

from wm.config import DEFAULT_CONFIG, EngineConfig


@dataclass
class ScenarioAction:
    """A single action in a scenario timeline.

    Attributes:
        type: One of "kill", "revive", "ooo_spike", "load_burst", "delay_inject".
        at: Cursor position (absolute event index) when this action triggers.
        target: Node ID to act on, or -1 for "all nodes".
        params: Type-specific kwargs (e.g. {"fraction": 0.5, "duration": 1000}).
        label: Human-readable label shown in the UI log.
    """

    type: str
    at: int
    target: int = -1
    params: dict = field(default_factory=dict)
    label: str = ""

    def __post_init__(self):
        if not self.label:
            self.label = f"{self.type} node={self.target} at cursor={self.at:,}"


@dataclass
class Scenario:
    """A complete simulation scenario — a named timeline of actions.

    Usage:
        s = Scenario(
            name="Chaos Cascade",
            description="Kill nodes one-by-one, revive in reverse order",
            n_nodes=4,
            timeline=[
                ScenarioAction("kill", at=1000, target=0),
                ScenarioAction("kill", at=2000, target=1),
                ScenarioAction("revive", at=3000, target=1),
                ScenarioAction("revive", at=4000, target=0),
            ],
        )
        d = s.to_dict()           # serialize for storage/sharing
        s2 = Scenario.from_dict(d)  # deserialize back
    """

    name: str
    description: str = ""
    n_nodes: int = 4
    config: EngineConfig = field(default_factory=EngineConfig)
    timeline: list[ScenarioAction] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Serialize to JSON-compatible dict."""
        return {
            "name": self.name,
            "description": self.description,
            "n_nodes": self.n_nodes,
            "config": {
                "window_size_s": self.config.window_size_s,
                "allowed_lateness_s": self.config.allowed_lateness_s,
                "checkpoint_interval": self.config.checkpoint_interval,
                "max_queue": self.config.max_queue,
            },
            "timeline": [
                {
                    "type": a.type,
                    "at": a.at,
                    "target": a.target,
                    "params": a.params,
                    "label": a.label,
                }
                for a in self.timeline
            ],
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Scenario":
        """Deserialize from JSON-compatible dict."""
        cfg = d.get("config", {})
        config = EngineConfig(
            window_size_s=cfg.get("window_size_s", 10.0),
            allowed_lateness_s=cfg.get("allowed_lateness_s", 2.0),
            checkpoint_interval=cfg.get("checkpoint_interval", 200),
            max_queue=cfg.get("max_queue", 10_000_000),
        )
        timeline = [
            ScenarioAction(
                type=a["type"],
                at=a["at"],
                target=a.get("target", -1),
                params=a.get("params", {}),
                label=a.get("label", ""),
            )
            for a in d.get("timeline", [])
        ]
        return cls(
            name=d.get("name", ""),
            description=d.get("description", ""),
            n_nodes=d.get("n_nodes", 4),
            config=config,
            timeline=timeline,
        )

    def to_schedule(self) -> list[tuple]:
        """Convert to legacy schedule format for backward compat with app.py.

        Returns list[(action_type, node_id, at_cursor)] for use with
        the existing auto-play schedule executor.
        """
        return [(a.type, a.target, a.at) for a in self.timeline]
