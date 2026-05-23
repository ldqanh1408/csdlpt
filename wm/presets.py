"""Predefined simulation scenarios for the distributed watermark tracker.

Each function returns a fully-formed Scenario object ready to be loaded
into the auto-play engine. Use these as building blocks, or serialize
via scenario.to_dict() / Scenario.from_dict() for custom presets.
"""

from wm.config import EngineConfig
from wm.scenario import Scenario, ScenarioAction


def chaos_cascade(n_nodes: int = 4, total_events: int = 10000) -> Scenario:
    """Kill each node in sequence, then revive in reverse order.

    Demonstrates: sequential failure, DLQ buildup, cascade recovery.
    Each node stays dead for ~15% of the stream before reviving.
    """
    step = total_events // (n_nodes * 2 + 1)
    actions = []
    for i in range(n_nodes):
        kill_at = step * (2 * i + 1)
        revive_at = step * (2 * i + 2)
        actions.append(ScenarioAction("kill", at=kill_at, target=i,
                                       label=f"Kill Node {i}"))
        actions.append(ScenarioAction("revive", at=revive_at, target=i,
                                       label=f"Revive Node {i}"))
    actions.sort(key=lambda a: a.at)
    return Scenario(
        name=f"Chaos Cascade ({n_nodes} nodes)",
        description="Kill each node sequentially, then revive in reverse. "
                    f"Each node dead for ~{step * 2:,} events.",
        n_nodes=n_nodes,
        timeline=actions,
    )


def network_split(n_nodes: int = 4, total_events: int = 10000) -> Scenario:
    """Kill half the cluster simultaneously, then revive them together."""
    half = max(n_nodes // 2, 1)
    kill_at = total_events // 3
    revive_at = total_events * 2 // 3
    actions = []
    for i in range(half):
        actions.append(ScenarioAction("kill", at=kill_at, target=i,
                                       label=f"Split: Kill Node {i}"))
        actions.append(ScenarioAction("revive", at=revive_at, target=i,
                                       label=f"Heal: Revive Node {i}"))
    actions.sort(key=lambda a: a.at)
    return Scenario(
        name=f"Network Split ({half}/{n_nodes} nodes)",
        description=f"Kill {half} nodes simultaneously at {kill_at:,}, "
                    f"revive together at {revive_at:,}.",
        n_nodes=n_nodes,
        timeline=actions,
    )


def slow_recovery(total_events: int = 10000) -> Scenario:
    """Kill a single node early, leave it dead for most of the stream."""
    return Scenario(
        name="Slow Recovery",
        description="Node 0 dies at 10%, revives at 90% — "
                    "testing large DLQ backlog drain.",
        n_nodes=4,
        timeline=[
            ScenarioAction("kill", at=total_events // 10, target=0,
                           label="Node 0 dies early"),
            ScenarioAction("revive", at=total_events * 9 // 10, target=0,
                           label="Node 0 revives (drains huge DLQ)"),
        ],
    )


def full_apocalypse(total_events: int = 10000) -> Scenario:
    """Kill ALL nodes mid-stream, then revive ALL — ultimate recovery test."""
    kill_at = total_events // 2
    revive_at = total_events * 4 // 5
    n = 4
    actions = []
    for i in range(n):
        actions.append(ScenarioAction("kill", at=kill_at, target=i,
                                       label=f"Apocalypse: Kill Node {i}"))
        actions.append(ScenarioAction("revive", at=revive_at, target=i,
                                       label=f"Recovery: Revive Node {i}"))
    actions.sort(key=lambda a: a.at)
    return Scenario(
        name="Full Apocalypse",
        description=f"All {n} nodes die at {kill_at:,}, revive at {revive_at:,}.",
        n_nodes=n,
        timeline=actions,
    )


def ooo_storm(total_events: int = 10000) -> Scenario:
    """Inject bursts of out-of-order events — no node failures."""
    return Scenario(
        name="OOO Storm",
        description="Inject OOO spike at 30% and 70% of the stream — "
                    "no node failures, pure watermark stress.",
        n_nodes=4,
        timeline=[
            ScenarioAction("ooo_spike", at=total_events * 3 // 10, target=-1,
                           params={"fraction": 0.8, "duration": 500},
                           label="OOO spike 80% at 30% cursor"),
            ScenarioAction("ooo_spike", at=total_events * 7 // 10, target=-1,
                           params={"fraction": 0.9, "duration": 500},
                           label="OOO spike 90% at 70% cursor"),
        ],
    )


# Registry of all presets for UI dropdown
PRESETS: dict[str, Scenario] = {}


def _register(s: Scenario):
    PRESETS[s.name] = s
    return s


_register(chaos_cascade())
_register(network_split())
_register(slow_recovery())
_register(full_apocalypse())
_register(ooo_storm())
