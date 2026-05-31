"""Hybrid router - combines Strict (0% loss) and Heuristic (low latency) paths.

Routes events based on priority:
  - CRITICAL -> Strict path (punctuation-based, 0% data loss)
  - STANDARD -> Heuristic path (DDSketch-based, <=1% bounded loss)
"""

from hybrid.router import HybridRouter, EventPriority, RouterConfig

__all__ = ["HybridRouter", "EventPriority", "RouterConfig"]
