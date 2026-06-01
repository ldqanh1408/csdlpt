"""Refactor - Stateful Stream Processing: Strict + Heuristic Watermark System.

Implements the full 3-document design specification:
  - Strict Watermark  (0% loss, Coordinator HA, Tiered Storage)
  - Heuristic Watermark + DDSketch (bounded loss, low latency, DLQ correction)
  - Deployment / Operations guide

Scaled down to single-machine Python for academic project use.
"""

__all__ = ["common", "ddsketch", "strict", "heuristic", "tests"]
