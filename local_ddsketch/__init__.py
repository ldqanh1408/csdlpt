"""
ddsketch — DDSketch quantile estimation for distributed stream processing.

Provides two classes:

    DDSketch
        Fixed-memory quantile sketch with configurable relative error
        guarantee.  Fully mergeable — two sketches with the same alpha
        can be combined by adding bucket counters with no accuracy loss.

    SlidingWindowDDSketch
        Wraps a deque of DDSketch sub-sketches (one per second) to
        estimate quantiles over a sliding time window.

Implementation note
-------------------
The classes exported here are compatibility wrappers around the official
``ddsketch`` library (v3.0.1).  They present the same legacy API so that
existing callers require no changes.  See ``compat.py`` for the wrappers
and ``sketch.py`` for the original custom implementation (deprecated).

Typical usage::

    from refactor.ddsketch import DDSketch

    sketch = DDSketch(alpha=0.01)
    for lag in latencies:
        sketch.add(lag)
    print(sketch.quantile(0.50))   # p50
    print(sketch.quantile(0.99))   # p99
"""

from .compat import DDSketch, SlidingWindowDDSketch

__all__ = ["DDSketch", "SlidingWindowDDSketch"]
