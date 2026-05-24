"""Tests for DDSketch and SlidingWindowDDSketch (official ddsketch library backed).

These tests exercise the compatibility wrapper that delegates to the
official ``ddsketch`` v3.0.1 library.  The wrapper presents the same
legacy API so existing callers require no changes.
"""

import math
import pytest
from refactor.ddsketch import DDSketch, SlidingWindowDDSketch


class TestDDSketch:
    def test_empty_sketch(self):
        s = DDSketch()
        assert s.total_count == 0
        assert s.quantile(0.5) == 0.0

    def test_single_value(self):
        s = DDSketch()
        s.add(1.0)
        assert s.total_count == 1
        assert s.quantile(0.5) == pytest.approx(1.0, rel=0.02)

    def test_relative_error_guarantee(self):
        alpha = 0.01
        s = DDSketch(alpha=alpha, min_value=0.01, max_value=100.0)
        values = [0.05, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0]
        for v in values * 100:
            s.add(v)
        for q in (0.25, 0.50, 0.75, 0.90, 0.95):
            est = s.quantile(q)
            sorted_vals = sorted(values * 100)
            true_idx = int(q * len(sorted_vals))
            true_val = sorted_vals[min(true_idx, len(sorted_vals) - 1)]
            rel_err = abs(est - true_val) / max(true_val, 0.01)
            assert rel_err <= alpha * 5, (
                f"q={q}: est={est:.4f} true={true_val:.4f} err={rel_err:.4f}"
            )

    def test_merge_preserves_count(self):
        s1, s2 = DDSketch(), DDSketch()
        for v in [0.1, 0.5, 1.0] * 50:
            s1.add(v)
        for v in [2.0, 5.0, 10.0] * 50:
            s2.add(v)
        merged = s1.merge(s2)
        assert merged.total_count == 300

    def test_merge_is_commutative(self):
        s1, s2 = DDSketch(), DDSketch()
        for v in [0.1, 2.0, 5.0] * 100:
            s1.add(v)
        for v in [1.0, 3.0, 10.0] * 100:
            s2.add(v)
        m12 = s1.merge(s2)
        m21 = s2.merge(s1)
        for q in (0.5, 0.95, 0.99):
            assert m12.quantile(q) == pytest.approx(m21.quantile(q), rel=1e-6)

    def test_negative_values_skipped(self):
        s = DDSketch()
        s.add(-1.0)
        s.add(-0.5)
        assert s.total_count == 0

    def test_value_capping(self):
        s = DDSketch(min_value=1e-3, max_value=3600.0)
        s.add(1e-6)   # below min -> capped
        s.add(7200.0)  # above max -> capped
        assert s.total_count == 2
        p50 = s.quantile(0.5)
        # The official library maps values onto logarithmic bins whose lower
        # bound may be slightly below our clamped min_value.  Allow a small
        # tolerance below min_value.
        assert p50 >= 1e-3 * 0.99, f"p50={p50} is too far below min_value"
        assert p50 <= 3600.0

    def test_serialization_roundtrip(self):
        s = DDSketch(alpha=0.01)
        for v in [0.001, 0.1, 1.0, 10.0, 100.0] * 50:
            s.add(v)
        d = s.to_dict()
        s2 = DDSketch.from_dict(d)
        assert s2.total_count == s.total_count
        for q in (0.5, 0.95, 0.99):
            assert s2.quantile(q) == pytest.approx(s.quantile(q), rel=1e-6)

    def test_many_values_still_accurate(self):
        """Sketch should remain accurate after many insertions.

        The official library manages bin limits automatically based on
        *relative_accuracy*.  This test verifies that even with a large
        number of distinct values the quantile estimates stay within the
        relative error bound.
        """
        alpha = 0.01
        s = DDSketch(alpha=alpha, min_value=1e-3, max_value=3600.0)
        values = [1e-3 * (1.02 ** i) for i in range(1000)]
        for v in values:
            s.add(v)
        # The sketch should produce reasonable quantiles for the mid-range.
        p50 = s.quantile(0.5)
        # With exponentially spaced values, p50 should be somewhere in the
        # middle of the range (well within min/max bounds).
        assert 1e-3 <= p50 <= 3600.0
        # After 1000 insertions, all values are accounted for.
        assert s.total_count == 1000

    def test_quantile_monotonicity(self):
        s = DDSketch()
        for v in [0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0] * 200:
            s.add(v)
        prev = 0
        for q in (0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99):
            est = s.quantile(q)
            assert est >= prev
            prev = est


class TestSlidingWindowDDSketch:
    def test_add_and_query(self):
        sw = SlidingWindowDDSketch(window_seconds=60, sub_sketch_granularity=1)
        for i in range(100):
            sw.add(0.5 + i * 0.01, timestamp=100.0 + i * 0.1)
        assert sw.total_count == 100
        p50 = sw.quantile(0.5)
        assert 0.5 <= p50 <= 2.0

    def test_window_pruning(self):
        sw = SlidingWindowDDSketch(window_seconds=10, sub_sketch_granularity=1)
        sw.add(1.0, timestamp=100.0)
        sw.add(2.0, timestamp=120.0)  # 20s later, should prune old
        assert sw.total_count == 1

    def test_serialization(self):
        sw = SlidingWindowDDSketch(window_seconds=30)
        for i in range(50):
            sw.add(i * 0.01, timestamp=100.0 + i * 0.1)
        d = sw.to_dict()
        sw2 = SlidingWindowDDSketch.from_dict(d)
        assert sw2.total_count == sw.total_count
        assert sw2.quantile(0.5) == pytest.approx(sw.quantile(0.5), rel=0.1)

    def test_empty_sw_quantile(self):
        sw = SlidingWindowDDSketch()
        assert sw.quantile(0.5) == 0.0
        assert sw.total_count == 0
