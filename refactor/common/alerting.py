"""PagerDuty alerting integration for the stream processor.

Sends alerts via PagerDuty Events API v2 when a routing key is configured.
Falls back to stderr logging when no routing key is provided (dev/local mode).

Default alert rules:
  1. watermark_lag_critical   -- W_lag > 60s              -> critical
  2. watermark_lag_warning    -- W_lag > 30s              -> warning
  3. node_skew_warning        -- skew > 1000ms            -> warning
  4. node_skew_critical       -- skew > 5000ms            -> critical
  5. combined_status_critical -- combined_status == 3     -> critical
  6. fencing_violation        -- fencing violations increase -> high
  7. ingestor_silent          -- ingestor silent > 15s    -> high
  8. negative_lag_warning     -- negative_lag_rate 1-5%   -> warning
  9. negative_lag_critical    -- negative_lag_rate > 5%   -> critical
 10. all_workers_idle         -- all workers idle         -> high
 11. replay_mode_extended     -- replay_mode > 2 for > 5m -> warning
 12. correction_latency_high  -- avg correction lat > 1h  -> warning
 13. dlq_backlog_critical     -- dlq_backlog > 50000      -> critical
 14. sketch_drift_high        -- estimator_drift > 0.5    -> warning
 15. extreme_lag_detected     -- extreme_lag_count > 0    -> warning
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.request
import urllib.error
from typing import Optional

from refactor.common.monitoring import AlertRule, MonitoringManager


# ---------------------------------------------------------------------------
# PagerDuty Events API v2 helper
# ---------------------------------------------------------------------------

PAGERDUTY_EVENTS_URL = "https://events.pagerduty.com/v2/enqueue"


def _send_pagerduty_event(
    routing_key: str,
    summary: str,
    severity: str,
    source: str = "csdlpt-refactor",
    dedup_key: str = "",
    custom_details: dict | None = None,
) -> bool:
    """Send an alert event to PagerDuty via Events API v2.

    Returns True on success (HTTP 202), False on failure.
    """
    severity_map = {
        "critical": "critical",
        "warning": "warning",
        "high": "error",
    }
    payload = {
        "routing_key": routing_key,
        "event_action": "trigger",
        "payload": {
            "summary": summary,
            "severity": severity_map.get(severity, "warning"),
            "source": source,
            "custom_details": custom_details or {},
        },
    }
    if dedup_key:
        payload["dedup_key"] = dedup_key

    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            PAGERDUTY_EVENTS_URL,
            data=data,
            headers={"Content-Type": "application/json"},
        )
        resp = urllib.request.urlopen(req, timeout=5)
        return resp.status == 202
    except Exception:
        return False


# ---------------------------------------------------------------------------
# AlertManager
# ---------------------------------------------------------------------------

class AlertManager:
    """Manages alert rules and sends to PagerDuty (or stdout fallback).

    Parameters
    ----------
    pagerduty_routing_key : str | None
        PagerDuty Events API v2 routing key.  If None, alerts are logged to
        stderr instead of being sent to PagerDuty.
    """

    def __init__(self, pagerduty_routing_key: str | None = None):
        if pagerduty_routing_key is None:
            pagerduty_routing_key = os.environ.get("PAGERDUTY_ROUTING_KEY", "").strip() or None
        self.routing_key = pagerduty_routing_key
        self.rules: list[AlertRule] = []
        self._fired: dict[str, float] = {}  # rule_name -> last_fired_ts
        self._cooldown_s: float = 60.0  # do not re-fire same rule within 60s
        self._setup_default_rules()

    # ------------------------------------------------------------------
    # Default rules
    # ------------------------------------------------------------------

    def _setup_default_rules(self) -> None:
        """Register the six default alert rules."""

        # 1. watermark_lag_critical: W_lag > 60s -> critical
        self.rules.append(
            AlertRule(
                name="watermark_lag_critical",
                description="Watermark lag exceeds 60 seconds",
                severity="critical",
                condition="W_lag > 60s",
                evaluate=lambda m: m.get("watermark_lag_s", 0) > 60.0,
            )
        )

        # 2. watermark_lag_warning: W_lag > 30s -> warning
        self.rules.append(
            AlertRule(
                name="watermark_lag_warning",
                description="Watermark lag exceeds 30 seconds",
                severity="warning",
                condition="W_lag > 30s",
                evaluate=lambda m: 30.0 < m.get("watermark_lag_s", 0) <= 60.0,
            )
        )

        # 3. node_skew_warning: 1000ms < skew <= 5000ms -> warning
        self.rules.append(
            AlertRule(
                name="node_skew_warning",
                description="Inter-node watermark skew exceeds 1000ms",
                severity="warning",
                condition="1000ms < skew <= 5000ms",
                evaluate=lambda m: 1000.0 < m.get("node_skew_ms", 0) <= 5000.0,
            )
        )

        # 4. node_skew_critical: skew > 5000ms -> critical
        self.rules.append(
            AlertRule(
                name="node_skew_critical",
                description="Inter-node watermark skew exceeds 5000ms",
                severity="critical",
                condition="skew > 5000ms",
                evaluate=lambda m: m.get("node_skew_ms", 0) > 5000.0,
            )
        )

        # 5. combined_status_critical: combined_status == Critical -> critical
        self.rules.append(
            AlertRule(
                name="combined_status_critical",
                description="Combined health status is Critical",
                severity="critical",
                condition="combined_status == Critical",
                evaluate=lambda m: m.get("combined_status", 0) >= 3,
            )
        )

        # 6. fencing_violation: fencing violations detected -> high
        self.rules.append(
            AlertRule(
                name="fencing_violation",
                description="Fencing token violations detected",
                severity="high",
                condition="fencing_violations increase",
                evaluate=lambda m: m.get("fencing_violations", 0) > 0,
            )
        )

        # 7. ingestor_silent: ingestor silent -> high
        self.rules.append(
            AlertRule(
                name="ingestor_silent",
                description="One or more ingestors are silent (>15s no heartbeat)",
                severity="high",
                condition="ingestor_silent > 0",
                evaluate=lambda m: m.get("ingestor_silent", 0) > 0,
            )
        )

        # 8. negative_lag_warning: 0.01 < negative_lag_rate <= 0.05 -> warning (Tier 3)
        self.rules.append(
            AlertRule(
                name="negative_lag_warning",
                description="Negative lag rate between 1% and 5% threshold (Tier 3)",
                severity="warning",
                condition="0.01 < negative_lag_rate <= 0.05",
                evaluate=lambda m: 0.01 < m.get("negative_lag_rate", 0.0) <= 0.05,
            )
        )

        # 9. negative_lag_critical: negative_lag_rate > 0.05 -> critical (spec §7.2 Tier 4)
        self.rules.append(
            AlertRule(
                name="negative_lag_critical",
                description="Negative lag rate exceeds 5% threshold (Tier 4)",
                severity="critical",
                condition="negative_lag_rate > 0.05",
                evaluate=lambda m: m.get("negative_lag_rate", 0.0) > 0.05,
            )
        )

        # 10. all_workers_idle: all workers idle -> high
        self.rules.append(
            AlertRule(
                name="all_workers_idle",
                description="All worker partitions are idle",
                severity="high",
                condition="all_workers_idle > 0",
                evaluate=lambda m: m.get("all_workers_idle", 0) > 0,
            )
        )

        # 11. replay_mode_extended: replay_mode > 2 workers for > 5min -> warning
        self.rules.append(
            AlertRule(
                name="replay_mode_extended",
                description="Replay mode active on > 2 workers for more than 5 minutes",
                severity="warning",
                condition="replay_mode_extended > 0",
                evaluate=lambda m: m.get("replay_mode_extended", 0) > 0,
            )
        )

        # 12. correction_latency_high: avg correction latency > 1h -> warning
        self.rules.append(
            AlertRule(
                name="correction_latency_high",
                description="Average correction latency exceeds 1 hour",
                severity="warning",
                condition="correction_latency > 3600s",
                evaluate=lambda m: m.get("correction_latency_s", 0.0) > 3600.0,
            )
        )

        # 13. dlq_backlog_critical: dlq_backlog > 50000 -> critical
        self.rules.append(
            AlertRule(
                name="dlq_backlog_critical",
                description="DLQ backlog exceeds 50000 entries",
                severity="critical",
                condition="dlq_backlog > 50000",
                evaluate=lambda m: m.get("dlq_backlog", 0) > 50000,
            )
        )

        # 14. sketch_drift_high: estimator_drift > 0.5 -> warning
        self.rules.append(
            AlertRule(
                name="sketch_drift_high",
                description="DDSketch estimator drift exceeds 0.5 threshold",
                severity="warning",
                condition="estimator_drift > 0.5",
                evaluate=lambda m: m.get("estimator_drift", 0.0) > 0.5,
            )
        )

        # 15. extreme_lag_detected: extreme_lag_count > 0 -> warning (spec §5.5)
        self.rules.append(
            AlertRule(
                name="extreme_lag_detected",
                description="Extreme lag events detected (lag > max_lag_accepted)",
                severity="warning",
                condition="extreme_lag_count > 0",
                evaluate=lambda m: m.get("extreme_lag_count", 0) > 0,
            )
        )

    # ------------------------------------------------------------------
    # Evaluation
    # ------------------------------------------------------------------

    def evaluate(self, metrics: dict) -> list[dict]:
        """Evaluate all rules against current metrics snapshot.

        Returns a list of triggered alert dicts, each with keys:
          name, description, severity, condition, timestamp.
        """
        now = time.time()
        triggered: list[dict] = []

        for rule in self.rules:
            try:
                if rule.evaluate(metrics):
                    # Cooldown check
                    last = self._fired.get(rule.name, 0)
                    if now - last < self._cooldown_s:
                        continue
                    self._fired[rule.name] = now
                    triggered.append(
                        {
                            "name": rule.name,
                            "description": rule.description,
                            "severity": rule.severity,
                            "condition": rule.condition,
                            "timestamp": now,
                            "metrics_snapshot": {
                                k: metrics.get(k)
                                for k in (
                                    "watermark_lag_s",
                                    "node_skew_ms",
                                    "combined_status",
                                    "fencing_violations",
                                    "ingestor_silent",
                                    "ingestor_stuck",
                                    "dlq_backlog",
                                    "estimator_drift",
                                    "negative_lag_rate",
                                    "extreme_lag_count",
                                )
                            },
                        }
                    )
            except Exception:
                # One misbehaving rule must not block others
                pass

        return triggered

    def send_alert(self, alert: dict) -> None:
        """Send alert to PagerDuty via Events API v2, or log to stderr if no key.

        Parameters
        ----------
        alert : dict
            Alert dict as returned by evaluate().
        """
        if self.routing_key:
            success = _send_pagerduty_event(
                routing_key=self.routing_key,
                summary=f"[{alert['severity'].upper()}] {alert['description']}",
                severity=alert["severity"],
                source="csdlpt-refactor",
                dedup_key=alert.get("name", ""),
                custom_details={
                    "rule": alert["name"],
                    "condition": alert["condition"],
                    "metrics": alert.get("metrics_snapshot", {}),
                },
            )
            if not success:
                print(
                    f"[alerting] PagerDuty delivery failed for rule={alert['name']}",
                    file=sys.stderr,
                )
        else:
            # No routing key: log to stderr
            print(
                f"[ALERT:{alert['severity'].upper()}] {alert['name']} -- "
                f"{alert['description']} (condition: {alert['condition']})",
                file=sys.stderr,
            )
            details = alert.get("metrics_snapshot", {})
            if details:
                print(f"         metrics: {json.dumps(details)}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Background evaluation loop helper
# ---------------------------------------------------------------------------

def alert_evaluation_loop(
    alert_mgr: AlertManager,
    mon_mgr: MonitoringManager,
    interval_s: float = 10.0,
    stop_event=None,
) -> None:
    """Run periodic alert evaluation in a background thread.

    Parameters
    ----------
    alert_mgr : AlertManager
        The alert manager with registered rules.
    mon_mgr : MonitoringManager
        The monitoring manager providing metrics snapshots.
    interval_s : float
        Seconds between evaluations.
    stop_event : threading.Event | None
        When set, the loop exits.
    """
    while stop_event is None or not stop_event.is_set():
        try:
            snapshot = mon_mgr.snapshot()
            triggered = alert_mgr.evaluate(snapshot)
            for alert in triggered:
                alert_mgr.send_alert(alert)
        except Exception:
            pass
        time.sleep(interval_s)
