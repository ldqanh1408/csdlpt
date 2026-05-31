"""Ingestor Health Monitor — per Strict Watermark spec Section 10.

Two-tier heartbeat: Ingestor → Coordinator every 5s with T_commit and clock info.
Alert conditions: SILENT (>15s no heartbeat), STUCK (punctuation stuck >5s),
CLOCK_SKEW (non-monotonic T_commit).
Computes W_meta_global = min(all ingestor T_commit).
"""

import time
from dataclasses import dataclass, field
from enum import Enum


class IngestorStatus(Enum):
    ACTIVE = "active"
    SILENT = "silent"
    STUCK = "stuck"
    CLOCK_SKEW_OK = "clock_skew_ok"
    CLOCK_SKEW_INFO = "clock_skew_info"
    CLOCK_SKEW_WARNING = "clock_skew_warning"
    CLOCK_SKEW_CRITICAL = "clock_skew_critical"


# 4-tier clock skew thresholds (Spec §10.2)
CLOCK_SKEW_OK_MS = 100.0
CLOCK_SKEW_INFO_MS = 500.0
CLOCK_SKEW_WARNING_MS = 2000.0


@dataclass
class HealthRecord:
    ingestor_id: str
    last_heartbeat: float = 0.0
    last_T_commit: float = 0.0
    clock_skew_ms: float = 0.0
    status: IngestorStatus = IngestorStatus.ACTIVE
    partitions_assigned: list[int] = field(default_factory=list)
    last_log_offset: dict[int, int] = field(default_factory=dict)
    ingestor_clock: float = 0.0
    network_rtt_ms: float = 0.0
    internal_queue_depth: int = 0

    def diagnose_clock_skew(self) -> IngestorStatus:
        """4-tier clock skew diagnosis per Spec §10.2."""
        ms = self.clock_skew_ms
        if ms < CLOCK_SKEW_OK_MS:
            return IngestorStatus.CLOCK_SKEW_OK
        elif ms < CLOCK_SKEW_INFO_MS:
            return IngestorStatus.CLOCK_SKEW_INFO
        elif ms < CLOCK_SKEW_WARNING_MS:
            return IngestorStatus.CLOCK_SKEW_WARNING
        else:
            return IngestorStatus.CLOCK_SKEW_CRITICAL


class IngestorHealthMonitor:
    """Tracks health of all ingestors, computes W_meta_global, raises alerts."""

    def __init__(
        self,
        silent_timeout_s: float = 15.0,
        stuck_timeout_s: float = 5.0,
        clock_skew_critical_ms: float = 2000.0,
        w_meta_deviation_s: float = 10.0,
    ):
        self.silent_timeout_s = silent_timeout_s
        self.stuck_timeout_s = stuck_timeout_s
        self.clock_skew_critical_ms = clock_skew_critical_ms
        self.w_meta_deviation_s = w_meta_deviation_s

        self.ingestors: dict[str, HealthRecord] = {}
        self.W_meta_global: float = float("-inf")
        self.alerts: list[dict] = []

    def receive_heartbeat(
        self,
        ingestor_id: str,
        partitions: list[int] = None,
        last_T_commit: float = 0.0,
        ingestor_clock: float = None,
        offsets: dict[int, int] = None,
        network_rtt_ms: float = 0.0,
    ) -> None:
        now = time.time()
        if ingestor_clock is None:
            ingestor_clock = now

        if ingestor_id not in self.ingestors:
            self.ingestors[ingestor_id] = HealthRecord(
                ingestor_id=ingestor_id,
                partitions_assigned=partitions or [],
            )

        rec = self.ingestors[ingestor_id]
        rec.last_heartbeat = now
        rec.ingestor_clock = ingestor_clock
        rec.network_rtt_ms = network_rtt_ms
        if partitions is not None:
            rec.partitions_assigned = partitions
        if offsets:
            rec.last_log_offset.update(offsets)

        # Check non-monotonic T_commit
        if last_T_commit < rec.last_T_commit:
            rec.status = IngestorStatus.CLOCK_SKEW_CRITICAL
            rec.clock_skew_ms = abs(last_T_commit - rec.last_T_commit) * 1000.0
        elif last_T_commit > rec.last_T_commit:
            rec.last_T_commit = last_T_commit
            rec.clock_skew_ms = abs(ingestor_clock - now) * 1000.0

    def evaluate(self, W_global: float = 0.0) -> dict:
        now = time.time()
        self.alerts.clear()
        commits = []

        for rec in self.ingestors.values():
            age = now - rec.last_heartbeat

            if age > self.silent_timeout_s:
                rec.status = IngestorStatus.SILENT
                self.alerts.append({
                    "severity": "high",
                    "ingestor": rec.ingestor_id,
                    "condition": "silent",
                    "last_seen_s": round(age, 1),
                    "message": f"Ingestor {rec.ingestor_id} silent for {age:.0f}s",
                })
            elif (now - rec.last_T_commit) > self.stuck_timeout_s and rec.last_T_commit > 0:
                rec.status = IngestorStatus.STUCK
                self.alerts.append({
                    "severity": "high",
                    "ingestor": rec.ingestor_id,
                    "condition": "stuck_punctuation",
                    "stuck_duration_s": round(now - rec.last_T_commit, 1),
                    "message": f"Ingestor {rec.ingestor_id} punctuation stuck for {now - rec.last_T_commit:.0f}s",
                })
            elif rec.clock_skew_ms > self.clock_skew_critical_ms:
                skew_status = rec.diagnose_clock_skew()
                rec.status = skew_status
                sev = "info" if skew_status == IngestorStatus.CLOCK_SKEW_INFO else \
                      "warning" if skew_status == IngestorStatus.CLOCK_SKEW_WARNING else "critical"
                self.alerts.append({
                    "severity": sev,
                    "ingestor": rec.ingestor_id,
                    "condition": "clock_skew",
                    "skew_ms": round(rec.clock_skew_ms, 1),
                    "tier": skew_status.value,
                    "message": f"Ingestor {rec.ingestor_id} clock skew {rec.clock_skew_ms:.0f}ms ({skew_status.value})",
                })
            else:
                rec.status = IngestorStatus.ACTIVE

            if rec.status in (IngestorStatus.ACTIVE, IngestorStatus.STUCK):
                commits.append(rec.last_T_commit)

        # W_meta_global
        if commits:
            self.W_meta_global = min(commits)
        else:
            self.W_meta_global = float("-inf")

        w_meta_alert = None
        if self.W_meta_global > float("-inf") and W_global > float("-inf"):
            deviation = W_global - self.W_meta_global
            if deviation > self.w_meta_deviation_s:
                w_meta_alert = {
                    "severity": "warning",
                    "condition": "w_meta_deviation",
                    "deviation_s": round(deviation, 1),
                    "message": f"W_meta_global deviates from W_global by {deviation:.0f}s",
                }

        return {
            "W_meta_global": self.W_meta_global,
            "ingestor_count": len(self.ingestors),
            "active_count": sum(1 for r in self.ingestors.values() if r.status == IngestorStatus.ACTIVE),
            "silent_count": sum(1 for r in self.ingestors.values() if r.status == IngestorStatus.SILENT),
            "alerts": self.alerts,
            "w_meta_alert": w_meta_alert,
            "ingestors": {
                ingestor_id: {
                    "clock_skew_ms": rec.clock_skew_ms,
                    "network_rtt_ms": rec.network_rtt_ms,
                    "status": rec.status.value,
                }
                for ingestor_id, rec in self.ingestors.items()
            }
        }

    def summary(self) -> dict:
        return {
            ingestor_id: {
                "status": rec.status.value,
                "last_heartbeat_s": round(time.time() - rec.last_heartbeat, 1) if rec.last_heartbeat else -1,
                "last_T_commit": rec.last_T_commit,
                "clock_skew_ms": round(rec.clock_skew_ms, 1),
            }
            for ingestor_id, rec in self.ingestors.items()
        }
