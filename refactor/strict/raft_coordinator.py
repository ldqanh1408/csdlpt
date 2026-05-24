"""Simulated Raft Coordinator HA — leader election + state replication via HTTP.

Spec §5.1-5.4: 3-instance Raft cluster with leader election and state replication.
"""

import json
import logging
import threading
import time
import urllib.request
from enum import Enum

from refactor.strict.coordinator import StrictCoordinator

logger = logging.getLogger("raft_coordinator")


class RaftRole(Enum):
    LEADER = "leader"
    FOLLOWER = "follower"
    CANDIDATE = "candidate"


class RaftCoordinator:
    def __init__(self, coordinator_id: str, peers: list[str],
                 delta_base_s: float = 10.0, state_path: str = "/tmp/coordinator-state.json",
                 db_path: str = None, heartbeat_interval_ms: int = 200,
                 zk_ensemble: bool = False):
        self.coordinator_id = coordinator_id
        self.peers = peers
        self.heartbeat_interval_ms = heartbeat_interval_ms
        self.role: RaftRole = RaftRole.FOLLOWER
        self.current_term: int = 0
        self.voted_for: str = ""
        self.leader_id: str = ""
        self._last_leader_heartbeat: float = 0.0
        self._lock = threading.Lock()
        self.coordinator = StrictCoordinator(delta_base_s=delta_base_s,
                                              state_path=state_path, db_path=db_path)
        self.coordinator.load_state()
        self._stop = threading.Event()
        self.zk_ensemble = zk_ensemble
        if zk_ensemble:
            logger.info("RaftCoordinator: ZK election mode enabled (ensemble with %s)", peers)
            self._start_zk_election_loop()
        elif peers:
            self._start_election_timer()

    def _start_election_timer(self):
        def _timer():
            while not self._stop.is_set():
                time.sleep(0.5)
                with self._lock:
                    if self.role == RaftRole.LEADER:
                        continue
                    if time.time() - self._last_leader_heartbeat > 3.0:
                        self._start_election()
        threading.Thread(target=_timer, daemon=True).start()

    def _start_election(self):
        self.current_term += 1
        self.role = RaftRole.CANDIDATE
        self.voted_for = self.coordinator_id
        votes = 1
        for peer in self.peers:
            try:
                data = json.dumps({"term": self.current_term,
                                   "candidate_id": self.coordinator_id,
                                   "W_global": self.coordinator.W_global}).encode()
                req = urllib.request.Request(f"http://{peer}/raft-vote", data=data,
                                             headers={"Content-Type": "application/json"})
                resp = urllib.request.urlopen(req, timeout=0.5)
                if json.loads(resp.read()).get("granted"):
                    votes += 1
            except Exception:
                pass
        with self._lock:
            if votes > (len(self.peers) + 1) // 2:
                self.role = RaftRole.LEADER
                self.leader_id = self.coordinator_id
                logger.info("Raft: %s elected LEADER (term=%d, votes=%d)",
                            self.coordinator_id, self.current_term, votes)
                self._start_heartbeat_loop()
            else:
                self.role = RaftRole.FOLLOWER

    def handle_vote_request(self, term: int, candidate_id: str, W_global: float) -> dict:
        with self._lock:
            if term > self.current_term:
                self.current_term = term
                self.role = RaftRole.FOLLOWER
                self.voted_for = ""
            if term >= self.current_term and (not self.voted_for or self.voted_for == candidate_id):
                self.voted_for = candidate_id
                self._last_leader_heartbeat = time.time()
                if W_global > self.coordinator.W_global:
                    self.coordinator.W_global = W_global
                return {"granted": True, "term": self.current_term}
            return {"granted": False, "term": self.current_term}

    def _start_heartbeat_loop(self):
        def _loop():
            while not self._stop.is_set() and self.role == RaftRole.LEADER:
                state = self.coordinator.broadcast()
                state["term"] = self.current_term
                state["leader_id"] = self.coordinator_id
                data = json.dumps(state).encode()
                for peer in self.peers:
                    try:
                        req = urllib.request.Request(f"http://{peer}/raft-state", data=data,
                                                     headers={"Content-Type": "application/json"})
                        urllib.request.urlopen(req, timeout=0.5)
                    except Exception:
                        pass
                time.sleep(self.heartbeat_interval_ms / 1000.0)
        threading.Thread(target=_loop, daemon=True).start()

    def receive_state(self, state: dict):
        with self._lock:
            self._last_leader_heartbeat = time.time()
            self.role = RaftRole.FOLLOWER
            term = state.get("term", 0)
            if term >= self.current_term:
                self.current_term = term
                self.leader_id = state.get("leader_id", "")
                wg = state.get("W_global", float("-inf"))
                if wg > self.coordinator.W_global:
                    self.coordinator.W_global = wg
                    self.coordinator.W_global_prev = wg

    def receive_heartbeat(self, hb):
        return self.coordinator.receive_heartbeat(hb)

    def set_failover_manager(self, fm: object) -> None:
        """Inject a FailoverManager, delegating to the inner StrictCoordinator."""
        self.coordinator.set_failover_manager(fm)

    def broadcast(self) -> dict:
        r = self.coordinator.broadcast()
        r["raft_role"] = self.role.value
        r["raft_term"] = self.current_term
        r["raft_leader"] = self.leader_id
        return r

    def save_state(self):
        self.coordinator.save_state()

    @property
    def W_global(self):
        return self.coordinator.W_global

    # ------------------------------------------------------------------
    # ZK-style leader election (simulated via HTTP)
    # ------------------------------------------------------------------

    def _start_zk_election_loop(self):
        """ZK-style election: leader is the reachable coordinator with lowest numeric ID."""
        def _zk_loop():
            while not self._stop.is_set():
                time.sleep(2.0)
                self._zk_elect_leader()
                if self.role == RaftRole.LEADER:
                    self._zk_heartbeat_loop()
        threading.Thread(target=_zk_loop, daemon=True).start()

    def _zk_elect_leader(self):
        """Elect leader: lowest coordinator ID that responds to health check."""
        candidates = [self.coordinator_id] + self.peers
        reachable = []
        for cid in candidates:
            if cid == self.coordinator_id:
                reachable.append(cid)
                continue
            try:
                data = json.dumps({"action": "zk-ping"}).encode()
                req = urllib.request.Request(f"http://{cid}/zk-vote", data=data,
                                             headers={"Content-Type": "application/json"})
                urllib.request.urlopen(req, timeout=0.5)
                reachable.append(cid)
            except Exception:
                pass

        if not reachable:
            return

        # Leader is the coordinator with lowest numeric ID among reachable
        def _parse_id(cid: str) -> int:
            try:
                return int(cid)
            except ValueError:
                return 99999

        reachable_sorted = sorted(reachable, key=_parse_id)
        new_leader = reachable_sorted[0]
        with self._lock:
            if new_leader == self.coordinator_id and self.role != RaftRole.LEADER:
                self.role = RaftRole.LEADER
                self.leader_id = self.coordinator_id
                logger.info("ZK election: %s elected LEADER (reachable=%s)",
                            self.coordinator_id, reachable)
            elif new_leader != self.coordinator_id:
                self.role = RaftRole.FOLLOWER
                self.leader_id = new_leader
                logger.debug("ZK election: %s is FOLLOWER, leader=%s",
                            self.coordinator_id, new_leader)

    def _zk_heartbeat_loop(self):
        """ZK leader replicates state to peers (single-shot per election)."""
        if self.role != RaftRole.LEADER:
            return
        state = self.coordinator.broadcast()
        state["leader_id"] = self.coordinator_id
        state["mode"] = "zk"
        data = json.dumps(state).encode()
        for peer in self.peers:
            try:
                req = urllib.request.Request(f"http://{peer}/zk-state", data=data,
                                             headers={"Content-Type": "application/json"})
                urllib.request.urlopen(req, timeout=0.5)
            except Exception:
                pass

    def handle_zk_vote(self, data: dict) -> dict:
        """Handle ZK vote/ping requests from peers."""
        action = data.get("action", "")
        if action == "zk-ping":
            return {"ok": True, "coordinator_id": self.coordinator_id}
        return {"ok": False, "error": "unknown zk action"}

    def handle_zk_state(self, data: dict) -> dict:
        """Receive ZK-replicated state from leader."""
        with self._lock:
            self._last_leader_heartbeat = time.time()
            self.role = RaftRole.FOLLOWER
            self.leader_id = data.get("leader_id", "")
            wg = data.get("W_global", float("-inf"))
            if wg > self.coordinator.W_global:
                self.coordinator.W_global = wg
                self.coordinator.W_global_prev = wg
        return {"ok": True}

    def shutdown(self):
        self._stop.set()
