"""Simulated Raft Coordinator HA — leader election + state replication via HTTP.

Spec §5.1-5.4: 3-instance Raft cluster with leader election and state replication.
"""

import json
import logging
import threading
import time
import urllib.request
from enum import Enum

try:
    import grpc
    from common import csdlpt_pb2_grpc
except ImportError:
    grpc = None

_grpc_channels = {}

def get_grpc_target(peer):
    if not peer:
        return None
    if ":" in peer:
        host, port_str = peer.rsplit(":", 1)
        try:
            grpc_port = int(port_str) + 50
            return f"{host}:{grpc_port}"
        except ValueError:
            pass
    return None

def get_grpc_stub(peer):
    if grpc is None:
        return None
    target = get_grpc_target(peer)
    if not target:
        return None
    if target not in _grpc_channels:
        try:
            channel = grpc.insecure_channel(target)
            stub = csdlpt_pb2_grpc.CoordinatorServiceStub(channel)
            _grpc_channels[target] = (channel, stub)
        except Exception:
            return None
    return _grpc_channels[target][1]


from common import csdlpt_pb2
from strict.coordinator import StrictCoordinator

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
        self._last_committed_term: int = 0
        self._lock = threading.RLock()  # RLock prevents self-deadlock in ZK election loop
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
            sent = False
            if grpc is not None:
                try:
                    stub = get_grpc_stub(peer)
                    if stub is not None:
                        reply = stub.RaftVote(
                            csdlpt_pb2.RaftVoteMsg(
                                term=self.current_term,
                                candidate_id=self.coordinator_id,
                                W_global=self.coordinator.W_global,
                            ),
                            timeout=0.5
                        )
                        if reply.granted:
                            votes += 1
                        sent = True
                except Exception:
                    pass
            if not sent:
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
                acks = 1  # leader self-ack
                quorum = (len(self.peers) + 1) // 2 + 1
                for peer in self.peers:
                    sent = False
                    if grpc is not None:
                        try:
                            stub = get_grpc_stub(peer)
                            if stub is not None:
                                json_state = json.dumps(state)
                                reply = stub.RaftState(
                                    csdlpt_pb2.RaftStateMsg(
                                        term=self.current_term,
                                        leader_id=self.coordinator_id,
                                        W_global=self.coordinator.W_global,
                                        json_state=json_state,
                                    ),
                                    timeout=0.5
                                )
                                if reply.ok:
                                    acks += 1
                                sent = True
                        except Exception:
                            pass
                    if not sent:
                        try:
                            req = urllib.request.Request(f"http://{peer}/raft-state", data=data,
                                                         headers={"Content-Type": "application/json"})
                            resp = urllib.request.urlopen(req, timeout=0.5)
                            if resp.status == 200:
                                acks += 1
                        except Exception:
                            pass
                if acks >= quorum:
                    self._last_committed_term = self.current_term
                else:
                    logger.warning("Raft heartbeat: %d/%d acks (quorum=%d)", acks, len(self.peers) + 1, quorum)
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
        # ── Leader guard: followers reject direct worker heartbeats ──
        # Workers should always target the leader (coordinator_health_check_loop
        # handles this). If a follower receives a heartbeat, it means the worker
        # hasn't discovered the leader yet — reject so the worker falls through
        # and the health check loop redirects.
        if self.role != RaftRole.LEADER:
            logger.warning(
                "RaftCoordinator: follower %s received WorkerHeartbeat from %s "
                "(leader is %s) — dropping. Worker should redirect to leader.",
                self.coordinator_id, getattr(hb, 'worker_id', '?'), self.leader_id,
            )
            return None  # signal to caller: not accepted
        self.coordinator.receive_heartbeat(hb)
        return True  # signal to caller: accepted by leader

    def set_failover_manager(self, fm: object) -> None:
        """Inject a FailoverManager, delegating to the inner StrictCoordinator."""
        self.coordinator.set_failover_manager(fm)

    def set_ingestor_health(self, monitor: object) -> None:
        """Proxy: inject ingestor health monitor into inner coordinator."""
        if hasattr(self.coordinator, "set_ingestor_health"):
            self.coordinator.set_ingestor_health(monitor)

    def load_state(self) -> None:
        """Proxy: inner StrictCoordinator already loaded state in __init__; no-op here."""
        pass

    def __getattr__(self, name):
        """Proxy any unresolved attribute lookups to the inner StrictCoordinator.
        This covers partitions, delta_base, _node_skew_max_ms, _watermark_lag_s, etc.
        """
        # Avoid infinite recursion on 'coordinator' itself (set in __init__ via __dict__)
        if name == "coordinator":
            raise AttributeError(name)
        try:
            return getattr(object.__getattribute__(self, "coordinator"), name)
        except AttributeError:
            raise AttributeError(f"'RaftCoordinator' object has no attribute '{name}'")

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
    # ZK-style leader election (simulated via HTTP or real ZooKeeper)
    # ------------------------------------------------------------------

    def _start_zk_election_loop(self):
        """ZK-style election. If ZK_HOSTS/ZK_ENSEMBLE is set (and not boolean),
        uses a real ZooKeeper client. Otherwise falls back to simulated HTTP election.
        """
        import os
        zk_hosts = os.environ.get("ZK_HOSTS") or os.environ.get("ZK_ENSEMBLE")
        if zk_hosts and zk_hosts.lower() in ("1", "true", "yes", "on"):
            zk_hosts = None

        # Try to import kazoo; fall back to simulated HTTP if unavailable
        kazoo_available = False
        if zk_hosts:
            try:
                from kazoo.client import KazooClient  # noqa: F401
                kazoo_available = True
            except ImportError:
                logger.warning("RaftCoordinator: kazoo not installed; falling back to simulated HTTP ZK election")
                zk_hosts = None

        if zk_hosts and kazoo_available:
            logger.info("RaftCoordinator: starting real ZooKeeper election on hosts=%s", zk_hosts)
            self._zk_hosts = zk_hosts
            self._zk_client = None
            self._zk_lock = None

            def _real_zk_loop():
                from kazoo.client import KazooClient
                while not self._stop.is_set():
                    try:
                        if not self._zk_client or not self._zk_client.connected:
                            if self._zk_client:
                                try:
                                    self._zk_client.stop()
                                    self._zk_client.close()
                                except Exception:
                                    pass
                            logger.info("RaftCoordinator: ZK connecting to %s", self._zk_hosts)
                            auth_data = None
                            default_acl = None
                            zk_user = os.environ.get("ZK_ACL_USER", "")
                            zk_password = os.environ.get("ZK_ACL_PASSWORD", "")
                            if zk_user and zk_password:
                                from kazoo.security import make_digest_acl
                                auth_data = [("digest", f"{zk_user}:{zk_password}")]
                                default_acl = [make_digest_acl(zk_user, zk_password, all=True)]

                            self._zk_client = KazooClient(hosts=self._zk_hosts, auth_data=auth_data, default_acl=default_acl)
                            self._zk_client.start()
                            self._zk_client.ensure_path("/csdlpt")
                            self._zk_lock = self._zk_client.Lock("/csdlpt/coordinator-lock", identifier=self.coordinator_id)

                        acquired = self._zk_lock.acquire(blocking=False)
                        if acquired:
                            with self._lock:
                                if self.role != RaftRole.LEADER:
                                    self.role = RaftRole.LEADER
                                    self.leader_id = self.coordinator_id
                                    logger.info("Real ZK Coordinator: elected LEADER")

                            # Write leader ID to ephemeral node
                            try:
                                if self._zk_client.exists("/csdlpt/coordinator-leader"):
                                    self._zk_client.set("/csdlpt/coordinator-leader", self.coordinator_id.encode('utf-8'))
                                else:
                                    self._zk_client.create("/csdlpt/coordinator-leader", self.coordinator_id.encode('utf-8'), ephemeral=True)
                            except Exception as e:
                                logger.warning("Real ZK Coordinator: failed to write leader node: %s", e)

                            # Replicate state to peers
                            self._zk_heartbeat_loop()
                        else:
                            # If the leader in ZK is still us, don't reset role
                            # (lock may have flapped due to ZK connection hiccup but
                            # we're still the designated leader)
                            try:
                                if self._zk_client.exists("/csdlpt/coordinator-leader"):
                                    data, _ = self._zk_client.get("/csdlpt/coordinator-leader")
                                    leader_id = data.decode('utf-8')
                                    with self._lock:
                                        self.leader_id = leader_id
                                        if leader_id == self.coordinator_id:
                                            if self.role != RaftRole.LEADER:
                                                logger.warning(
                                                    "Real ZK Coordinator: ZK still shows us as leader, "
                                                    "restoring LEADER role (lock acquire flapped)")
                                            self.role = RaftRole.LEADER
                                        else:
                                            self.role = RaftRole.FOLLOWER
                                else:
                                    with self._lock:
                                        self.role = RaftRole.FOLLOWER
                                        self.leader_id = ""
                            except Exception as e:
                                logger.debug("Real ZK Coordinator: failed to read leader node: %s", e)
                    except Exception as e:
                        logger.warning("Real ZK Coordinator election loop error: %s", e)
                        with self._lock:
                            self.role = RaftRole.FOLLOWER
                            self.leader_id = ""
                    time.sleep(2.0)

            threading.Thread(target=_real_zk_loop, daemon=True).start()
        else:
            logger.info("RaftCoordinator: ZK_HOSTS not configured. Starting simulated HTTP ZK election.")
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
            sent = False
            if grpc is not None:
                try:
                    stub = get_grpc_stub(cid)
                    if stub is not None:
                        reply = stub.ZkVote(
                            csdlpt_pb2.ZkVoteMsg(
                                action="zk-ping"
                            ),
                            timeout=0.5
                        )
                        if reply.ok:
                            reachable.append(cid)
                        sent = True
                except Exception:
                    pass
            if not sent:
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
            import re
            host = cid.split(":")[0]
            m = re.search(r'\d+', host)
            if m:
                return int(m.group(0))
            m = re.search(r'\d+', cid)
            if m:
                return int(m.group(0))
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
            sent = False
            if grpc is not None:
                try:
                    stub = get_grpc_stub(peer)
                    if stub is not None:
                        json_state = json.dumps(state)
                        stub.ZkState(
                            csdlpt_pb2.ZkStateMsg(
                                leader_id=self.coordinator_id,
                                mode="zk",
                                json_state=json_state,
                            ),
                            timeout=0.5
                        )
                        sent = True
                except Exception:
                    pass
            if not sent:
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
        if hasattr(self, "_zk_lock") and self._zk_lock:
            try:
                self._zk_lock.release()
            except Exception:
                pass
        if hasattr(self, "_zk_client") and self._zk_client:
            try:
                self._zk_client.stop()
                self._zk_client.close()
            except Exception:
                pass
