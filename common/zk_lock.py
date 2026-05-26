"""ZooKeeper-based leader election lock for Aggregator HA."""

import logging
import os
from kazoo.client import KazooClient
from kazoo.recipe.lock import Lock

logger = logging.getLogger("zk_lock")


class ZKLeaderElection:
    """Leader election lock via ZooKeeper ephemeral nodes.

    Uses Kazoo's Lock recipe. It attempts to acquire an exclusive lock
    under the specified path. If successful, this node becomes the active
    leader. On crash or network loss, the ephemeral node is automatically
    removed by ZooKeeper, allowing standbys to take over.
    """

    def __init__(self, zk_hosts: str, lock_path: str = "/csdlpt/aggregator-lock"):
        self.zk_hosts = zk_hosts
        self.lock_path = lock_path
        self._client = None
        self._lock = None
        self._is_leader = False

    def start(self):
        logger.info("ZK: connecting to hosts=%s", self.zk_hosts)
        auth_data = None
        default_acl = None
        zk_user = os.environ.get("ZK_ACL_USER", "")
        zk_password = os.environ.get("ZK_ACL_PASSWORD", "")
        if zk_user and zk_password:
            from kazoo.security import make_digest_acl
            auth_data = [("digest", f"{zk_user}:{zk_password}")]
            default_acl = [make_digest_acl(zk_user, zk_password, all=True)]

        self._client = KazooClient(hosts=self.zk_hosts, auth_data=auth_data, default_acl=default_acl)
        self._client.start()
        self._lock = self._client.Lock(self.lock_path)

    def try_acquire(self) -> bool:
        if not self._client or not self._client.connected:
            return False
        try:
            self._is_leader = self._lock.acquire(blocking=False)
            if self._is_leader:
                logger.info("ZK: acquired lock at path=%s (leader)", self.lock_path)
            return self._is_leader
        except Exception as e:
            logger.warning("ZK: try_acquire failed: %s", e)
            return False

    @property
    def is_leader(self) -> bool:
        return self._is_leader

    def release(self):
        if self._lock and self._is_leader:
            try:
                self._lock.release()
                logger.info("ZK: released lock at path=%s", self.lock_path)
            except Exception:
                pass
        self._is_leader = False
        if self._client:
            try:
                self._client.stop()
                self._client.close()
            except Exception:
                pass
            self._client = None
            self._lock = None
