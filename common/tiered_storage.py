"""
Quản lý tiered storage dựa trên MinIO với giao thức eviction bốn trạng thái.

Cửa sổ đã đóng có thể được upload ra object storage, xác nhận, đánh dấu uploaded và khôi phục khi cần để giảm áp lực RocksDB/RAM.
"""

import io
import json
import logging
import os
import threading
import time

from common.types import EvictionState

logger = logging.getLogger("tiered_storage")

MAX_RETRIES = 3
RETRY_BASE_DELAY = 1.0


class EvictionManager:
    """Lớp `EvictionManager` quản lý trạng thái và thao tác nghiệp vụ tương ứng.
    
    Ghi chú gốc:
    Tracks eviction state transitions per window with retry logic.
    """

    def __init__(self):
        """Khởi tạo đối tượng của `EvictionManager` và thiết lập trạng thái ban đầu."""
        self._states: dict[str, EvictionState] = {}
        self._retry_counts: dict[str, int] = {}
        self._etags: dict[str, str] = {}  # ETag written on UPLOADING→UPLOADED (spec §8.4)
        self._lock = threading.Lock()

    def get_state(self, window_id: str) -> EvictionState | None:
        """Trả về thông tin `state` từ trạng thái hiện tại."""
        with self._lock:
            return self._states.get(window_id)

    def set_state(self, window_id: str, state: EvictionState):
        """Cập nhật giá trị `state` vào trạng thái hiện tại."""
        with self._lock:
            self._states[window_id] = state

    def set_etag(self, window_id: str, etag: str) -> None:
        """Cập nhật giá trị `etag` vào trạng thái hiện tại."""
        with self._lock:
            self._etags[window_id] = etag

    def get_etag(self, window_id: str) -> str | None:
        """Trả về thông tin `etag` từ trạng thái hiện tại."""
        with self._lock:
            return self._etags.get(window_id)

    def can_retry(self, window_id: str, max_retries: int = MAX_RETRIES) -> bool:
        """Kiểm tra khả năng thực hiện `retry` trước khi chạy thao tác."""
        with self._lock:
            return self._retry_counts.get(window_id, 0) < max_retries

    def record_attempt(self, window_id: str) -> int:
        """Hàm `record_attempt` thực hiện phần xử lý liên quan đến record attempt của `EvictionManager`."""
        with self._lock:
            count = self._retry_counts.get(window_id, 0) + 1
            self._retry_counts[window_id] = count
            return count

    def reset_retry(self, window_id: str):
        """Hàm `reset_retry` thực hiện phần xử lý liên quan đến reset retry của `EvictionManager`."""
        with self._lock:
            self._retry_counts.pop(window_id, None)

    def summary(self) -> dict:
        """Tạo bản tóm tắt trạng thái `summary` để trả về API hoặc báo cáo."""
        with self._lock:
            states_count = {}
            for s in self._states.values():
                key = s.value
                states_count[key] = states_count.get(key, 0) + 1
            return {
                "total_tracked": len(self._states),
                "by_state": states_count,
                "pending_retries": len(self._retry_counts),
            }


class TieredStorageManager:
    """Lớp `TieredStorageManager` quản lý trạng thái và thao tác nghiệp vụ tương ứng.
    
    Ghi chú gốc:
    Manages window data across memory and MinIO object storage.
    """

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket_name: str,
        secure: bool = False,
    ):
        """Khởi tạo đối tượng của `TieredStorageManager` và thiết lập trạng thái ban đầu."""
        self.endpoint = endpoint
        self.bucket = bucket_name
        self.eviction = EvictionManager()
        # Counter visible to MonitoringManager — incremented every time
        # _do_upload exhausts retries. The monitoring loop reads this via
        # `getattr(storage, 'upload_error_count', 0)` so the
        # `csdlpt_minio_upload_errors_total` Counter can be inc'd by diff.
        self.upload_error_count: int = 0

        try:
            from minio import Minio
            import urllib3

            # Customize HTTP pool client to optimize connection reuse and multiplexing
            http_client = urllib3.PoolManager(
                maxsize=32,
                block=False,
                retries=urllib3.util.Retry(total=3, backoff_factor=0.2)
            )

            self.client = Minio(
                endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure,
                http_client=http_client,
            )
            self._ensure_bucket()
            logger.info(
                "TieredStorage: connected to %s, bucket=%s", endpoint, bucket_name
            )
        except Exception as e:
            logger.warning("TieredStorage: MinIO unavailable (%s), tiered storage disabled", e)
            self.client = None

    def _ensure_bucket(self):
        """Đảm bảo điều kiện/tài nguyên `ensure bucket` đã sẵn sàng trước khi dùng."""
        if self.client is None:
            return
        try:
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket)
        except Exception as e:
            logger.warning("TieredStorage: bucket check failed: %s, disabling", e)
            self.client = None

    def _object_key(self, window_id: str, partition_id: int) -> str:
        """Hàm `_object_key` thực hiện phần xử lý liên quan đến object key của `TieredStorageManager`."""
        return f"strict-watermark/historical/{partition_id}/{window_id}.json"

    def upload_window(self, window_id: str, window_data: dict, partition_id: int = 0,
                      sync: bool = False) -> bool:
        """Hàm `upload_window` thực hiện phần xử lý liên quan đến upload window của `TieredStorageManager`.
        
        Ghi chú gốc:
        Upload closed window to MinIO. Returns True if upload succeeded.
        
                When sync=True, blocks until the upload completes and returns the actual
                result. When sync=False (default), starts an async daemon thread and
                returns True if the thread was started.
        """
        if self.client is None:
            self.eviction.set_state(window_id, EvictionState.UPLOADED)
            return True

        self.eviction.set_state(window_id, EvictionState.UPLOADING)

        if sync:
            return self._do_upload(window_id, window_data, partition_id)

        t = threading.Thread(
            target=self._do_upload,
            args=(window_id, window_data, partition_id),
            daemon=True,
        )
        t.start()
        return True

    def download_window(self, window_id: str, partition_id: int = 0) -> dict | None:
        """Hàm `download_window` thực hiện phần xử lý liên quan đến download window của `TieredStorageManager`.
        
        Ghi chú gốc:
        Download window data from MinIO. Returns None if not found or on error.
        """
        if self.client is None:
            return None
        try:
            response = self.client.get_object(self.bucket, self._object_key(window_id, partition_id))
            data = response.read()
            response.close()
            response.release_conn()
            import gzip
            if data.startswith(b'\x1f\x8b'):
                data = gzip.decompress(data)
            return json.loads(data)
        except Exception as e:
            logger.warning("TieredStorage: download failed for %s: %s", window_id, e)
            return None

    def purge_window(self, window_id: str, partition_id: int = 0) -> bool:
        """Loại bỏ dữ liệu `purge window` đã hết hạn hoặc không còn cần thiết.
        
        Ghi chú gốc:
        Delete window from MinIO. Returns True on success.
        """
        if self.client is None:
            self.eviction.set_state(window_id, EvictionState.PURGED)
            return True
        try:
            self.client.remove_object(self.bucket, self._object_key(window_id, partition_id))
            self.eviction.set_state(window_id, EvictionState.PURGED)
            return True
        except Exception as e:
            logger.warning("TieredStorage: purge failed for %s: %s", window_id, e)
            return False

    def list_windows(self, partition_id: int, prefix: str = "") -> list[str]:
        """Liệt kê các mục `windows` hiện có.
        
        Ghi chú gốc:
        List all window object keys for a partition.
        """
        if self.client is None:
            return []
        try:
            list_prefix = f"strict-watermark/historical/{partition_id}/"
            if prefix:
                list_prefix += prefix
            objects = self.client.list_objects(self.bucket, prefix=list_prefix)
            return [obj.object_name for obj in objects]
        except Exception as e:
            logger.warning("TieredStorage: list failed for partition %s: %s", partition_id, e)
            return []

    def get_storage_stats(self) -> dict:
        """Trả về thông tin `storage stats` từ trạng thái hiện tại.
        
        Ghi chú gốc:
        Return storage usage statistics.
        """
        if self.client is None:
            return {"status": "disabled", "total_objects": 0, "total_size_bytes": 0}
        try:
            objects = self.client.list_objects(self.bucket)
            total = 0
            total_size = 0
            for obj in objects:
                total += 1
                total_size += obj.size
            return {
                "status": "connected",
                "endpoint": self.endpoint,
                "bucket": self.bucket,
                "total_objects": total,
                "total_size_bytes": total_size,
            }
        except Exception as e:
            logger.warning("TieredStorage: stats query failed: %s", e)
            return {"status": "error", "total_objects": 0, "total_size_bytes": 0}

    def _do_upload(self, window_id: str, window_data: dict, partition_id: int = 0) -> None:
        """Hàm `_do_upload` thực hiện phần xử lý liên quan đến do upload của `TieredStorageManager`."""
        import gzip
        data = gzip.compress(json.dumps(window_data).encode())
        headers = {}
        sse_kms_key = os.environ.get("MINIO_SSE_KMS_KEY_ID", "")
        if sse_kms_key:
            headers["x-amz-server-side-encryption"] = "aws:kms"
            headers["x-amz-server-side-encryption-aws-kms-key-id"] = sse_kms_key
        elif os.environ.get("MINIO_SSE_ENABLE", "false").lower() in ("true", "1", "yes"):
            headers["x-amz-server-side-encryption"] = "AES256"

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                result = self.client.put_object(
                    self.bucket,
                    self._object_key(window_id, partition_id),
                    data=io.BytesIO(data),
                    length=len(data),
                    headers=headers,
                )
                if hasattr(result, "etag") and result.etag:
                    self.eviction.set_etag(window_id, result.etag)
                self.eviction.set_state(window_id, EvictionState.UPLOADED)
                self.eviction.reset_retry(window_id)
                return
            except Exception as e:
                count = self.eviction.record_attempt(window_id)
                if count < MAX_RETRIES:
                    delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                    logger.warning(
                        "TieredStorage: upload attempt %d/%d failed for %s: %s, retrying in %.1fs",
                        attempt, MAX_RETRIES, window_id, e, delay,
                    )
                    time.sleep(delay)
                else:
                    self.eviction.set_state(window_id, EvictionState.CLOSED)
                    self.upload_error_count += 1
                    logger.error(
                        "TieredStorage: upload failed after %d attempts for %s: %s",
                        MAX_RETRIES, window_id, e,
                    )
