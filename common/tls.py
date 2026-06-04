"""
Tiện ích tạo SSLContext và bọc socket HTTP bằng TLS tùy chọn.

Service chỉ bật TLS khi có certificate/key trong cấu hình, nhờ đó cùng code chạy được ở local không TLS và môi trường triển khai có mã hóa.
"""

import logging
import os
import ssl

logger = logging.getLogger("tls")


def create_tls_context(cert_file: str = "", key_file: str = "") -> ssl.SSLContext | None:
    """Hàm `create_tls_context` thực hiện phần xử lý liên quan đến create tls context."""
    cert = cert_file or os.environ.get("TLS_CERT_FILE", "")
    key = key_file or os.environ.get("TLS_KEY_FILE", "")
    if not cert or not key:
        return None
    try:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(cert, key)
        logger.info("TLS enabled: cert=%s", cert)
        return ctx
    except Exception as e:
        logger.warning("TLS setup failed: %s", e)
        return None


def wrap_socket_with_tls(sock, ctx: ssl.SSLContext | None = None):
    """Hàm `wrap_socket_with_tls` thực hiện phần xử lý liên quan đến wrap socket with tls."""
    if ctx is None:
        return sock
    return ctx.wrap_socket(sock, server_side=True)
