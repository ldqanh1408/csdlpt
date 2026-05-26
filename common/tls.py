"""Optional TLS support for HTTP servers (Spec Deploy §10)."""

import logging
import os
import ssl

logger = logging.getLogger("tls")


def create_tls_context(cert_file: str = "", key_file: str = "") -> ssl.SSLContext | None:
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
    if ctx is None:
        return sock
    return ctx.wrap_socket(sock, server_side=True)
