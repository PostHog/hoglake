"""Operational HTTP surface: /metrics, /healthz, /readyz.

/metrics is the Prometheus exposition — the staged-bytes and
oldest-staged-age gauges are the backpressure signals, and metrics never
feed control flow. /healthz and /readyz are the probe paths an operator
reaches for exactly when the pipeline is busiest:

- ``/healthz`` is LIVENESS: 200 while the process is sound, 503 (with
  the reason in the body) when the pipeline is in a terminal state the
  supervisor must see — a loudly halted flush pipeline, a fenced-off
  partition. The states that feed it are LATCHED (a halt never
  un-halts; a fence mark holds until the assignment revokes it), so the
  probe cannot flap. The default (no ``live`` callback) is the old
  always-200.
- ``/readyz`` is READINESS: 200 once the pipeline is consuming, 503
  before it — and 503 when liveness fails (a pod that lost a
  partition's ownership is not ready to serve it).

Both callbacks return ``None`` for OK or a string with the failure
detail (carried in the 503 body); the decision logic itself lives in the
caller as a pure function (main.py's, millpond's ``_liveness_status``
pattern — unit-tested without I/O).
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Self

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

log = logging.getLogger(__name__)

#: A probe decision: None = OK; a string = the failure detail (503 body).
ProbeStatus = Callable[[], str | None]


class OpsServer:
    """The metrics/health listener, on its own daemon thread.

    Plain ``http.server``: the surface is three GET routes with
    byte-counted responses, so HTTP/1.1 keep-alive is safe. Probes must
    answer while the pipeline loop is saturated — the server shares no
    lock, executor or event loop with the consume/flush paths.
    """

    def __init__(
        self,
        host: str,
        port: int,
        *,
        ready: ProbeStatus,
        live: ProbeStatus | None = None,
    ) -> None:
        self._httpd = ThreadingHTTPServer(
            (host, port), self._handler_class(ready, live or (lambda: None))
        )
        self._thread = threading.Thread(
            target=self._httpd.serve_forever,
            name="millrace-ops",
            daemon=True,
        )

    @property
    def port(self) -> int:
        """The bound port (with port=0, the ephemeral one chosen)."""
        return int(self._httpd.server_address[1])

    def start(self) -> None:
        self._thread.start()

    def close(self) -> None:
        """Stop serving and release the port. Idempotent."""
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join(timeout=5)

    def __enter__(self) -> Self:
        # serve() already started the thread; entering twice is a bug
        # ("threads can only be started once" is the stdlib's answer).
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @staticmethod
    def _handler_class(
        ready: ProbeStatus, live: ProbeStatus
    ) -> type[BaseHTTPRequestHandler]:
        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self) -> None:
                path = self.path.split("?", 1)[0].rstrip("/") or "/"
                if path == "/healthz":
                    problem = live()
                    if problem is None:
                        self._respond(200, b"ok\n")
                    else:
                        self._respond(503, f"unhealthy: {problem}\n".encode())
                elif path == "/readyz":
                    problem = ready()
                    if problem is None:
                        self._respond(200, b"ready\n")
                    else:
                        self._respond(503, f"not ready: {problem}\n".encode())
                elif path == "/metrics":
                    self._respond(200, generate_latest(), CONTENT_TYPE_LATEST)
                else:
                    self._respond(404, b"not found\n")

            def _respond(
                self, status: int, body: bytes, content_type: str = "text/plain"
            ) -> None:
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, fmt: str, *args: object) -> None:
                log.debug("ops http: " + fmt, *args)

        return Handler


def serve(
    host: str, port: int, *, ready: ProbeStatus, live: ProbeStatus | None = None
) -> OpsServer:
    """Bind and start the metrics/health listener; returns the handle.
    ``port=0`` binds an ephemeral port (tests); read :attr:`OpsServer.port`
    for the choice. The probes take :data:`ProbeStatus` callbacks — None
    is OK, a string is the 503's reason."""
    server = OpsServer(host, port, ready=ready, live=live)
    server.start()
    log.info("operational surface listening on %s:%d", host, server.port)
    return server
