"""The operational HTTP surface: /healthz, /readyz, /metrics."""

import httpx

from millrace.server import serve


def test_ops_server_routes():
    ready = {"ok": False}
    with serve(
        "127.0.0.1", 0, ready=lambda: None if ready["ok"] else "not started"
    ) as server:
        base = f"http://127.0.0.1:{server.port}"

        r = httpx.get(f"{base}/healthz", timeout=5)
        assert r.status_code == 200

        r = httpx.get(f"{base}/readyz", timeout=5)
        assert r.status_code == 503
        assert "not started" in r.text  # the reason rides the 503 body
        ready["ok"] = True
        r = httpx.get(f"{base}/readyz", timeout=5)
        assert r.status_code == 200

        r = httpx.get(f"{base}/metrics", timeout=5)
        assert r.status_code == 200
        assert "text/plain" in r.headers["content-type"]

        r = httpx.get(f"{base}/nope", timeout=5)
        assert r.status_code == 404


def test_healthz_reflects_the_live_callback():
    """Liveness is the caller's decision: a problem string is a 503 with
    the reason in the body; None recovers the 200. The default (no
    callback) is the legacy always-200."""
    state = {"problem": None}
    with serve(
        "127.0.0.1", 0, ready=lambda: None, live=lambda: state["problem"]
    ) as server:
        base = f"http://127.0.0.1:{server.port}"
        assert httpx.get(f"{base}/healthz", timeout=5).status_code == 200
        state["problem"] = "flush pipeline halted: receipt horizon exceeded"
        r = httpx.get(f"{base}/healthz", timeout=5)
        assert r.status_code == 503
        assert "receipt horizon exceeded" in r.text
        state["problem"] = None
        assert httpx.get(f"{base}/healthz", timeout=5).status_code == 200


def test_serve_default_is_ready_and_alive():
    with serve("127.0.0.1", 0, ready=lambda: None) as server:
        base = f"http://127.0.0.1:{server.port}"
        assert httpx.get(f"{base}/readyz", timeout=5).status_code == 200
        assert httpx.get(f"{base}/healthz", timeout=5).status_code == 200
