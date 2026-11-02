import os

import pytest

try:  # one process-global hypothesis profile for the qe_* suites
    from hypothesis import HealthCheck, settings

    settings.register_profile(
        "qe",
        deadline=None,  # CI/laptop timing variance must not flake tests
        suppress_health_check=[HealthCheck.too_slow],
    )
    settings.load_profile("qe")
except ImportError:  # pragma: no cover
    pass

HOGLAKE_URL = os.environ.get("HOGLAKE_URL", "http://localhost:8080")
S3_ENDPOINT = os.environ.get("HOGLAKE_S3_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.environ.get("HOGLAKE_S3_ACCESS_KEY", "hoglake")
S3_SECRET_KEY = os.environ.get("HOGLAKE_S3_SECRET_KEY", "hoglake123")


def _server_reachable() -> bool:
    import httpx

    try:
        r = httpx.get(HOGLAKE_URL.rstrip("/") + "/v1/catalogs", timeout=3.0)
        return r.status_code == 200
    except Exception:
        return False


@pytest.fixture(scope="session")
def live_server_url() -> str:
    if not _server_reachable():
        pytest.skip(f"no live hoglake server at {HOGLAKE_URL} (set HOGLAKE_URL)")
    return HOGLAKE_URL


@pytest.fixture(autouse=True)
def _codec_fallbacks_fail(monkeypatch):
    """With the Rust VARIANT codec installed, the suites' encodes run through
    it, and a fallback to Python would hide a difference: it fails the test.
    A test of the fallback itself puts the real one back."""
    from pyhoglake import _variant_backend

    def fail(reason, row, column, report):
        raise AssertionError(f"VARIANT codec fallback: {reason} at row {row}")

    monkeypatch.setattr(_variant_backend, "_fall_back", fail)
