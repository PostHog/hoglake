import os

import pytest

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


def info_at_head(catalog, table):
    """``table.info()`` at the catalog's CURRENT HEAD, i.e. with EXACT totals.

    A plain ``info()`` serves ``record_count`` / ``file_count`` /
    ``file_size_bytes`` from the server's maintenance sample and returns
    ``None`` for all three until a published generation covers the table
    (hoglake #232). That is right for the endpoint -- the aggregate it
    replaced was ~10M manifest rows per call on a busy table -- and
    useless to an integration test asserting what a replication cycle
    just published, where the question is "what does the manifest hold
    RIGHT NOW".

    Naming head puts the read on the time-travel path, which still
    aggregates the manifest, so the numbers are a fact and no sampler has
    to be waited for. Same helper, same reason, as pyhoglake's own
    ``_info_at_head`` and the server suite's ``tableWithExactTotals``.

    hedgerow's PRODUCTION code needs none of this: ``discovery.py`` and
    ``daemon.py`` read the changefeed plan's PER-FILE ``record_count``,
    which is manifest data and unaffected.
    """
    return table.info(snapshot=catalog.refresh().head_snapshot_id)
