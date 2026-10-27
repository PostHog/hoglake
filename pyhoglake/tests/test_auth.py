"""The client's side of hoglake authentication, against in-process fakes."""

from __future__ import annotations

import base64
import threading
import types

import httpx
import pytest

from pyhoglake import BearerToken, ClientCredentials, HoglakeClient, HoglakeError
from pyhoglake.client import USER_AGENT


class Fakes:
    """A token endpoint and a hoglake that accepts exactly one token."""

    def __init__(self, expires_in: int = 900) -> None:
        self.expires_in = expires_in
        self.mints = 0
        self.accepted = "t1"
        self.token_status = 200
        self.token_requests: list[httpx.Request] = []
        self.api_auth: list[str | None] = []

    def token(self, request: httpx.Request) -> httpx.Response:
        self.token_requests.append(request)
        if self.token_status != 200:
            return httpx.Response(self.token_status, json={"error": "invalid_client"})
        self.mints += 1
        return httpx.Response(
            200,
            json={
                "access_token": f"t{self.mints}",
                "token_type": "Bearer",
                "expires_in": self.expires_in,
            },
        )

    def api(self, request: httpx.Request) -> httpx.Response:
        auth = request.headers.get("authorization")
        self.api_auth.append(auth)
        assert request.headers["user-agent"] == USER_AGENT
        if auth != f"Bearer {self.accepted}":
            return httpx.Response(
                401,
                json={"error": "unknown_key"},
                headers={"WWW-Authenticate": 'Bearer error="unknown_key"'},
            )
        return httpx.Response(200, json=[])

    def credentials(self) -> ClientCredentials:
        return ClientCredentials(
            "http://issuer/oauth/token",
            "hlc_test",
            "s3cret",
            http=httpx.Client(transport=httpx.MockTransport(self.token)),
        )

    def client(self) -> HoglakeClient:
        client = HoglakeClient("http://hoglake", auth=self.credentials())
        client._http._transport = httpx.MockTransport(self.api)
        return client


def test_a_minted_token_is_attached_and_reused() -> None:
    fakes = Fakes()
    with fakes.client() as client:
        client._request("GET", "/catalogs")
        client._request("GET", "/catalogs")
    assert fakes.mints == 1
    assert fakes.api_auth == ["Bearer t1", "Bearer t1"]
    minted = fakes.token_requests[0]
    assert (
        minted.headers["authorization"]
        == "Basic " + base64.b64encode(b"hlc_test:s3cret").decode()
    )
    assert minted.content == b"grant_type=client_credentials&audience=hoglake"


def test_a_refused_token_is_replaced_once_and_the_request_retried() -> None:
    fakes = Fakes()
    with fakes.client() as client:
        client._request("GET", "/catalogs")
        fakes.accepted = "t2"
        client._request("GET", "/catalogs")
    assert fakes.api_auth == ["Bearer t1", "Bearer t1", "Bearer t2"]


def test_a_second_401_is_not_retried_again() -> None:
    fakes = Fakes()
    fakes.accepted = "never"
    with fakes.client() as client, pytest.raises(HoglakeError):
        client._request("GET", "/catalogs")
    assert len(fakes.api_auth) == 2


def test_a_failing_token_endpoint_raises_without_calling_the_api() -> None:
    fakes = Fakes()
    fakes.token_status = 401
    with fakes.client() as client, pytest.raises(HoglakeError, match="token endpoint"):
        client._request("GET", "/catalogs")
    assert fakes.api_auth == []


def test_a_fixed_bearer_token() -> None:
    fakes = Fakes()
    client = HoglakeClient("http://hoglake", auth=BearerToken("t1"))
    client._http._transport = httpx.MockTransport(fakes.api)
    with client:
        client._request("GET", "/catalogs")
    assert fakes.api_auth == ["Bearer t1"]


class Clock:
    """Stands in for ``time`` inside pyhoglake.auth, which only reads monotonic()."""

    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    fake = Clock()
    monkeypatch.setattr(
        "pyhoglake.auth.time", types.SimpleNamespace(monotonic=fake.monotonic)
    )
    return fake


@pytest.mark.parametrize(
    ("expires_in", "refresh_after"),
    [
        # Long-lived: refreshed 60s before expiry.
        (900, 840.0),
        # Short-lived: the margin is capped at a fifth of the lifetime.
        (100, 80.0),
    ],
)
def test_a_token_is_reused_until_its_refresh_point_then_reminted(
    clock: Clock, expires_in: int, refresh_after: float
) -> None:
    fakes = Fakes(expires_in=expires_in)
    creds = fakes.credentials()
    assert creds._current() == "t1"
    clock.now += refresh_after - 0.001
    assert creds._current() == "t1"
    clock.now += 0.001
    assert creds._current() == "t2"
    assert fakes.mints == 2


def test_threads_that_find_the_token_stale_together_mint_once() -> None:
    threads = 8
    fakes = Fakes()
    creds = fakes.credentials()

    # Count arrivals at the lock, and hold the one mint until every thread has
    # arrived, so all of them have seen the token missing before it exists.
    real_lock = creds._lock
    arrivals = threading.Semaphore(0)

    class CountingLock:
        def __enter__(self) -> None:
            arrivals.release()
            real_lock.acquire()

        def __exit__(self, *exc: object) -> None:
            real_lock.release()

    creds._lock = CountingLock()  # type: ignore[assignment]
    mint = fakes.token

    def held_mint(request: httpx.Request) -> httpx.Response:
        if fakes.mints == 0:
            for _ in range(threads):
                assert arrivals.acquire(timeout=5), "not every thread reached the lock"
        return mint(request)

    fakes.token = held_mint  # type: ignore[method-assign]
    creds._http = httpx.Client(transport=httpx.MockTransport(fakes.token))

    tokens: list[str] = []
    workers = [
        threading.Thread(target=lambda: tokens.append(creds._current()))
        for _ in range(threads)
    ]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=10)
    assert tokens == ["t1"] * threads
    assert fakes.mints == 1
