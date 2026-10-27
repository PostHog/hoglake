"""The client's side of hoglake authentication, against in-process fakes."""

from __future__ import annotations

import base64

import httpx
import pytest

from pyhoglake import BearerToken, ClientCredentials, HoglakeClient, HoglakeError
from pyhoglake.client import USER_AGENT


class Fakes:
    """A token endpoint and a hoglake that accepts exactly one token."""

    def __init__(self) -> None:
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
                "expires_in": 900,
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

    def client(self) -> HoglakeClient:
        creds = ClientCredentials(
            "http://issuer/oauth/token",
            "hlc_test",
            "s3cret",
            http=httpx.Client(transport=httpx.MockTransport(self.token)),
        )
        client = HoglakeClient("http://hoglake", auth=creds)
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
