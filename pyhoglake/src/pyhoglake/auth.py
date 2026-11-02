"""Bearer authentication for the hoglake API.

hoglake verifies tokens; it does not mint them. The issuer is the
managed-warehouse control plane (hogtower's ``POST /oauth/token``), and a
client authenticates to it with the client id and secret it was given.
Pass one of these as ``HoglakeClient(..., auth=...)``.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Generator

import httpx

from pyhoglake.errors import HoglakeError

__all__ = ["BearerToken", "ClientCredentials"]


class BearerToken(httpx.Auth):
    """A fixed access token, for scripts that already hold one.

    No refresh: when it expires, hoglake answers 401 and the call raises.
    """

    def __init__(self, token: str) -> None:
        self._token = token

    def auth_flow(
        self, request: httpx.Request
    ) -> Generator[httpx.Request, httpx.Response, None]:
        request.headers["Authorization"] = f"Bearer {self._token}"
        yield request


class ClientCredentials(httpx.Auth):
    """RFC 6749 client credentials, cached until shortly before expiry.

    A 401 from hoglake replaces the token once and retries the request.
    That is safe for writes too: hoglake decides authentication before any
    handler runs, so a 401 means the request did nothing, and the causes a
    fresh token fixes — revocation, a signing key the server has not loaded
    yet, early expiry by clock skew — are the ones that produce it.
    """

    # Refresh this far before expiry, capped at a fifth of the lifetime.
    REFRESH_MARGIN = 60.0

    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        *,
        audience: str = "hoglake",
        timeout: float = 10.0,
        http: httpx.Client | None = None,
    ) -> None:
        self._token_url = token_url
        self._credentials = (client_id, client_secret)
        self._audience = audience
        self._timeout = timeout
        # The client tokens are minted with; a fresh one per mint by default.
        self._http = http
        self._lock = threading.Lock()
        self._token: str | None = None
        self._refresh_at = 0.0

    def auth_flow(
        self, request: httpx.Request
    ) -> Generator[httpx.Request, httpx.Response, None]:
        token = self._current()
        request.headers["Authorization"] = f"Bearer {token}"
        response = yield request
        if response.status_code != 401:
            return
        self._invalidate(token)
        request.headers["Authorization"] = f"Bearer {self._current()}"
        yield request

    def _current(self) -> str:
        # One lock, so threads that all find the token stale mint once.
        with self._lock:
            if self._token is None or time.monotonic() >= self._refresh_at:
                self._mint()
            assert self._token is not None
            return self._token

    def _invalidate(self, token: str) -> None:
        with self._lock:
            if self._token == token:
                self._token = None

    def _mint(self) -> None:
        # Deferred: client.py imports this module.
        from pyhoglake.client import USER_AGENT

        try:
            resp = (self._http or httpx).post(
                self._token_url,
                auth=self._credentials,
                data={"grant_type": "client_credentials", "audience": self._audience},
                headers={"Accept": "application/json", "User-Agent": USER_AGENT},
                timeout=self._timeout,
            )
        except httpx.HTTPError as e:
            raise HoglakeError(
                f"could not reach the hoglake token endpoint {self._token_url}: {e}"
            ) from e
        if resp.status_code != 200:
            # An RFC 6749 §5.2 error object: its `error` tells a bad secret
            # (invalid_client) from an unavailable issuer.
            raise HoglakeError(
                f"hoglake token endpoint answered HTTP {resp.status_code}",
                status_code=resp.status_code,
                detail=resp.text,
            )
        body = resp.json()
        token = body.get("access_token")
        expires_in = float(body.get("expires_in") or 0)
        if not token or expires_in <= 0:
            raise HoglakeError("hoglake token endpoint returned no usable token")
        self._token = token
        self._refresh_at = (
            time.monotonic() + expires_in - min(self.REFRESH_MARGIN, expires_in / 5)
        )
