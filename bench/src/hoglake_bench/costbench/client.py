"""Small Trino statement-protocol client; timings include all result pages."""

from __future__ import annotations

import hashlib
import json
import math
import ssl
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Self
from urllib.parse import urlsplit

import httpx


def quote_identifier(value: str) -> str:
    if not value or any(ord(c) < 32 for c in value):
        raise ValueError(
            "identifiers must be nonempty and contain no control characters"
        )
    return '"' + value.replace('"', '""') + '"'


def table_name(catalog: str, schema: str, table: str) -> str:
    return ".".join(map(quote_identifier, (catalog, schema, table)))


@dataclass
class QueryResult:
    query_id: str | None
    elapsed_s: float
    server_elapsed_ms: float | None
    stats: dict[str, Any]
    row_count: int
    rows: list[list[Any]] = field(default_factory=list)
    columns: list[dict[str, Any]] = field(default_factory=list)
    result_sha256: str | None = None


class QueryError(RuntimeError):
    """Details are available locally; only ``safe_error`` belongs in artifacts."""

    def __init__(
        self,
        message: str,
        *,
        name: str,
        query_id: str | None = None,
        elapsed_s: float = 0,
        error_type: str | None = None,
        code: int | None = None,
        safe_to_continue: bool = True,
    ) -> None:
        super().__init__(message)
        self.query_id = query_id
        self.elapsed_s = elapsed_s
        self.safe_error = {"name": name, "type": error_type, "code": code}
        self.safe_to_continue = safe_to_continue


QUERY_STATES = {
    "QUEUED",
    "WAITING_FOR_RESOURCES",
    "DISPATCHING",
    "PLANNING",
    "STARTING",
    "RUNNING",
    "FINISHING",
    "FINISHED",
    "FAILED",
}


def public_stats(stats: dict[str, Any]) -> dict[str, Any]:
    """Keep scalar counters; omit nested stage/location and arbitrary text fields."""
    return {
        key: value
        for key, value in stats.items()
        if isinstance(value, (bool, int))
        or isinstance(value, float)
        and math.isfinite(value)
        or key == "state"
        and value in QUERY_STATES
    }


class TrinoClient:
    def __init__(
        self,
        base_url: str,
        *,
        user: str = "benchmark",
        catalog: str = "hoglake",
        schema: str = "costbench",
        timeout_s: float = 1800,
        password: str | None = None,
        ca_file: Path | None = None,
        http_client: httpx.Client | None = None,
    ) -> None:
        parsed = urlsplit(base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("Trino URL must use http or https")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("Trino URL must not embed credentials, query, or fragment")
        if timeout_s <= 0 or not math.isfinite(timeout_s):
            raise ValueError("query timeout must be finite and positive")
        if password is not None and parsed.scheme != "https":
            raise ValueError("password authentication requires HTTPS")
        if ca_file is not None and http_client is not None:
            raise ValueError("configure the CA on a supplied HTTP client directly")
        self.base_url = base_url.rstrip("/")
        self.timeout_s = timeout_s
        self.headers = {
            "X-Trino-User": user,
            "X-Trino-Catalog": catalog,
            "X-Trino-Schema": schema,
            "X-Trino-Time-Zone": "UTC",
            "X-Trino-Source": "hoglake-costbench-2025",
            "X-Trino-Session": f"query_max_run_time={math.ceil(timeout_s)}s",
        }
        self.auth = httpx.BasicAuth(user, password) if password is not None else None
        self.http = http_client or httpx.Client(
            follow_redirects=False,
            verify=ssl.create_default_context(cafile=str(ca_file)) if ca_file else True,
        )
        self._own_http = http_client is None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def close(self) -> None:
        if self._own_http:
            self.http.close()

    def _statement_url(self, uri: str) -> str:
        # Coordinator-advertised hosts can differ behind an SSH/SSM tunnel.
        # Keep the statement path but always use the explicitly configured host.
        parsed = urlsplit(uri)
        if not parsed.path.startswith("/v1/statement/"):
            raise ValueError("unexpected Trino result-page path")
        return (
            self.base_url + parsed.path + ("?" + parsed.query if parsed.query else "")
        )

    def execute(self, sql: str, *, collect_rows: bool = False) -> QueryResult:
        start = time.perf_counter()
        deadline = time.monotonic() + self.timeout_s
        query_id = None
        next_uri = None
        stats: dict[str, Any] = {}
        columns: list[dict[str, Any]] = []
        rows: list[list[Any]] = []
        row_count = 0
        digest = hashlib.sha256()

        def request(method: str, url: str, **kwargs: Any) -> dict[str, Any]:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("query execution deadline reached")
            response = self.http.request(
                method,
                url,
                headers=self.headers,
                auth=self.auth,
                timeout=httpx.Timeout(remaining, connect=min(10.0, remaining)),
                **kwargs,
            )
            response.raise_for_status()
            return response.json()

        try:
            page = request(
                "POST", self.base_url + "/v1/statement", content=sql.encode()
            )
            while True:
                if (
                    not isinstance(page, dict)
                    or not isinstance(page.get("id"), str)
                    or not page["id"]
                ):
                    raise ValueError("Trino response has no valid query id")
                if query_id is not None and page["id"] != query_id:
                    raise ValueError("Trino result page changed the query id")
                query_id = page["id"]
                if (
                    not isinstance(page.get("stats"), dict)
                    or page["stats"].get("state") not in QUERY_STATES
                ):
                    raise ValueError("Trino response has no valid query state")
                stats = page["stats"]
                columns = page.get("columns") or columns
                if error := page.get("error"):
                    if not isinstance(error, dict) or stats["state"] != "FAILED":
                        raise ValueError(
                            "Trino error response is not a terminal failure"
                        )
                    raise QueryError(
                        error.get("message", "Trino query failed"),
                        name=error.get("errorName", "TRINO_ERROR"),
                        error_type=error.get("errorType"),
                        code=error.get("errorCode"),
                    )
                uri = page.get("nextUri")
                if uri is not None and (not isinstance(uri, str) or not uri):
                    raise ValueError("Trino response has an invalid nextUri")
                if uri is None and stats["state"] != "FINISHED":
                    raise ValueError("Trino result stream ended before FINISHED")
                next_uri = uri
                for row in page.get("data") or []:
                    row_count += 1
                    digest.update(json.dumps(row, separators=(",", ":")).encode())
                    digest.update(b"\n")
                    if collect_rows:
                        rows.append(row)
                if not next_uri:
                    break
                page = request("GET", self._statement_url(next_uri))
            # wallTimeMillis is total scheduled task time, which grows with
            # parallelism. Only elapsedTimeMillis represents query latency.
            server_elapsed = stats.get("elapsedTimeMillis")
            return QueryResult(
                query_id=query_id,
                elapsed_s=time.perf_counter() - start,
                server_elapsed_ms=float(server_elapsed)
                if server_elapsed is not None
                else None,
                stats=public_stats(stats),
                row_count=row_count,
                rows=rows,
                columns=columns,
                result_sha256=digest.hexdigest(),
            )
        except (httpx.HTTPError, ValueError, TimeoutError, QueryError) as exc:
            if isinstance(exc, QueryError):
                exc.query_id = query_id
                exc.elapsed_s = time.perf_counter() - start
                raise
            cancellation_confirmed = False
            if next_uri or query_id:
                try:
                    cancel_url = (
                        self._statement_url(next_uri)
                        if next_uri
                        else self.base_url + "/v1/query/" + query_id
                    )
                    cancelled = self.http.delete(
                        cancel_url, headers=self.headers, auth=self.auth, timeout=5
                    )
                    cancelled.raise_for_status()
                    cancellation_confirmed = True
                except (httpx.HTTPError, ValueError):
                    pass
            name = (
                "CLIENT_TIMEOUT"
                if isinstance(exc, (TimeoutError, httpx.TimeoutException))
                else "HTTP_ERROR"
                if isinstance(exc, httpx.HTTPError)
                else "PROTOCOL_ERROR"
            )
            raise QueryError(
                str(exc),
                name=name,
                query_id=query_id,
                elapsed_s=time.perf_counter() - start,
                safe_to_continue=cancellation_confirmed,
            ) from exc
