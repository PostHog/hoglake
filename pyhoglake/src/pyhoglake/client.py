"""The hoglake client: a thin wrapper over the control-plane REST API.

Data never flows through the server: ``Table.append`` writes parquet to
object storage itself (pyarrow S3FileSystem) and registers the file with
footer-derived stats via the commit endpoint (footer-shipping commits).
"""

from __future__ import annotations

import contextlib
import io
import struct
import uuid as _uuid
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Self
from urllib.parse import quote

import httpx
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .errors import (
    AlreadyExistsError,
    CommitConflictError,
    ExpiredError,
    HoglakeError,
    IncarnationChangedError,
    NotFoundError,
    OffsetRegressionError,
    ValidationError,
)
from .models import (
    AppendedFile,
    AppendResult,
    CatalogInfo,
    CatalogOptions,
    ChangesPlan,
    CleanupResult,
    Column,
    CommitResult,
    ConsumerOffset,
    DataFile,
    ExpiryResult,
    PartitionSpec,
    ScanFile,
    Snapshot,
    TableInfo,
    TableSummary,
    ViewInfo,
)
from .ops import AlterOp
from .parquet_schema import validate_variant_file
from .stats import extract_column_stats
from .transforms import partition_source_array, transform_strings
from .types import columns_to_arrow_schema, is_list_family, schema_to_column_defs

DEFAULT_TIMEOUT = 30.0

# Sentinel for Table.append(expected_table_uuid=...): opt out of the
# incarnation guard entirely — no pre-flight uuid check, and the commit
# carries no expected_table_uuid field (name-only resolution).
UNGUARDED = object()

# The server's commit 409 for a mismatched expected_table_uuid carries
# this phrase in its ApiError message/detail; it is how the client tells
# a recreation refusal (IncarnationChangedError, never retryable) from an
# ordinary commit conflict (CommitConflictError, retryable).
_RECREATED_MARKER = "the table was recreated"

# COLUMN names starting with this prefix are reserved for hoglake internals
# (``_hog_row_id`` is compaction's row-id carrier). The SERVER enforces
# this too, at every nesting level (Identifiers.validateColumn, reached
# from ColumnTrees for create/add and directly for rename) — it did not
# when this check was written, which is what hoglake#36 was about. The
# client check stays as a FAST FAIL: it refuses before the parquet upload
# and the request, and the message names every offending path at once
# instead of the first one the server trips on.
# Namespace/table/view names are NOT affected.
_RESERVED_COLUMN_PREFIX = "_hog"


def _reserved_field_paths(fields: object, prefix: str = "") -> list[str]:
    """Dotted paths of every field — at ANY nesting level — whose name
    uses the reserved prefix.

    Recursive because the reserved name is reserved everywhere: a
    ``_hog_row_id`` field inside a struct reaches parquet exactly like a
    top-level one, and compaction writes its own ``_hog_row_id`` at the
    top level of the OUTPUT, so a nested collision is a name clash in
    waiting rather than a present one. Refusing both is cheaper than
    explaining the difference.
    """
    out: list[str] = []
    for f in fields:  # type: ignore[attr-defined]
        path = f"{prefix}.{f.name}" if prefix else f.name
        if f.name.startswith(_RESERVED_COLUMN_PREFIX):
            out.append(path)
        t = f.type
        if pa.types.is_struct(t):
            out += _reserved_field_paths(
                [t.field(i) for i in range(t.num_fields)], path
            )
        elif pa.types.is_map(t):
            out += _reserved_field_paths([t.key_field, t.item_field], path)
        elif is_list_family(t):
            out += _reserved_field_paths([t.value_field], path)
    return out


def _check_reserved_columns(schema: pa.Schema) -> None:
    """Fast-fail schema field names using the reserved ``_hog`` column
    prefix BEFORE any request or parquet upload.

    The server refuses these as well (see [_RESERVED_COLUMN_PREFIX]), so
    this is an optimisation rather than the only barrier it once was — it
    saves an upload, and reports every offending path together instead of
    one per round trip.

    Checked at every nesting level, which is the level the server checks
    at too."""
    reserved = _reserved_field_paths(list(schema))
    if reserved:
        # WORDING MATCHES THE SERVER'S on purpose: "uses the reserved
        # prefix '_hog'" is the phrase Identifiers.validateColumn raises,
        # so a user who hits the local check and a user who hits the 422
        # can search for the same string and find the same answer. The
        # only deliberate difference is plurality — this one reports
        # EVERY offending path at once, which is the point of checking
        # before the request.
        raise ValidationError(
            f"column name(s) {reserved} use the reserved prefix "
            f"'{_RESERVED_COLUMN_PREFIX}': names starting with it belong to "
            "hoglake's own physical columns (compaction's _hog_row_id); "
            "rename them",
            status_code=None,
        )


def _seg(name: object) -> str:
    """Percent-encode one URL path segment. Identifiers are user data:
    a table named "a/b" or "a?x" must stay inside its segment, not
    rewrite the route (QE find, 2026-09-05)."""
    return quote(str(name), safe="")


def _record_uploads(error: BaseException, uploaded: Sequence[str]) -> None:
    """Stamp completed-upload progress on an exception before re-raise.

    ``uploaded_files`` / ``uploaded_uris`` are set on the exception the
    caller already catches rather than wrapped in a new type: pyhoglake
    is published, and consumers catch ``ValidationError`` /
    ``HoglakeError`` / ``OSError`` today. No existing type changes, and
    ``getattr(error, "uploaded_files", 0)`` reads correctly on any
    exception — including one raised before the first upload, where
    these are 0 and ``()``.

    Kept out of the message on purpose: a wide fanout would otherwise
    put a few hundred uris into every log line that formats the error.

    Best-effort: an exception type that refuses attributes (``__slots__``
    on a third-party error) must never turn a real object-store failure
    into an ``AttributeError`` from the annotation.
    """
    with contextlib.suppress(AttributeError, TypeError):
        error.uploaded_files = len(uploaded)  # type: ignore[attr-defined]
        error.uploaded_uris = tuple(uploaded)  # type: ignore[attr-defined]


@dataclass
class S3Config:
    """Object-store connection settings for the parquet write path.

    ``endpoint_override`` may carry the scheme (``http://localhost:9000``);
    path-style addressing is used when an endpoint override is set.
    """

    access_key: str | None = None
    secret_key: str | None = None
    endpoint_override: str | None = None
    region: str | None = None
    allow_bucket_creation: bool = False

    def filesystem(self):
        from pyarrow import fs

        kwargs: dict[str, Any] = {}
        if self.access_key is not None:
            kwargs["access_key"] = self.access_key
        if self.secret_key is not None:
            kwargs["secret_key"] = self.secret_key
        if self.endpoint_override is not None:
            kwargs["endpoint_override"] = self.endpoint_override
        if self.region is not None:
            kwargs["region"] = self.region
        if self.allow_bucket_creation:
            kwargs["allow_bucket_creation"] = True
        return fs.S3FileSystem(**kwargs)


def _ts_param(value: datetime | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            # the server requires an ISO-8601 instant (with offset);
            # naive datetimes are taken as UTC

            value = value.replace(tzinfo=UTC)
        return value.isoformat()
    return value


def _travel_params(
    snapshot: int | None, at_timestamp: datetime | str | None
) -> dict[str, Any]:
    if snapshot is not None and at_timestamp is not None:
        raise ValueError("snapshot and at_timestamp are mutually exclusive")
    params: dict[str, Any] = {}
    if snapshot is not None:
        params["snapshot"] = snapshot
    if at_timestamp is not None:
        params["at_timestamp"] = _ts_param(at_timestamp)
    return params


class HoglakeClient:
    """Entry point. ``base_url`` is the server root (``/v1`` is appended)."""

    def __init__(
        self,
        base_url: str,
        *,
        s3: S3Config | None = None,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.s3 = s3
        self._http = httpx.Client(base_url=self.base_url + "/v1", timeout=timeout)
        self._fs = None

    # -- lifecycle ---------------------------------------------------------

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- transport ---------------------------------------------------------

    def _request(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        params: dict[str, Any] | None = None,
        conflict: type[HoglakeError] = AlreadyExistsError,
    ) -> Any:
        if params:
            params = {k: v for k, v in params.items() if v is not None}
        resp = self._http.request(method, path, json=json, params=params or None)
        if resp.status_code < 300:
            if not resp.content:
                return None
            return resp.json()
        if resp.status_code < 400:
            # Redirects are not followed (httpx default) and the hoglake
            # API never issues them: treating a 3xx as success would feed
            # an empty/HTML body to resp.json() and leak a raw
            # JSONDecodeError outside the error taxonomy (bugs.md #19).
            location = resp.headers.get("location")
            raise HoglakeError(
                f"unexpected redirect HTTP {resp.status_code} from the "
                "hoglake API (redirects are not followed; check base_url)",
                status_code=resp.status_code,
                detail=f"Location: {location}" if location else None,
            )
        self._raise(resp, conflict)

    @staticmethod
    def _raise(resp: httpx.Response, conflict: type[HoglakeError]) -> None:
        message = f"HTTP {resp.status_code}"
        detail = None
        try:
            body = resp.json()
            if isinstance(body, dict):
                message = body.get("error", message)
                detail = body.get("detail")
        except Exception:  # noqa: BLE001  # any body-parse failure falls back to raw text
            detail = resp.text[:500] or None
        cls: type[HoglakeError]
        if resp.status_code == 404:
            cls = NotFoundError
        elif resp.status_code == 409:
            cls = conflict
        elif resp.status_code == 410:
            cls = ExpiredError
        elif resp.status_code == 422:
            cls = ValidationError
        else:
            cls = HoglakeError
        raise cls(message, status_code=resp.status_code, detail=detail)

    def _filesystem(self):
        if self._fs is None:
            if self.s3 is None:
                raise HoglakeError(
                    "no S3 configuration: pass s3=S3Config(...) to "
                    "HoglakeClient to enable the parquet write path"
                )
            self._fs = self.s3.filesystem()
        return self._fs

    # -- catalogs ----------------------------------------------------------

    def create_catalog(self, name: str, data_path: str) -> Catalog:
        body = self._request(
            "POST", "/catalogs", json={"name": name, "data_path": data_path}
        )
        return Catalog(self, CatalogInfo.from_wire(body))

    def catalog(self, name: str) -> Catalog:
        body = self._request("GET", f"/catalogs/{_seg(name)}")
        return Catalog(self, CatalogInfo.from_wire(body))

    def list_catalogs(self) -> list[CatalogInfo]:
        body = self._request("GET", "/catalogs")
        return [CatalogInfo.from_wire(c) for c in body]


class Catalog:
    def __init__(self, client: HoglakeClient, info: CatalogInfo) -> None:
        self._client = client
        self._info = info

    # -- identity ----------------------------------------------------------

    @property
    def name(self) -> str:
        return self._info.name

    @property
    def data_path(self) -> str:
        return self._info.data_path

    @property
    def info(self) -> CatalogInfo:
        return self._info

    def refresh(self) -> CatalogInfo:
        body = self._client._request("GET", f"/catalogs/{_seg(self.name)}")
        self._info = CatalogInfo.from_wire(body)
        return self._info

    def _path(self, suffix: str = "") -> str:
        return f"/catalogs/{_seg(self.name)}{suffix}"

    def __repr__(self) -> str:  # pragma: no cover
        return f"<Catalog {self.name!r} data_path={self.data_path!r}>"

    # -- namespaces --------------------------------------------------------

    def create_namespace(self, name: str) -> Namespace:
        self._client._request("POST", self._path("/namespaces"), json={"name": name})
        return Namespace(self, name)

    def namespace(self, name: str) -> Namespace:
        # No per-namespace GET in the API; existence-check via the listing.
        if name not in self.list_namespaces():
            raise NotFoundError(
                f"namespace {name!r} not found in catalog {self.name!r}",
                status_code=404,
            )
        return Namespace(self, name)

    def list_namespaces(self) -> list[str]:
        body = self._client._request("GET", self._path("/namespaces"))
        return [ns["name"] for ns in body]

    # -- snapshots ---------------------------------------------------------

    def snapshots(
        self, after: int = 0, before: int | None = None, limit: int = 1000
    ) -> Iterator[Snapshot]:
        """Iterate snapshots; auto-paginates on has_more.

        Ascending (default): id > ``after``, oldest first. Descending:
        pass ``before`` to walk id < ``before``, newest first (a UI pages
        down from head + 1 with the last id of each page). ``before`` is
        mutually exclusive with a non-zero ``after`` — enforced here with
        a ValueError before any request (the server 422s the same
        combination).
        """
        if before is not None and after != 0:
            # eager (not deferred to first iteration): the bad call fails
            # where it is written, before any request
            raise ValueError(
                "snapshots(): 'before' and a non-zero 'after' are mutually "
                "exclusive cursors — pass exactly one (ascending walks use "
                "'after', descending walks use 'before')"
            )
        return self._snapshot_pages(
            "after" if before is None else "before",
            after if before is None else before,
            limit,
        )

    def _snapshot_pages(self, key: str, cursor: int, limit: int) -> Iterator[Snapshot]:
        while True:
            body = self._client._request(
                "GET",
                self._path("/snapshots"),
                params={key: cursor, "limit": limit},
            )
            snaps = body.get("snapshots") or []
            for s in snaps:
                yield Snapshot.from_wire(s)
            if not body.get("has_more") or not snaps:
                return
            cursor = snaps[-1]["snapshot_id"]

    # -- options / retention ----------------------------------------------

    def options(self) -> CatalogOptions:
        body = self._client._request("GET", self._path("/options"))
        return CatalogOptions.from_wire(body)

    def set_retention(
        self, seconds: int | None, consumer_floor: bool | None = None
    ) -> CatalogOptions:
        payload: dict[str, Any] = {"snapshot_retention_seconds": seconds}
        if consumer_floor is not None:
            payload["consumer_floor"] = consumer_floor
        body = self._client._request("PATCH", self._path("/options"), json=payload)
        return CatalogOptions.from_wire(body)

    # -- maintenance -------------------------------------------------------

    def expire(self, batch: int | None = None) -> ExpiryResult:
        body = self._client._request(
            "POST", self._path("/maintenance/expire"), params={"batch": batch}
        )
        return ExpiryResult.from_wire(body)

    def cleanup(self, batch: int | None = None) -> CleanupResult:
        body = self._client._request(
            "POST", self._path("/maintenance/cleanup"), params={"batch": batch}
        )
        return CleanupResult.from_wire(body)

    # -- consumer offsets --------------------------------------------------

    def commit_offset(
        self, consumer_id: str, table_uuid: str, snapshot_id: int
    ) -> ConsumerOffset:
        body = self._client._request(
            "PUT",
            self._path(f"/consumers/{_seg(consumer_id)}/offsets/{_seg(table_uuid)}"),
            json={"snapshot_id": snapshot_id},
            conflict=OffsetRegressionError,
        )
        return ConsumerOffset.from_wire(body)

    def offsets(self, consumer_id: str) -> list[ConsumerOffset]:
        body = self._client._request(
            "GET", self._path(f"/consumers/{_seg(consumer_id)}/offsets")
        )
        return [ConsumerOffset.from_wire(o) for o in body]

    def offset(self, consumer_id: str, table_uuid: str) -> ConsumerOffset | None:
        """One (consumer, table_uuid) offset, or None when the server has
        none stored (404).

        Deliberate divergence from the client's raise-on-404 idiom: an
        absent offset is the routine state of every consumer that has not
        committed yet, not an error condition.
        """
        try:
            body = self._client._request(
                "GET",
                self._path(
                    f"/consumers/{_seg(consumer_id)}/offsets/{_seg(table_uuid)}"
                ),
            )
        except NotFoundError:
            return None
        return ConsumerOffset.from_wire(body)

    # -- commit (internal; Table.append is the public writer path) ---------

    def commit_prepared(self, payload: dict[str, Any]) -> CommitResult:
        """Publish a durably saved request; retry the EXACT payload on uncertainty.

        Requires a server supporting CommitRequest.idempotency_key (V7 migration).
        This API does not apply event-level deduplication.
        """
        if not payload.get("idempotency_key"):
            raise ValueError("prepared commits require an idempotency_key")
        _uuid.UUID(payload["idempotency_key"])
        return self._commit(payload, prepared=True)

    def _commit(
        self, payload: dict[str, Any], *, prepared: bool = False
    ) -> CommitResult:
        try:
            body = self._client._request(
                "POST",
                self._path("/commit/prepared" if prepared else "/commit"),
                json=payload,
                conflict=CommitConflictError,
            )
        except CommitConflictError as e:
            # Discriminate the expected_table_uuid guard's 409 from an
            # ordinary (retryable) commit conflict: the server's
            # recreation refusal says "the table was recreated" in its
            # message/detail. That refusal is atomic (zero writes) and
            # never retryable — surface it as IncarnationChangedError.
            text = f"{e.message} {e.detail or ''}".lower()
            if _RECREATED_MARKER in text:
                raise IncarnationChangedError(
                    e.message, status_code=e.status_code, detail=e.detail
                ) from e
            raise
        return CommitResult.from_wire(body)


class Namespace:
    def __init__(self, catalog: Catalog, name: str) -> None:
        self._catalog = catalog
        self.name = name

    def _path(self, suffix: str = "") -> str:
        return self._catalog._path(f"/namespaces/{_seg(self.name)}{suffix}")

    def __repr__(self) -> str:  # pragma: no cover
        return f"<Namespace {self._catalog.name}.{self.name}>"

    # -- tables ------------------------------------------------------------

    def create_table(self, name: str, schema: pa.Schema) -> Table:
        _check_reserved_columns(schema)
        body = self._catalog._client._request(
            "POST",
            self._path("/tables"),
            json={"name": name, "columns": schema_to_column_defs(schema)},
        )
        return Table(self, TableInfo.from_wire(body))

    def table(self, name: str) -> Table:
        body = self._catalog._client._request(
            "GET", self._path(f"/tables/{_seg(name)}")
        )
        return Table(self, TableInfo.from_wire(body))

    def list_tables(self) -> list[TableSummary]:
        body = self._catalog._client._request("GET", self._path("/tables"))
        return [TableSummary.from_wire(t) for t in body]

    # -- views -------------------------------------------------------------

    def create_view(self, name: str, sql: str, dialect: str = "trino") -> View:
        body = self._catalog._client._request(
            "POST",
            self._path("/views"),
            json={"name": name, "sql": sql, "dialect": dialect},
        )
        return View(self, ViewInfo.from_wire(body))

    def view(self, name: str) -> View:
        body = self._catalog._client._request("GET", self._path(f"/views/{_seg(name)}"))
        return View(self, ViewInfo.from_wire(body))

    def list_views(self) -> list[ViewInfo]:
        body = self._catalog._client._request("GET", self._path("/views"))
        return [ViewInfo.from_wire(v) for v in body]


class View:
    def __init__(self, namespace: Namespace, info: ViewInfo) -> None:
        self._namespace = namespace
        self._info = info

    @property
    def name(self) -> str:
        return self._info.name

    @property
    def sql(self) -> str:
        return self._info.sql

    @property
    def dialect(self) -> str:
        return self._info.dialect

    @property
    def view_uuid(self) -> str:
        return self._info.view_uuid

    @property
    def info(self) -> ViewInfo:
        return self._info

    def drop(self) -> CommitResult:
        body = self._namespace._catalog._client._request(
            "DELETE", self._namespace._path(f"/views/{_seg(self.name)}")
        )
        return CommitResult.from_wire(body)


class Table:
    def __init__(self, namespace: Namespace, info: TableInfo) -> None:
        self._namespace = namespace
        self._info = info

    # -- identity ----------------------------------------------------------

    @property
    def name(self) -> str:
        return self._info.name

    @property
    def namespace(self) -> str:
        return self._namespace.name

    @property
    def table_uuid(self) -> str:
        return self._info.table_uuid

    @property
    def columns(self):
        return self._info.columns

    def _path(self, suffix: str = "") -> str:
        return self._namespace._path(f"/tables/{_seg(self._info.name)}{suffix}")

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"<Table {self._namespace._catalog.name}.{self.namespace}."
            f"{self.name} uuid={self.table_uuid}>"
        )

    # -- reads -------------------------------------------------------------

    def info(
        self,
        snapshot: int | None = None,
        at_timestamp: datetime | str | None = None,
    ) -> TableInfo:
        body = self._namespace._catalog._client._request(
            "GET", self._path(), params=_travel_params(snapshot, at_timestamp)
        )
        info = TableInfo.from_wire(body)
        if snapshot is None and at_timestamp is None:
            self._info = info
        return info

    def files(
        self,
        snapshot: int | None = None,
        at_timestamp: datetime | str | None = None,
    ) -> list[DataFile]:
        body = self._namespace._catalog._client._request(
            "GET",
            self._path("/files"),
            params=_travel_params(snapshot, at_timestamp),
        )
        return [DataFile.from_wire(f) for f in body]

    def scan_plan(
        self,
        snapshot: int | None = None,
        at_timestamp: datetime | str | None = None,
    ) -> list[ScanFile]:
        body = self._namespace._catalog._client._request(
            "GET",
            self._path("/scan"),
            params=_travel_params(snapshot, at_timestamp),
        )
        return [ScanFile.from_wire(f) for f in body]

    def changes(
        self, from_snapshot: int, to_snapshot: int | None = None
    ) -> ChangesPlan:
        """Changefeed plan for rows appended in (from_snapshot, to_snapshot].

        Raises :class:`ExpiredError` (410) when part of the range has been
        expired — reconcile from a full scan.
        """
        body = self._namespace._catalog._client._request(
            "GET",
            self._path("/changes"),
            params={"from_snapshot": from_snapshot, "to_snapshot": to_snapshot},
        )
        return ChangesPlan.from_wire(body)

    # -- DDL ---------------------------------------------------------------

    def alter(self, ops: list[AlterOp]) -> TableInfo:
        body = self._namespace._catalog._client._request(
            "POST",
            self._path("/alter"),
            json={"ops": [op.to_wire() for op in ops]},
            conflict=CommitConflictError,
        )
        self._info = TableInfo.from_wire(body)
        return self._info

    def drop(self) -> CommitResult:
        body = self._namespace._catalog._client._request("DELETE", self._path())
        return CommitResult.from_wire(body)

    # -- THE writer path ---------------------------------------------------

    def append(
        self,
        data: pa.Table,
        *,
        expected_table_uuid: _uuid.UUID | str | object | None = None,
        deferred_stats: bool = False,
        read_snapshot: int | None = None,
        author: str | None = None,
        message: str | None = None,
        row_group_size: int | None = None,
    ) -> AppendResult:
        """Write ``data`` to the catalog's data path and register it via a
        footer-shipping commit: one parquet file for an unpartitioned
        table, or — when the table has a live partition spec — one file
        per distinct partition tuple (fanout), all registered in ONE
        atomic commit.

        The parquet schema carries the catalog's field ids
        (``PARQUET:field_id``). Unless ``deferred_stats``, per-column stats
        are extracted from the writer's own footer metadata (never re-read
        from object storage) and shipped with the commit.

        **Partitioned tables.** Each row's partition tuple is computed
        client-side under the table's CURRENT spec (Iceberg-semantics
        transforms — see :mod:`pyhoglake.transforms`); rows with a null
        source value land in a null partition group, per Iceberg. Every
        registered file carries its ``partition_values`` (transformed
        values as wire strings, by key_index), which the returned
        :class:`AppendResult` exposes per file. The spec used is the one
        the pre-flight resolve returned; if the spec changes between that
        resolve and the commit, the server refuses the commit itself
        (409 -> :class:`CommitConflictError` for concurrent DDL, 422 ->
        :class:`ValidationError` for an arity mismatch) — the client
        never silently re-specs. A refused commit orphans the uploaded
        parquet files (cleanup's problem, never the catalog's).

        **Incarnation guard — atomic at commit.** The commit payload is
        addressed by (namespace, table) NAME, so it lands on whatever
        table currently holds that name. ``expected_table_uuid``
        (default: the ``table_uuid`` this ``Table`` object was resolved
        as; pass one explicitly to pin a specific incarnation) is shipped
        ON the commit body, and the server rejects the whole commit with
        409 — atomically, zero writes — when the live table's uuid
        differs (drop + recreate under the same name). The client maps
        that refusal to :class:`IncarnationChangedError`; ordinary commit
        conflicts stay :class:`CommitConflictError` (retryable).

        A single cheap pre-flight re-resolve runs before the parquet
        upload as an optimization only (fast-fail on an already-dead
        incarnation saves the S3 write); the server-side guard is the
        safety mechanism. A commit-time refusal orphans the uploaded
        parquet (cleanup's problem, never the catalog's).

        Pass ``expected_table_uuid=pyhoglake.UNGUARDED`` to opt out: no
        pre-flight uuid check, and the commit carries no
        ``expected_table_uuid`` field (name-only resolution).
        """
        catalog = self._namespace._catalog
        client = catalog._client

        # Reserved-prefix fast-fail before ANY request or upload: a user
        # `_hog*` field could otherwise reach parquet on a pre-reservation
        # table and waste the S3 write.
        _check_reserved_columns(data.schema)

        if expected_table_uuid is UNGUARDED:
            expected = None
        elif expected_table_uuid is None:
            expected = self._info.table_uuid
        else:
            expected = str(expected_table_uuid)

        if expected is not None:
            # Pre-flight fast-fail (optimization, not the guarantee):
            # re-resolve by name before paying for the parquet upload.
            info = self._check_incarnation(expected)  # current columns + spec
        else:
            info = self.info()  # UNGUARDED: name-only resolution

        target_schema = columns_to_arrow_schema(info.columns)
        data = _align_table(data, target_schema)

        groups: Sequence[tuple[tuple[str | None, ...] | None, pa.Table]]
        spec = info.partition_spec
        if spec is not None and spec.fields:
            if data.num_rows == 0:
                raise ValidationError(
                    f"cannot append 0 rows to partitioned table "
                    f"{self.namespace}.{self.name}: no partition tuple is "
                    "derivable and a commit registers at least one file",
                    status_code=None,
                )
            groups = _partition_groups(data, info, spec)
        else:
            groups = [(None, data)]

        file_regs: list[dict[str, Any]] = []
        appended: list[AppendedFile] = []
        for partition_values, part in groups:
            reg = _write_one_file(
                part,
                info,
                catalog,
                client,
                deferred_stats=deferred_stats,
                row_group_size=row_group_size,
                partition_values=partition_values,
            )
            file_regs.append(reg)
            appended.append(
                AppendedFile(
                    path=reg["path"],
                    record_count=reg["record_count"],
                    partition_values=partition_values,
                )
            )

        append_entry: dict[str, Any] = {
            "namespace": self.namespace,
            "table": self.name,
            "files": file_regs,
        }
        if expected is not None:
            # The atomic guard: the server 409s the whole commit (zero
            # writes) when the live table's uuid differs.
            append_entry["expected_table_uuid"] = expected

        payload: dict[str, Any] = {"appends": [append_entry]}
        if read_snapshot is not None:
            payload["read_snapshot"] = read_snapshot
        if author is not None:
            payload["author"] = author
        if message is not None:
            payload["message"] = message

        # No second re-resolve: the server enforces expected_table_uuid
        # atomically at commit time (409, zero writes), superseding the
        # old post-upload check. A refusal orphans the uploaded parquet
        # (cleanup's problem, never the catalog's).
        result = catalog._commit(payload)
        return AppendResult(
            snapshot_id=result.snapshot_id,
            schema_version=result.schema_version,
            files=tuple(appended),
        )

    def prepare_append_files(
        self,
        files: Sequence[tuple[str, tuple[str | None, ...] | None]],
        *,
        idempotency_key: str,
        expected_table_uuid: str | None = None,
        expected_table_info: TableInfo | None = None,
        allow_optional_fields: bool = False,
    ) -> dict[str, Any]:
        """Upload already partitioned/sorted local Parquet without loading it in RAM.

        The caller owns row-to-partition correctness and sort order, exactly as
        other footer-shipping writers do. Field IDs and schema must match the
        resolved table. Return an immutable commit request; persist it durably
        BEFORE calling ``Catalog.commit_prepared``. A failed prepare may orphan
        uploads, but cannot publish rows. Never regenerate files after preparing.
        With allow_optional_fields, external writers may use optional physical
        fields for required catalog columns only when footer counts prove no nulls.

        Orphan accounting: every exception out of this call carries what it
        already wrote, on the exception object itself (no new type, so
        existing ``except`` clauses keep working):

        ``uploaded_files``
            How many uploads COMPLETED — the output stream closed without
            error. A file whose upload raised partway is NOT counted, in
            either direction: the open may never have succeeded, or the
            close may have failed over a TRUNCATED object that exists in
            the store. So the count is a lower bound on objects present,
            and the failing file must be treated as possibly-there.
        ``uploaded_uris``
            The uris of exactly those completed uploads, in order, always
            ``uploaded_files`` long.

        A refusal raised before the first upload carries ``0`` / ``()``.
        Sweep the uris, not the ``{idempotency_key}/`` prefix: a retry
        under the same key writes new object names beside the old ones,
        so a prefix sweep after a later success deletes live files.
        """
        uploaded: list[str] = []
        try:
            _uuid.UUID(idempotency_key)
            catalog = self._namespace._catalog
            read_snapshot = catalog.refresh().head_snapshot_id
            expected = expected_table_uuid or self.table_uuid
            info = self._check_incarnation(expected)
            if expected_table_info is not None and (
                info.columns,
                info.partition_spec,
                info.sort_spec,
            ) != (
                expected_table_info.columns,
                expected_table_info.partition_spec,
                expected_table_info.sort_spec,
            ):
                raise ValidationError(
                    "prepared append destination layout changed", status_code=None
                )
            has_variant = any(c.type == "variant" for c in info.columns)
            schema = columns_to_arrow_schema(
                tuple(c for c in info.columns if c.type != "variant")
            )
            # Parquet has no seconds timestamp unit: our writer stores timestamp_s
            # as milliseconds. Preserve all field IDs/nullability/metadata checks.
            schema = pa.schema(
                [
                    field.with_type(pa.timestamp("ms"))
                    if field.type == pa.timestamp("s")
                    else field
                    for field in schema
                ],
                metadata=schema.metadata,
            )
            registrations = []
            for index, (path, partition) in enumerate(files):
                with pq.ParquetFile(path) as parquet:
                    if has_variant or allow_optional_fields:
                        validate_variant_file(path, parquet, info.columns)
                    elif not parquet.schema_arrow.equals(schema, check_metadata=True):
                        raise ValidationError(
                            "prepared Parquet schema/field IDs differ from destination",
                            status_code=None,
                        )
                    metadata = parquet.metadata
                arity = len(info.partition_spec.fields) if info.partition_spec else 0
                if (arity and (partition is None or len(partition) != arity)) or (
                    not arity and partition is not None
                ):
                    raise ValidationError(
                        "prepared file partition arity differs from destination",
                        status_code=None,
                    )
                if metadata.num_rows <= 0:
                    raise ValidationError(
                        "prepared file must contain rows", status_code=None
                    )
                uri = f"{catalog.data_path.rstrip('/')}/data/{info.namespace}/{info.name}/{idempotency_key}/{_uuid.uuid4()}-{index}.parquet"
                with open(path, "rb") as source:
                    source.seek(0, 2)
                    size = source.tell()
                    source.seek(-8, 2)
                    trailer = source.read(8)
                    footer_size = struct.unpack("<I", trailer[:4])[0]
                    source.seek(0)
                    with catalog._client._filesystem().open_output_stream(
                        uri.removeprefix("s3://")
                    ) as sink:
                        while chunk := source.read(8 * 1024 * 1024):
                            sink.write(chunk)
                # Only past the stream's close: an object-store write is
                # not durable until then, so counting any earlier would
                # claim uploads that never landed.
                uploaded.append(uri)
                reg: dict[str, Any] = {
                    "path": uri,
                    "record_count": metadata.num_rows,
                    "file_size_bytes": size,
                    "footer_size": footer_size,
                    "column_stats": [
                        stat.to_wire()
                        for stat in extract_column_stats(metadata, info.columns)
                    ],
                }
                if partition is not None:
                    reg["partition_values"] = list(partition)
                registrations.append(reg)
            if not registrations:
                raise ValidationError(
                    "prepared append must contain files", status_code=None
                )
            return {
                "idempotency_key": idempotency_key,
                "read_snapshot": read_snapshot,
                "appends": [
                    {
                        "namespace": self.namespace,
                        "table": self.name,
                        "expected_table_uuid": expected,
                        "files": registrations,
                    }
                ],
            }
        except BaseException as error:
            # BaseException on purpose: a KeyboardInterrupt through a wide
            # fanout orphans objects exactly like an OSError does, and the
            # operator needs the same list.
            _record_uploads(error, uploaded)
            raise

    def _check_incarnation(self, expected_uuid: str) -> TableInfo:
        """Pre-flight fast-fail: re-resolve this table by name and raise
        IncarnationChangedError if the name now binds to a different
        table_uuid. Purely an optimization — it saves the parquet upload
        when the incarnation is already dead; the server-side
        ``expected_table_uuid`` commit guard is the atomic safety
        mechanism. ``self._info`` is only adopted when the incarnation
        matches, so the pinned identity (and the default
        ``expected_table_uuid`` of later appends) is never silently
        rebased onto a recreated table."""
        body = self._namespace._catalog._client._request("GET", self._path())
        info = TableInfo.from_wire(body)
        if info.table_uuid != expected_uuid:
            raise IncarnationChangedError(
                f"table {self._namespace._catalog.name}/{self.namespace}."
                f"{self.name} was recreated: expected table_uuid "
                f"{expected_uuid}, name now resolves to {info.table_uuid}. "
                "Refusing to append across incarnations."
            )
        self._info = info
        return info


def _nested_field_mismatch(
    have: pa.DataType, want: pa.DataType, path: str
) -> tuple[list[str], list[str]]:
    """(missing, extra) dotted paths comparing a caller's nested type
    against the catalog's, recursively."""
    missing: list[str] = []
    extra: list[str] = []
    if pa.types.is_struct(want) and pa.types.is_struct(have):
        want_names = [want.field(i).name for i in range(want.num_fields)]
        have_names = [have.field(i).name for i in range(have.num_fields)]
        missing += [f"{path}.{n}" for n in want_names if n not in have_names]
        extra += [f"{path}.{n}" for n in have_names if n not in want_names]
        for name in want_names:
            if name in have_names:
                m, e = _nested_field_mismatch(
                    have.field(name).type, want.field(name).type, f"{path}.{name}"
                )
                missing += m
                extra += e
    elif pa.types.is_map(want) and pa.types.is_map(have):
        for label, h, w in (
            ("key", have.key_field.type, want.key_field.type),
            ("value", have.item_field.type, want.item_field.type),
        ):
            m, e = _nested_field_mismatch(h, w, f"{path}.{label}")
            missing += m
            extra += e
    elif is_list_family(want) and is_list_family(have):
        # The FAMILY, not the canonical member. large_list and
        # fixed_size_list both normalize to catalog `list`, so a caller
        # appending either walked into an `is_list`-only branch that
        # answered "no mismatch" without looking — and the typo'd inner
        # field the recursion exists to catch reached the cast and
        # appended as an all-NULL column.
        m, e = _nested_field_mismatch(
            have.value_field.type, want.value_field.type, f"{path}.element"
        )
        missing += m
        extra += e
    return missing, extra


def _align_table(data: pa.Table, target: pa.Schema) -> pa.Table:
    """Reorder ``data`` to the catalog column order and cast to the target
    schema (which carries the field-id metadata).

    The name comparison is RECURSIVE. At top level a missing or unknown
    column has always been a refusal; below it, ``cast`` quietly
    null-filled what the caller had not supplied and dropped what the
    catalog did not know — so a typo'd struct field appended as an
    all-NULL column whose own stats said ``null_count == record_count``,
    and the only evidence was that the data was not there. The same
    mistake deserves the same answer at every level.
    """
    have = set(data.schema.names)
    want = list(target.names)
    missing = [n for n in want if n not in have]
    if missing:
        raise ValidationError(
            f"data is missing table columns: {missing}", status_code=None
        )
    extra = [n for n in data.schema.names if n not in set(want)]
    if extra:
        raise ValidationError(
            f"data has columns not in the table schema: {extra}",
            status_code=None,
        )
    nested_missing: list[str] = []
    nested_extra: list[str] = []
    for name in want:
        m, e = _nested_field_mismatch(
            data.schema.field(name).type, target.field(name).type, name
        )
        nested_missing += m
        nested_extra += e
    if nested_missing:
        raise ValidationError(
            f"data is missing nested table fields: {nested_missing}",
            status_code=None,
        )
    if nested_extra:
        raise ValidationError(
            f"data has nested fields not in the table schema: {nested_extra}",
            status_code=None,
        )
    data = data.select(want)
    return data.cast(target)


def _write_one_file(
    part: pa.Table,
    info: TableInfo,
    catalog: Catalog,
    client: HoglakeClient,
    *,
    deferred_stats: bool,
    row_group_size: int | None,
    partition_values: tuple[str | None, ...] | None,
) -> dict[str, Any]:
    """THE single-file writer path: serialize ``part`` to parquet (field
    ids already on the schema), upload it under the catalog's data path,
    and build its FileRegistration wire dict — unchanged conventions
    (footer stats from the writer's own metadata, exact ``footer_size``),
    plus ``partition_values`` when the table is partitioned."""
    sink = io.BytesIO()
    if row_group_size is not None:
        pq.write_table(part, sink, row_group_size=row_group_size)
    else:
        pq.write_table(part, sink)
    raw = sink.getvalue()
    metadata = pq.read_metadata(io.BytesIO(raw))

    column_stats = None
    if not deferred_stats:
        column_stats = extract_column_stats(metadata, info.columns)

    data_path = catalog.data_path
    if not data_path.endswith("/"):
        data_path += "/"
    file_uri = f"{data_path}data/{info.namespace}/{info.name}/{_uuid.uuid4()}.parquet"
    _upload(client._filesystem(), file_uri, raw)

    file_reg: dict[str, Any] = {
        "path": file_uri,
        "record_count": metadata.num_rows,
        "file_size_bytes": len(raw),
        "footer_size": _footer_size(raw),
    }
    if column_stats is not None:
        file_reg["column_stats"] = [s.to_wire() for s in column_stats]
    if partition_values is not None:
        file_reg["partition_values"] = list(partition_values)
    return file_reg


def _field_id_chains(
    columns: tuple[Column, ...] | list[Column],
    prefix: tuple[Column, ...] = (),
) -> dict[int, list[Column]]:
    """Every column NODE by field id, mapped to its root-to-node chain.

    Containers are included: the partition path needs to see them to
    refuse a spec that points at one (or at something under a list or a
    map), and a "not a live column" error would be the wrong answer for a
    field that plainly exists.
    """
    out: dict[int, list[Column]] = {}
    for c in columns:
        chain = (*prefix, c)
        out[c.field_id] = list(chain)
        if c.children:
            out.update(_field_id_chains(c.children, chain))
    return out


def _partition_groups(
    data: pa.Table, info: TableInfo, spec: PartitionSpec
) -> list[tuple[tuple[str | None, ...], pa.Table]]:
    """Split an aligned batch by partition tuple under the table's live
    spec: one (wire-string tuple, sub-table) per distinct tuple, ordered
    by first occurrence in the batch (so file registration — and the
    server's rows-then-offset row-id assignment — follows input order).

    Transforms run arrow-native where possible and per UNIQUE value in
    Python otherwise (see :func:`pyhoglake.transforms.transform_strings`);
    a null source value yields a null partition value forming its own
    group, per Iceberg.
    """
    # Chains, not a flat map: a partition source may be a struct LEAF, so
    # reaching it needs the whole root-to-leaf path (and the path is what
    # tells us whether a list or a map sits in the way).
    chains = _field_id_chains(info.columns)
    key_names = [f"__hog_pk_{i}" for i in range(len(spec.fields))]
    key_arrays: list[pa.Array] = []
    for pf in spec.fields:
        chain = chains.get(pf.source_field_id)
        if chain is None:
            raise ValidationError(
                f"partition spec (spec_id={spec.spec_id}) references "
                f"field_id {pf.source_field_id}, which is not a live column "
                f"of {info.namespace}.{info.name}",
                status_code=None,
            )
        col = chain[-1]
        key_arrays.append(
            transform_strings(
                pf.transform,
                pf.transform_param,
                partition_source_array(data, chain),
                col.type,
                col.type_params,
            )
        )
    keyed = pa.table(
        {
            **dict(zip(key_names, key_arrays, strict=True)),
            "__hog_row": pa.array(range(data.num_rows), pa.int64()),
        }
    )
    combos = (
        keyed.group_by(key_names)
        .aggregate([("__hog_row", "min")])
        .sort_by("__hog_row_min")
    )
    out: list[tuple[tuple[str | None, ...], pa.Table]] = []
    for i in range(combos.num_rows):
        values = tuple(combos.column(k)[i].as_py() for k in key_names)
        mask = None
        for name, value in zip(key_names, values, strict=True):
            key_col = keyed.column(name)
            if value is None:
                field_mask = pc.is_null(key_col)
            else:
                field_mask = pc.fill_null(pc.equal(key_col, value), False)
            mask = field_mask if mask is None else pc.and_(mask, field_mask)
        out.append((values, data.filter(mask)))
    return out


def _footer_size(raw: bytes) -> int:
    """Thrift footer-metadata length for the commit's ``footer_size``.

    Wire convention (bugs.md #7): ``footer_size`` is EXACTLY the 4-byte
    LE length stored in the parquet trailer — the serialized thrift
    FileMetaData size, EXCLUDING the trailing 8-byte suffix (4-byte
    length + ``PAR1`` magic). The server's hydrator tail-reads
    ``[file_size - footer_size - 8, file_size)`` and compaction stores
    the same value for its own outputs; shipping ``meta_len + 8`` here
    (the old behavior) made every client-written file 8 bytes off.
    """
    (meta_len,) = struct.unpack("<I", raw[-8:-4])
    return meta_len


def _upload(fs, uri: str, raw: bytes) -> None:
    if not uri.startswith("s3://"):
        raise HoglakeError(
            f"unsupported data_path scheme for the write path: {uri!r} "
            "(only s3:// is supported)"
        )
    key = uri[len("s3://") :]
    with fs.open_output_stream(key) as out:
        out.write(raw)
