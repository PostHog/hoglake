"""The hoglake client: a thin wrapper over the control-plane REST API.

Data never flows through the server: ``Table.append`` writes Parquet to
object storage itself and registers footer-derived stats. Packed MergeTree
tables use the separate :mod:`pyhoglake.packed` adapter.
"""

from __future__ import annotations

import contextlib
import importlib.metadata
import io
import logging
import struct
import time
import uuid as _uuid
from collections.abc import Iterator, Mapping, Sequence
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
    DdlSinceReadSnapshotError,
    ExpiredError,
    HoglakeError,
    IncarnationChangedError,
    NotFoundError,
    OffsetRegressionError,
    ReadSnapshotExpiredError,
    ReconciliationRequiredError,
    ValidationError,
)
from .formats import require_parquet
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
from .parquet_schema import prepared_schema_matches, validate_variant_file
from .stats import extract_column_stats
from .transforms import partition_source_array, transform_strings
from .types import columns_to_arrow_schema, is_list_family, schema_to_column_defs
from .upload import (
    Upload,
    perform_upload,
    resolve_concurrency,
    run_uploads,
    s3_key,
    widen_io_threads_for,
)

DEFAULT_TIMEOUT = 30.0


def _user_agent() -> str:
    """`pyhoglake/<version>`, from the installed metadata.

    Set on every request because the SERVER reads it: its transition
    warnings name the client that has to change
    (`HOGLAKE_REFUSE_BLIND_PARTITIONED_APPENDS`), and without this the
    line said `python-httpx/0.28` — true, useless, and identical for
    every other httpx caller in the fleet. Same source as
    `pyhoglake.__version__`, and unknown rather than a guess if the
    package is not installed (a source checkout on sys.path).
    """
    try:
        return f"pyhoglake/{importlib.metadata.version('pyhoglake')}"
    except importlib.metadata.PackageNotFoundError:  # pragma: no cover
        return "pyhoglake/unknown"


USER_AGENT = _user_agent()

# The package's logger, named for the package rather than the module: a
# consumer configures one name to hear from pyhoglake, and the handful of
# things worth saying are all about the writer path. __init__ attaches a
# NullHandler, so a library that is never configured stays silent.
logger = logging.getLogger("pyhoglake")

# Sentinel for Table.append(expected_table_uuid=...): opt out of the
# incarnation guard entirely — no pre-flight uuid check, and the commit
# carries no expected_table_uuid field (name-only resolution).
UNGUARDED = object()

# The 409/410 codes whose recovery is "re-read the table and prepare a
# new request", never "replay this payload". The server says the same
# thing in the body as `retry: re-prepare`.
_RE_PREPARE_ERRORS: dict[str, type[HoglakeError]] = {
    "ddl_since_read_snapshot": DdlSinceReadSnapshotError,
    "table_recreated": IncarnationChangedError,
}

# Assumed snapshot retention when the catalog will not say: half of this
# is how stale a cached read_snapshot may get before the writer path
# refreshes it. 30 min is deliberately shorter than any retention worth
# configuring, so an unknown answer costs an extra read rather than an
# expired commit.
_ASSUMED_RETENTION_SECONDS = 1800.0
# How long a FAILED options read is remembered before it is retried. Long
# enough that a persistently broken options endpoint cannot reintroduce a
# per-flush GET, short enough that a real retention change is picked up
# within a minute of the endpoint recovering.
_RETENTION_RETRY_SECONDS = 60.0

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


def _basis_was_cached(table: Table, payload: Mapping[str, Any]) -> bool:
    """Whether this payload's `read_snapshot` is the one the Table cached.

    The question a 4xx has to answer before it invalidates: a payload
    built from a FRESH read already has the best basis there is, so
    dropping the cache for it would buy a read per malformed request and
    fix nothing.
    """
    if table._cache is None:
        return False
    return payload.get("read_snapshot") == table._cache[0]


def _cache_entry(info: TableInfo) -> tuple[int, float] | None:
    """The writer cache for ``info``, or None when there is none to keep.

    None exactly when the response did not say which snapshot it was
    resolved at (`read_snapshot_id`, NOT `snapshot_id` — that one is a
    DDL receipt): without it there is nothing to send as a
    ``read_snapshot``, so the writer path falls back to the two reads it
    always made. This is the ONE place that rule lives.

    A CURRENT server always sends it (required and non-null in the spec),
    so this branch — and the `else head` fallbacks it forces in
    `_prepared_read` and `Table.append` — is reachable only against an
    older one. Kept rather than deleted for exactly that reason, and
    tested on both sides.
    """
    if info.read_snapshot_id is None:
        return None
    return info.read_snapshot_id, time.monotonic()


def _re_prepare_error(code: str, body: Any, detail: str | None) -> HoglakeError:
    """Build one of the re-prepare refusals from the server's body.

    Every field is read with ``.get``: a partial or older body must still
    produce the right TYPE, because the type is what tells a retry loop
    to re-prepare instead of replaying. The values are for the operator
    and for cache invalidation.
    """
    fields = body if isinstance(body, dict) else {}
    tables = tuple(fields.get("tables") or ())
    if code == "table_recreated":
        return IncarnationChangedError(
            code,
            status_code=409,
            detail=detail,
            table=tables[0] if tables else None,
        )
    return DdlSinceReadSnapshotError(
        code,
        status_code=409,
        detail=detail,
        tables=tables,
        read_snapshot=fields.get("read_snapshot"),
    )


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
    # Small objects go up as ONE PutObject (see put_client). Turn it off
    # to send every object through pyarrow's streaming multipart writer —
    # an escape hatch for an endpoint that handles one and not the other,
    # not a performance choice.
    single_request_uploads: bool = True

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

    def put_client(self, connections: int):
        """An S3 client for single-request ``PutObject`` uploads, built
        from this config's own fields, or ``None`` when single-request
        uploads are unavailable.

        pyarrow cannot do this job: its S3 output stream always opens a
        multipart upload (pyarrow 25.0.1, measured against a recording
        endpoint — CreateMultipartUpload + UploadPart +
        CompleteMultipartUpload for a 12 KiB buffer written and closed in
        one go), and Arrow exposes no whole-buffer write. boto3 is an
        optional extra (``pip install 'pyhoglake[fast-upload]'``), so its
        absence answers ``None`` rather than raising: the objects still
        go up, by the streaming path, at three requests each.

        **What is guaranteed about parity with :meth:`filesystem`.** The
        two are given the same explicit fields — access key, secret key,
        endpoint override (with the same path-style addressing), and the
        region when this config names one — and ambient endpoint
        configuration is refused on both sides
        (``ignore_configured_endpoint_urls``), because botocore honours
        ``AWS_ENDPOINT_URL`` / ``AWS_ENDPOINT_URL_S3`` and the shared
        config's ``endpoint_url`` while ``S3FileSystem`` reads none of
        them: without that, one flush could put its small objects in one
        store and its large ones in another and register both under the
        same uris.

        **What is not.** Credentials still come from two implementations
        of the same chain (botocore's, and the AWS C++ SDK's inside
        Arrow), and with ``region=None`` Arrow resolves the bucket's
        region while botocore falls back to its own default and leans on
        S3's region redirect. Neither has been tested against an
        instance-role pod or a bucket outside the configured region.
        ``single_request_uploads=False`` sidesteps both if a deployment
        needs one code path.

        ``connections`` sizes the connection pool. botocore's default is
        10, and a 64-wide upload fan-out through 10 connections is a
        64-wide fan-out that behaves like a 10-wide one.
        """
        if not self.single_request_uploads:
            # An explicit choice, so no complaint: the operator asked for
            # the streaming path.
            return None
        try:
            import boto3
            from botocore.config import Config
            from botocore.exceptions import BotoCoreError
        except ImportError:
            logger.warning(
                "pyhoglake: boto3 is not installed, so parquet uploads will use "
                "pyarrow's streaming multipart writer — three S3 requests per "
                "object instead of one. Install pyhoglake[fast-upload] to get "
                "the single-request path, or set "
                "S3Config(single_request_uploads=False) to choose it deliberately."
            )
            return None
        kwargs: dict[str, Any] = {}
        if self.access_key is not None:
            kwargs["aws_access_key_id"] = self.access_key
        if self.secret_key is not None:
            kwargs["aws_secret_access_key"] = self.secret_key
        if self.region is not None:
            kwargs["region_name"] = self.region
        if self.endpoint_override is not None:
            endpoint = self.endpoint_override
            if "://" not in endpoint:
                # The override may omit the scheme; pyarrow defaults a
                # bare host to https, so mirror that rather than invent
                # a second rule.
                endpoint = f"https://{endpoint}"
            kwargs["endpoint_url"] = endpoint
        try:
            return boto3.client(
                "s3",
                config=Config(
                    # Path style whenever the endpoint is overridden,
                    # which is what the pyarrow filesystem does: MinIO
                    # and friends have no bucket-as-subdomain DNS.
                    s3={
                        "addressing_style": "path" if self.endpoint_override else "auto"
                    },
                    # Arrow reads none of botocore's endpoint environment
                    # (AWS_ENDPOINT_URL, AWS_ENDPOINT_URL_S3, the shared
                    # config's endpoint_url), so honouring them here would
                    # split a single flush across two object stores while
                    # registering every file under one set of uris. Set
                    # unconditionally: an explicit endpoint_url kwarg
                    # still wins (verified against botocore 1.43.105).
                    ignore_configured_endpoint_urls=True,
                    # Arrow's SDK defaults to roughly 1 s connect / 3 s
                    # request; botocore defaults to 60/60, and run_uploads
                    # waits for every in-flight upload before it re-raises,
                    # so a brownout with those defaults turns a failing
                    # flush into an ~8 minute stall (4 tries x 120 s).
                    # max_attempts is botocore's count of RETRIES, so this
                    # is four tries: worst case ~2.5 minutes for the
                    # unluckiest object, and the whole fan-out waits out
                    # only that one, not the sum.
                    connect_timeout=3,
                    read_timeout=30,
                    retries={"mode": "standard", "max_attempts": 3},
                    max_pool_connections=max(connections, 10),
                ),
                **kwargs,
            )
        except BotoCoreError as refused:
            # NOT the missing-region case: botocore falls back to its own
            # default for S3 and does not raise. This catches a broken
            # profile or a malformed shared config file — the streaming
            # path needs neither, so fall back rather than fail a write
            # over the fast path's own configuration.
            logger.warning(
                "pyhoglake: could not build the single-request upload client "
                "(%s), so parquet uploads will use pyarrow's streaming "
                "multipart writer — three S3 requests per object instead of "
                "one. The AWS profile/config this process reads is the thing "
                "to check.",
                refused,
            )
            return None


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
        self._http = httpx.Client(
            base_url=self.base_url + "/v1",
            timeout=timeout,
            headers={"User-Agent": USER_AGENT},
        )
        self._fs = None
        self._put: Any = None
        self._put_pool = 0
        self._put_declined = False

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
        body: Any = None
        try:
            body = resp.json()
            if isinstance(body, dict):
                message = body.get("error", message)
                detail = body.get("detail")
        except Exception:  # noqa: BLE001  # any body-parse failure falls back to raw text
            detail = resp.text[:500] or None
        # The re-prepare family is built here rather than by the
        # generic constructor below: `ddl_since_read_snapshot` carries
        # the tables and the snapshot that were refused, and a caller
        # acts on those rather than on the detail prose. The CODE is the
        # contract — this used to be a search for the phrase "the table
        # was recreated" inside `detail`.
        if resp.status_code == 409 and message in _RE_PREPARE_ERRORS:
            raise _re_prepare_error(message, body, detail)
        cls: type[HoglakeError]
        if resp.status_code == 409 and message == "reconciliation_required":
            cls = ReconciliationRequiredError
        elif resp.status_code == 404:
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

    def _put_client(self, connections: int = 1):
        """The cached single-request upload client
        (:meth:`S3Config.put_client`), or ``None`` when small objects
        have to take the streaming path.

        Cached beside the filesystem so both are always built from the
        same :class:`S3Config`, and rebuilt only when a wider fan-out
        needs a bigger connection pool than the one the cached client
        actually got — ``put_client`` floors the pool at botocore's own
        10, so comparing the request rather than the effective size would
        rebuild a client that was already wide enough and leak its
        connection pool.

        A decline is asked for ONCE per client: boto3 being absent or the
        AWS config being unreadable cannot change under us, and asking
        again would repeat the warning that goes with it on every flush.
        """
        self._filesystem()  # the same "no S3 configuration" refusal, first
        if self._put_declined:
            return None
        if self._put_pool < connections:
            # Duck-typed on purpose: an S3 stand-in that implements only
            # filesystem() (every test double here, and any caller that
            # passes its own) keeps the streaming path.
            factory = getattr(self.s3, "put_client", None)
            replaced = self._put
            self._put = None if factory is None else factory(connections)
            self._put_declined = self._put is None
            self._put_pool = max(connections, 10)
            if replaced is not None:
                # Its urllib3 pool would otherwise live until GC. Named
                # exceptions, like _record_uploads' annotation: OSError
                # because tearing down sockets is the only thing a real
                # close does, AttributeError because a duck-typed
                # stand-in need not implement one. Anything else here is
                # a bug worth seeing, not worth swallowing.
                with contextlib.suppress(AttributeError, OSError):
                    replaced.close()
        return self._put

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
        # See _retention_seconds: one options GET per Catalog object.
        self._retention_cache: float | None = None
        # ...and, when the read FAILS, one per _RETENTION_RETRY_SECONDS.
        self._retention_failed_at: float | None = None

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

    def list_namespaces(
        self,
        snapshot: int | None = None,
        at_timestamp: datetime | str | None = None,
    ) -> list[str]:
        """Namespace names at head, or at ``snapshot`` / ``at_timestamp``.

        A pinned listing names what existed at the pin, including a
        namespace dropped after it, so it agrees with pinned table reads.
        """
        body = self._client._request(
            "GET",
            self._path("/namespaces"),
            params=_travel_params(snapshot, at_timestamp),
        )
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
        # The writer cache's staleness threshold is derived from
        # retention, so changing it here has to invalidate it. Shortening
        # retention is a live operational lever, and a long-lived writer
        # keeping the old threshold would hold a basis past the new floor.
        self._retention_cache = None
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

    def commit_prepared(
        self, payload: dict[str, Any], *, table: Table | None = None
    ) -> CommitResult:
        """Publish a durably saved request; retry the EXACT payload on uncertainty.

        Requires a server supporting CommitRequest.idempotency_key (V7 migration).
        This API does not apply event-level deduplication.

        SHELF LIFE, and it shortened: the payload carries its own
        ``read_snapshot``, and that basis is refused once it sinks below
        the catalog's expiry floor. A payload prepared from a warm cache
        may already be up to half the retention old when it is written, so
        a persisted request is good for **at least half** the retention
        rather than the full window it used to get (30 minutes at
        prod-us's 3600 s). A committer that backlogs or restarts past that
        gets :class:`ReadSnapshotExpiredError` — after the parquet is
        uploaded — and must re-prepare. Persist and publish promptly.

        ``table`` is the ``Table`` the request was prepared from, when
        the caller has it. A ``re_prepare`` refusal then invalidates that
        Table's cached info, so its next ``prepare_*`` re-reads instead
        of rebuilding the same doomed payload. It is optional because
        this call is explicitly cross-process — the committer need not
        be, and after a restart cannot be, the process that prepared —
        and such a caller re-prepares from a fresh
        ``Namespace.table()``, which reads anyway. Pass it whenever you
        can; see ``Table.invalidate``.
        """
        if not payload.get("idempotency_key"):
            raise ValueError("prepared commits require an idempotency_key")
        _uuid.UUID(payload["idempotency_key"])
        return self._commit(payload, prepared=True, table=table)

    def _commit_uploads(
        self, payload: dict[str, Any], *, table: Table | None = None
    ) -> CommitResult:
        """Publish an exact request whose object paths have durable upload claims."""
        if not payload.get("idempotency_key") or payload.get("read_snapshot") is None:
            raise ValueError(
                "claimed upload commits require idempotency_key and read_snapshot"
            )
        _uuid.UUID(payload["idempotency_key"])
        return self._commit(payload, table=table, endpoint="/commit/uploads")

    def _commit(
        self,
        payload: dict[str, Any],
        *,
        prepared: bool = False,
        table: Table | None = None,
        endpoint: str | None = None,
    ) -> CommitResult:
        try:
            body = self._client._request(
                "POST",
                self._path(endpoint or ("/commit/prepared" if prepared else "/commit")),
                json=payload,
                conflict=CommitConflictError,
            )
        except ExpiredError as e:
            # A commit's 410 is its own read_snapshot below the expiry
            # floor, which is a DIFFERENT fact from a changefeed 410
            # ("reconcile from a full scan"): the floor only moves
            # forward, so this payload can never be accepted, and the
            # recovery is to re-read and prepare again. Subclass, so an
            # existing `except ExpiredError` still catches it.
            #
            # It is also the one observation that proves the retention
            # this Catalog cached is wrong — retention can be SHORTENED
            # live, and a writer holding the old, longer threshold would
            # otherwise take this 410 once per new-retention period
            # forever. Dropping it makes the degradation self-healing.
            self._retention_cache = None
            if table is not None:
                table.invalidate()
            raise ReadSnapshotExpiredError(
                e.message, status_code=e.status_code, detail=e.detail
            ) from e
        except HoglakeError as e:
            # Two reasons to drop the cache, and the second is the safety
            # net for the first.
            #
            # `re_prepare` is the designed path: the server named the
            # drift, so the cached info is stale by definition.
            #
            # Any other NON-RETRYABLE refusal of a payload whose basis
            # came from the cache is the undesigned one. A DDL change and
            # the file-level symptom it produces arrive together — DROP
            # COLUMN leaves stats naming a dead field_id, a new spec leaves
            # the arity wrong — and if the symptom is answered first the
            # client gets a 422 that says nothing about its basis. The
            # server now runs the conflict check first so that should not
            # happen, but "should not" is not a mechanism: without this the
            # cost of being wrong is a writer rebuilding an identical
            # doomed payload, re-encoding and orphaning its parquet every
            # flush, until the cache ages out. The cost of being wrong the
            # other way is one extra table read per genuinely-malformed
            # request.
            #
            # RETRYABLE is excluded, and that exclusion is why the flag is
            # consulted rather than the status: `commit_conflict` is
            # replayed with the SAME payload, so the basis it was built
            # from must survive, or every OCC retry buys a table read —
            # the cost #232 removes.
            if table is not None and (
                e.re_prepare or (not e.retryable and _basis_was_cached(table, payload))
            ):
                table.invalidate()
            raise
        return CommitResult.from_wire(body)

    def _retention_seconds(self) -> float:
        """Cached `snapshot_retention_seconds`, as a float number of seconds.

        ONE GET per Catalog object, not per flush: it decides how stale a
        cached read_snapshot may get, and retention does not change on a
        flush timescale. `inf` when retention is disabled (the expiry
        floor never advances on its own, so a cached snapshot cannot age
        out).

        Cleared by :meth:`set_retention` and by a
        :class:`ReadSnapshotExpiredError`, which are the two events that
        can falsify it.

        A failed options read does not become the cached VALUE — it
        answers [_ASSUMED_RETENTION_SECONDS] for this call, so one
        transient blip cannot pin the process to 30-minute refreshes for
        its life; an unknown retention must cost a read, never an expired
        commit. But the failure itself is remembered for
        [_RETENTION_RETRY_SECONDS], because _cache_is_usable calls this on
        every prepare: a PERSISTENT failure (the endpoint 5xx-ing, a
        permission problem, a server without it) would otherwise issue one
        options GET per flush forever — silently restoring the per-flush
        round trip this whole path exists to remove, in the one situation
        where nobody is watching. Bounded at one read a minute, the
        assumption costs nothing and still heals within a minute.
        """
        if self._retention_cache is None:
            now = time.monotonic()
            if (
                self._retention_failed_at is not None
                and now - self._retention_failed_at < _RETENTION_RETRY_SECONDS
            ):
                return _ASSUMED_RETENTION_SECONDS
            try:
                seconds = self.options().snapshot_retention_seconds
            except HoglakeError:
                self._retention_failed_at = now
                return _ASSUMED_RETENTION_SECONDS
            self._retention_failed_at = None
            self._retention_cache = float("inf") if seconds is None else float(seconds)
        return self._retention_cache


class Namespace:
    def __init__(self, catalog: Catalog, name: str) -> None:
        self._catalog = catalog
        self.name = name

    def _path(self, suffix: str = "") -> str:
        return self._catalog._path(f"/namespaces/{_seg(self.name)}{suffix}")

    def __repr__(self) -> str:  # pragma: no cover
        return f"<Namespace {self._catalog.name}.{self.name}>"

    # -- tables ------------------------------------------------------------

    def create_table(
        self,
        name: str,
        schema: pa.Schema,
        *,
        properties: Mapping[str, str] | None = None,
    ) -> Table:
        _check_reserved_columns(schema)
        request: dict[str, Any] = {
            "name": name,
            "columns": schema_to_column_defs(schema),
        }
        if properties is not None:
            request["properties"] = dict(properties)
        body = self._catalog._client._request(
            "POST",
            self._path("/tables"),
            json=request,
        )
        return Table(self, TableInfo.from_wire(body))

    def table(self, name: str) -> Table:
        body = self._catalog._client._request(
            "GET", self._path(f"/tables/{_seg(name)}")
        )
        return Table(self, TableInfo.from_wire(body))

    def list_tables(
        self,
        snapshot: int | None = None,
        at_timestamp: datetime | str | None = None,
    ) -> list[TableSummary]:
        """Table summaries at head, or at ``snapshot`` / ``at_timestamp``.

        Every field of every row answers for that one snapshot. A pinned
        listing includes a table dropped after the pin and resolves this
        namespace at the pin too.
        """
        body = self._catalog._client._request(
            "GET",
            self._path("/tables"),
            params=_travel_params(snapshot, at_timestamp),
        )
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
        # The freshest snapshot this client's own DDL made (create/alter),
        # kept SEPARATELY from _info: a later info() refresh replaces _info
        # from a read response that carries no snapshot_id, and must not
        # erase a pin that is still valid. See snapshot_id.
        self._ddl_snapshot_id: int | None = info.snapshot_id
        # (snapshot the info was resolved at, when it was read) — ONE
        # field, so "is there a usable cache" has one answer and the two
        # halves cannot drift apart. None = no usable cache, which is
        # also what a server that does not report the resolved snapshot
        # leaves behind. The clock is MONOTONIC: elapsed time is the
        # question and a wall-clock step must not answer it.
        self._cache: tuple[int, float] | None = _cache_entry(info)

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
    def snapshot_id(self) -> int | None:
        """The freshest snapshot this table's create/alter commit made, or
        None when this client has not done DDL on it (or the server is too
        old to return one).

        Reads pin to it automatically: files()/scan_plan() with no explicit
        snapshot resolve at this snapshot, so a create/alter followed by a
        read sees that DDL instead of racing a head read (#35). Pass an
        explicit snapshot (or at_timestamp) to read elsewhere. Sticky
        across info() refreshes (a read carries no snapshot_id, and a pin
        to an earlier snapshot stays valid); a new alter replaces it;
        drop() clears it."""
        return self._ddl_snapshot_id

    @property
    def columns(self):
        return self._info.columns

    @property
    def comment(self) -> str | None:
        """The table's versioned comment; None when it has none."""
        return self._info.comment

    @property
    def properties(self) -> dict[str, str] | None:
        """The table's versioned properties; None when none are set."""
        return self._info.properties

    def _path(self, suffix: str = "") -> str:
        return self._namespace._path(f"/tables/{_seg(self._info.name)}{suffix}")

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"<Table {self._namespace._catalog.name}.{self.namespace}."
            f"{self.name} uuid={self.table_uuid}>"
        )

    # -- reads -------------------------------------------------------------

    def _read_snapshot(
        self,
        snapshot: int | None,
        at_timestamp: datetime | str | None,
    ) -> int | None:
        """The snapshot a read resolves to.

        Explicit travel always wins (snapshot or at_timestamp). Otherwise
        the read pins to this client's own DDL snapshot when one exists:
        the whole point of #35 is that a create/alter followed by a read
        must see that DDL, not race a head read that could miss or
        overshoot it. With no pin (a table this client only ever read),
        it is None and the read goes to head as before.
        """
        if snapshot is not None or at_timestamp is not None:
            return snapshot
        return self._ddl_snapshot_id

    def info(
        self,
        snapshot: int | None = None,
        at_timestamp: datetime | str | None = None,
        *,
        totals: bool = True,
    ) -> TableInfo:
        """This table's shape, and by default its live totals.

        ``totals=False`` asks the server to omit record_count /
        file_count / file_size_bytes and skip the scan that produces
        them — a count and two sums over every live file row of the
        table, which on a large table is the whole cost of the call. The
        three fields come back as ``None``, never 0. The WRITER path
        always asks this way: it needs the uuid, the columns and the
        specs, and has never read a total.

        Sending the parameter is safe against a server that predates it:
        unknown query parameters are ignored and the totals come back as
        before.
        """
        params = _travel_params(snapshot, at_timestamp)
        if not totals:
            params["totals"] = "false"
        body = self._namespace._catalog._client._request(
            "GET", self._path(), params=params
        )
        info = TableInfo.from_wire(body)
        if snapshot is None and at_timestamp is None:
            self._adopt(info)
        return info

    def _adopt(self, info: TableInfo) -> None:
        """Take a head read as this table's cached shape.

        One function, so the read timestamp can never be forgotten: the
        cache is only usable while it is younger than half the catalog's
        retention, and a cache with a stale (or absent) timestamp is not
        used at all.
        """
        self._info = info
        self._cache = _cache_entry(info)

    def commit_prepared(self, payload: dict[str, Any]) -> CommitResult:
        """Publish a request this Table prepared, invalidating on refusal.

        The same call as :meth:`Catalog.commit_prepared` with
        ``table=self``, and the form to prefer whenever the publishing
        code still holds the ``Table``: the cache invalidation a
        ``re_prepare`` refusal needs cannot be forgotten, because there is
        no parameter to forget.

        The Catalog form stays for the cross-process case the contract is
        built around — a committer that persisted the payload and may be a
        different process entirely. **A caller using that form without
        ``table=`` owns invalidation itself** (millpond does, by dropping
        its whole cached ``Table`` in `reset_caches`); not doing so turns
        a one-flush refusal into a loop until the cache ages out.
        """
        return self._namespace._catalog.commit_prepared(payload, table=self)

    def invalidate(self) -> None:
        """Drop this Table's cached info so the next ``prepare_*`` re-reads.

        Call it after any ``re_prepare`` refusal from a commit this
        Table's info was prepared for —
        :class:`DdlSinceReadSnapshotError`,
        :class:`IncarnationChangedError`,
        :class:`ReadSnapshotExpiredError`. ``Table.append`` and
        ``Catalog.commit_prepared(..., table=self)`` already do it; this
        is the lever for a caller that publishes some other way.

        Not calling it is a livelock, not a slowdown: the next prepare
        would rebuild a payload carrying the same stale ``read_snapshot``
        and take the same refusal.
        """
        self._cache = None

    def files(
        self,
        snapshot: int | None = None,
        at_timestamp: datetime | str | None = None,
    ) -> list[DataFile]:
        body = self._namespace._catalog._client._request(
            "GET",
            self._path("/files"),
            params=_travel_params(
                self._read_snapshot(snapshot, at_timestamp), at_timestamp
            ),
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
            params=_travel_params(
                self._read_snapshot(snapshot, at_timestamp), at_timestamp
            ),
        )
        return [ScanFile.from_wire(f) for f in body]

    def changes(
        self, from_snapshot: int, to_snapshot: int | None = None
    ) -> ChangesPlan:
        """Changefeed plan for rows appended in (from_snapshot, to_snapshot].

        Raises :class:`ExpiredError` (410) when part of the range has been
        expired — reconcile from a full scan. A window crossing TRUNCATE raises
        :class:`ReconciliationRequiredError` (409); reconcile before checkpointing.
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
        # _adopt, not a bare assignment: the cache's (snapshot, read time)
        # has to move with the shape. An alter receipt carries its OWN
        # read_snapshot_id (AlterService sets it to the alter's snapshot,
        # and the field is required on Table), so this re-seeds the cache
        # at the post-alter snapshot: the next prepare sends a basis the
        # alter is not newer than, with no GET. Assigning _info directly
        # left the PRE-alter snapshot paired with the new shape, making the
        # next flush a guaranteed 409 with its uploads orphaned. Against a
        # server too old to report the field, _adopt clears instead, and
        # the next prepare re-reads; both outcomes are correct.
        self._adopt(TableInfo.from_wire(body))
        # A newer DDL snapshot supersedes the old pin.
        if self._info.snapshot_id is not None:
            self._ddl_snapshot_id = self._info.snapshot_id
        return self._info

    def drop(self) -> CommitResult:
        body = self._namespace._catalog._client._request("DELETE", self._path())
        result = CommitResult.from_wire(body)
        # The table is gone; a pin to one of its snapshots resolves a dead
        # incarnation, not this table. Drop it rather than hand back a
        # snapshot_id that points at nothing (or, after a same-name
        # recreate, at the wrong table). Same for the writer cache: a
        # prepare against it would be refused by the incarnation guard.
        self._ddl_snapshot_id = None
        self._cache = None
        return result

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
        require_parquet(self._info.properties, "Table.append")

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

        # The conflict basis, read BEFORE the resolve and long before the
        # upload, and the order is the whole point. A partitioned append
        # must carry a read_snapshot (the server refuses a blind one:
        # partition values are only valid under the spec they were
        # computed with), and the server's window is
        # `snapshot_id > read_snapshot` — so a basis taken AFTER the
        # resolve sits above any DDL that landed in between, putting the
        # respec this guard exists to catch OUTSIDE the window. Taken
        # first, the window covers the resolve, the parquet write and the
        # whole upload. `_prepared_read` reads the catalog first for
        # exactly this reason.
        #
        # `read_snapshot_id` supersedes it when the server reports one:
        # that pairs the spec the values were computed under with the
        # window at the same instant, which is strictly tighter. So this
        # read is ONLY for a server too old to report it — hence the third
        # condition. Against any current server the field is required, the
        # selection below always takes it, and doing this read anyway
        # would be a second GET per partitioned append whose result is
        # discarded. The cached info carries the capability hint itself:
        # if the last read got no `read_snapshot_id`, the next will not
        # either.
        #
        # Reading the hint from a cache that may be stale is safe in both
        # directions: if the server has since gained the field, the
        # selection below falls through to its own fresh pre-upload head
        # read; if it has lost it, this read is merely wasted.
        head_before_resolve: int | None = None
        if (
            read_snapshot is None
            and self._info.partition_spec is not None
            and self._info.read_snapshot_id is None
        ):
            head_before_resolve = self._namespace._catalog.refresh().head_snapshot_id

        if expected is not None:
            # Pre-flight fast-fail (optimization, not the guarantee):
            # re-resolve by name before paying for the parquet upload.
            info = self._check_incarnation(expected)  # current columns + spec
        else:
            # UNGUARDED: name-only resolution, and still a WRITER read —
            # it wants the columns and the spec, never the totals.
            info = self.info(totals=False)

        # The basis is SETTLED HERE, before a single byte is written, and
        # that is the whole point: everything below this line is the
        # parquet fanout and the upload, and a basis read after them would
        # exclude their entire duration from the conflict window.
        #
        # Three sources, in order of tightness. `read_snapshot_id` pairs
        # the spec the values are about to be computed under with the
        # window at the same instant. `head_before_resolve` is the head
        # taken before the resolve, so the window covers the resolve too.
        # The third reads head now — still pre-upload — and is reached
        # only when the cached shape looked unpartitioned and the fresh
        # one is partitioned, i.e. a spec was installed between the last
        # cache fill and this resolve. That install is at a snapshot <=
        # head so it is NOT inside the window, and it does not need to be:
        # the values below are computed under the spec the resolve just
        # returned, not under the stale one. What the window is for is a
        # FURTHER change after this point, which is exactly what it covers.
        if read_snapshot is None and info.partition_spec is not None:
            read_snapshot = (
                info.read_snapshot_id
                if info.read_snapshot_id is not None
                else head_before_resolve
                if head_before_resolve is not None
                else self._namespace._catalog.refresh().head_snapshot_id
            )

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
        # `table=self`: a re_prepare refusal (DDL, recreation, or a
        # read_snapshot below the floor) drops this Table's cached info,
        # so the caller's next append or prepare re-reads instead of
        # rebuilding the same doomed request.
        result = catalog._commit(payload, table=self)
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
        concurrency: int | None = None,
    ) -> dict[str, Any]:
        """Upload already partitioned/sorted local Parquet without loading it in RAM.

        The caller owns row-to-partition correctness and sort order, exactly as
        other footer-shipping writers do. Field IDs and schema must match the
        resolved table. Return an immutable commit request; persist it durably
        BEFORE calling ``Catalog.commit_prepared``. A failed prepare may orphan
        uploads, but cannot publish rows. Never regenerate files after preparing.
        With allow_optional_fields, external writers may use optional physical
        fields for required catalog columns only when footer counts prove no nulls.

        Each file is read only when its own upload runs — whole, by the
        uploading thread, when it is at or under
        :data:`~pyhoglake.upload.SINGLE_REQUEST_MAX_BYTES` (one
        ``PutObject`` instead of a three-request multipart upload), and in
        chunks otherwise. So a wide prepare holds ``concurrency`` files in
        memory, never all of them. ``concurrency`` (or
        ``PYHOGLAKE_UPLOAD_CONCURRENCY``) bounds how many are in flight;
        see :func:`~pyhoglake.upload.run_uploads` for what a mid-fanout
        failure does and :func:`~pyhoglake.upload.widen_io_threads` for
        the one process-global setting this touches.

        Orphan accounting: every exception out of this call carries what it
        already wrote, on the exception object itself (no new type, so
        existing ``except`` clauses keep working):

        ``uploaded_files``
            How many uploads COMPLETED — the object was written without
            error. A file whose upload raised partway is NOT counted, in
            either direction: the request may never have been accepted, or
            a multipart close may have failed over a TRUNCATED object that
            exists in the store. So the count is a lower bound on objects
            present, and the failing file must be treated as
            possibly-there. Uploads that were still in flight when
            another one failed are waited for and counted if they land.
        ``uploaded_uris``
            The uris of exactly those completed uploads, in the order the
            files were given (not the order they finished), always
            ``uploaded_files`` long.

        **Changed in the concurrent upload path** (it was a serial loop):
        this list is no longer a contiguous PREFIX of ``files``. Uploads
        run in parallel, so a fault can leave gaps — file 7 uploaded and
        file 6 not — and nothing can be inferred about a file from the
        position or the count. Only the uris themselves are meaningful:
        those objects exist, everything else either does not or (for the
        one that raised) cannot be known. The default fan-out is 64, so
        an existing caller gets this shape without asking; pass
        ``concurrency=1`` for the old serial behaviour.

        Every validation now runs before the first upload, so a refusal —
        of any file, not just the first — carries ``0`` / ``()``.
        Sweep the uris, not the ``{idempotency_key}/`` prefix: a retry
        under the same key writes new object names beside the old ones,
        so a prefix sweep after a later success deletes live files.
        """
        return self._prepare_append(
            files,
            from_tables=False,
            idempotency_key=idempotency_key,
            expected_table_uuid=expected_table_uuid,
            expected_table_info=expected_table_info,
            allow_optional_fields=allow_optional_fields,
            concurrency=concurrency,
        )

    def prepare_append_tables(
        self,
        groups: Sequence[tuple[pa.Table, tuple[str | None, ...] | None]],
        *,
        idempotency_key: str,
        expected_table_uuid: str | None = None,
        expected_table_info: TableInfo | None = None,
        concurrency: int | None = None,
    ) -> dict[str, Any]:
        """:meth:`prepare_append_files` without the disk: encode each
        already-partitioned Arrow table straight to a parquet buffer,
        upload the buffers, and return the same immutable commit request.

        For a writer that already holds its rows in Arrow — one table per
        partition tuple — a temp file is pure overhead: write, fsync,
        re-read for the footer, re-read for the upload, unlink. The
        registrations this produces are the ones the file path produces
        for the same rows (same path shape, ``record_count``,
        ``file_size_bytes``, ``footer_size``, ``column_stats``,
        ``partition_values``), because it is the same code reading the
        same footer — out of a buffer instead of off a disk.

        The caller still owns row-to-partition correctness, sort order,
        and the schema: each table's schema must be the destination's,
        field ids included (``columns_to_arrow_schema(table.columns)``
        builds it), and it is checked on the ENCODED footer by the same
        comparison a prepared file gets. Everything else about the
        prepared-append contract — persist the request before
        ``Catalog.commit_prepared``, never regenerate after preparing,
        the ``uploaded_files`` / ``uploaded_uris`` orphan accounting on
        every exception — is unchanged, so read
        :meth:`prepare_append_files` for it.

        **Memory.** Peak is the caller's Arrow input PLUS every encoded
        buffer, both alive at once. The buffers accumulate because each
        group is encoded before any upload starts (validating everything
        first is what keeps a refusal from orphaning objects), and the
        input tables are the caller's — this method never drops a
        reference, so nothing is released until the caller releases the
        sequence it passed in, after the call returns. Budget for both:
        the prod-us events writer's ~105K rows over ~271 partitions
        encode to a few MiB against ~1 GiB of Arrow input on 16 GiB
        pods. Nothing is held twice, though — there is no temp file, and
        the upload reads the buffer it was handed.

        Destinations with ``variant`` columns are refused: an Arrow
        rewrite drops the native Parquet VARIANT annotation, so those
        files have to come from a variant-aware writer through
        :meth:`prepare_append_files`.
        """
        return self._prepare_append(
            groups,
            from_tables=True,
            idempotency_key=idempotency_key,
            expected_table_uuid=expected_table_uuid,
            expected_table_info=expected_table_info,
            allow_optional_fields=False,
            concurrency=concurrency,
        )

    def _prepare_append(
        self,
        groups: Sequence[tuple[Any, tuple[str | None, ...] | None]],
        *,
        from_tables: bool,
        idempotency_key: str,
        expected_table_uuid: str | None,
        expected_table_info: TableInfo | None,
        allow_optional_fields: bool,
        concurrency: int | None,
    ) -> dict[str, Any]:
        """The prepared-append path both public entry points are: resolve,
        validate every group, upload them all, build the commit request.

        The two differ only in where a group's parquet comes from — a file
        the caller wrote, or a buffer this encodes — which is one branch
        in the loop. Validation, uri shape, registration fields, upload
        fan-out and orphan accounting are shared BECAUSE they must not
        drift: a registration that differs between the two paths is a
        wire-shape bug nothing else would catch.

        Validation of every group precedes the first upload (the serial
        loop this replaced interleaved them), so a refusal orphans
        nothing and the fan-out is handed work already known to be good.
        """
        require_parquet(self._info.properties, "Table.prepare_append_files")
        uploaded: list[str] = []
        try:
            _uuid.UUID(idempotency_key)
            catalog = self._namespace._catalog
            client = catalog._client
            expected = expected_table_uuid or self.table_uuid
            if expected_table_info is None:
                # The cached-read path: info and conflict basis from the
                # same read, so the steady state is zero GETs per flush.
                info, read_snapshot = self._prepared_read(expected)
            else:
                # expected_table_info OPTS OUT of the cache, on purpose:
                # its contract is "refuse if the DESTINATION's layout is
                # not the one I planned against", and comparing the
                # caller's copy with this client's own cache would be a
                # comparison of two client-side values. Same two reads
                # as before, same order (head first — see
                # _prepared_read).
                read_snapshot = catalog.refresh().head_snapshot_id
                info = self._check_incarnation(expected)
            require_parquet(info.properties, "Table.prepare_append_files")
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
            if has_variant and from_tables:
                raise ValidationError(
                    "cannot prepare Arrow tables for a destination with variant "
                    "columns: an Arrow rewrite loses the native Parquet VARIANT "
                    "annotation — use prepare_append_files with files a "
                    "variant-aware writer produced",
                    status_code=None,
                )
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
            # Resolved before any encode or footer read so a bad
            # setting costs nothing. The object-store clients are NOT
            # built yet: doing that here would answer an empty or
            # malformed prepare with "no S3 configuration" instead of
            # naming the caller's actual mistake.
            fanout = resolve_concurrency(len(groups), concurrency)
            registrations = []
            uploads: list[Upload] = []
            for index, (source, partition) in enumerate(groups):
                part = (
                    _encode_group(source, schema)
                    if from_tables
                    else _describe_prepared_file(
                        source,
                        info,
                        schema,
                        has_variant=has_variant,
                        allow_optional_fields=allow_optional_fields,
                    )
                )
                arity = len(info.partition_spec.fields) if info.partition_spec else 0
                if (arity and (partition is None or len(partition) != arity)) or (
                    not arity and partition is not None
                ):
                    raise ValidationError(
                        "prepared file partition arity differs from destination",
                        status_code=None,
                    )
                if part.metadata.num_rows <= 0:
                    raise ValidationError(
                        "prepared file must contain rows", status_code=None
                    )
                uri = f"{catalog.data_path.rstrip('/')}/data/{info.namespace}/{info.name}/{idempotency_key}/{_uuid.uuid4()}-{index}.parquet"
                uploads.append(
                    Upload(uri=uri, size=part.size, body=part.body, path=part.path)
                )
                reg: dict[str, Any] = {
                    "path": uri,
                    "record_count": part.metadata.num_rows,
                    "file_size_bytes": part.size,
                    "footer_size": part.footer_size,
                    "column_stats": [
                        stat.to_wire()
                        for stat in extract_column_stats(part.metadata, info.columns)
                    ],
                }
                if partition is not None:
                    reg["partition_values"] = list(partition)
                registrations.append(reg)
            if not registrations:
                raise ValidationError(
                    "prepared append must contain files", status_code=None
                )
            # Everything is validated; now pay for the clients. The pool
            # is sized to the fan-out that is actually about to run.
            filesystem = client._filesystem()
            put_client = client._put_client(fanout)
            widen_io_threads_for(uploads, put_client, fanout)
            run_uploads(
                lambda upload: perform_upload(filesystem, put_client, upload),
                uploads,
                concurrency=fanout,
                completed=uploaded,
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
        mechanism.

        NOT on every writer path: ``prepare_append_files`` skips this read
        entirely while its cache is warm, so the only check before the
        upload there is the cached uuid against the caller's expectation
        (two client-side values). The server's guard still catches a
        recreation, and it costs that one flush's uploads. ``self._info`` is only adopted when the incarnation
        matches, so the pinned identity (and the default
        ``expected_table_uuid`` of later appends) is never silently
        rebased onto a recreated table.

        ``totals=false``: this is a WRITER read and it wants the uuid,
        the columns and the specs — never the live totals, whose scan is
        the whole cost of the call (#232)."""
        body = self._namespace._catalog._client._request(
            "GET", self._path(), params={"totals": "false"}
        )
        info = TableInfo.from_wire(body)
        if info.table_uuid != expected_uuid:
            raise IncarnationChangedError(
                f"table {self._namespace._catalog.name}/{self.namespace}."
                f"{self.name} was recreated: expected table_uuid "
                f"{expected_uuid}, name now resolves to {info.table_uuid}. "
                "Refusing to append across incarnations.",
                table=f"{self.namespace}.{self.name}",
            )
        self._adopt(info)
        return info

    def _cache_is_usable(self) -> bool:
        """Whether the cached info can be prepared against without a read.

        Two conditions, and both are about `read_snapshot`:

        * the cache must KNOW the snapshot it was read at. A server that
          predates ``Table.read_snapshot_id`` sends none, so there is
          nothing to send as a read_snapshot and the writer path falls
          back to what it always did;
        * the cache must be younger than half the catalog's snapshot
          retention. An old read_snapshot is not a correctness problem —
          the server evaluates the conflict window over
          `hog_snapshot_change_conflict (catalog_id, object_id, kind,
          snapshot_id)`, a range over ONE table's change rows, so it
          costs the same whether it reaches back ten snapshots or ten
          thousand — but once it sinks BELOW the expiry floor the commit
          is a 410, and by then the flush's parquet is already uploaded.
          One refusal per writer per retention period would be a
          re-encode and a set of orphaned objects each time, so the cache
          refreshes at half the retention instead. On
          `snapshot_retention_seconds = 3600` (prod-us) that is one
          refresh per 30 minutes per table, against a flush rate of many
          per minute.

        The age is the age of the READ, not of the snapshot, and what it
        bounds is how far the floor can move AFTER the read — NOT how far
        the read is from head. On a BUSY catalog those coincide, because
        retention is measured in snapshot time and snapshot time advances
        with wall clock: half the retention of slack is half a retention
        period of headroom, which is the design.

        On an IDLE catalog they diverge without limit, and this threshold
        does not protect the flush. `ExpiryService` sets the floor to
        `min(firstFresh, head, earliest + batchSize)`, where `firstFresh`
        is the oldest snapshot still inside the retention window. Let
        retention be an hour and the catalog be idle for three: every
        snapshot 1..100 is outside the window, a writer reads and caches
        head = 100 with age zero, one unrelated commit lands as 101, and
        the next sweep computes `firstFresh = 101` — so the floor jumps to
        101 and expires a snapshot read seconds ago. No cache age avoids
        this; only a read after that commit does.

        It stays a non-issue because the cost is bounded, not because it
        cannot happen: the commit is refused 410, the refusal invalidates
        the cache, the re-prepare reads fresh, and the price is ONE
        orphaned parquet for ONE flush, on a catalog that by construction
        flushes rarely. Which is also the reason not to read this
        threshold as a safety margin against the floor and raise it: it
        buys headroom on the busy catalogs, where the arithmetic holds,
        and nothing at all on the idle ones.
        """
        if self._cache is None:
            return False
        _, read_at = self._cache
        return (
            time.monotonic() - read_at
            < self._namespace._catalog._retention_seconds() / 2
        )

    def _prepared_read(self, expected_uuid: str) -> tuple[TableInfo, int]:
        """The (info, read_snapshot) a prepare binds its files to.

        THE POINT (#232/#233): the info and the conflict basis come from
        the SAME read, so a prepared append needs no per-flush table GET
        and no new wire field. The server already validates the pairing —
        an append committed with `read_snapshot = S` is accepted exactly
        when nothing has altered, dropped or recreated the table since S
        (CommitService.checkConflicts), which is the entire question the
        writer was re-reading the table to answer. Anything that has is
        one of the typed `re_prepare` refusals, which invalidates this
        cache.

        The old shape was two GETs per flush: a catalog GET for head and
        a table GET whose live-totals scan (a count and two sums over
        every live file row, ~10M rows on prod-us) was the reason #232
        exists. Now it is zero, plus one identity read per half-retention.

        ONE DRIFT `read_snapshot` DOES NOT COVER, and it matters: a
        DROP + RECREATE under the same name. The server's conflict check
        keys on the RESOLVED (new) table id, and `table_created` is only a
        conflict for a guarded request's delete targets, so a recreate is
        outside its window for an append. It is closed solely by
        ``expected_table_uuid`` -> 409 `table_recreated`, which holds
        because this function always binds a real uuid: `prepare_*` takes
        `expected_table_uuid or self.table_uuid` and its signature does
        not accept ``UNGUARDED``. A future prepare that allowed UNGUARDED
        would silently reopen it, so it must not use this path.
        """
        if self._cache is not None and self._cache_is_usable():
            if self._info.table_uuid != expected_uuid:
                raise IncarnationChangedError(
                    f"table {self._namespace._catalog.name}/{self.namespace}."
                    f"{self.name} was recreated: expected table_uuid "
                    f"{expected_uuid}, cached identity is {self._info.table_uuid}. "
                    "Refusing to append across incarnations.",
                    table=f"{self.namespace}.{self.name}",
                )
            return self._info, self._cache[0]
        # No usable cache. The catalog GET comes FIRST and the order is
        # load-bearing: a read_snapshot taken AFTER the identity read
        # would sit above any DDL that landed between the two, putting it
        # outside the conflict window and silently defeating the guard.
        # (This is the order main has always used, for the same reason.)
        # It is wasted work against a server that reports the resolved
        # snapshot on a read — one GET, on the refresh path only, never
        # per flush — and paying it is cheaper than learning the server's
        # capability by guessing.
        head = self._namespace._catalog.refresh().head_snapshot_id
        info = self._check_incarnation(expected_uuid)
        # Prefer the snapshot the info was actually RESOLVED at: it pairs
        # the shape and the conflict basis exactly, and it is >= head.
        return (
            info,
            info.read_snapshot_id if info.read_snapshot_id is not None else head,
        )


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


@dataclass(frozen=True)
class _PreparedPart:
    """One prepared group, measured and validated, ready to register.

    ``metadata`` is the parquet footer the registration's ``record_count``
    and ``column_stats`` come from; ``size`` and ``footer_size`` are the
    wire fields the server's tail read depends on. Exactly one of ``body``
    (an in-memory buffer this process encoded) and ``path`` (a file the
    caller wrote, still unread) says where the bytes are.
    """

    metadata: Any
    size: int
    footer_size: int
    body: Any | None = None
    path: str | None = None


def _encode_group(data: pa.Table, schema: pa.Schema) -> _PreparedPart:
    """Encode one group's parquet into a buffer — never to disk — and read
    back everything the registration needs out of that same buffer.

    The schema check is the prepared-FILE check, run on the ENCODED
    footer rather than on the caller's Arrow schema: same helper, same
    error text, so a table that would have been refused as a file is
    refused here, and conversions the writer performs (timestamp_s stored
    as milliseconds) are compared as written rather than as intended.
    """
    sink = pa.BufferOutputStream()
    pq.write_table(data, sink)
    raw = sink.getvalue()
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        if not prepared_schema_matches(parquet.schema_arrow, schema):
            raise ValidationError(
                "prepared Parquet schema/field IDs differ from destination",
                status_code=None,
            )
        metadata = parquet.metadata
    # Only the 8-byte trailer is copied out of the buffer, so the one
    # definition of the footer_size wire convention keeps its bytes
    # signature and stays shared with the file path.
    return _PreparedPart(
        metadata=metadata,
        size=raw.size,
        footer_size=_footer_size(bytes(raw[-8:])),
        body=raw,
    )


def _describe_prepared_file(
    path: str,
    info: TableInfo,
    schema: pa.Schema,
    *,
    has_variant: bool,
    allow_optional_fields: bool,
) -> _PreparedPart:
    """Validate and measure one prepared file WITHOUT reading its data:
    the footer for schema and stats, the 8-byte trailer for
    ``footer_size``, the file length for ``file_size_bytes``.

    The bytes stay on disk until this file's upload runs, which is what
    keeps a wide prepare's memory proportional to the fan-out instead of
    to the flush.
    """
    with pq.ParquetFile(path) as parquet:
        if has_variant or allow_optional_fields:
            validate_variant_file(path, parquet, info.columns)
        elif not prepared_schema_matches(parquet.schema_arrow, schema):
            raise ValidationError(
                "prepared Parquet schema/field IDs differ from destination",
                status_code=None,
            )
        metadata = parquet.metadata
    with open(path, "rb") as source:
        source.seek(0, 2)
        size = source.tell()
        source.seek(-8, 2)
        trailer = source.read(8)
    return _PreparedPart(
        metadata=metadata,
        size=size,
        footer_size=_footer_size(trailer),
        path=path,
    )


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
    """``Table.append``'s upload: pyarrow's streaming multipart write,
    unchanged.

    Deliberately NOT the prepared path's single-request upload. That
    would change the transport of the library's most-used method for
    every install with boto3 importable, and buy only 3 requests -> 1 per
    file: append writes its per-partition files in a serial loop, so it
    gains none of the concurrency that makes the prepared path's upload
    change worth its risk. It belongs in its own change, with its own
    verification.
    """
    with fs.open_output_stream(s3_key(uri)) as out:
        out.write(raw)
