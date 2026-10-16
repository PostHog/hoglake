"""The flush: staged rows → parquet → hoglake commits → staged rows deleted.

Per stage, the due groups not already in flight, under a process-wide
semaphore (:class:`FlushRunner`): a size-lane or backlog group alone,
the others packed into batches whose files share commits
(:meth:`HoglakeFlusher.flush_batch`). Per group:

1. Scan a bounded window of the group's rows: a key prefix, at most
   ``flush_bytes`` of payload and always at least one row (stage.py
   ``PartitionStage.scan``).
2. Build parquet (pure, on the I/O pool): decode each payload against
   the table's columns (a record no column can hold is dropped and
   counted, never batch-fatal), sort by the live sort spec, fan out by
   the live partition spec, one file per tuple.
3. Upload every file, then commit. Each commit carries a FRESH uuid4
   idempotency key minted per build; an ambiguous answer (5xx, 408,
   429, a plain 409 conflict, a transport error) resends the identical
   bytes under the same key, which the server's receipt dedupes. More
   files than ``max_files_per_commit`` split into several commits.
4. Delete exactly the scanned keys in one durable batch (a batch's
   groups together).

At-least-once: a row is deleted only after every commit naming it
succeeded (or it was a counted drop). A crash or a failed delete after a
commit landed re-flushes the window under a new key: a duplicate, never
a loss.

Failure handling is one rule: any failure fails the decision, the rows
stay staged, and the group is retried with backoff (a refused batch
retries each of its groups alone; a group whose build fails leaves its
batch); after
:data:`STUCK_AFTER` consecutive failures it is logged at ERROR and
counted, and retries continue. Two conditions stop the process: the
destination table was recreated (:class:`FlushHalted`, exit 3) and a
stage was fenced by a newer writer (exit 4).
"""

from __future__ import annotations

import asyncio
import importlib.metadata
import logging
import struct
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from concurrent.futures import Executor, ThreadPoolExecutor
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from functools import partial
from typing import Any, Final, Literal, Protocol, TypeVar
from urllib.parse import quote

import httpx
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyhoglake import (
    Catalog,
    Column,
    CommitConflictError,
    CommitResult,
    DdlSinceReadSnapshotError,
    HoglakeClient,
    HoglakeError,
    IncarnationChangedError,
    Namespace,
    ReadSnapshotExpiredError,
    Table,
    TableInfo,
    UnsupportedTypeError,
    ValidationError,
)
from pyhoglake.models import PartitionSpec
from pyhoglake.parquet_schema import prepared_schema_matches
from pyhoglake.stats import extract_column_stats
from pyhoglake.transforms import (
    partition_source_array,
    transform_strings,
    transform_value,
)
from pyhoglake.types import column_to_arrow_field
from pyhoglake.upload import (
    Upload,
    perform_upload,
    resolve_concurrency,
    run_uploads,
    widen_io_threads_for,
)

from . import keyspace
from .coerce import (
    REASON_UNCASTABLE_COLUMN,
    DecodeError,
    UnsupportedColumnError,
    coercer_for,
    load_json_object,
)
from .config import Config
from .grouping import column_chains, field_names
from .planner import PlannerKnobs, decide, due_floor_ts
from .stage import (
    DroppedRecord,
    FencedError,
    PartitionStage,
    RowStream,
    Scan,
    StageClosedError,
    StageManager,
    is_fenced,
)

log = logging.getLogger(__name__)

_R = TypeVar("_R")

# The commit ladder: ambiguous or retryable answers resend the identical
# bytes under the same key.
_PUBLISH_MAX_ATTEMPTS: Final = 8
_CONFLICT_MAX_ATTEMPTS: Final = 5
_BASE_DELAY_S: Final = 0.5
_MAX_DELAY_S: Final = 30.0
# Only statuses that mean "not now" may slow a retry down.
_RETRY_AFTER_STATUS: Final = frozenset({429, 503})
# pyhoglake's assumed retention when the catalog will not say; the shape
# TTL is a quarter of it.
_ASSUMED_RETENTION_S: Final = 1800.0
#: Builds per decision when the server refuses the build's read basis
#: (a DDL since the read snapshot, or the snapshot expired): each
#: rebuild re-reads the shape and mints a fresh key.
_REBUILD_ATTEMPTS: Final = 3
#: A file at least this large registers with footer stats; smaller files
#: register deferred and the server's hydrator backfills.
FOOTER_STATS_MIN_BYTES: Final = 32 * 1024 * 1024
#: Consecutive failures after which a group is reported stuck (ERROR
#: log, counter, gauge). Retries continue at the capped backoff.
STUCK_AFTER: Final = 3
_RETRY_BASE_S: Final = 2.0
_RETRY_MAX_S: Final = 120.0

_ORPHAN_URIS_LOGGED: Final = 20
_CLAMP_LOG_INTERVAL_S: Final = 60.0
_DROPPED_OFFSETS_LOGGED: Final = 20

_VERSION: str | None = None


def _millrace_version() -> str:
    global _VERSION
    if _VERSION is None:
        try:
            _VERSION = importlib.metadata.version("millrace")
        except importlib.metadata.PackageNotFoundError:  # pragma: no cover
            _VERSION = "unknown"
    return _VERSION


# -- errors --------------------------------------------------------------------


class FlushError(Exception):
    """A flush decision failed; its rows stay staged and the group is
    retried."""


class FlushStartupError(FlushError):
    """Startup resolution or validation of the destination failed."""


class FlushHalted(FlushError):
    """The pipeline must stop for a human: ``table_recreated`` (the
    destination name now resolves to another incarnation) or
    ``server_capability`` (the server stopped reporting
    ``read_snapshot_id``). Staged rows stay durable."""

    def __init__(self, reason: str, detail: str) -> None:
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail


class CommitRefused(FlushError):
    """The server answered a 4xx that waiting cannot change (a 400/422
    payload refusal, 401, 403, 404, 413, a dropped table)."""

    def __init__(self, status: int | None, message: str) -> None:
        super().__init__(message)
        self.status = status


class CommitExhausted(FlushError):
    """The commit ladder ran out on ambiguous or retryable answers."""


# -- counters ------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FlushStats:
    """Cumulative flush counters (observability only)."""

    decisions_committed: int = 0
    decisions_dropped: int = 0
    decisions_failed: int = 0
    commits: int = 0
    commit_attempts: int = 0
    commit_retries: int = 0
    commit_refusals: Mapping[str, int] = dataclass_field(default_factory=dict)
    records_dropped: Mapping[str, int] = dataclass_field(default_factory=dict)
    rows_published: int = 0
    files_written: int = 0
    bytes_written: int = 0
    orphaned_uploads: int = 0
    ambiguous_uploads: int = 0
    dropped_payload_keys: int = 0
    values_clamped: Mapping[str, int] = dataclass_field(default_factory=dict)


# -- the payload decoder --------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DecodedRecord:
    """One staged payload decoded to column-aligned values. ``values``
    holds every destination column (null where the payload lacked a
    nullable key); ``dropped_keys`` are payload keys the table does not
    have (dropped, never silently: the flusher counts and logs them)."""

    values: dict[str, Any]
    dropped_keys: tuple[str, ...]


class PayloadDecoder(Protocol):
    """Staged payload bytes → one row's column values, aligned to the
    destination's columns. v1 is JSON objects; a schema-registry
    (Avro/Proto) decoder slots in behind this protocol (design doc §Open
    questions)."""

    def decode_record(self, payload: bytes) -> DecodedRecord:
        """Decode one staged payload. Raises :class:`DecodeError` —
        per-record, never the flush's death."""
        ...


class JsonObjectDecoder:
    """v1 decoder: staged payloads are JSON objects; keys are column
    names. Defensive per record (the flush drops failures, never
    crashes): strict JSON (no NaN/Infinity literals — and no
    magnitude-overflow literals: ``1e400`` parses to ``inf`` without
    tripping the parse hook, and the float coercion refuses anything
    non-finite), per-column coercion with int64-per-unit bounds,
    missing nullable keys → null, missing/explicit-null non-nullable
    keys → failure, unknown keys dropped (counted and logged by the
    flusher)."""

    def __init__(self, columns: tuple[Column, ...] | Sequence[Column]) -> None:
        self._columns = tuple(columns)
        #: Timestamps clamped to years 1–9999, by dotted column path.
        self.clamped: dict[str, int] = {}
        self._plans = tuple(
            (c, coercer_for(c, on_clamp=self._note_clamp)) for c in self._columns
        )
        self._names = frozenset(c.name for c in self._columns)
        # The Arrow schema is built once here too, so a column the batch
        # build could not describe (a malformed container) is a startup
        # refusal like any other.
        arrow_schema(self._columns)

    @property
    def columns(self) -> tuple[Column, ...]:
        return self._columns

    def _note_clamp(self, path: str) -> None:
        self.clamped[path] = self.clamped.get(path, 0) + 1

    def decode_record(self, payload: bytes) -> DecodedRecord:
        doc = load_json_object(payload)
        values: dict[str, Any] = {}
        for column, coerce in self._plans:
            raw = doc.get(column.name)
            if raw is None:
                if not column.nullable:
                    raise DecodeError(
                        f"column {column.name!r} is not nullable and the "
                        "payload has no usable value",
                        reason=REASON_UNCASTABLE_COLUMN,
                        path=column.name,
                    )
                values[column.name] = None
                continue
            try:
                values[column.name] = coerce(raw)
            except DecodeError as e:
                # A nested failure already names its child's path; a
                # top-level scalar's is the column itself.
                if e.path is None:
                    e.path = column.name
                raise
        dropped = tuple(sorted(k for k in doc if k not in self._names))
        return DecodedRecord(values=values, dropped_keys=dropped)


# -- Arrow building -------------------------------------------------------------


def arrow_schema(columns: Sequence[Column]) -> pa.Schema:
    """The batch's Arrow schema: pyhoglake's
    (:func:`~pyhoglake.types.column_to_arrow_field` — field ids on every
    level). Raises :class:`UnsupportedColumnError` naming a column it
    cannot describe."""
    fields = []
    for column in sorted(columns, key=lambda c: c.ordinal):
        try:
            fields.append(column_to_arrow_field(column))
        except (UnsupportedTypeError, ValidationError) as e:
            raise UnsupportedColumnError(
                f"column {column.name!r} has no Arrow form this writer can build: {e}"
            ) from e
    return pa.schema(fields)


def _build_type(t: pa.DataType) -> pa.DataType:
    """The type ``pa.array`` builds a column's coerced Python values at,
    before ONE cast to the catalog type — the single place the leaf
    conversions live, at any depth: ``uuid`` is built as its 16-byte
    storage (pyarrow cannot build an extension value from Python inside
    a container), ``json`` as its string storage, ``timestamptz`` as the
    naive micros it is coerced to (no int → tz kernel). Field names,
    nullability and field-id metadata are kept, so the cast lands on the
    catalog type exactly. A type that needs none of this is returned as
    is (equal, and the cast is then skipped)."""
    if isinstance(t, pa.BaseExtensionType):
        return t.storage_type
    if pa.types.is_timestamp(t) and t.tz is not None:
        return pa.timestamp(t.unit)
    if pa.types.is_struct(t):
        fields = [_build_field(t.field(i)) for i in range(t.num_fields)]
        if all(f.type == t.field(i).type for i, f in enumerate(fields)):
            return t
        return pa.struct(fields)
    if pa.types.is_map(t):
        key, item = _build_field(t.key_field), _build_field(t.item_field)
        if key.type == t.key_type and item.type == t.item_type:
            return t
        return pa.map_(key, item, keys_sorted=t.keys_sorted)
    if pa.types.is_list(t):
        value = _build_field(t.value_field)
        if value.type == t.value_type:
            return t
        return pa.list_(value)
    return t


def _build_field(f: pa.Field) -> pa.Field:
    built = _build_type(f.type)
    return f if built == f.type else f.with_type(built)


def _array_for(column: Column, arrow_type: pa.DataType, values: list[Any]) -> pa.Array:
    """One column's Arrow array from coerced values, at exactly the type
    :func:`arrow_schema` gives the column (so ``Table.from_arrays``
    against that schema needs no cast): built at :func:`_build_type` and
    cast once."""
    build = _build_type(arrow_type)
    if build == arrow_type:
        return pa.array(values, type=arrow_type)
    return pa.array(values, type=build).cast(arrow_type)


def _encode_type(t: pa.DataType) -> pa.DataType:
    """``t`` with every ``timestamp[s]`` — at any depth — stored as
    milliseconds (Parquet has no seconds unit)."""
    if t == pa.timestamp("s"):
        return pa.timestamp("ms")
    if pa.types.is_struct(t):
        return pa.struct([_encode_field(t.field(i)) for i in range(t.num_fields)])
    if pa.types.is_map(t):
        return pa.map_(
            _encode_field(t.key_field),
            _encode_field(t.item_field),
            keys_sorted=t.keys_sorted,
        )
    if pa.types.is_list(t):
        return pa.list_(_encode_field(t.value_field))
    return t


def _encode_field(f: pa.Field) -> pa.Field:
    return f.with_type(_encode_type(f.type))


def _map_entries_type(t: pa.DataType) -> pa.DataType:
    """``t`` with every map's entries struct under Arrow's default name.
    A parquet round trip names a map's entries after the COLUMN (pyarrow
    reads ``map<k, v ('m')>`` back for a column ``m`` it wrote as
    ``entries``), and Arrow type equality compares that name; nothing
    else about the type, field ids included, is touched."""
    if pa.types.is_map(t):
        return pa.map_(
            t.key_field.with_type(_map_entries_type(t.key_type)),
            t.item_field.with_type(_map_entries_type(t.item_type)),
            keys_sorted=t.keys_sorted,
        )
    if pa.types.is_struct(t):
        return pa.struct(
            [
                t.field(i).with_type(_map_entries_type(t.field(i).type))
                for i in range(t.num_fields)
            ]
        )
    if pa.types.is_list(t):
        return pa.list_(t.value_field.with_type(_map_entries_type(t.value_type)))
    return t


def file_schema(schema: pa.Schema) -> pa.Schema:
    """A written file's Arrow schema, comparable with the encoding schema
    (:func:`_map_entries_type`)."""
    return pa.schema(
        [f.with_type(_map_entries_type(f.type)) for f in schema],
        metadata=schema.metadata,
    )


def _encode_schema(schema: pa.Schema) -> pa.Schema:
    """The parquet-encoding schema: the catalog schema with field ids,
    plus the one storage conversion pyhoglake's writer path makes —
    Parquet has no seconds timestamp unit, so ``timestamp_s`` is stored
    as milliseconds (mirrors ``Table._prepare_append``, which does it for
    top-level columns; this does it at every depth)."""
    return pa.schema(
        [_encode_field(field) for field in schema], metadata=schema.metadata
    )


def _build_arrow(
    records: Sequence[DecodedRecord], columns: tuple[Column, ...]
) -> pa.Table:
    """Column-aligned records → the Arrow table at the catalog's schema
    (field ids embedded). Rows keep SCAN order here — Kafka offset order
    within the group; :func:`build_prepared_plan` then
    sorts the batch by the live catalog sort spec (payload event time)
    before the partition fanout — the sort, not the scan order, is what
    the ingest-sort / catalog-sort-spec convergence rests on
    (docs/kafka-ingestion.md §What the server owns)."""
    schema = arrow_schema(columns)
    arrays = [
        _array_for(
            column,
            schema.field(column.name).type,
            [r.values[column.name] for r in records],
        )
        for column in columns
    ]
    return pa.Table.from_arrays(arrays, schema=schema)


#: The catalog sort spec's wire vocabulary → Arrow's (pyhoglake
#: ``SortField.direction`` / ``null_order``, spec ``SortField`` schema).
#: Null placement is INDEPENDENT of direction (Iceberg's rule, and the
#: server comparator's — ParquetRewriter), and Arrow takes it per key.
_SORT_DIRECTIONS: Final = {"asc": "ascending", "desc": "descending"}
_SORT_NULL_ORDERS: Final = {"nulls_first": "at_start", "nulls_last": "at_end"}


#: Column types that are no partition or sort source: containers have no
#: single value per row, and a json value has no order or transform this
#: writer (or the server) defines.
_UNORDERED_TYPES: Final = frozenset({"struct", "list", "map", "json"})
_REPEATED_TYPES: Final = frozenset({"list", "map"})


def _source_fault(chain: tuple[Column, ...]) -> str | None:
    """Why a partition/sort source chain cannot be read as one scalar per
    row, or None: a container or json leaf, or a leaf under a
    list or a map (many values per row). A struct leaf is fine — it is
    read by path."""
    leaf = chain[-1]
    path = ".".join(c.name for c in chain)
    if leaf.type in _UNORDERED_TYPES:
        return f"{path!r} is a {leaf.type!r} column"
    repeated = next((c for c in chain[:-1] if c.type in _REPEATED_TYPES), None)
    if repeated is not None:
        return (
            f"{path!r} sits under {repeated.name!r}, a {repeated.type!r}: a row "
            "has many such values"
        )
    return None


def _spec_sort_keys(info: TableInfo) -> list[tuple[tuple[str, ...], str, str]]:
    """The table's LIVE sort spec as Arrow sort keys — ``(column path,
    direction, null placement)`` per field, in spec order (a struct leaf
    is its root-to-leaf name path); ``[]`` for an unsorted table.

    A field that does not resolve to a live column, resolves to one that
    is not one scalar per row (a container, json, or a leaf
    under a list or a map), or carries a direction/null_order outside the
    wire vocabulary, is a shape this writer cannot honor — refused
    LOUDLY (the same posture as the partition-spec guard below), never
    silently unsorted: an unsorted file under a sorted spec is exactly
    what compaction's sortedness check demotes, so "looked odd, skipped
    it" would tax every file forever. Startup validation
    (:meth:`HoglakeFlusher.resolve`) refuses the same shapes fail-fast;
    this guard covers a spec that changed under a warm shape cache.
    """
    spec = info.sort_spec
    if spec is None or not spec.fields:
        return []
    chains = column_chains(info.columns)
    keys: list[tuple[tuple[str, ...], str, str]] = []
    for field in spec.fields:
        chain = chains.get(field.source_field_id)
        if chain is None:
            raise FlushError(
                f"sort spec (sort_id={spec.sort_id}) references field_id "
                f"{field.source_field_id}, which is not a live column of "
                f"{info.namespace}.{info.name}"
            )
        fault = _source_fault(chain)
        if fault is not None:
            raise FlushError(
                f"sort spec (sort_id={spec.sort_id}) field "
                f"{field.source_field_id} cannot be a sort source: {fault}"
            )
        direction = _SORT_DIRECTIONS.get(field.direction)
        null_order = _SORT_NULL_ORDERS.get(field.null_order)
        if direction is None or null_order is None:
            raise FlushError(
                f"sort spec (sort_id={spec.sort_id}) field "
                f"{field.source_field_id} carries direction="
                f"{field.direction!r} null_order={field.null_order!r}, "
                "outside the asc/desc × nulls_first/nulls_last vocabulary"
            )
        keys.append((tuple(c.name for c in chain), direction, null_order))
    return keys


def _sort_batch_by_spec(
    data: pa.Table, keys: list[tuple[tuple[str, ...], str, str]]
) -> pa.Table:
    """Sort the encoded batch by the catalog's sort keys — native Arrow
    compute (``pc.sort_indices`` + ``take``; ``take`` preserves the
    schema's field-id metadata).

    The keys are computed into a SHADOW table, one temporary column per
    key, and only the permutation is applied to ``data`` (so nothing is
    added to or dropped from the batch). A struct-leaf key is the leaf
    read by path (``pc.struct_field``; a null struct on the path is a
    null key). Extension-typed keys (``uuid``) have no Arrow sort
    kernel, so they sort on their STORAGE — fixed-size binary compares
    unsigned-lexicographic in Arrow exactly as the server comparator
    reads the same physical bytes (ParquetRewriter's
    BINARY/FIXED_LEN_BYTE_ARRAY arm), so the orders agree. NaN cannot
    appear (the decoder refuses non-finite floats), so Arrow's NaN
    placement never diverges from the server's NaN-greatest rule here.
    """
    names: list[str] = []
    arrays: list[pa.ChunkedArray] = []
    sort_keys: list[tuple[str, str, str]] = []
    for i, (path, direction, null_order) in enumerate(keys):
        column: pa.ChunkedArray = data.column(path[0])
        for step in path[1:]:
            column = pc.struct_field(column, step)
        if isinstance(column.type, pa.BaseExtensionType):
            column = pa.chunked_array(
                [chunk.storage for chunk in column.chunks],
                type=column.type.storage_type,
            )
        name = f"__millrace_sort_{i}"
        names.append(name)
        arrays.append(column)
        sort_keys.append((name, direction, null_order))
    shadow = pa.table(dict(zip(names, arrays, strict=True)))
    return data.take(pc.sort_indices(shadow, sort_keys=sort_keys))


#: What the Arrow batch build (array construction plus the parquet-encoding
#: cast) can raise on a single bad VALUE. Coercion is total — every bad
#: value is a per-record :class:`DecodeError` BEFORE the build — so a
#: failure here is encoder drift (a coercion that under-refuses), and the
#: build isolates the guilty record rather than losing the batch.
_BUILD_FAILURES: Final = (pa.ArrowException, OverflowError)
#: The row-index column carried through the sort and fanout.
_ROW_INDEX: Final = "__millrace_row"


def _build_batch(
    records: Sequence[DecodedRecord], columns: tuple[Column, ...]
) -> tuple[pa.Table, pa.Table]:
    """The batch's Arrow table and its parquet-encoding cast (the two
    build steps a value can still fail)."""
    table = _build_arrow(records, columns)
    return table, table.cast(_encode_schema(table.schema))


def _first_unbuildable(
    records: Sequence[DecodedRecord], columns: tuple[Column, ...]
) -> int | None:
    """The index of the first record that fails the batch build ON ITS
    OWN, or None. Arrow array builds convert per value, so a record that
    fails alone is exactly the record that fails the batch. This probe
    only ever runs on the failure path — never in the steady state."""
    for index, record in enumerate(records):
        try:
            _build_batch([record], columns)
        except _BUILD_FAILURES:
            return index
    return None


def _build_batch_or_raise(
    good: Sequence[tuple[keyspace.RowKey, bytes, DecodedRecord]],
    columns: tuple[Column, ...],
) -> pa.Table:
    """The batch's parquet-encoding table. Coercion is total, so a build
    failure here is a millrace encoder bug, never bad data: it raises
    (naming the first record that fails alone, if any) and the decision
    fails with every row still staged, rather than dropping a row that
    is representable."""
    records = [r for _, _, r in good]
    try:
        return _build_batch(records, columns)[1]
    except _BUILD_FAILURES as e:
        index = _first_unbuildable(records, columns)
        where = (
            "no single record reproduces it"
            if index is None
            else f"the record at offset {good[index][0].offset} fails alone"
        )
        raise FlushError(
            f"the Arrow batch build failed ({where}): a millrace encoder bug; "
            "the rows stay staged"
        ) from e


def _partition_groups(
    data: pa.Table, info: TableInfo, spec: PartitionSpec
) -> list[tuple[tuple[str | None, ...], pa.Table]]:
    """Split the batch by partition tuple under the table's live spec:
    one (wire-string tuple, sub-table) per distinct tuple, ordered by
    first occurrence (so file registration — and the server's
    rows-then-offset row-id assignment — follows scan order).

    The transform math is pyhoglake's (Iceberg semantics have exactly one
    implementation); the grouping is millrace's — pyhoglake's own
    ``_partition_groups`` and millpond's are the same shape, kept local
    because the prepared path puts row-to-partition correctness on the
    caller. A null source value forms its own group, per Iceberg; a
    struct-leaf source under a null struct is a null source value.
    """
    chains = column_chains(info.columns)
    key_names = [f"__millrace_pk_{i}" for i in range(len(spec.fields))]
    key_arrays: list[pa.Array] = []
    for field in spec.fields:
        chain = chains.get(field.source_field_id)
        if chain is None:  # startup validation refuses this; re-refuse loudly
            raise FlushError(
                f"partition spec (spec_id={spec.spec_id}) references field_id "
                f"{field.source_field_id}, which is not a live column of "
                f"{info.namespace}.{info.name}"
            )
        leaf = chain[-1]
        # A struct leaf is read by path (pyhoglake's partition_source_array
        # walks it with struct_field and refuses a container or a source
        # under a list/map itself).
        key_arrays.append(
            transform_strings(
                field.transform,
                field.transform_param,
                partition_source_array(data, list(chain)),
                leaf.type,
                leaf.type_params,
            )
        )
    keyed = pa.table(
        {
            **dict(zip(key_names, key_arrays, strict=True)),
            "__millrace_row": pa.array(range(data.num_rows), pa.int64()),
        }
    )
    combos = (
        keyed.group_by(key_names)
        .aggregate([("__millrace_row", "min")])
        .sort_by("__millrace_row_min")
    )
    out: list[tuple[tuple[str | None, ...], pa.Table]] = []
    for i in range(combos.num_rows):
        values = tuple(combos.column(k)[i].as_py() for k in key_names)
        mask = None
        for name, value in zip(key_names, values, strict=True):
            key_col = keyed.column(name)
            field_mask = (
                pc.is_null(key_col)
                if value is None
                else pc.fill_null(pc.equal(key_col, value), False)
            )
            mask = field_mask if mask is None else pc.and_(mask, field_mask)
        out.append((values, data.filter(mask)))
    return out


# -- commit message --------------------------------------------------------------


def format_commit_message(
    *,
    table_uuid: str,
    group: str | None,
    groups: int = 1,
    trigger: str,
    records: int,
    dropped: int,
    files: int,
    commit_index: int,
    commits: int,
    topic: str,
    stage: int,
    version: str,
) -> str:
    """The snapshot's ``message``: what the flush was (``key=value``
    pairs, no value holding a space), then where it came from. A
    one-group commit names its group (``group=<label>``); a batched
    commit says how many it carries (``groups=<n>``, ``group`` None)."""
    what = f"group={group}" if group is not None else f"groups={groups}"
    summary = (
        f"millrace=v2 table={table_uuid} {what} trigger={trigger} "
        f"records={records} dropped={dropped} files={files} "
        f"commit={commit_index + 1}/{commits} millrace={_message_token(version)}"
    )
    return f"{summary}\noffsets topic={_message_token(topic)} stage={stage}"


def _message_token(value: Any) -> str:
    text = "".join("_" if c.isspace() else c for c in str(value))
    return text or "unknown"


# -- the pure plan builder -----------------------------------------------------


@dataclass(frozen=True, slots=True)
class FlushContext:
    """Per-build inputs of :func:`build_flush_plan`. ``build`` is a
    fresh token per build (object names ``…/{build}/{index}.parquet``),
    so a rebuild never overwrites objects a landed commit may name."""

    topic: str
    #: The stage's slot (its home: the replica's smallest partition).
    slot: int
    group: keyspace.PartitionTuple
    group_fields: tuple[str, ...]
    trigger: str
    read_snapshot: int
    table_uuid: str
    build: str

    def __post_init__(self) -> None:
        if not self.build or "/" in self.build:
            raise ValueError(
                f"build token must be a non-empty path segment: {self.build!r}"
            )


@dataclass(frozen=True, slots=True)
class CommitPlan:
    """One commit request (without its idempotency key, which the
    flusher mints when it first sends it) and the files it registers."""

    payload: dict[str, Any]
    uploads: tuple[Upload, ...]
    records: int
    #: The staged rows this commit publishes, as indices into the build's
    #: input: settled as soon as it lands.
    row_indices: tuple[int, ...] = ()

    @property
    def uris(self) -> tuple[str, ...]:
        return tuple(u.uri for u in self.uploads)


@dataclass(frozen=True, slots=True)
class FlushPlan:
    """The pure half of a flush. ``commits`` is empty exactly when every
    scanned row was dropped. ``dropped_records`` are counted and logged
    by the caller and deleted with the window."""

    commits: tuple[CommitPlan, ...]
    dropped_records: tuple[DroppedRecord, ...]
    records: int
    files: int
    staged_bytes: int
    dropped_keys: tuple[str, ...]
    #: The dropped records, as indices into the build's input.
    dropped_indices: tuple[int, ...] = ()
    #: Timestamps clamped to years 1–9999, by dotted column path.
    clamped: Mapping[str, int] = dataclass_field(default_factory=dict)


def _footer_size(raw: bytes) -> int:
    """The 4-byte LE thrift footer length stored in the parquet trailer,
    excluding the 8-byte suffix (pyhoglake's wire convention)."""
    (meta_len,) = struct.unpack("<I", raw[-8:-4])
    return int(meta_len)


def build_flush_plan(
    records: Sequence[tuple[keyspace.RowKey, bytes]],
    info: TableInfo,
    *,
    ctx: FlushContext,
    data_path: str,
    max_files_per_commit: int,
    footer_stats_min_bytes: int,
    version: str,
) -> FlushPlan:
    """Decode → Arrow → sort by the live sort spec → fan out by the live
    partition spec → parquet → commit requests of at most
    ``max_files_per_commit`` files each. Pure: no I/O, no clock.

    Sorting happens before the fanout (a group filter keeps row order),
    so every file is key-nondecreasing under the sort spec compaction
    enforces; staging order is Kafka offset order, not event time."""
    if max_files_per_commit < 1:
        raise ValueError("max_files_per_commit must be >= 1")
    decoder = JsonObjectDecoder(info.columns)
    staged_bytes = sum(len(payload) for _, payload in records)
    good: list[tuple[keyspace.RowKey, bytes, DecodedRecord]] = []
    good_indices: list[int] = []
    dropped_records: list[DroppedRecord] = []
    dropped_indices: list[int] = []
    dropped_keys: set[str] = set()
    for index, (row_key, payload_bytes) in enumerate(records):
        try:
            record = decoder.decode_record(payload_bytes)
        except DecodeError as e:
            dropped_records.append(
                DroppedRecord(row_key.offset, e.reason, e.path, row_key.partition)
            )
            dropped_indices.append(index)
            continue
        dropped_keys.update(record.dropped_keys)
        good.append((row_key, payload_bytes, record))
        good_indices.append(index)
    if not good:
        return FlushPlan(
            (),
            tuple(dropped_records),
            0,
            0,
            staged_bytes,
            tuple(sorted(dropped_keys)),
            tuple(dropped_indices),
            dict(decoder.clamped),
        )

    encoded = _build_batch_or_raise(good, decoder.columns)
    good_count = len(good)
    # The decoded rows are the bulk of the build's memory; the parquet
    # writes below need only the encoded table.
    del good
    encode_schema = encoded.schema
    # A row-index column rides through the sort and the fanout so each
    # file knows which staged rows it holds (a commit settles its own
    # rows as soon as it lands); it is dropped before the parquet write.
    encoded = encoded.append_column(
        _ROW_INDEX, pa.array(range(good_count), type=pa.int64())
    )
    sort_keys = _spec_sort_keys(info)
    if sort_keys:
        encoded = _sort_batch_by_spec(encoded, sort_keys)
    spec = info.partition_spec
    parts: list[tuple[tuple[str | None, ...] | None, pa.Table]]
    if spec is not None and spec.fields:
        parts = list(_partition_groups(encoded, info, spec))
    else:
        parts = [(None, encoded)]
    del encoded

    label = keyspace.group_label(ctx.group_fields, ctx.group)
    base = f"{data_path.rstrip('/')}/data/{info.namespace}/{info.name}/{ctx.build}"
    files: list[tuple[dict[str, Any], Upload, list[int]]] = []
    for index in range(len(parts)):
        partition_values, part = parts[index]
        # Release each fanout slice once written.
        parts[index] = (partition_values, _EMPTY)
        rows: list[int] = part.column(_ROW_INDEX).to_pylist()
        part = part.drop_columns([_ROW_INDEX])
        sink = pa.BufferOutputStream()
        # zstd at compaction's level (ParquetRewriter.DEFAULT_ZSTD_LEVEL):
        # pyarrow's default snappy left files ~3x larger than compaction's
        # rewrite of them, in upload bytes and in the spool.
        pq.write_table(part, sink, compression="zstd", compression_level=3)
        del part
        raw = sink.getvalue()
        with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
            if not prepared_schema_matches(
                file_schema(parquet.schema_arrow), encode_schema
            ):
                raise FlushError(
                    "encoded parquet schema/field ids differ from the "
                    "destination schema (a millrace encoder bug)"
                )
            metadata = parquet.metadata
        uri = f"{base}/{index}.parquet"
        registration: dict[str, Any] = {
            "path": uri,
            "record_count": metadata.num_rows,
            "file_size_bytes": raw.size,
            "footer_size": _footer_size(bytes(raw[-8:])),
        }
        if raw.size >= footer_stats_min_bytes:
            registration["column_stats"] = [
                stat.to_wire() for stat in extract_column_stats(metadata, info.columns)
            ]
        if partition_values is not None:
            registration["partition_values"] = list(partition_values)
        files.append((registration, Upload(uri=uri, size=raw.size, body=raw), rows))

    chunks = [
        files[i : i + max_files_per_commit]
        for i in range(0, len(files), max_files_per_commit)
    ]
    commits = []
    for index, chunk in enumerate(chunks):
        records_in = sum(int(reg["record_count"]) for reg, _, _ in chunk)
        payload: dict[str, Any] = {
            "read_snapshot": ctx.read_snapshot,
            "appends": [
                {
                    "namespace": info.namespace,
                    "table": info.name,
                    "expected_table_uuid": ctx.table_uuid,
                    "files": [reg for reg, _, _ in chunk],
                }
            ],
            "author": f"millrace/{ctx.topic}/{ctx.slot}",
            "message": format_commit_message(
                table_uuid=ctx.table_uuid,
                group=label,
                trigger=ctx.trigger,
                records=records_in,
                dropped=len(dropped_records),
                files=len(chunk),
                commit_index=index,
                commits=len(chunks),
                topic=ctx.topic,
                stage=ctx.slot,
                version=version,
            ),
        }
        commits.append(
            CommitPlan(
                payload=payload,
                uploads=tuple(u for _, u, _ in chunk),
                records=records_in,
                row_indices=tuple(
                    sorted(good_indices[i] for _, _, rows in chunk for i in rows)
                ),
            )
        )
    return FlushPlan(
        commits=tuple(commits),
        dropped_records=tuple(dropped_records),
        records=sum(c.records for c in commits),
        files=len(files),
        staged_bytes=staged_bytes,
        dropped_keys=tuple(sorted(dropped_keys)),
        dropped_indices=tuple(dropped_indices),
        clamped=dict(decoder.clamped),
    )


_EMPTY: Final = pa.table({})


FlushOutcome = Literal["committed", "dropped", "empty"]


@dataclass(frozen=True, slots=True)
class FlushReport:
    """The outcome of one decision. ``dropped`` counts rows left out of
    the publication; ``commits`` the commits that landed;
    ``staged_rows``/``staged_bytes`` the window's rows and payload bytes
    that were settled (every one, for a committed or dropped outcome).
    ``partition`` is the stage's slot."""

    topic: str
    partition: int
    group: keyspace.PartitionTuple
    trigger: str
    outcome: FlushOutcome
    records: int = 0
    files: int = 0
    dropped: int = 0
    commits: int = 0
    complete: bool = True
    snapshot_ids: tuple[int, ...] = ()
    staged_rows: int = 0
    staged_bytes: int = 0


class _GroupProgress:
    """One group's progress through a :meth:`HoglakeFlusher.flush_batch`:
    which rows of its scan are settled (a bitmap by position, so a
    settle never hashes row keys), what landed and in which snapshots."""

    __slots__ = (
        "_settled",
        "dropped",
        "files",
        "left",
        "records",
        "scan",
        "snapshot_ids",
    )

    def __init__(self, scan: Scan) -> None:
        self.scan = scan
        self._settled = bytearray(len(scan.rows))
        self.left = len(scan.rows)
        self.dropped = 0
        self.records = 0
        self.files = 0
        self.snapshot_ids: list[int] = []

    def unsettled(self) -> list[int]:
        """Positions of the rows still staged, in scan order."""
        return [k for k, done in enumerate(self._settled) if not done]

    def settle(self, positions: Sequence[int]) -> Scan:
        """Mark ``positions`` settled; the :class:`Scan` to settle them
        with (complete only once every row of a complete scan is)."""
        for k in positions:
            self._settled[k] = 1
        self.left -= len(positions)
        scan = self.scan
        return Scan(
            scan.group,
            [scan.rows[k] for k in positions],
            scan.complete and self.left == 0,
            scan.gen,
            scan.started_us,
        )

    def landed(self, commit: CommitPlan, snapshot_id: int) -> None:
        self.records += commit.records
        self.files += len(commit.uploads)
        if snapshot_id not in self.snapshot_ids:
            self.snapshot_ids.append(snapshot_id)

    def report(
        self, stage: PartitionStage, trigger: str, outcome: FlushOutcome
    ) -> FlushReport:
        return FlushReport(
            stage.topic,
            stage.partition,
            self.scan.group,
            trigger,
            outcome,
            records=self.records,
            files=self.files,
            dropped=self.dropped,
            commits=len(self.snapshot_ids),
            complete=self.scan.complete,
            snapshot_ids=tuple(self.snapshot_ids),
            staged_rows=len(self.scan.rows),
            staged_bytes=self.scan.nbytes,
        )


@dataclass(frozen=True, slots=True)
class BatchReport:
    """The outcome of one batched decision: a :class:`FlushReport` per
    group that was published or dropped, and the groups whose build
    failed (left out of the batch, rows still staged) with their
    errors."""

    reports: tuple[FlushReport, ...]
    failed: tuple[tuple[keyspace.PartitionTuple, Exception], ...] = ()
    commits: int = 0
    #: Groups a refused commit left unpublished (rows staged), and why.
    refused: tuple[keyspace.PartitionTuple, ...] = ()
    refusal: CommitRefused | None = None


def _transient_delay(attempts: int, retry_after: float | None) -> float:
    backoff: float = min(_BASE_DELAY_S * (2 ** (attempts - 1)), _MAX_DELAY_S)
    return max(backoff, retry_after or 0.0)


def _seg(name: object) -> str:
    """Percent-encode one URL path segment."""
    return quote(str(name), safe="")


def read_table_identity(
    client: HoglakeClient, catalog: str, namespace: str, table: str
) -> TableInfo:
    """The ``totals=false`` identity read of one table — EVERY table read
    this pipeline makes (the startup resolve, the shape-cache refresh,
    destination.py's existence check): name, uuid, columns, specs and
    the ``read_snapshot_id`` it resolved at, with no manifest aggregate.
    Raises pyhoglake's errors (``NotFoundError`` for a missing table) and
    :class:`FlushStartupError` when pyhoglake no longer exposes the
    transport this read needs."""
    request = getattr(client, "_request", None)
    if request is None:
        raise FlushStartupError(
            "pyhoglake no longer exposes HoglakeClient._request; "
            "millrace's identity reads and receipt lookups cannot run"
        )
    body = request(
        "GET",
        f"/catalogs/{_seg(catalog)}/namespaces/{_seg(namespace)}/tables/{_seg(table)}",
        params={"totals": "false"},
    )
    return TableInfo.from_wire(body)


def read_destination(client: HoglakeClient, cfg: Config) -> tuple[Catalog, TableInfo]:
    """The startup identity read: catalog → namespace → table, the table
    read ``totals=false`` (the ``Table`` handle is built straight from it
    rather than through ``Namespace.table()``, which issues a plain GET).
    A server that does not report ``read_snapshot_id`` is refused: without
    it there is no conflict basis worth sending. Raises
    :class:`FlushStartupError`."""
    try:
        catalog = client.catalog(cfg.catalog)
        # Existence-check the namespace (the API has no per-namespace
        # GET; the listing is the check).
        catalog.namespace(cfg.namespace)
        info = read_table_identity(client, cfg.catalog, cfg.namespace, cfg.table)
    except FlushStartupError:
        raise
    except (HoglakeError, httpx.HTTPError, OSError) as e:
        raise FlushStartupError(
            f"cannot resolve the hoglake destination "
            f"{cfg.catalog}/{cfg.namespace}/{cfg.table}: {e}"
        ) from e
    if info.read_snapshot_id is None:
        raise FlushStartupError(
            f"the hoglake server did not report read_snapshot_id for "
            f"{cfg.namespace}.{cfg.table}: millrace requires a server "
            "whose table reads carry the snapshot they resolved at "
            "(the commit's conflict basis)"
        )
    return catalog, info


def retention_ttl_s(catalog: Catalog) -> float:
    """A QUARTER of the catalog's snapshot retention — the destination
    shape's re-read cadence (:meth:`HoglakeFlusher._retention_ttl_s`
    carries the argument), with pyhoglake's assumed retention (so 450 s)
    when the catalog will not say; ``inf`` when retention is disabled."""
    read = getattr(catalog, "_retention_seconds", None)
    if read is None:
        return _ASSUMED_RETENTION_S / 4
    try:
        retention = read()
    except Exception:  # noqa: BLE001 - a retention read must never fail a flush
        return _ASSUMED_RETENTION_S / 4
    if (
        not isinstance(retention, (int, float))
        or isinstance(retention, bool)
        or retention <= 0
    ):
        return _ASSUMED_RETENTION_S / 4
    return float(retention) / 4


def _group_fields(info: TableInfo, group: keyspace.PartitionTuple) -> tuple[str, ...]:
    """hoglake's display names for a staged tuple's fields, from the live
    spec (:func:`millrace.grouping.field_names`). A tuple keyed under a
    spec of another arity (a spec change while it was staged) gets
    positional names ``key_<i>`` — the label is identity material, not a
    claim about the live spec."""
    names = field_names(info)
    if len(names) == len(group):
        return names
    return tuple(f"key_{i}" for i in range(len(group)))


class ShapeReader:
    """The destination's shape for the CONSUMER's keyer refresh
    (``consumer.ShapeSource``): its own ``Table`` handle over the
    flusher's resolved catalog, pinned to the flusher's incarnation.
    :meth:`read` is a ``totals=false`` identity read that refuses a
    different ``table_uuid`` (``table_recreated``) or a missing
    ``read_snapshot_id`` (``server_capability``) the way the flusher's
    shape cache does; :meth:`ttl_s` is the flusher's cadence
    (:func:`retention_ttl_s`). Both block (pyhoglake I/O)."""

    def __init__(self, catalog: Catalog, namespace: str, info: TableInfo) -> None:
        self._catalog = catalog
        self._table = Table(Namespace(catalog, namespace), info)
        self._table_uuid = info.table_uuid

    def read(self) -> TableInfo:
        info = self._table.info(totals=False)
        if info.table_uuid != self._table_uuid:
            raise FlushHalted(
                "table_recreated",
                f"table {info.namespace}.{info.name} now resolves to "
                f"{info.table_uuid}, not the pinned incarnation {self._table_uuid}",
            )
        if info.read_snapshot_id is None:
            raise FlushHalted(
                "server_capability",
                f"the server stopped reporting read_snapshot_id for "
                f"{info.namespace}.{info.name}",
            )
        return info

    def ttl_s(self) -> float:
        return retention_ttl_s(self._catalog)


class HoglakeFlusher:
    """The hoglake side of the flush for one pipeline: the client, the
    pinned destination incarnation, the shape cache (TTL = a quarter of
    the catalog's snapshot retention), the commit ladder and the
    counters. Built by :meth:`resolve`; lifecycle is caller-owned."""

    @classmethod
    def resolve(
        cls,
        client: HoglakeClient,
        cfg: Config,
        *,
        now_us: Callable[[], int],
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        executor: Executor | None = None,
        io_workers: int = 1,
        new_token: Callable[[], str] = lambda: uuid.uuid4().hex,
    ) -> HoglakeFlusher:
        """Resolve catalog → namespace → table, validate that this writer
        can honor the destination's columns, partition spec and sort
        spec, and PIN the incarnation: every commit sends it as
        ``expected_table_uuid`` and a later read naming another uuid
        halts ``table_recreated``."""
        flusher = cls(
            client,
            cfg,
            now_us=now_us,
            sleep=sleep,
            monotonic=monotonic,
            executor=executor,
            io_workers=io_workers,
            new_token=new_token,
        )
        catalog, info = read_destination(client, cfg)
        flusher._catalog = catalog
        flusher._table = Table(Namespace(catalog, cfg.namespace), info)
        flusher._table_uuid = info.table_uuid
        flusher._shape_info = info
        flusher._shape_read_at = flusher._monotonic()
        flusher._resolved_info = info
        try:
            JsonObjectDecoder(info.columns)
        except UnsupportedColumnError as e:
            raise FlushStartupError(str(e)) from e
        spec = info.partition_spec
        if spec is not None:
            chains = column_chains(info.columns)
            for field in spec.fields:
                chain = chains.get(field.source_field_id)
                if chain is None:
                    raise FlushStartupError(
                        f"partition spec (spec_id={spec.spec_id}) references "
                        f"field_id {field.source_field_id}, which is not a "
                        f"live column of {info.namespace}.{info.name}"
                    )
                fault = _source_fault(chain)
                if fault is not None:
                    raise FlushStartupError(
                        f"partition spec (spec_id={spec.spec_id}) field "
                        f"{field.source_field_id} cannot be a partition "
                        f"source: {fault}"
                    )
                leaf = chain[-1]
                try:
                    # The validity dry-run: vocabulary, param, type gate.
                    transform_value(
                        field.transform,
                        field.transform_param,
                        leaf.type,
                        None,
                        leaf.type_params,
                    )
                except ValidationError as e:
                    raise FlushStartupError(
                        f"partition spec (spec_id={spec.spec_id}) cannot be "
                        f"honored by this writer: {e}"
                    ) from e
        try:
            _spec_sort_keys(info)
        except FlushError as e:
            raise FlushStartupError(str(e)) from e
        # Retry-After is read off raw responses (pyhoglake drops headers).
        # Best-effort: without it the exponential curve applies.
        try:
            flusher._client._http.event_hooks["response"].append(flusher._note_response)
        except Exception:  # noqa: BLE001 - optional enhancement, never fatal
            log.debug("could not install the Retry-After hook")
        log.info(
            "millrace destination resolved: %s/%s/%s table_uuid=%s "
            "read_snapshot=%d (%d columns, spec %s)",
            cfg.catalog,
            cfg.namespace,
            cfg.table,
            flusher._table_uuid,
            info.read_snapshot_id,
            len(info.columns),
            "none"
            if spec is None
            else f"id={spec.spec_id} ({len(spec.fields)} fields)",
        )
        return flusher

    def __init__(
        self,
        client: HoglakeClient,
        cfg: Config,
        *,
        now_us: Callable[[], int],
        sleep: Callable[[float], Awaitable[None]],
        monotonic: Callable[[], float],
        executor: Executor | None,
        io_workers: int = 1,
        new_token: Callable[[], str] = lambda: uuid.uuid4().hex,
    ) -> None:
        if io_workers < 1:
            raise ValueError(f"io_workers must be >= 1, got {io_workers}")
        self._client = client
        self._cfg = cfg
        self._now_us = now_us
        self._sleep = sleep
        self._monotonic = monotonic
        self._new_token = new_token
        self._owns_executor = executor is None
        self._executor = (
            ThreadPoolExecutor(
                max_workers=io_workers, thread_name_prefix="millrace-hoglake"
            )
            if executor is None
            else executor
        )
        self._closed = False
        self._catalog: Catalog
        self._table: Table
        self._table_uuid: str
        self._resolved_info: TableInfo
        self._shape_info: TableInfo | None = None
        self._shape_read_at: float | None = None
        self._retry_after: float | None = None
        self._warned_dropped: set[str] = set()
        self._decisions_committed = 0
        self._decisions_dropped = 0
        self._decisions_failed = 0
        self._commits = 0
        self._commit_attempts = 0
        self._commit_retries = 0
        self._commit_refusals: dict[str, int] = {}
        self._records_dropped: dict[str, int] = {}
        self._rows_published = 0
        self._files_written = 0
        self._bytes_written = 0
        self._orphaned_uploads = 0
        self._dropped_payload_keys = 0
        self._ambiguous_uploads = 0
        self._pinned_stages: set[str] = set()
        self._values_clamped: dict[str, int] = {}
        self._clamp_logged: dict[str, float] = {}

    def close(self) -> None:
        """Close the HTTP client and (if owned) the executor. Idempotent."""
        if self._closed:
            return
        self._closed = True
        self._client.close()
        if self._owns_executor:
            self._executor.shutdown(wait=True)

    @property
    def table_uuid(self) -> str:
        return self._table_uuid

    @property
    def resolved_info(self) -> TableInfo:
        """The shape read at startup (the consumer's first keyer)."""
        return self._resolved_info

    def shape_reader(self) -> ShapeReader:
        """The consumer's keyer-refresh source, pinned to this incarnation."""
        return ShapeReader(self._catalog, self._cfg.namespace, self._resolved_info)

    def stats(self) -> FlushStats:
        return FlushStats(
            decisions_committed=self._decisions_committed,
            decisions_dropped=self._decisions_dropped,
            decisions_failed=self._decisions_failed,
            commits=self._commits,
            commit_attempts=self._commit_attempts,
            commit_retries=self._commit_retries,
            commit_refusals=dict(self._commit_refusals),
            records_dropped=dict(self._records_dropped),
            rows_published=self._rows_published,
            files_written=self._files_written,
            bytes_written=self._bytes_written,
            orphaned_uploads=self._orphaned_uploads,
            ambiguous_uploads=self._ambiguous_uploads,
            values_clamped=dict(self._values_clamped),
            dropped_payload_keys=self._dropped_payload_keys,
        )

    async def check_stage_identity(
        self, stage: PartitionStage, *, pin: bool = False
    ) -> None:
        """Halt when the stage's rows were first committed to another
        incarnation of the destination (``meta/table_uuid``): the halt
        survives restarts, not only the process that saw the recreate.
        With ``pin``, an unpinned stage records this incarnation."""
        if stage.path in self._pinned_stages:
            return
        pinned = await stage.pinned_table_uuid()
        if pinned is None:
            if not pin:
                return
            await stage.pin_table_uuid(self._table_uuid)
        elif pinned != self._table_uuid:
            raise FlushHalted(
                "table_recreated",
                f"{stage.topic}[{stage.partition}]'s staged rows were committed "
                f"to incarnation {pinned} of {self._cfg.namespace}."
                f"{self._cfg.table}, which now resolves to {self._table_uuid}. "
                "A human decides what happened; staged rows stay durable.",
            )
        self._pinned_stages.add(stage.path)

    def note_failure(self) -> None:
        self._decisions_failed += 1

    # -- internals -------------------------------------------------------------

    async def _io(self, fn: Callable[[], _R]) -> _R:
        """Run blocking pyhoglake or CPU work off the shared event loop."""
        return await asyncio.get_running_loop().run_in_executor(self._executor, fn)

    def _note_response(self, response: httpx.Response) -> None:
        if response.status_code not in _RETRY_AFTER_STATUS:
            return
        raw = response.headers.get("retry-after")
        if raw is None:
            return
        try:
            self._retry_after = float(raw.strip())
        except ValueError:
            log.debug("ignoring non-numeric Retry-After %r", raw)

    def _retry_after_hint(self) -> float | None:
        """The most recent Retry-After, consumed once."""
        hint, self._retry_after = self._retry_after, None
        return hint

    def _count_orphans(self, uris: Sequence[str], why: str) -> None:
        """Uploaded objects no commit names. Logged by name: sweeping
        their shared prefix is never safe."""
        if not uris:
            return
        listed = list(uris[:_ORPHAN_URIS_LOGGED])
        omitted = len(uris) - len(listed)
        log.warning(
            "orphaned %d uploaded parquet file(s): %s: %s%s",
            len(uris),
            why,
            ", ".join(listed),
            f" and {omitted} more" if omitted else "",
        )
        self._orphaned_uploads += len(uris)

    # -- the destination shape -----------------------------------------------------

    def _drop_shape(self) -> None:
        """Forget the cached shape so the next build re-reads it (on any
        commit error: whatever can drop pyhoglake's cache drops ours)."""
        self._shape_info = None
        self._shape_read_at = None

    def _shape_sync(self) -> TableInfo:
        """The destination's shape: cached, re-read (``totals=false``) on
        a cold cache or past the TTL. The TTL is a quarter of the
        catalog's retention, half of pyhoglake's own cache threshold, so
        this cache is never staler than pyhoglake's and a spec change is
        always inside a commit's conflict window."""
        if (
            self._shape_info is not None
            and self._shape_read_at is not None
            and self._monotonic() - self._shape_read_at < retention_ttl_s(self._catalog)
        ):
            return self._shape_info
        info = self._table.info(totals=False)
        _check_identity(info, self._table_uuid)
        self._shape_info = info
        self._shape_read_at = self._monotonic()
        return info

    # -- publishing ------------------------------------------------------------

    async def _publish(self, payload: dict[str, Any]) -> CommitResult:
        """Send one commit. Ambiguous or retryable answers resend the
        IDENTICAL bytes under the same key (the server's receipt makes
        that exactly-once). Answered refusals raise at once."""
        attempts = 0
        conflicts = 0
        while True:
            attempts += 1
            self._commit_attempts += 1
            try:
                return await self._io(partial(self._table.commit_prepared, payload))
            except IncarnationChangedError as e:
                self._drop_shape()
                raise FlushHalted(
                    "table_recreated",
                    f"{self._cfg.namespace}.{self._cfg.table} is no longer "
                    f"incarnation {self._table_uuid}: {e}",
                ) from e
            except (DdlSinceReadSnapshotError, ReadSnapshotExpiredError):
                # Subclasses of the conflict/validation family: these
                # arms come first. The caller rebuilds on a fresh basis.
                self._drop_shape()
                raise
            except CommitConflictError as e:
                self._drop_shape()
                if e.message == "table_dropped":
                    raise CommitRefused(409, f"the destination was dropped: {e}") from e
                conflicts += 1
                if conflicts >= _CONFLICT_MAX_ATTEMPTS:
                    raise CommitExhausted(
                        f"commit_conflict {conflicts} times in a row ({e})"
                    ) from e
                error: Exception = e
                delay = min(_BASE_DELAY_S * (2 ** (conflicts - 1)), _MAX_DELAY_S)
            except HoglakeError as e:
                self._drop_shape()
                status = e.status_code
                if (
                    status is not None
                    and 400 <= status < 500
                    and status not in (408, 429)
                ):
                    label = str(status)
                    self._commit_refusals[label] = (
                        self._commit_refusals.get(label, 0) + 1
                    )
                    raise CommitRefused(
                        status, f"commit refused ({status}): {e}"
                    ) from e
                if attempts >= _PUBLISH_MAX_ATTEMPTS:
                    raise CommitExhausted(
                        f"commit failed {attempts} times ({e})"
                    ) from e
                error = e
                delay = _transient_delay(attempts, self._retry_after_hint())
            except (httpx.HTTPError, OSError) as e:
                # The commit may have applied and the answer was lost.
                if attempts >= _PUBLISH_MAX_ATTEMPTS:
                    raise CommitExhausted(
                        f"commit transport failed {attempts} times ({e})"
                    ) from e
                error = e
                delay = _transient_delay(attempts, self._retry_after_hint())
            self._commit_retries += 1
            log.warning(
                "commit attempt %d failed (%s), resending the identical request in %.1fs",
                attempts,
                error,
                delay,
            )
            await self._sleep(delay)

    def _upload(self, uploads: Sequence[Upload]) -> None:
        """Write the parquet buffers through pyhoglake's upload path. A
        mid-fanout failure carries ``completed_uris`` for orphan
        accounting."""
        filesystem = getattr(self._client, "_filesystem", None)
        put_client_fn = getattr(self._client, "_put_client", None)
        if filesystem is None or put_client_fn is None:
            raise FlushError(
                "pyhoglake no longer exposes _filesystem/_put_client; the "
                "flush cannot write parquet"
            )
        fanout = resolve_concurrency(len(uploads), None)
        # One pool for every decision in flight, the same size on every
        # call: sized per call it overflowed (2 decisions x 64 uploads on
        # a 64-connection pool, a fresh TLS handshake per discarded
        # connection), and a wider request rebuilds the shared client and
        # closes the old one under a concurrent decision's uploads.
        put_client = put_client_fn(
            resolve_concurrency(self._cfg.max_files_per_commit, None)
            * self._cfg.flush_concurrency
        )
        widen_io_threads_for(list(uploads), put_client, fanout)
        completed: list[str] = []
        try:
            run_uploads(
                lambda upload: perform_upload(filesystem(), put_client, upload),
                list(uploads),
                concurrency=fanout,
                completed=completed,
            )
        except BaseException as e:
            try:
                e.completed_uris = tuple(completed)  # type: ignore[attr-defined]
            except (AttributeError, TypeError):
                pass
            raise

    def _note_drops(
        self, stage: PartitionStage, label: str, dropped: Sequence[DroppedRecord]
    ) -> None:
        """Count flush-time drops per reason and log one WARNING per
        reason per decision (offsets and the first refusing column path;
        never a payload value)."""
        by_reason: dict[str, list[str]] = {}
        first_path: dict[str, str] = {}
        for entry in dropped:
            where = (
                str(entry.offset)
                if entry.partition is None
                else f"{entry.partition}:{entry.offset}"
            )
            by_reason.setdefault(entry.reason, []).append(where)
            if entry.path is not None:
                first_path.setdefault(
                    entry.reason, f" first_path={entry.path} (offset {where})"
                )
        for reason, offsets in sorted(by_reason.items()):
            self._records_dropped[reason] = self._records_dropped.get(reason, 0) + len(
                offsets
            )
            shown = offsets[:_DROPPED_OFFSETS_LOGGED]
            log.warning(
                "flush dropped %d records: %s[%d] group %s reason=%s offsets=%s%s%s",
                len(offsets),
                stage.topic,
                stage.partition,
                label,
                reason,
                ",".join(shown),
                "" if len(offsets) == len(shown) else ",…",
                first_path.get(reason, ""),
            )

    def _note_dropped_keys(self, info: TableInfo, keys: Sequence[str]) -> None:
        for key in keys:
            if key not in self._warned_dropped:
                self._warned_dropped.add(key)
                log.warning(
                    "payload key %r is not a column of %s.%s; dropping it from "
                    "every flush (no schema evolution)",
                    key,
                    info.namespace,
                    info.name,
                )
        self._dropped_payload_keys += len(keys)

    # -- one decision -------------------------------------------------------------

    @property
    def max_files_per_commit(self) -> int:
        return self._cfg.max_files_per_commit

    async def flush_batch(
        self,
        stage: PartitionStage,
        scans: Sequence[Scan],
        trigger: str,
        *,
        durable: bool = True,
    ) -> BatchReport:
        """Flush one or more groups' scans of ONE stage as one decision:
        every group's files are built on one read basis, packed into
        commits of at most ``max_files_per_commit`` files, and each
        commit's rows are settled in one delete batch as soon as it
        lands, so a decision that fails partway never republishes what
        landed. Dropped rows are settled right after the build (they are
        never published), so a rebuild never drops them twice. A refused
        read basis rebuilds the unpublished rows on a fresh shape (at
        most :data:`_REBUILD_ATTEMPTS` builds).

        Never raises for one group's trouble: a group whose BUILD fails
        is reported in ``failed``; a refused commit stops the decision
        and the groups it did not publish are reported in ``refused``
        (with ``refusal``), beside reports for those that landed. Any
        other failure propagates and the decision is retried whole."""
        reports: list[FlushReport] = []
        empty = [scan for scan in scans if not scan.rows]
        if empty:
            # The index held a group with nothing on disk (a write that
            # failed after the index update): settle removes it.
            await stage.settle_many(empty, durable=durable)
            reports += [
                FlushReport(
                    stage.topic,
                    stage.partition,
                    scan.group,
                    trigger,
                    "empty",
                    complete=scan.complete,
                )
                for scan in empty
            ]
        live = [scan for scan in scans if scan.rows]
        if not live:
            return BatchReport(tuple(reports))
        await self.check_stage_identity(stage, pin=True)
        state = [_GroupProgress(scan) for scan in live]
        failed: dict[int, Exception] = {}
        refusal: CommitRefused | None = None
        commits = 0
        for build in range(_REBUILD_ATTEMPTS):
            todo = [i for i, g in enumerate(state) if g.left and i not in failed]
            if not todo:
                break
            info = await self._io(self._shape_sync)
            inputs = {i: state[i].unsettled() for i in todo}
            built: list[tuple[int, FlushPlan, str]] = []
            for i, result in zip(
                todo,
                await self._build_plans(stage, live, inputs, info, trigger),
                strict=True,
            ):
                if isinstance(result, Exception):
                    failed[i] = result
                    continue
                plan, label = result
                if build == 0:
                    self._note_dropped_keys(info, plan.dropped_keys)
                    self._note_clamps(stage, plan.clamped)
                if plan.dropped_records:
                    self._note_drops(stage, label, plan.dropped_records)
                    state[i].dropped += len(plan.dropped_records)
                built.append((i, plan, label))
            # Dropped rows are never published: settle them now, once.
            drops = [
                state[i].settle([inputs[i][k] for k in plan.dropped_indices])
                for i, plan, _ in built
                if plan.dropped_indices
            ]
            if drops:
                await stage.settle_many(drops, durable=durable)
            chunks = self._chunk(built)
            try:
                for index, chunk in enumerate(chunks):
                    snapshot_id = await self._publish_chunk(
                        stage, chunk, index, len(chunks), trigger
                    )
                    commits += 1
                    landed: dict[int, list[int]] = {}
                    for i, commit, _ in chunk:
                        landed.setdefault(i, []).extend(
                            inputs[i][k] for k in commit.row_indices
                        )
                        state[i].landed(commit, snapshot_id)
                        self._rows_published += commit.records
                        self._files_written += len(commit.uploads)
                    await stage.settle_many(
                        [state[i].settle(rows) for i, rows in sorted(landed.items())],
                        durable=durable,
                    )
                break
            except CommitRefused as e:
                refusal = e
                break
            except (DdlSinceReadSnapshotError, ReadSnapshotExpiredError) as e:
                if build + 1 >= _REBUILD_ATTEMPTS:
                    raise FlushError(
                        f"read basis refused {build + 1} times: {e}"
                    ) from e
                log.info(
                    "commit on %s[%d] refused its read basis (%s); rebuilding the "
                    "unpublished rows on a fresh shape",
                    stage.topic,
                    stage.partition,
                    e,
                )
        refused: list[keyspace.PartitionTuple] = []
        for i, g in enumerate(state):
            if i in failed:
                continue
            if g.left:
                # Unpublished (a refused commit): not a finished decision.
                refused.append(g.scan.group)
                continue
            outcome: FlushOutcome = "committed" if g.snapshot_ids else "dropped"
            if outcome == "committed":
                self._decisions_committed += 1
            else:
                self._decisions_dropped += 1
            reports.append(g.report(stage, trigger, outcome))
        return BatchReport(
            tuple(reports),
            tuple((live[i].group, err) for i, err in sorted(failed.items())),
            commits,
            refused=tuple(refused),
            refusal=refusal if refused else None,
        )

    async def _build_plans(
        self,
        stage: PartitionStage,
        live: Sequence[Scan],
        inputs: Mapping[int, Sequence[int]],
        info: TableInfo,
        trigger: str,
    ) -> list[tuple[FlushPlan, str] | Exception]:
        """Build each group's plan from its unsettled rows (``inputs``:
        positions in its scan), in parallel on the pool. A halt
        propagates; any other build error is the group's own."""
        read_snapshot = info.read_snapshot_id
        assert read_snapshot is not None  # _check_identity refuses

        async def build(i: int) -> tuple[FlushPlan, str]:
            scan = live[i]
            fields = _group_fields(info, scan.group)
            ctx = FlushContext(
                topic=stage.topic,
                slot=stage.partition,
                group=scan.group,
                group_fields=fields,
                trigger=trigger,
                read_snapshot=read_snapshot,
                table_uuid=self._table_uuid,
                build=self._new_token(),
            )
            plan = await self._io(
                partial(
                    build_flush_plan,
                    [scan.rows[k] for k in inputs[i]],
                    info,
                    ctx=ctx,
                    data_path=self._catalog.data_path,
                    max_files_per_commit=self._cfg.max_files_per_commit,
                    footer_stats_min_bytes=FOOTER_STATS_MIN_BYTES,
                    version=_millrace_version(),
                )
            )
            return plan, keyspace.group_label(fields, scan.group)

        outcomes = await asyncio.gather(
            *(build(i) for i in inputs), return_exceptions=True
        )
        out: list[tuple[FlushPlan, str] | Exception] = []
        for outcome in outcomes:
            if isinstance(outcome, FlushHalted):
                raise outcome
            if isinstance(outcome, BaseException) and not isinstance(
                outcome, Exception
            ):
                raise outcome
            out.append(outcome)
        return out

    def _chunk(
        self, built: Sequence[tuple[int, FlushPlan, str]]
    ) -> list[list[tuple[int, CommitPlan, str]]]:
        """Pack the groups' commit parts (each with its group's label)
        into commits of at most ``max_files_per_commit`` files, in
        order."""
        chunks: list[list[tuple[int, CommitPlan, str]]] = []
        files = 0
        for i, plan, label in built:
            for commit in plan.commits:
                if (
                    chunks
                    and files + len(commit.uploads) <= self._cfg.max_files_per_commit
                ):
                    chunks[-1].append((i, commit, label))
                    files += len(commit.uploads)
                else:
                    chunks.append([(i, commit, label)])
                    files = len(commit.uploads)
        return chunks

    async def _publish_chunk(
        self,
        stage: PartitionStage,
        chunk: Sequence[tuple[int, CommitPlan, str]],
        index: int,
        count: int,
        trigger: str,
    ) -> int:
        """Upload and commit one chunk. One group's part keeps its own
        payload (its per-group message); several groups' parts merge
        into one append with a batch message."""
        if len(chunk) == 1:
            _, commit, label = chunk[0]
            return await self._upload_and_commit(commit, label)
        uploads = tuple(u for _, c, _ in chunk for u in c.uploads)
        records = sum(c.records for _, c, _ in chunk)
        template = chunk[0][1].payload
        payload = {
            **template,
            "appends": [
                {
                    **template["appends"][0],
                    "files": [
                        f for _, c, _ in chunk for f in c.payload["appends"][0]["files"]
                    ],
                }
            ],
            "message": format_commit_message(
                table_uuid=self._table_uuid,
                group=None,
                groups=len(chunk),
                trigger=trigger,
                records=records,
                dropped=0,
                files=len(uploads),
                commit_index=index,
                commits=count,
                topic=stage.topic,
                stage=stage.partition,
                version=_millrace_version(),
            ),
        }
        merged = CommitPlan(payload=payload, uploads=uploads, records=records)
        return await self._upload_and_commit(merged, f"a batch of {len(chunk)} groups")

    def _note_clamps(self, stage: PartitionStage, clamped: Mapping[str, int]) -> None:
        """Count timestamps clamped to years 1–9999; one WARNING per
        column per :data:`_CLAMP_LOG_INTERVAL_S`."""
        now = self._monotonic()
        for path, count in sorted(clamped.items()):
            self._values_clamped[path] = self._values_clamped.get(path, 0) + count
            last = self._clamp_logged.get(path)
            if last is not None and now - last < _CLAMP_LOG_INTERVAL_S:
                continue
            self._clamp_logged[path] = now
            log.warning(
                "clamped %d %s value(s) outside years 1-9999 to the nearest "
                "representable instant (%s[%d]; %d clamped in total)",
                count,
                path,
                stage.topic,
                stage.partition,
                self._values_clamped[path],
            )

    async def _upload_and_commit(self, commit: CommitPlan, label: str) -> int:
        """Upload every file of one commit, then publish it under a fresh
        key. Every object exists before a commit names it."""
        try:
            await self._io(partial(self._upload, commit.uploads))
        except BaseException as e:
            self._count_orphans(
                getattr(e, "completed_uris", ()),
                f"the upload for group {label} failed partway",
            )
            raise
        payload = {"idempotency_key": str(uuid.uuid4()), **commit.payload}
        try:
            result = await self._publish(payload)
        except FlushHalted:
            raise
        except CommitExhausted:
            # The ladder ran out on ambiguous answers: the commit may have
            # landed, so these objects may be live table files. Never
            # counted as orphans (deleting them could delete data).
            self._ambiguous_uploads += len(commit.uris)
            log.warning(
                "commit for group %s exhausted its retries on ambiguous "
                "answers; it may have landed, so its %d object(s) may be "
                "live: %s",
                label,
                len(commit.uris),
                ", ".join(commit.uris[:_ORPHAN_URIS_LOGGED]),
            )
            raise
        except BaseException:
            self._count_orphans(
                commit.uris, f"the commit for group {label} did not land"
            )
            raise
        self._commits += 1
        self._bytes_written += sum(u.size for u in commit.uploads)
        return result.snapshot_id


# -- the flush loop ---------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SweepReport:
    decisions: int = 0
    committed: int = 0
    dropped: int = 0
    empty: int = 0
    failed: int = 0


@dataclass(slots=True)
class _Failures:
    count: int
    retry_at: float
    #: The group's last failure was a refused batch (or its build failed):
    #: it retries ALONE until it lands.
    split: bool = False


_GroupKey = tuple[str, int, keyspace.PartitionTuple]

#: Group scans one decision runs at once.
_SCAN_CONCURRENCY: Final = 8

#: Startup-drain progress is logged every this many windows or seconds.
DRAIN_LOG_EVERY_WINDOWS: Final = 1000
DRAIN_LOG_EVERY_S: Final = 30.0


class _DrainProgress:
    """One partition's startup-drain progress: counted per flushed
    window, logged every :data:`DRAIN_LOG_EVERY_WINDOWS` windows or
    :data:`DRAIN_LOG_EVERY_S` seconds, and once at the end."""

    def __init__(
        self, where: str, stream: RowStream, monotonic: Callable[[], float]
    ) -> None:
        self._where = where
        self._stream = stream
        self._monotonic = monotonic
        self._started = self._last_log = monotonic()
        self._windows_at_log = 0
        self.windows = 0
        self.rows = 0
        self.bytes = 0
        self.held = 0

    def flushed(self, window: Scan) -> None:
        self.windows += 1
        self.rows += len(window.rows)
        self.bytes += window.nbytes
        self.maybe_log()

    def _line(self) -> str:
        stream = self._stream
        elapsed = max(self._monotonic() - self._started, 1e-9)
        return (
            f"{self.windows} window(s), {self.rows} row(s), "
            f"{self.bytes / 2**20:.1f} MiB flushed from {stream.groups_seen} "
            f"group(s) in {elapsed:.1f}s ({self.rows / elapsed:.0f} rows/s, "
            f"{self.bytes / 2**20 / elapsed:.2f} MiB/s); {self.held} failed "
            f"window(s), {stream.rows_skipped} row(s) of failed groups left staged"
        )

    def maybe_log(self) -> None:
        now = self._monotonic()
        if (
            self.windows - self._windows_at_log < DRAIN_LOG_EVERY_WINDOWS
            and now - self._last_log < DRAIN_LOG_EVERY_S
        ):
            return
        self._last_log = now
        self._windows_at_log = self.windows
        log.info("startup drain of %s: %s", self._where, self._line())

    def finish(self, outcome: str) -> None:
        """``outcome``: streamed, stopped (a stop or a terminal
        condition), failed (an error the drain re-raises) or cancelled (a
        sibling partition's stream failed)."""
        (log.info if outcome in ("streamed", "stopped") else log.warning)(
            "startup drain of %s %s: %s", self._where, outcome, self._line()
        )


class FlushRunner:
    """The flush loop. Every ``sweep_s`` it reads each open stage's
    in-memory index, picks the due groups (planner.py, or the retry
    backoff for a group whose last attempt failed) that are not already
    in flight, and launches them as decisions: a size-lane, backlog or
    split-out group alone, every other due group of a stage packed into
    batches that share commits. At most ``concurrency`` decisions run at
    once (each holds at most about ``flush_bytes`` of scans in memory).

    :meth:`drain` is the startup drain: every staged row is read in one
    sequential pass per stage and flushed in batches of windows, stages
    concurrently; a group whose window fails stays staged and is retried
    until it is empty or stuck.

    Terminal conditions: a :class:`FlushHalted` (table recreated) and a
    fenced stage (:class:`~millrace.stage.FencedError`) stop the loop and
    re-raise from :meth:`run`, :meth:`run_once` and :meth:`drain`."""

    def __init__(
        self,
        stages: StageManager,
        flusher: HoglakeFlusher,
        knobs: PlannerKnobs,
        *,
        now_us: Callable[[], int],
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        sweep_s: float = 1.0,
        concurrency: int = 4,
        batch_max_groups: int = 512,
    ) -> None:
        if sweep_s <= 0:
            raise ValueError(f"sweep_s must be > 0, got {sweep_s}")
        if concurrency < 1:
            raise ValueError(f"concurrency must be >= 1, got {concurrency}")
        if batch_max_groups < 1:
            raise ValueError(f"batch_max_groups must be >= 1, got {batch_max_groups}")
        self._batch_max_groups = batch_max_groups
        self._stages = stages
        self._flusher = flusher
        self._knobs = knobs
        self._now_us = now_us
        self._monotonic = monotonic
        self._sleep = sleep
        self._sweep_s = sweep_s
        self._semaphore = asyncio.Semaphore(concurrency)
        self._inflight: dict[_GroupKey, asyncio.Task[list[FlushReport]]] = {}
        self._failures: dict[_GroupKey, _Failures] = {}
        self._stuck_total = 0
        self._oldest_due_ts: int | None = None
        self._fatal: BaseException | None = None
        self._stopping = False
        self._sweeps = 0
        self._concurrency = concurrency
        self._drain_rows = 0
        self._drain_bytes = 0
        self._in_drain = False

    # -- surface -----------------------------------------------------------------

    @property
    def halted(self) -> BaseException | None:
        """The terminal condition (a halt or a fence), once latched."""
        return self._fatal

    @property
    def sweeps(self) -> int:
        return self._sweeps

    @property
    def oldest_due_staged_ts(self) -> int | None:
        """The oldest ``first_staged_ts`` among groups due and not in
        flight at the last sweep (the alertable backlog signal); None when
        nothing is due."""
        return self._oldest_due_ts

    @property
    def stuck_groups(self) -> int:
        """Groups whose consecutive failures reached :data:`STUCK_AFTER`."""
        return sum(1 for f in list(self._failures.values()) if f.count >= STUCK_AFTER)

    @property
    def drain_rows(self) -> int:
        """Staged rows the startup drain flushed (published or dropped),
        by its streams and its retry pass."""
        return self._drain_rows

    @property
    def drain_bytes(self) -> int:
        """Staged payload bytes the startup drain flushed."""
        return self._drain_bytes

    @property
    def stuck_total(self) -> int:
        """Times a group crossed :data:`STUCK_AFTER` (a counter)."""
        return self._stuck_total

    def stop(self) -> None:
        """Stop after the current sweep. Safe from a signal handler."""
        self._stopping = True

    @property
    def stopping(self) -> bool:
        return self._stopping

    # -- loops -------------------------------------------------------------------

    async def run(self) -> None:
        """Sweep until :meth:`stop` or a terminal condition. Flushes run
        as tasks across sweeps; on the way out the in-flight ones finish."""
        try:
            while not self._stopping:
                self._raise_fatal()
                self._launch(drain=False)
                await self._close_drained_foreign()
                await self._sleep(self._sweep_s)
            self._raise_fatal()
        finally:
            await self._wait_inflight()
        self._raise_fatal()

    async def run_once(self, *, drain: bool = False) -> SweepReport:
        """One sweep, waiting for the decisions it launched."""
        self._raise_fatal()
        launched = self._launch(drain=drain)
        results = await asyncio.gather(*(task for task, _ in launched))
        self._raise_fatal()
        reports = [r for batch in results for r in batch]
        groups = sum(n for _, n in launched)
        return SweepReport(
            decisions=groups,
            committed=sum(r.outcome == "committed" for r in reports),
            dropped=sum(r.outcome == "dropped" for r in reports),
            empty=sum(r.outcome == "empty" for r in reports),
            failed=groups - len(reports),
        )

    async def drain(self) -> None:
        """Flush everything staged, before the consumer starts (nothing
        new arrives meanwhile). First every stage's pinned incarnation is
        checked: a recreated destination halts before anything is
        flushed. Then each stage (the home and every foreign slot)
        streams its rows in one sequential pass (:meth:`_drain_stage`),
        stages concurrently, at most ``concurrency`` batches in memory at
        once. A group whose window
        failed stays staged and indexed; it is retried here until it is
        gone or stuck, and stuck ones keep retrying in :meth:`run`."""
        for topic, partition in self._stages.partitions():
            try:
                await self._flusher.check_stage_identity(
                    self._stages.stage(topic, partition)
                )
            except FlushHalted as halt:
                self._fatal = halt
                raise
        slots = asyncio.Semaphore(self._concurrency)
        streams = [
            asyncio.create_task(
                self._drain_stage(self._stages.stage(t, p), slots),
                name=f"millrace-drain-{t}-{p}",
            )
            for t, p in self._stages.partitions()
        ]
        try:
            # A terminal condition latches and every stream stops at its
            # next window; any other error stops the siblings at once.
            await asyncio.wait(streams, return_when=asyncio.FIRST_EXCEPTION)
        finally:
            for task in streams:
                if not task.done():
                    task.cancel()
            outcomes = await asyncio.gather(*streams, return_exceptions=True)
        self._raise_fatal()
        errors = [o for o in outcomes if isinstance(o, BaseException)]
        # The first real error, not a sibling cancelled because of it.
        errors.sort(key=lambda e: isinstance(e, asyncio.CancelledError))
        if errors:
            raise errors[0]
        self._in_drain = True
        try:
            await self._drain_retries()
        finally:
            self._in_drain = False
        if not self._stopping and self._fatal is None:
            # After a stop the streams left rows unread and unindexed: an
            # "empty" foreign index proves nothing then.
            await self._close_drained_foreign()

    async def _drain_retries(self) -> None:
        """The drain's retry pass over the groups its streams held."""
        while not self._stopping:
            self._raise_fatal()
            pending = [
                key
                for key in self._index_keys()
                if (f := self._failures.get(key)) is None or f.count < STUCK_AFTER
            ]
            if not pending:
                stuck = len(self._index_keys())
                if stuck:
                    log.error(
                        "startup drain finished with %d stuck group(s) still "
                        "staged; they keep retrying while the pod consumes",
                        stuck,
                    )
                return
            report = await self.run_once(drain=True)
            if report.decisions == 0:
                # Everything pending is waiting out a retry backoff.
                await self._sleep(min(self._sweep_s, _RETRY_BASE_S))

    async def _drain_stage(
        self, stage: PartitionStage, slots: asyncio.Semaphore
    ) -> None:
        """Stream one stage's staged rows and flush them window by
        window (stage.py :class:`~millrace.stage.RowStream`). A slot is
        taken BEFORE a window is read and given back when its flush
        ends, so ``slots`` bounds the windows in memory across every
        partition. The older layout's keys are deleted alongside."""
        where = f"{stage.topic}[{stage.partition}]"
        stage.forget_index()
        stream = stage.row_stream(
            max_bytes=self._knobs.flush_bytes, now_us=self._now_us()
        )
        progress = _DrainProgress(where, stream, self._monotonic)
        log.info("startup drain of %s: streaming its staged rows", where)
        cleanup = asyncio.create_task(self._delete_foreign_keys(stage, where))
        flushes: set[asyncio.Task[None]] = set()
        failed: set[keyspace.PartitionTuple] = set()
        outcome = "failed"
        try:
            await self._stream_stage(stage, stream, slots, progress, failed, flushes)
            await stage.make_durable()
            outcome = (
                "stopped" if self._stopping or self._fatal is not None else "streamed"
            )
        except StageClosedError:
            outcome = "stopped"
        except asyncio.CancelledError:
            outcome = "cancelled"
            raise
        except Exception as e:
            if not is_fenced(e):
                raise
            self._fatal = FencedError([(stage.topic, stage.partition)])
            outcome = "stopped"
        finally:
            if flushes:
                await asyncio.gather(*list(flushes), return_exceptions=True)
            if outcome in ("failed", "cancelled"):
                cleanup.cancel()
            await asyncio.gather(cleanup, return_exceptions=True)
            progress.finish(outcome)

    async def _stream_stage(
        self,
        stage: PartitionStage,
        stream: RowStream,
        slots: asyncio.Semaphore,
        progress: _DrainProgress,
        failed: set[keyspace.PartitionTuple],
        flushes: set[asyncio.Task[None]],
    ) -> None:
        """Read windows into batches (a slot each, at most about
        ``flush_bytes`` and ``batch_max_groups`` windows: a window that
        would overflow a non-empty batch is carried into the next one)
        and launch their flushes; returns once the stream is exhausted
        (or stopped) and every launched flush has finished."""
        budget = self._knobs.flush_bytes
        carry: Scan | None = None
        exhausted = False
        try:
            while (
                not self._stopping
                and self._fatal is None
                and not (exhausted and carry is None)
            ):
                await slots.acquire()
                batch: list[Scan] = []
                nbytes = 0
                if carry is not None:
                    batch, nbytes, carry = [carry], carry.nbytes, None
                try:
                    while len(batch) < self._batch_max_groups and nbytes < budget:
                        window = await stream.next_window()
                        if window is None:
                            exhausted = True
                            break
                        if batch and nbytes + window.nbytes > budget:
                            carry = window
                            break
                        batch.append(window)
                        nbytes += window.nbytes
                except BaseException:
                    slots.release()
                    raise
                if not batch:
                    slots.release()
                    break
                task = asyncio.create_task(
                    self._drain_batch(stage, stream, batch, slots, progress, failed)
                )
                flushes.add(task)
                task.add_done_callback(flushes.discard)
                progress.maybe_log()
        finally:
            if flushes:
                await asyncio.gather(*list(flushes), return_exceptions=True)

    async def _drain_batch(
        self,
        stage: PartitionStage,
        stream: RowStream,
        windows: list[Scan],
        slots: asyncio.Semaphore,
        progress: _DrainProgress,
        failed: set[keyspace.PartitionTuple],
    ) -> None:
        held: list[tuple[Scan, Exception, bool]] = []
        try:
            if self._fatal is not None or self._stopping:
                return  # the rows stay staged; the next start drains them
            # Deletes need no durability for at-least-once (a lost one
            # re-flushes a duplicate after a crash), and waiting for each
            # serializes the drain on one object-store PUT per batch.
            # The stream ends with one durable flush (_drain_stage).
            # A fence is therefore only seen at a WAL flush: until then a
            # fenced drain may publish windows the new owner also
            # publishes (duplicates only).
            report = await self._flusher.flush_batch(
                stage, windows, "drain", durable=False
            )
        except FlushHalted as halt:
            self._fatal = halt
        except StageClosedError:
            pass
        except Exception as e:  # noqa: BLE001 - contained per batch
            if is_fenced(e):
                self._fatal = FencedError([(stage.topic, stage.partition)])
                return
            held = [(w, e, False) for w in windows]
        else:
            # Build failures and a refused commit's unpublished groups
            # stay staged and retry alone.
            errors: dict[keyspace.PartitionTuple, Exception] = dict(report.failed)
            if report.refusal is not None:
                errors.update((group, report.refusal) for group in report.refused)
            held = [(w, errors[w.group], True) for w in windows if w.group in errors]
            self._drained([w for w in windows if w.group not in errors], progress)
        finally:
            # Keep each failed group staged and indexed: the stream skips
            # the rest of its rows and the retry loop takes it from here.
            for window, error, split in held:
                stream.hold(
                    window.group,
                    nbytes=window.nbytes,
                    rows=len(window.rows),
                    now_us=self._now_us(),
                )
                if window.group not in failed:
                    failed.add(window.group)
                    self._note_failure(
                        (stage.topic, stage.partition, window.group),
                        stage,
                        error,
                        split=split,
                    )
                progress.held += 1
            slots.release()

    def _drained(self, windows: Sequence[Scan], progress: _DrainProgress) -> None:
        for window in windows:
            self._drain_rows += len(window.rows)
            self._drain_bytes += window.nbytes
            progress.flushed(window)

    async def _delete_foreign_keys(self, stage: PartitionStage, where: str) -> None:
        """Housekeeping alongside the drain: an older build's keys go
        (bulk scans, bounded delete batches). A failure is logged, never
        drain-fatal, except a fence."""
        started = self._monotonic()
        try:
            deleted = await stage.delete_foreign_keys()
        except StageClosedError:
            return
        except Exception as e:  # noqa: BLE001 - housekeeping only
            if is_fenced(e):
                self._fatal = FencedError([(stage.topic, stage.partition)])
                return
            log.warning(
                "%s: deleting an older staging layout's keys failed (%s); "
                "the next start retries",
                where,
                e,
            )
            return
        if deleted:
            log.info(
                "%s: deleted %d key(s) of an older staging layout in %.1fs",
                where,
                deleted,
                self._monotonic() - started,
            )

    # -- internals ---------------------------------------------------------------

    async def _close_drained_foreign(self) -> None:
        """Close the foreign stages (slots other than the home) whose
        rows are all flushed; a stuck group keeps its stage open."""
        try:
            await self._stages.close_idle_foreign()
        except StageClosedError:
            pass

    def _raise_fatal(self) -> None:
        if self._fatal is not None:
            raise self._fatal

    def _index_keys(self) -> list[_GroupKey]:
        keys: list[_GroupKey] = []
        for topic, partition in self._stages.partitions():
            try:
                stage = self._stages.stage(topic, partition)
            except KeyError:
                continue
            keys.extend((topic, partition, s.group) for s in stage.snapshot())
        return keys

    def _launch(
        self, *, drain: bool
    ) -> list[tuple[asyncio.Task[list[FlushReport]], int]]:
        """Start the due groups not in flight, oldest first: a size-lane
        or backlog group, or one whose last failure split it out of a
        batch, alone; every other due group of a stage packed into
        batches of at most ``flush_bytes`` staged and
        ``batch_max_groups`` groups. Each unit is one decision
        (:meth:`_flush`). Returns each task with its group count. The
        index is read in place (no copies): this runs every second."""
        self._sweeps += 1
        now_us = self._now_us()
        now = self._monotonic()
        # Most groups are neither failed, in flight nor due: a sweep
        # skips them on two comparisons, without building a key or
        # calling the planner.
        failures_map = self._failures
        inflight = self._inflight
        size_cut = self._knobs.flush_bytes
        age_cut = due_floor_ts(now_us=now_us, knobs=self._knobs)
        due: list[tuple[int, PartitionStage, keyspace.PartitionTuple, str, int]] = []
        for topic, partition in self._stages.partitions():
            try:
                stage = self._stages.stage(topic, partition)
            except KeyError:
                continue
            for group, state in stage.groups():
                failures = None
                if failures_map or inflight:
                    key = (topic, partition, group)
                    if key in inflight:
                        continue
                    failures = failures_map.get(key)
                if failures is not None:
                    if now < failures.retry_at:
                        continue
                    trigger: str | None = "retry"
                elif (
                    not drain
                    and not state.backlog
                    and state.staged_bytes < size_cut
                    and state.first_staged_ts > age_cut
                ):
                    continue
                elif drain:
                    trigger = "drain"
                elif state.backlog:
                    # A capped scan left rows behind: keep draining them.
                    trigger = "backlog"
                else:
                    trigger = decide(
                        staged_bytes=state.staged_bytes,
                        first_staged_ts=state.first_staged_ts,
                        now_us=now_us,
                        knobs=self._knobs,
                    )
                if trigger is not None:
                    due.append(
                        (
                            state.first_staged_ts,
                            stage,
                            group,
                            trigger,
                            state.staged_bytes,
                        )
                    )
        self._prune_failures()
        self._oldest_due_ts = min((ts for ts, *_ in due), default=None)
        units: list[
            tuple[PartitionStage, str, list[tuple[keyspace.PartitionTuple, int]]]
        ] = []
        open_batch: dict[
            tuple[str, int], tuple[list[tuple[keyspace.PartitionTuple, int]], int]
        ] = {}
        budget = self._knobs.flush_bytes
        for _ts, stage, group, trigger, nbytes in sorted(due, key=lambda d: d[0]):
            key = (stage.topic, stage.partition, group)
            failures = failures_map.get(key)
            if trigger in ("size", "backlog") or (
                failures is not None and failures.split
            ):
                units.append((stage, trigger, [(group, nbytes)]))
                continue
            skey = (stage.topic, stage.partition)
            members, used = open_batch.get(skey, ([], 0))
            if members and (
                len(members) >= self._batch_max_groups or used + nbytes > budget
            ):
                members, used = [], 0
            if not members:
                # The batch carries its oldest group's trigger.
                units.append((stage, trigger, members))
            members.append((group, nbytes))
            open_batch[skey] = (members, used + nbytes)
        launched: list[tuple[asyncio.Task[list[FlushReport]], int]] = []
        for stage, trigger, members in units:
            keys = [(stage.topic, stage.partition, group) for group, _ in members]
            task = asyncio.create_task(
                self._flush(keys, stage, trigger, [nbytes for _, nbytes in members]),
                name=f"millrace-flush-{stage.topic}-{stage.partition}",
            )
            for key in keys:
                self._inflight[key] = task
            launched.append((task, len(keys)))
        return launched

    def _prune_failures(self) -> None:
        """Forget the failure state of groups that left the index (drained,
        or their stage closed). Failures are few; the index is not
        walked."""
        for key in list(self._failures):
            topic, partition, group = key
            try:
                stage = self._stages.stage(topic, partition)
            except KeyError:
                del self._failures[key]
                continue
            if not stage.has_group(group):
                del self._failures[key]

    async def _scan_unit(
        self, stage: PartitionStage, keys: Sequence[_GroupKey], hints: Sequence[int]
    ) -> list[Scan]:
        """Scan a decision's groups, at most :data:`_SCAN_CONCURRENCY` at
        once. Each group's byte cap is its indexed estimate plus an even
        share of the budget the batch leaves unused, so the caps sum to
        ``flush_bytes`` (one group: the whole budget); a group the index
        under-counted is capped and comes back as backlog."""
        budget = self._knobs.flush_bytes
        if len(keys) == 1:
            caps = [budget]
        else:
            slack = max(0, budget - sum(hints)) // len(keys)
            caps = [max(1, hint + slack) for hint in hints]
        now_us = self._now_us()
        gate = asyncio.Semaphore(_SCAN_CONCURRENCY)

        async def one(group: keyspace.PartitionTuple, cap: int, hint: int) -> Scan:
            async with gate:
                return await stage.scan(
                    group, max_bytes=cap, now_us=now_us, size_hint=hint
                )

        return list(
            await asyncio.gather(
                *(
                    one(key[2], cap, hint)
                    for key, cap, hint in zip(keys, caps, hints, strict=True)
                )
            )
        )

    async def _flush(
        self,
        keys: list[_GroupKey],
        stage: PartitionStage,
        trigger: str,
        hints: list[int],
    ) -> list[FlushReport]:
        """One decision: scan the unit's groups, then one
        :meth:`HoglakeFlusher.flush_batch`."""
        try:
            async with self._semaphore:
                if self._fatal is not None:
                    return []
                scans = await self._scan_unit(stage, keys, hints)
                report = await self._flusher.flush_batch(stage, scans, trigger)
        except FlushHalted as halt:
            self._fatal = halt
            return []
        except StageClosedError:
            return []  # shutdown or a fenced stage closed locally
        except Exception as e:  # noqa: BLE001 - contained per decision
            if is_fenced(e):
                self._fatal = FencedError([(stage.topic, stage.partition)])
                return []
            self._note_failures(keys, stage, e, split=False)
            return []
        finally:
            for key in keys:
                self._inflight.pop(key, None)
        # A refused commit splits its unpublished groups out (each then
        # retries alone, which isolates a poisoned one); a group whose
        # build failed retries alone too.
        build_failed = dict(report.failed)
        refused = set(report.refused)
        if report.refusal is not None:
            self._note_failures(
                [k for k in keys if k[2] in refused], stage, report.refusal, split=True
            )
        for key in keys:
            error = build_failed.get(key[2])
            if error is not None:
                self._note_failure(key, stage, error, split=True)
            elif key[2] not in refused:
                self._failures.pop(key, None)
        self._count_drained(report.reports)
        return list(report.reports)

    def _count_drained(self, reports: Sequence[FlushReport]) -> None:
        if self._in_drain:
            # The drain's retry pass: its rows count as drained too.
            for report in reports:
                self._drain_rows += report.staged_rows
                self._drain_bytes += report.staged_bytes

    def _note_failures(
        self,
        keys: Sequence[_GroupKey],
        stage: PartitionStage,
        error: Exception,
        *,
        split: bool,
    ) -> None:
        """Count a failed decision against each of its groups: one group
        logs as itself, a batch logs once (and once if any group became
        stuck)."""
        if not keys:
            return
        if len(keys) == 1:
            self._note_failure(keys[0], stage, error, split=split)
            return
        stuck = sum(
            self._note_failure(key, stage, error, split=split, quiet=True)
            for key in keys
        )
        what = "retrying each group alone" if split else "retrying the batch"
        log.warning(
            "batched flush of %d group(s) on %s[%d] failed (%s); %s",
            len(keys),
            stage.topic,
            stage.partition,
            error,
            what,
        )
        if stuck:
            log.error(
                "flush STUCK: %d group(s) on %s[%d] failed %d times in a row "
                "(last: %s); their rows stay staged and they keep retrying "
                "every %.0fs at most",
                stuck,
                stage.topic,
                stage.partition,
                STUCK_AFTER,
                error,
                _RETRY_MAX_S,
            )

    def _note_failure(
        self,
        key: _GroupKey,
        stage: PartitionStage,
        error: Exception,
        *,
        split: bool = False,
        quiet: bool = False,
    ) -> bool:
        """Count one group's failure and schedule its retry; returns
        whether it just became stuck. ``quiet`` leaves the logging to the
        caller (a batch logs once)."""
        self._flusher.note_failure()
        previous = self._failures.get(key)
        # Once split out of a batch, a group retries alone until it lands.
        split = split or (previous is not None and previous.split)
        count = 1 if previous is None else previous.count + 1
        delay = min(_RETRY_BASE_S * (2 ** (count - 1)), _RETRY_MAX_S)
        self._failures[key] = _Failures(count, self._monotonic() + delay, split)
        became_stuck = count == STUCK_AFTER
        if became_stuck:
            self._stuck_total += 1
        if quiet:
            return became_stuck
        where = f"{stage.topic}[{stage.partition}] group {key[2]}"
        if became_stuck:
            log.error(
                "flush STUCK: %s failed %d times in a row (last: %s); its rows "
                "stay staged and it keeps retrying every %.0fs at most",
                where,
                count,
                error,
                _RETRY_MAX_S,
            )
        elif isinstance(error, FlushError):
            log.warning(
                "flush failed for %s (attempt %d, retry in %.0fs): %s",
                where,
                count,
                delay,
                error,
            )
        else:
            log.warning(
                "flush failed for %s (attempt %d, retry in %.0fs)",
                where,
                count,
                delay,
                exc_info=error,
            )
        return became_stuck

    async def _wait_inflight(self) -> None:
        tasks = list(set(self._inflight.values()))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


def _check_identity(info: TableInfo, table_uuid: str) -> None:
    """A shape read must name the pinned incarnation and carry the
    commit's read basis; anything else halts."""
    if info.table_uuid != table_uuid:
        raise FlushHalted(
            "table_recreated",
            f"table {info.namespace}.{info.name} now resolves to "
            f"{info.table_uuid}, not the pinned incarnation {table_uuid}. "
            "A human decides what happened; staged rows stay durable.",
        )
    if info.read_snapshot_id is None:
        raise FlushHalted(
            "server_capability",
            f"the server stopped reporting read_snapshot_id for "
            f"{info.namespace}.{info.name}",
        )
