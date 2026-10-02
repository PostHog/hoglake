"""Wire-object dataclasses. Attribute names match the wire (snake_case) exactly."""

from __future__ import annotations

import base64
from collections.abc import Callable, Mapping
from dataclasses import dataclass, fields
from datetime import datetime
from typing import Any, TypeVar

from .errors import MalformedResponseError

T = TypeVar("T")


def _parse_dt(value: str) -> datetime:
    # Python >= 3.11 fromisoformat accepts 'Z' and fractional offsets.
    return datetime.fromisoformat(value)


def _pick(cls: type, d: Mapping[str, Any]) -> dict[str, Any]:
    names = {f.name for f in fields(cls)}
    return {k: v for k, v in d.items() if k in names}


def _wire(model: str, d: Any, build: Callable[[Mapping[str, Any]], T]) -> T:
    """Shared ``from_wire`` guard (bugs.md #24): every structural defect
    in a response body — missing required field, wrong-typed value,
    non-object where an object was expected — surfaces as ONE typed
    client-side error, :class:`MalformedResponseError`, naming the model
    and the offending field. Without it the hand-rolled parsers leaked
    ``KeyError`` (direct indexing), ``AttributeError`` (non-dict nesteds
    hitting ``_pick``), and ``TypeError`` (``_pick`` models missing a
    required constructor argument)."""
    if not isinstance(d, Mapping):
        raise MalformedResponseError(
            f"{model}: expected a JSON object, got {type(d).__name__}"
        )
    try:
        return build(d)
    except MalformedResponseError:
        raise  # a nested model already produced the precise error
    except KeyError as e:
        raise MalformedResponseError(
            f"{model}: missing required field {e.args[0]!r}"
        ) from e
    except (TypeError, ValueError, AttributeError) as e:
        raise MalformedResponseError(f"{model}: malformed response: {e}") from e


@dataclass(frozen=True)
class CatalogInfo:
    name: str
    data_path: str
    head_snapshot_id: int
    schema_version: int

    @classmethod
    def from_wire(cls, d: dict[str, Any]) -> CatalogInfo:
        return _wire("CatalogInfo", d, lambda d: cls(**_pick(cls, d)))


@dataclass(frozen=True)
class CatalogOptions:
    consumer_floor: bool
    earliest_snapshot_id: int
    snapshot_retention_seconds: int | None = None

    @classmethod
    def from_wire(cls, d: dict[str, Any]) -> CatalogOptions:
        return _wire("CatalogOptions", d, lambda d: cls(**_pick(cls, d)))


@dataclass(frozen=True)
class ExpiryResult:
    snapshots_expired: int
    data_files_queued: int
    delete_files_queued: int
    new_earliest_snapshot_id: int
    floored_by_consumer: str | None = None
    #: Superseded consumer offsets the sweep deleted — the only work a
    #: retention-disabled sweep can do. Defaulted: a run recorded before
    #: the counter existed replays without it.
    offsets_released: int = 0

    @classmethod
    def from_wire(cls, d: dict[str, Any]) -> ExpiryResult:
        return _wire("ExpiryResult", d, lambda d: cls(**_pick(cls, d)))


@dataclass(frozen=True)
class CleanupResult:
    removed: int
    missing: int
    still_referenced: int

    @classmethod
    def from_wire(cls, d: dict[str, Any]) -> CleanupResult:
        return _wire("CleanupResult", d, lambda d: cls(**_pick(cls, d)))


@dataclass(frozen=True)
class SnapshotChange:
    kind: str
    object_id: int | None = None

    @classmethod
    def from_wire(cls, d: dict[str, Any]) -> SnapshotChange:
        return _wire("SnapshotChange", d, lambda d: cls(**_pick(cls, d)))


@dataclass(frozen=True)
class Snapshot:
    snapshot_id: int
    snapshot_time: datetime
    schema_version: int
    author: str | None = None
    message: str | None = None
    changes: tuple[SnapshotChange, ...] = ()

    @classmethod
    def from_wire(cls, d: dict[str, Any]) -> Snapshot:
        return _wire(
            "Snapshot",
            d,
            lambda d: cls(
                snapshot_id=d["snapshot_id"],
                snapshot_time=_parse_dt(d["snapshot_time"]),
                schema_version=d["schema_version"],
                author=d.get("author"),
                message=d.get("message"),
                changes=tuple(
                    SnapshotChange.from_wire(c) for c in (d.get("changes") or ())
                ),
            ),
        )


@dataclass(frozen=True)
class ConsumerOffset:
    consumer_id: str
    table_uuid: str
    committed_snapshot: int
    updated_at: datetime

    @classmethod
    def from_wire(cls, d: dict[str, Any]) -> ConsumerOffset:
        def build(d: Mapping[str, Any]) -> ConsumerOffset:
            kw = dict(_pick(cls, d))
            kw["updated_at"] = _parse_dt(d["updated_at"])
            return cls(**kw)

        return _wire("ConsumerOffset", d, build)


@dataclass(frozen=True)
class TableSummary:
    """One row of a namespace's table listing.

    Everything past ``table_uuid`` is resolved at the catalog HEAD (the
    listing takes no snapshot parameter), and every one of those fields
    defaults to ``None`` so a client on this version keeps working
    against a server that predates them — ``_pick`` drops what the wire
    does not carry, and the constructor supplies the default.

    ``None`` rather than ``0`` for the counts, deliberately: against an
    older server "this table has no files" and "this server never told
    me" are different facts, and zero asserts the wrong one. Against a
    current server they are always present.
    """

    name: str
    table_uuid: str
    #: Table comment at head; ``None`` when unset (or unsupported).
    comment: str | None = None
    #: Rows in the data files live at head.
    record_count: int | None = None
    #: Data files live at head.
    file_count: int | None = None
    #: Total size of the data files live at head.
    file_size_bytes: int | None = None
    #: Retained snapshots carrying a change row for this table. Snapshots
    #: are catalog-wide, so a table's are defined through the change log;
    #: the number SHRINKS as expiry advances the catalog floor.
    snapshot_count: int | None = None
    #: Smallest such snapshot id — the oldest point this table can still
    #: be read at. ``None`` when no retained snapshot names the table.
    earliest_snapshot_id: int | None = None

    @classmethod
    def from_wire(cls, d: dict[str, Any]) -> TableSummary:
        return _wire("TableSummary", d, lambda d: cls(**_pick(cls, d)))


@dataclass(frozen=True)
class Column:
    """One catalog column.

    Recursive: a container type (``list``/``struct``/``map``) carries its
    ``children``, each with its own server-assigned ``field_id``.
    ``ordinal`` orders a column among its SIBLINGS — top-level columns
    share one sequence, and each container's children have their own —
    so it is not a table-wide position.
    """

    name: str
    type: str
    field_id: int
    ordinal: int
    nullable: bool = True
    type_params: dict[str, Any] | None = None
    #: Versioned column comment; None when the column has none.
    comment: str | None = None
    #: Present only for list/struct/map.
    children: tuple[Column, ...] | None = None

    @classmethod
    def from_wire(cls, d: dict[str, Any]) -> Column:
        def build(d: Mapping[str, Any]) -> Column:
            kw = dict(_pick(cls, d))
            kids = kw.get("children")
            if kids is not None:
                if not isinstance(kids, (list, tuple)):
                    raise MalformedResponseError(
                        "Column: children must be an array or null, got "
                        f"{type(kids).__name__}"
                    )
                # Sorted by ordinal HERE, once, at the wire boundary.
                # The server returns children in ordinal order today, but
                # ordinal is the contract and array order is not — and
                # every consumer downstream (the Arrow schema builder,
                # the stats walk, the console) reads position. One sort
                # at the edge beats three that can disagree.
                kw["children"] = tuple(
                    sorted(
                        (Column.from_wire(c) for c in kids),
                        key=lambda c: c.ordinal,
                    )
                )
            return cls(**kw)

        return _wire("Column", d, build)


@dataclass(frozen=True)
class PartitionField:
    source_field_id: int
    transform: str
    transform_param: int | None = None

    @classmethod
    def from_wire(cls, d: dict[str, Any]) -> PartitionField:
        return _wire("PartitionField", d, lambda d: cls(**_pick(cls, d)))

    def to_wire(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "source_field_id": self.source_field_id,
            "transform": self.transform,
        }
        if self.transform_param is not None:
            out["transform_param"] = self.transform_param
        return out


@dataclass(frozen=True)
class PartitionSpec:
    spec_id: int
    fields: tuple[PartitionField, ...] = ()

    @classmethod
    def from_wire(cls, d: dict[str, Any]) -> PartitionSpec:
        return _wire(
            "PartitionSpec",
            d,
            lambda d: cls(
                spec_id=d["spec_id"],
                fields=tuple(
                    PartitionField.from_wire(f) for f in (d.get("fields") or ())
                ),
            ),
        )


@dataclass(frozen=True)
class SortField:
    source_field_id: int
    direction: str
    null_order: str

    @classmethod
    def from_wire(cls, d: dict[str, Any]) -> SortField:
        return _wire("SortField", d, lambda d: cls(**_pick(cls, d)))


@dataclass(frozen=True)
class SortSpec:
    sort_id: int
    fields: tuple[SortField, ...]

    @classmethod
    def from_wire(cls, d: dict[str, Any]) -> SortSpec:
        return _wire(
            "SortSpec",
            d,
            lambda d: cls(
                sort_id=d["sort_id"],
                fields=tuple(SortField.from_wire(f) for f in d["fields"]),
            ),
        )


@dataclass(frozen=True)
class TableInfo:
    name: str
    namespace: str
    table_uuid: str
    columns: tuple[Column, ...]
    #: Rows, files and bytes over the table's live files -- and all three
    #: are OPTIONAL on the wire since the server stopped aggregating the
    #: manifest on every table GET (hoglake #232). They are ``None``
    #: when:
    #:
    #: * the read asked for ``totals=false`` (the identity read: name,
    #:   UUID, columns, specs, no file read at all); or
    #: * a head read the server's maintenance sample does not cover --
    #:   it has never published for the catalog, or the table was created
    #:   after the published sample's snapshot.
    #:
    #: ``None`` means NOT SAMPLED and never zero -- an unsampled table
    #: and an empty one are different facts, and the server keeps them
    #: apart (a covered table with no files samples as real zeros). A
    #: time-travel read (``snapshot``/``at_timestamp``) still aggregates
    #: the manifest, so its numbers are exact and never None.
    #:
    #: Accepting None is what lets an OLDER client keep working against a
    #: newer server: this is tolerance, not a feature, and a caller that
    #: needs a number must handle the absence rather than assume it.
    record_count: int | None
    file_count: int | None
    file_size_bytes: int | None
    partition_spec: PartitionSpec | None = None
    sort_spec: SortSpec | None = None
    # The snapshot a create/alter commit just made; None on any read
    # (a getTable response carries no snapshot_id), so a client can tell
    # "this TableInfo came with a fresh DDL pin" apart from a read.
    snapshot_id: int | None = None
    #: Versioned table comment; None when the table has none.
    comment: str | None = None
    #: Inert user metadata; None when no properties are set.
    properties: dict[str, str] | None = None
    #: The snapshot the three totals above are exact AS OF, when they are
    #: a SAMPLE; None when they are exact (a time-travel read, a DDL
    #: receipt), absent, or the server is too old to report it. Its
    #: presence is how a caller tells a sampled total from an exact one.
    #:
    #: The same value ``PartitionListing.sampled_snapshot_id`` carries,
    #: from the same sampler row; the ``totals_`` prefix scopes it to the
    #: three fields above, because a TableInfo mixes them with metadata
    #: resolved at :attr:`read_snapshot_id`.
    totals_snapshot_id: int | None = None
    #: When ``totals_snapshot_id`` was captured, as the server's ISO-8601
    #: instant -- the sampler's scan START, not its publish, so it is the
    #: instant these numbers describe. Present and absent exactly when
    #: ``totals_snapshot_id`` is.
    #:
    #: The same instant ``PartitionListing.sample_started`` carries, NOT
    #: its ``sampled_at`` twin (the publish, which a generation's runtime
    #: puts up to tens of minutes later).
    totals_as_of: str | None = None
    #: The snapshot this whole response was RESOLVED at: head when the
    #: read named none, the named one otherwise. Present on every
    #: response of a current server, ``totals=false`` included, because
    #: it describes the read rather than the totals -- a caller can cache
    #: a TableInfo and send this as a commit's ``read_snapshot``, and the
    #: server's OCC then validates the cache.
    #:
    #: OPTIONAL so an older server still parses, and DISTINCT from
    #: :attr:`snapshot_id`: that one is set only by create/alter and this
    #: client treats its presence as a DDL pin.
    read_snapshot_id: int | None = None

    @classmethod
    def from_wire(cls, d: dict[str, Any]) -> TableInfo:
        def build(d: Mapping[str, Any]) -> TableInfo:
            spec = d.get("partition_spec")
            return cls(
                name=d["name"],
                namespace=d["namespace"],
                table_uuid=d["table_uuid"],
                columns=tuple(Column.from_wire(c) for c in (d.get("columns") or ())),
                # .get, not [...]: optional on the wire now -- see the
                # field comments. Indexing would turn every unsampled
                # table and every totals=false read into a
                # MalformedResponseError.
                record_count=d.get("record_count"),
                file_count=d.get("file_count"),
                file_size_bytes=d.get("file_size_bytes"),
                partition_spec=PartitionSpec.from_wire(spec) if spec else None,
                sort_spec=SortSpec.from_wire(d["sort_spec"])
                if d.get("sort_spec")
                else None,
                snapshot_id=d.get("snapshot_id"),
                comment=d.get("comment"),
                properties=dict(d["properties"]) if d.get("properties") else None,
                totals_snapshot_id=d.get("totals_snapshot_id"),
                totals_as_of=d.get("totals_as_of"),
                read_snapshot_id=d.get("read_snapshot_id"),
            )

        return _wire("TableInfo", d, build)


@dataclass(frozen=True)
class ColumnStats:
    """Client-computed footer stats for one column of one file.

    Bounds are raw Iceberg single-value binary; base64 is applied at
    serialization time.
    """

    field_id: int
    value_count: int
    null_count: int
    nan_count: int | None = None
    size_bytes: int | None = None
    lower_bound: bytes | None = None
    upper_bound: bytes | None = None

    def to_wire(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "field_id": self.field_id,
            "value_count": self.value_count,
            "null_count": self.null_count,
        }
        if self.nan_count is not None:
            out["nan_count"] = self.nan_count
        if self.size_bytes is not None:
            out["size_bytes"] = self.size_bytes
        if self.lower_bound is not None:
            out["lower_bound"] = base64.b64encode(self.lower_bound).decode("ascii")
        if self.upper_bound is not None:
            out["upper_bound"] = base64.b64encode(self.upper_bound).decode("ascii")
        return out


@dataclass(frozen=True)
class DataFile:
    data_file_id: int
    path: str
    file_format: str
    record_count: int
    file_size_bytes: int
    row_id_start: int
    stats_state: str
    begin_snapshot: int
    #: Serialized thrift FileMetaData length: the 4-byte LE value stored
    #: in the parquet trailer, EXCLUDING the trailing 8-byte suffix
    #: (length + "PAR1"). The server tail-reads the footer as
    #: [file_size - footer_size - 8, file_size).
    footer_size: int | None = None
    spec_id: int | None = None
    partition_values: tuple[str | None, ...] | None = None

    @classmethod
    def from_wire(cls, d: dict[str, Any]) -> DataFile:
        def build(d: Mapping[str, Any]) -> DataFile:
            kw = dict(_pick(cls, d))
            pv = kw.get("partition_values")
            if pv is not None:
                # bugs.md #18: tuple("abc") would silently char-split a
                # wrong-typed string into ('a', 'b', 'c'); only an array
                # (or null) is a legal wire shape here.
                if not isinstance(pv, (list, tuple)):
                    raise MalformedResponseError(
                        "DataFile: partition_values must be an array or "
                        f"null, got {type(pv).__name__}"
                    )
                kw["partition_values"] = tuple(pv)
            return cls(**kw)

        return _wire("DataFile", d, build)


@dataclass(frozen=True)
class DeleteFile:
    delete_file_id: int
    data_file_id: int
    path: str
    file_format: str
    delete_count: int
    file_size_bytes: int
    begin_snapshot: int

    @classmethod
    def from_wire(cls, d: dict[str, Any]) -> DeleteFile:
        return _wire("DeleteFile", d, lambda d: cls(**_pick(cls, d)))


@dataclass(frozen=True)
class ScanFile:
    data_file: DataFile
    delete_file: DeleteFile | None = None

    @classmethod
    def from_wire(cls, d: dict[str, Any]) -> ScanFile:
        def build(d: Mapping[str, Any]) -> ScanFile:
            df = d.get("delete_file")
            return cls(
                data_file=DataFile.from_wire(d["data_file"]),
                delete_file=DeleteFile.from_wire(df) if df else None,
            )

        return _wire("ScanFile", d, build)


@dataclass(frozen=True)
class ChangesPlan:
    table_uuid: str
    from_snapshot: int
    to_snapshot: int
    files: tuple[DataFile, ...] = ()
    delete_files: tuple[DeleteFile, ...] = ()

    @classmethod
    def from_wire(cls, d: dict[str, Any]) -> ChangesPlan:
        return _wire(
            "ChangesPlan",
            d,
            lambda d: cls(
                table_uuid=d["table_uuid"],
                from_snapshot=d["from_snapshot"],
                to_snapshot=d["to_snapshot"],
                files=tuple(DataFile.from_wire(f) for f in (d.get("files") or ())),
                delete_files=tuple(
                    DeleteFile.from_wire(f) for f in (d.get("delete_files") or ())
                ),
            ),
        )


@dataclass(frozen=True)
class ViewInfo:
    name: str
    namespace: str
    view_uuid: str
    dialect: str
    sql: str

    @classmethod
    def from_wire(cls, d: dict[str, Any]) -> ViewInfo:
        return _wire("ViewInfo", d, lambda d: cls(**_pick(cls, d)))


@dataclass(frozen=True)
class CommitResult:
    snapshot_id: int
    schema_version: int | None = None

    @classmethod
    def from_wire(cls, d: dict[str, Any]) -> CommitResult:
        return _wire("CommitResult", d, lambda d: cls(**_pick(cls, d)))


@dataclass(frozen=True)
class AppendedFile:
    """One parquet file :meth:`pyhoglake.Table.append` wrote and registered.

    ``partition_values`` is the transformed partition tuple shipped on
    the wire for this file (by key_index of the table's live spec), or
    None for an unpartitioned table.
    """

    path: str
    record_count: int
    partition_values: tuple[str | None, ...] | None = None


@dataclass(frozen=True)
class AppendResult(CommitResult):
    """:class:`CommitResult` plus the files the CLIENT wrote for this
    append (client-side knowledge, not parsed from the wire): one per
    partition tuple for partitioned tables, exactly one otherwise."""

    files: tuple[AppendedFile, ...] = ()
