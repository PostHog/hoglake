"""Environment-driven configuration, validated at startup.

Every knob is a ``MILLRACE_*`` environment variable; :func:`load_config`
reports every problem at once, each naming its knob. One process = one
topic → one table.

| variable | default | meaning |
|---|---|---|
| ``MILLRACE_KAFKA_BOOTSTRAP_SERVERS`` | required | librdkafka bootstrap list |
| ``MILLRACE_KAFKA_TOPIC`` | required | the one topic this pipeline consumes |
| ``MILLRACE_KAFKA_GROUP_ID`` | ``millrace-{topic}`` | offset-storage group (static assignment; it only namespaces ``__consumer_offsets``) |
| ``MILLRACE_KAFKA_AUTO_OFFSET_RESET`` | ``latest`` | where a partition with no committed offset starts: ``latest`` or ``earliest`` |
| ``MILLRACE_KAFKA_CONSUMER_<KEY>`` | none | librdkafka passthrough, applied last: ``<KEY>`` lowercased with ``_`` → ``.`` (``..._SECURITY_PROTOCOL=SSL``). Keys millrace owns are refused |
| ``MILLRACE_REPLICA_COUNT`` | required | StatefulSet replicas; this replica claims every partition ``p`` with ``p % replica_count == ordinal`` |
| ``POD_NAME`` (fallback ``HOSTNAME``) | required | the ordinal is its trailing ``-<n>`` |
| ``MILLRACE_CATALOG`` / ``MILLRACE_NAMESPACE`` / ``MILLRACE_TABLE`` | required | the destination table |
| ``MILLRACE_CATALOG_DATA_PATH`` | none | ``s3://bucket/prefix/`` for creating a missing catalog |
| ``MILLRACE_TABLE_COLUMNS`` | none | ``name:type,…`` for creating a missing table (complete; no schema evolution) |
| ``MILLRACE_PARTITION_BY`` | none | partition spec for creating the table: ``col``, ``identity(col)``, ``year/month/day/hour(col)``, ``bucket(col, N)`` |
| ``MILLRACE_SORT_BY`` | none | sort order for creating the table: ``col [asc|desc] [nulls_first|nulls_last]``, … |
| ``MILLRACE_STAGE_URL`` | required | staging root (``s3://bucket/prefix``, ``file:///path``, ``memory:///``); one SlateDB per replica (its home stage) at ``<root>/<topic>/<slot>``, the slot of its smallest claimed partition |
| ``MILLRACE_FLUSH_BYTES`` | 268435456 (256 MiB) | staged bytes per flush decision: the size lane fires at it and a scan reads at most it (always at least one row) |
| ``MILLRACE_FLUSH_DEADLINE_S`` | 900 | age lane: a group at least this old with at least 1 MiB staged flushes |
| ``MILLRACE_SLOW_LANE_DEADLINE_S`` | 21600 | slow lane: any group this old flushes; must be ≥ the flush deadline |
| ``MILLRACE_METRICS_PORT`` | 8000 | ops HTTP port (0 = ephemeral) |

Everything else is a constant (fields of :class:`Config` with defaults,
overridable in-process by tests). The missing catalog, namespace or
table is created from the creation knobs at startup (destination.py);
an existing object is never altered.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any, Final

from pyhoglake import UnsupportedTypeError, ValidationError
from pyhoglake.transforms import check_transform
from pyhoglake.types import NESTED_TYPES, arrow_type_to_coltype, coltype_to_arrow

from .planner import (
    FLUSH_BYTES_DEFAULT,
    FLUSH_DEADLINE_S_DEFAULT,
    MIN_FLUSH_BYTES_DEFAULT,
    SLOW_LANE_DEADLINE_S_DEFAULT,
    PlannerKnobs,
)


class ConfigError(Exception):
    """Startup configuration is invalid; ``problems`` names every knob
    at fault."""

    def __init__(self, problems: list[str]) -> None:
        self.problems = tuple(problems)
        super().__init__(
            f"invalid millrace configuration ({len(problems)} problem(s)):\n"
            + "\n".join(f"  - {p}" for p in problems)
        )


class AutoOffsetReset(Enum):
    """Where a partition with no committed offset starts. ``LATEST`` is
    the fleet policy (a fresh group under ``earliest`` replays the
    topic's whole retention)."""

    EARLIEST = "earliest"
    LATEST = "latest"


#: The binding requires ``max_unflushed_bytes > l0_sst_size_bytes``
#: (64 MiB, never overridden).
SLATEDB_L0_SST_SIZE_BYTES: Final = 64 * 1024 * 1024
#: The pod-level shared SST cache (the library default is 512 + 128 MiB
#: per INSTANCE).
SLATEDB_BLOCK_CACHE_BYTES_DEFAULT: Final = 128 * 1024 * 1024
SLATEDB_META_CACHE_BYTES_DEFAULT: Final = 128 * 1024 * 1024
SLATEDB_CACHE_MIN_BYTES: Final = 4096
SLATEDB_CACHE_MAX_BYTES_EXCLUSIVE: Final = 2**64


@dataclass(frozen=True)
class SlateDbConfig:
    """SlateDB tuning for the partition stages. The library defaults
    (1 GiB unflushed and a 640 MiB cache per instance) times K
    partitions per pod have no pod-level cap; these do."""

    max_unflushed_bytes: int = 256 * 1024 * 1024
    l0_max_ssts: int = 8
    block_cache_bytes: int = SLATEDB_BLOCK_CACHE_BYTES_DEFAULT
    meta_cache_bytes: int = SLATEDB_META_CACHE_BYTES_DEFAULT

    def __post_init__(self) -> None:
        if self.max_unflushed_bytes <= SLATEDB_L0_SST_SIZE_BYTES:
            raise ValueError(
                f"max_unflushed_bytes must be greater than l0_sst_size_bytes "
                f"({SLATEDB_L0_SST_SIZE_BYTES}), got {self.max_unflushed_bytes}"
            )
        if self.l0_max_ssts < 1:
            raise ValueError(f"l0_max_ssts must be >= 1, got {self.l0_max_ssts}")
        validate_slatedb_cache_bytes(self.block_cache_bytes, self.meta_cache_bytes)


def validate_slatedb_cache_bytes(block_cache_bytes: int, meta_cache_bytes: int) -> None:
    """Each half is 0 or in ``[4096, 2**64)``, and 0 means "no cache"
    only for the pair: the split cache needs both halves."""
    for name, value in (
        ("block_cache_bytes", block_cache_bytes),
        ("meta_cache_bytes", meta_cache_bytes),
    ):
        if value < 0 or value >= SLATEDB_CACHE_MAX_BYTES_EXCLUSIVE:
            raise ValueError(f"{name}={value} must be in [0, 2**64)")
        if 0 < value < SLATEDB_CACHE_MIN_BYTES:
            raise ValueError(
                f"{name}={value} is below {SLATEDB_CACHE_MIN_BYTES} (one SST "
                "block); use 0 for both halves to disable the cache"
            )
    if (block_cache_bytes == 0) != (meta_cache_bytes == 0):
        raise ValueError(
            "block_cache_bytes and meta_cache_bytes must both be 0 (cache "
            f"disabled) or both positive; got {block_cache_bytes}, {meta_cache_bytes}"
        )


@dataclass(frozen=True)
class ColumnSpec:
    """One configured destination column (``MILLRACE_TABLE_COLUMNS``):
    a hoglake type name plus its parameters (``decimal(p,s)`` →
    ``(("precision", p), ("scale", s))``). Always nullable."""

    name: str
    col_type: str
    type_params: tuple[tuple[str, int], ...] = ()

    def params(self) -> dict[str, int] | None:
        return dict(self.type_params) or None

    def arrow_type(self) -> Any:
        return coltype_to_arrow(self.col_type, self.params())


@dataclass(frozen=True)
class PartitionFieldSpec:
    """One configured partition field (``MILLRACE_PARTITION_BY``):
    millpond's ``(column, transform, param)`` triple."""

    column: str
    transform: str
    param: int | None = None

    def describe(self) -> str:
        """The field in the knob's own grammar (pasteable)."""
        if self.param is not None:
            return f"{self.transform}({self.column}, {self.param})"
        return f"{self.transform}({self.column})"


#: millpond's ``_HOGLAKE_TRANSFORMS`` (config.py): the server's
#: partition-transform vocabulary as the grammar admits it.
PARTITION_TRANSFORMS: Final = ("identity", "year", "month", "day", "hour", "bucket")
#: The sort direction and null order a bare ``MILLRACE_SORT_BY`` column
#: gets (millpond's ``_want_sort_fields``: asc, nulls_last — the order
#: its own batch sort writes).
SORT_DIRECTION: Final = "asc"
SORT_NULL_ORDER: Final = "nulls_last"
SORT_DIRECTIONS: Final = ("asc", "desc")
SORT_NULL_ORDERS: Final = ("nulls_first", "nulls_last")


@dataclass(frozen=True)
class SortFieldSpec:
    """One configured sort field (``MILLRACE_SORT_BY``): a column, its
    direction and its null order — the server's ``SortField`` by name."""

    column: str
    direction: str = SORT_DIRECTION
    null_order: str = SORT_NULL_ORDER

    def describe(self) -> str:
        """The field in the knob's own grammar (pasteable)."""
        return f"{self.column} {self.direction} {self.null_order}"


_DEFAULT_STAGE_BASE: Final = "millrace"

# Kafka topic names: https://kafka.apache.org/documentation/#topicnames
_TOPIC_NAME: Final = re.compile(r"^[a-zA-Z0-9._-]{1,249}$")
# hoglake identifier rules.
_CATALOG_NAME: Final = re.compile(r"^[a-z][a-z0-9_-]{0,62}$")
_IDENTIFIER: Final = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]{0,127}$")
_S3_BUCKET: Final = re.compile(r"^[a-z0-9][a-z0-9.\-]{1,61}[a-z0-9]$")
# A StatefulSet pod name's trailing ordinal.
_POD_ORDINAL: Final = re.compile(r"-(\d+)$")
# ``transform(args)`` in MILLRACE_PARTITION_BY.
_PARTITION_CALL: Final = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)\s*\(\s*(.*?)\s*\)$")
# ``decimal(p,s)`` in MILLRACE_TABLE_COLUMNS.
_DECIMAL_TYPE: Final = re.compile(r"^decimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$")
_DECIMAL_MAX_PRECISION: Final = 38
# Column names the server reserves for its own physical columns.
_RESERVED_COLUMN_PREFIX: Final = "_hog"

_KAFKA_CONSUMER_PREFIX: Final = "MILLRACE_KAFKA_CONSUMER_"
#: librdkafka keys the passthrough may not set (millrace owns them for
#: correctness); the value names the dedicated knob, if any.
_OWNED_KAFKA_KEYS: Final[dict[str, str | None]] = {
    "bootstrap.servers": "MILLRACE_KAFKA_BOOTSTRAP_SERVERS",
    "group.id": "MILLRACE_KAFKA_GROUP_ID",
    "auto.offset.reset": "MILLRACE_KAFKA_AUTO_OFFSET_RESET",
    "enable.auto.commit": None,
    "enable.auto.offset.store": None,
    "partition.assignment.strategy": None,
}


@dataclass(frozen=True)
class Config:
    """Validated configuration for one pipeline. Fields below the knob
    block are constants (no environment variable) that tests may
    override in-process."""

    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_group_id: str
    replica_count: int
    ordinal: int
    catalog: str
    namespace: str
    table: str
    stage_store_url: str
    stage_base_path: str
    kafka_auto_offset_reset: AutoOffsetReset = AutoOffsetReset.LATEST
    kafka_consumer_overrides: tuple[tuple[str, str], ...] = ()
    flush_bytes: int = FLUSH_BYTES_DEFAULT
    flush_deadline_s: int = FLUSH_DEADLINE_S_DEFAULT
    slow_lane_deadline_s: int = SLOW_LANE_DEADLINE_S_DEFAULT
    metrics_port: int = 8000
    catalog_data_path: str | None = None
    table_columns: tuple[ColumnSpec, ...] | None = None
    partition_by: tuple[PartitionFieldSpec, ...] | None = None
    sort_by: tuple[SortFieldSpec, ...] | None = None
    # -- constants --
    #: The age lane's minimum staged bytes.
    min_flush_bytes: int = MIN_FLUSH_BYTES_DEFAULT
    #: Files per commit; a decision that fans out wider splits into
    #: several commits.
    max_files_per_commit: int = 512
    #: Flush decisions in flight process-wide (each holds one scan and
    #: its build, ~4x the scan at peak). 2, not more: the builds are
    #: GIL-bound, so more decisions add memory, not throughput.
    flush_concurrency: int = 2
    #: Pause between flush sweeps. A sweep reads only the in-memory
    #: index, so it is cheap.
    flush_sweep_s: float = 1.0
    #: Records per poll, and the poll's byte bound (the record count is
    #: clamped to this ÷ the observed mean record size).
    consume_batch_size: int = 65536
    consume_batch_max_bytes: int = 256 * 1024 * 1024
    poll_timeout_ms: int = 500
    slatedb: SlateDbConfig = SlateDbConfig()

    def planner_knobs(self) -> PlannerKnobs:
        return PlannerKnobs(
            flush_bytes=self.flush_bytes,
            flush_deadline_s=self.flush_deadline_s,
            slow_lane_deadline_s=self.slow_lane_deadline_s,
            min_flush_bytes=self.min_flush_bytes,
        )


def _required(env: Mapping[str, str], name: str, problems: list[str]) -> str | None:
    raw = env.get(name, "").strip()
    if not raw:
        problems.append(f"{name} is required and is not set (or is empty)")
        return None
    return raw


def _positive_int(
    env: Mapping[str, str], name: str, default: int, problems: list[str]
) -> int | None:
    """A positive-integer knob, or its default. Zero is refused: a
    misrendered unset value must fail loudly."""
    raw = env.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        problems.append(f"{name}={raw!r} must be a positive integer")
        return None
    if value <= 0:
        problems.append(f"{name}={value} must be a positive integer")
        return None
    return value


def _enum[E: Enum](
    env: Mapping[str, str],
    name: str,
    choices: Sequence[E],
    default: E,
    problems: list[str],
) -> E | None:
    raw = env.get(name, "").strip()
    if not raw:
        return default
    for member in choices:
        if member.value == raw:
            return member
    valid = ", ".join(str(m.value) for m in choices)
    problems.append(f"{name}={raw!r} must be one of: {valid}")
    return None


def _parse_ordinal(pod_name: str) -> int:
    """The StatefulSet ordinal from a pod name: its trailing ``-<n>``
    (``millrace-events-3`` → 3). millpond's ``_parse_ordinal`` verbatim
    (millpond/config.py), so the two services read the same pod name
    the same way."""
    match = _POD_ORDINAL.search(pod_name)
    if not match:
        raise ValueError(f"cannot parse an ordinal from pod name {pod_name!r}")
    return int(match.group(1))


def _replica_and_ordinal(
    env: Mapping[str, str], problems: list[str]
) -> tuple[int | None, int | None]:
    """``MILLRACE_REPLICA_COUNT`` and the pod ordinal (``POD_NAME``,
    falling back to ``HOSTNAME`` — the downward API sets the former,
    Kubernetes the latter), each a problem on its own, plus the one
    relation between them: the ordinal names a replica that exists."""
    replica_count: int | None = None
    if _required(env, "MILLRACE_REPLICA_COUNT", problems) is not None:
        # Present, so the default is never used: _positive_int only
        # parses and refuses zero/negative/garbage.
        replica_count = _positive_int(env, "MILLRACE_REPLICA_COUNT", 1, problems)
    pod_var = "POD_NAME" if env.get("POD_NAME", "").strip() else "HOSTNAME"
    pod_name = env.get(pod_var, "").strip()
    # A refusal of the fallback must say so: the fix is almost always
    # "wire POD_NAME from the downward API", not "rename the host".
    fallback = (
        " (HOSTNAME was the fallback because POD_NAME is unset; set POD_NAME "
        "from the downward API, metadata.name)"
        if pod_var == "HOSTNAME"
        else ""
    )
    ordinal: int | None = None
    if not pod_name:
        problems.append(
            "POD_NAME (or its fallback HOSTNAME) is required and is not set: "
            "the replica's ordinal is the pod name's trailing -<n>, "
            "e.g. millrace-events-3; set POD_NAME from the downward API "
            "(metadata.name)"
        )
    else:
        try:
            ordinal = _parse_ordinal(pod_name)
        except ValueError:
            problems.append(
                f"{pod_var}={pod_name!r} carries no trailing -<n> ordinal "
                f"(expected a StatefulSet pod name, e.g. millrace-events-3)" + fallback
            )
    if ordinal is not None and replica_count is not None and ordinal >= replica_count:
        problems.append(
            f"{pod_var}={pod_name!r} ordinal {ordinal} >= "
            f"MILLRACE_REPLICA_COUNT={replica_count}: the ordinal must name "
            f"one of the replicas (0..{replica_count - 1})" + fallback
        )
        ordinal = None
    return replica_count, ordinal


def _kafka_consumer_overrides(
    env: Mapping[str, str], problems: list[str]
) -> tuple[tuple[str, str], ...]:
    """``MILLRACE_KAFKA_CONSUMER_<KEY>=value`` → ``(<key>, value)`` with
    ``<KEY>`` lowercased and ``_`` → ``.`` (millpond's ``KAFKA_CONSUMER_*``
    rule), sorted. A key millrace owns is a problem naming the knob
    that sets it (or saying there is none on purpose); an empty value
    is a problem (librdkafka would take the empty string literally)."""
    out: list[tuple[str, str]] = []
    for name in sorted(env):
        if not name.startswith(_KAFKA_CONSUMER_PREFIX):
            continue
        key = name[len(_KAFKA_CONSUMER_PREFIX) :].lower().replace("_", ".")
        value = env[name]
        if not key:
            problems.append(
                f"{name} names no librdkafka key (expected e.g. "
                f"{_KAFKA_CONSUMER_PREFIX}SECURITY_PROTOCOL)"
            )
            continue
        if key in _OWNED_KAFKA_KEYS:
            knob = _OWNED_KAFKA_KEYS[key]
            problems.append(
                f"{name} is refused: millrace owns librdkafka {key!r} — "
                + (
                    f"set {knob} instead"
                    if knob is not None
                    else "it is pinned for correctness (offsets move only "
                    "through the loop's explicit commits; there is no "
                    "group membership) and has no knob on purpose"
                )
            )
            continue
        if not value.strip():
            problems.append(f"{name} is empty (librdkafka {key!r} needs a value)")
            continue
        out.append((key, value))
    return tuple(out)


def _parse_stage_url(raw: str, problems: list[str]) -> tuple[str, str] | None:
    """Split ``MILLRACE_STAGE_URL`` into (object-store URL, base path).

    slatedb's ``ObjectStore.resolve`` accepts NO path component — the
    path belongs to the DbBuilder — so the split is config's job:
    ``s3://bucket/millrace`` → (``s3://bucket``, ``millrace``),
    ``file:///var/lib/millrace`` → (``file:///``, ``/var/lib/millrace``),
    ``memory:///`` → (``memory:///``, ``millrace``).
    """
    if raw.startswith("memory://"):
        rest = raw[len("memory://") :].strip("/")
        return "memory:///", rest or _DEFAULT_STAGE_BASE
    if raw.startswith("file://"):
        rest = raw[len("file://") :].rstrip("/")
        if not rest.strip("/"):
            problems.append(
                f"MILLRACE_STAGE_URL {raw!r} names no directory; "
                f"expected e.g. file:///var/lib/millrace"
            )
            return None
        return "file:///", rest
    if raw.startswith("s3://"):
        rest = raw[len("s3://") :]
        bucket, _, prefix = rest.partition("/")
        if not _S3_BUCKET.match(bucket):
            problems.append(
                f"MILLRACE_STAGE_URL {raw!r}: {bucket!r} is not a valid s3 "
                f"bucket name (expected s3://bucket/prefix)"
            )
            return None
        if not prefix.strip("/"):
            problems.append(
                f"MILLRACE_STAGE_URL {raw!r} names no prefix below the "
                f"bucket; the staging root is per-pipeline state, "
                f"e.g. s3://{bucket}/millrace"
            )
            return None
        return f"s3://{bucket}", prefix.strip("/")
    problems.append(
        f"MILLRACE_STAGE_URL {raw!r} must be an s3://bucket/prefix, "
        f"file:///path or memory:/// URL"
    )
    return None


def _split_top_level_commas(raw: str) -> list[str]:
    """millpond's ``_split_top_level_commas`` (config.py): split on
    commas outside parentheses, so ``bucket(team_id, 16)`` and
    ``amount:decimal(10,2)`` stay one entry. Blank entries are dropped
    (trailing commas tolerated). Extended to treat ``<…>`` as brackets
    too, so a DDL-style ``m:map<string,string>`` reaches the type parser
    as ONE entry and gets ONE nested-type refusal rather than two
    half-entries' worth."""
    entries: list[str] = []
    depth = 0
    current: list[str] = []
    for ch in raw:
        if ch in "(<":
            depth += 1
        elif ch in ")>":
            depth = max(0, depth - 1)
        if ch == "," and depth == 0:
            entries.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
    entries.append("".join(current).strip())
    return [e for e in entries if e]


def _parse_catalog_data_path(raw: str, problems: list[str]) -> str | None:
    """``MILLRACE_CATALOG_DATA_PATH``: an ``s3://bucket/prefix`` URL,
    held to the same bucket-name and non-empty-prefix rules as
    ``MILLRACE_STAGE_URL``'s s3 form; passed to the server verbatim
    (a trailing slash included — the catalog's data_path is the lake's
    root, hoglake's own examples carry one)."""
    if not raw.startswith("s3://"):
        problems.append(
            f"MILLRACE_CATALOG_DATA_PATH {raw!r} must be an s3://bucket/prefix "
            f"URL (the catalog's data_path when millrace creates it)"
        )
        return None
    bucket, _, prefix = raw[len("s3://") :].partition("/")
    if not _S3_BUCKET.match(bucket):
        problems.append(
            f"MILLRACE_CATALOG_DATA_PATH {raw!r}: {bucket!r} is not a valid s3 "
            f"bucket name (expected s3://bucket/prefix)"
        )
        return None
    if not prefix.strip("/"):
        problems.append(
            f"MILLRACE_CATALOG_DATA_PATH {raw!r} names no prefix below the "
            f"bucket; a catalog's data path is a lake root, e.g. "
            f"s3://{bucket}/lake/"
        )
        return None
    return raw


def _s3_bucket_url(data_path: str) -> str:
    """``s3://bucket/prefix/`` → ``s3://bucket`` (the shape
    ``_parse_stage_url`` returns as the store URL)."""
    return "s3://" + data_path[len("s3://") :].partition("/")[0]


def _parse_column_type(
    entry: str, type_raw: str, problems: list[str]
) -> tuple[str, tuple[tuple[str, int], ...]] | None:
    """A hoglake type name, with ``decimal(p,s)``'s parameters, as
    ``coltype_to_arrow`` will accept it. Nested containers and
    ``variant`` are refused here: the grammar cannot express a
    container's children, and millrace cannot write a variant column
    (coerce.py) — a column it could never stage is not a column to
    create."""
    type_name = type_raw.lower()
    params: tuple[tuple[str, int], ...] = ()
    if "<" in type_name or ">" in type_name:
        # DDL-style ``map<string,string>`` / ``list<long>``: a container.
        problems.append(
            f"MILLRACE_TABLE_COLUMNS entry {entry!r}: nested type "
            f"{type_name!r} cannot be expressed in this grammar (a container "
            f"needs its children); create the table with pyhoglake and point "
            f"millrace at it"
        )
        return None
    decimal = _DECIMAL_TYPE.match(type_name)
    if decimal is not None:
        type_name = "decimal"
        precision, scale = int(decimal.group(1)), int(decimal.group(2))
        if not 1 <= precision <= _DECIMAL_MAX_PRECISION or not 0 <= scale <= precision:
            problems.append(
                f"MILLRACE_TABLE_COLUMNS entry {entry!r}: decimal(p,s) needs "
                f"1 <= p <= {_DECIMAL_MAX_PRECISION} and 0 <= s <= p, got "
                f"decimal({precision},{scale})"
            )
            return None
        params = (("precision", precision), ("scale", scale))
    elif type_name == "decimal":
        problems.append(
            f"MILLRACE_TABLE_COLUMNS entry {entry!r}: decimal needs its "
            f"precision and scale, e.g. decimal(10,2)"
        )
        return None
    if type_name in NESTED_TYPES:
        problems.append(
            f"MILLRACE_TABLE_COLUMNS entry {entry!r}: nested type {type_name!r} "
            f"cannot be expressed in this grammar (a container needs its "
            f"children); create the table with pyhoglake and point millrace "
            f"at it"
        )
        return None
    if type_name == "variant":
        problems.append(
            f"MILLRACE_TABLE_COLUMNS entry {entry!r}: millrace cannot write a "
            f"variant column (pyarrow has no native Parquet VARIANT writer)"
        )
        return None
    try:
        arrow_type = coltype_to_arrow(type_name, dict(params) or None)
    except UnsupportedTypeError as e:
        problems.append(f"MILLRACE_TABLE_COLUMNS entry {entry!r}: {e}")
        return None
    # A type whose Arrow round trip changes it (uint32 → long: pyhoglake's
    # writer contract) would create a column of ANOTHER type than the
    # one configured, and the agreement check would then warn forever.
    stored = arrow_type_to_coltype(arrow_type)[0]
    if stored != type_name:
        problems.append(
            f"MILLRACE_TABLE_COLUMNS entry {entry!r}: {type_name!r} is stored "
            f"as {stored!r} by the writer; configure {stored}"
        )
        return None
    return type_name, params


def _parse_table_columns(
    raw: str, problems: list[str]
) -> tuple[ColumnSpec, ...] | None:
    """``MILLRACE_TABLE_COLUMNS``: ``name:type,name:type,…`` (module
    docstring). Every problem is recorded; None when any was."""
    entries = _split_top_level_commas(raw)
    if not entries:
        problems.append("MILLRACE_TABLE_COLUMNS is set but contains no entries")
        return None
    out: list[ColumnSpec] = []
    seen: set[str] = set()
    ok = True
    for entry in entries:
        name, sep, type_raw = (part.strip() for part in entry.partition(":"))
        if not sep or not name or not type_raw:
            problems.append(
                f"MILLRACE_TABLE_COLUMNS entry {entry!r} is not name:type "
                f"(e.g. team_id:long)"
            )
            ok = False
            continue
        if not _IDENTIFIER.match(name):
            problems.append(
                f"MILLRACE_TABLE_COLUMNS column {name!r} is not a valid hoglake "
                f"identifier (must match [A-Za-z_][A-Za-z0-9_-]{{0,127}})"
            )
            ok = False
            continue
        if name.startswith(_RESERVED_COLUMN_PREFIX):
            problems.append(
                f"MILLRACE_TABLE_COLUMNS column {name!r} uses the reserved prefix "
                f"{_RESERVED_COLUMN_PREFIX!r} (hoglake's own physical columns)"
            )
            ok = False
            continue
        if name in seen:
            problems.append(f"MILLRACE_TABLE_COLUMNS column {name!r} is duplicated")
            ok = False
            continue
        seen.add(name)
        parsed = _parse_column_type(entry, type_raw, problems)
        if parsed is None:
            ok = False
            continue
        out.append(ColumnSpec(name, parsed[0], parsed[1]))
    return tuple(out) if ok else None


def _parse_partition_by(
    raw: str, problems: list[str]
) -> tuple[PartitionFieldSpec, ...] | None:
    """``MILLRACE_PARTITION_BY``: millpond's ``_parse_hoglake_partition_by``
    (config.py) — the same grammar, so a millpond value works here:

    - ``col``                → identity(col)
    - ``identity(col)``      → identity(col)
    - ``year(col)`` / ``month(col)`` / ``day(col)`` / ``hour(col)``
    - ``bucket(col, N)``     → bucket with a positive integer param

    Everything else is refused HERE, with the vocabulary in the message.
    Column names are held to the hoglake identifier rule (a superset of
    millpond's SAFE_IDENTIFIER). Every problem is recorded; None when
    any was."""

    def bad(entry: str, why: str) -> None:
        problems.append(
            f"MILLRACE_PARTITION_BY entry {entry!r} {why}; supported forms: "
            f"col | transform(col) | bucket(col, N) with transforms "
            f"{', '.join(PARTITION_TRANSFORMS)}"
        )

    entries = _split_top_level_commas(raw)
    if not entries:
        problems.append("MILLRACE_PARTITION_BY is set but contains no entries")
        return None
    fields: list[PartitionFieldSpec] = []
    seen: set[PartitionFieldSpec] = set()
    ok = True
    for entry in entries:
        call = _PARTITION_CALL.match(entry)
        param: int | None
        if call is None:
            if "(" in entry or ")" in entry:
                bad(entry, "is malformed")
                ok = False
                continue
            transform, column, param = "identity", entry, None
        else:
            transform = call.group(1).lower()
            args = (
                [a.strip() for a in call.group(2).split(",")] if call.group(2) else []
            )
            if transform not in PARTITION_TRANSFORMS:
                bad(entry, f"uses unknown transform {transform!r}")
                ok = False
                continue
            if transform == "bucket":
                if len(args) != 2:
                    bad(entry, "must be bucket(col, N)")
                    ok = False
                    continue
                column = args[0]
                try:
                    param = int(args[1])
                except ValueError:
                    bad(entry, f"has non-integer bucket count {args[1]!r}")
                    ok = False
                    continue
                if param <= 0:
                    bad(entry, f"has non-positive bucket count {param}")
                    ok = False
                    continue
            else:
                if len(args) != 1 or not args[0]:
                    bad(entry, f"must be {transform}(col)")
                    ok = False
                    continue
                column, param = args[0], None
        if not _IDENTIFIER.match(column):
            bad(
                entry,
                f"has an invalid column name {column!r} (must match "
                f"[A-Za-z_][A-Za-z0-9_-]{{0,127}})",
            )
            ok = False
            continue
        field = PartitionFieldSpec(column, transform, param)
        if field in seen:
            bad(entry, "is duplicated")
            ok = False
            continue
        seen.add(field)
        fields.append(field)
    return tuple(fields) if ok else None


def _parse_sort_by(raw: str, problems: list[str]) -> tuple[SortFieldSpec, ...] | None:
    """``MILLRACE_SORT_BY``: a superset of millpond's ``_parse_column_list``
    (config.py, behind ``MILLPOND_SORT_BY``). Comma-separated entries,
    each ``col [asc|desc] [nulls_first|nulls_last]`` (keywords
    case-insensitive; ``nulls first`` / ``nulls last`` accepted too),
    defaulting to ``asc nulls_last`` — so a bare millpond value parses
    to exactly the order millpond declares. Whitespace trimmed, empty
    entries dropped, duplicate columns de-duplicated (first wins). None
    for a value with no entries."""
    fields: list[SortFieldSpec] = []
    seen: set[str] = set()
    ok = True

    def bad(entry: str, why: str) -> None:
        nonlocal ok
        problems.append(
            f"MILLRACE_SORT_BY entry {entry!r} {why}; expected "
            f"col [asc|desc] [nulls_first|nulls_last]"
        )
        ok = False

    for entry in (t.strip() for t in raw.split(",")):
        if not entry:
            continue
        tokens = entry.split()
        name = tokens[0]
        if not _IDENTIFIER.match(name):
            problems.append(
                f"MILLRACE_SORT_BY column {name!r} is not a valid hoglake "
                f"identifier (must match [A-Za-z_][A-Za-z0-9_-]{{0,127}})"
            )
            ok = False
            continue
        # ``nulls first`` → ``nulls_first``, so both spellings read alike.
        rest = " ".join(t.lower() for t in tokens[1:]).replace("nulls ", "nulls_")
        modifiers = rest.split()
        direction, null_order = SORT_DIRECTION, SORT_NULL_ORDER
        if modifiers and modifiers[0] in SORT_DIRECTIONS:
            direction = modifiers.pop(0)
        if modifiers and modifiers[0] in SORT_NULL_ORDERS:
            null_order = modifiers.pop(0)
        if modifiers:
            bad(entry, f"has unrecognised modifier(s) {' '.join(modifiers)!r}")
            continue
        if name in seen:
            continue
        seen.add(name)
        fields.append(SortFieldSpec(name, direction, null_order))
    if not ok:
        return None
    return tuple(fields) or None


def _check_fields_against_columns(
    columns: tuple[ColumnSpec, ...],
    partition_by: tuple[PartitionFieldSpec, ...] | None,
    sort_by: tuple[SortFieldSpec, ...] | None,
    problems: list[str],
) -> None:
    """The relation between the three: with ``MILLRACE_TABLE_COLUMNS``
    given, every partition source and sort column names a configured
    column, and each transform is valid for its source's type (the
    check the server's ``set_partition_spec`` would make, made at boot
    so the create cannot 422), and none is a variant: the server refuses
    a variant partition or sort source (``AlterService.requireSourceField``)
    and pyhoglake's ``check_transform`` takes identity of one, so it is
    refused here by type, once, before any transform check."""
    by_name = {c.name: c for c in columns}
    for field in partition_by or ():
        column = by_name.get(field.column)
        if column is None:
            problems.append(
                f"MILLRACE_PARTITION_BY {field.describe()} names column "
                f"{field.column!r}, which is not in MILLRACE_TABLE_COLUMNS "
                f"(columns: {sorted(by_name)})"
            )
            continue
        if column.col_type == "variant":
            problems.append(
                f"MILLRACE_PARTITION_BY {field.describe()}: {field.column!r} is a "
                "variant column, which is no partition source"
            )
            continue
        try:
            check_transform(field.transform, column.col_type, field.param)
        except ValidationError as e:
            problems.append(
                f"MILLRACE_PARTITION_BY {field.describe()} cannot be applied to "
                f"{field.column}:{column.col_type}: {e.message}"
            )
    for sort_field in sort_by or ():
        sort_column = by_name.get(sort_field.column)
        if sort_column is None:
            problems.append(
                f"MILLRACE_SORT_BY column {sort_field.column!r} is not in "
                f"MILLRACE_TABLE_COLUMNS (columns: {sorted(by_name)})"
            )
        elif sort_column.col_type == "variant":
            problems.append(
                f"MILLRACE_SORT_BY column {sort_field.column!r} is a variant "
                "column, which is no sort source"
            )


def load_config(env: Mapping[str, str]) -> Config:
    """Parse and validate the ``MILLRACE_*`` environment. Every problem
    is collected into one :class:`ConfigError`."""
    problems: list[str] = []

    bootstrap = _required(env, "MILLRACE_KAFKA_BOOTSTRAP_SERVERS", problems)
    topic = _required(env, "MILLRACE_KAFKA_TOPIC", problems)
    if topic is not None and not _TOPIC_NAME.match(topic):
        problems.append(
            f"MILLRACE_KAFKA_TOPIC {topic!r} is not a valid Kafka topic "
            f"name (must match [a-zA-Z0-9._-]{{1,249}})"
        )
        topic = None
    group_id = env.get("MILLRACE_KAFKA_GROUP_ID", "").strip() or (
        f"millrace-{topic}" if topic is not None else None
    )
    replica_count, ordinal = _replica_and_ordinal(env, problems)
    kafka_consumer_overrides = _kafka_consumer_overrides(env, problems)
    auto_offset_reset = _enum(
        env,
        "MILLRACE_KAFKA_AUTO_OFFSET_RESET",
        list(AutoOffsetReset),
        AutoOffsetReset.LATEST,
        problems,
    )

    catalog = _required(env, "MILLRACE_CATALOG", problems)
    if catalog is not None and not _CATALOG_NAME.match(catalog):
        problems.append(
            f"MILLRACE_CATALOG {catalog!r} is not a valid hoglake catalog "
            f"name (must match [a-z][a-z0-9_-]{{0,62}})"
        )
        catalog = None
    namespace = _required(env, "MILLRACE_NAMESPACE", problems)
    if namespace is not None and not _IDENTIFIER.match(namespace):
        problems.append(
            f"MILLRACE_NAMESPACE {namespace!r} is not a valid hoglake "
            f"identifier (must match [A-Za-z_][A-Za-z0-9_-]{{0,127}})"
        )
        namespace = None
    table = _required(env, "MILLRACE_TABLE", problems)
    if table is not None and not _IDENTIFIER.match(table):
        problems.append(
            f"MILLRACE_TABLE {table!r} is not a valid hoglake identifier "
            f"(must match [A-Za-z_][A-Za-z0-9_-]{{0,127}})"
        )
        table = None

    stage_raw = _required(env, "MILLRACE_STAGE_URL", problems)
    stage: tuple[str, str] | None = None
    if stage_raw is not None:
        stage = _parse_stage_url(stage_raw, problems)

    catalog_data_path: str | None = None
    if raw := env.get("MILLRACE_CATALOG_DATA_PATH", "").strip():
        catalog_data_path = _parse_catalog_data_path(raw, problems)
    # Staging and the lake must be different buckets: the lake role's
    # prefix-scoped grants would let SlateDB half-write a staging root
    # into the lake bucket before the first refused write.
    if (
        catalog_data_path is not None
        and stage is not None
        and stage[0].startswith("s3://")
        and stage[0] == _s3_bucket_url(catalog_data_path)
    ):
        problems.append(
            f"MILLRACE_STAGE_URL {stage_raw!r} and MILLRACE_CATALOG_DATA_PATH "
            f"{catalog_data_path!r} name the same s3 bucket "
            f"({stage[0][len('s3://') :]!r}): the staging store and the lake "
            f"must be different buckets"
        )
    table_columns: tuple[ColumnSpec, ...] | None = None
    if raw := env.get("MILLRACE_TABLE_COLUMNS", "").strip():
        table_columns = _parse_table_columns(raw, problems)
    partition_by: tuple[PartitionFieldSpec, ...] | None = None
    if raw := env.get("MILLRACE_PARTITION_BY", "").strip():
        partition_by = _parse_partition_by(raw, problems)
    sort_by: tuple[SortFieldSpec, ...] | None = None
    if raw := env.get("MILLRACE_SORT_BY", "").strip():
        sort_by = _parse_sort_by(raw, problems)
    if table_columns is not None:
        _check_fields_against_columns(table_columns, partition_by, sort_by, problems)

    flush_bytes = _positive_int(
        env, "MILLRACE_FLUSH_BYTES", FLUSH_BYTES_DEFAULT, problems
    )
    flush_deadline_s = _positive_int(
        env, "MILLRACE_FLUSH_DEADLINE_S", FLUSH_DEADLINE_S_DEFAULT, problems
    )
    slow_lane_deadline_s = _positive_int(
        env, "MILLRACE_SLOW_LANE_DEADLINE_S", SLOW_LANE_DEADLINE_S_DEFAULT, problems
    )
    if (
        slow_lane_deadline_s is not None
        and flush_deadline_s is not None
        and slow_lane_deadline_s < flush_deadline_s
    ):
        problems.append(
            f"MILLRACE_SLOW_LANE_DEADLINE_S={slow_lane_deadline_s} must be >= "
            f"MILLRACE_FLUSH_DEADLINE_S={flush_deadline_s}"
        )
        slow_lane_deadline_s = None

    port_raw = env.get("MILLRACE_METRICS_PORT", "").strip() or "8000"
    metrics_port: int | None
    try:
        metrics_port = int(port_raw)
    except ValueError:
        problems.append(f"MILLRACE_METRICS_PORT {port_raw!r} is not an integer")
        metrics_port = None
    else:
        if not 0 <= metrics_port <= 65535:
            problems.append(
                f"MILLRACE_METRICS_PORT {metrics_port} is out of range "
                f"(0-65535; 0 = ephemeral)"
            )
            metrics_port = None

    if problems:
        raise ConfigError(problems)

    assert bootstrap is not None and topic is not None and group_id is not None
    assert replica_count is not None and ordinal is not None
    assert isinstance(auto_offset_reset, AutoOffsetReset)
    assert catalog is not None and namespace is not None and table is not None
    assert stage is not None
    assert flush_bytes is not None
    assert flush_deadline_s is not None and slow_lane_deadline_s is not None
    assert metrics_port is not None
    return Config(
        kafka_bootstrap_servers=bootstrap,
        kafka_topic=topic,
        kafka_group_id=group_id,
        replica_count=replica_count,
        ordinal=ordinal,
        kafka_auto_offset_reset=auto_offset_reset,
        kafka_consumer_overrides=kafka_consumer_overrides,
        catalog=catalog,
        namespace=namespace,
        table=table,
        stage_store_url=stage[0],
        stage_base_path=stage[1],
        flush_bytes=flush_bytes,
        flush_deadline_s=flush_deadline_s,
        slow_lane_deadline_s=slow_lane_deadline_s,
        metrics_port=metrics_port,
        catalog_data_path=catalog_data_path,
        table_columns=table_columns,
        partition_by=partition_by,
        sort_by=sort_by,
    )
