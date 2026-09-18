"""Column and schema builders for the full 1.1.0 type system.

The scalar vocabulary here is the server's ColumnDef ``type`` enum
(openapi/hoglake.yaml) minus containers and minus ``variant``; the
caps are the server's, looked up rather than guessed:

- nesting depth: ``MAX_COLUMN_NESTING_DEPTH`` (imported from pyhoglake,
  which mirrors ``Model.kt``'s cap; a top-level column is depth 1);
- nodes per table: :data:`MAX_COLUMN_NODES` (``ColumnTrees.kt``'s
  ``MAX_COLUMN_NODES`` — a column plus every descendant, the synthetic
  list element and map key/value included).
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pyarrow as pa
from pyhoglake.types import MAX_COLUMN_NESTING_DEPTH

#: Server-side cap on nested column NODES per table (a column and every
#: descendant each count as one) — ColumnTrees.kt `MAX_COLUMN_NODES`.
MAX_COLUMN_NODES = 10_000

#: Why ``variant`` is not in the matrix. bench writes parquet through
#: pyhoglake/pyarrow, and pyarrow cannot WRITE native Parquet VARIANT(1)
#: (arrow 25 reads the annotation but drops it on write); the server
#: refuses anything else for a variant column. The only honest producers
#: today are foreign writers (the pyhoglake test fixtures use DuckDB),
#: so generating "variant" data here would mean fabricating files we
#: cannot actually write. Excluded rather than faked.
VARIANT_EXCLUSION_REASON = (
    "variant requires native Parquet VARIANT(1), which pyarrow cannot "
    "write; bench's write path is pyhoglake/pyarrow, so variant data "
    "cannot be honestly produced and is excluded from generation "
    "(variant DDL is still exercised where no data is written)"
)

#: Every server scalar type bench's write path can honestly produce
#: (the ColumnDef enum minus list/struct/map and minus variant).
WRITABLE_SCALARS: tuple[str, ...] = (
    "boolean",
    "int8",
    "int16",
    "int",
    "long",
    "uint8",
    "uint16",
    "uint32",
    "uint64",
    "float",
    "double",
    "decimal",
    "date",
    "time",
    "timestamp_s",
    "timestamp_ms",
    "timestamp",
    "timestamp_ns",
    "timestamptz",
    "string",
    "json",
    "uuid",
    "binary",
)


def _json_type() -> pa.DataType:
    """The arrow type that stamps the parquet JSON annotation (pyarrow
    >= 19); plain string elsewhere — bytes identical, annotation lost."""
    try:
        return pa.json_()
    except AttributeError:  # pragma: no cover - old pyarrow
        return pa.string()


#: Arrow generation type per scalar. These are the WRITER-side types:
#: uint32 stays pa.uint32() here so DDL derives coltype "uint32", and
#: pyhoglake's append path casts it to the contract's int64 physical.
ARROW_BY_SCALAR: dict[str, pa.DataType] = {
    "boolean": pa.bool_(),
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int": pa.int32(),
    "long": pa.int64(),
    "uint8": pa.uint8(),
    "uint16": pa.uint16(),
    "uint32": pa.uint32(),
    "uint64": pa.uint64(),
    "float": pa.float32(),
    "double": pa.float64(),
    "decimal": pa.decimal128(10, 2),
    "date": pa.date32(),
    "time": pa.time64("us"),
    "timestamp_s": pa.timestamp("s"),
    "timestamp_ms": pa.timestamp("ms"),
    "timestamp": pa.timestamp("us"),
    "timestamp_ns": pa.timestamp("ns"),
    "timestamptz": pa.timestamp("us", tz="UTC"),
    "string": pa.string(),
    "json": _json_type(),
    "uuid": pa.binary(16),
    "binary": pa.binary(),
}


@dataclass(frozen=True)
class ColSpec:
    """One generated column: its hoglake type name (the coverage matrix
    key — 'list'/'struct'/'map' for containers), the arrow type to
    generate (children included), and the top-level null behavior."""

    name: str
    coltype: str
    dtype: pa.DataType
    nullable: bool = True
    null_rate: float = 0.0

    def __post_init__(self) -> None:
        if not self.nullable and self.null_rate > 0:
            raise ValueError(f"{self.name}: non-nullable with null_rate > 0")


def node_count(dtype: pa.DataType) -> int:
    """Nodes this type contributes under the server's counting rule: the
    column itself plus every descendant (list element, map key and value,
    each struct field), recursively."""
    if pa.types.is_map(dtype):
        return 1 + node_count(dtype.key_type) + node_count(dtype.item_type)
    if (
        pa.types.is_list(dtype)
        or pa.types.is_large_list(dtype)
        or pa.types.is_fixed_size_list(dtype)
    ):
        return 1 + node_count(dtype.value_type)
    if pa.types.is_struct(dtype):
        return 1 + sum(node_count(dtype.field(i).type) for i in range(dtype.num_fields))
    return 1


def type_depth(dtype: pa.DataType) -> int:
    """Nesting depth under the server's rule (a top-level column is 1)."""
    if pa.types.is_map(dtype):
        return 1 + max(type_depth(dtype.key_type), type_depth(dtype.item_type))
    if (
        pa.types.is_list(dtype)
        or pa.types.is_large_list(dtype)
        or pa.types.is_fixed_size_list(dtype)
    ):
        return 1 + type_depth(dtype.value_type)
    if pa.types.is_struct(dtype):
        return 1 + max(
            (type_depth(dtype.field(i).type) for i in range(dtype.num_fields)),
            default=0,
        )
    return 1


def to_arrow_schema(specs: list[ColSpec] | tuple[ColSpec, ...]) -> pa.Schema:
    return pa.schema([pa.field(s.name, s.dtype, nullable=s.nullable) for s in specs])


# -- fixed showcases ---------------------------------------------------------


def scalar_showcase() -> list[ColSpec]:
    """One column per writable scalar. A few are non-nullable (the
    null_count == 0 path is a path too); the rest carry realistic,
    varied null rates so null_count/bounds interplay gets exercised."""
    non_nullable = {"long", "uuid", "timestamp"}
    rates = {  # varied on purpose; anything absent gets the default
        "string": 0.25,
        "json": 0.40,
        "binary": 0.50,
        "timestamp_ns": 0.10,
        "uint64": 0.05,
        "double": 0.15,
    }
    out = []
    for coltype in WRITABLE_SCALARS:
        nullable = coltype not in non_nullable
        out.append(
            ColSpec(
                name=f"sc_{coltype}",
                coltype=coltype,
                dtype=ARROW_BY_SCALAR[coltype],
                nullable=nullable,
                null_rate=rates.get(coltype, 0.2) if nullable else 0.0,
            )
        )
    return out


def deep_list_chain(depth: int) -> ColSpec:
    """A ``list<list<...<long>>>`` column of exactly ``depth`` nesting
    levels (top-level column counted as 1, per the server's rule)."""
    if depth < 1 or depth > MAX_COLUMN_NESTING_DEPTH:
        raise ValueError(
            f"depth {depth} outside [1, {MAX_COLUMN_NESTING_DEPTH}] — the "
            "server refuses deeper nesting (MAX_COLUMN_NESTING_DEPTH)"
        )
    dtype: pa.DataType = pa.int64()
    for _ in range(depth - 1):
        dtype = pa.list_(dtype)
    coltype = "list" if depth > 1 else "long"
    return ColSpec(name="deep", coltype=coltype, dtype=dtype, null_rate=0.1)


def _wide_struct() -> pa.DataType:
    """A struct with many fields — most scalar kinds plus a nested list
    and an inner struct (struct-of-list, struct-of-struct)."""
    inner = pa.struct(
        [
            pa.field("lat", pa.float64()),
            pa.field("lon", pa.float64()),
            pa.field("accuracy_m", pa.int32()),
        ]
    )
    return pa.struct(
        [
            pa.field("flag", pa.bool_()),
            pa.field("tiny", pa.int8()),
            pa.field("small", pa.int16()),
            pa.field("count", pa.int32()),
            pa.field("big", pa.int64()),
            pa.field("level", pa.uint8()),
            pa.field("port", pa.uint16()),
            pa.field("code", pa.uint32()),
            pa.field("ratio", pa.float32()),
            pa.field("score", pa.float64()),
            pa.field("day", pa.date32()),
            pa.field("at", pa.timestamp("us")),
            pa.field("label", pa.string()),
            pa.field("token", pa.binary(16)),
            pa.field("recent_paths", pa.list_(pa.string())),
            pa.field("geo", inner),
        ]
    )


def nested_showcase(
    max_depth: int = MAX_COLUMN_NESTING_DEPTH,
) -> list[ColSpec]:
    """Deterministic nested exemplars: maps with non-string keys, a
    many-field struct, list-of-struct, struct-of-list, list-of-list, and
    a chain that reaches ``max_depth`` exactly."""
    span = pa.struct(
        [
            pa.field("op", pa.string()),
            pa.field("started", pa.timestamp("ms")),
            pa.field("dur_us", pa.int64()),
            pa.field("ok", pa.bool_()),
        ]
    )
    daily_value = pa.struct(
        [pa.field("count", pa.int64()), pa.field("total", pa.float64())]
    )
    specs = [
        ColSpec("attrs", "map", pa.map_(pa.string(), pa.string()), null_rate=0.2),
        ColSpec("metrics", "map", pa.map_(pa.string(), pa.float64()), null_rate=0.1),
        # non-string map keys
        ColSpec("codes", "map", pa.map_(pa.int32(), pa.string()), null_rate=0.15),
        ColSpec("daily", "map", pa.map_(pa.date32(), daily_value), null_rate=0.1),
        ColSpec("tags", "list", pa.list_(pa.string()), null_rate=0.25),
        ColSpec("samples", "list", pa.list_(pa.list_(pa.float64())), null_rate=0.2),
        ColSpec("spans", "list", pa.list_(span), null_rate=0.15),
        ColSpec("context", "struct", _wide_struct(), null_rate=0.1),
        deep_list_chain(max_depth),
    ]
    over = [s.name for s in specs if type_depth(s.dtype) > max_depth]
    if over:  # pragma: no cover - showcase shapes are depth <= 4 + chain
        raise ValueError(f"showcase columns exceed depth {max_depth}: {over}")
    return specs


def full_coverage_columns(
    max_depth: int = MAX_COLUMN_NESTING_DEPTH,
) -> list[ColSpec]:
    """The whole matrix: every writable scalar plus the nested showcase."""
    return scalar_showcase() + nested_showcase(max_depth)


# -- seeded random schemas ---------------------------------------------------

#: Map keys stay in types that make natural keys; non-string choices are
#: deliberately present so map<non-string, _> paths get exercised.
_KEYABLE: tuple[str, ...] = ("string", "long", "int", "date", "timestamp", "uuid")
_CONTAINER_KINDS: tuple[str, ...] = ("list", "struct", "map")


def _random_type(
    rng: np.random.Generator, depth_left: int, budget: list[int]
) -> pa.DataType:
    """One random type within the remaining depth and node budget.
    ``budget`` is a single-cell mutable node counter."""
    make_container = depth_left > 0 and budget[0] > 3 and rng.random() < 0.35
    if not make_container:
        budget[0] -= 1
        scalar = WRITABLE_SCALARS[int(rng.integers(0, len(WRITABLE_SCALARS)))]
        return ARROW_BY_SCALAR[scalar]
    kind = _CONTAINER_KINDS[int(rng.integers(0, len(_CONTAINER_KINDS)))]
    budget[0] -= 1
    if kind == "list":
        return pa.list_(_random_type(rng, depth_left - 1, budget))
    if kind == "map":
        budget[0] -= 1  # the key node
        key = ARROW_BY_SCALAR[_KEYABLE[int(rng.integers(0, len(_KEYABLE)))]]
        return pa.map_(key, _random_type(rng, depth_left - 1, budget))
    n_fields = int(rng.integers(1, 5))
    fields = []
    for i in range(n_fields):
        if budget[0] <= 0:
            break
        fields.append(pa.field(f"f{i}", _random_type(rng, depth_left - 1, budget)))
    if not fields:  # a struct must have at least one field
        budget[0] -= 1
        fields = [pa.field("f0", pa.int64())]
    return pa.struct(fields)


def random_columns(
    rng: np.random.Generator,
    count: int,
    max_depth: int = 4,
    max_nodes: int = MAX_COLUMN_NODES,
) -> list[ColSpec]:
    """``count`` random columns, a pure function of ``rng``'s state,
    never exceeding ``max_depth`` (server cap 8) or ``max_nodes`` total
    nodes (server cap :data:`MAX_COLUMN_NODES`)."""
    if max_depth < 1 or max_depth > MAX_COLUMN_NESTING_DEPTH:
        raise ValueError(
            f"max_depth {max_depth} outside [1, {MAX_COLUMN_NESTING_DEPTH}]"
        )
    budget = [min(max_nodes, MAX_COLUMN_NODES)]
    specs: list[ColSpec] = []
    for i in range(count):
        if budget[0] <= 0:
            break
        dtype = _random_type(rng, max_depth - 1, budget)
        if pa.types.is_map(dtype):
            coltype = "map"
        elif pa.types.is_struct(dtype):
            coltype = "struct"
        elif pa.types.is_list(dtype):
            coltype = "list"
        else:
            coltype = next(k for k, v in ARROW_BY_SCALAR.items() if v.equals(dtype))
        specs.append(
            ColSpec(
                name=f"c{i}_{coltype}",
                coltype=coltype,
                dtype=dtype,
                null_rate=round(float(rng.uniform(0.0, 0.4)), 3),
            )
        )
    return specs
