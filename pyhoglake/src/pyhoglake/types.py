"""pyarrow <-> hoglake column-type mapping.

Supported mappings (both directions):

    pa.bool_()               <-> boolean
    pa.int8()                <-> int8
    pa.int16()               <-> int16
    pa.int32()               <-> int
    pa.int64()               <-> long
    pa.uint8()               <-> uint8
    pa.uint16()              <-> uint16
    pa.uint32()               -> uint32 -> pa.int64()   (see below)
    pa.uint64()              <-> uint64
    pa.float32()             <-> float
    pa.float64()             <-> double
    pa.string()/large_string <-> string
    pa.json_()               <-> json   (pyarrow >= 19; else pa.string())
    pa.binary()/large_binary <-> binary
    pa.date32()              <-> date
    pa.time64("us")          <-> time
    pa.timestamp("s")        <-> timestamp_s
    pa.timestamp("ms")       <-> timestamp_ms
    pa.timestamp("us")       <-> timestamp
    pa.timestamp("ns")       <-> timestamp_ns
    pa.timestamp("us", tz)   <-> timestamptz
    pa.decimal128(p, s)      <-> decimal  (type_params: {"precision": p, "scale": s})
    pa.uuid()                <-> uuid   (pa.binary(16) also reads as uuid)

Note on uuid: hoglake's ``uuid`` column is parquet
``FIXED_LEN_BYTE_ARRAY(16)`` carrying the ``UUID`` logical annotation
(docs/iceberg-federation.md, and what compaction's ParquetRewriter
writes on every file it rewrites). pyarrow stamps the annotation only for
its canonical uuid extension type, so ``coltype_to_arrow("uuid")``
returns ``pa.uuid()``. The storage is 16 big-endian bytes
(``uuid.UUID(...).bytes``) either way, so ``pa.binary(16)`` is still
accepted on input and still WRITES the same bytes — it just leaves the
annotation off, which is the state every file written before this
contract is in. Measured, the annotation needs pyarrow >= 21: 18 through
20 have ``pa.uuid()`` but write a bare ``FIXED_LEN_BYTE_ARRAY(16)``.
Below the floor (no ``pa.uuid()`` at all) the mapping falls back to
``pa.binary(16)``, losing the annotation and not one byte.

Note on uint32 — the one deliberately asymmetric mapping. ``uint32``
maps to Iceberg ``long``, but pyarrow writes ``pa.uint32()`` as parquet
INT32 + ``Int(32, isSigned=false)``, and an Iceberg reader takes that
annotation's physical INT32 as a SIGNED int32: every value above 2^31
would come back negative through the facade. So hoglake's WRITER
contract for uint32 is parquet INT64, i.e. ``coltype_to_arrow("uint32")``
returns ``pa.int64()``. The read path still accepts both physical forms
— only the writer is pinned — and a ``pa.uint32()`` array casts to
int64 losslessly, so this costs the caller nothing but one hop through
"long" if they round-trip the type name.

Note on timestamp_s: parquet has no seconds unit, so pyarrow writes a
``pa.timestamp("s")`` column as INT64 + ``Timestamp(MILLIS)`` and reads
it back as ``timestamp[ms]``. The catalog type stays ``timestamp_s`` —
declared precision is metadata — but do not expect the file's physical
annotation to say seconds.

Note on json: ``pa.string()``/``pa.large_string()`` always map to
``string``, NEVER to ``json``. Arrow's plain string carries no JSON
validity claim, so inferring json from it would attach a claim the data
never made; a json column must be declared explicitly in DDL (or carried
in ``pa.json_()``, which does make the claim). In the other direction
``coltype_to_arrow("json")`` prefers ``pa.json_()`` because that is what
makes pyarrow stamp the parquet JSON logical annotation; on pyarrow < 19
it falls back to ``pa.string()``, which loses that annotation but not
one byte of the document.

Nested types (phase 2) map structurally, both ways::

    pa.list_(field)          <-> list   (one child, named "element")
    pa.struct([fields])      <-> struct (children keep their names)
    pa.map_(key, value)      <-> map    (children "key" and "value")

The synthetic child names are Iceberg's, and the server enforces them,
so :func:`schema_to_column_defs` emits them regardless of what an Arrow
list's value field or a map's key/item fields happen to be called
locally. A map's key is emitted ``nullable=False``: Iceberg map keys are
non-nullable, arrow's map keys always are, and the server refuses a
nullable one.

Field ids ride ``PARQUET:field_id`` on EVERY level, containers included
— pyarrow stamps them into the parquet SchemaElements at any depth
(verified against pyarrow 25), which is what lets the catalog bind a
struct field or a list element by id rather than by position. The
synthetic repetition groups parquet inserts (``list``, ``key_value``)
get no id, because Iceberg has nothing to match one against.

Nesting depth is capped at :data:`MAX_COLUMN_NESTING_DEPTH`, mirroring
the server's cap so a client fails locally instead of paying a round
trip to be refused.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa

from .errors import UnsupportedTypeError
from .models import Column

PARQUET_FIELD_ID_KEY = b"PARQUET:field_id"

#: Maximum column nesting depth, top-level counting as 1. Mirrors the
#: server's MAX_COLUMN_NESTING_DEPTH — a deeper schema is refused here so
#: the caller learns before the upload, not after the 422.
MAX_COLUMN_NESTING_DEPTH = 8

#: The container column types. They have children and no values.
NESTED_TYPES = frozenset({"list", "struct", "map"})

#: Iceberg's synthetic child names, by container type and position.
SYNTHETIC_CHILD_NAMES = {"list": ("element",), "map": ("key", "value")}

_SUPPORTED = (
    "bool, int8, int16, int32, int64, uint8, uint16, uint32, uint64, "
    "float32, float64, string, large_string, json, binary, large_binary, "
    "uuid (extension), fixed_size_binary(16) [uuid], date32, time64(us), "
    "timestamp(s|ms|us|ns), timestamp(us, tz), decimal128, "
    "list, struct, map"
)


def _is_uuid_extension(t: pa.DataType) -> bool:
    try:
        return t.equals(pa.uuid())  # pyarrow >= 18
    except AttributeError:  # pragma: no cover - old pyarrow
        return False


def _is_json_extension(t: pa.DataType) -> bool:
    try:
        return t.equals(pa.json_())  # pyarrow >= 19
    except AttributeError:  # pragma: no cover - old pyarrow
        return False


def arrow_type_to_coltype(t: pa.DataType) -> tuple[str, dict[str, Any] | None]:
    """Map an Arrow type to (hoglake column type, type_params)."""
    if pa.types.is_boolean(t):
        return "boolean", None
    if pa.types.is_int8(t):
        return "int8", None
    if pa.types.is_int16(t):
        return "int16", None
    if pa.types.is_int32(t):
        return "int", None
    if pa.types.is_int64(t):
        return "long", None
    if pa.types.is_uint8(t):
        return "uint8", None
    if pa.types.is_uint16(t):
        return "uint16", None
    if pa.types.is_uint32(t):
        return "uint32", None
    if pa.types.is_uint64(t):
        return "uint64", None
    if pa.types.is_float32(t):
        return "float", None
    if pa.types.is_float64(t):
        return "double", None
    # Extension checks come before the plain-string check they shadow:
    # json is an extension over utf8, and only the extension makes the
    # JSON validity claim. A bare string stays "string" (module docstring).
    if _is_json_extension(t):
        return "json", None
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "string", None
    if _is_uuid_extension(t):
        return "uuid", None
    if pa.types.is_fixed_size_binary(t):
        if t.byte_width == 16:
            return "uuid", None
        raise UnsupportedTypeError(
            f"unsupported Arrow type {t!r}: only fixed_size_binary(16) (uuid) "
            f"is supported; supported types: {_SUPPORTED}"
        )
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return "binary", None
    if pa.types.is_date32(t):
        return "date", None
    if pa.types.is_time64(t):
        if t.unit == "us":
            return "time", None
        raise UnsupportedTypeError(
            f"unsupported Arrow type {t!r}: time must be time64('us'); "
            f"supported types: {_SUPPORTED}"
        )
    if pa.types.is_timestamp(t):
        if t.tz:
            # timestamptz is micros-only and stays that way: hoglake has
            # no timestamptz_s/_ms/_ns to carry another unit, so mapping
            # one here would silently reinterpret the values as micros.
            if t.unit != "us":
                raise UnsupportedTypeError(
                    f"unsupported Arrow type {t!r}: a tz-aware timestamp must "
                    f"have microsecond unit (timestamptz is micros-only; the "
                    f"other units exist only tz-naive, as timestamp_s/_ms/_ns); "
                    f"supported types: {_SUPPORTED}"
                )
            return "timestamptz", None
        by_unit = {
            "s": "timestamp_s",
            "ms": "timestamp_ms",
            "us": "timestamp",
            "ns": "timestamp_ns",
        }
        if t.unit not in by_unit:  # pragma: no cover - arrow has no other unit
            raise UnsupportedTypeError(
                f"unsupported Arrow type {t!r}: unknown timestamp unit; "
                f"supported types: {_SUPPORTED}"
            )
        return by_unit[t.unit], None
    if pa.types.is_decimal128(t):
        return "decimal", {"precision": t.precision, "scale": t.scale}
    # Containers: the TYPE NAME is all this function returns — the
    # children's names and nullability need the Arrow FIELDS, which this
    # signature does not have (schema_to_column_defs does). The children's
    # TYPES are still validated here, recursively: without that,
    # `list<float16>` would answer "list" and the rejection would move to
    # whichever caller happened to recurse, which is exactly the silent
    # wrong-mapping this function exists to prevent.
    # is_map FIRST. Arrow models a map as a list of key/value structs,
    # and while pyarrow 25's `is_list` answers False for one, the
    # ordering is what makes that an implementation detail rather than
    # something this dispatch depends on. (An earlier comment here
    # asserted `is_list` said yes; measured against pyarrow 25.0.1 it
    # does not. Order it correctly and the question stops mattering.)
    if pa.types.is_map(t):
        arrow_type_to_coltype(t.key_type)
        arrow_type_to_coltype(t.item_type)
        return "map", None
    if is_list_family(t):
        arrow_type_to_coltype(t.value_type)
        return "list", None
    if pa.types.is_struct(t):
        if t.num_fields == 0:
            raise UnsupportedTypeError(
                f"unsupported Arrow type {t!r}: a struct must have at least one "
                "field (an empty struct has no representation in parquet or "
                f"Iceberg); supported types: {_SUPPORTED}"
            )
        for i in range(t.num_fields):
            arrow_type_to_coltype(t.field(i).type)
        return "struct", None
    raise UnsupportedTypeError(
        f"unsupported Arrow type {t!r}; supported types: {_SUPPORTED}"
    )


def coltype_to_arrow(
    type_: str, type_params: dict[str, Any] | None = None
) -> pa.DataType:
    """Map a hoglake column type back to an Arrow type."""
    if type_ == "boolean":
        return pa.bool_()
    if type_ == "int8":
        return pa.int8()
    if type_ == "int16":
        return pa.int16()
    if type_ == "int":
        return pa.int32()
    if type_ == "long":
        return pa.int64()
    if type_ == "uint8":
        return pa.uint8()
    if type_ == "uint16":
        return pa.uint16()
    if type_ == "uint32":
        # NOT pa.uint32(): pyarrow writes that as parquet INT32 +
        # Int(32, unsigned), which an Iceberg reader takes as a SIGNED
        # int32, so everything above 2^31 reads back negative through
        # the facade. uint32 maps to Iceberg long, so the writer
        # contract is parquet INT64. A pa.uint32() array casts to int64
        # losslessly, so this costs the caller nothing.
        return pa.int64()
    if type_ == "uint64":
        return pa.uint64()
    if type_ == "float":
        return pa.float32()
    if type_ == "double":
        return pa.float64()
    if type_ == "string":
        return pa.string()
    if type_ == "json":
        # pa.json_() is what makes pyarrow stamp the parquet JSON logical
        # annotation. The pyarrow < 19 fallback loses that annotation,
        # not the bytes.
        try:
            return pa.json_()
        except AttributeError:  # pragma: no cover - old pyarrow
            return pa.string()
    if type_ == "binary":
        return pa.binary()
    if type_ == "date":
        return pa.date32()
    if type_ == "time":
        return pa.time64("us")
    if type_ == "timestamp_s":
        return pa.timestamp("s")
    if type_ == "timestamp_ms":
        return pa.timestamp("ms")
    if type_ == "timestamp":
        return pa.timestamp("us")
    if type_ == "timestamp_ns":
        return pa.timestamp("ns")
    if type_ == "timestamptz":
        return pa.timestamp("us", tz="UTC")
    if type_ == "uuid":
        # pa.uuid() is what makes pyarrow stamp the parquet UUID logical
        # annotation (pyarrow >= 21; see the module docstring). The
        # fallback loses that annotation, not the bytes — and a reader
        # accepts both forms either way.
        #
        # It is NOT the whole guard: on 18 through 20 pa.uuid() exists
        # and this branch is never taken, yet the file still comes out
        # unannotated. Only the resolver floor (pyarrow>=21 in
        # pyproject.toml) enforces the wire contract; this arm just keeps
        # an environment built below the floor writing correct bytes
        # instead of raising.
        try:
            return pa.uuid()
        except AttributeError:
            return pa.binary(16)
    if type_ == "decimal":
        params = type_params or {}
        try:
            return pa.decimal128(int(params["precision"]), int(params["scale"]))
        except KeyError as e:
            raise UnsupportedTypeError(
                f"decimal column missing type_params key {e}"
            ) from None
    if type_ in NESTED_TYPES:
        # A container's Arrow type is not determined by its name alone —
        # it needs the children, and the children need their field ids.
        # column_to_arrow_field is the recursive entry point that has
        # both; this scalar-only signature cannot, so it refuses rather
        # than inventing an element type.
        raise UnsupportedTypeError(
            f"column type {type_!r} is a nested container: its Arrow type depends "
            "on its children, so build it with columns_to_arrow_schema() "
            "(or column_to_arrow_field()), which carry them"
        )
    raise UnsupportedTypeError(f"unknown hoglake column type {type_!r}")


def _column_def_depth(defs: list[dict[str, Any]]) -> int:
    return max(
        (1 + _column_def_depth(d.get("children") or []) for d in defs),
        default=0,
    )


def _field_to_column_def(f: pa.Field) -> dict[str, Any]:
    """One Arrow field as a CreateTableRequest column def, recursively."""
    type_, params = arrow_type_to_coltype(f.type)
    col: dict[str, Any] = {"name": f.name, "type": type_, "nullable": f.nullable}
    if params:
        col["type_params"] = params
    if type_ == "struct":
        col["children"] = [
            _field_to_column_def(f.type.field(i)) for i in range(f.type.num_fields)
        ]
    elif type_ == "list":
        # The element's own nullability is carried; its NAME is not.
        # Arrow calls it "item" by default and lets a writer call it
        # anything, but the catalog's name for it is Iceberg's:
        # "element". Passing the local name through would make the DDL
        # depend on which library built the array.
        value = f.type.value_field
        child = _field_to_column_def(value)
        child["name"] = "element"
        col["children"] = [child]
    elif type_ == "map":
        key = _field_to_column_def(f.type.key_field)
        key["name"] = "key"
        # `nullable` is NOT overridden here: arrow refuses to construct a
        # map with a nullable key at all ("Map key field should be
        # non-nullable" — pinned by a test), and Iceberg requires the
        # same, so the value already read off the field is False. Writing
        # it again would be a guard no test could ever fail.
        value = _field_to_column_def(f.type.item_field)
        value["name"] = "value"
        col["children"] = [key, value]
    return col


def is_list_family(t: pa.DataType) -> bool:
    """Whether ``t`` is one of the Arrow types that map to catalog ``list``.

    THE canonical answer, in one place, because the mapping is
    many-to-one and every consumer has to agree with it. ``list``,
    ``large_list`` and ``fixed_size_list`` all become ``list`` in
    :func:`_field_to_column_def`, and a validator that recognised only
    the canonical member skipped the other two: the recursive
    nested-name check in ``_align_table`` walked into a ``list`` and
    silently returned "no mismatch" for a ``large_list``, so a typo'd
    inner struct field reached the very ``cast`` that check exists to
    prevent and appended as an all-NULL column.

    Callers must dispatch on ``is_map`` FIRST regardless: a map is
    modelled as a list of key/value structs, and whether a given pyarrow
    release reports one as a list is not a thing this code should depend
    on either way.
    """
    return (
        pa.types.is_list(t)
        or pa.types.is_large_list(t)
        or pa.types.is_fixed_size_list(t)
    )


def _uuid_storage_field(f: pa.Field, depth: int) -> pa.Field:
    return f.with_type(uuid_storage_form(f.type, depth))


def uuid_storage_form(t: pa.DataType, _depth: int = 1) -> pa.DataType:
    """``t`` with every ``pa.uuid()`` replaced by its 16-byte storage.

    THE canonical way to compare a file's Arrow type against the
    catalog's for a ``uuid`` column, because the catalog type has two
    legal spellings on the wire: ``pa.uuid()`` (annotated) and
    ``pa.binary(16)`` (bare). They differ in the parquet annotation only,
    never in the bytes, so a comparison that discriminates between them
    refuses files it must accept — which is exactly what
    ``prepare_append_files`` did to any writer that annotated.

    Only the uuid extension is unwrapped. ``pa.json_()`` stays an
    extension: a bare utf8 column makes no JSON validity claim (see the
    module docstring), so equating the two would be a different, wrong
    licence. Every other property a container carries — a map's
    ``keys_sorted``, a fixed-size list's width, list-vs-large_list — is
    carried through unchanged, because loosening one of THOSE would
    accept a file whose shape genuinely differs.

    A type holding no uuid extension is returned UNCHANGED — the same
    object, never a rebuilt one — so every comparison that passed before
    compares identically now.

    Recursion stops at :data:`MAX_COLUMN_NESTING_DEPTH`, the depth the
    catalog itself caps columns at, because this walks types read out of
    a caller-written footer. Past the cap the type is returned as-is:
    both sides of a comparison stop at the same depth, so a legal schema
    is unaffected, and a file nested deeper than any legal destination
    column is compared un-normalized — i.e. refused, which is the safe
    direction.
    """
    if _is_uuid_extension(t):
        return pa.binary(16)
    if _depth >= MAX_COLUMN_NESTING_DEPTH:
        return t
    deeper = _depth + 1
    if pa.types.is_struct(t):
        fields = [_uuid_storage_field(t.field(i), deeper) for i in range(t.num_fields)]
        if all(f.type == t.field(i).type for i, f in enumerate(fields)):
            return t
        return pa.struct(fields)
    # is_map before the list family, for the reason is_list_family states.
    if pa.types.is_map(t):
        key = _uuid_storage_field(t.key_field, deeper)
        item = _uuid_storage_field(t.item_field, deeper)
        if key.type == t.key_type and item.type == t.item_type:
            return t
        # keys_sorted rides along: it is part of the type, nothing to do
        # with the uuid spelling, and dropping it made a sorted map equal
        # an unsorted one for uuid-bearing maps only.
        return pa.map_(key, item, keys_sorted=t.keys_sorted)
    # list/large_list/fixed_size_list only. The *_view spellings are not
    # in hoglake's type set (arrow_type_to_coltype refuses them), so a
    # schema built from catalog columns can never hold one and a file
    # that does is refused before any of this matters.
    if is_list_family(t):
        value = _uuid_storage_field(t.value_field, deeper)
        if value.type == t.value_type:
            return t
        if pa.types.is_large_list(t):
            return pa.large_list(value)
        if pa.types.is_fixed_size_list(t):
            return pa.list_(value, t.list_size)
        return pa.list_(value)
    return t


def uuid_storage_form_schema(schema: pa.Schema) -> pa.Schema:
    """:func:`uuid_storage_form` over a whole schema, field metadata
    (the ``PARQUET:field_id`` chain), nullability and the schema's own
    metadata preserved, so the normalized schemas can be compared with
    ``check_metadata=True``."""
    fields = [_uuid_storage_field(f, 1) for f in schema]
    if all(f.type == schema.field(i).type for i, f in enumerate(fields)):
        return schema
    return pa.schema(fields, metadata=schema.metadata)


def schema_to_column_defs(schema: pa.Schema) -> list[dict[str, Any]]:
    """Convert a pyarrow schema into CreateTableRequest column defs.

    Recursive: a list/struct/map field produces ``children`` with
    Iceberg's synthetic names. Depth past
    :data:`MAX_COLUMN_NESTING_DEPTH` is refused locally, with the same
    cap the server applies.
    """
    out = [_field_to_column_def(f) for f in schema]
    depth = _column_def_depth(out)
    if depth > MAX_COLUMN_NESTING_DEPTH:
        raise UnsupportedTypeError(
            f"column nesting depth {depth} exceeds the maximum "
            f"{MAX_COLUMN_NESTING_DEPTH} (a top-level column is depth 1)"
        )
    return out


def _field_id_metadata(field_id: int) -> dict[bytes, bytes]:
    return {PARQUET_FIELD_ID_KEY: str(field_id).encode("ascii")}


def column_to_arrow_field(c: Column, _depth: int = 1) -> pa.Field:
    """One catalog column as an Arrow field, field ids on every level.

    Containers recurse. The synthetic repetition groups parquet inserts
    (``list``, ``key_value``) are arrow's own business and carry no field
    id — Iceberg has nothing to match one against.
    """
    if _depth > MAX_COLUMN_NESTING_DEPTH:
        raise UnsupportedTypeError(
            f"column {c.name!r} nests deeper than the maximum "
            f"{MAX_COLUMN_NESTING_DEPTH} (a top-level column is depth 1)"
        )
    if c.type not in NESTED_TYPES:
        return pa.field(
            c.name,
            coltype_to_arrow(c.type, c.type_params),
            nullable=c.nullable,
            metadata=_field_id_metadata(c.field_id),
        )

    children = tuple(sorted(c.children or (), key=lambda k: k.ordinal))
    expected = SYNTHETIC_CHILD_NAMES.get(c.type)
    if expected is not None and len(children) != len(expected):
        raise UnsupportedTypeError(
            f"{c.type} column {c.name!r} must have exactly {len(expected)} "
            f"child(ren) {list(expected)}; got {len(children)}"
        )
    if c.type == "struct":
        if not children:
            raise UnsupportedTypeError(
                f"struct column {c.name!r} must have at least one child field"
            )
        inner: pa.DataType = pa.struct(
            [column_to_arrow_field(k, _depth + 1) for k in children]
        )
    elif c.type == "list":
        inner = pa.list_(column_to_arrow_field(children[0], _depth + 1))
    else:  # map
        key = column_to_arrow_field(children[0], _depth + 1)
        # Arrow refuses a nullable map key outright; so does the server.
        inner = pa.map_(
            key.with_nullable(False),
            column_to_arrow_field(children[1], _depth + 1),
        )
    return pa.field(
        c.name,
        inner,
        nullable=c.nullable,
        metadata=_field_id_metadata(c.field_id),
    )


def columns_to_arrow_schema(columns: list[Column] | tuple[Column, ...]) -> pa.Schema:
    """Build the target Arrow schema for a table's columns, with parquet
    field ids embedded as ``PARQUET:field_id`` field metadata (pyarrow
    writes these into the parquet SchemaElement field_id slots, at every
    nesting level)."""
    return pa.schema(
        [column_to_arrow_field(c) for c in sorted(columns, key=lambda c: c.ordinal)]
    )
