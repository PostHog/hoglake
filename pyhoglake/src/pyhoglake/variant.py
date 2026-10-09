"""VARIANT columns: declaring a shredded layout, and encoding and reading values.

A top-level ``variant`` column may declare in ``type_params.shredding``
the layout that writers which shred VARIANT values give it in their
Parquet files: which object fields and array elements get Parquet
columns of their own, and the Variant type of each. Readers never need
it, because each file's footer records its own layout, so a declaration
steers only the writers that honour it. It is fixed when the column is
defined; no alter op changes it.

A declaration is a tree of JSON objects, each with a ``type``::

    {"type": "object", "fields": [
        {"name": "$browser", "type": "string"},
        {"name": "price", "type": "decimal8", "precision": 18, "scale": 2},
        {"name": "tags", "type": "array", "element": {"type": "string"}},
        {"name": "payload", "type": "variant"}]}

The grammar is the server's (``server/.../service/VariantShredding.kt``,
which is the Trino connector's), and the OpenAPI ``ColumnDef.type_params``
states it. :func:`validate_shredding` mirrors it locally, so a
declaration the server would answer with a 422 fails before the round
trip, with the server's own message. The server stays authoritative:
whether two field names differ only by case turns on the JDK's
lowercase mapping, which Python reproduces except around GREEK CAPITAL
SIGMA and for case pairs newer than one side's Unicode data, and there
the client accepts and leaves the decision to the server (see
:func:`validate_shredding`).

Two DDL entry points take a declaration:

- :func:`variant_field` builds the Arrow field for a schema handed to
  ``Namespace.create_table`` (or to
  :func:`pyhoglake.types.schema_to_column_defs`): a ``pa.json_()`` field
  whose metadata carries :data:`VARIANT_FIELD_KEY`. JSON text is the
  type, so the schema that declares the column also describes the data a
  caller holds for it.
- ``ops.add_column(name, "variant", shredding=...)``.

It also holds the codec, at the Arrow level: :func:`encode_json` and
:func:`encode_python` turn JSON text or Python objects into the storage of
a VARIANT column, shredded as a declaration lays it out, and
:func:`to_python` and :func:`verify` read any conformant storage back. The
Arrow write paths do not call it yet (``Table.append`` and
``prepare_append_tables`` still refuse a VARIANT column, and
``Table.prepare_append_files`` publishes files another writer produced).
The storage is the write form, field for field the Trino connector's
``VariantShreddingSchema.toParquetType`` layout once the writer puts the
decimal32/decimal64 leaves in the file as INT32/INT64 with DECIMAL(p, s)
and adds the VARIANT annotation, which Arrow cannot carry, to the group.
The policies (D1-D19) are in the README.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from dataclasses import field as _field
from typing import Any, Final, Literal, TypeAlias, TypeVar
from unicodedata import unidata_version

import pyarrow as pa

from . import _variant_codec as _codec
from .errors import UnsupportedShreddingError, ValidationError
from .models import Column

#: One node of the ``type_params.shredding`` grammar, as JSON decodes it.
Shredding: TypeAlias = Mapping[str, Any]

#: The Arrow field-metadata key that marks a ``pa.json_()`` field as a
#: ``variant`` column for DDL. Its value is the canonical JSON of
#: ``{"shredding": <declaration>}``, or ``{}`` when the column declares
#: nothing. Only the DDL path reads it; the Arrow write paths ignore it.
VARIANT_FIELD_KEY: Final = b"pyhoglake:variant"

#: The ``type_params`` key of a declaration.
SHREDDING_KEY: Final = "shredding"

# The server's limits (VariantShredding.kt). test_variant_ddl.py parses
# them out of that file rather than restating them, so a limit that moves
# on the server fails the suite here.

#: How deeply objects and arrays may nest, the root being depth 0.
MAX_SHREDDING_DEPTH: Final = 16

#: How many object fields and arrays a declaration may have, counted
#: together over the whole tree: each gives every data file Parquet
#: columns of its own.
MAX_SHREDDING_FIELDS: Final = 1000

#: The UTF-8 length of the field names on the way to any field: the
#: footer repeats them once for each column below them.
MAX_PATH_NAME_BYTES: Final = 1024

#: The Variant primitive types a node may name, besides the decimals.
PRIMITIVE_TYPES: Final = frozenset(
    {
        "boolean", "int8", "int16", "int32", "int64", "float", "double",
        "date", "time", "timestamp", "timestamp_ns", "timestamptz",
        "timestamptz_ns", "binary", "string", "uuid",
    }
)  # fmt: skip

#: The decimal types, with the largest precision each holds.
DECIMAL_TYPES: Final = {"decimal4": 9, "decimal8": 18, "decimal16": 38}

#: The key an object field has besides those of its type.
_FIELD_KEYS: Final = frozenset({"name"})

#: Identifiers.cap: a value quoted in a refusal is at most this many
#: UTF-16 code units, the last three of them "...".
_CAP_UNITS: Final = 64

#: The precision and scale bound a JSON integer has to fit to count as
#: one: the server reads them into a Kotlin Int, and a Long that would
#: narrow to 18 is not 18.
_INT_MIN: Final = -(2**31)
_INT_MAX: Final = 2**31 - 1

#: The Unicode version of the server's case mappings: Kotlin's
#: String.lowercase() on the JDK it runs (25, whose character data is
#: Unicode 16.0). test_variant_ddl.py reads the JDK out of the server's
#: build and image, so a server moved to an older one, or this pinned
#: below the toolchain's, fails there.
_SERVER_UNICODE: Final = (16, 0)

#: This Python's, which str.lower() reads.
_LOCAL_UNICODE: Final = tuple(int(part) for part in unidata_version.split(".")[:2])

#: The one character whose lowercase the JDK and Python choose by context
#: (validate_shredding).
_CAPITAL_SIGMA: Final = "\u03a3"


# -- declarations -------------------------------------------------------------


def validate_shredding(
    shredding: Shredding | None, *, column: str | None = None
) -> None:
    """Refuse a declaration the server would refuse, with its message.

    ``None`` declares nothing and is accepted, as ``"shredding": null``
    is on the wire. Anything else is walked as the server walks it, in
    the same order, so the first fault reported is the one the server
    would report: :class:`~pyhoglake.ValidationError` naming the path of
    the node at fault (``$.price``, ``$.tags[*]``) and, when ``column``
    is given, the column, word for word as the server's 422 detail.

    One refusal is Python's own: a name holding a surrogate code point is
    refused even when the next one would pair with it. In a ``str`` a
    surrogate is never half of a pair (``json.loads`` already joins an
    escaped pair into one character), and the request encoder could not
    send it.

    And one rule is checked only where Python decides it as the server
    does: two names that differ only by case. The server lowercases with
    the JDK, which ``str.lower()`` matches except in two places, and in
    both the client leaves the pair to the server rather than refuse a
    declaration the server would accept:

    - GREEK CAPITAL SIGMA lowercases to final ``ς`` or medial ``σ`` by
      context. The JDK decides the context by word boundaries (``A_Σ`` is
      one word, so ``a_ς``), Python by Unicode's Final_Sigma rule
      (``a_σ``). A name with a ``Σ`` is left out of the case check, in
      either place of a pair; names without one are still checked
      against each other.
    - A case pair newer than one side's Unicode data. When this Python's
      data is newer than the server's, a pair of names that are not both
      ASCII is the server's; when it is older, Python only misses a pair
      the server knows.

    So for such names a declaration the server refuses may pass here, or
    fail here on a later fault than the server's first; the 422 says why.
    """
    if shredding is None:
        return
    _Walk(column).visit(shredding, "$", 0, 0, frozenset())


def json_unshreddable_paths(shredding: Shredding | None) -> list[tuple[str, str]]:
    """``(path, reason)`` for the declared leaves JSON text fills rarely or never.

    A declaration is immutable, so a leaf that data written from JSON
    can never reach is a column that stays empty for the life of the
    table. The report follows the rules by which pyhoglake's own writer
    will encode JSON text (from 1.4): an integer within the int64 range
    is the smallest integer type that holds it, a larger one decimal16
    with scale 0, any other number a double, a string a string. A value
    shreds only into a leaf of its own type class, which is the Trino
    connector's rule: an integer into any integer leaf that holds it, a
    decimal into any decimal leaf of the same scale whose precision holds
    it, and nothing across classes (an integer into a double, or into a
    decimal). So an integer within int64 never fills a double or a
    decimal leaf, and a larger one, which is a decimal with scale 0,
    fills only decimal16 with scale 0 and a precision of at least 19
    (decimal4 and decimal8 hold too few digits). JSON then never fills a
    temporal, binary, uuid, float, decimal4, decimal8 or fractional
    decimal16 leaf.
    Two leaves are reachable but narrow, and are reported too: ``double``
    (literals with a fraction or an exponent only) and ``decimal16`` with
    scale 0 (integers beyond the int64 range only). Other writers type
    JSON their own way (DuckDB, for one, types a non-negative integer as
    unsigned), so the report speaks for pyhoglake's writer only.

    The declaration is validated first. Paths are spelled as
    :class:`VariantReport` spells them, uncapped (a field ``a.b`` is
    ``$["a.b"]``, apart from a field ``b`` inside ``a``, ``$.a.b``), so a
    leaf reported here is the same key as its counts there. The server's
    refusals join names with dots, as :func:`validate_shredding`'s do.
    """
    validate_shredding(shredding)
    out: list[tuple[str, str]] = []
    if shredding is not None:
        _collect_unshreddable(shredding, "$", out)
    return out


def variant_field(
    name: str, shredding: Shredding | None = None, *, nullable: bool = True
) -> pa.Field:
    """A ``variant`` column for ``create_table``, declared with ``shredding``.

    The field is ``pa.json_()`` carrying :data:`VARIANT_FIELD_KEY`, which
    the DDL path turns into ``{"type": "variant"}`` with
    ``type_params.shredding`` when ``shredding`` is given (and no
    ``type_params`` when it is not). The declaration is validated here
    and copied, so changing the caller's dict afterwards changes nothing.

    Only a top-level column may declare a layout. A ``variant_field``
    without a declaration may also stand inside a struct, list or map; one
    with a declaration there is refused when the schema is turned into
    column definitions, as the server would refuse it.
    """
    validate_shredding(shredding, column=name)
    marker: dict[str, Any] = (
        {} if shredding is None else {SHREDDING_KEY: _plain(shredding)}
    )
    return pa.field(
        name,
        pa.json_(),
        nullable=nullable,
        metadata={VARIANT_FIELD_KEY: _canonical_json(marker).encode("ascii")},
    )


# -- values -------------------------------------------------------------------

#: A Variant null as a whole row, where None means SQL NULL. Inside an
#: object or an array None is a Variant null, and :func:`to_python` returns
#: None there; :func:`encode_python` takes either.
VARIANT_NULL: Final = _codec.VARIANT_NULL

#: How deeply a value may nest, the root being depth 0: a value at depth d
#: holds its fields and elements at depth d + 1. A deeper value makes the
#: row invalid (``nesting_too_deep``).
MAX_VARIANT_DEPTH: Final = _codec.MAX_VARIANT_DEPTH

_K = TypeVar("_K")

#: Why a row is invalid: :attr:`VariantEncodingError.reason` and the keys of
#: :attr:`VariantReport.invalid_by_reason`. The last three are the reader's,
#: from :func:`to_python` and :func:`verify`; the encoders raise
#: ``invalid_shredding`` only when their own output fails its structure
#: check, which is a pyhoglake bug. The reader raises ``nesting_too_deep``
#: too, for a value deeper than :data:`MAX_VARIANT_DEPTH`.
InvalidReason: TypeAlias = _codec.Reason


@dataclass
class VariantReport:
    """What one or more encodes did, for metrics. Meaningful only after a
    call that returned: one that raises may have counted part of its input.

    Counts are of rows, except under an array's element path
    (``$.tags[*]``), where they are of elements. A path names each declared
    field ``.name``, or ``["name"]`` (JSON-quoted) when the name holds a
    dot, a bracket, a quote or a backslash, or is empty, so that a field
    ``a.b`` and a field ``b`` inside ``a`` are ``$["a.b"]`` and ``$.a.b``;
    a name with a control character, a surrogate, or U+2028 or U+2029 is
    quoted with ``\\u`` escapes (``$["\\ud800"]``), so a path always fits
    a UTF-8 log line, the same on every Python.
    Error paths and :func:`json_unshreddable_paths` spell keys the same
    way. A declared path is never long, but a row's is as long as its keys:
    the path in ``first_invalid``, as in an error, is cut to its first
    1024 characters, ``...`` marking the cut. ``typed``, ``fallback`` and
    ``variant_null`` are keyed by ``(column, declared path)`` for every
    declared node with a type (not ``variant``), and say how well a
    declaration fits the data, which matters because it is immutable:

    - ``typed``: values that went to ``typed_value``; an object counts
      there with or without a residual;
    - ``fallback``: values kept in ``value`` because their type did not
      match (a string under an ``int64``, an int under a ``double``);
    - ``variant_null``: present JSON nulls, which are ``value`` = ``00``.
      They are counted apart so that a key that is often null does not
      read as a mismatch.

    A missing field is in none of them. ``column`` is the Column's name, or
    None when the encode was given a declaration rather than a column.
    """

    rows: int = 0
    #: SQL NULL rows of the input.
    sql_null_rows: int = 0
    #: Rows written as SQL NULL because they could not be encoded
    #: (``on_invalid="null"``); not in sql_null_rows.
    invalid_rows: int = 0
    invalid_by_reason: dict[str, int] = _field(default_factory=dict)
    #: ``(column, row, reason, path)`` of the first invalid row.
    first_invalid: tuple[str | None, int, str, str | None] | None = None
    typed: dict[tuple[str | None, str], int] = _field(default_factory=dict)
    fallback: dict[tuple[str | None, str], int] = _field(default_factory=dict)
    variant_null: dict[tuple[str | None, str], int] = _field(default_factory=dict)
    #: Wall time spent encoding.
    encode_seconds: float = 0.0

    def merge(self, other: VariantReport) -> None:
        """Adds ``other``'s counts to this report. The first invalid row
        stays this report's when it has one."""
        self.rows += other.rows
        self.sql_null_rows += other.sql_null_rows
        self.invalid_rows += other.invalid_rows
        _add_counts(self.invalid_by_reason, other.invalid_by_reason)
        _add_counts(self.typed, other.typed)
        _add_counts(self.fallback, other.fallback)
        _add_counts(self.variant_null, other.variant_null)
        if self.first_invalid is None:
            self.first_invalid = other.first_invalid
        self.encode_seconds += other.encode_seconds


def _add_counts(mine: dict[_K, int], theirs: Mapping[_K, int]) -> None:
    for key, count in theirs.items():
        mine[key] = mine.get(key, 0) + count


@dataclass(frozen=True)
class EncodedVariant:
    """The storage of a VARIANT column, in the write form of its
    declaration (:func:`storage_type`), and what encoding it found.

    ``array`` is one StructArray, or a ChunkedArray when the input was
    large enough to be built in chunks: a chunk ends at 8192 rows, or at
    the row that brings its encoded bytes to 512 MiB, which keeps every
    binary child well under int32 offsets and lets the Python objects of
    one chunk go before the next.
    """

    array: pa.StructArray | pa.ChunkedArray
    report: VariantReport


def storage_type(
    shredding: Shredding | None = None, *, form: Literal["write", "read"] = "write"
) -> pa.StructType:
    """The Arrow storage struct of a VARIANT column with this declaration.

    The layout is the Trino connector's (``toParquetType``): REQUIRED
    ``metadata``; without a declaration (or with a root ``variant``) a
    REQUIRED ``value`` and nothing else; with one an OPTIONAL ``value``
    and an OPTIONAL ``typed_value`` per node, objects as structs of
    REQUIRED field groups in declared order and case, arrays as lists of
    REQUIRED ``element`` groups. The two forms differ only at
    ``decimal4``/``decimal8`` leaves: written as ``decimal32(p, s)`` and
    ``decimal64(p, s)``, whose unscaled int32/int64 a Parquet writer
    stores with DECIMAL(p, s) in the footer, and read back as
    ``decimal128(p, s)`` from a file with no Arrow schema in it (pyarrow
    gives a file it wrote with one the write form; both forms read).
    Each leaf's type names its precision and scale, so declarations of
    different types never share storage. The one overlap is ``decimal4``,
    ``decimal8`` and ``decimal16`` of one (p, s): the first two read back
    as ``decimal128(p, s)``, which is the third's write form, and all
    three hold the same values.

    A node type this version does not know raises
    :class:`~pyhoglake.UnsupportedShreddingError`; any other fault in the
    declaration, the ValidationError of :func:`validate_shredding`.
    """
    _not_a_column(shredding, takes_a_column=False)
    plan = _codec.compile_plan(shredding)
    if form == "write":
        return plan.write_type
    if form == "read":
        return plan.read_type
    raise ValueError(f"form is 'write' or 'read', not {form!r}")


def encode_json(
    values: pa.Array | pa.ChunkedArray | Sequence[str | bytes | None],
    column: Column | None = None,
    *,
    shredding: Shredding | None = None,
    nullable: bool | None = None,
    on_invalid: Literal["raise", "null"] = "raise",
) -> EncodedVariant:
    """Encodes JSON text as the storage of a VARIANT column.

    ``values`` is Arrow text (``pa.json_()``, ``pa.string()``,
    ``pa.large_string()``, ``pa.string_view()``, or the null type), Arrow
    binary holding UTF-8 (``pa.binary()``, ``pa.large_binary()``,
    ``pa.binary_view()``), or a sequence of str or bytes (UTF-8); anything
    else is a TypeError. A null or None is SQL NULL, and the text ``null``
    a Variant null. Give the target as a catalog ``column`` (its
    declaration and nullability are used), or as ``shredding`` and
    ``nullable`` (True when not given), not both.

    The JSON is read strictly: a duplicate key, ``NaN``/``Infinity`` or a
    number beyond the double range, an integer of more than 38 digits, an
    unpaired surrogate, or nesting deeper than :data:`MAX_VARIANT_DEPTH`
    makes a row invalid. Integers become the smallest integer type that
    holds them, or decimal16 with scale 0 beyond int64; other numbers
    become doubles. A row that is invalid raises
    :class:`~pyhoglake.VariantEncodingError` naming the row, the path and
    the reason, or with ``on_invalid="null"`` becomes SQL NULL and is
    counted in the report. ``"null"`` is refused for a NOT NULL target,
    and SQL NULL into one always raises.
    """
    if isinstance(values, (pa.Table, pa.RecordBatch, pa.RecordBatchReader)):
        # Iterated, a table is its columns and a reader its batches, and
        # the first fails as no JSON text, with a hint that does not fit.
        raise TypeError(
            "encode_json takes one column of JSON text, not an Arrow table; "
            "pass table.column(name)"
        )
    _refuse_one_row(values, "encode_json")
    name, plan, target_nullable = _target(column, shredding, nullable, on_invalid)
    report = VariantReport()
    chunks = _codec.encode(
        _json_rows(values),
        plan,
        report,
        json_text=True,
        column=name,
        nullable=target_nullable,
        on_invalid=on_invalid,
    )
    return EncodedVariant(_one_array(chunks, plan), report)


def encode_python(
    values: Iterable[object],
    column: Column | None = None,
    *,
    shredding: Shredding | None = None,
    nullable: bool | None = None,
    on_invalid: Literal["raise", "null"] = "raise",
) -> EncodedVariant:
    """Encodes Python objects as the storage of a VARIANT column.

    A top-level None is SQL NULL; :data:`VARIANT_NULL`, or None inside a
    value, is a Variant null. ``bool``, ``int`` (as JSON integers are),
    ``float`` (NaN and infinities included), ``Decimal`` (decimal4, 8 or 16
    by precision, the scale kept), ``str``, bytes-like, ``date``, naive
    ``time``, ``datetime`` (aware ones normalised to UTC), ``uuid.UUID``,
    ``dict`` with str keys and ``list``/``tuple``; anything else, an aware
    ``time`` included, makes the row invalid, as does an aware datetime
    whose UTC time is outside the years 1 to 9999, which no Python reader
    could give back (``timestamp_out_of_range``). Types are matched
    exactly, containers too, so a numpy scalar, a str subclass, an
    OrderedDict or a namedtuple is refused rather than guessed at.
    The target and ``on_invalid`` are as for :func:`encode_json`.
    """
    if isinstance(values, (pa.Table, pa.RecordBatch, pa.RecordBatchReader)):
        # Iterated, a table is its columns and a reader its batches: one
        # row each, of no Variant type, so the row count would change too.
        raise TypeError(
            "encode_python takes Python values, not an Arrow table; pass "
            "one column, table.column(name).to_pylist()"
        )
    if isinstance(values, (pa.Array, pa.ChunkedArray, pa.Scalar)):
        # Iterated, an Arrow array is pa.Scalar rows, none of which has a
        # Variant type; under on_invalid="null", a column of SQL NULLs.
        raise TypeError(
            "encode_python takes Python values, not Arrow; pass JSON text to "
            "encode_json, or other Arrow values as .to_pylist()"
        )
    _refuse_one_row(values, "encode_python")
    name, plan, target_nullable = _target(column, shredding, nullable, on_invalid)
    report = VariantReport()
    chunks = _codec.encode(
        values,
        plan,
        report,
        json_text=False,
        column=name,
        nullable=target_nullable,
        on_invalid=on_invalid,
    )
    return EncodedVariant(_one_array(chunks, plan), report)


def to_python(
    array: pa.Array | pa.ChunkedArray,
    column: Column | None = None,
    *,
    shredding: Shredding | None = None,
) -> list[object]:
    """Every row of VARIANT storage as Python values.

    Any conformant storage: pyhoglake's, the Trino connector's, or
    DuckDB's, whose field and element groups are OPTIONAL (a null field
    group reads as a missing field), as pyarrow reads it from Parquet, with
    any of its ``binary_type`` and ``list_type`` options.
    SQL NULL is None and a Variant null :data:`VARIANT_NULL`; inside a value
    a Variant null is None. DuckDB writes a SQL NULL as a top-level Variant
    null, which the Trino connector reads back as SQL NULL in a file whose
    ``created_by`` names DuckDB; storage carries no footer, so this reads
    it as :data:`VARIANT_NULL`, and a caller reading DuckDB's files maps a
    top-level :data:`VARIANT_NULL` to None. Integers are int, floats
    float, decimals ``Decimal`` with their scale, timestamps ``datetime``
    (aware in UTC when adjusted to UTC), nanosecond timestamps
    ``pa.TimestampScalar``, binary ``bytes`` and uuids ``uuid.UUID``. A
    date or timestamp outside the years 1 to 9999 that Python's types hold
    is valid all the same, and reads as the pyarrow scalar of its type.
    An object is a dict in the Variant's key order, however it was
    shredded, and in whatever order its bytes list the fields: DuckDB
    1.5.5 lists them as they were inserted, and flags every dictionary
    sorted, sorted or not, and the Trino connector reads both as this
    does (the spec makes them a writer's faults, which :func:`verify`
    refuses). An object that holds one name twice is refused, and so is
    an object or array whose values leave bytes uncovered: the first must
    start at offset 0, and each end where the next one up begins (an
    empty one's last offset is 0). DuckDB 1.5.5 spells a string of
    exactly 64 bytes as an empty short string followed by the bytes,
    which DuckDB itself reads as ``""``, and this reader refuses wherever
    it sits.

    The storage's own types say how to read it, so a table's files need no
    declaration, and those of a writer that honours none (DuckDB's, and so
    hedgerow's) are no form of their column's: read them without one. A
    catalog ``column`` or a ``shredding`` declaration, not both, checks
    storage that pyhoglake or the Trino connector built from that
    declaration: it must be the write or the read form of it, and storage
    of another one is refused (``invalid_shredding``). A column that
    declares nothing is checked against the unshredded form, as
    ``shredding={"type": "variant"}`` is, and its declaration is read as
    the encoders read it (D12); only its declaration is used, not its
    NOT NULL, which :func:`verify` checks. With neither, the storage is not
    checked against any declaration, so ``shredding=None`` is no check
    here, not the unshredded form it is to the encoders.

    A value the spec calls invalid raises VariantEncodingError: bytes that
    do not decode, a typed decimal of more digits than its precision, a
    primitive with both columns set, a shredded field repeated in an
    object's residual, an object in ``value`` beside a null
    ``typed_value``, a group without its ``value`` column or with a column
    named twice. So does a valid value nested deeper than
    :data:`MAX_VARIANT_DEPTH` (``nesting_too_deep``), in bytes or through
    shredded objects and arrays: the spec sets no bound, but this reader
    does, the encoder's.
    """
    _storage_argument(array)
    name, plan = _reader_target(column, shredding)
    return _codec.reassemble(array, plan, strict=False, column=name)


def verify(
    array: pa.Array | pa.ChunkedArray,
    column: Column | None = None,
    *,
    shredding: Shredding | None = None,
    canonical: bool = True,
) -> None:
    """Checks VARIANT storage byte by byte, raising VariantEncodingError at
    the first fault. Opt-in: it decodes every row.

    Beyond what :func:`to_python` refuses, the spec's writer rules: groups
    are never null, no array element is missing, a present row has a value,
    every key is in the row's metadata, a dictionary flagged sorted is
    sorted and unique, and an object lists its fields in key order.
    With ``canonical`` (the default), also the encoding pyhoglake writes,
    which is what makes it deterministic: the dictionary holds exactly the
    row's keys, sorted, with the smallest offset width; every width is the
    smallest, ``is_large`` is set exactly above 255 elements, values are
    laid out in key order, integers and decimals take their smallest type,
    strings under 64 bytes are short strings, no float or nanosecond
    timestamp (in ``value`` or in a typed leaf), no empty residual, no
    reserved header bit set, and no primitive in ``value`` that its
    group's typed leaf takes (D3: an integer the integer leaf holds, a
    decimal of the decimal leaf's scale and precision, a boolean, double,
    string, binary, date, time, timestamp or uuid of the leaf's own type).
    With a ``column`` or ``shredding``, the storage must be the write or
    the read form of that declaration, as for :func:`to_python`: an
    undeclared column's is the unshredded form, ``{"type": "variant"}``,
    and with neither the form is not checked. A NOT NULL ``column`` also
    refuses a SQL NULL row (``sql_null_in_not_null``), as the encoders do
    (D5): this is the check for storage built elsewhere before it is
    appended to that column.
    """
    _storage_argument(array)
    name, plan = _reader_target(column, shredding)
    _codec.reassemble(
        array,
        plan,
        strict=True,
        canonical=canonical,
        column=name,
        nullable=column is None or column.nullable,
    )


def _storage_argument(array: object) -> None:
    # Easy slips: the EncodedVariant itself, or the table around a column.
    if isinstance(array, EncodedVariant):
        raise TypeError("pass the storage, encoded.array, not the EncodedVariant")
    if not isinstance(array, (pa.Array, pa.ChunkedArray)):
        raise TypeError(
            "VARIANT storage is a pyarrow Array or ChunkedArray, not a "
            f"{type(array).__name__}"
        )


def _target(
    column: Column | None,
    shredding: Shredding | None,
    nullable: bool | None,
    on_invalid: str,
) -> tuple[str | None, _codec.Plan, bool]:
    if on_invalid not in ("raise", "null"):
        raise ValueError(f"on_invalid is 'raise' or 'null', not {on_invalid!r}")
    _not_a_column(shredding)
    name = None
    if column is not None:
        _variant_column(column)
        if shredding is not None or nullable is not None:
            raise TypeError("pass a column, or shredding and nullable, not both")
        name = column.name
        nullable = column.nullable
        plan = _column_plan(column, (column.type_params or {}).get(SHREDDING_KEY))
    else:
        plan = _codec.compile_plan(shredding)
    target_nullable = True if nullable is None else nullable
    if on_invalid == "null" and not target_nullable:
        where = f"column '{name}'" if name is not None else "the target"
        raise ValidationError(
            f"on_invalid='null' writes an invalid row as SQL NULL, and {where} is "
            "NOT NULL",
            status_code=None,
        )
    return name, plan, target_nullable


def _reader_target(
    column: Column | None, shredding: Shredding | None
) -> tuple[str | None, _codec.Plan | None]:
    """The column name and the plan to_python and verify check against:
    the encoders' target, but with no target no plan, rather than the
    unshredded one, since storage needs none to be read."""
    _not_a_column(shredding)
    if column is None:
        return None, None if shredding is None else _codec.compile_plan(shredding)
    _variant_column(column)
    if shredding is not None:
        raise TypeError("pass a column or shredding, not both")
    return column.name, _column_plan(
        column, (column.type_params or {}).get(SHREDDING_KEY)
    )


def _variant_column(column: object) -> None:
    if not isinstance(column, Column):
        # variant_field and storage_type take the declaration positionally
        # (variant_field second, storage_type first), so it is an easy slip.
        raise TypeError(
            f"column is a catalog Column, not a {type(column).__name__}; pass "
            "a declaration as shredding="
        )
    if column.type.lower() != "variant":
        raise ValidationError(
            f"column '{column.name}' is {column.type}, not variant",
            status_code=None,
        )


def _not_a_column(shredding: object, *, takes_a_column: bool = True) -> None:
    # The mirror slip: a catalog Column where its declaration goes, which
    # the grammar would refuse only as "$ is not a JSON object".
    if isinstance(shredding, Column):
        hint = (
            "pass the column as the second argument"
            if takes_a_column
            else f"pass (column.type_params or {{}}).get({SHREDDING_KEY!r})"
        )
        raise TypeError(f"shredding is a declaration, not a catalog Column; {hint}")


def _column_plan(column: Column, shredding: Shredding | None) -> _codec.Plan:
    """The plan of a catalog column's declaration.

    The server accepted that declaration, so one this client refuses is
    a grammar newer than it (a new key, a relaxed limit), not the
    caller's mistake: it fails as an unknown node type does, as
    UnsupportedShreddingError (D12). A declaration a caller hands in is
    checked as the server would check it, with its ValidationError.
    """
    try:
        return _codec.compile_plan(shredding)
    except UnsupportedShreddingError as exc:
        # An unknown node type: the same refusal, naming the column, of
        # which a table may have several.
        raise UnsupportedShreddingError(
            f"column '{column.name}': {exc.message}", status_code=None
        ) from exc
    except ValidationError as exc:
        raise UnsupportedShreddingError(
            f"column '{column.name}' declares a type_params.shredding this version "
            f"of pyhoglake does not know ({exc.message}); the server accepted it, "
            "so upgrade pyhoglake",
            status_code=None,
        ) from exc


def _refuse_one_row(values: object, function: str) -> None:
    # One row passed bare would be iterated as rows: a string as its
    # characters, a dict as its keys.
    if isinstance(values, (str, bytes, bytearray, memoryview, Mapping)):
        raise TypeError(
            f"{function} takes a sequence of rows, not one "
            f"{type(values).__name__}; pass [value] for a single row"
        )


def _json_rows(
    values: pa.Array | pa.ChunkedArray | Sequence[str | bytes | None],
) -> Iterable[object]:
    """The rows of JSON input, a slice of Arrow text at a time, so that the
    Python strings of one chunk are gone before the next."""
    if not isinstance(values, (pa.Array, pa.ChunkedArray)):
        yield from values
        return
    kind = values.type
    if _is_json_type(kind):
        kind = kind.storage_type
    if not (
        pa.types.is_string(kind)
        or pa.types.is_large_string(kind)
        or pa.types.is_string_view(kind)
        or pa.types.is_binary(kind)
        or pa.types.is_large_binary(kind)
        or pa.types.is_binary_view(kind)
        or pa.types.is_null(kind)
    ):
        # A TypeError, as the same slip in a list of rows is: the argument
        # is wrong, not the data, which a ValidationError would say.
        raise TypeError(
            "encode_json takes JSON text (pa.json_(), a string type, or binary "
            f"holding UTF-8), not {values.type}"
        )
    chunks = values.chunks if isinstance(values, pa.ChunkedArray) else [values]
    step = _codec.CHUNK_ROWS
    for chunk in chunks:
        if isinstance(chunk, pa.ExtensionArray):
            chunk = chunk.storage
        # As bytes: Arrow does not check that text is UTF-8 (a view, a
        # Parquet file, IPC), and to_pylist would fail the whole batch on
        # one bad row. The encoder decodes each row and refuses that one
        # as invalid_unicode (D6). Binary needs no view: it is those bytes
        # already, and the usual Arrow form of a message payload.
        if pa.types.is_string(chunk.type):
            chunk = chunk.view(pa.binary())
        elif pa.types.is_large_string(chunk.type):
            chunk = chunk.view(pa.large_binary())
        elif pa.types.is_string_view(chunk.type):
            chunk = chunk.view(pa.binary_view())
        for start in range(0, len(chunk), step):
            yield from chunk.slice(start, step).to_pylist()


def _one_array(
    chunks: list[pa.StructArray], plan: _codec.Plan
) -> pa.StructArray | pa.ChunkedArray:
    if len(chunks) == 1:
        return chunks[0]
    return pa.chunked_array(chunks, type=plan.write_type)


# -- internals ----------------------------------------------------------------


def _canonical_json(value: Any) -> str:
    """The one spelling of a declaration that compares and caches.

    The server stores ``type_params`` as JSONB, which reorders object
    keys and keeps array order and integers, so a declaration read back
    is equal to the one sent only up to key order. Sorting keys while
    keeping arrays in order is exactly that equivalence. ASCII-only:
    other characters are ``\\u`` escapes, so the bytes depend on no
    text encoding.
    """
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _plain(node: Any) -> Any:
    """A declaration as plain dicts and lists.

    What goes on the wire is a copy, not the caller's object, and any
    Mapping or tuple the caller built it from becomes the JSON shape the
    request encoder knows. Called on a validated declaration, whose depth
    and size the rules bound, or on one ops.add_column sends unvalidated
    for the server to refuse with its column type; one of those too deep
    to recurse into fails here as it would in the request encoder.
    """
    if isinstance(node, Mapping):
        return {key: _plain(value) for key, value in node.items()}
    if isinstance(node, (list, tuple)):
        return [_plain(value) for value in node]
    return node


def _is_json_type(t: pa.DataType) -> bool:
    # pa.JsonType is pyarrow >= 19, below the project's floor of 21.
    return isinstance(t, getattr(pa, "JsonType", ()))


def _object_without_repeats(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """A JSON object of a marker, refusing a key it repeats: json.loads
    would keep the last value, and which declaration (or none) a hand-built
    marker meant is not to be guessed. variant_field writes from a dict,
    so never repeats one."""
    obj = dict(pairs)
    if len(obj) != len(pairs):
        raise ValueError("repeated key")
    return obj


def _marker_shredding(field: pa.Field, column: str) -> tuple[bool, Shredding | None]:
    """Whether ``field`` carries the variant marker, and its declaration.

    ``column`` is the field's qualified name, for messages. A marker this
    module did not write — on a type other than JSON, not a JSON object,
    with a key other than ``shredding``, or with a key repeated at any
    level — is refused rather than guessed at: the column it would define
    is permanent. The declaration itself is not validated here;
    :func:`_variant_column_def` does that after the nesting check, in the
    server's order.
    """
    metadata = field.metadata
    if not metadata or VARIANT_FIELD_KEY not in metadata:
        return False, None
    if not _is_json_type(field.type):
        raise ValidationError(
            f"field '{column}' carries the {VARIANT_FIELD_KEY.decode()} marker on "
            f"{field.type}; a variant field is pa.json_(), so build it with "
            "pyhoglake.variant.variant_field",
            status_code=None,
        )
    try:
        marker = json.loads(
            metadata[VARIANT_FIELD_KEY], object_pairs_hook=_object_without_repeats
        )
    except (ValueError, RecursionError):
        marker = None
    if not isinstance(marker, dict) or set(marker) - {SHREDDING_KEY}:
        raise ValidationError(
            f"field '{column}' has a malformed {VARIANT_FIELD_KEY.decode()} marker; "
            "build the field with pyhoglake.variant.variant_field",
            status_code=None,
        )
    return True, marker.get(SHREDDING_KEY)


def _variant_column_def(
    field: pa.Field, name: str, column: str, *, top_level: bool
) -> dict[str, Any] | None:
    """The column definition a marked field declares, or None when unmarked.

    ``name`` is the column's name in the catalog (a list element's is
    ``element`` whatever Arrow calls it) and ``column`` its qualified
    name (``r.x``, ``l.element``), for messages.

    A declaration below the top level is refused locally, in the server's
    words: no writer shreds a nested variant, so the server answers one
    with a 422. That comes before the declaration's own faults, as the
    server's ColumnTrees checks it.
    """
    marked, shredding = _marker_shredding(field, column)
    if not marked:
        return None
    col: dict[str, Any] = {"name": name, "type": "variant", "nullable": field.nullable}
    if shredding is not None:
        if not top_level:
            raise ValidationError(
                f"variant column '{column}' is nested, and only a top-level variant "
                "column can declare type_params.shredding",
                status_code=None,
            )
        validate_shredding(shredding, column=column)
        col["type_params"] = {SHREDDING_KEY: _plain(shredding)}
    return col


def _cap(value: object) -> str:
    """Identifiers.cap, to the code unit: a quoted value over 64 UTF-16
    code units keeps its first 61 and gains "...".

    UTF-16, not code points, because the server's ``String.length``
    counts units: a name of astral characters is cut where the server
    cuts it, which can be between the two halves of a pair, and then the
    message carries the lone half exactly as the server's does.
    """
    text = str(value)
    units = text.encode("utf-16-le", "surrogatepass")
    if len(units) <= 2 * _CAP_UNITS:
        return text
    return units[: 2 * (_CAP_UNITS - 3)].decode("utf-16-le", "surrogatepass") + "..."


def _has_surrogate(name: str) -> bool:
    return any("\ud800" <= char <= "\udfff" for char in name)


def _lowercase_as_on_server(name: str) -> str | None:
    """``name`` lowercased as the server lowercases it, or None where
    ``str.lower()`` may not.

    Kotlin's ``lowercase()`` is the JDK's ``toLowerCase(Locale.ROOT)``,
    which maps every character as ``str.lower()`` does, ``İ`` to ``i̇``
    included, but one: ``Σ``, which becomes final ``ς`` when a cased
    letter precedes it in its word and none follows. The JDK finds the
    word with its word BreakIterator, which takes ``_``, digits and
    hyphens into the word (``A_Σ`` is ``a_ς``); Python skips only
    case-ignorable characters (``a_σ``). So the server's lowercase of a
    name with a ``Σ`` is unknown here, whichever side of a pair it is on.
    """
    return None if _CAPITAL_SIGMA in name else name.lower()


def _same_case_on_server(a: str, b: str) -> bool:
    """Whether the server, too, lowercases ``a`` and ``b``, two names
    without a ``Σ`` that ``str.lower()`` makes equal, to one name.

    Case pairs are stable once both characters are encoded, so the
    server's mappings are a superset of any older Unicode's: a pair equal
    here is equal there, unless this Python's data is the newer. Then
    only ASCII, whose case no Unicode version changes, is certain.
    """
    return _LOCAL_UNICODE <= _SERVER_UNICODE or (a.isascii() and b.isascii())


def _int_param(node: Mapping[Any, Any], key: str) -> int | None:
    """An integer that fits the server's Int. ``True``, ``18.0`` and
    ``"18"`` are not integers, as they are not to the server."""
    value = node.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if _INT_MIN <= value <= _INT_MAX else None


class _Walk:
    """One validation, mirroring the server's ``VariantShredding.Walk``.

    Recursion is once per object or array, refused one past
    :data:`MAX_SHREDDING_DEPTH` before it recurses, and an unknown key
    is refused before its value is looked at, so the walk is bounded
    whatever the caller passes.
    """

    def __init__(self, column: str | None) -> None:
        self._column = column
        #: Fields and arrays seen so far, over the whole declaration.
        self._fields = 0

    def visit(
        self,
        node: object,
        path: str,
        depth: int,
        name_bytes: int,
        context: frozenset[str],
    ) -> None:
        """One node at ``path``. ``name_bytes`` is the UTF-8 length of the
        field names on the way to it, and ``context`` holds the keys it may
        have besides those of its type: ``name`` for an object field."""
        if not isinstance(node, Mapping):
            raise self._refusal(path, "is not a JSON object")
        type_ = node.get("type")
        if not isinstance(type_, str):
            raise self._refusal(path, "has no type")
        if type_ == "variant" or type_ in PRIMITIVE_TYPES:
            self._check_keys(node, path, context)
        elif type_ == "object":
            self._check_keys(node, path, context, "fields")
            self._check_depth(path, depth)
            self._visit_fields(node.get("fields"), path, depth, name_bytes)
        elif type_ == "array":
            self._check_keys(node, path, context, "element")
            self._check_depth(path, depth)
            self._count()
            element = node.get("element")
            if element is None:
                raise self._refusal(path, "has no element")
            # An array adds no name, and keeps the names above it.
            self.visit(element, f"{path}[*]", depth + 1, name_bytes, frozenset())
        elif type_ in DECIMAL_TYPES:
            self._check_keys(node, path, context, "precision", "scale")
            precision = _int_param(node, "precision")
            if precision is None:
                raise self._refusal(path, "has no integer precision")
            scale = _int_param(node, "scale")
            if scale is None:
                raise self._refusal(path, "has no integer scale")
            if not (1 <= precision <= DECIMAL_TYPES[type_] and 0 <= scale <= precision):
                raise self._refusal(
                    path,
                    f"has precision {precision} and scale {scale}, which {type_} "
                    "does not hold",
                )
        else:
            raise self._refusal(path, f"has an unknown type '{_cap(type_)}'")

    def _visit_fields(
        self, fields: object, path: str, depth: int, name_bytes: int
    ) -> None:
        # A JSON array: a list, or a tuple, which the request encodes as one.
        if not isinstance(fields, (list, tuple)) or not fields:
            raise self._refusal(path, "has no fields")
        # A reader finds the Parquet column of a field by its lowercase
        # name, so it cannot tell apart two that differ only by case.
        # `names` maps the lowercase of a name to the first name with it,
        # as the server's map does String.lowercase(). A name whose
        # lowercase may differ there (a Σ) stays out of it, so it neither
        # collides here nor hides a later pair that collides on the
        # server too; `seen` holds the names as written, so an exact
        # duplicate is refused even then.
        names: dict[str, str] = {}
        seen: set[str] = set()
        for field in fields:
            self._count()
            name = field.get("name") if isinstance(field, Mapping) else None
            if not isinstance(name, str):
                raise self._refusal(path, "has a field without a name")
            if not name:
                raise self._refusal(path, "has a field with an empty name")
            # JSONB has no NUL character, and UTF-8 has no unpaired
            # surrogates: either would be stored as something other than
            # what was declared.
            if "\x00" in name:
                raise self._refusal(path, "has a field name with a NUL character")
            if _has_surrogate(name):
                raise self._refusal(path, "has a field name with an unpaired surrogate")
            field_name_bytes = name_bytes + len(name.encode("utf-8"))
            if field_name_bytes > MAX_PATH_NAME_BYTES:
                raise self._refusal(
                    path,
                    "has a field whose name, with the names above it, is longer "
                    f"than {MAX_PATH_NAME_BYTES} bytes",
                )
            if name in seen:
                raise self._refusal(path, f"has duplicate field '{_cap(name)}'")
            seen.add(name)
            lower = _lowercase_as_on_server(name)
            previous = name if lower is None else names.setdefault(lower, name)
            if previous != name and _same_case_on_server(previous, name):
                raise self._refusal(
                    path,
                    "has fields that differ only by case: "
                    f"'{_cap(previous)}' and '{_cap(name)}'",
                )
            # Depth first, as the server walks: a fault inside this field
            # is reported before one in a later sibling.
            self.visit(
                field, f"{path}.{_cap(name)}", depth + 1, field_name_bytes, _FIELD_KEYS
            )

    def _check_keys(
        self,
        node: Mapping[Any, Any],
        path: str,
        context: frozenset[str],
        *type_keys: str,
    ) -> None:
        for key in node:
            if key != "type" and key not in context and key not in type_keys:
                raise self._refusal(path, f"has an unknown key '{_cap(key)}'")

    def _check_depth(self, path: str, depth: int) -> None:
        if depth >= MAX_SHREDDING_DEPTH:
            raise self._refusal(
                path, f"is nested more than {MAX_SHREDDING_DEPTH} levels deep"
            )

    def _count(self) -> None:
        """Counts an object field or an array."""
        self._fields += 1
        if self._fields > MAX_SHREDDING_FIELDS:
            raise self._refusal(
                "$", f"has more than {MAX_SHREDDING_FIELDS} fields and arrays"
            )

    def _refusal(self, path: str, problem: str) -> ValidationError:
        message = f"invalid type_params.shredding: {path} {problem}"
        if self._column is not None:
            message = f"variant column '{self._column}' has an {message}"
        return ValidationError(message, status_code=None)


#: Why JSON text fills a leaf rarely or never (json_unshreddable_paths).
_NEVER_FROM_JSON: Final = {
    "float": "JSON numbers become int or double values, never float",
    "date": "JSON has no date; strings are never read as dates",
    "time": "JSON has no time; strings are never read as times",
    "timestamp": "JSON has no timestamp; strings are never read as timestamps",
    "timestamp_ns": "JSON has no timestamp; strings are never read as timestamps",
    "timestamptz": "JSON has no timestamp; strings are never read as timestamps",
    "timestamptz_ns": "JSON has no timestamp; strings are never read as timestamps",
    "binary": "JSON has no binary values",
    "uuid": "JSON has no uuid; strings are never read as uuids",
    "decimal4": "JSON numbers become int, double or wide decimal16 values, never decimal4",
    "decimal8": "JSON numbers become int, double or wide decimal16 values, never decimal8",
}


def _collect_unshreddable(
    node: Shredding, path: str, out: list[tuple[str, str]]
) -> None:
    type_ = node["type"]
    if type_ == "object":
        for field in node["fields"]:
            _collect_unshreddable(field, path + _codec.member(field["name"]), out)
        return
    if type_ == "array":
        _collect_unshreddable(node["element"], f"{path}[*]", out)
        return
    reason = _NEVER_FROM_JSON.get(type_)
    if reason is not None:
        reason = f"{type_}: {reason}"
    elif type_ == "double":
        reason = (
            "double: only numbers written with a fraction or an exponent; "
            "integers are int values"
        )
    elif type_ == "decimal16":
        label = f"decimal16({node['precision']},{node['scale']})"
        if node["scale"] > 0:
            reason = f"{label}: JSON fractions become double values, never decimals"
        elif node["precision"] < 19:
            # 10^18 - 1 is the widest a precision below 19 holds, and it is
            # inside the int64 range, where JSON integers are int values.
            reason = (
                f"{label}: integers within the int64 range are int values, "
                "and nothing wider fits"
            )
        else:
            reason = f"{label}: only integers beyond the int64 range"
    if reason is not None:
        out.append((path, reason))
