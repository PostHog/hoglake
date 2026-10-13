"""Property-based checks of the VARIANT footer splice and layout checks
(pyhoglake.parquet_schema).

The splice is a rewriter of pyarrow's footers, so its oracle is GROUND
TRUTH from another codec: tests/footer_oracle.py decodes the whole footer,
adds the fields and re-encodes it, sharing no code with the splice, and the
stamps it is given come from variant_corpus.oracle_stamps, which reads the
leaves' physical types off the footer itself.

Properties:

- write: for generated declarations and rows, write_variant_parquet's body
  is the oracle's stamp of the bytes pyarrow wrote, byte for byte; its
  footer_size is the trailer's; and strict validation (spec mode, and the
  declaration) accepts it;
- robustness: schema elements mutated from real footers (pyhoglake's,
  DuckDB's and parquet-testing's) get a fault or None from
  variant_layout_fault in both modes, never an exception, which is the
  promise validate_variant_file(strict=True) makes of a foreign file
  (docs/fuzzing.md: refused, never another exception);
- footers: the same files' footer bytes overwritten at random, and their
  created_by forged (non-UTF-8 among it), as columns of either
  nullability: whatever pyarrow opens, validate_variant_file accepts or
  refuses with ValidationError on both paths, never another exception, and
  extract_column_stats, which prepare_append_files runs next, reads what
  it accepted. The element mutations above cannot reach what strict reads
  past the schema (created_by and the statistics of the DuckDB rule);
- chunks: the same files with the fields pyarrow reads only when a chunk's
  metadata or statistics are first read (its type, its statistics' bounds,
  its SizeStatistics, its encryption) forged, each set in place, given a
  second time, or spelled with a wire type parquet.thrift does not
  declare (a map of keys and values of other types among them): the same
  promise, and validate_column_chunks's, after which every chunk's
  metadata and statistics read;
- elements: the same files with a schema element's fields spelled with
  another wire type, given twice or dropped, or the schema list given
  twice, and a level histogram forged beside them: the same promise;
- lists: the same files with a list parquet.thrift declares spelled with
  a header naming another item type (which Thrift's generated readers
  read as the declared one), its items of that type, or a chunk's
  encodings holding chunk fields the guard checks inside a binary item:
  the same promise.

Random byte overwrites rarely spell the last three. A breach of any of the
last four is not a failed assertion but an aborted process: pyarrow's
C++ exception there is never translated.
"""

from __future__ import annotations

import copy
from dataclasses import replace
from functools import cache
from pathlib import Path

import footer_oracle
import pyarrow as pa
import pyarrow.parquet as pq
from hypothesis import given, settings
from hypothesis import strategies as st
from qe_prop_variant_codec import DECLARATIONS, PYTHON_VALUES
from thrift.Thrift import TType
from variant_corpus import integer_view, oracle_stamps

from pyhoglake import variant
from pyhoglake._variant_codec import compile_plan
from pyhoglake.errors import UnsupportedTypeError, ValidationError
from pyhoglake.models import Column
from pyhoglake.parquet_schema import (
    _footer_bytes,
    _leaf_levels,
    _schema_elements_from_footer,
    _top_level,
    validate_column_chunks,
    validate_variant_file,
    variant_layout_fault,
    write_variant_parquet,
)
from pyhoglake.stats import extract_column_stats
from pyhoglake.types import arrow_type_to_coltype

# The "qe" hypothesis profile (deadline=None) is loaded in conftest.py.

DATA = Path(__file__).parents[1] / "data"


def _column(decl, nullable: bool) -> Column:
    params = None if decl is None else {"shredding": decl}
    return Column("v", "variant", 3, 0, nullable, type_params=params)


def _table(column: Column, storage: pa.Array) -> pa.Table:
    field = pa.field(
        "v",
        storage.type,
        nullable=column.nullable,
        metadata={b"PARQUET:field_id": str(column.field_id).encode()},
    )
    return pa.table([storage], schema=pa.schema([field]))


def _pyarrow_bytes(table: pa.Table) -> bytes:
    """What pyarrow writes of ``table``, its decimal leaves viewed as the
    integers they hold and no Arrow schema stored: the splice's input,
    written here apart from write_variant_parquet."""
    kind = integer_view(table.schema.field("v").type)
    chunks = [chunk.view(kind) for chunk in table.column("v").chunks]
    plain = pa.table(
        [pa.chunked_array(chunks, kind)],
        schema=pa.schema([table.schema.field("v").with_type(kind)]),
    )
    sink = pa.BufferOutputStream()
    pq.write_table(plain, sink, store_schema=False)
    return sink.getvalue().to_pybytes()


@settings(max_examples=150)
@given(DECLARATIONS, st.booleans(), st.lists(PYTHON_VALUES, max_size=5))
def test_written_files_are_the_oracles_stamp_and_pass_strict(decl, nullable, values):
    column = _column(decl, nullable)
    rows = values if nullable else [v for v in values if v is not None]
    table = _table(column, variant.encode_python(rows, column).array)
    written = write_variant_parquet(table, [column], {"v": compile_plan(decl)})
    raw = _pyarrow_bytes(table)
    assert written.body == footer_oracle.stamp(raw, oracle_stamps(raw, [column]))
    assert written.footer_size == len(footer_oracle.split(written.body)[1])
    with pq.ParquetFile(pa.BufferReader(written.body)) as parquet:
        validate_variant_file(written.body, parquet, (column,), strict=True)


#: The one file under tests/data whose footer the reader refuses, a union
#: of two fields (test_variant_schema.SERVER_CANNOT_READ), so it has no
#: schema list to mutate.
UNREADABLE = "native_variant_union_two_fields.parquet"


@cache
def _footers() -> list[tuple[str, list[dict], list[int]]]:
    """Each footer's schema elements and the indexes of its top-level
    groups, from every VARIANT file under tests/data."""
    paths = [
        *sorted(DATA.glob("*.parquet")),
        DATA / "variant" / "corpus_stamped.parquet",
        *sorted((DATA / "parquet-testing" / "shredded_variant").glob("*.parquet")),
    ]
    out = []
    for path in paths:
        if path.name == UNREADABLE:
            continue
        elements = _schema_elements_from_footer(_footer_bytes(path.read_bytes())[1])
        groups = [index for element, _, index in _top_level(elements) if 5 in element]
        if groups:
            out.append((path.name, elements, groups))
    return out


#: What a mutated field may become: the ids, counts, names and annotation
#: shapes a footer holds, and the ones Thrift can spell that it should not.
FIELD_VALUES = st.one_of(
    st.sampled_from([None, True, False, b"x", "", "value", "typed_value", "metadata"]),
    st.sampled_from(["list", "element", "array", "typed_value_tuple", "a", "A"]),
    st.integers(-3, 40),
    st.sampled_from([2**31 - 1, -(2**31), 10**12]),
    st.sampled_from(
        [
            {},
            {16: {1: 1}},
            {16: 1},
            {3: {}},
            {1: {}},
            {5: {1: 2, 2: 9}},
            {5: {}},
            {10: {1: 8}},
            {10: {1: 8, 2: "x"}},
            {8: {1: True, 2: 7}},
            {7: {2: {1: {}}}},
            {11: {}},
            {1: {}, 4: {}},
        ]
    ),
)


@settings(max_examples=400)
@given(st.data())
def test_mutated_layouts_get_a_fault_or_none_never_an_exception(data):
    name, base, groups = data.draw(st.sampled_from(_footers()), label="file")
    start = data.draw(st.sampled_from(groups), label="group")
    elements = copy.deepcopy(base)
    for _ in range(data.draw(st.integers(1, 4))):
        index = data.draw(st.integers(start, len(elements) - 1))
        field = data.draw(st.integers(1, 10))
        if data.draw(st.booleans()):
            elements[index].pop(field, None)
        else:
            elements[index][field] = copy.deepcopy(data.draw(FIELD_VALUES))
    group = elements[start]
    column = Column(
        group[4] if isinstance(group.get(4), str) else "v",
        "variant",
        group[9] if isinstance(group.get(9), int) else 1,
        0,
        True,
    )
    plan = compile_plan(data.draw(DECLARATIONS))
    for mode in ("exact", "spec"):
        for each in (None, plan):
            fault = variant_layout_fault(elements, start, column, each, mode=mode)
            assert fault is None or isinstance(fault, str), (name, fault)


@cache
def _files() -> list[tuple[str, bytes, tuple[Column, ...]]]:
    """Each VARIANT file under tests/data, as bytes, with the catalog
    columns it is a file of: a top-level group annotated VARIANT is a
    variant column, any other column the type its Arrow type maps to."""
    paths = [
        *sorted(DATA.glob("*.parquet")),
        DATA / "variant" / "corpus_stamped.parquet",
        *sorted((DATA / "parquet-testing" / "shredded_variant").glob("*.parquet")),
    ]
    out = []
    for path in paths:
        if path.name == UNREADABLE:
            continue
        raw = path.read_bytes()
        elements = _schema_elements_from_footer(_footer_bytes(raw)[1])
        with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
            arrow = parquet.schema_arrow
        columns = []
        for ordinal, ((element, _, _), field) in enumerate(
            zip(_top_level(elements), arrow, strict=True)
        ):
            field_id = element.get(9, 0)
            if 16 in element.get(10, {}):
                columns.append(Column(field.name, "variant", field_id, ordinal, True))
                continue
            try:
                kind, params = arrow_type_to_coltype(field.type)
            except UnsupportedTypeError:
                break
            columns.append(
                Column(field.name, kind, field_id, ordinal, True, type_params=params)
            )
        else:
            if any(column.type == "variant" for column in columns):
                out.append((path.name, raw, tuple(columns)))
    return out


#: created_by values to forge: DuckDB's, which turns on the DuckDB rule,
#: ones that are not quite it, and bytes that are not UTF-8.
CREATED_BY = st.one_of(
    st.sampled_from(
        [
            b"DuckDB version v1.5.5 (build d8cdaa33fd)",
            b"DuckDB",
            b"DuckDB \xff",
            b"\xffDuckDB",
            b"parquet-cpp-arrow (via DuckDB)",
            b"",
        ]
    ),
    st.binary(max_size=12),
)


@settings(max_examples=300)
@given(st.data())
def test_mutated_footers_are_accepted_or_refused_never_raised_past(data):
    _name, raw, columns = data.draw(st.sampled_from(_files()), label="file")
    columns = tuple(
        replace(column, nullable=data.draw(st.booleans())) for column in columns
    )
    if data.draw(st.booleans(), label="forge created_by"):
        created_by = data.draw(CREATED_BY, label="created_by")
        raw = footer_oracle.rebuild(
            raw, lambda tree: footer_oracle.put(tree, 6, TType.STRING, created_by)
        )
    start, footer = _footer_bytes(raw)
    body = bytearray(raw)
    for _ in range(data.draw(st.integers(0, 3), label="overwrites")):
        index = start + data.draw(st.integers(0, len(footer) - 1))
        body[index] = data.draw(st.integers(0, 255))
    _open_then_validate(bytes(body), columns)


def _validate_then_extract(raw, parquet, columns):
    """validate_variant_file, strict and not, and validate_column_chunks;
    and when the first accepts, the statistics prepare_append_files reads
    after it, and when the last does, every chunk's metadata and
    statistics."""
    with parquet:
        accepted = False
        for strict in (False, True):
            try:
                validate_variant_file(raw, parquet, columns, strict=strict)
                accepted = True
            except ValidationError:
                pass
        if accepted:
            extract_column_stats(parquet.metadata, columns)
        try:
            validate_column_chunks(raw, parquet)
        except ValidationError:
            return
        metadata = parquet.metadata
        for group in range(metadata.num_row_groups):
            row_group = metadata.row_group(group)
            for index in range(row_group.num_columns):
                chunk = row_group.column(index)
                _ = chunk.statistics
                # The path the readers find the chunk's column by is its
                # leaf's, as pyarrow itself spells the leaf.
                assert chunk.path_in_schema == metadata.schema.column(index).path


#: ColumnCryptoMetaData's union: the footer's key, or a column's own.
_ENCRYPTION = st.sampled_from(
    [
        [[1, TType.STRUCT, []]],
        [[2, TType.STRUCT, [[1, TType.LIST, [TType.STRING, [b"v"]]]]]],
    ]
)
_LEVELS = st.lists(st.integers(0, 2**40), max_size=5)

#: One forged ColumnMetaData field, as (what, its value): its type, its
#: codec, its path_in_schema (a name of the file's, or another), one
#: PageEncodingStats (a page type and an encoding, each in or just past
#: its enum), a Statistics field (the legacy max and min, the null count,
#: max_value, min_value), or a SizeStatistics (unencoded byte array
#: bytes, and the repetition and definition level histograms).
FIELD_EDITS = st.one_of(
    st.tuples(st.just("type"), st.integers(0, 7)),
    st.tuples(st.just("codec"), st.integers(-1, 9)),
    st.tuples(
        st.just("path"),
        st.lists(
            st.sampled_from([b"id", b"v", b"properties", b"metadata", b"value", b"x"]),
            min_size=1,
            max_size=3,
        ),
    ),
    st.tuples(
        st.just("encoding_stats"), st.tuples(st.integers(-1, 5), st.integers(-1, 11))
    ),
    st.tuples(
        st.just("bound"),
        st.tuples(st.sampled_from([1, 2, 5, 6]), st.binary(max_size=20)),
    ),
    st.tuples(st.just("null_count"), st.integers(-1, 2**40)),
    st.tuples(
        st.just("size_statistics"),
        st.tuples(
            st.none() | st.integers(0, 2**40), st.none() | _LEVELS, st.none() | _LEVELS
        ),
    ),
)

#: One forged field of a chunk: one of those, no statistics, an
#: encryption, or no metadata at all.
CHUNK_EDITS = st.one_of(
    FIELD_EDITS,
    st.tuples(st.just("no_statistics"), st.none()),
    st.tuples(st.just("encryption"), _ENCRYPTION),
    st.tuples(st.just("no_metadata"), st.none()),
)


#: A map of a few entries whose keys and values are of different types, each
#: of which a skip must read as its own: an i32 key read as the value's
#: binary, or a double read as an i64's varint, reads the bytes after it
#: out of step with pyarrow.
_MIXED_MAPS = st.one_of(
    st.lists(
        st.tuples(st.integers(-(2**31), 2**31 - 1), st.binary(max_size=8)),
        min_size=1,
        max_size=3,
    ).map(lambda pairs: [TType.I32, TType.STRING, pairs]),
    st.lists(
        st.tuples(st.binary(max_size=8), st.floats(allow_nan=False)),
        min_size=1,
        max_size=3,
    ).map(lambda pairs: [TType.STRING, TType.DOUBLE, pairs]),
)

#: A value of each wire type, to spell a field with a type parquet.thrift
#: does not declare for it (which Thrift's generated readers skip).
WIRE_VALUES = st.one_of(
    st.tuples(st.just(TType.BOOL), st.booleans()),
    st.tuples(st.just(TType.BYTE), st.integers(-128, 127)),
    st.tuples(st.just(TType.I16), st.integers(-(2**15), 2**15 - 1)),
    st.tuples(st.just(TType.I32), st.integers(-(2**31), 2**31 - 1)),
    st.tuples(st.just(TType.I64), st.integers(-(2**40), 2**40)),
    st.tuples(st.just(TType.DOUBLE), st.floats(allow_nan=False)),
    st.tuples(st.just(TType.STRING), st.binary(max_size=8)),
    st.tuples(st.just(TType.STRUCT), st.sampled_from([[], [[1, TType.BYTE, 1]]])),
    st.tuples(st.just(TType.LIST), st.tuples(st.just(TType.I64), _LEVELS).map(list)),
    st.tuples(st.just(TType.MAP), _MIXED_MAPS),
)

#: How a forged field is spelled: set in place of the one there, given a
#: second time after the struct's other fields, or set with another wire
#: type (and a value of that type) than the one drawn.
SPELLINGS = st.sampled_from(["put", "repeat", "mistype"])


def _set(fields, field_id, kind, value, how, wire):
    if how == "repeat":
        footer_oracle.repeat(fields, field_id, kind, value)
    elif how == "mistype" and wire[0] != kind:
        footer_oracle.put(fields, field_id, *wire)
    else:
        footer_oracle.put(fields, field_id, kind, value)


def _forge(chunk, what, value, how, wire):
    meta = footer_oracle.field(chunk, 3)
    if what == "encryption":
        _set(chunk, 8, TType.STRUCT, value, how, wire)
    elif what == "no_metadata":
        footer_oracle.drop(chunk, 3)
    elif meta is None:
        return
    elif what == "type":
        _set(meta, 1, TType.I32, value, how, wire)
    elif what == "codec":
        _set(meta, 4, TType.I32, value, how, wire)
    elif what == "path":
        _set(meta, 3, TType.LIST, [TType.STRING, value], how, wire)
    elif what == "encoding_stats":
        page_type, encoding = value
        stats = [[1, TType.I32, page_type], [2, TType.I32, encoding], [3, TType.I32, 1]]
        _set(meta, 13, TType.LIST, [TType.STRUCT, [stats]], how, wire)
    elif what == "no_statistics":
        footer_oracle.drop(meta, 12)
    elif what == "size_statistics":
        unencoded, repetition, definition = value
        spelled = [] if unencoded is None else [[1, TType.I64, unencoded]]
        for field_id, levels in ((2, repetition), (3, definition)):
            if levels is not None:
                spelled.append([field_id, TType.LIST, [TType.I64, levels]])
        _set(meta, 16, TType.STRUCT, spelled, how, wire)
    else:
        statistics = footer_oracle.field(meta, 12)
        if statistics is None:
            statistics = []
            footer_oracle.put(meta, 12, TType.STRUCT, statistics)
        if what == "null_count":
            _set(statistics, 3, TType.I64, value, how, wire)
        else:
            field_id, bound = value
            _set(statistics, field_id, TType.STRING, bound, how, wire)


@settings(max_examples=300)
@given(st.data())
def test_forged_column_chunks_are_accepted_or_refused_never_raised_past(data):
    _name, raw, columns = data.draw(st.sampled_from(_files()), label="file")
    columns = tuple(
        replace(column, nullable=data.draw(st.booleans())) for column in columns
    )
    leaves = len(_leaf_levels(_schema_elements_from_footer(_footer_bytes(raw)[1])))
    edits = data.draw(
        st.lists(
            st.tuples(st.integers(0, leaves - 1), CHUNK_EDITS, SPELLINGS, WIRE_VALUES),
            min_size=1,
            max_size=3,
        ),
        label="edits",
    )

    def apply(tree):
        for row_group in footer_oracle.field(tree, 4)[1]:
            chunks = footer_oracle.field(row_group, 1)[1]
            for leaf, (what, value), how, wire in edits:
                _forge(chunks[leaf], what, value, how, wire)

    _open_then_validate(footer_oracle.rebuild(raw, apply), columns)


#: The lists a forged item type reaches what the guard checks through, as
#: (whose, field id, the item type parquet.thrift declares): a chunk's
#: ColumnMetaData's or SizeStatistics's, a RowGroup's, or the
#: FileMetaData's.
LIST_SITES = st.sampled_from(
    [
        ("meta", 2, TType.I32),
        ("meta", 3, TType.STRING),
        ("meta", 8, TType.STRUCT),
        ("meta", 13, TType.STRUCT),
        ("size", 2, TType.I64),
        ("size", 3, TType.I64),
        ("row_group", 4, TType.STRUCT),
        ("file", 5, TType.STRUCT),
        ("file", 7, TType.STRUCT),
    ]
)

#: Items of each type a list's header may name.
ITEMS = {
    TType.BOOL: st.booleans(),
    TType.BYTE: st.integers(-128, 127),
    TType.I32: st.integers(-(2**31), 2**31 - 1),
    TType.I64: st.integers(-(2**40), 2**40),
    TType.DOUBLE: st.floats(allow_nan=False),
    TType.STRING: st.binary(max_size=8),
    TType.STRUCT: st.sampled_from([[], [[1, TType.BYTE, 1]], [[1, TType.STRING, b""]]]),
}


def _list_site(tree, leaf, whose, field_id):
    """The struct holding the list ``field_id`` of ``whose``, in every
    row group (one, for the FileMetaData's): a SizeStatistics is added
    where a chunk has none."""
    if whose == "file":
        return [tree]
    out = []
    for row_group in footer_oracle.field(tree, 4)[1]:
        if whose == "row_group":
            out.append(row_group)
            continue
        meta = footer_oracle.field(footer_oracle.field(row_group, 1)[1][leaf], 3)
        if meta is None:
            continue
        if whose == "size" and footer_oracle.field(meta, 16) is None:
            footer_oracle.put(meta, 16, TType.STRUCT, [])
        out.append(meta if whose == "meta" else footer_oracle.field(meta, 16))
    return out


@settings(max_examples=300)
@given(st.data())
def test_list_items_of_another_type_are_accepted_or_refused_never_raised_past(data):
    """A list whose header names another item type than the declared one,
    which Thrift's generated readers read as the declared type and a reader
    that follows the header does not: its items drawn of the header's type,
    or, in a chunk's encodings (i32s), one binary whose bytes are chunk
    fields the guard checks, which pyarrow reads as the chunk's own."""
    _name, raw, columns = data.draw(st.sampled_from(_files()), label="file")
    columns = tuple(
        replace(column, nullable=data.draw(st.booleans())) for column in columns
    )
    leaves = len(_leaf_levels(_schema_elements_from_footer(_footer_bytes(raw)[1])))
    leaf = data.draw(st.integers(0, leaves - 1), label="leaf")
    if data.draw(st.booleans(), label="hidden in encodings"):
        (what, value), wire = data.draw(
            st.tuples(FIELD_EDITS, WIRE_VALUES), label="hidden"
        )
        chunk: list = [[3, TType.STRUCT, []]]
        _forge(chunk, what, value, data.draw(SPELLINGS, label="spelling"), wire)
        hidden = footer_oracle.field(chunk, 3)
        _open_then_validate(footer_oracle.hide_in_encodings(raw, leaf, hidden), columns)
        return
    whose, field_id, declared = data.draw(LIST_SITES, label="list")
    kind = data.draw(
        st.sampled_from([kind for kind in ITEMS if kind != declared]), label="items"
    )
    items = data.draw(st.lists(ITEMS[kind], min_size=1, max_size=3), label="values")

    def apply(tree):
        for fields in _list_site(tree, leaf, whose, field_id):
            footer_oracle.put(fields, field_id, TType.LIST, [kind, items])

    _open_then_validate(footer_oracle.rebuild(raw, apply), columns)


def _open_then_validate(forged, columns):
    try:
        parquet = pq.ParquetFile(pa.BufferReader(forged))
        parquet.schema_arrow  # noqa: B018 -- pyarrow builds it lazily
    except (pa.ArrowException, OSError, ValueError):
        return  # a footer pyarrow refuses never reaches the check
    _validate_then_extract(forged, parquet, columns)


#: One forged field of a schema element: a SchemaElement field (type,
#: type_length, repetition_type, name, num_children, converted_type, scale,
#: precision, field_id, logicalType) given another wire type, a second
#: time, or dropped.
ELEMENT_EDITS = st.tuples(
    st.integers(1, 10), st.sampled_from(["mistype", "repeat", "drop"]), WIRE_VALUES
)

_UNIT = st.sampled_from([[[unit, TType.STRUCT, []]] for unit in (1, 2, 3)])

#: A LogicalType union of one member, well formed, of each kind.
LOGICAL_TYPES = st.one_of(
    st.sampled_from(
        [[[member, TType.STRUCT, []]] for member in (1, 2, 3, 4, 6, 11, 12, 13)]
        + [[[member, TType.STRUCT, []]] for member in (14, 15, 17, 18)]
    ),
    st.tuples(st.integers(-1, 40), st.integers(-1, 40)).map(
        lambda sp: [[5, TType.STRUCT, [[1, TType.I32, sp[0]], [2, TType.I32, sp[1]]]]]
    ),
    st.tuples(st.sampled_from([7, 8]), st.booleans(), _UNIT).map(
        lambda t: [
            [t[0], TType.STRUCT, [[1, TType.BOOL, t[1]], [2, TType.STRUCT, t[2]]]]
        ]
    ),
    st.tuples(st.sampled_from([8, 16, 32, 64, 7]), st.booleans()).map(
        lambda t: [[10, TType.STRUCT, [[1, TType.BYTE, t[0]], [2, TType.BOOL, t[1]]]]]
    ),
    st.just([[16, TType.STRUCT, [[1, TType.BYTE, 1]]]]),
)

#: An element's annotation, forged: a converted type (one ConvertedType
#: defines, or just past it), a logical type, a DECIMAL's scale and
#: precision fields, each set or left as it is.
ANNOTATIONS = st.tuples(
    st.none() | st.integers(-1, 22),
    st.none() | LOGICAL_TYPES,
    st.none() | st.integers(-1, 40),
    st.none() | st.integers(-1, 40),
)


@settings(max_examples=300)
@given(st.data())
def test_forged_schema_elements_are_accepted_or_refused_never_raised_past(data):
    """What pyarrow builds the chunks' leaves from, spelled as Thrift's
    generated readers read otherwise than a reader that takes any wire
    type, or last-wins: an i8 repetition_type, a type given twice. A
    histogram is forged beside it, since what aborts is a chunk that no
    longer fits its leaf. And annotations and column orders, which pyarrow
    and parquet-java read apart where the readers' own rules differ (the
    vector file, test_variant_schema's, decides which): whatever is drawn,
    the file is accepted or refused, never raised past."""
    _name, raw, columns = data.draw(st.sampled_from(_files()), label="file")
    columns = tuple(
        replace(column, nullable=data.draw(st.booleans())) for column in columns
    )
    elements = _schema_elements_from_footer(_footer_bytes(raw)[1])
    leaves = len(_leaf_levels(elements))
    edits = data.draw(
        st.lists(
            st.tuples(st.integers(1, len(elements) - 1), ELEMENT_EDITS),
            min_size=1,
            max_size=3,
        ),
        label="element edits",
    )
    annotations = data.draw(
        st.lists(st.tuples(st.integers(1, len(elements) - 1), ANNOTATIONS), max_size=2),
        label="annotations",
    )
    orders = data.draw(
        st.lists(st.tuples(st.integers(0, leaves - 1), st.integers(1, 3)), max_size=2),
        label="column orders",
    )
    twice = data.draw(st.booleans(), label="schema list twice")
    histogram = data.draw(
        st.tuples(st.integers(0, leaves - 1), _LEVELS), label="definition histogram"
    )

    def apply(tree):
        schema = footer_oracle.schema(tree)
        if twice:
            schema = copy.deepcopy(schema)
        for index, (field_id, how, wire) in edits:
            element = schema[index]
            if how == "drop":
                footer_oracle.drop(element, field_id)
            elif how == "repeat":
                footer_oracle.repeat(element, field_id, *wire)
            else:
                footer_oracle.put(element, field_id, *wire)
        for index, (converted, logical, scale, precision) in annotations:
            for field_id, kind, value in (
                (6, TType.I32, converted),
                (7, TType.I32, scale),
                (8, TType.I32, precision),
                (10, TType.STRUCT, logical),
            ):
                if value is not None:
                    footer_oracle.put(schema[index], field_id, kind, value)
        column_orders = footer_oracle.field(tree, 7)
        for leaf, member in orders:
            if column_orders is not None and leaf < len(column_orders[1]):
                column_orders[1][leaf][:] = [[member, TType.STRUCT, []]]
        if twice:
            footer_oracle.repeat(tree, 2, TType.LIST, [TType.STRUCT, schema])
        leaf, levels = histogram
        for row_group in footer_oracle.field(tree, 4)[1]:
            meta = footer_oracle.field(footer_oracle.field(row_group, 1)[1][leaf], 3)
            if meta is not None:
                definition = [[3, TType.LIST, [TType.I64, levels]]]
                footer_oracle.put(meta, 16, TType.STRUCT, definition)

    _open_then_validate(footer_oracle.rebuild(raw, apply), columns)
