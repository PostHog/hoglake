"""Suite F: the VARIANT footer splice, the exact layout check, and the
writer around them.

pyarrow writes a VARIANT column's storage as a plain struct and its
decimal4/decimal8 leaves, viewed as integers, as plain INT32/INT64.
``parquet_schema.stamp_variant_footer`` splices the annotations into the
footer; ``tests/footer_oracle.py``, a generic compact-Thrift codec that
re-encodes the whole footer, is its oracle, and the two must agree byte for
byte. ``variant_layout_fault`` holds the result to the declaration, and
``write_variant_parquet`` does all three for pyhoglake's own files.
"""

from __future__ import annotations

import copy
import io
import re
import struct
import sys
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import footer_oracle
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from hypothesis import given
from hypothesis import strategies as st
from thrift.Thrift import TType

from pyhoglake import variant
from pyhoglake._variant_codec import compile_plan
from pyhoglake.errors import ValidationError
from pyhoglake.models import Column
from pyhoglake.parquet_schema import (
    FooterStamp,
    _footer_bytes,
    _positioned_elements,
    _schema_elements,
    _schema_elements_from_footer,
    _top_level,
    stamp_variant_footer,
    variant_layout_fault,
    variant_stamps,
    write_variant_parquet,
)
from pyhoglake.stats import extract_column_stats

sys.path.insert(0, str(Path(__file__).parent / "variant_conformance"))
import qe_prop_variant_codec as prop
from variant_corpus import DECLARATIONS, golden_table
from variant_helpers import same

DATA = Path(__file__).parent / "data"
#: Files other writers produced: DuckDB's, and parquet-java's (Iceberg's)
#: in parquet-testing.
FOREIGN = (
    sorted(DATA.glob("*.parquet"))
    + sorted((DATA / "parquet-testing" / "shredded_variant").glob("*.parquet"))[:4]
)

DECIMALS = {
    "type": "object",
    "fields": [
        {"name": "p", "type": "decimal4", "precision": 9, "scale": 2},
        {"name": "q", "type": "decimal8", "precision": 3, "scale": 0},
        {"name": "a.b", "type": "decimal8", "precision": 18, "scale": 18},
        {
            "name": "arr",
            "type": "array",
            "element": {"type": "decimal4", "precision": 1, "scale": 1},
        },
        {"name": "w", "type": "decimal16", "precision": 10, "scale": 2},
    ],
}
DECIMAL_ROWS = [
    {
        "p": Decimal("1234567.89"),
        "q": Decimal(-999),
        "a.b": Decimal("0.1" + "0" * 17),
    },
    {"arr": [Decimal("0.5"), None, Decimal("-0.9")], "w": Decimal("12345678.90")},
    None,
    {"p": "not a decimal", "q": Decimal("1.5")},
]


def _column(name, field_id, decl, *, nullable=True):
    params = None if decl is None else {"shredding": decl}
    return Column(name, "variant", field_id, field_id - 1, nullable, type_params=params)


def _plan(column):
    return compile_plan((column.type_params or {}).get("shredding"))


def _storage(column, rows):
    return variant.encode_python(rows, column).array


def _table(columns, arrays, extra=()):
    """A table of variant storage in the write form, field ids on."""
    fields = [
        pa.field(
            column.name,
            array.type,
            nullable=column.nullable,
            metadata={b"PARQUET:field_id": str(column.field_id).encode()},
        )
        for column, array in zip(columns, arrays, strict=True)
    ]
    data = list(arrays)
    for field, array in extra:
        fields.append(field)
        data.append(array)
    return pa.table(data, schema=pa.schema(fields))


def _integer_view(kind):
    """``kind`` with its decimal32/decimal64 leaves as int32/int64: what the
    writer hands pyarrow. Written apart from parquet_schema's own."""
    if pa.types.is_decimal32(kind):
        return pa.int32()
    if pa.types.is_decimal64(kind):
        return pa.int64()
    if pa.types.is_struct(kind):
        return pa.struct([f.with_type(_integer_view(f.type)) for f in kind])
    if pa.types.is_list(kind):
        return pa.list_(
            kind.value_field.with_type(_integer_view(kind.value_field.type))
        )
    return kind


def _pyarrow_bytes(table, **options):
    """What pyarrow writes for ``table`` with its decimal leaves viewed as
    integers and no Arrow schema stored: the splice's input."""
    fields, data = [], []
    for field in table.schema:
        kind = _integer_view(field.type)
        fields.append(field.with_type(kind))
        chunks = table.column(field.name).chunks
        data.append(pa.chunked_array([chunk.view(kind) for chunk in chunks], kind))
    sink = pa.BufferOutputStream()
    pq.write_table(
        pa.table(data, schema=pa.schema(fields)), sink, store_schema=False, **options
    )
    return sink.getvalue().to_pybytes()


def _stamps(columns):
    return [
        stamp for column in columns for stamp in variant_stamps(column, _plan(column))
    ]


def _corpus():
    """golden_table()'s every declaration and edge row, as one table."""
    table = golden_table()
    columns = [
        _column(name, index + 1, DECLARATIONS[name.split("/", 1)[1]])
        for index, name in enumerate(table.column_names)
    ]
    return _table(columns, [table.column(c.name) for c in columns]), columns


def _footer(raw):
    return footer_oracle.split(raw)[1]


def _splice(raw, stamps):
    """``raw`` stamped, as one file: stamp_variant_footer returns where the
    footer starts and what replaces it, the body being the caller's."""
    start, tail, elements = stamp_variant_footer(raw, stamps)
    return bytes(raw[:start]) + tail, elements


def _one_decimal():
    column = _column("v", 3, {"type": "decimal4", "precision": 9, "scale": 2})
    raw = _pyarrow_bytes(_table([column], [_storage(column, [Decimal("1.00")])]))
    return column, raw


def _written(column, rows):
    """pyhoglake's own file of ``rows``, and its schema elements."""
    table = _table([column], [_storage(column, rows)])
    written = write_variant_parquet(table, [column])
    return written, _schema_elements(written.body)


# -- reading the footer --------------------------------------------------------


@pytest.mark.parametrize(
    "form", [bytes, bytearray, memoryview, pa.py_buffer], ids=lambda f: f.__name__
)
def test_footer_bytes_reads_a_path_and_every_in_memory_form_alike(tmp_path, form):
    _, raw = _one_decimal()
    path = tmp_path / "f.parquet"
    path.write_bytes(raw)
    start, footer = footer_oracle.split(raw)
    assert _footer_bytes(str(path)) == (start, footer)
    assert _footer_bytes(path) == (start, footer)
    assert _footer_bytes(form(raw)) == (start, footer)
    with io.BytesIO(raw).getbuffer() as view:
        assert _footer_bytes(view) == (start, footer)
    assert _schema_elements(str(path)) == _schema_elements(form(raw))


@pytest.mark.parametrize(
    ("tail", "message"), [(b"PARE", "encrypted"), (b"PAR0", "invalid or oversized")]
)
def test_footer_bytes_refuses_anything_but_a_plaintext_footer(tmp_path, tail, message):
    _, raw = _one_decimal()
    path = tmp_path / "f.parquet"
    path.write_bytes(raw[:-4] + tail)
    for source in (raw[:-4] + tail, path):
        with pytest.raises(ValueError, match=message):
            _footer_bytes(source)


@pytest.mark.parametrize("length", [2**32 - 1, 64 * 1024 * 1024 + 1, 101])
def test_footer_bytes_refuses_a_length_longer_than_the_file(length):
    """101 of these 112 bytes would reach into the leading magic."""
    raw = b"PAR1" + b"\x00" * 100 + struct.pack("<I", length) + b"PAR1"
    with pytest.raises(ValueError, match="oversized"):
        _footer_bytes(raw)
    # Everything between the magics is the most a footer can be.
    raw = b"PAR1" + b"\x01" * 100 + struct.pack("<I", 100) + b"PAR1"
    assert _footer_bytes(raw) == (4, b"\x01" * 100)
    with pytest.raises(ValueError, match="invalid or oversized"):
        _footer_bytes(b"PAR1")


def test_footer_bytes_refuses_a_file_shorter_than_its_trailer(tmp_path):
    """Under 8 bytes there is no trailer to read: in memory, the length
    would not unpack (struct.error, not a ValueError); by path, the seek
    would raise OSError. Neither is what the callers catch."""
    path = tmp_path / "short.parquet"
    for short in (b"", b"PAR"):
        path.write_bytes(short)
        for source in (short, path):
            with pytest.raises(ValueError, match="invalid or oversized"):
                _footer_bytes(source)
    path.write_bytes(b"PAR1")
    with pytest.raises(ValueError, match="invalid or oversized"):
        _footer_bytes(path)


def test_footer_bytes_refuses_a_footer_over_64_mib_in_a_file_that_holds_it():
    """The footer is read whole before any of it is checked, so its length
    is capped whatever the file's size."""
    cap = 64 * 1024 * 1024
    raw = bytearray(cap + 13)
    raw[:4] = raw[-4:] = b"PAR1"
    raw[-8:-4] = struct.pack("<I", cap + 1)
    with pytest.raises(ValueError, match="oversized"):
        _footer_bytes(raw)
    raw[-8:-4] = struct.pack("<I", cap)
    assert _footer_bytes(raw)[0] == 5


@pytest.mark.parametrize("which", ["corpus", *range(len(FOREIGN))])
def test_positions_place_every_stop_and_path_as_the_oracle_reads_them(which):
    """STOP offsets, last field ids and tuple paths, against the oracle's
    own reading: its recursive walk for the paths, and its decoded fields
    for the last id."""
    raw = _pyarrow_bytes(_corpus()[0]) if which == "corpus" else FOREIGN[which]
    footer = _footer(raw if which == "corpus" else raw.read_bytes())
    elements = _positioned_elements(footer)
    tree = footer_oracle.schema(footer_oracle.decode(footer))
    assert [e.path for e in elements] == footer_oracle.paths(tree)
    assert [e.last for e in elements] == [entry[-1][0] for entry in tree]
    assert all(footer[e.stop] == 0 for e in elements)
    # The positions are an addition: the fields read the same either way.
    assert [e.fields for e in elements] == _schema_elements_from_footer(footer)


def test_paths_are_tuples_so_a_dotted_name_is_one_name():
    column = _column("v", 1, DECIMALS)
    raw = _pyarrow_bytes(_table([column], [_storage(column, DECIMAL_ROWS)]))
    paths = [e.path for e in _positioned_elements(_footer(raw))]
    assert ("v", "typed_value", "a.b", "typed_value") in paths
    assert ("v", "typed_value", "a", "b", "typed_value") not in paths


def _over_claiming_root(tree):
    root = footer_oracle.schema(tree)[0]
    footer_oracle.put(root, 5, TType.I32, footer_oracle.field(root, 5) + 1)


def _trailing_element(tree):
    extra = [[1, TType.I32, 6], [3, TType.I32, 1], [4, TType.STRING, b"extra"]]
    footer_oracle.schema(tree).append(extra)


def _negative_children(tree):
    footer_oracle.put(footer_oracle.schema(tree)[2], 5, TType.I32, -1)


def _text_children(tree):
    footer_oracle.put(footer_oracle.schema(tree)[2], 5, TType.STRING, b"1")


def _numbered_name(tree):
    footer_oracle.put(footer_oracle.schema(tree)[2], 4, TType.I32, 1)


@pytest.mark.parametrize(
    ("edit", "message"),
    [
        (_over_claiming_root, "truncated Parquet schema"),
        (_trailing_element, "invalid Parquet schema tree"),
        (_negative_children, "negative schema child count"),
        (_text_children, "negative schema child count"),
        (_numbered_name, "invalid Parquet schema tree"),
    ],
    ids=[
        "over_claiming_root",
        "trailing_element",
        "negative_children",
        "text_children",
        "numbered_name",
    ],
)
def test_positions_refuse_a_schema_tree_that_does_not_close(edit, message):
    """The splice places its bytes by these paths: a tree that claims more
    elements than it holds, holds more than it claims, counts children
    below zero or in anything but an i32, or names an element with
    anything but a string has none to trust."""
    column, raw = _one_decimal()
    forged = footer_oracle.rebuild(raw, edit)
    with pytest.raises(ValueError, match=message):
        _positioned_elements(_footer(forged))
    with pytest.raises(ValueError, match=message):
        stamp_variant_footer(forged, _stamps([column]))


@pytest.mark.parametrize(
    ("arrow", "logical"),
    [
        (pa.timestamp("us", "UTC"), {8: {1: True, 2: {2: {}}}}),
        (pa.timestamp("us"), {8: {1: False, 2: {2: {}}}}),
        (pa.time64("us"), {7: {1: False, 2: {2: {}}}}),
        (pa.int8(), {10: {1: 8, 2: True}}),
        (pa.uint8(), {10: {1: 8, 2: False}}),
    ],
    ids=str,
)
def test_bool_fields_of_a_logical_type_are_read(arrow, logical):
    """isAdjustedToUTC and isSigned are Thrift BOOLs, which the schema
    reader skipped: TIMESTAMP(true) and TIMESTAMP(false) read alike, and so
    did INT(8, true) and UINT8."""
    sink = pa.BufferOutputStream()
    pq.write_table(pa.table({"c": pa.array([None], arrow)}), sink)
    (_, element) = _schema_elements(sink.getvalue())
    assert element[10] == logical


# -- the splice against the oracle -------------------------------------------


def _decimal_table():
    column = _column("v", 7, DECIMALS)
    return _table([column], [_storage(column, DECIMAL_ROWS)]), [column]


def _many_columns():
    columns = [
        _column("a", 1, None),
        _column("b", 2, DECLARATIONS["posthog"], nullable=False),
        _column("c", 3, DECIMALS),
        _column("d", 4, {"type": "variant"}, nullable=False),
    ]
    rows = [
        [None, {"x": 1}],
        [{"$browser": "Chrome", "price": Decimal("9.99")}, {"payload": [1]}],
        DECIMAL_ROWS[:2],
        [1, "two"],
    ]
    arrays = [_storage(c, r) for c, r in zip(columns, rows, strict=True)]
    scalar = pa.field("n", pa.int64(), metadata={b"PARQUET:field_id": b"5"})
    return _table(columns, arrays, [(scalar, pa.array([1, 2], pa.int64()))]), columns


def _wide():
    """A footer of about 400 KB: 650 scalar columns beside two variants,
    over four row groups."""
    columns = [_column("v", 1, DECIMALS), _column("w", 2, DECLARATIONS["events"])]
    rows = [DECIMAL_ROWS, [[{"type": "x", "ids": [1]}], None, "s", []]]
    arrays = [_storage(c, r) for c, r in zip(columns, rows, strict=True)]
    extra = [
        (
            pa.field(
                f"scalar_column_with_a_long_name_{i:04d}",
                pa.int64(),
                metadata={b"PARQUET:field_id": str(i + 3).encode()},
            ),
            pa.array(range(4), pa.int64()),
        )
        for i in range(650)
    ]
    return _table(columns, arrays, extra), columns


SCENARIOS = {
    "corpus": (_corpus, {}),
    "page_index": (_corpus, {"write_page_index": True}),
    "zstd": (_corpus, {"compression": "zstd"}),
    "five_row_groups": (_corpus, {"row_group_size": 7}),
    "many_columns": (_many_columns, {}),
    "decimals": (_decimal_table, {}),
    "decimals_page_index_small_pages": (
        _decimal_table,
        {"write_page_index": True, "data_page_size": 64},
    ),
    "wide_footer": (_wide, {"row_group_size": 1}),
}


@pytest.mark.parametrize("scenario", SCENARIOS)
def test_splice_equals_the_generic_oracle_byte_for_byte(scenario):
    build, options = SCENARIOS[scenario]
    table, columns = build()
    raw = _pyarrow_bytes(table, **options)
    stamps = _stamps(columns)
    patched, elements = _splice(raw, stamps)
    assert patched == footer_oracle.stamp(raw, stamps)
    start, footer = footer_oracle.split(raw)
    # The body is untouched: every offset into it (column chunks, page
    # indexes) still holds.
    assert patched[:start] == raw[:start]
    groups = sum(s.kind == "variant" for s in stamps)
    decimals = len(stamps) - groups
    assert len(_footer(patched)) == len(footer) + 7 * groups + 14 * decimals
    # The elements it returns without reading the patch again are the
    # patch's.
    assert elements == _schema_elements(patched)
    if scenario == "five_row_groups":
        assert pq.ParquetFile(pa.BufferReader(patched)).metadata.num_row_groups == 5
    if scenario == "wide_footer":
        assert len(footer) > 390_000
    pq.read_table(pa.BufferReader(patched))


def test_the_splice_takes_its_stamps_in_any_order():
    """stamp_variant_footer takes any sequence of stamps: one column's in
    reverse, and two columns' interleaved backwards, splice as in schema
    order. Spliced in the order given, a later offset first, the pieces
    between them would repeat a stretch of the footer."""
    table, columns = _many_columns()
    raw = _pyarrow_bytes(table)
    stamps = _stamps(columns)
    assert len(stamps) > 2
    expected = footer_oracle.stamp(raw, stamps)
    assert _splice(raw, stamps[::-1])[0] == expected
    a, b = _stamps(columns[2:3]), _stamps(columns[1:2])
    assert _splice(raw, [*a[::-1], *b[::-1]])[0] == footer_oracle.stamp(raw, [*b, *a])


def test_the_variant_bytes_follow_the_field_id():
    """pyarrow writes a group's type-less fields 3, 4, 5 and 9, so field 10
    is a delta of 1: ``1C``, then LogicalType's VARIANT (field 16, long
    form ``0C 20``), VariantType{1: i8 1} (``13 01``), two STOPs."""
    column = _column("v", 9, None)
    raw = _pyarrow_bytes(_table([column], [_storage(column, [1])]))
    footer = _footer(raw)
    (group,) = [e for e in _positioned_elements(footer) if e.path == ("v",)]
    assert group.last == 9
    patched = _footer(_splice(raw, _stamps([column]))[0])
    blob = bytes.fromhex("1C 0C 20 13 01 00 00")
    assert patched == footer[: group.stop] + blob + footer[group.stop :]


def test_the_decimal_bytes_follow_the_name():
    """A leaf's fields are 1, 3 and 4, so converted_type (6) is a delta of
    2; precision and scale are zigzag varints of one byte each."""
    column = _column("v", 1, {"type": "decimal8", "precision": 18, "scale": 7})
    raw = _pyarrow_bytes(_table([column], [_storage(column, [Decimal("1.0000000")])]))
    footer = _footer(raw)
    (leaf,) = [
        e for e in _positioned_elements(footer) if e.path == ("v", "typed_value")
    ]
    assert leaf.last == 4
    patched = _footer(_splice(raw, _stamps([column]))[0])
    blob = bytes.fromhex("25 0A 15 0E 15 24 2C 5C 15 0E 15 24 00 00")
    # The group's 7 bytes come before the leaf's.
    assert patched[leaf.stop + 7 : leaf.stop + 21] == blob


def test_a_decimal_leaf_ending_at_another_field_is_spliced_as_the_oracle_does():
    """A leaf whose last field is num_children (5), not pyarrow's
    repetition-and-name (4): the splice's first byte is a delta from that
    last field, and only the generic re-encoding says what it must be."""
    column, raw = _one_decimal()
    forged = footer_oracle.edit_element(
        raw, ("v", "typed_value"), lambda f: footer_oracle.put(f, 5, TType.I32, 0)
    )
    (leaf,) = [
        e
        for e in _positioned_elements(_footer(forged))
        if e.path == ("v", "typed_value")
    ]
    assert leaf.last == 5
    stamps = _stamps([column])
    assert _splice(forged, stamps)[0] == footer_oracle.stamp(forged, stamps)


@pytest.mark.parametrize("path", FOREIGN, ids=lambda p: p.name)
def test_an_empty_splice_leaves_a_footer_alone(path):
    """A splice of nothing changes nothing, on what other writers produce
    too (DuckDB's, and parquet-java's in parquet-testing)."""
    raw = path.read_bytes()
    assert _splice(raw, [])[0] == raw


# -- the splice's refusals ---------------------------------------------------


def _refused(raw, stamps, message):
    with pytest.raises(ValueError, match=message):
        stamp_variant_footer(raw, stamps)


def test_refuses_a_footer_that_is_not_plaintext_parquet():
    column, raw = _one_decimal()
    _refused(raw[:-4] + b"PARE", _stamps([column]), "encrypted")
    _refused(raw[:-4] + b"XXXX", _stamps([column]), "invalid")


def test_refuses_a_group_already_annotated():
    """A pyarrow that started writing VARIANT itself stops the writes
    rather than gets a second annotation. DuckDB's file is such a group,
    and so is a file stamped once already."""
    duckdb = (DATA / "native_variant.parquet").read_bytes()
    _refused(duckdb, [FooterStamp(("properties",), "variant", 2)], "already annotated")
    column, raw = _one_decimal()
    once, _ = _splice(raw, _stamps([column]))
    _refused(once, _stamps([column])[:1], "already annotated")
    _refused(once, _stamps([column])[1:], r"already has fields \[6, 7, 8, 10\]")


def test_refuses_a_group_with_a_converted_type():
    column, raw = _one_decimal()
    forged = footer_oracle.edit_element(
        raw, ("v",), lambda f: footer_oracle.put(f, 6, TType.I32, 1)
    )
    _refused(forged, _stamps([column])[:1], "already annotated")


def test_refuses_a_group_whose_field_id_is_not_the_columns():
    column, raw = _one_decimal()
    _refused(raw, [FooterStamp(("v",), "variant", 4)], "field id 3, not 4")
    forged = footer_oracle.edit_element(raw, ("v",), lambda f: footer_oracle.drop(f, 9))
    _refused(forged, _stamps([column])[:1], "field id None, not 3")


def test_refuses_a_group_with_a_field_after_its_field_id():
    """Field 10 is encoded as a delta from the group's last field; one
    after the field id would make the delta another field."""
    column, raw = _one_decimal()
    forged = footer_oracle.edit_element(
        raw, ("v",), lambda f: footer_oracle.put(f, 11, TType.I32, 0)
    )
    _refused(forged, _stamps([column])[:1], "field 11 after its field id")
    # Nor a lower one: the compact protocol lets a writer put fields out of
    # order, and a delta of 1 from num_children would add field 6.
    forged = footer_oracle.edit_element(
        raw, ("v",), lambda f: f.sort(key=lambda entry: entry[0] != 9)
    )
    assert _schema_elements(forged)[1][9] == 3
    _refused(forged, _stamps([column])[:1], "field 5 after its field id")


def test_a_field_the_reader_skips_still_counts_as_the_last():
    """The schema reader skips a field of a type it does not keep (an i64
    here, where parquet.thrift declares a struct or an i32). Skipped, it
    still moves the compact protocol's delta base, so a splice after it
    would add another field than the one it means to."""
    column, raw = _one_decimal()
    forged = footer_oracle.edit_element(
        raw, ("v",), lambda f: footer_oracle.put(f, 10, TType.I64, 0)
    )
    assert 10 not in _schema_elements(forged)[1]
    _refused(forged, _stamps([column])[:1], "field 10 after its field id")
    forged = footer_oracle.edit_element(
        raw, ("v", "typed_value"), lambda f: footer_oracle.put(f, 6, TType.I64, 0)
    )
    assert 6 not in _schema_elements(forged)[-1]
    _refused(forged, _stamps([column])[1:], "field 6 after num_children")


def test_refuses_a_variant_stamp_on_anything_but_a_type_less_group():
    column, raw = _one_decimal()
    _refused(raw, [FooterStamp(("v", "metadata"), "variant", 3)], "not a group")
    # Nor on an element with neither a type nor children.
    forged = footer_oracle.edit_element(
        raw, ("v", "metadata"), lambda f: footer_oracle.drop(f, 1)
    )
    _refused(forged, [FooterStamp(("v", "metadata"), "variant", 3)], "not a group")
    forged = footer_oracle.edit_element(
        raw, ("v",), lambda f: footer_oracle.put(f, 1, TType.I32, 6)
    )
    _refused(forged, _stamps([column])[:1], "not a group")


@pytest.mark.parametrize(
    ("path", "kind", "message"),
    [
        (("v", "typed_value"), "decimal8", "physical type 1, not 2"),
        (("v", "metadata"), "decimal4", "physical type 6, not 1"),
        (("v",), "decimal4", "physical type None, not 1"),
    ],
)
def test_refuses_a_decimal_on_a_leaf_of_another_physical_type(path, kind, message):
    _, raw = _one_decimal()
    _refused(raw, [FooterStamp(path, kind, precision=9, scale=2)], message)


def test_a_refusal_spells_a_dotted_name_as_one_name():
    column = _column("v", 1, DECIMALS)
    raw = _pyarrow_bytes(_table([column], [_storage(column, DECIMAL_ROWS)]))
    stamp = FooterStamp(
        ("v", "typed_value", "a.b", "typed_value"), "decimal4", precision=9, scale=2
    )
    _refused(raw, [stamp], re.escape('v.typed_value["a.b"].typed_value is physical'))


def test_refuses_a_decimal_leaf_with_a_field_id():
    column, raw = _one_decimal()
    forged = footer_oracle.edit_element(
        raw, ("v", "typed_value"), lambda f: footer_oracle.put(f, 9, TType.I32, 77)
    )
    _refused(forged, _stamps([column])[1:], "has a field id")


@pytest.mark.parametrize("field_id", [6, 7, 8, 10])
def test_refuses_a_decimal_leaf_already_carrying_part_of_one(field_id):
    column, raw = _one_decimal()
    value = [[1, TType.STRUCT, []]] if field_id == 10 else 2
    kind = TType.STRUCT if field_id == 10 else TType.I32
    forged = footer_oracle.edit_element(
        raw, ("v", "typed_value"), lambda f: footer_oracle.put(f, field_id, kind, value)
    )
    _refused(forged, _stamps([column])[1:], rf"already has fields \[{field_id}\]")


def test_refuses_a_decimal_leaf_with_a_later_field():
    column, raw = _one_decimal()
    forged = footer_oracle.edit_element(
        raw, ("v", "typed_value"), lambda f: footer_oracle.put(f, 12, TType.I32, 0)
    )
    _refused(forged, _stamps([column])[1:], "field 12 after num_children")


@pytest.mark.parametrize(
    ("kind", "precision", "scale"),
    [
        ("decimal4", 10, 0),
        ("decimal8", 19, 0),
        ("decimal4", 0, 0),
        ("decimal4", 2, 3),
        ("decimal4", 9, -1),
    ],
)
def test_refuses_a_decimal_its_width_cannot_hold(kind, precision, scale):
    _, raw = _one_decimal()
    if kind == "decimal8":
        column = _column("v", 3, {"type": "decimal8", "precision": 18, "scale": 0})
        raw = _pyarrow_bytes(_table([column], [_storage(column, [Decimal(1)])]))
    stamp = FooterStamp(("v", "typed_value"), kind, precision=precision, scale=scale)
    _refused(raw, [stamp], f"is not a {kind}")


@pytest.mark.parametrize("kind", ["decimal", "decimal16", "DECIMAL8", "varaint"])
def test_refuses_a_stamp_of_a_kind_it_does_not_know(kind):
    """Each would be stamped as a decimal8: a typo would stamp a type."""
    with pytest.raises(ValueError, match=f"x: no stamp of kind '{kind}'"):
        FooterStamp(("x",), kind, precision=10, scale=2)


def test_a_stamp_carries_a_field_id_if_and_only_if_it_stamps_a_group():
    """A group's field id is checked against the stamp's, so a stamp
    without one would pass a group without one; a leaf's is ignored."""
    for field_id in (None, True, "3"):
        with pytest.raises(ValueError, match="v: a variant stamp with field id"):
            FooterStamp(("v",), "variant", field_id)
    with pytest.raises(ValueError, match=r"v\.x: a decimal4 stamp with field id 5"):
        FooterStamp(("v", "x"), "decimal4", 5, precision=9, scale=2)


def test_refuses_a_path_absent_or_present_twice():
    column, raw = _one_decimal()
    _refused(raw, [FooterStamp(("w",), "variant", 3)], "0 times")
    # The root is never in the splice's search: refused when the stamp is made.
    for kind in ("variant", "bogus"):
        with pytest.raises(ValueError, match="a stamp needs a path below the root"):
            FooterStamp((), kind, 3)
    # Two children of one name: Parquet's schema allows it.
    twice = footer_oracle.edit_element(
        raw,
        ("v", "value"),
        lambda f: footer_oracle.put(f, 4, TType.STRING, b"metadata"),
    )
    stamp = FooterStamp(("v", "metadata"), "decimal4", precision=9, scale=2)
    _refused(twice, [stamp], "2 times")
    stamps = _stamps([column])
    _refused(raw, [*stamps, stamps[0]], "stamped twice")


def test_a_stamp_of_the_wrong_length_is_refused(monkeypatch):
    import pyhoglake.parquet_schema as ps

    column, raw = _one_decimal()
    real = ps._annotation
    monkeypatch.setattr(ps, "_annotation", lambda s, e: real(s, e) + b"\x00")
    _refused(raw, _stamps([column]), "wrong length")


# -- canaries: pyarrow learning VARIANT --------------------------------------


def test_canary_pyarrow_writes_no_variant_annotation_and_plain_integer_leaves():
    """If this fails, pyarrow has started writing VARIANT (or DECIMAL on
    the integer view) itself, and the splice now refuses every write as
    "already annotated": revisit stamp_variant_footer (apache/arrow#50131,
    #50132, #50252)."""
    column = _column("v", 1, DECIMALS)
    raw = _pyarrow_bytes(_table([column], [_storage(column, DECIMAL_ROWS)]))
    elements = {e.path: e.fields for e in _positioned_elements(_footer(raw))}
    assert not {6, 10} & set(elements[("v",)])
    for path, _, _ in _plan(column).stamps:
        leaf = elements[("v", *path)]
        assert set(leaf) == {1, 3, 4}, f"pyarrow {pa.__version__} annotates {path}"


@pytest.mark.parametrize("source", ["pyhoglake", "duckdb"])
def test_canary_pyarrow_reads_a_variant_group_as_its_plain_struct(source):
    """If this fails, pyarrow surfaces VARIANT as an extension type (or as
    something else again): the plans' read forms, and every reader of a
    VARIANT column's storage, need revisiting. Never register a Python
    extension type named arrow.parquet.variant to make it pass: that
    crashed the process."""
    if source == "duckdb":
        raw = (DATA / "native_variant.parquet").read_bytes()
    else:
        table, columns = _many_columns()
        raw = write_variant_parquet(table, columns).body
    with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
        read = parquet.schema_arrow
        back = parquet.read()
    for schema in (read, back.schema):
        for field in schema:
            assert not isinstance(field.type, pa.BaseExtensionType), field
            assert pa.types.is_struct(field.type) or field.name in ("id", "n")
    if source == "pyhoglake":
        for column in columns:
            assert read.field(column.name).type == _plan(column).read_type


# -- write_variant_parquet -----------------------------------------------------


def test_write_variant_parquet_round_trips_the_corpus_and_measures_after_the_patch():
    table, columns = _corpus()
    written = write_variant_parquet(table, columns)
    body = written.body.to_pybytes()
    assert written.size == len(body)
    assert written.footer_size == struct.unpack("<I", body[-8:-4])[0]
    # Measured on the patched footer, not pyarrow's: they differ by the stamps.
    unpatched = _pyarrow_bytes(table)
    stamps = _stamps(columns)
    extra = 7 * len(columns) + 14 * (len(stamps) - len(columns))
    assert written.footer_size == len(_footer(unpatched)) + extra
    assert written.size == len(unpatched) + extra
    assert body == footer_oracle.stamp(unpatched, stamps)
    # A tail read of [size - footer_size - 8, size) is the whole footer.
    tail = body[written.size - written.footer_size - 8 :]
    assert _schema_elements_from_footer(tail[:-8])[1][10] == {16: {1: 1}}
    back = pq.read_table(pa.BufferReader(body))
    for column in columns:
        assert back.schema.field(column.name).type == _plan(column).read_type
        # repr, which tells NaN from NaN as equal and -0.0 from 0.0 apart.
        got = variant.to_python(back.column(column.name), column)
        want = variant.to_python(table.column(column.name), column)
        assert repr(got) == repr(want), column.name


def test_write_variant_parquet_metadata_is_the_files_for_the_stats():
    """The metadata is the writer's own, collected as it closed, not read
    off the body: its chunk paths, value counts, sizes and null counts are
    the stamped file's, and so are the stats pyhoglake registers. The decoded
    statistics of the decimal leaves are not (no DECIMAL annotation there:
    see VariantParquet.metadata)."""
    table, columns = _many_columns()
    written = write_variant_parquet(table, columns, row_group_size=1)
    read = pq.read_metadata(pa.BufferReader(written.body))
    catalog = (*columns, Column("n", "long", 5, 4, True))
    stats = extract_column_stats(written.metadata, catalog)
    assert stats == extract_column_stats(read, catalog)
    assert [s.field_id for s in stats] == [5]
    assert stats[0].lower_bound == struct.pack("<q", 1)
    assert written.metadata.num_rows == read.num_rows == 2
    assert written.metadata.num_row_groups == read.num_row_groups == 2
    for group in range(read.num_row_groups):
        mine, theirs = written.metadata.row_group(group), read.row_group(group)
        for chunk in range(read.num_columns):
            a, b = mine.column(chunk), theirs.column(chunk)
            assert (a.path_in_schema, a.num_values, a.total_compressed_size) == (
                b.path_in_schema,
                b.num_values,
                b.total_compressed_size,
            )
            assert a.statistics.null_count == b.statistics.null_count


def test_write_variant_parquet_takes_the_write_options_and_row_group_size():
    """The codec options are parquet_write_options', as on the other write
    paths, and none of them can store the Arrow schema: a stored one
    describes the group as the plain struct with integer leaves, and
    pyarrow reads that back instead of the footer."""
    from pyhoglake import parquet_write_options

    table, columns = _decimal_table()
    options = {**parquet_write_options("zstd", 3), "store_schema": True}
    written = write_variant_parquet(
        table, columns, row_group_size=2, write_options=options
    )
    metadata = pq.read_metadata(pa.BufferReader(written.body))
    assert metadata.num_row_groups == 2
    assert metadata.row_group(0).column(0).compression == "ZSTD"
    assert b"ARROW:schema" not in (metadata.metadata or {})
    plain = write_variant_parquet(table, columns)
    assert plain.metadata.row_group(0).column(0).compression == "SNAPPY"


def test_write_variant_parquet_writes_a_not_null_column_as_a_required_group():
    column = _column("v", 2, DECLARATIONS["posthog"], nullable=False)
    _, elements = _written(column, [{"$browser": "x"}, 1])
    assert elements[1][3] == 0


def test_write_variant_parquet_refuses_sql_null_rows_in_a_not_null_column():
    """Storage built elsewhere can hold SQL NULL (the encoder refuses it for
    a NOT NULL column). pyarrow writes it into the REQUIRED group as an
    empty metadata, an invalid variant, and the layout is right, so the
    splice and the layout check would both pass it."""
    for decl in (None, DECIMALS):
        nullable = _column("v", 2, decl)
        column = replace(nullable, nullable=False)
        storage = _storage(nullable, [1, None, {"p": Decimal("1.00")}])
        table = _table([column], [storage])
        with pytest.raises(ValueError, match="v is NOT NULL but has SQL NULL rows"):
            write_variant_parquet(table, [column])
        table = _table([nullable], [storage])
        write_variant_parquet(table, [nullable])


def _null_below(storage, kind):
    """``storage`` with its first list element group or its object field
    group ``a`` masked SQL NULL: REQUIRED groups, written by pyarrow as the
    masked children's values."""
    typed = storage.field("typed_value")
    if kind == "element":
        group = typed.values
        mask = [i == 1 for i in range(len(group))]
    else:
        group = typed.field("a")
        mask = [True] * len(group)
    nulled = pa.StructArray.from_arrays(
        group.flatten(), fields=list(group.type), mask=pa.array(mask)
    )
    if kind == "element":
        typed = pa.ListArray.from_arrays(typed.offsets, nulled, type=typed.type)
    else:
        typed = pa.StructArray.from_arrays([nulled], fields=list(typed.type))
    return pa.StructArray.from_arrays(
        [storage.field("metadata"), storage.field("value"), typed],
        fields=list(storage.type),
    )


def _null_metadata(storage):
    """``storage`` with the REQUIRED metadata leaf of its present first row
    null, which pyarrow would write as empty bytes."""
    metadata = storage.field("metadata").to_pylist()
    return pa.StructArray.from_arrays(
        [pa.array([None, *metadata[1:]], pa.binary()), storage.field("value")],
        fields=list(storage.type),
    )


@pytest.mark.parametrize(
    ("kind", "decl", "rows", "fault"),
    [
        (
            "element",
            {"type": "array", "element": {"type": "int64"}},
            [[1, 2]],
            r"v: typed_value\.element is null under a present list",
        ),
        (
            "field",
            {"type": "object", "fields": [{"name": "a", "type": "int64"}]},
            [{"a": 7}],
            r"v: typed_value\.a is null under a present parent",
        ),
        ("metadata", None, [1, "x"], "v: metadata is null under a present parent"),
    ],
)
def test_write_variant_parquet_refuses_a_null_in_a_required_field_of_a_present_row(
    kind, decl, rows, fault
):
    """No decimal leaf, so the integer view leaves the type as it is. The
    null would read back as the masked value ([1, 2] for [1, None], {"a": 7}
    for {}) or as empty metadata; the refusal names where it is."""
    column = _column("v", 1, decl)
    storage = _storage(column, rows)
    broken = (
        _null_metadata(storage) if kind == "metadata" else _null_below(storage, kind)
    )
    table = _table([column], [broken])
    with pytest.raises(ValueError, match=fault):
        write_variant_parquet(table, [column])
    write_variant_parquet(_table([column], [storage]), [column])


@pytest.mark.parametrize(
    "decl", [None, {"type": "object", "fields": [{"name": "a", "type": "int64"}]}]
)
@pytest.mark.parametrize("build", ["take", "nulls", "slice"])
def test_write_variant_parquet_refuses_nulls_under_a_sql_null_row(decl, build):
    """Arrow's take on a null index and pa.nulls null every child of a SQL
    NULL row, REQUIRED ones too, which the view refuses (pq.write_table
    refuses them as well): the message says where the null is, not a null
    below the top of a present row. A slice is refused for such a row
    outside its own rows, since the view looks at a child's whole buffer.
    The encoder's own SQL NULL rows hold placeholders, and are written."""
    column = _column("v", 1, decl)
    storage = _storage(column, [1, None])
    if build == "take":
        broken = storage.take(pa.array([0, None]))
    elif build == "nulls":
        broken = pa.nulls(2, storage.type)
    else:
        broken = pa.concat_arrays([storage, pa.nulls(1, storage.type)]).slice(0, 2)
    with pytest.raises(
        ValueError,
        match="v has a null in a non-nullable field under a SQL NULL row or "
        "outside its rows",
    ):
        write_variant_parquet(_table([column], [broken]), [column])
    written = write_variant_parquet(_table([column], [storage]), [column])
    back = pq.read_table(pa.BufferReader(written.body)).column("v")
    assert variant.to_python(back) == [1, None]


def test_write_variant_parquet_views_sliced_storage_at_its_offset():
    """A group of a partitioned append is a slice of the caller's table:
    the integer view must keep the slice's offset into the decimal leaves."""
    column = _column("v", 1, DECIMALS)
    whole = _table([column], [_storage(column, DECIMAL_ROWS * 3)])
    part = whole.slice(5, 4)
    written = write_variant_parquet(part, [column])
    back = pq.read_table(pa.BufferReader(written.body)).column("v")
    assert variant.to_python(back) == variant.to_python(part.column("v"))


def test_write_variant_parquet_writes_every_chunk_of_a_chunked_column():
    """Tables from Table.from_batches, concat_tables or a partition are
    chunked: every chunk is viewed and written, at its own offset, and a
    SQL NULL in a later chunk of a NOT NULL column is still refused."""
    nullable = _column("v", 1, DECIMALS)
    storage = _storage(nullable, [*DECIMAL_ROWS, *DECIMAL_ROWS])
    batches = [storage.slice(0, 2), storage.slice(3, 5)]
    table = pa.concat_tables(_table([nullable], [batch]) for batch in batches)
    assert table.column("v").num_chunks == 2
    written = write_variant_parquet(table, [nullable])
    back = pq.read_table(pa.BufferReader(written.body)).column("v")
    assert len(back) == 7
    assert variant.to_python(back) == variant.to_python(table.column("v"))
    # DECIMAL_ROWS' SQL NULL is the second chunk's only one.
    column = replace(nullable, nullable=False)
    table = pa.concat_tables(_table([column], [batch]) for batch in batches)
    assert table.column("v").chunk(0).null_count == 0
    with pytest.raises(ValueError, match="v is NOT NULL but has SQL NULL rows"):
        write_variant_parquet(table, [column])


def test_stamped_decimal_leaves_read_back_with_their_statistics():
    """pyarrow reads a stamped INT32/INT64 leaf as decimal128(p, s), with
    min and max in its scale. pyarrow 21 and 22 cannot extract statistics
    for an INT32/INT64 DECIMAL column at all: one reason the floor is at
    least 23 (test_pyarrow_floor.py)."""
    table, columns = _decimal_table()
    written = write_variant_parquet(table, columns)
    group = pq.read_metadata(pa.BufferReader(written.body)).row_group(0)
    chunks = {group.column(i).path_in_schema: i for i in range(group.num_columns)}
    p = group.column(chunks["v.typed_value.p.typed_value"]).statistics
    assert (p.min, p.max) == (Decimal("1234567.89"), Decimal("1234567.89"))
    assert p.null_count == 3
    q = group.column(chunks["v.typed_value.q.typed_value"]).statistics
    assert (q.min, q.max) == (Decimal(-999), Decimal(-999))
    arr = group.column(chunks["v.typed_value.arr.typed_value.list.element.typed_value"])
    assert (arr.statistics.min, arr.statistics.max) == (Decimal("-0.9"), Decimal("0.5"))


def test_write_variant_parquet_refuses_what_is_not_a_variant_in_its_write_form():
    column = _column("v", 1, DECIMALS)
    read_form = variant.storage_type(DECIMALS, form="read")
    table = _table([column], [pa.nulls(1, read_form)])
    with pytest.raises(ValueError, match="v is not in its plan's write form"):
        write_variant_parquet(table, [column])
    # A table whose variant storage no catalog column claims as a variant
    # would go out a plain struct with no annotation.
    table = _table([column], [_storage(column, [1])])
    with pytest.raises(ValueError, match="no variant columns"):
        write_variant_parquet(table, [replace(column, type="long")])
    with pytest.raises(ValueError, match="no variant columns"):
        write_variant_parquet(table, [])


def test_write_variant_parquet_holds_storage_to_its_columns_own_declaration():
    """Each plan is the column's declaration's, so storage another
    declaration built is not its write form, even when the two read back
    alike; a root variant declares the unshredded layout, as no
    declaration does."""
    declared = _column(
        "v", 1, {"type": "object", "fields": [{"name": "a", "type": "int64"}]}
    )
    other = _column("v", 1, {"type": "string"})
    table = _table([other], [_storage(other, ["x"])])
    with pytest.raises(ValueError, match="v is not in its plan's write form"):
        write_variant_parquet(table, [declared])
    with pytest.raises(ValueError, match="v is not in its plan's write form"):
        write_variant_parquet(table, [_column("v", 1, None)])

    # One read type (decimal128(9, 2)), two layouts (INT32, INT64).
    def p(kind):
        field = {"name": "p", "type": kind, "precision": 9, "scale": 2}
        return _column("v", 1, {"type": "object", "fields": [field]})

    declared, other = p("decimal4"), p("decimal8")
    assert _plan(declared).read_type == _plan(other).read_type
    table = _table([other], [_storage(other, [{"p": Decimal("1.25")}])])
    with pytest.raises(ValueError, match="v is not in its plan's write form"):
        write_variant_parquet(table, [declared])
    plain, undeclared = _column("v", 1, {"type": "variant"}), _column("v", 1, None)
    unshredded = _table([plain], [_storage(plain, [1])])
    assert write_variant_parquet(unshredded, [plain]).size
    assert write_variant_parquet(unshredded, [undeclared]).size


@pytest.mark.parametrize(
    ("change", "refusal"),
    [
        ("field_id", "v carries field id 7, not its column's 1"),
        ("no_field_id", "v carries no field id, not its column's 1"),
        ("nullable", "v is nullable, and its column NOT NULL"),
    ],
)
def test_write_variant_parquet_refuses_a_field_that_is_not_the_columns(change, refusal):
    """The table's field carries the column's field id and nullability: a
    caller's table that does not is the caller's mistake, refused before
    anything is written, not a layout fault blaming the writer."""
    column = _column("v", 1, DECIMALS, nullable=False)
    rows = [row for row in DECIMAL_ROWS if row is not None]
    table = _table([column], [_storage(column, rows)])
    field = table.schema.field("v")
    if change == "nullable":
        field = field.with_nullable(True)
    else:
        field = field.remove_metadata()
        if change == "field_id":
            field = field.with_metadata({b"PARQUET:field_id": b"7"})
    forged = pa.table([table.column("v")], schema=pa.schema([field]))
    with pytest.raises(ValueError, match=re.escape(refusal)) as refused:
        write_variant_parquet(forged, [column])
    assert not isinstance(refused.value, ValidationError)
    write_variant_parquet(table, [column])
    # The other way round: a NOT NULL field for a nullable column.
    nullable = replace(column, nullable=True)
    with pytest.raises(ValueError, match="v is NOT NULL, and its column nullable"):
        write_variant_parquet(table, [nullable])


def test_write_variant_parquet_writes_no_field_id_below_the_group():
    """A field id the caller's storage carries below the group is not
    written (D14): the type written is the plan's, and its only field id is
    the group's, so the file is the declaration's, not a layout fault."""
    column = _column("v", 1, DECIMALS, nullable=False)
    rows = [row for row in DECIMAL_ROWS if row is not None]
    table = _table([column], [_storage(column, rows)])
    field = table.schema.field("v")
    children = [
        f.with_metadata({b"PARQUET:field_id": str(90 + i).encode()})
        for i, f in enumerate(field.type)
    ]
    field = field.with_type(pa.struct(children))
    tagged = pa.table([table.column("v").cast(field.type)], schema=pa.schema([field]))
    written = write_variant_parquet(tagged, [column])
    ids = [element.get(9) for element in _schema_elements(written.body.to_pybytes())]
    assert ids[:2] == [None, 1]  # the root, then the group
    assert set(ids[2:]) == {None}
    # Byte for byte the file of the same storage with no tags.
    assert written.body.equals(write_variant_parquet(table, [column]).body)


@pytest.mark.parametrize("container", ["struct", "list"])
def test_write_variant_parquet_refuses_a_variant_nested_in_a_container(container):
    """No writer shreds a nested variant (D17): a container holding one is
    refused, not written as the plain struct its storage would be."""
    from pyhoglake.errors import UnsupportedTypeError

    top = _column("v", 1, None)
    child = Column("x" if container == "struct" else "element", "variant", 3, 0, True)
    nested = Column("c", container, 2, 1, True, children=(child,))
    storage = _storage(top, [1])
    kind = (
        pa.struct([pa.field("x", storage.type)])
        if container == "struct"
        else pa.list_(pa.field("element", storage.type))
    )
    table = _table([top], [storage], extra=[(pa.field("c", kind), pa.nulls(1, kind))])
    with pytest.raises(UnsupportedTypeError, match="'(x|element)' is nested in a"):
        write_variant_parquet(table, [top, nested])


def test_write_variant_parquet_stamps_the_file_where_pyarrow_wrote_it():
    """The stamps replace the footer in the buffer pyarrow wrote, and the
    body (hundreds of MiB for a big flush) is not joined to a new tail:
    Python's peak while writing is one file and the buffer's slack, not
    two files."""
    import tracemalloc

    column = _column("v", 1, None)
    rows = [{"k": "x" * 200, "n": i} for i in range(20_000)]
    table = _table([column], [_storage(column, rows)])
    tracemalloc.start()
    try:
        written = write_variant_parquet(
            table, [column], write_options={"compression": "none"}
        )
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert written.size > 4_000_000
    assert peak < 1.5 * written.size


def test_write_variant_parquet_raises_a_layout_fault_as_a_validation_error(
    monkeypatch,
):
    import pyhoglake.parquet_schema as ps

    table, columns = _decimal_table()
    monkeypatch.setattr(ps, "variant_layout_fault", lambda *a, **k: "forced")
    with pytest.raises(ValidationError, match="not its declaration's: forced"):
        write_variant_parquet(table, columns)


#: Every node type but object and array (which the positions below are),
#: and each width of decimal at the edges the grammar allows. Drawn from
#: the grammar's vocabulary (pinned to the server's by test_variant_ddl),
#: not the codec's leaf table, so a type the table lacks fails here.
NODE_TYPES = [
    {"type": kind} for kind in ("variant", *sorted(variant.PRIMITIVE_TYPES))
] + [
    {"type": kind, "precision": precision, "scale": scale}
    for kind, precision, scale in (
        ("decimal4", 1, 0),
        ("decimal4", 9, 9),
        ("decimal8", 1, 0),
        ("decimal8", 18, 18),
        ("decimal16", 1, 0),
        ("decimal16", 7, 2),
        ("decimal16", 38, 38),
    )
]


def test_node_types_are_every_type_the_grammar_has():
    assert {node["type"] for node in NODE_TYPES} == {
        "variant",
        *variant.PRIMITIVE_TYPES,
        *variant.DECIMAL_TYPES,
    }


def _positioned(node, position):
    if position == "root":
        return node
    if position == "field":
        return {"type": "object", "fields": [{"name": "x", **node}]}
    return {"type": "array", "element": node}


@pytest.mark.parametrize("nullable", [True, False])
@pytest.mark.parametrize("position", ["root", "field", "element"])
@pytest.mark.parametrize(
    "node", NODE_TYPES, ids=lambda n: "-".join(map(str, n.values()))
)
def test_write_variant_parquet_writes_every_node_type_as_declared(
    node, position, nullable
):
    """Each node type's typed_value, as a root, an object field and a list
    element, REQUIRED or OPTIONAL, through the writer on this pyarrow (CI's
    pyarrow matrix runs each), reads back as its plan's read form.

    The layout check lets a writer leave out a converted type the codec's
    table lists, so a converted type wrongly added to a row (UTF8 on
    binary, which a reader of converted types takes for a string) would
    pass it: every element pyarrow writes is also held to the table as it
    stands, which pyarrow 26 writes whole, converted types included."""
    column = _column("v", 7, _positioned(node, position), nullable=nullable)
    storage = variant.encode_json(["null", "1", '{"x": 1}', "[1]"], column).array
    table = _table([column], [storage])
    written = write_variant_parquet(table, [column])
    elements = _schema_elements(written.body)
    expected = [want for _, want in _plan(column).expected_elements(column)]
    assert elements[1:] == expected
    back = pq.read_table(pa.BufferReader(written.body)).column("v")
    assert back.type == _plan(column).read_type
    assert variant.to_python(back, column) == variant.to_python(storage, column)


# -- the exact layout check ----------------------------------------------------

LAYOUT = {
    "type": "object",
    "fields": [
        {"name": "ts", "type": "timestamp"},
        {"name": "tz", "type": "timestamptz"},
        {"name": "t", "type": "time"},
        {"name": "n", "type": "int32"},
        {"name": "d4", "type": "decimal4", "precision": 9, "scale": 2},
        {"name": "tags", "type": "array", "element": {"type": "string"}},
        {"name": "any", "type": "variant"},
        {"name": "u", "type": "uuid"},
    ],
}
LAYOUT_ROWS = [
    {"ts": "x", "n": 1, "tags": ["a", None], "any": {"k": 1}},
    {"d4": Decimal("1.25"), "tz": None},
    None,
]
TV = ("v", "typed_value")


def _layout_elements():
    column = _column("v", 1, LAYOUT)
    return column, _written(column, LAYOUT_ROWS)[1]


def _element_paths(elements):
    out, stack = [], []
    for index, element in enumerate(elements):
        path = ()
        if index:
            while not stack[-1][0]:
                stack.pop()
            stack[-1][0] -= 1
            path = (*stack[-1][1], element[4])
        if element.get(5):
            stack.append([element[5], path])
        out.append(path)
    return out


def _at(elements, *path):
    return elements[_element_paths(elements).index(path)]


def _remove(elements, path):
    """Remove the subtree at ``path`` and count one child fewer above it."""
    paths = _element_paths(elements)
    index = paths.index(path)
    end = index + 1
    while end < len(paths) and paths[end][: len(path)] == path:
        end += 1
    del elements[index:end]
    _at(elements, *path[:-1])[5] -= 1


def _insert_before(elements, path, element, parent):
    index = _element_paths(elements).index(path)
    _at(elements, *parent)[5] += 1
    elements[index:index] = [element]


def _two_level_list(e):
    _at(e, *TV, "tags", "typed_value", "list", "element")[3] = 2
    del e[_element_paths(e).index((*TV, "tags", "typed_value", "list"))]


LIST = (*TV, "tags", "typed_value", "list")

#: The layout mutations the check must refuse, each a footer some writer
#: (or a pyarrow that changed) could spell.
MUTATIONS = {
    "group_not_annotated": lambda e: e[1].pop(10),
    "group_variant_version_2": lambda e: e[1].update({10: {16: {1: 2}}}),
    "group_other_field_id": lambda e: e[1].update({9: 2}),
    "group_required": lambda e: e[1].update({3: 0}),
    "group_renamed": lambda e: e[1].update({4: "w"}),
    "no_metadata": lambda e: _remove(e, ("v", "metadata")),
    "metadata_not_binary": lambda e: _at(e, "v", "metadata").update({1: 1}),
    "metadata_field_id": lambda e: _at(e, "v", "metadata").update({9: 2}),
    "required_value": lambda e: _at(e, "v", "value").update({3: 0}),
    "optional_field_group": lambda e: _at(e, *TV, "ts").update({3: 1}),
    "optional_element": lambda e: _at(e, *LIST, "element").update({3: 1}),
    "required_typed_value": lambda e: _at(e, *TV, "n", "typed_value").update({3: 0}),
    "timestamptz_for_timestamp": lambda e: _at(e, *TV, "ts", "typed_value").update(
        {10: {8: {1: True, 2: {2: {}}}}}
    ),
    "timestamp_millis": lambda e: _at(e, *TV, "ts", "typed_value").update(
        {6: 9, 10: {8: {1: False, 2: {1: {}}}}}
    ),
    "time_utc": lambda e: _at(e, *TV, "t", "typed_value").update(
        {10: {7: {1: True, 2: {2: {}}}}}
    ),
    "unsigned_int": lambda e: _at(e, *TV, "n", "typed_value").update(
        {6: 13, 10: {10: {1: 32, 2: False}}}
    ),
    "int32_with_a_logical_type": lambda e: _at(e, *TV, "n", "typed_value").update(
        {10: {10: {1: 32, 2: True}}}
    ),
    "other_leaf_type": lambda e: _at(e, *TV, "n", "typed_value").update({1: 2}),
    "flba_decimal4": lambda e: _at(e, *TV, "d4", "typed_value").update({1: 7, 2: 4}),
    "decimal_precision": lambda e: _at(e, *TV, "d4", "typed_value").update(
        {8: 8, 10: {5: {1: 2, 2: 8}}}
    ),
    "uuid_length": lambda e: _at(e, *TV, "u", "typed_value").update({2: 15}),
    "child_field_id": lambda e: _at(e, *TV, "n", "typed_value").update({9: 1}),
    "renamed_field": lambda e: _at(e, *TV, "n").update({4: "m"}),
    "duplicate_field": lambda e: _at(e, *TV, "ts").update({4: "n"}),
    "missing_field": lambda e: _remove(e, (*TV, "n")),
    "missing_value": lambda e: _remove(e, (*TV, "n", "value")),
    "extra_child": lambda e: _insert_before(
        e, (*TV,), {1: 6, 3: 1, 4: "extra"}, ("v",)
    ),
    "list_not_repeated": lambda e: _at(e, *LIST).update({3: 1}),
    "list_named_array": lambda e: _at(e, *LIST).update({4: "array"}),
    "two_level_list": _two_level_list,
}


def test_the_check_accepts_pyhoglakes_own_layout():
    column, elements = _layout_elements()
    assert variant_layout_fault(elements, 1, column, _plan(column)) is None


@pytest.mark.parametrize("mutation", MUTATIONS)
def test_the_check_refuses_each_layout_mutation(mutation):
    column, elements = _layout_elements()
    MUTATIONS[mutation](elements)
    fault = variant_layout_fault(elements, 1, column, _plan(column))
    assert fault is not None
    assert "not the declaration's" in fault


@pytest.mark.parametrize("name", DECLARATIONS)
@pytest.mark.parametrize("nullable", [True, False])
def test_the_check_accepts_pyhoglakes_file_for_every_corpus_declaration(name, nullable):
    """Every corpus layout, as this pyarrow writes it, REQUIRED or OPTIONAL
    as the column is, wherever the column stands in the file."""
    column = _column("v", 4, DECLARATIONS[name], nullable=nullable)
    rows = [{"a": 1}, [1, "x"], "s", Decimal("1.5")]
    other = _column("a", 1, DECIMALS)
    table = _table(
        [other, column], [_storage(other, DECIMAL_ROWS), _storage(column, rows)]
    )
    written = write_variant_parquet(table, [other, column])
    elements = _schema_elements(written.body)
    (start,) = [i for e, _, i in _top_level(elements) if e[4] == "v"]
    assert elements[start][3] == (1 if nullable else 0)
    assert variant_layout_fault(elements, start, column, _plan(column)) is None
    assert variant_layout_fault(elements, 1, column, _plan(column)) is not None


def test_the_check_holds_the_group_to_the_columns_name_id_and_nullability():
    column, elements = _layout_elements()
    plan = _plan(column)
    for other in (
        replace(column, field_id=2),
        replace(column, nullable=False),
        replace(column, name="w"),
    ):
        assert variant_layout_fault(elements, 1, other, plan) is not None


def test_the_check_lets_a_writer_leave_out_a_legacy_converted_type_and_no_more():
    """A converted type is the legacy spelling of the logical type beside
    it. pyarrow writes one; a later one may not, and the file is the same
    to every reader that knows logical types. A different one, or half of
    a DECIMAL's, is not that."""
    column, elements = _layout_elements()
    plan = _plan(column)
    string = (*LIST, "element", "typed_value")
    decimal = (*TV, "d4", "typed_value")
    assert _at(elements, *string)[6] == 0 and _at(elements, *decimal)[6] == 5
    gone = copy.deepcopy(elements)
    del _at(gone, *string)[6]
    for field in (6, 7, 8):
        del _at(gone, *decimal)[field]
    assert variant_layout_fault(gone, 1, column, plan) is None
    half = copy.deepcopy(elements)
    del _at(half, *decimal)[6]
    assert variant_layout_fault(half, 1, column, plan) is not None
    other = copy.deepcopy(elements)
    _at(other, *string)[6] = 19
    assert variant_layout_fault(other, 1, column, plan) is not None
    # The licence is for a converted type the declaration's table lists.
    added = copy.deepcopy(elements)
    _at(added, *TV, "n", "typed_value")[6] = 17
    assert variant_layout_fault(added, 1, column, plan) is not None


def test_a_subtree_that_does_not_close_is_a_fault_not_a_crash():
    column, elements = _layout_elements()
    plan = _plan(column)
    assert variant_layout_fault(elements[:6], 1, column, plan) == (
        "truncated Parquet schema"
    )
    for children in (-1, "1"):
        _at(elements, "v", "metadata")[5] = children
        assert variant_layout_fault(elements, 1, column, plan) == (
            "negative schema child count"
        )


# -- property: the writer over generated declarations and values -------------


#: A SQL NULL row, drawn apart from PYTHON_VALUES' None (a variant null).
SQL_NULL = object()


@given(
    prop.DECLARATIONS,
    st.booleans(),
    st.integers(1, 2**31 - 1),
    st.lists(prop.PYTHON_VALUES | st.just(SQL_NULL), max_size=5),
)
def test_written_files_are_the_oracles_stamp_and_read_back_as_written(
    decl, nullable, field_id, values
):
    """AGENT.md's round-trip fuzzer for the splice and the writer, over
    qe_prop_variant_codec's declarations and values. The oracles are ground
    truth: the generic codec's re-encoded footer (footer_oracle), the plan's
    expected elements, and the source rows read back. A SQL NULL row in a
    NOT NULL column is refused; elsewhere it reads back as None."""
    column = _column("v", field_id, decl, nullable=nullable)
    rows = [
        None if value is SQL_NULL else variant.VARIANT_NULL if value is None else value
        for value in values
    ]
    # The encoder refuses SQL NULL for a NOT NULL column, so the storage is
    # the nullable twin's, as storage built elsewhere could be.
    storage = _storage(replace(column, nullable=True), rows)
    table = _table([column], [storage])
    plan = _plan(column)
    if not nullable and any(row is None for row in rows):
        with pytest.raises(ValueError, match="v is NOT NULL but has SQL NULL rows"):
            write_variant_parquet(table, [column])
        return
    written = write_variant_parquet(table, [column])
    assert written.body.to_pybytes() == footer_oracle.stamp(
        _pyarrow_bytes(table), variant_stamps(column, plan)
    )
    assert written.footer_size == len(_footer(written.body))
    assert variant_layout_fault(_schema_elements(written.body), 1, column, plan) is None
    back = pq.read_table(pa.BufferReader(written.body)).column("v")
    assert back.type == plan.read_type
    got = variant.to_python(back, column)
    assert len(got) == len(rows)
    assert all(same(g, want) for g, want in zip(got, rows, strict=True)), decl
