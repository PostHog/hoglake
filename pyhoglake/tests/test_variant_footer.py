"""Suite F: the VARIANT footer splice, and the writer helper around it.

pyarrow writes a VARIANT column's storage as a plain struct and its
decimal4/decimal8 leaves, viewed as integers, as plain INT32/INT64.
``parquet_schema.stamp_variant_footer`` splices the annotations into the
footer; ``tests/footer_oracle.py``, a generic compact-Thrift codec that
re-encodes the whole footer, is its oracle, and the two must agree byte for
byte on every file here.
"""

from __future__ import annotations

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
from thrift.Thrift import TType

from pyhoglake import variant
from pyhoglake._variant_codec import compile_plan
from pyhoglake.errors import ValidationError
from pyhoglake.models import Column
from pyhoglake.parquet_schema import (
    FooterStamp,
    _footer_bytes,
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
from variant_corpus import (
    DECLARATIONS,
    corpus_columns,
    golden_table,
    integer_view,
    oracle_stamps,
)

DATA = Path(__file__).parent / "data"
STAMPED = DATA / "variant" / "corpus_stamped.parquet"
FOREIGN = sorted(DATA.glob("*.parquet")) + sorted(
    (DATA / "parquet-testing" / "shredded_variant").glob("*.parquet")
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


def _storage(column, rows):
    return variant.encode_python(rows, column).array


def _table(columns, arrays, extra=None):
    """A table of variant storage in the write form, field ids on."""
    fields, data = [], []
    for column, array in zip(columns, arrays, strict=True):
        fields.append(
            pa.field(
                column.name,
                array.type,
                nullable=column.nullable,
                metadata={b"PARQUET:field_id": str(column.field_id).encode()},
            )
        )
        data.append(array)
    for field, array in extra or ():
        fields.append(field)
        data.append(array)
    return pa.table(data, schema=pa.schema(fields))


def _pyarrow_bytes(table, **options):
    """What pyarrow writes for ``table`` with its decimal leaves viewed as
    integers and no Arrow schema stored: the splice's input."""
    fields, data = [], []
    for field in table.schema:
        kind = integer_view(field.type)
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


def _plan(column):
    return compile_plan((column.type_params or {}).get("shredding"))


def _corpus():
    table = golden_table()
    columns = corpus_columns(table)
    return _table(columns, [table.column(c.name) for c in columns]), columns


def _footer(raw):
    return footer_oracle.split(raw)[1]


# -- reading the footer --------------------------------------------------------


@pytest.mark.parametrize(
    "form", [bytes, bytearray, memoryview, pa.py_buffer], ids=lambda f: f.__name__
)
def test_footer_bytes_reads_a_path_and_every_in_memory_form_alike(tmp_path, form):
    raw = STAMPED.read_bytes()
    path = tmp_path / "f.parquet"
    path.write_bytes(raw)
    start, footer = footer_oracle.split(raw)
    assert _footer_bytes(str(path)) == (start, footer)
    assert _footer_bytes(path) == (start, footer)
    assert _footer_bytes(form(raw)) == (start, footer)


@pytest.mark.parametrize(
    ("tail", "message"),
    [
        (b"PARE", "encrypted"),
        (b"PAR0", "invalid or oversized"),
    ],
)
def test_footer_bytes_refuses_anything_but_a_plaintext_footer(tail, message):
    raw = STAMPED.read_bytes()[:-4] + tail
    with pytest.raises(ValueError, match=message):
        _footer_bytes(raw)


@pytest.mark.parametrize("length", [2**32 - 1, 64 * 1024 * 1024 + 1, 101])
def test_footer_bytes_refuses_a_length_longer_than_the_file(length):
    """101 of these 112 bytes would reach into the leading magic."""
    raw = b"PAR1" + b"\x00" * 100 + struct.pack("<I", length) + b"PAR1"
    with pytest.raises(ValueError, match="oversized"):
        _footer_bytes(raw)
    # Everything between the magics is the most a footer can be.
    raw = b"PAR1" + b"\x01" * 100 + struct.pack("<I", 100) + b"PAR1"
    assert _footer_bytes(raw) == (4, b"\x01" * 100)


def test_footer_bytes_refuses_bytes_too_short_for_a_trailer():
    with pytest.raises(ValueError, match="invalid or oversized"):
        _footer_bytes(b"PAR1")


@pytest.mark.parametrize("path", FOREIGN[:8] + [STAMPED], ids=lambda p: p.name)
def test_positions_place_every_stop_and_path_as_the_oracle_reads_them(path):
    """STOP offsets, last field ids and tuple paths, against the oracle's
    own reading: its recursive walk for the paths, and its decoded fields
    for the last id."""
    footer = _footer(path.read_bytes())
    elements = _schema_elements_from_footer(footer, positions=True)
    tree = footer_oracle.schema(footer_oracle.decode(footer))
    assert [e.path for e in elements] == footer_oracle.paths(tree)
    assert [e.last for e in elements] == [entry[-1][0] for entry in tree]
    assert all(footer[e.stop] == 0 for e in elements)
    # The positions are an addition: the fields read the same either way.
    assert [e.fields for e in elements] == _schema_elements_from_footer(footer)


def test_paths_are_tuples_so_a_dotted_name_is_one_name():
    column = _column("v", 1, DECIMALS)
    raw = _pyarrow_bytes(_table([column], [_storage(column, DECIMAL_ROWS)]))
    paths = [e.path for e in _schema_elements_from_footer(_footer(raw), positions=True)]
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


@pytest.mark.parametrize(
    ("edit", "message"),
    [
        (_over_claiming_root, "truncated Parquet schema"),
        (_trailing_element, "invalid Parquet schema tree"),
        (_negative_children, "negative schema child count"),
    ],
    ids=["over_claiming_root", "trailing_element", "negative_children"],
)
def test_positions_refuse_a_schema_tree_that_does_not_close(edit, message):
    """The splice places its bytes by these paths: a tree that claims more
    elements than it holds, holds more than it claims, or counts children
    below zero has none to trust."""
    _, raw = _one_decimal()
    footer = _footer(footer_oracle.rebuild(raw, edit))
    with pytest.raises(ValueError, match=message):
        _schema_elements_from_footer(footer, positions=True)


def test_schema_elements_of_a_path_is_the_footer_reading():
    footer = _footer(STAMPED.read_bytes())
    assert _schema_elements(str(STAMPED)) == _schema_elements_from_footer(footer)


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
    (_, element) = _schema_elements_from_footer(_footer(sink.getvalue().to_pybytes()))
    assert element[10] == logical


# -- the splice against the oracle -------------------------------------------


def _decimal_table(**_):
    column = _column("v", 7, DECIMALS)
    return _table([column], [_storage(column, DECIMAL_ROWS)]), [column]


def _many_columns(**_):
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


def _wide(**_):
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
    "corpus": (lambda: _corpus(), {}),
    "page_index": (lambda: _corpus(), {"write_page_index": True}),
    "zstd": (lambda: _corpus(), {"compression": "zstd"}),
    "five_row_groups": (lambda: _corpus(), {"row_group_size": 7}),
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
    patched, elements = stamp_variant_footer(raw, stamps)
    assert patched == footer_oracle.stamp(raw, stamps)
    start, footer = footer_oracle.split(raw)
    # The body is untouched: every offset into it (column chunks, page
    # indexes) still holds.
    assert patched[:start] == raw[:start]
    groups = sum(s.kind == "variant" for s in stamps)
    decimals = len(stamps) - groups
    assert len(_footer(patched)) == len(footer) + 7 * groups + 14 * decimals
    assert elements == _schema_elements_from_footer(_footer(patched))
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
    assert stamp_variant_footer(raw, stamps[::-1])[0] == expected
    a, b = _stamps(columns[2:3]), _stamps(columns[1:2])
    assert stamp_variant_footer(raw, [*a[::-1], *b[::-1]])[0] == footer_oracle.stamp(
        raw, [*b, *a]
    )


def test_the_committed_corpus_is_the_splice_of_what_pyarrow_wrote():
    """The fixed input: pyarrow 25.0.1's bytes, whatever pyarrow runs the
    suite, stamped by the oracle when the fixture was written."""
    stamped = STAMPED.read_bytes()
    columns = corpus_columns(golden_table())
    raw = footer_oracle.unstamp(stamped, oracle_stamps(stamped, columns))
    patched, _ = stamp_variant_footer(raw, _stamps(columns))
    assert patched == stamped


def test_variant_stamps_agree_with_the_oracle_on_decimal_widths():
    stamped = STAMPED.read_bytes()
    columns = corpus_columns(golden_table())
    mine = [(s.path, s.kind, s.precision, s.scale) for s in _stamps(columns)]
    theirs = [
        (s.path, s.kind, s.precision, s.scale) for s in oracle_stamps(stamped, columns)
    ]
    assert mine == theirs
    assert {kind for _, kind, _, _ in mine} == {"variant", "decimal4", "decimal8"}


def test_the_variant_bytes_follow_the_field_id():
    """pyarrow writes a group's type-less fields 3, 4, 5 and 9, so field 10
    is a delta of 1: ``1C``, then LogicalType's VARIANT (field 16, long
    form ``0C 20``), VariantType{1: i8 1} (``13 01``), two STOPs."""
    column = _column("v", 9, None)
    raw = _pyarrow_bytes(_table([column], [_storage(column, [1])]))
    footer = _footer(raw)
    (group,) = [
        e
        for e in _schema_elements_from_footer(footer, positions=True)
        if e.path == ("v",)
    ]
    assert group.last == 9
    patched = _footer(stamp_variant_footer(raw, _stamps([column]))[0])
    blob = bytes.fromhex("1C 0C 20 13 01 00 00")
    assert patched[group.stop : group.stop + 7] == blob
    assert patched[: group.stop] == footer[: group.stop]
    assert patched[group.stop + 7 :] == footer[group.stop :]


def test_the_decimal_bytes_follow_the_name():
    """A leaf's fields are 1, 3 and 4, so converted_type (6) is a delta of
    2; precision and scale are zigzag varints of one byte each."""
    column = _column("v", 1, {"type": "decimal8", "precision": 18, "scale": 7})
    raw = _pyarrow_bytes(_table([column], [_storage(column, [Decimal("1.0000000")])]))
    footer = _footer(raw)
    (leaf,) = [
        e
        for e in _schema_elements_from_footer(footer, positions=True)
        if e.path == ("v", "typed_value")
    ]
    assert leaf.last == 4
    patched = _footer(stamp_variant_footer(raw, _stamps([column]))[0])
    blob = bytes.fromhex("25 0A 15 0E 15 24 2C 5C 15 0E 15 24 00 00")
    assert patched[leaf.stop + 7 : leaf.stop + 21] == blob


@pytest.mark.parametrize("path", FOREIGN, ids=lambda p: p.name)
def test_the_oracle_and_an_empty_splice_leave_foreign_footers_alone(path):
    """Identity on what other writers produce (DuckDB, and Iceberg's
    parquet-java files in parquet-testing): the oracle round-trips each
    footer byte for byte, which is what makes it an oracle, and a splice of
    nothing changes nothing. The oracle's own encoder, which spells what
    Thrift's writer refuses to (a field id outside an i16), writes the same
    bytes Thrift's does."""
    raw = path.read_bytes()
    footer = _footer(raw)
    tree = footer_oracle.decode(footer)
    assert footer_oracle.encode(tree) == footer
    assert footer_oracle.encode_unchecked(tree) == footer
    if path.name == "native_variant_union_two_fields.parquet":
        # A footer parquet-java cannot read, which no splice reads either.
        with pytest.raises(ValueError, match="LogicalType union holds 2 fields"):
            stamp_variant_footer(raw, [])
        return
    assert stamp_variant_footer(raw, [])[0] == raw


# -- the splice's refusals ---------------------------------------------------


def _one_decimal():
    column = _column("v", 3, {"type": "decimal4", "precision": 9, "scale": 2})
    raw = _pyarrow_bytes(_table([column], [_storage(column, [Decimal("1.00")])]))
    return column, raw


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
    once, _ = stamp_variant_footer(raw, _stamps([column]))
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


def test_a_field_the_reader_skips_still_counts_as_the_last():
    """The schema reader skips a field parquet.thrift declares with another
    wire type, as Thrift's generated readers do: a logicalType (10) or a
    converted_type (6) spelled as an i64 here. Skipped, it still moves the
    compact protocol's delta base, so a splice after it would add another
    field than the one it means to."""
    column, raw = _one_decimal()
    forged = footer_oracle.edit_element(
        raw, ("v",), lambda f: footer_oracle.put(f, 10, TType.I64, 0)
    )
    assert 10 not in _schema_elements_from_footer(_footer(forged))[1]
    _refused(forged, _stamps([column])[:1], "field 10 after its field id")
    forged = footer_oracle.edit_element(
        raw, ("v", "typed_value"), lambda f: footer_oracle.put(f, 6, TType.I64, 0)
    )
    assert 6 not in _schema_elements_from_footer(_footer(forged))[-1]
    _refused(forged, _stamps([column])[1:], "field 6 after num_children")


def test_refuses_a_variant_stamp_on_a_leaf():
    _, raw = _one_decimal()
    _refused(raw, [FooterStamp(("v", "metadata"), "variant", 3)], "not a group")
    # Nor on an element with neither a type nor children.
    forged = footer_oracle.edit_element(
        raw, ("v", "metadata"), lambda f: footer_oracle.drop(f, 1)
    )
    _refused(forged, [FooterStamp(("v", "metadata"), "variant", 3)], "not a group")


def test_refuses_a_variant_stamp_on_a_group_with_a_physical_type():
    column, raw = _one_decimal()
    forged = footer_oracle.edit_element(
        raw, ("v",), lambda f: footer_oracle.put(f, 1, TType.I32, 6)
    )
    _refused(forged, _stamps([column])[:1], "not a group")


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
        for e in _schema_elements_from_footer(_footer(forged), positions=True)
        if e.path == ("v", "typed_value")
    ]
    assert leaf.last == 5
    stamps = _stamps([column])
    body, _ = stamp_variant_footer(forged, stamps)
    assert body == footer_oracle.stamp(forged, stamps)


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


def test_refuses_a_decimal_leaf_with_a_field_id():
    column, raw = _one_decimal()
    forged = footer_oracle.edit_element(
        raw, ("v", "typed_value"), lambda f: footer_oracle.put(f, 9, TType.I32, 77)
    )
    _refused(forged, _stamps([column])[1:], "has a field id")


@pytest.mark.parametrize("field_id", [7, 8, 10])
def test_refuses_a_decimal_leaf_already_carrying_part_of_one(field_id):
    column, raw = _one_decimal()
    # A logicalType of one member (a union of none is no footer: below).
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
    [("decimal4", 10, 0), ("decimal8", 19, 0), ("decimal4", 0, 0), ("decimal4", 2, 3)],
)
def test_refuses_a_decimal_its_width_cannot_hold(kind, precision, scale):
    _, raw = _one_decimal()
    physical = "decimal4" if kind == "decimal4" else "decimal8"
    if physical == "decimal8":
        column = _column("v", 3, {"type": "decimal8", "precision": 18, "scale": 0})
        raw = _pyarrow_bytes(_table([column], [_storage(column, [Decimal(1)])]))
    stamp = FooterStamp(("v", "typed_value"), kind, precision=precision, scale=scale)
    _refused(raw, [stamp], f"is not a {kind}")


@pytest.mark.parametrize("kind", ["decimal", "decimal16", "DECIMAL8", "varaint"])
def test_refuses_a_stamp_of_a_kind_it_does_not_know(kind):
    """Each was stamped as a decimal8 (pyarrow read an INT64 leaf so
    stamped back as decimal128(10, 2)): a typo stamped a type."""
    with pytest.raises(ValueError, match=f"x: no stamp of kind '{kind}'"):
        FooterStamp(("x",), kind, precision=10, scale=2)


def test_refuses_a_path_absent_or_present_twice():
    _, raw = _one_decimal()
    _refused(raw, [FooterStamp(("w",), "variant", 3)], "0 times")
    # Two children of one name: Parquet's schema allows it.
    twice = footer_oracle.edit_element(
        raw,
        ("v", "value"),
        lambda f: footer_oracle.put(f, 4, TType.STRING, b"metadata"),
    )
    stamp = FooterStamp(("v", "metadata"), "decimal4", precision=9, scale=2)
    _refused(twice, [stamp], "2 times")


def test_refuses_a_stamp_given_twice():
    column, raw = _one_decimal()
    stamps = _stamps([column])
    _refused(raw, [*stamps, stamps[0]], "stamped twice")


def test_a_stamp_of_the_wrong_length_is_refused(monkeypatch):
    import pyhoglake.parquet_schema as ps

    column, raw = _one_decimal()
    real = ps._annotation
    monkeypatch.setattr(ps, "_annotation", lambda s, e: real(s, e) + b"\x00")
    _refused(raw, _stamps([column]), "wrong length")


def test_a_stamp_that_reads_back_as_another_field_is_refused(monkeypatch):
    """The splice re-reads what it wrote: bytes that decode to another
    field (here, a delta from the wrong last id) never leave it."""
    import pyhoglake.parquet_schema as ps

    column, raw = _one_decimal()
    real = ps._annotation

    def off_by_one(stamp, element):
        blob = real(stamp, element)
        return bytes([blob[0] + 0x10, *blob[1:]])

    monkeypatch.setattr(ps, "_annotation", off_by_one)
    _refused(raw, _stamps([column]), "does not read back as stamped")


# -- canaries: pyarrow learning VARIANT --------------------------------------


def test_canary_pyarrow_writes_no_variant_annotation_and_plain_integer_leaves():
    """If this fails, pyarrow has started writing VARIANT (or DECIMAL on
    the integer view) itself, and the splice now refuses every write:
    revisit stamp_variant_footer (apache/arrow#50131, #50132, #50252)."""
    column = _column("v", 1, DECIMALS)
    raw = _pyarrow_bytes(_table([column], [_storage(column, DECIMAL_ROWS)]))
    elements = {
        e.path: e.fields
        for e in _schema_elements_from_footer(_footer(raw), positions=True)
    }
    assert not {6, 10} & set(elements[("v",)])
    for path, _, _ in _plan(column).stamps:
        leaf = elements[("v", *path)]
        assert set(leaf) == {1, 3, 4}, f"pyarrow {pa.__version__} annotates {path}"


@pytest.mark.parametrize("path", [STAMPED, DATA / "native_variant.parquet"])
def test_canary_pyarrow_reads_a_variant_group_as_its_plain_struct(path):
    """If this fails, pyarrow surfaces VARIANT as an extension type (or as
    something else again), and every read-form check fails closed: revisit
    the plans' read forms. Never register a Python extension type named
    arrow.parquet.variant to make it pass: that crashed the process."""
    with pq.ParquetFile(path) as parquet:
        for field in parquet.schema_arrow:
            assert not isinstance(field.type, pa.BaseExtensionType) or (
                field.type.extension_name == "arrow.uuid"
            )
        table = parquet.read()
    for field in table.schema:
        if pa.types.is_struct(field.type):
            assert type(field.type) is pa.StructType


def test_canary_the_committed_corpus_reads_back_as_each_plans_read_form():
    columns = corpus_columns(golden_table())
    with pq.ParquetFile(STAMPED) as parquet:
        read = parquet.schema_arrow
        table = parquet.read()
    for column in columns:
        assert read.field(column.name).type == _plan(column).read_type, column.name
    expected = golden_table()
    for column in columns:
        # repr, which tells NaN from NaN as equal and -0.0 from 0.0 apart.
        got = variant.to_python(table.column(column.name))
        assert repr(got) == repr(variant.to_python(expected.column(column.name)))


# -- write_variant_parquet -----------------------------------------------------


def test_write_variant_parquet_round_trips_the_corpus_and_measures_after_the_patch():
    table, columns = _corpus()
    plans = {c.name: _plan(c) for c in columns}
    written = write_variant_parquet(table, columns, plans)
    body = written.body
    assert written.size == len(body)
    assert written.footer_size == struct.unpack("<I", body[-8:-4])[0]
    # Measured on the patched footer, not pyarrow's: they differ by the stamps.
    unpatched = _pyarrow_bytes(table)
    stamps = _stamps(columns)
    extra = 7 * len(columns) + 14 * (len(stamps) - len(columns))
    assert written.footer_size == len(_footer(unpatched)) + extra
    assert written.size == len(unpatched) + extra
    assert body == footer_oracle.stamp(unpatched, stamps)
    assert written.metadata.equals(pq.read_metadata(pa.BufferReader(body)))
    # A tail read of [size - footer_size - 8, size) is the whole footer.
    tail = body[written.size - written.footer_size - 8 :]
    assert _schema_elements_from_footer(tail[:-8])[1][10] == {16: {1: 1}}
    back = pq.read_table(pa.BufferReader(body))
    for column in columns:
        got = variant.to_python(back.column(column.name))
        want = variant.to_python(table.column(column.name))
        assert repr(got) == repr(want), column.name


def test_write_variant_parquet_leaves_scalar_columns_and_their_stats_alone():
    table, columns = _many_columns()
    plans = {c.name: _plan(c) for c in columns}
    written = write_variant_parquet(table, columns, plans)
    scalar = Column("n", "long", 5, 4, True)
    stats = extract_column_stats(written.metadata, (*columns, scalar))
    assert [s.field_id for s in stats] == [5]
    assert pq.read_table(pa.BufferReader(written.body)).column("n").to_pylist() == [
        1,
        2,
    ]


def test_write_variant_parquet_writes_a_not_null_column_as_a_required_group():
    column = _column("v", 2, DECLARATIONS["posthog"], nullable=False)
    table = _table([column], [_storage(column, [{"$browser": "x"}, 1])])
    written = write_variant_parquet(table, [column], {"v": _plan(column)})
    (_, group, *_) = _schema_elements_from_footer(_footer(written.body))
    assert group[3] == 0


def test_write_variant_parquet_honours_row_group_size():
    table, columns = _corpus()
    plans = {c.name: _plan(c) for c in columns}
    written = write_variant_parquet(table, columns, plans, row_group_size=10)
    assert written.metadata.num_row_groups == 4


def test_write_variant_parquet_views_sliced_storage_at_its_offset():
    """A group of a partitioned append is a slice of the caller's table:
    the integer view must keep the slice's offset into the decimal leaves."""
    column = _column("v", 1, DECIMALS)
    whole = _table([column], [_storage(column, DECIMAL_ROWS * 3)])
    part = whole.slice(5, 4)
    written = write_variant_parquet(part, [column], {"v": _plan(column)})
    back = pq.read_table(pa.BufferReader(written.body)).column("v")
    assert variant.to_python(back) == variant.to_python(part.column("v"))


def test_write_variant_parquet_refuses_storage_not_in_the_write_form():
    column = _column("v", 1, DECIMALS)
    read_form = variant.storage_type(DECIMALS, form="read")
    table = _table([column], [pa.nulls(1, read_form)])
    with pytest.raises(ValueError, match="write form"):
        write_variant_parquet(table, [column], {"v": _plan(column)})
    with pytest.raises(ValueError, match="no variant columns"):
        write_variant_parquet(table, [column], {})
    with pytest.raises(ValueError, match="not in the table"):
        write_variant_parquet(table, [column], {"v": _plan(column), "w": _plan(column)})
    scalar = replace(column, type="long")
    with pytest.raises(ValueError, match="not a variant column"):
        write_variant_parquet(table, [scalar], {"v": _plan(column)})


def test_write_variant_parquet_refuses_a_variant_column_it_has_no_plan_for():
    """Written as it stands, an unplanned variant column would go out a
    plain struct with no annotation, and an undeclared one's write form is
    its read form, so no later check would see it."""
    a, b = _column("a", 1, None), _column("b", 2, {"type": "int64"})
    table = _table([a, b], [_storage(a, [1]), _storage(b, [2])])
    with pytest.raises(ValueError, match="variant column b has no plan"):
        write_variant_parquet(table, [a, b], {"a": _plan(a)})
    # A planned column the catalog does not have is refused by name.
    with pytest.raises(ValueError, match="b is not a variant column of the table"):
        write_variant_parquet(table, [a], {"a": _plan(a), "b": _plan(b)})


@pytest.mark.parametrize(
    ("change", "refusal"),
    [
        ({"nullable": True}, "v is nullable in the table, and not in the catalog"),
        (
            {"metadata": {b"PARQUET:field_id": b"7"}},
            "v has field id b'7' in the table, not the column's 1",
        ),
        ({"metadata": None}, "v has field id None in the table, not the column's 1"),
        ({"child_id": ("metadata",)}, "v.metadata has a field id in the table"),
        ({"child_id": ("typed_value", "p")}, "v.typed_value.p has a field id"),
    ],
    ids=["nullable", "other_field_id", "no_field_id", "child_id", "nested_id"],
)
def test_write_variant_parquet_refuses_a_field_that_is_not_the_columns(change, refusal):
    """The caller's table carries the column's nullability and field id, and
    no field id below it (Arrow's type equality ignores a child's metadata,
    so the write-form check let one through, for pyarrow to write); another
    was written, then refused as pyhoglake's fault (a ValidationError, which
    a daemon reads as the data's)."""
    column = _column("v", 1, DECIMALS, nullable=False)
    rows = [row for row in DECIMAL_ROWS if row is not None]
    table = _table([column], [_storage(column, rows)])
    field = table.schema.field("v")
    if "nullable" in change:
        field = field.with_nullable(change["nullable"])
    elif "child_id" in change:
        field = field.with_type(_with_field_id(field.type, change["child_id"]))
        assert field.type == table.schema.field("v").type
    else:
        field = field.remove_metadata()
        if change["metadata"] is not None:
            field = field.with_metadata(change["metadata"])
    forged = pa.table([table.column("v").cast(field.type)], schema=pa.schema([field]))
    with pytest.raises(ValueError, match=re.escape(refusal)):
        write_variant_parquet(forged, [column], {"v": _plan(column)})
    write_variant_parquet(table, [column], {"v": _plan(column)})


def _with_field_id(kind, path):
    """The struct ``kind`` with ``PARQUET:field_id`` on the field at
    ``path`` below it."""
    head, *rest = path
    return pa.struct(
        [
            (
                child.with_type(_with_field_id(child.type, rest))
                if rest
                else child.with_metadata({b"PARQUET:field_id": b"99"})
            )
            if child.name == head
            else child
            for child in kind
        ]
    )


def test_write_variant_parquet_refuses_a_plan_that_is_not_the_columns():
    """The exact layout check holds the file to the plan it is given, so a
    stale or swapped plan wrote, stamped and passed, in a layout the
    column's own strict check refuses."""
    declared = _column(
        "v", 1, {"type": "object", "fields": [{"name": "a", "type": "int64"}]}
    )
    other = _column("v", 1, {"type": "string"})
    table = _table([other], [_storage(other, ["x"])])
    with pytest.raises(ValueError, match="the plan of v is not its declaration's"):
        write_variant_parquet(table, [declared], {"v": _plan(other)})
    # An undeclared column is held to the unshredded layout.
    undeclared = _column("v", 1, None)
    with pytest.raises(ValueError, match="the plan of v is not its declaration's"):
        write_variant_parquet(table, [undeclared], {"v": _plan(other)})
    # A root variant declares the unshredded layout too.
    plain = _column("v", 1, {"type": "variant"})
    unshredded = _table([plain], [_storage(plain, [1])])
    written = write_variant_parquet(unshredded, [plain], {"v": _plan(undeclared)})
    assert written.size > 0
    written = write_variant_parquet(unshredded, [undeclared], {"v": _plan(plain)})
    assert written.size > 0


#: Every one of the 22 node types but object and array (which the
#: positions below are), as each width of decimal the grammar allows.
NODE_TYPES = [
    {"type": kind}
    for kind in (
        "variant",
        "boolean",
        "int8",
        "int16",
        "int32",
        "int64",
        "float",
        "double",
        "date",
        "time",
        "timestamp",
        "timestamp_ns",
        "timestamptz",
        "timestamptz_ns",
        "binary",
        "string",
        "uuid",
    )
] + [
    {"type": kind, "precision": precision, "scale": scale}
    for kind, precision, scale in (
        ("decimal4", 1, 0),
        ("decimal4", 9, 9),
        ("decimal8", 1, 0),
        ("decimal8", 10, 3),
        ("decimal8", 18, 18),
        ("decimal16", 1, 0),
        ("decimal16", 19, 2),
        ("decimal16", 38, 38),
    )
]


def _positioned(node, position):
    if position == "root":
        return node
    if position == "field":
        return {"type": "object", "fields": [{"name": "x", **node}]}
    return {"type": "array", "element": node}


def test_node_types_name_every_leaf_the_codec_writes():
    from pyhoglake._variant_codec import _PARQUET_LEAF

    kinds = {node["type"] for node in NODE_TYPES}
    assert set(_PARQUET_LEAF) <= kinds
    assert len(kinds | {"object", "array"}) == 22


@pytest.mark.parametrize("nullable", [True, False])
@pytest.mark.parametrize("position", ["root", "field", "element"])
@pytest.mark.parametrize(
    "node", NODE_TYPES, ids=lambda n: "-".join(map(str, n.values()))
)
def test_write_variant_parquet_writes_every_node_type_as_declared(
    node, position, nullable
):
    """Each node type's typed_value, as a root, an object field and a list
    element, REQUIRED or OPTIONAL, through the writer on this pyarrow (the
    CI matrix runs each). write_variant_parquet holds the file to the
    codec's expected elements (exact); spec mode reads each leaf as the
    connector maps it, an independent derivation of the same type, and
    must find the declared one. A wrong row of the codec's table refuses
    every write of that type.

    Exact mode lets a writer leave out a converted type the table lists, so
    a converted type wrongly added to a row (UTF8 on binary, which a reader
    of converted types takes for a string) would pass it: every element
    pyarrow writes is also held to the table as it stands. pyarrow 23.0.0
    to 26.0.0 write every one of them, converted types included."""
    decl = _positioned(node, position)
    column = _column("v", 7, decl, nullable=nullable)
    rows = ["null", "1", '{"x": 1}', "[1]"]
    storage = variant.encode_json(rows, column).array
    written = write_variant_parquet(
        _table([column], [storage]), [column], {"v": _plan(column)}
    )
    elements = _schema_elements_from_footer(_footer(written.body))
    fault = variant_layout_fault(elements, 1, column, _plan(column), mode="spec")
    assert fault is None, fault
    assert elements[1][3] == (1 if nullable else 0)
    expected = [want for _, want in _plan(column).expected_elements(column)]
    assert elements[1 : 1 + len(expected)] == expected


def test_write_variant_parquet_refuses_a_splice_refusal_as_a_validation_error(
    monkeypatch,
):
    """A precondition the splice refuses is pyhoglake's problem, raised
    before any upload, and says what it is."""
    import pyhoglake.parquet_schema as ps

    column = _column("v", 1, DECIMALS)
    table = _table([column], [_storage(column, DECIMAL_ROWS)])
    monkeypatch.setattr(
        ps, "variant_stamps", lambda c, p: [FooterStamp(("v",), "variant", 99)]
    )
    with pytest.raises(ValidationError, match="cannot annotate VARIANT footer"):
        write_variant_parquet(table, [column], {"v": _plan(column)})


def test_write_variant_parquet_refuses_a_file_that_reads_back_in_another_form(
    monkeypatch,
):
    import pyhoglake.parquet_schema as ps

    column = _column("v", 1, DECIMALS)
    table = _table([column], [_storage(column, DECIMAL_ROWS)])
    # No decimal stamps: the leaves read back as the integers they are.
    real = ps.variant_stamps
    monkeypatch.setattr(ps, "variant_stamps", lambda c, p: real(c, p)[:1])
    with pytest.raises(ValidationError, match="reads it back as"):
        write_variant_parquet(table, [column], {"v": _plan(column)})


def test_write_variant_parquet_refuses_a_layout_that_is_not_the_declarations(
    monkeypatch,
):
    import pyhoglake.parquet_schema as ps

    column = _column("v", 1, DECIMALS)
    table = _table([column], [_storage(column, DECIMAL_ROWS)])
    monkeypatch.setattr(ps, "variant_layout_fault", lambda *a, **k: "forced")
    with pytest.raises(ValidationError, match="not its declaration's: forced"):
        write_variant_parquet(table, [column], {"v": _plan(column)})


def test_stamped_decimal_leaves_read_back_with_their_statistics():
    """pyarrow reads a stamped INT32/INT64 leaf as decimal128(p, s), with
    min and max in its scale. pyarrow 21 and 22 cannot extract statistics
    for an INT32/INT64 DECIMAL column at all, one reason the floor is 23."""
    column = _column("v", 1, DECIMALS)
    table = _table([column], [_storage(column, DECIMAL_ROWS)])
    written = write_variant_parquet(table, [column], {"v": _plan(column)})
    chunks = {
        written.metadata.row_group(0).column(i).path_in_schema: i
        for i in range(written.metadata.num_columns)
    }
    group = written.metadata.row_group(0)
    p = group.column(chunks["v.typed_value.p.typed_value"]).statistics
    assert (p.min, p.max) == (Decimal("1234567.89"), Decimal("1234567.89"))
    assert p.null_count == 3
    q = group.column(chunks["v.typed_value.q.typed_value"]).statistics
    assert (q.min, q.max) == (Decimal(-999), Decimal(-999))
    arr = group.column(chunks["v.typed_value.arr.typed_value.list.element.typed_value"])
    assert (arr.statistics.min, arr.statistics.max) == (Decimal("-0.9"), Decimal("0.5"))


def test_write_variant_parquet_stores_no_arrow_schema():
    """A stored ARROW:schema describes the group as the
    plain struct with integer leaves, and pyarrow reads that back."""
    column = _column("v", 1, DECIMALS)
    table = _table([column], [_storage(column, DECIMAL_ROWS)])
    written = write_variant_parquet(table, [column], {"v": _plan(column)})
    metadata = written.metadata.metadata or {}
    assert b"ARROW:schema" not in metadata


def test_top_level_reports_each_columns_element_index():
    elements = _schema_elements(str(STAMPED))
    for element, _, index in _top_level(elements):
        assert elements[index] is element
    # And variant_layout_fault starts where it says.
    columns = corpus_columns(golden_table())
    for (_, _, index), column in zip(_top_level(elements), columns, strict=True):
        assert variant_layout_fault(elements, index, column, _plan(column)) is None


def test_a_footer_read_from_a_bytesio_buffer_is_the_same():
    raw = STAMPED.read_bytes()
    sink = io.BytesIO(raw)
    with sink.getbuffer() as view:
        assert _footer_bytes(view) == footer_oracle.split(raw)
