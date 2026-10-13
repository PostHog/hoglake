"""pyhoglake.variant's codec API: targets, reports, chunks, refusals, verify.

The bytes themselves are pinned by tests/variant_conformance (suites E, P,
S and the properties); this file pins what surrounds them.
"""

from __future__ import annotations

import copy
import io
import itertools
import json
import pickle
import uuid
from collections import OrderedDict, UserDict
from datetime import UTC, date, datetime, time
from decimal import Decimal
from functools import partial
from pathlib import Path
from types import MappingProxyType

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyhoglake import (
    UnsupportedShreddingError,
    UnsupportedTypeError,
    ValidationError,
    VariantEncodingError,
    VariantReport,
    variant,
)
from pyhoglake import _variant_codec as codec
from pyhoglake.models import Column
from pyhoglake.variant import MAX_VARIANT_DEPTH, VARIANT_NULL, json_unshreddable_paths

DATA = Path(__file__).parent / "data"

DECL = {
    "type": "object",
    "fields": [
        {"name": "s", "type": "string"},
        {"name": "n", "type": "int64"},
        {"name": "tags", "type": "array", "element": {"type": "string"}},
        {"name": "any", "type": "variant"},
    ],
}

INT64 = {"type": "int64"}
EVENT = {"type": "object", "fields": [{"name": "a", "type": "int64"}]}
LIST = {"type": "array", "element": {"type": "int64"}}


def column(decl=DECL, *, nullable=True, name="props", type_="variant"):
    params = None if decl is None else {"shredding": decl}
    return Column(name, type_, 3, 0, nullable, params)


def storage(rows: list[dict | None], decl) -> pa.StructArray:
    """Hand-made storage of a declaration's write form."""
    return pa.array(rows, variant.storage_type(decl))


# -- targets ------------------------------------------------------------------


def test_a_column_gives_the_declaration_and_nullability():
    encoded = variant.encode_json(['{"s":"x"}'], column(nullable=False))
    assert encoded.array.type == variant.storage_type(DECL)
    with pytest.raises(VariantEncodingError) as raised:
        variant.encode_json(['{"s":"x"}', None], column(nullable=False))
    assert (raised.value.reason, raised.value.row, raised.value.column) == (
        "sql_null_in_not_null",
        1,
        "props",
    )
    unshredded = variant.encode_json(["1"], column(None)).array
    assert unshredded.type == variant.storage_type()


def test_a_column_and_a_declaration_are_not_both_given():
    with pytest.raises(TypeError, match="not both"):
        variant.encode_json(["1"], column(), shredding=DECL)
    with pytest.raises(TypeError, match="not both"):
        variant.encode_python([1], column(), nullable=True)


def test_a_column_of_another_type_is_refused():
    with pytest.raises(ValidationError, match="'props' is long, not variant"):
        variant.encode_json(["1"], column(type_="long"))
    assert variant.encode_json(["1"], column(type_="VARIANT")).array.type


def test_nulling_invalid_rows_is_refused_for_a_not_null_target():
    # An invalid row written as SQL NULL would be the NULL the target
    # refuses; and SQL NULL into it raises whatever on_invalid says (D5).
    with pytest.raises(ValidationError, match="'props' is NOT NULL"):
        variant.encode_json(["1"], column(nullable=False), on_invalid="null")
    with pytest.raises(ValidationError, match="the target is NOT NULL"):
        variant.encode_python([1], nullable=False, on_invalid="null")
    with pytest.raises(VariantEncodingError, match="SQL NULL into a NOT NULL"):
        variant.encode_python([1, None], nullable=False)
    with pytest.raises(ValueError, match="'raise' or 'null'"):
        variant.encode_python([1], on_invalid="skip")


def test_variant_null_into_a_not_null_target_is_a_value():
    encoded = variant.encode_python([VARIANT_NULL], nullable=False)
    assert encoded.array.null_count == 0
    assert variant.to_python(encoded.array) == [VARIANT_NULL]


def test_input_that_is_not_json_text_is_refused():
    # A TypeError however it arrives: the argument is wrong, not the data,
    # and a caller catching ValidationError for bad rows must not catch it.
    for values in (pa.array([1, 2]), pa.chunked_array([pa.array([1, 2])])):
        with pytest.raises(TypeError, match="not int64$"):
            variant.encode_json(values)
    with pytest.raises(TypeError, match="not bool$"):
        variant.encode_json(pa.array([True]))
    with pytest.raises(TypeError, match="encode_python"):
        variant.encode_json([{"a": 1}])


@pytest.mark.parametrize(
    "kind", [pa.binary(), pa.large_binary(), pa.binary_view()], ids=str
)
def test_json_text_as_arrow_binary_is_read_as_bytes_rows_are(kind):
    # The usual Arrow form of a message payload; a cast to string would
    # check every row, and fail the batch on one that is not UTF-8.
    rows = [b'{"a":1}', None, b"null", '"é"'.encode(), b'"\xff"']
    values = pa.array(rows, kind)
    for given in (values, pa.chunked_array([values[:2], values[2:]])):
        encoded = variant.encode_json(given, on_invalid="null")
        expected = variant.encode_json(rows, on_invalid="null")
        assert encoded.array.equals(expected.array)
        assert variant.to_python(encoded.array) == [
            {"a": 1},
            None,
            VARIANT_NULL,
            "é",
            None,
        ]
        assert encoded.report.invalid_by_reason == {"invalid_unicode": 1}


@pytest.mark.parametrize(
    "encode", [variant.encode_json, variant.encode_python], ids=["json", "python"]
)
@pytest.mark.parametrize("target", [EVENT, "props"], ids=["declaration", "name"])
def test_a_column_is_a_catalog_column(encode, target):
    # variant_field and storage_type take the declaration positionally
    # (variant_field second, storage_type first), so it is an easy slip
    # here, where the second argument is the column.
    with pytest.raises(TypeError, match="pass a declaration as shredding="):
        encode(['{"a":1}'] if encode is variant.encode_json else [{"a": 1}], target)


@pytest.mark.parametrize(
    "values",
    [
        pa.array(['{"a":1}', None], pa.json_()),
        pa.chunked_array([pa.array(['{"a":1}'], pa.json_())]),
        pa.array([{"a": 1}]),
        pa.scalar({"a": 1}),
    ],
    ids=["array", "chunked", "struct", "scalar"],
)
def test_encode_python_refuses_arrow(values):
    # Iterated, Arrow is pa.Scalar rows, each of no Variant type: under
    # on_invalid="null" a column of SQL NULLs, the data gone.
    with pytest.raises(TypeError, match="encode_json, or other Arrow values as"):
        variant.encode_python(values, on_invalid="null")


_TABLE = pa.table({"id": [1, 2, 3, 4], "p": [{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}]})


@pytest.mark.parametrize(
    "values",
    [_TABLE, _TABLE.to_batches()[0], _TABLE.to_reader(max_chunksize=1)],
    ids=["table", "batch", "reader"],
)
def test_encode_python_refuses_arrow_tables(values):
    # Iterated, a table or batch is its columns, a reader its batches: one
    # row each, of no Variant type, so under on_invalid="null" four rows
    # would become two SQL NULLs, or four, and nothing else.
    with pytest.raises(TypeError, match=r"table\.column\(name\)\.to_pylist\(\)"):
        variant.encode_python(values, on_invalid="null")


@pytest.mark.parametrize(
    "values",
    [_TABLE, _TABLE.to_batches()[0], _TABLE.to_reader(max_chunksize=1)],
    ids=["table", "batch", "reader"],
)
def test_encode_json_refuses_arrow_tables(values):
    # The JSON text is one column of the table, which encode_json takes.
    with pytest.raises(TypeError, match=r"pass table\.column\(name\)$"):
        variant.encode_json(values, on_invalid="null")


#: An object of one int64 field a.
OBJECT_A = {"type": "object", "fields": [{"name": "a", "type": "int64"}]}


@pytest.mark.parametrize(
    "read", [variant.to_python, variant.verify], ids=["to_python", "verify"]
)
def test_the_readers_check_a_catalog_columns_declaration(read):
    # A column is the readers' target as it is the encoders': one that
    # declares nothing (D11) is the unshredded form, {"type": "variant"},
    # where no target at all is no check of the form.
    shredded = variant.encode_python([{"a": 1}], shredding=OBJECT_A).array
    plain = variant.encode_python([{"a": 1}]).array
    for params in (None, {}, {"shredding": None}, {"shredding": {"type": "variant"}}):
        undeclared = Column("p", "variant", 3, 0, True, params)
        read(plain, undeclared)
        with pytest.raises(
            VariantEncodingError, match="^variant column 'p': "
        ) as raised:
            read(shredded, undeclared)
        assert (raised.value.reason, raised.value.column) == ("invalid_shredding", "p")
    with pytest.raises(VariantEncodingError, match="differs at value"):
        read(shredded, shredding={"type": "variant"})
    read(shredded)
    read(shredded, column(OBJECT_A))
    with pytest.raises(VariantEncodingError, match="column 'props'") as raised:
        read(plain, column(OBJECT_A))
    # Files of a writer that honours no declaration are no form of it.
    assert str(raised.value).endswith(
        "; storage from a writer that honours no declaration, as DuckDB's, "
        "is read without the column"
    )
    with pytest.raises(VariantEncodingError) as raised:
        read(plain, shredding=OBJECT_A)
    assert str(raised.value).endswith("differs at value)")
    # A catalog declaration this client refuses is an old client (D12),
    # which reading cannot check against, so says it does not know it.
    for decl in ({"type": "decimal4", "precision": 10, "scale": 0}, {"type": "x"}):
        with pytest.raises(
            UnsupportedShreddingError, match="^column 'props'.* does not know"
        ):
            read(plain, column(decl))
    with pytest.raises(ValidationError, match="is string, not variant"):
        read(plain, column(type_="string"))
    with pytest.raises(TypeError, match="pass a column or shredding, not both"):
        read(shredded, column(OBJECT_A), shredding=OBJECT_A)
    with pytest.raises(TypeError, match="pass a declaration as shredding="):
        read(shredded, OBJECT_A)


def test_a_column_is_not_a_declaration():
    # The mirror of passing a declaration as the column, which the grammar
    # would refuse only as "$ is not a JSON object".
    target = column(OBJECT_A)
    second = "pass the column as the second argument"
    for call in (
        partial(variant.encode_json, ["1"]),
        partial(variant.encode_python, [1]),
        partial(variant.to_python, variant.encode_python([1]).array),
        partial(variant.verify, variant.encode_python([1]).array),
    ):
        with pytest.raises(TypeError, match=second):
            call(shredding=target)
    with pytest.raises(TypeError, match=r"pass \(column\.type_params or \{\}\)"):
        variant.storage_type(target)


@pytest.mark.parametrize("params", [None, {}, {"shredding": None}])
def test_a_column_that_declares_nothing_is_unshredded(params):
    # D11: the three stored forms of "no declaration" are one.
    target = Column("p", "variant", 3, 0, True, params)
    encoded = variant.encode_json(['{"a":1}'], target)
    assert encoded.array.type.equals(variant.storage_type(), check_metadata=True)
    assert variant.to_python(encoded.array) == [{"a": 1}]


@pytest.mark.parametrize(
    ("encode", "one_row"),
    [
        (variant.encode_json, '{"a":1}'),
        (variant.encode_json, b"12"),
        (variant.encode_python, "abc"),
        (variant.encode_python, {"a": 1}),
        (variant.encode_python, b"ab"),
        (variant.encode_python, bytearray(b"ab")),
        (variant.encode_python, memoryview(b"ab")),
    ],
)
def test_one_bare_row_is_refused_not_iterated(encode, one_row):
    # A str or dict is iterable, and would be split into wrong rows.
    with pytest.raises(TypeError, match=r"pass \[value\] for a single row"):
        encode(one_row)


def test_an_aware_time_is_refused_at_a_time_leaf_too():
    decl = {"type": "object", "fields": [{"name": "t", "type": "time"}]}
    encoded = variant.encode_python(
        [{"t": time(1)}, {"t": [time(2, tzinfo=UTC)]}, {"t": time(3, tzinfo=UTC)}],
        shredding=decl,
        on_invalid="null",
    )
    assert encoded.report.invalid_by_reason == {"aware_time": 2}
    assert encoded.report.first_invalid == (None, 1, "aware_time", "$.t[0]")
    with pytest.raises(VariantEncodingError) as raised:
        variant.encode_python([{"t": time(3, tzinfo=UTC)}], shredding=decl)
    assert (raised.value.reason, raised.value.path) == ("aware_time", "$.t")


@pytest.mark.parametrize("decl", [None, DECL])
def test_no_rows_is_an_empty_array(decl):
    for encoded in (
        variant.encode_json([], shredding=decl),
        variant.encode_python(iter(()), shredding=decl),
    ):
        assert isinstance(encoded.array, pa.StructArray)
        assert len(encoded.array) == 0
        assert encoded.array.type.equals(
            variant.storage_type(decl), check_metadata=True
        )
        assert encoded.report.rows == 0


# -- declarations ---------------------------------------------------------------


@pytest.mark.parametrize(
    "decl",
    [
        {"type": "geometry"},
        {"type": "object", "fields": [{"name": "a", "type": "interval"}]},
        {"type": "array", "element": {"type": "variant2"}},
        # Below the first level, in objects and arrays.
        {"type": "array", "element": {"type": "array", "element": {"type": "x"}}},
        {
            "type": "object",
            "fields": [
                {
                    "name": "a",
                    "type": "array",
                    "element": {
                        "type": "object",
                        "fields": [{"name": "b", "type": "geography"}],
                    },
                }
            ],
        },
    ],
    ids=["root", "field", "element", "element-of-element", "field-of-element"],
)
def test_a_node_type_this_client_does_not_know_fails_closed(decl):
    with pytest.raises(UnsupportedShreddingError, match="upgrade pyhoglake") as raised:
        variant.storage_type(decl)
    assert isinstance(raised.value, UnsupportedTypeError)
    assert isinstance(raised.value, TypeError)
    with pytest.raises(UnsupportedShreddingError):
        variant.encode_json(["1"], shredding=decl)


@pytest.mark.parametrize("container", ["array", "object"])
def test_an_unknown_type_at_the_deepest_level_fails_closed(container):
    # At depth 16, the deepest a declaration nests: no shallower search
    # than the grammar's own bound may stop looking for it.
    decl: dict = {"type": "geometry"}
    for _ in range(variant.MAX_SHREDDING_DEPTH):
        if container == "array":
            decl = {"type": "array", "element": decl}
        else:
            decl = {"type": "object", "fields": [{"name": "a", **decl}]}
    with pytest.raises(UnsupportedShreddingError, match="upgrade pyhoglake"):
        variant.storage_type(decl)
    # The same declaration of a known type is valid.
    variant.storage_type(json.loads(json.dumps(decl).replace("geometry", "int64")))


def test_the_codec_knows_every_type_the_grammar_does():
    # The grammar's vocabulary is pinned to the server's (test_variant_ddl);
    # a type it gains that the codec cannot write would pass the grammar,
    # then fail every encode as an old client's, though it is this one.
    assert codec._KNOWN_TYPES == {
        "object",
        "array",
        "variant",
        *variant.PRIMITIVE_TYPES,
        *variant.DECIMAL_TYPES,
    }


def _nested_arrays(depth: int) -> dict:
    decl: dict = {"type": "int64"}
    for _ in range(depth):
        decl = {"type": "array", "element": decl}
    return decl


def _nested_objects(depth: int) -> dict:
    decl: dict = {"type": "int64"}
    for _ in range(depth):
        decl = {"type": "object", "fields": [{"name": "a", **decl}]}
    return decl


@pytest.mark.parametrize(
    ("decl", "words"),
    [
        ({"type": "object", "fields": []}, r"\$ has no fields"),
        ({"type": "decimal4", "precision": 10, "scale": 0}, "precision 10"),
        # The search for unknown types runs first, on a declaration nothing
        # has checked yet, and must leave each of these to the grammar.
        ({"fields": []}, r"\$ has no type"),
        ({"type": 5}, r"\$ has no type"),
        ({"type": ["object"]}, r"\$ has no type"),
        ({"type": "object", "fields": [5]}, "without a name"),
        (_nested_arrays(3000), "more than 16 levels deep"),
        (_nested_objects(3000), "more than 16 levels deep"),
    ],
    ids=[
        "no-fields",
        "precision",
        "no-type",
        "int-type",
        "list-type",
        "field-not-an-object",
        "3000-deep-arrays",
        "3000-deep-objects",
    ],
)
def test_any_other_fault_is_the_grammars_refusal(decl, words):
    with pytest.raises(ValidationError, match=words) as raised:
        variant.storage_type(decl)
    assert not isinstance(raised.value, UnsupportedShreddingError)


@pytest.mark.parametrize(
    "decl",
    [
        # A newer grammar: a key on a known type, a relaxed limit.
        {"type": "string", "collation": "x"},
        {"type": "decimal4", "precision": 10, "scale": 0},
    ],
)
def test_a_catalog_declaration_this_client_refuses_is_unsupported(decl):
    # The server accepted the column's declaration, so a refusal here is
    # an old client, not a caller's mistake (D12); a declaration handed in
    # as shredding= is the caller's, and keeps the grammar's refusal.
    with pytest.raises(UnsupportedShreddingError, match="upgrade pyhoglake") as raised:
        variant.encode_json(["1"], column(decl))
    assert "column 'props'" in str(raised.value)
    assert isinstance(raised.value.__cause__, ValidationError)
    with pytest.raises(ValidationError):
        variant.encode_json(["1"], shredding=decl)


def test_an_unknown_type_in_a_catalog_column_names_the_column():
    # A table may have several variant columns: the refusal says which one
    # holds the newer declaration, chained from the codec's own.
    for call in (
        partial(variant.encode_json, ["1"]),
        partial(variant.encode_python, [1]),
        partial(variant.to_python, variant.encode_python([1]).array),
        partial(variant.verify, variant.encode_python([1]).array),
    ):
        with pytest.raises(UnsupportedShreddingError) as raised:
            call(column({"type": "interval"}))
        assert str(raised.value) == (
            "column 'props': type_params.shredding node $ has type 'interval', "
            "which this version of pyhoglake does not know; upgrade pyhoglake"
        )
        assert isinstance(raised.value.__cause__, UnsupportedShreddingError)


def test_verify_holds_storage_to_a_not_null_columns_refusal_of_sql_null():
    # D5: storage bound for a NOT NULL column may hold no SQL NULL, which
    # pyarrow would write as a present group of empty bytes. A Variant
    # null is a value. to_python reads the row all the same.
    rows = [{"s": "x"}, VARIANT_NULL, None, None]
    storage_ = variant.encode_python(rows, column()).array
    target = column(nullable=False)
    with pytest.raises(VariantEncodingError) as encoding:
        variant.encode_python(rows, target)
    for given in (storage_, pa.chunked_array([storage_[:1], storage_[1:]])):
        with pytest.raises(VariantEncodingError) as raised:
            variant.verify(given, target, canonical=False)
        error = raised.value
        assert (error.reason, error.column, error.row, error.path) == (
            "sql_null_in_not_null",
            "props",
            2,
            None,
        )
        assert (
            str(error)
            == str(encoding.value)
            == ("variant column 'props' row 2: SQL NULL into a NOT NULL column")
        )
        assert variant.to_python(given, target) == [
            {"s": "x"},
            VARIANT_NULL,
            None,
            None,
        ]
        variant.verify(given, column())
        variant.verify(given, shredding=DECL)
    variant.verify(storage_[:2], target)


def test_a_declaration_of_any_mapping_and_tuple_is_its_json_shape():
    # The grammar, variant_field and ops.add_column take these
    # (test_variant_ddl); so does every entry point of the codec, which
    # canonicalises a plain copy, since json writes only dicts and lists.
    proxy = MappingProxyType(
        {
            "type": "object",
            "fields": (
                OrderedDict(name="a", type="string"),
                UserDict(
                    name="b",
                    type="array",
                    element=MappingProxyType({"type": "int64"}),
                ),
                MappingProxyType(
                    {"name": "p", "type": "decimal8", "precision": 18, "scale": 2}
                ),
            ),
        }
    )
    plain = json.loads(json.dumps(variant._plain(proxy)))
    assert type(plain["fields"][1]) is dict
    assert codec.compile_plan(proxy) is codec.compile_plan(plain)
    for form in ("write", "read"):
        assert variant.storage_type(proxy, form=form) == variant.storage_type(
            plain, form=form
        )
    rows = [{"a": "x", "b": [1, 2], "p": Decimal("1.25")}]
    for encode, values in (
        (variant.encode_python, rows),
        (variant.encode_json, ['{"a":"x","b":[1,2]}']),
    ):
        want = encode(values, shredding=plain).array
        assert encode(values, shredding=proxy).array.equals(want)
        assert encode(values, column(proxy)).array.equals(want)
    array = variant.encode_python(rows, shredding=plain).array
    for target in ({"shredding": proxy}, {"column": column(proxy)}):
        assert variant.to_python(array, **target) == rows
        variant.verify(array, **target)
    assert variant.storage_type(UserDict(type="int64")) == variant.storage_type(INT64)


def test_only_decimal_widths_of_one_precision_and_scale_share_storage():
    # Each leaf's type names its precision and scale, so no two
    # declarations share a form, but for one overlap: decimal4 and decimal8
    # read back as decimal128(p, s), decimal16's write form, and all three
    # hold the same values. A check of a form takes any of the three.
    decls = [{"type": kind} for kind in sorted(variant.PRIMITIVE_TYPES)]
    decls += [
        {"type": kind, "precision": p, "scale": s}
        for kind, top in variant.DECIMAL_TYPES.items()
        for p, s in ((9, 2), (9, 0), (18, 2), (top, 0))
        if p <= top
    ]
    for one, other in itertools.combinations(decls, 2):
        shared = {
            variant.storage_type(one, form=a) == variant.storage_type(other, form=b)
            for a in ("write", "read")
            for b in ("write", "read")
        }
        overlap = (one.get("precision"), one.get("scale")) == (
            other.get("precision"),
            other.get("scale"),
        ) and {one["type"], other["type"]} <= set(variant.DECIMAL_TYPES)
        assert (True in shared) == overlap, (one, other)
    widths = [
        {"type": kind, "precision": 9, "scale": 2} for kind in variant.DECIMAL_TYPES
    ]
    for source in widths:
        array = variant.encode_python([Decimal("1.25")], shredding=source).array
        read_back = array.cast(variant.storage_type(source, form="read"))
        for declared in widths:
            assert variant.to_python(read_back, shredding=declared) == [Decimal("1.25")]
            variant.verify(read_back, shredding=declared)


def test_plans_are_cached_on_canonical_json():
    reordered = {
        "fields": list(reversed([dict(reversed(f.items())) for f in DECL["fields"]]))
    }
    reordered["type"] = "object"
    assert codec.compile_plan(DECL) is codec.compile_plan(dict(DECL))
    # Field order is the declaration's; key order inside a node is not.
    assert codec.compile_plan(reordered) is not codec.compile_plan(DECL)
    same_nodes = {
        "type": "object",
        "fields": [dict(reversed(f.items())) for f in DECL["fields"]],
    }
    assert codec.compile_plan(same_nodes) is codec.compile_plan(DECL)


def test_storage_type_forms():
    with pytest.raises(ValueError, match="'write' or 'read'"):
        variant.storage_type(DECL, form="parquet")


# -- reports ----------------------------------------------------------------------


def test_report_counts_by_declared_path():
    rows = [
        '{"s":"a","n":1,"tags":["x",null,1],"any":{"z":1},"extra":true}',
        '{"s":null,"n":"two","tags":"none"}',
        '{"s":3}',
        "[1]",
        "null",
        None,
        '{"s":"\\ud800"}',
    ]
    encoded = variant.encode_json(rows, column(), on_invalid="null")
    report = encoded.report
    assert (report.rows, report.sql_null_rows, report.invalid_rows) == (7, 1, 1)
    assert report.invalid_by_reason == {"invalid_unicode": 1}
    assert report.first_invalid == ("props", 6, "invalid_unicode", "$.s")

    def key(path: str) -> tuple[str, str]:
        return ("props", path)

    # Objects land typed (with or without a residual); a non-object falls back.
    assert report.typed[key("$")] == 3
    assert report.fallback[key("$")] == 1
    assert report.variant_null[key("$")] == 1
    # A present null is not a mismatch; a missing field is in no count.
    assert (report.typed[key("$.s")], report.fallback[key("$.s")]) == (1, 1)
    assert report.variant_null[key("$.s")] == 1
    assert (report.typed[key("$.n")], report.fallback[key("$.n")]) == (1, 1)
    assert (report.typed[key("$.tags")], report.fallback[key("$.tags")]) == (1, 1)
    # Under an array's element path, the counts are of elements.
    assert report.typed[key("$.tags[*]")] == 1
    assert report.fallback[key("$.tags[*]")] == 1
    assert report.variant_null[key("$.tags[*]")] == 1
    # A variant node declares no type, so it has no counts.
    assert key("$.any") not in report.typed
    assert report.encode_seconds > 0


def test_reports_merge():
    first = variant.encode_json(
        ['{"s":"x"}', "1e400"], shredding=DECL, on_invalid="null"
    )
    second = variant.encode_json(
        ['{"s":1}', '{"a":1,"a":1}', None], shredding=DECL, on_invalid="null"
    )
    total = VariantReport()
    total.merge(first.report)
    total.merge(second.report)
    assert (total.rows, total.invalid_rows, total.sql_null_rows) == (5, 2, 1)
    assert total.invalid_by_reason == {"non_finite_number": 1, "duplicate_key": 1}
    assert total.first_invalid == (None, 1, "non_finite_number", None)
    assert total.typed[(None, "$.s")] == 1 and total.fallback[(None, "$.s")] == 1
    assert total.encode_seconds == pytest.approx(
        first.report.encode_seconds + second.report.encode_seconds
    )
    # Variant nulls merge too: a null-heavy key is not a mismatch (18a).
    third = variant.encode_json(['{"s":null}', "null"], shredding=DECL)
    total.merge(third.report)
    assert total.variant_null[(None, "$.s")] == 1
    assert total.variant_null[(None, "$")] == 1


def test_the_first_invalid_row_is_kept():
    # D18: the first, not the last, whatever the reasons.
    report = variant.encode_json(
        ["1", '{"a":1,"a":2}', "NaN", "{"], on_invalid="null"
    ).report
    assert report.first_invalid == (None, 1, "duplicate_key", None)
    assert report.invalid_rows == 3


def test_paths_keep_a_dotted_name_apart_from_a_nested_one():
    decl = {
        "type": "object",
        "fields": [
            {"name": "a.b", "type": "string"},
            {
                "name": "a",
                "type": "object",
                "fields": [{"name": "b", "type": "string"}],
            },
        ],
    }
    rows = ['{"a.b":"x","a":{"b":1}}', '{"a.b":2,"a":{"b":"y"},"":3}']
    report = variant.encode_json(rows, shredding=decl).report
    assert report.typed == {
        (None, "$"): 2,
        (None, '$["a.b"]'): 1,
        (None, "$.a"): 2,
        (None, "$.a.b"): 1,
    }
    assert report.fallback[(None, '$["a.b"]')] == report.fallback[(None, "$.a.b")] == 1
    with pytest.raises(VariantEncodingError) as raised:
        variant.encode_json(['{"x.y":{"[0]":{"":"\\ud800"}}}'], shredding=decl)
    assert raised.value.path == '$["x.y"]["[0]"][""]'


def test_paths_escape_a_fixed_set_of_characters():
    # A bracket, a quote or a backslash makes a name a quoted step, and
    # control characters, surrogates and line separators are escaped in
    # it. Nothing else is: isprintable() follows the interpreter's Unicode
    # (U+1F6DC is not printable on 3.11 and is on 3.13), and a path is a
    # report key and a metric label, the same on every Python.
    for name in ("a\U0001f6dc", "\u00ad", "\u0378", "é"):
        assert codec.member(name) == "." + name
    for name, path in (
        ('a"b', '["a\\"b"]'),
        ("a\\b", '["a\\\\b"]'),
        ("a[", '["a["]'),
        ("a]", '["a]"]'),
        # Quoted, and still not escaped: é is no character of the set.
        ("é.b", '["é.b"]'),
        ("a\nb", '["a\\nb"]'),
        ("\x00", '["\\u0000"]'),
        ("\x1f", '["\\u001f"]'),
        ("\x7f", '["\\u007f"]'),
        ("\x85", '["\\u0085"]'),
        ("\x9f", '["\\u009f"]'),
        # A name to quote that holds one to escape too.
        ("a.\ud800", '["a.\\ud800"]'),
        ("a[\n", '["a[\\n"]'),
        ("\u2028", '["\\u2028"]'),
        ("\u2029", '["\\u2029"]'),
        ("\ud800", '["\\ud800"]'),
        ("\udfff", '["\\udfff"]'),
    ):
        assert codec.member(name) == path


def test_unshreddable_paths_are_the_reports_paths():
    # json_unshreddable_paths is logged beside the per-path counts; a leaf
    # must be one key in both, a dotted name and a nested one apart.
    decl = {
        "type": "object",
        "fields": [
            {"name": "a.b", "type": "date"},
            {"name": "a", "type": "object", "fields": [{"name": "b", "type": "uuid"}]},
            {"name": "t[0]", "type": "array", "element": {"type": "binary"}},
        ],
    }
    paths = [path for path, _ in json_unshreddable_paths(decl)]
    assert paths == ['$["a.b"]', "$.a.b", '$["t[0]"][*]']
    rows = ['{"a.b":1,"a":{"b":2},"t[0]":[3]}']
    report = variant.encode_json(rows, shredding=decl).report
    assert set(paths) <= {path for _, path in report.fallback}


def test_unshredded_columns_report_no_paths():
    report = variant.encode_json(['{"a":1}', "2"]).report
    assert report.typed == report.fallback == report.variant_null == {}


# -- chunks and rows that fail part-way ---------------------------------------


def _encode(values, decl, **chunking):
    report = VariantReport()
    chunks = codec.encode(
        values,
        codec.compile_plan(decl),
        report,
        json_text=True,
        column=None,
        nullable=True,
        on_invalid="null",
        **chunking,
    )
    return chunks, report


def test_chunks_are_capped_by_rows():
    rows = [f'{{"s":"{i}","tags":["{i}"]}}' for i in range(10)] + [None, "[]"]
    chunks, _ = _encode(rows, DECL, chunk_rows=4)
    assert [len(chunk) for chunk in chunks] == [4, 4, 4]
    whole, _ = _encode(rows, DECL)
    assert len(whole) == 1
    assert pa.chunked_array(chunks).combine_chunks().equals(whole[0])


def test_chunks_are_capped_by_encoded_bytes():
    # Each row is about 70 bytes of metadata, residual and typed strings;
    # a 150-byte cap closes a chunk every third row. Without the cap one
    # chunk would hold every row and a binary child could pass int32
    # offsets.
    rows = ['{"s":"' + "x" * 30 + '","other":"' + "y" * 20 + '"}'] * 7
    chunks, _ = _encode(rows, DECL, chunk_bytes=150)
    assert [len(chunk) for chunk in chunks] == [3, 3, 1]
    whole, _ = _encode(rows, DECL)
    assert pa.chunked_array(chunks).combine_chunks().equals(whole[0])


@pytest.mark.parametrize(
    "decl",
    [None, {"type": "variant"}, INT64, EVENT, LIST],
    ids=["undeclared", "root-variant", "unmatched-primitive", "object", "array"],
)
def test_bytes_kept_in_value_count_against_the_byte_cap(decl):
    # Every row is a 105-byte long string the declaration does not take,
    # so its bytes all land in a value column, beside 3 bytes of metadata:
    # a 250-byte cap closes a chunk every third row. The unshredded column
    # is the common case, and without the count only its metadata would
    # bound a chunk.
    rows = ['"' + "x" * 100 + '"'] * 7
    chunks, _ = _encode(rows, decl, chunk_bytes=250)
    assert [len(chunk) for chunk in chunks] == [3, 3, 1]
    # A chunk closes on reaching the cap, not only on passing it.
    chunks, _ = _encode(rows, decl, chunk_bytes=216)
    assert [len(chunk) for chunk in chunks] == [2, 2, 2, 1]


@pytest.mark.parametrize(
    ("decl", "value"),
    [({"type": "string"}, "x" * 105), ({"type": "binary"}, b"x" * 105)],
    ids=["string", "binary"],
)
def test_typed_text_and_bytes_count_against_the_byte_cap(decl, value):
    # 105 bytes in the typed leaf, which is a binary child too, beside 3
    # of metadata: a 250-byte cap closes a chunk every third row.
    report = VariantReport()
    chunks = codec.encode(
        [value] * 7,
        codec.compile_plan(decl),
        report,
        json_text=False,
        column=None,
        nullable=True,
        on_invalid="raise",
        chunk_bytes=250,
    )
    assert [len(chunk) for chunk in chunks] == [3, 3, 1]
    assert all(chunk.field("value").null_count == len(chunk) for chunk in chunks)


def test_metadata_counts_against_the_byte_cap():
    # Every value lands in a typed int64, so the row's dictionary is all the
    # binary bytes it adds: about 126 a row, a chunk every second row.
    names = ["k" * 40 + str(i) for i in range(3)]
    decl = {"type": "object", "fields": [{"name": n, "type": "int64"} for n in names]}
    rows = ["{" + ",".join(f'"{n}":1' for n in names) + "}"] * 5
    chunks, _ = _encode(rows, decl, chunk_bytes=200)
    assert [len(chunk) for chunk in chunks] == [2, 2, 1]
    assert all(chunk.field("value").null_count == len(chunk) for chunk in chunks)


def test_list_elements_count_against_the_byte_cap():
    # Typed numbers add no binary bytes, but list offsets are int32 too.
    decl = {"type": "array", "element": {"type": "int64"}}
    rows = ["[" + ",".join(["1"] * 100) + "]"] * 3
    chunks, _ = _encode(rows, decl, chunk_bytes=150)
    assert [len(chunk) for chunk in chunks] == [2, 1]


def test_large_input_comes_back_chunked():
    rows = ["1"] * (codec.CHUNK_ROWS + 1)
    array = variant.encode_json(pa.array(rows, pa.json_())).array
    assert isinstance(array, pa.ChunkedArray)
    assert [len(chunk) for chunk in array.chunks] == [codec.CHUNK_ROWS, 1]
    assert array.type == variant.storage_type()
    variant.verify(array)


@pytest.mark.parametrize(
    "bad",
    [
        # Fails in the second field, after the first was placed.
        '{"s":"ok","n":1,"tags":["x"],"any":"\\ud800"}',
        # Fails in the second element, after the first was placed.
        '{"s":"ok","tags":["x","\\ud800"]}',
        # Fails in the residual, before any field.
        '{"s":"ok","z":"\\ud800"}',
    ],
)
def test_a_row_that_fails_part_way_leaves_nothing_behind(bad):
    rows = ['{"s":"a","tags":["p"]}', bad, '{"s":"b","tags":["q","r"]}']
    encoded = variant.encode_json(rows, shredding=DECL, on_invalid="null")
    assert variant.to_python(encoded.array, shredding=DECL) == [
        {"s": "a", "tags": ["p"]},
        None,
        {"s": "b", "tags": ["q", "r"]},
    ]
    variant.verify(encoded.array, shredding=DECL)
    chunks, _ = _encode([bad, *rows], DECL, chunk_rows=2)
    assert sum(len(chunk) for chunk in chunks) == 4


# -- structure and verify ------------------------------------------------------


def test_structure_fault_finds_nulls_under_present_parents():
    unshredded = variant.storage_type()
    bad = pa.StructArray.from_arrays(
        [pa.array([b"\x01\x00\x00", None]), pa.array([b"\x0c\x01", b"\x00"])],
        fields=list(unshredded),
    )
    assert codec.structure_fault(bad) == "metadata is null under a present parent"
    masked = pa.StructArray.from_arrays(
        [pa.array([b"\x01\x00\x00", None]), pa.array([b"\x0c\x01", None])],
        fields=list(unshredded),
        mask=pa.array([False, True]),
    )
    assert codec.structure_fault(masked) is None
    decl = {"type": "array", "element": {"type": "int64"}}
    group = variant.storage_type(decl).field("typed_value").type.value_type
    elements = pa.StructArray.from_arrays(
        [pa.array([None, None], pa.binary()), pa.array([1, 2])],
        fields=list(group),
        mask=pa.array([False, True]),
    )
    listed = pa.ListArray.from_arrays(
        pa.array([0, 2], pa.int32()),
        elements,
        type=variant.storage_type(decl).field("typed_value").type,
    )
    array = pa.StructArray.from_arrays(
        [pa.array([b"\x01\x00\x00"]), pa.array([None], pa.binary()), listed],
        fields=list(variant.storage_type(decl)),
    )
    assert (
        codec.structure_fault(array)
        == "typed_value.element is null under a present list"
    )
    assert (
        codec.structure_fault(variant.encode_json(["[1]", None], shredding=decl).array)
        is None
    )
    # Under a null list, the same null element is never written: no fault.
    null_list = pa.ListArray.from_arrays(
        pa.array([0, 2], pa.int32()),
        elements,
        type=variant.storage_type(decl).field("typed_value").type,
        mask=pa.array([True]),
    )
    array = pa.StructArray.from_arrays(
        [pa.array([b"\x01\x00\x00"]), pa.array([b"\x00"], pa.binary()), null_list],
        fields=list(variant.storage_type(decl)),
    )
    assert codec.structure_fault(array) is None


def test_structure_fault_spells_columns_as_paths_do():
    # A field named "a.b" is not b inside a, and a line break in a name
    # does not break the message.
    for name, spelt in (("a.b", '["a.b"]'), ("a\nb", '["a\\nb"]')):
        decl = {"type": "object", "fields": [{"name": name, "type": "int64"}]}
        group = variant.storage_type(decl).field("typed_value").type
        typed = pa.StructArray.from_arrays(
            [pa.array([None], group.field(0).type)], fields=list(group)
        )
        array = pa.StructArray.from_arrays(
            [pa.array([b"\x01\x00\x00"]), pa.array([None], pa.binary()), typed],
            fields=list(variant.storage_type(decl)),
        )
        assert codec.structure_fault(array) == (
            f"typed_value{spelt} is null under a present parent"
        )


@pytest.mark.parametrize("list_present", [True, False])
def test_structure_fault_looks_below_an_element_only_under_a_present_list(
    list_present,
):
    # A present element whose REQUIRED field group is null: a fault in a
    # list a reader sees, nothing in a null one, however deep.
    decl = {
        "type": "array",
        "element": {"type": "object", "fields": [{"name": "a", "type": "int64"}]},
    }
    kind = variant.storage_type(decl)
    element_type = kind.field("typed_value").type.value_type
    object_type = element_type.field("typed_value").type
    field_group = object_type.field("a").type
    missing_group = pa.StructArray.from_arrays(
        [pa.nulls(1, pa.binary()), pa.nulls(1, pa.int64())],
        fields=list(field_group),
        mask=pa.array([True]),
    )
    element = pa.StructArray.from_arrays(
        [
            pa.nulls(1, pa.binary()),
            pa.StructArray.from_arrays([missing_group], fields=list(object_type)),
        ],
        fields=list(element_type),
    )
    listed = pa.ListArray.from_arrays(
        pa.array([0, 1], pa.int32()),
        element,
        type=kind.field("typed_value").type,
        mask=pa.array([not list_present]),
    )
    array = pa.StructArray.from_arrays(
        [pa.array([b"\x01\x00\x00"]), pa.array([b"\x00"], pa.binary()), listed],
        fields=list(kind),
    )
    assert codec.structure_fault(array) == (
        "typed_value.element.typed_value.a is null under a present parent"
        if list_present
        else None
    )


@pytest.mark.parametrize("nullable", [False, True], ids=["required", "optional"])
@pytest.mark.parametrize("large", [False, True], ids=["list", "large_list"])
def test_structure_fault_holds_a_required_element_in_any_list(large, nullable):
    # Storage built elsewhere may hold a large_list; DuckDB's elements are
    # OPTIONAL, and a null one there is a missing element, not a fault.
    element = pa.struct(
        [pa.field("value", pa.binary()), pa.field("typed_value", pa.int64())]
    )
    field = pa.field("element", element, nullable=nullable)
    kind = pa.large_list(field) if large else pa.list_(field)
    values = pa.StructArray.from_arrays(
        [pa.nulls(1, pa.binary()), pa.nulls(1, pa.int64())],
        fields=list(element),
        mask=pa.array([True]),
    )
    listed = (pa.LargeListArray if large else pa.ListArray).from_arrays(
        pa.array([0, 1], pa.int64() if large else pa.int32()), values, type=kind
    )
    array = pa.StructArray.from_arrays(
        [pa.array([b"\x01\x00\x00"]), pa.nulls(1, pa.binary()), listed],
        names=["metadata", "value", "typed_value"],
    )
    assert codec.structure_fault(array) == (
        None if nullable else "typed_value.element is null under a present list"
    )


def test_structure_fault_looks_below_a_list_only_under_a_present_parent():
    # A present list under a null object holds elements no reader sees: a
    # null REQUIRED element there is no fault, as under a null list.
    decl = {
        "type": "object",
        "fields": [{"name": "tags", "type": "array", "element": INT64}],
    }
    kind = variant.storage_type(decl)
    object_type = kind.field("typed_value").type
    tags_type = object_type.field("tags").type
    list_type = tags_type.field("typed_value").type
    elements = pa.StructArray.from_arrays(
        [pa.nulls(1, pa.binary()), pa.nulls(1, pa.int64())],
        fields=list(list_type.value_type),
        mask=pa.array([True]),
    )
    tags = pa.StructArray.from_arrays(
        [
            pa.nulls(1, pa.binary()),
            pa.ListArray.from_arrays(
                pa.array([0, 1], pa.int32()), elements, type=list_type
            ),
        ],
        fields=list(tags_type),
    )
    for object_present in (True, False):
        typed = pa.StructArray.from_arrays(
            [tags], fields=list(object_type), mask=pa.array([not object_present])
        )
        array = pa.StructArray.from_arrays(
            [pa.array([b"\x01\x00\x00"]), pa.array([b"\x0c\x01"]), typed],
            fields=list(kind),
        )
        assert codec.structure_fault(array) == (
            "typed_value.tags.typed_value.element is null under a present list"
            if object_present
            else None
        )


#: Storage, its declaration, what to_python reads (or the reason it
#: raises), and the reason verify refuses it with.
BAD_STORAGE = [
    # A reader ignores the flag, as DuckDB 1.5.5 sets it on any dictionary.
    pytest.param(
        [{"metadata": b"\x11\x02\x00\x01\x02ba", "value": b"\x00"}],
        None,
        VARIANT_NULL,
        "malformed_variant",
        id="sorted-flag-on-unsorted-strings",
    ),
    pytest.param(
        [{"metadata": b"\x02\x00\x00", "value": b"\x00"}],
        None,
        "malformed_variant",
        "malformed_variant",
        id="metadata-version-2",
    ),
    pytest.param(
        [{"metadata": b"\x01\x00\x00", "value": b"\x0c"}],
        None,
        "malformed_variant",
        "malformed_variant",
        id="truncated",
    ),
    pytest.param(
        [{"metadata": b"\x01\x00\x00", "value": b"\x0c\x01\x00"}],
        None,
        "malformed_variant",
        "malformed_variant",
        id="trailing-bytes",
    ),
    pytest.param(
        [{"metadata": b"\x01\x00\x00", "value": b"\x54"}],
        None,
        "malformed_variant",
        "malformed_variant",
        id="unknown-primitive-21",
    ),
    pytest.param(
        [{"metadata": b"\x11\x01\x00\x01a", "value": b"\x02\x01\x01\x00\x01\x00"}],
        None,
        "malformed_variant",
        "malformed_variant",
        id="field-id-beyond-metadata",
    ),
    pytest.param(
        [
            {
                "metadata": b"\x11\x01\x00\x01a",
                "value": b"\x02\x02\x00\x00\x00\x01\x02\x00\x00",
            }
        ],
        None,
        "malformed_variant",
        "malformed_variant",
        id="repeated-key",
    ),
    pytest.param(
        [{"metadata": b"\x01\x00\x00", "value": b"\x18\x01" + b"\x00" * 7}],
        None,
        1,
        "non_canonical",
        id="int-wider-than-needed",
    ),
    pytest.param(
        [
            {
                "metadata": b"\x01\x00\x00",
                "value": b"\x13\x01\x00\x00\x00\x00\x02\x0c\x01",
            }
        ],
        None,
        [1],
        "non_canonical",
        id="is-large-under-256",
    ),
    pytest.param(
        [{"metadata": b"\x01\x00\x00", "value": b"\x38\x00\x00\xc0\x3f"}],
        None,
        1.5,
        "non_canonical",
        id="float",
    ),
    pytest.param(
        [{"metadata": b"\x11\x02\x00\x01\x02ab", "value": b"\x02\x01\x00\x00\x01\x00"}],
        None,
        {"a": None},
        "non_canonical",
        id="metadata-key-not-used",
    ),
    pytest.param(
        [
            {
                "metadata": b"\x11\x02\x00\x01\x02ab",
                "value": b"\x02\x02\x00\x01\x02\x00\x04\x0c\x02\x0c\x01",
            }
        ],
        None,
        {"a": 1, "b": 2},
        "non_canonical",
        id="values-out-of-key-order",
    ),
    pytest.param(
        [{"metadata": b"\x01\x00\x00", "value": b"\x0c\x01", "typed_value": 1}],
        INT64,
        "invalid_shredding",
        "invalid_shredding",
        id="primitive-with-both-sides",
    ),
    pytest.param(
        [{"metadata": b"\x01\x00\x00", "value": None, "typed_value": None}],
        INT64,
        VARIANT_NULL,
        "invalid_shredding",
        id="row-with-neither-side",
    ),
    pytest.param(
        [
            {
                "metadata": b"\x11\x01\x00\x01a",
                "value": b"\x02\x01\x00\x00\x02\x0c\x01",
                "typed_value": {"a": {"value": None, "typed_value": 1}},
            }
        ],
        EVENT,
        "invalid_shredding",
        "invalid_shredding",
        id="declared-key-in-residual",
    ),
    pytest.param(
        [
            {
                "metadata": b"\x11\x01\x00\x01a",
                "value": b"\x02\x01\x00\x00\x02\x0c\x01",
                "typed_value": None,
            }
        ],
        EVENT,
        "invalid_shredding",
        "invalid_shredding",
        id="object-in-value-beside-null-typed",
    ),
    pytest.param(
        [
            {
                "metadata": b"\x01\x00\x00",
                "value": b"\x0c\x01",
                "typed_value": {"a": {"value": None, "typed_value": None}},
            }
        ],
        EVENT,
        "invalid_shredding",
        "invalid_shredding",
        id="non-object-with-shredded-fields",
    ),
    pytest.param(
        [
            {
                # The dictionary holds no key, so the empty residual is the
                # one fault.
                "metadata": b"\x01\x00\x00",
                "value": b"\x02\x00\x00",
                "typed_value": {"a": {"value": None, "typed_value": None}},
            }
        ],
        EVENT,
        {},
        "non_canonical",
        id="empty-residual",
    ),
    pytest.param(
        [
            {
                "metadata": b"\x01\x00\x00",
                "value": None,
                "typed_value": {"a": {"value": None, "typed_value": 1}},
            }
        ],
        EVENT,
        {"a": 1},
        "invalid_shredding",
        id="shredded-key-not-in-metadata",
    ),
    pytest.param(
        [
            {
                "metadata": b"\x01\x00\x00",
                "value": None,
                "typed_value": [{"value": None, "typed_value": None}],
            }
        ],
        LIST,
        [None],
        "invalid_shredding",
        id="missing-element",
    ),
    pytest.param(
        [
            {
                "metadata": b"\x01\x00\x00",
                "value": b"\x03\x01\x00\x02\x0c\x01",
                "typed_value": None,
            }
        ],
        LIST,
        [1],
        "invalid_shredding",
        id="array-in-value-beside-null-typed",
    ),
    pytest.param(
        [
            {
                "metadata": b"\x01\x00\x00",
                "value": b"\x03\x00\x00",
                "typed_value": [{"value": None, "typed_value": 1}],
            }
        ],
        LIST,
        "invalid_shredding",
        "invalid_shredding",
        id="array-with-both-sides",
    ),
]


def le(value: int, size: int) -> bytes:
    return value.to_bytes(size, "little", signed=True)


#: The sorted dictionary of the one key "a".
KEY_A = b"\x11\x01\x00\x01a"
#: The sorted dictionary ["a", "b"].
KEY_AB = b"\x11\x02\x00\x01\x02ab"


def field_a(value: bytes) -> bytes:
    """The object {"a": <value>}, whose offsets say value is all of it."""
    return b"\x02\x01\x00\x00" + bytes((len(value),)) + value


#: Unshredded bytes that break one rule each: the metadata, the value, the
#: reason and the words of the refusal, which name the rule (a fault one
#: check misses can fail another, with other words). A non_canonical one
#: reads, and passes verify(canonical=False).
DECODER_FAULTS = [
    # The dictionary (VE:74-146).
    pytest.param(
        b"\x01", b"\x00", "malformed_variant", r": truncated$",
        id="metadata-without-its-size",
    ),
    # Version 1 in the low nibble, whose high bit is not the sorted flag.
    pytest.param(
        b"\x09\x00\x00", b"\x00", "malformed_variant", "metadata version 9",
        id="metadata-version-9",
    ),
    pytest.param(
        b"\x01\x02\x00", b"\x00", "malformed_variant", "metadata is truncated",
        id="metadata-short-of-its-offsets",
    ),
    pytest.param(
        b"\x01\x00\x00\x00", b"\x00", "malformed_variant", "length does not match",
        id="metadata-trailing-byte",
    ),
    pytest.param(
        b"\x01\x01\x01\x02ab", b"\x00", "malformed_variant", "do not climb from 0",
        id="metadata-first-offset-1",
    ),
    pytest.param(
        b"\x01\x02\x00\x02\x01a", b"\x00", "malformed_variant", "do not climb",
        id="metadata-offsets-fall",
    ),
    pytest.param(
        b"\x01\x01\x00\x01\xff", b"\x00", "malformed_variant", "not UTF-8",
        id="metadata-key-not-utf8",
    ),
    # Values (VE:147-446).
    pytest.param(
        b"\x01\x00\x00", b"\x09a", "malformed_variant", "short string is truncated",
        id="short-string-truncated",
    ),
    pytest.param(
        b"\x01\x00\x00", b"\x05\xff", "malformed_variant", "string is not UTF-8",
        id="short-string-not-utf8",
    ),
    pytest.param(
        b"\x01\x00\x00", b"\x40\x05\x00\x00\x00abc", "malformed_variant",
        "binary or string is truncated", id="long-string-truncated",
    ),
    # A size of 2**24 is four bytes, the top one set: read as three, it
    # is an empty string with bytes after it.
    pytest.param(
        b"\x01\x00\x00", b"\x40" + le(2**24, 4) + b"x", "malformed_variant",
        "binary or string is truncated", id="long-string-of-2**24-truncated",
    ),
    pytest.param(
        b"\x01\x00\x00", b"\x3c" + le(2**24 + 1, 4) + b"x", "malformed_variant",
        "binary or string is truncated", id="binary-of-2**24+1-truncated",
    ),
    pytest.param(
        b"\x01\x00\x00", b"\x20\x02\x01\x00", "malformed_variant",
        "decimal is truncated", id="decimal-truncated",
    ),
    pytest.param(
        b"\x01\x00\x00", b"\x50" + bytes(8), "malformed_variant", "uuid is truncated",
        id="uuid-truncated",
    ),
    pytest.param(
        b"\x01\x00\x00", b"\x20\x27" + le(1, 4), "malformed_variant", "scale 39",
        id="decimal-scale-39",
    ),
    pytest.param(
        b"\x01\x00\x00", b"\x20\x00" + le(2_000_000_000, 4), "malformed_variant",
        "decimal4 holds 10 digits", id="decimal4-of-10-digits",
    ),
    pytest.param(
        b"\x01\x00\x00", b"\x24\x00" + le(9 * 10**18, 8), "malformed_variant",
        "decimal8 holds 19 digits", id="decimal8-of-19-digits",
    ),
    pytest.param(
        b"\x01\x00\x00", b"\x28\x00" + le(10**38, 16), "malformed_variant",
        "decimal16 holds 39 digits", id="decimal16-of-39-digits",
    ),
    pytest.param(
        b"\x01\x00\x00", b"\x44" + le(86_400_000_000, 8), "malformed_variant",
        "not a time of day", id="time-of-a-whole-day",
    ),
    pytest.param(
        b"\x01\x00\x00", b"\x44" + le(-1, 8), "malformed_variant",
        "not a time of day", id="time-before-midnight",
    ),
    pytest.param(
        b"\x11\x01\x00\x01a", b"\x02\x01\x00\x00\x05\x0c\x01", "malformed_variant",
        "object is truncated", id="object-past-its-end",
    ),
    pytest.param(
        b"\x01\x00\x00", b"\x03\x01\x00\x05\x0c\x01", "malformed_variant",
        "array is truncated", id="array-past-its-end",
    ),
    # {"a": 1} and [1], each one byte short of its last offset: the value
    # would be read past the end of the buffer.
    pytest.param(
        KEY_A, b"\x02\x01\x00\x00\x02\x0c", "malformed_variant",
        "object is truncated", id="object-one-byte-short",
    ),
    pytest.param(
        b"\x01\x00\x00", b"\x03\x01\x00\x02\x0c", "malformed_variant",
        "array is truncated", id="array-one-byte-short",
    ),
    pytest.param(
        b"\x01\x00\x00", b"\x03\x01\x00\x03\x0c\x01\x00", "malformed_variant",
        "does not end at the next offset", id="array-element-short-of-its-offset",
    ),
    # An object's values lie in any order, each ending where the next one
    # up begins (VE:293-296): {"a": 1, "b": 1} with one value for both,
    # {"a": 1, "b": ""} where b is a's second byte, and DuckDB 1.5.5's
    # {"a": <64-byte string>}, an empty short string and then the bytes.
    pytest.param(
        KEY_AB, b"\x02\x02\x00\x01\x00\x00\x02\x0c\x01", "malformed_variant",
        "two object fields start at one offset", id="fields-sharing-a-value",
    ),
    pytest.param(
        KEY_AB, b"\x02\x02\x00\x01\x00\x01\x02\x0c\x01", "malformed_variant",
        "does not end where the next value begins", id="fields-overlapping",
    ),
    pytest.param(
        KEY_A, b"\x02\x01\x00\x00\x41\x01" + b"x" * 64, "malformed_variant",
        "does not end where the next value begins", id="field-short-of-its-end",
    ),
    # Offsets count from the first byte of the first value (VE:293, 349),
    # so bytes before it, or under an empty container, are as hidden as
    # bytes after one: [1] and {"a": 1} after a Variant null, {"a": 1}
    # after a whole short string, and {}, [], {"a": {}} and {"a": []}
    # holding bytes.
    pytest.param(
        b"\x01\x00\x00", b"\x03\x01\x01\x03\x00\x0c\x01", "malformed_variant",
        "first array element does not start at offset 0", id="array-first-offset-1",
    ),
    pytest.param(
        KEY_A, b"\x02\x01\x00\x01\x03\x00\x0c\x01", "malformed_variant",
        "no object field starts at offset 0", id="object-first-offset-1",
    ),
    pytest.param(
        KEY_A, b"\x02\x01\x00\x08\x0a\x1dsecret!\x0c\x01", "malformed_variant",
        "no object field starts at offset 0", id="object-after-a-hidden-string",
    ),
    pytest.param(
        b"\x01\x00\x00", b"\x02\x00\x05hello", "malformed_variant",
        "empty object's last offset is not 0", id="empty-object-holding-bytes",
    ),
    pytest.param(
        b"\x01\x00\x00", b"\x03\x00\x05hello", "malformed_variant",
        "empty array's last offset is not 0", id="empty-array-holding-bytes",
    ),
    pytest.param(
        KEY_A, field_a(b"\x02\x00\x05hello"), "malformed_variant",
        "empty object's last offset is not 0", id="nested-empty-object-holding-bytes",
    ),
    pytest.param(
        KEY_A, field_a(b"\x03\x00\x05hello"), "malformed_variant",
        "empty array's last offset is not 0", id="nested-empty-array-holding-bytes",
    ),
    pytest.param(
        b"\x01\x00\x00", bytes((21 << 2,)) + bytes(16), "malformed_variant",
        "unknown primitive type 21", id="primitive-type-21",
    ),
    pytest.param(
        b"\x01\x00\x00", bytes((63 << 2,)) + bytes(16), "malformed_variant",
        "unknown primitive type 63", id="primitive-type-63",
    ),
    # One byte short, as an object's last field: the object's end bounds
    # the field, so no check of the bytes after the value catches these.
    pytest.param(
        KEY_A, field_a(b"\x20\x02" + le(1, 4)[:3]), "malformed_variant",
        "decimal is truncated", id="decimal-one-byte-short",
    ),
    pytest.param(
        KEY_A, field_a(b"\x3c" + le(4, 4) + b"abc"), "malformed_variant",
        "binary or string is truncated", id="binary-one-byte-short",
    ),
    pytest.param(
        KEY_A, field_a(b"\x40" + le(64, 4) + b"x" * 63), "malformed_variant",
        "binary or string is truncated", id="long-string-one-byte-short",
    ),
    pytest.param(
        KEY_A, field_a(b"\x50" + bytes(15)), "malformed_variant",
        "uuid is truncated", id="uuid-one-byte-short",
    ),
    # {"a": {"b": <3-byte string>}, "c": 1}, whose inner object ends after
    # two of the string's bytes: the third is c's header, inside the outer
    # object but past the inner one.
    pytest.param(
        b"\x11\x03\x00\x01\x02\x03abc",
        b"\x02\x02\x00\x02\x00\x08\x0a"
        + b"\x02\x01\x01\x00\x03\x0dxy"
        + b"\x0c\x01",
        "malformed_variant", "short string is truncated",
        id="field-past-its-own-object",
    ),
    # The canonical encoding (D8, D9).
    pytest.param(
        b"\x01\x01\x00\x01a", b"\x02\x01\x00\x00\x02\x0c\x01", "non_canonical",
        "sorted_strings is not set", id="unsorted-flag-on-a-dictionary",
    ),
    # Empty is 01 00 00 (D8); DuckDB 1.5.5 writes 11 00 00.
    pytest.param(
        b"\x11\x00\x00", b"\x0c\x01", "non_canonical",
        "sorted_strings is not set", id="sorted-flag-on-the-empty-dictionary",
    ),
    pytest.param(
        b"\x51\x01\x00\x00\x00\x01\x00a", b"\x02\x01\x00\x00\x02\x0c\x01",
        "non_canonical", "metadata offset width", id="metadata-offsets-too-wide",
    ),
    pytest.param(
        b"\x01\x00\x00", b"\x07\x01\x00\x00\x02\x00\x0c\x01", "non_canonical",
        "offset width is not the smallest", id="array-offsets-too-wide",
    ),
    pytest.param(
        b"\x11\x01\x00\x01a", b"\x12\x01\x00\x00\x00\x02\x0c\x01",
        "non_canonical", "field id width", id="field-ids-too-wide",
    ),
    pytest.param(
        b"\x11\x03\x00\x01\x02\x03abc",
        b"\x02\x03\x00\x01\x02\x00\x04\x02\x06\x0c\x01\x0c\x03\x0c\x02",
        "non_canonical", "not laid out in key order", id="values-in-another-order",
    ),
    pytest.param(
        b"\x01\x00\x00", b"\x40\x3f\x00\x00\x00" + b"x" * 63, "non_canonical",
        "under 64 bytes", id="long-string-of-63-bytes",
    ),
    pytest.param(
        b"\x01\x00\x00", b"\x24\x00" + le(123, 8), "non_canonical",
        "precision 3 is not in its smallest width", id="decimal8-of-3-digits",
    ),
    # Reserved header bits, which a reader ignores (VE:90, 210, 228): the
    # value is {"a": [1]}, as pyhoglake writes it but for the one bit.
    pytest.param(
        b"\x31\x01\x00\x01a", b"\x02\x01\x00\x00\x06\x03\x01\x00\x02\x0c\x01",
        "non_canonical", "metadata header sets a reserved bit",
        id="metadata-reserved-bit",
    ),
    pytest.param(
        KEY_A, b"\x82\x01\x00\x00\x06\x03\x01\x00\x02\x0c\x01", "non_canonical",
        "container header sets a reserved bit", id="object-reserved-bit",
    ),
    pytest.param(
        KEY_A, b"\x02\x01\x00\x00\x06\x23\x01\x00\x02\x0c\x01", "non_canonical",
        "container header sets a reserved bit", id="array-lowest-reserved-bit",
    ),
    pytest.param(
        KEY_A, b"\x02\x01\x00\x00\x06\x43\x01\x00\x02\x0c\x01", "non_canonical",
        "container header sets a reserved bit", id="array-middle-reserved-bit",
    ),
    pytest.param(
        KEY_A, b"\x02\x01\x00\x00\x06\x83\x01\x00\x02\x0c\x01", "non_canonical",
        "container header sets a reserved bit", id="array-highest-reserved-bit",
    ),
]  # fmt: skip


@pytest.mark.parametrize(("metadata", "value", "reason", "words"), DECODER_FAULTS)
def test_decoder_faults(metadata, value, reason, words):
    array = unshredded(metadata, value)
    if reason == "non_canonical":
        variant.to_python(array)
        variant.verify(array, canonical=False)
    else:
        for read in (variant.to_python, partial(variant.verify, canonical=False)):
            with pytest.raises(VariantEncodingError, match=words) as raised:
                read(array)
            assert raised.value.reason == reason
    with pytest.raises(VariantEncodingError, match=words) as raised:
        variant.verify(array)
    assert (raised.value.reason, raised.value.row) == (reason, 0)


def test_a_fault_in_bytes_is_found_at_its_path():
    # {"a": [1, <a short string that is not UTF-8>]}, all in value.
    array = unshredded(KEY_A, field_a(b"\x03\x02\x00\x02\x04\x0c\x01\x05\xff"))
    for read in (variant.to_python, variant.verify):
        with pytest.raises(VariantEncodingError, match="not UTF-8") as raised:
            read(array)
        assert (raised.value.row, raised.value.path) == (0, "$.a[1]")


def test_a_fault_in_a_shredded_array_is_at_its_rows_own_index():
    # The elements of every row are one Arrow array: an index into it is
    # not the element's index in its own row.
    def element(value=None, typed=None):
        return {"value": value, "typed_value": typed}

    rows = [
        {"metadata": b"\x01\x00\x00", "typed_value": [element(typed=1)] * 3},
        {
            "metadata": b"\x01\x00\x00",
            "typed_value": [element(typed=1), element(value=b"\x05\xff")],
        },
    ]
    array = storage(rows, LIST)
    for read in (variant.to_python, variant.verify):
        with pytest.raises(VariantEncodingError, match="not UTF-8") as raised:
            read(array, shredding=LIST)
        assert (raised.value.row, raised.value.path) == (1, "$[1]")


def test_fields_that_share_a_value_never_multiply_the_work():
    # Each level's fields a and b both point at the level below, so 16
    # levels, 114 bytes, would decode to 65,536 leaves, and 30 to 2**30.
    # The outermost object is refused before anything below it is read.
    value = b"\x0c\x01"
    for _ in range(16):
        value = b"\x02\x02\x00\x01\x00\x00" + bytes((len(value),)) + value
    array = unshredded(KEY_AB, value)
    for read in (variant.to_python, partial(variant.verify, canonical=False)):
        with pytest.raises(VariantEncodingError, match="start at one offset") as raised:
            read(array)
        assert raised.value.path == "$"


def unshredded(metadata: bytes, value: bytes) -> pa.StructArray:
    return pa.StructArray.from_arrays(
        [pa.array([metadata], pa.binary()), pa.array([value], pa.binary())],
        fields=list(variant.storage_type()),
    )


@pytest.mark.parametrize(("rows", "decl", "read", "refused"), BAD_STORAGE)
def test_storage_faults(rows, decl, read, refused):
    array = storage(rows, decl)
    if isinstance(read, str) and read in codec.Reason.__args__:
        with pytest.raises(VariantEncodingError) as raised:
            variant.to_python(array, shredding=decl)
        assert raised.value.reason == read
    else:
        assert variant.to_python(array, shredding=decl) == [read]
    with pytest.raises(VariantEncodingError) as raised:
        variant.verify(array, shredding=decl)
    assert raised.value.reason == refused
    assert raised.value.row == 0
    if refused == "non_canonical":
        variant.verify(array, shredding=decl, canonical=False)


#: {"b": 1, "a": 2} with its fields in field-id order over the unsorted
#: dictionary ["b", "a"], as DuckDB 1.5.5 lists them.
UNSORTED_BA = b"\x01\x02\x00\x01\x02ba"
OBJECT_BA = b"\x02\x02\x00\x01\x00\x02\x04\x0c\x01\x0c\x02"
#: The same object over the sorted dictionary ["a", "b"], ids 1 then 0.
DESCENDING_BA = b"\x02\x02\x01\x00\x00\x02\x04\x0c\x01\x0c\x02"

#: Bytes a reader takes and the spec forbids a writer to write: a sorted
#: flag over a dictionary that is not sorted and unique, and object fields
#: out of key order (VE:146, 454-465). The metadata, the value, what
#: to_python reads, and the words of verify's refusal.
MISORDERED = [
    pytest.param(
        b"\x11\x02\x00\x01\x02aa", b"\x00", VARIANT_NULL, "sorted and unique",
        id="sorted-flag-over-a-repeat",
    ),
    pytest.param(
        b"\x11\x02\x00\x01\x02ba", OBJECT_BA, {"a": 2, "b": 1},
        "sorted_strings is set", id="sorted-flag-over-unsorted-keys",
    ),
    pytest.param(
        UNSORTED_BA, OBJECT_BA, {"a": 2, "b": 1}, "keys are not sorted",
        id="fields-in-id-order",
    ),
    pytest.param(
        b"\x11\x02\x00\x01\x02ab", DESCENDING_BA, {"a": 2, "b": 1},
        "keys are not sorted", id="ids-out-of-order-over-a-sorted-dictionary",
    ),
    pytest.param(
        UNSORTED_BA, b"\x03\x01\x00\x0b" + OBJECT_BA, [{"a": 2, "b": 1}],
        "keys are not sorted", id="fields-in-id-order-in-an-array",
    ),
    pytest.param(
        b"\x01\x03\x00\x01\x02\x03bax", b"\x02\x01\x02\x00\x0b" + OBJECT_BA,
        {"x": {"a": 2, "b": 1}}, "keys are not sorted",
        id="fields-in-id-order-in-an-object",
    ),
]  # fmt: skip


@pytest.mark.parametrize(("metadata", "value", "read", "words"), MISORDERED)
def test_a_reader_takes_what_some_writers_misorder(metadata, value, read, words):
    # DuckDB 1.5.5 writes both, and the Trino connector reads them
    # (VariantRepairs): the flag is ignored, the fields are taken in any
    # order and given back in key order. verify holds a writer to the spec.
    array = unshredded(metadata, value)
    (got,) = variant.to_python(array)
    # repr, which shows the order of the keys.
    assert repr(got) == repr(read)
    with pytest.raises(VariantEncodingError, match=words) as raised:
        variant.verify(array, canonical=False)
    assert (raised.value.reason, raised.value.row) == ("malformed_variant", 0)
    with pytest.raises(VariantEncodingError):
        variant.verify(array)


OBJECT_C = {"type": "object", "fields": [{"name": "c", "type": "int64"}]}


@pytest.mark.parametrize(
    ("decl", "row", "read"),
    [
        (
            {"type": "string"},
            {"value": DESCENDING_BA},
            {"a": 2, "b": 1},
        ),
        (
            OBJECT_C,
            {"value": DESCENDING_BA, "typed_value": {"c": {}}},
            {"a": 2, "b": 1},
        ),
        (
            OBJECT_C,
            {"value": b"\x03\x01\x00\x0b" + DESCENDING_BA},
            [{"a": 2, "b": 1}],
        ),
        (LIST, {"value": DESCENDING_BA}, {"a": 2, "b": 1}),
    ],
    ids=["fallback", "residual", "non-object", "non-list"],
)
def test_verify_holds_every_value_column_to_key_order(decl, row, read):
    # Every place a group's value is decoded, among them the second decode
    # of a fallback for canonical verify's placement check: an object over
    # the sorted dictionary ["a", "b"] that lists b, id 1, before a.
    array = storage([{"metadata": b"\x11\x02\x00\x01\x02ab", **row}], decl)
    assert repr(variant.to_python(array, shredding=decl)) == repr([read])
    for canonical in (False, True):
        with pytest.raises(VariantEncodingError, match="keys are not sorted") as raised:
            variant.verify(array, shredding=decl, canonical=canonical)
        assert raised.value.reason == "malformed_variant"


@pytest.mark.parametrize(
    ("metadata", "value"),
    [
        (KEY_A, b"\x02\x02\x00\x00\x00\x02\x04\x0c\x01\x0c\x02"),
        (b"\x01\x02\x00\x01\x02aa", b"\x02\x02\x00\x01\x00\x02\x04\x0c\x01\x0c\x02"),
    ],
    ids=["one-id-twice", "one-name-under-two-ids"],
)
def test_an_object_never_holds_one_name_twice(metadata, value):
    # In any order, a name is one field (VE:465): which of two values a
    # reader keeps is a guess.
    array = unshredded(metadata, value)
    with pytest.raises(VariantEncodingError, match="two fields named 'a'") as raised:
        variant.to_python(array)
    assert (raised.value.reason, raised.value.path) == ("malformed_variant", "$")
    for canonical in (False, True):
        with pytest.raises(VariantEncodingError):
            variant.verify(array, canonical=canonical)


#: DuckDB 1.5.5's own ``v::JSON`` of duckdb_unsorted_variant.parquet.
DUCKDB_JSON = [
    '{"a":{"b":"x","y":true},"m":[{"c":null,"q":2}],"z":1}',
    '{"a":{"b":"w","y":false},"m":[{"c":"v","q":4}],"z":3}',
    '{"a":{"b":"u","k":7,"y":null},"e":0,"m":[{"c":[1],"q":6}],"z":5}',
    '[{"a":2,"z":{"b":2,"y":1}}]',
    "null",
]


def test_duckdb_dictionaries_and_field_order_read():
    # tests/data/README.md: DuckDB 1.5.5 sets sorted_strings on every
    # dictionary, sorted or not, and lists an object's fields in the order
    # they were inserted, here in shredded rows, in an object inside an
    # array, and in an object inside that. to_python reads what DuckDB
    # does, keys in order; verify refuses both as a writer's faults. The
    # last row is SQL NULL, which DuckDB writes as a present 00.
    column = pq.read_table(DATA / "duckdb_unsorted_variant.parquet").column("v")
    got = variant.to_python(column)
    assert got[-1] is VARIANT_NULL
    assert [
        json.dumps(None if value is VARIANT_NULL else value, separators=(",", ":"))
        for value in got
    ] == DUCKDB_JSON
    for canonical in (False, True):
        with pytest.raises(
            VariantEncodingError, match="sorted_strings is set"
        ) as raised:
            variant.verify(column, canonical=canonical)
        assert (raised.value.reason, raised.value.row) == ("malformed_variant", 0)


#: Where duckdb_64_byte_strings.parquet's rows put DuckDB 1.5.5's string
#: of exactly 64 bytes, an empty short string followed by the 64 bytes,
#: which DuckDB itself reads as "": the row, the path of the refusal, and
#: its words.
DUCKDB_64_BYTES = [
    (5, "$.k", "object field does not end where the next value begins"),
    (6, "$.a.k", "object field does not end where the next value begins"),
    (7, "$[1].k", "object field does not end where the next value begins"),
    (8, "$[0]", "64 bytes follow the value"),
    (9, "$", "64 bytes follow the value"),
    (12, "$.a[0]", "array element does not end at the next offset"),
]


def test_duckdbs_64_byte_strings_are_refused_wherever_they_sit():
    # tests/data/README.md: in an object (the first three, in value), as a
    # shredded element, at the top, and as an element in value. Reading
    # them as "" would drop the 64 bytes without a word, so both readers
    # refuse the row. The strings of 63 and 65 bytes beside them read.
    column = pq.read_table(DATA / "duckdb_64_byte_strings.parquet").column("v")
    refused = {row for row, _, _ in DUCKDB_64_BYTES}
    for row in range(len(column)):
        if row not in refused:
            variant.verify(column.slice(row, 1), canonical=False)
    assert variant.to_python(column.slice(10, 2)) == [
        {"k": "x" * 63, "n": 1},
        {"k": "x" * 65, "n": 1},
    ]
    for row, path, words in DUCKDB_64_BYTES:
        one = column.slice(row, 1)
        for read in (variant.to_python, partial(variant.verify, canonical=False)):
            with pytest.raises(VariantEncodingError, match=words) as raised:
                read(one)
            assert (raised.value.reason, raised.value.path) == (
                "malformed_variant",
                path,
            )


#: 5 as an int16, which is not its smallest width.
WIDE_5 = b"\x10\x05\x00"
KEY_B = b"\x11\x01\x00\x01b"


@pytest.mark.parametrize(
    ("decl", "row", "read", "words"),
    [
        # Beside a typed leaf that does not take it, at the root, in a
        # field and in an element.
        (
            {"type": "string"},
            {"metadata": b"\x01\x00\x00", "value": WIDE_5},
            5,
            "the integer 5 is not in its smallest width",
        ),
        (
            INT64,
            {"metadata": b"\x01\x00\x00", "value": b"\x40\x03\x00\x00\x00abc"},
            "abc",
            "under 64 bytes is not a short string",
        ),
        (
            {"type": "double"},
            {"metadata": b"\x01\x00\x00", "value": b"\x24\x02" + le(125, 8)},
            Decimal("1.25"),
            "precision 3 is not in its smallest width",
        ),
        (
            {"type": "object", "fields": [{"name": "a", "type": "string"}]},
            {"metadata": KEY_A, "typed_value": {"a": {"value": WIDE_5}}},
            {"a": 5},
            "smallest width",
        ),
        (
            {"type": "array", "element": {"type": "string"}},
            {"metadata": b"\x01\x00\x00", "typed_value": [{"value": WIDE_5}]},
            [5],
            "smallest width",
        ),
        # The residual of a partially shredded object.
        (
            EVENT,
            {
                "metadata": KEY_B,
                "value": b"\x02\x01\x00\x00\x03" + WIDE_5,
                "typed_value": {"a": {}},
            },
            {"b": 5},
            "smallest width",
        ),
        # What an object or an array node holds that is not one.
        (EVENT, {"metadata": b"\x01\x00\x00", "value": WIDE_5}, 5, "smallest width"),
        (LIST, {"metadata": b"\x01\x00\x00", "value": WIDE_5}, 5, "smallest width"),
    ],
    ids=[
        "root-fallback",
        "root-long-string",
        "root-wide-decimal",
        "field-fallback",
        "element-fallback",
        "residual",
        "non-object",
        "non-list",
    ],
)
def test_canonical_bytes_are_checked_in_every_value_column(decl, row, read, words):
    # Each place a group's value is decoded, which DECODER_FAULTS (bytes
    # in an unshredded column) never reaches.
    array = storage([row], decl)
    assert variant.to_python(array, shredding=decl) == [read]
    variant.verify(array, shredding=decl, canonical=False)
    with pytest.raises(VariantEncodingError, match=words) as raised:
        variant.verify(array, shredding=decl)
    assert (raised.value.reason, raised.value.row) == ("non_canonical", 0)


def test_a_null_optional_field_group_reads_as_missing():
    # DuckDB writes field groups OPTIONAL; a null one is a missing field to
    # a reader, and a writer fault to verify (VS:193).
    group = pa.struct(
        [pa.field("value", pa.binary()), pa.field("typed_value", pa.int64())]
    )
    kind = pa.struct(
        [
            pa.field("metadata", pa.binary(), nullable=False),
            pa.field("value", pa.binary()),
            pa.field(
                "typed_value", pa.struct([pa.field("a", group), pa.field("b", group)])
            ),
        ]
    )
    array = pa.array(
        [
            {
                "metadata": b"\x11\x01\x00\x01b",
                "value": None,
                "typed_value": {"a": None, "b": {"value": None, "typed_value": 2}},
            }
        ],
        kind,
    )
    assert variant.to_python(array) == [{"b": 2}]
    with pytest.raises(VariantEncodingError, match="group is null"):
        variant.verify(array)


def leaf_storage(typed: pa.Array) -> pa.StructArray:
    """Primitive-rooted storage around a hand-made typed leaf, a row a value."""
    kind = pa.struct(
        [
            pa.field("metadata", pa.binary(), nullable=False),
            pa.field("value", pa.binary()),
            pa.field("typed_value", typed.type),
        ]
    )
    return pa.StructArray.from_arrays(
        [
            pa.array([b"\x01\x00\x00"] * len(typed)),
            pa.nulls(len(typed), pa.binary()),
            typed,
        ],
        fields=list(kind),
    )


@pytest.mark.parametrize(
    ("typed", "words"),
    [
        # pyarrow's own conversion wraps these round midnight.
        (pa.array([86_400_000_000], pa.int64()).view(pa.time64("us")), "time of day"),
        (pa.array([-5], pa.int64()).view(pa.time64("us")), "time of day"),
        (pa.array([b"ok", b"\xff"], pa.binary()).view(pa.string()), "not UTF-8"),
        (pa.array([b"\xff"], pa.large_binary()).view(pa.large_string()), "not UTF-8"),
    ],
    ids=["time-of-a-whole-day", "negative-time", "string", "large-string"],
)
def test_typed_leaves_are_refused_at_their_row(typed, words):
    array = leaf_storage(typed)
    for read in (variant.to_python, variant.verify):
        with pytest.raises(VariantEncodingError, match=words) as raised:
            read(array)
        assert (raised.value.reason, raised.value.row) == (
            "malformed_variant",
            len(typed) - 1,
        )


@pytest.mark.parametrize(
    ("leaf", "precision", "scale", "bad"),
    [
        (pa.decimal32(5, 2), 5, 2, 100_000),
        (pa.decimal32(5, 2), 5, 2, -100_000),
        (pa.decimal32(9, 2), 9, 2, 2**31 - 1),
        (pa.decimal64(18, 2), 18, 2, 2**63 - 1),
        (pa.decimal64(3, 0), 3, 0, 1000),
        (pa.decimal128(5, 2), 5, 2, 100_000),
        (pa.decimal128(9, 2), 9, 2, -(10**9)),
        (pa.decimal128(20, 2), 20, 2, 10**30),
        (pa.decimal128(38, 0), 38, 0, 10**38),
    ],
)
def test_typed_decimals_hold_at_most_their_precision(leaf, precision, scale, bad):
    # Parquet's DECIMAL(p, s) holds p digits at most, as a Variant
    # decimal4 holds 9 (VE:442-446). Nothing upstream checks it: a
    # decimal32 viewed over an int32 holds 10 digits whatever its
    # precision, and Arrow builds and pyarrow writes a decimal128 of any.
    edge = 10**precision - 1
    good = [edge, -edge, 0]
    if pa.types.is_decimal128(leaf):
        typed = codec._decimal128([*good, bad], precision, scale)
    else:
        width = pa.int32() if pa.types.is_decimal32(leaf) else pa.int64()
        typed = pa.array([*good, bad], width).view(leaf)
    fine = leaf_storage(typed.slice(0, 3))
    assert variant.to_python(fine) == [
        Decimal(v).scaleb(-scale, codec._EXACT) for v in good
    ]
    variant.verify(fine)
    words = f"holds {len(str(abs(bad)))} digits, beyond its {precision}"
    for read in (variant.to_python, variant.verify):
        with pytest.raises(VariantEncodingError, match=words) as raised:
            read(leaf_storage(typed))
        assert (raised.value.reason, raised.value.row) == ("malformed_variant", 3)


@pytest.mark.parametrize(
    "typed",
    [
        # Units a shredded leaf does not have: read as microseconds, they
        # would be silently wrong by a factor of 1000.
        pa.array([1_700_000_000_000], pa.timestamp("ms")),
        pa.array([1_700_000_000], pa.timestamp("s", tz="UTC")),
        pa.array([1], pa.time64("ns")),
        pa.array([1], pa.time32("ms")),
        pa.array(["{}"], pa.json_()),
        pa.array([b"x" * 16], pa.binary(16)),
        pa.array([1], pa.uint8()),
        # No Variant decimal: more than 38 digits, or a negative scale.
        pa.array([10**45], pa.decimal256(50, 0)),
        pa.array([1], pa.decimal256(39, 0)),
        pa.array([1], pa.decimal256(40, 39)),
        pa.array([Decimal(100)], pa.decimal128(5, -2)),
    ],
    ids=lambda typed: str(typed.type),
)
def test_typed_leaves_of_no_shredded_type_are_refused(typed):
    for read in (variant.to_python, variant.verify):
        with pytest.raises(VariantEncodingError, match="typed_value of type") as raised:
            read(leaf_storage(typed))
        assert raised.value.reason == "invalid_shredding"


@pytest.mark.parametrize(
    "typed",
    [
        pa.array([None, 0.5], pa.float32()),
        pa.array([None, 5], pa.timestamp("ns")),
        pa.array([None, 5], pa.timestamp("ns", tz="UTC")),
    ],
    ids=lambda typed: str(typed.type),
)
def test_typed_float_and_nanosecond_leaves_are_not_canonical(typed):
    # pyhoglake writes neither (D2, D3), typed or in value: a faster
    # encoder that did would lose precision, and canonical verify says so.
    # Row 0 is a Variant null in value, beside a null leaf.
    array = pa.StructArray.from_arrays(
        [
            pa.array([b"\x01\x00\x00"] * 2),
            pa.array([b"\x00", None], pa.binary()),
            typed,
        ],
        names=["metadata", "value", "typed_value"],
    )
    variant.verify(array, canonical=False)
    variant.verify(array.slice(0, 1))
    with pytest.raises(VariantEncodingError, match="never writes") as raised:
        variant.verify(array)
    assert (raised.value.reason, raised.value.row) == ("non_canonical", 1)


def _misplaced(value: object, decl) -> pa.StructArray:
    """The storage of ``value`` under a primitive ``decl``, the Variant bytes
    in ``value`` and ``typed_value`` null, whatever the encoder chose."""
    shredded = variant.encode_python([value], shredding=decl).array
    plain = variant.encode_python([value]).array
    return pa.StructArray.from_arrays(
        [
            plain.field("metadata"),
            plain.field("value").cast(pa.binary()),
            pa.nulls(1, shredded.type.field("typed_value").type),
        ],
        fields=list(shredded.type),
    )


_UUID = uuid.UUID("00112233-4455-6677-8899-aabbccddeeff")
_AWARE = datetime(2020, 1, 2, 3, 4, 5, 6, tzinfo=UTC)
_NAIVE = datetime(2020, 1, 2, 3, 4, 5, 6)


@pytest.mark.parametrize(
    ("decl", "value", "taken"),
    [
        ({"type": "int8"}, 5, True),
        ({"type": "int8"}, -128, True),
        ({"type": "int8"}, 127, True),
        ({"type": "int8"}, 300, False),
        ({"type": "int16"}, 300, True),
        ({"type": "int16"}, -(2**15), True),
        ({"type": "int16"}, 2**15 - 1, True),
        ({"type": "int16"}, -(2**15) - 1, False),
        ({"type": "int16"}, 2**15, False),
        ({"type": "int16"}, 70_000, False),
        ({"type": "int32"}, 70_000, True),
        ({"type": "int32"}, -(2**31), True),
        ({"type": "int32"}, 2**31 - 1, True),
        ({"type": "int32"}, -(2**31) - 1, False),
        ({"type": "int32"}, 2**31, False),
        ({"type": "int64"}, -(2**63), True),
        ({"type": "int64"}, 2**63 - 1, True),
        ({"type": "int64"}, 5, True),
        ({"type": "int64"}, 2**63, False),
        ({"type": "int64"}, True, False),
        ({"type": "int64"}, Decimal(5), False),
        ({"type": "int64"}, 5.0, False),
        ({"type": "decimal4", "precision": 9, "scale": 2}, Decimal("1.25"), True),
        ({"type": "decimal4", "precision": 9, "scale": 2}, Decimal("1.250"), False),
        ({"type": "decimal4", "precision": 9, "scale": 2}, Decimal("1.2"), False),
        ({"type": "decimal4", "precision": 5, "scale": 2}, Decimal("999.99"), True),
        ({"type": "decimal4", "precision": 5, "scale": 2}, Decimal("1000.00"), False),
        ({"type": "decimal4", "precision": 5, "scale": 2}, Decimal("-1000.00"), False),
        ({"type": "decimal4", "precision": 5, "scale": 2}, Decimal("-999.99"), True),
        ({"type": "decimal4", "precision": 9, "scale": 0}, 5, False),
        ({"type": "decimal8", "precision": 18, "scale": 2}, Decimal("-1.25"), True),
        ({"type": "decimal8", "precision": 18, "scale": 12}, Decimal("1E-12"), True),
        ({"type": "decimal16", "precision": 10, "scale": 2}, Decimal("1.25"), True),
        # An integer beyond int64 is decimal16 of scale 0 (D1).
        ({"type": "decimal16", "precision": 38, "scale": 0}, 10**20, True),
        ({"type": "decimal16", "precision": 38, "scale": 0}, -(10**20), True),
        ({"type": "decimal16", "precision": 20, "scale": 0}, 10**20, False),
        ({"type": "boolean"}, True, True),
        ({"type": "boolean"}, False, True),
        ({"type": "boolean"}, 1, False),
        ({"type": "double"}, 1.5, True),
        ({"type": "double"}, float("nan"), True),
        ({"type": "double"}, 1, False),
        ({"type": "float"}, 1.5, False),
        ({"type": "string"}, "x", True),
        ({"type": "string"}, "x" * 70, True),
        ({"type": "string"}, b"x", False),
        ({"type": "binary"}, b"x", True),
        ({"type": "binary"}, "x", False),
        ({"type": "date"}, date(2020, 1, 2), True),
        ({"type": "date"}, _NAIVE, False),
        ({"type": "time"}, time(1, 2, 3, 4), True),
        ({"type": "timestamp"}, _NAIVE, True),
        ({"type": "timestamp"}, _AWARE, False),
        ({"type": "timestamptz"}, _AWARE, True),
        ({"type": "timestamptz"}, _NAIVE, False),
        ({"type": "timestamp_ns"}, _NAIVE, False),
        ({"type": "timestamptz_ns"}, _AWARE, False),
        ({"type": "uuid"}, _UUID, True),
        ({"type": "uuid"}, str(_UUID), False),
        ({"type": "variant"}, 5, False),
    ],
    ids=lambda v: str(v)[:40],
)
def test_canonical_storage_puts_a_value_where_its_leaf_takes_it(decl, value, taken):
    # Where a value goes is a function of the value and the leaf (D3, D10),
    # so the same row with a value its typed leaf takes left in value is a
    # second spelling of it, which canonical verify refuses: an encoder
    # that stopped shredding a leaf would otherwise round-trip, and pass.
    encoded = variant.encode_python([value], shredding=decl).array
    assert encoded.field("value").is_null().to_pylist() == [taken]
    variant.verify(encoded)
    if "typed_value" not in encoded.type.names:
        return
    misplaced = _misplaced(value, decl)
    # repr, as NaN is not equal to itself.
    assert repr(variant.to_python(misplaced)) == repr(variant.to_python(encoded))
    variant.verify(misplaced, canonical=False)
    if not taken:
        assert misplaced.equals(encoded)
        return
    for check in (variant.verify, lambda a: variant.verify(a, shredding=decl)):
        with pytest.raises(VariantEncodingError, match="leaf takes") as raised:
            check(misplaced)
        assert (raised.value.reason, raised.value.row, raised.value.path) == (
            "non_canonical", 0, "$"
        )  # fmt: skip


def test_canonical_storage_puts_elements_and_fields_where_their_leaves_take_them():
    # The same of an array element and of an object field, read through the
    # same groups.
    decl = {"type": "array", "element": {"type": "int64"}}
    encoded = variant.encode_python([[7, "x"]], shredding=decl).array
    elements = encoded.field("typed_value").values
    moved = pa.StructArray.from_arrays(
        [
            pa.array([b"\x0c\x07", elements.field("value")[1].as_py()]),
            pa.nulls(2, pa.int64()),
        ],
        fields=list(elements.type),
    )
    misplaced = pa.StructArray.from_arrays(
        [
            encoded.field("metadata"),
            encoded.field("value"),
            pa.ListArray.from_arrays(
                encoded.field("typed_value").offsets,
                moved.cast(elements.type),
                type=encoded.type.field("typed_value").type,
            ),
        ],
        fields=list(encoded.type),
    )
    assert variant.to_python(misplaced) == [[7, "x"]]
    variant.verify(misplaced, canonical=False)
    with pytest.raises(VariantEncodingError, match="int64 leaf takes") as raised:
        variant.verify(misplaced)
    assert (raised.value.reason, raised.value.path) == ("non_canonical", "$[0]")
    decl = {"type": "object", "fields": [{"name": "a", "type": "string"}]}
    good = storage(
        [{"metadata": KEY_A, "typed_value": {"a": {"typed_value": "hi"}}}], decl
    )
    bad = storage(
        [{"metadata": KEY_A, "typed_value": {"a": {"value": b"\x09hi"}}}], decl
    )
    assert variant.to_python(bad) == variant.to_python(good) == [{"a": "hi"}]
    variant.verify(good)
    with pytest.raises(VariantEncodingError, match="string leaf takes") as raised:
        variant.verify(bad)
    assert raised.value.path == "$.a"


@pytest.mark.parametrize(
    ("value", "leaf"),
    [
        ("x", pa.large_string()),
        ("x", pa.string_view()),
        (b"x", pa.large_binary()),
        (b"x", pa.binary_view()),
    ],
    ids=lambda v: str(v),
)
def test_a_large_or_view_leaf_takes_what_its_plain_type_does(value, leaf):
    # The leaf types pyarrow reads with binary_type=large_binary() or
    # binary_view(): placement is by the leaf's kind, not its Arrow width.
    plain = variant.encode_python([value]).array
    array = pa.StructArray.from_arrays(
        [plain.field("metadata"), plain.field("value"), pa.nulls(1, leaf)],
        names=["metadata", "value", "typed_value"],
    )
    assert variant.to_python(array) == [value]
    variant.verify(array, canonical=False)
    with pytest.raises(
        VariantEncodingError, match=f"typed {leaf} leaf takes"
    ) as raised:
        variant.verify(array)
    assert raised.value.reason == "non_canonical"


def test_large_and_view_columns_read():
    # pyarrow reads Parquet binary as binary, but storage from elsewhere
    # may be large_binary or binary_view, and a list large_list.
    for kind in (pa.large_binary(), pa.binary_view()):
        array = pa.StructArray.from_arrays(
            [pa.array([b"\x01\x00\x00"], kind), pa.array([b"\x0c\x01"], kind)],
            names=["metadata", "value"],
        )
        assert variant.to_python(array) == [1]
        variant.verify(array)
    # Typed leaves too, as pyarrow reads them with binary_type=binary_view
    # (tests/variant_conformance reads the parquet-testing files so).
    for typed, want in (
        (pa.array([b"ab"], pa.large_binary()), b"ab"),
        (pa.array([b"ab"], pa.binary_view()), b"ab"),
        (pa.array(["ab"], pa.string_view()), "ab"),
    ):
        array = leaf_storage(typed)
        assert variant.to_python(array) == [want]
        variant.verify(array)
    bad = pa.array([b"\xff"], pa.binary_view()).view(pa.string_view())
    with pytest.raises(VariantEncodingError, match="not UTF-8") as raised:
        variant.to_python(leaf_storage(bad))
    assert raised.value.reason == "malformed_variant"
    element = pa.struct(
        [pa.field("value", pa.binary()), pa.field("typed_value", pa.int64())]
    )
    kind = pa.struct(
        [
            pa.field("metadata", pa.binary(), nullable=False),
            pa.field("value", pa.binary()),
            pa.field(
                "typed_value",
                pa.large_list(pa.field("element", element, nullable=False)),
            ),
        ]
    )
    array = pa.array(
        [
            {
                "metadata": b"\x01\x00\x00",
                "value": None,
                "typed_value": [{"value": None, "typed_value": 7}],
            }
        ],
        kind,
    )
    assert variant.to_python(array) == [[7]]
    variant.verify(array)


#: pyarrow's read options, each of which reads the same file to other
#: types: large or view binary and strings, and large lists.
PARQUET_READ_OPTIONS = [
    {},
    {"binary_type": pa.large_binary()},
    {"binary_type": pa.binary_view()},
    {"list_type": pa.LargeListType},
]


@pytest.mark.parametrize("store_schema", [False, True])
@pytest.mark.parametrize(
    "options", PARQUET_READ_OPTIONS, ids=["default", "large", "view", "large-list"]
)
def test_a_declaration_is_checked_however_pyarrow_read_the_file(store_schema, options):
    # Each option reads the file to other types, which say nothing of the
    # declaration: a file with no Arrow schema in it (Trino's, DuckDB's)
    # reads to large and view types under these options.
    decl = {
        "type": "object",
        "fields": [
            {"name": "s", "type": "string"},
            {"name": "b", "type": "binary"},
            {"name": "d", "type": "decimal8", "precision": 18, "scale": 2},
            {"name": "l", "type": "array", "element": {"type": "string"}},
        ],
    }
    rows = [{"s": "x", "b": b"y", "d": Decimal("1.25"), "l": ["z"]}, {"o": 1}]
    sink = io.BytesIO()
    table = pa.table({"v": variant.encode_python(rows, shredding=decl).array})
    pq.write_table(table, sink, store_schema=store_schema)
    column = pq.read_table(sink, **options).column("v")
    assert variant.to_python(column, shredding=decl) == rows
    variant.verify(column, shredding=decl)
    # Another declaration's storage is still refused, at the column.
    other = {**decl, "fields": [{"name": "s", "type": "int64"}, *decl["fields"][1:]]}
    for read in (variant.to_python, variant.verify):
        with pytest.raises(
            VariantEncodingError, match=r"differs at typed_value\.s\.typed_value\)"
        ) as raised:
            read(column, shredding=other)
        assert raised.value.reason == "invalid_shredding"


@pytest.mark.parametrize("nest", ["object", "array"])
@pytest.mark.parametrize(
    "options", PARQUET_READ_OPTIONS[1:], ids=["large", "view", "large-list"]
)
def test_the_deepest_declaration_is_checked_however_pyarrow_read_the_file(
    nest, options
):
    # 16 levels, the grammar's most, nest the storage's types 34 deep, and
    # the string leaf at the bottom is read to a large or view type too.
    decl: dict = {"type": "string"}
    value: object = "x"
    for _ in range(16):
        if nest == "object":
            decl = {"type": "object", "fields": [{"name": "a", **decl}]}
            value = {"a": value}
        else:
            decl = {"type": "array", "element": decl}
            value = [value]
    sink = io.BytesIO()
    table = pa.table({"v": variant.encode_python([value], shredding=decl).array})
    pq.write_table(table, sink, store_schema=False)
    column = pq.read_table(sink, **options).column("v")
    assert variant.to_python(column, shredding=decl) == [value]
    variant.verify(column, shredding=decl)


def test_a_timestamp_in_any_zone_is_adjusted_to_utc():
    # Parquet's isAdjustedToUTC has no zone; storage built elsewhere may
    # spell UTC another way.
    for tz in ("UTC", "+00:00"):
        typed = pa.array([0], pa.int64()).view(pa.timestamp("us", tz=tz))
        assert variant.to_python(leaf_storage(typed)) == [
            datetime(1970, 1, 1, tzinfo=UTC)
        ]
        # And such a leaf takes an aware timestamp, as placement goes.
        plain = variant.encode_python([datetime(1970, 1, 1, tzinfo=UTC)]).array
        misplaced = pa.StructArray.from_arrays(
            [plain.field("metadata"), plain.field("value"), pa.nulls(1, typed.type)],
            names=["metadata", "value", "typed_value"],
        )
        with pytest.raises(VariantEncodingError, match="leaf takes"):
            variant.verify(misplaced)


def test_dates_and_timestamps_beyond_pythons_years_read_as_scalars():
    # Valid storage (Trino writes such values), whose values no date or
    # datetime holds: the pyarrow scalar of the type, typed or not.
    days, micros = 2_932_897, 253_402_300_800_000_000  # 10000-01-01
    for typed, want in (
        (pa.array([days], pa.int32()).view(pa.date32()), pa.scalar(days, pa.date32())),
        (
            pa.array([-micros], pa.int64()).view(pa.timestamp("us")),
            pa.scalar(-micros, pa.timestamp("us")),
        ),
        (
            pa.array([micros], pa.int64()).view(pa.timestamp("us", tz="UTC")),
            pa.scalar(micros, pa.timestamp("us", tz="UTC")),
        ),
    ):
        array = leaf_storage(typed)
        (got,) = variant.to_python(array)
        assert got.type == want.type and got.value == want.value
        variant.verify(array)
    for value, want in (
        (b"\x2c" + le(days, 4), pa.scalar(days, pa.date32())),
        (b"\x2c" + le(-719_163, 4), pa.scalar(-719_163, pa.date32())),
        (b"\x30" + le(micros, 8), pa.scalar(micros, pa.timestamp("us", tz="UTC"))),
        (b"\x34" + le(-micros, 8), pa.scalar(-micros, pa.timestamp("us"))),
    ):
        array = unshredded(b"\x01\x00\x00", value)
        (got,) = variant.to_python(array)
        assert got.type == want.type and got.value == want.value
        variant.verify(array)
    # The last values Python holds are still Python's.
    edges = unshredded(b"\x01\x00\x00", b"\x2c" + le(days - 1, 4))
    assert variant.to_python(edges) == [date(9999, 12, 31)]
    edges = unshredded(b"\x01\x00\x00", b"\x34" + le(-62_135_596_800_000_000, 8))
    assert variant.to_python(edges) == [datetime(1, 1, 1)]


def test_storage_that_is_not_a_variant_group_is_refused():
    for array in (
        pa.array([{"value": b"\x00"}]),
        pa.array([{"metadata": b"\x01\x00\x00", "value": b"\x00", "x": 1}]),
        pa.array([{"metadata": b"\x01\x00\x00", "typed_value": 1}]),
        pa.array(
            [{"metadata": b"\x01\x00\x00", "value": None, "typed_value": 1}],
            pa.struct(
                [
                    pa.field("metadata", pa.binary()),
                    pa.field("value", pa.binary()),
                    pa.field("typed_value", pa.uint8()),
                ]
            ),
        ),
        # Binary columns that are not binary.
        pa.array([{"metadata": b"\x01\x00\x00", "value": "\x00"}]),
        pa.array([{"metadata": 1, "value": b"\x00"}]),
        pa.array(
            [
                {
                    "metadata": b"\x01\x00\x00",
                    "value": None,
                    "typed_value": {"a": {"value": "x"}},
                }
            ]
        ),
        # Not a struct at all.
        pa.array([b"\x00"]),
        pa.array([[1]]),
        # No rows to read, and no chunks to read them from: the type is
        # still checked, as Table.from_batches([], schema) gives it.
        pa.chunked_array([], pa.int64()),
        pa.chunked_array([], pa.struct([pa.field("x", pa.int8())])),
    ):
        for read in (variant.to_python, variant.verify):
            with pytest.raises(VariantEncodingError) as raised:
                read(array)
            assert raised.value.reason == "invalid_shredding"
    # A column of no chunks of a storage type is no rows.
    empty = pa.chunked_array([], variant.storage_type(DECL))
    assert variant.to_python(empty, shredding=DECL) == []
    variant.verify(empty, shredding=DECL)


@pytest.mark.parametrize(
    "decl",
    [
        {"type": "uuid"},
        {
            "type": "object",
            "fields": [
                {"name": "id", "type": "uuid"},
                {"name": "n", "type": "int64"},
            ],
        },
        {"type": "array", "element": {"type": "uuid"}},
    ],
    ids=["root", "field", "element"],
)
def test_no_chunks_of_a_uuid_leafs_storage_are_no_rows(decl):
    # A uuid leaf's storage holds an extension type, of which pyarrow
    # cannot build an empty array with pa.array. A filter that keeps no
    # row, and a table of no batches, both give a column of no chunks.
    value = uuid.uuid4()
    row = {"id": value, "n": 1} if decl["type"] == "object" else value
    row = [value] if decl["type"] == "array" else row
    table = pa.table({"v": variant.encode_python([row], shredding=decl).array})
    for column in (
        table.filter(pa.array([False])).column("v"),
        pa.Table.from_batches([], table.schema).column("v"),
    ):
        assert column.num_chunks == 0
        for shredding in (None, decl):
            assert variant.to_python(column, shredding=shredding) == []
            variant.verify(column, shredding=shredding)


@pytest.mark.parametrize(
    ("name", "spelt"),
    [("a", ".a"), ("a.b", '["a.b"]'), ("a\nb\u2028c", '["a\\nb\\u2028c"]')],
    ids=["plain", "dotted", "line-breaks"],
)
def test_a_field_or_element_group_must_be_a_group(name, spelt):
    # A field group or an element group that is not a struct holds no
    # value column to read, and the message spells the column as paths
    # are spelt, on one line, with its type cut short and escaped: Arrow's
    # text of a struct holds its field names.
    wide = pa.struct(
        [pa.field(f"f{i}" if i else "x\ny\u2028z", pa.int64()) for i in range(2000)]
    )
    field = pa.StructArray.from_arrays(
        [pa.array([None], pa.list_(wide))], fields=[pa.field(name, pa.list_(wide))]
    )
    element = pa.ListArray.from_arrays(
        pa.array([0, 1], pa.int32()), pa.array([1], pa.int64())
    )
    for typed, where in (
        (field, f"typed_value{spelt}"),
        (element, "typed_value.list.element"),
    ):
        array = pa.StructArray.from_arrays(
            [pa.array([b"\x01\x00\x00"]), pa.array([None], pa.binary()), typed],
            names=["metadata", "value", "typed_value"],
        )
        for read in (variant.to_python, variant.verify):
            with pytest.raises(VariantEncodingError, match="not a group") as raised:
                read(array)
            assert raised.value.reason == "invalid_shredding"
            message = str(raised.value)
            assert f"{where} is " in message
            assert len(message.splitlines()) == 1 and len(message) < 200


#: A name as long as data may make one.
LONG = "k" * 10_000


def _short(error: VariantEncodingError, *, path: bool = False) -> None:
    """The error's message is one bounded line, its path cut to PATH_CAP."""
    message = str(error)
    limit = 200 + (codec.PATH_CAP if path else 0)
    assert len(message.splitlines()) == 1 and len(message) < limit, len(message)
    if path:
        assert len(error.path) == codec.PATH_CAP
        assert error.path.startswith("$.kkk") and error.path.endswith("...")


def test_the_encoder_cuts_names_from_the_data_short():
    with pytest.raises(VariantEncodingError, match="appears twice") as raised:
        variant.encode_json([f'{{"{LONG}":1,"{LONG}":2}}'])
    _short(raised.value)
    text = f'{{"{LONG}":"\\ud800"}}'
    with pytest.raises(VariantEncodingError, match="unpaired surrogate") as raised:
        variant.encode_json([text])
    _short(raised.value, path=True)
    report = variant.encode_json([text], on_invalid="null").report
    assert report.first_invalid == (None, 0, "invalid_unicode", raised.value.path)


def _long_field(value: bytes | None, typed: int | None, *, metadata: bytes) -> pa.Array:
    group = pa.struct(
        [pa.field("value", pa.binary()), pa.field("typed_value", pa.int64())]
    )
    kind = pa.struct(
        [
            pa.field("metadata", pa.binary(), nullable=False),
            pa.field("value", pa.binary()),
            pa.field("typed_value", pa.struct([pa.field(LONG, group, nullable=False)])),
        ]
    )
    return pa.array(
        [
            {
                "metadata": metadata,
                "value": value,
                "typed_value": {LONG: {"value": None, "typed_value": typed}},
            }
        ],
        kind,
    )


def test_the_reader_cuts_names_from_the_storage_short():
    # The field's value is a truncated string, at a path through its name.
    group = pa.struct([pa.field("value", pa.binary())])
    kind = pa.struct(
        [
            pa.field("metadata", pa.binary(), nullable=False),
            pa.field("typed_value", pa.struct([pa.field(LONG, group)])),
            pa.field("value", pa.binary()),
        ]
    )
    array = pa.array(
        [{"metadata": b"\x01\x00\x00", "typed_value": {LONG: {"value": b"\x09"}}}],
        kind,
    )
    for read in (variant.to_python, variant.verify):
        with pytest.raises(VariantEncodingError, match="truncated") as raised:
            read(array)
        _short(raised.value, path=True)
    # The names the reader's refusals quote.
    long_key = (
        b"\x81\x01\x00\x00\x00\x00\x00"
        + len(LONG).to_bytes(3, "little")
        + LONG.encode()
    )
    residual = b"\x02\x01\x00\x00\x02\x0c\x01"
    for array, read, words in (
        (
            _long_field(None, 1, metadata=b"\x01\x00\x00"),
            variant.verify,
            "is not in the metadata",
        ),
        (
            _long_field(residual, 1, metadata=long_key),
            variant.to_python,
            "is in value too",
        ),
        (
            pa.StructArray.from_arrays(
                [pa.array([b"\x01\x00\x00"]), pa.array([b"\x00"]), pa.array([1])],
                names=["metadata", "value", LONG],
            ),
            variant.to_python,
            "has a column",
        ),
        (
            pa.StructArray.from_arrays(
                [pa.array([b"\x01\x00\x00"]), pa.array([b"\x00"]), pa.array([b"\x00"])],
                names=["metadata", LONG, LONG],
            ),
            variant.to_python,
            "two columns named",
        ),
    ):
        with pytest.raises(VariantEncodingError, match=words) as raised:
            read(array)
        _short(raised.value)


def test_the_reader_cuts_types_short():
    # A type's text is as long as its fields are many, and holds their
    # names, a line break among them.
    wide = pa.struct(
        [pa.field(f"f{i}" if i else "x\ny\u2028z", pa.int64()) for i in range(2000)]
    )
    for array, words in (
        (
            pa.StructArray.from_arrays(
                [pa.array([b"\x01\x00\x00"]), pa.nulls(1, wide)],
                names=["metadata", "value"],
            ),
            "value is struct<",
        ),
        (
            leaf_storage(pa.nulls(1, pa.map_(pa.string(), wide))),
            "typed_value of type map<",
        ),
    ):
        for read in (variant.to_python, variant.verify):
            with pytest.raises(VariantEncodingError, match=words) as raised:
                read(array)
            assert raised.value.reason == "invalid_shredding"
            assert "<x\\u000ay\\u2028z: int64" in str(raised.value)
            _short(raised.value)


def test_an_unknown_type_is_cut_short_in_its_refusal():
    for decl in (
        {"type": "x" * 10_000},
        {"type": "object", "fields": [{"name": LONG, "type": "geometry"}]},
    ):
        with pytest.raises(UnsupportedShreddingError, match="upgrade") as raised:
            variant.storage_type(decl)
        assert len(str(raised.value)) < 200 + codec.PATH_CAP


@pytest.mark.parametrize(
    ("decl", "spelt"),
    [
        (
            {"type": "object", "fields": [{"name": "a\ud800", "type": "geometry"}]},
            r"node $.a\ud800 has type 'geometry'",
        ),
        (
            {"type": "object", "fields": [{"name": "a\nb", "type": "geometry"}]},
            r"node $.a\u000ab has type",
        ),
        ({"type": "geo\ud800"}, r"node $ has type 'geo\ud800'"),
    ],
    ids=["surrogate-name", "newline-name", "surrogate-type"],
)
def test_an_unknown_type_is_refused_in_text_any_log_can_write(decl, spelt):
    # It runs before the grammar, which refuses a lone surrogate in a name,
    # so it sees names and types nothing has checked.
    with pytest.raises(UnsupportedShreddingError) as raised:
        variant.storage_type(decl)
    text = str(raised.value)
    assert spelt in text
    text.encode("utf-8")
    assert "\n" not in text


def test_an_object_reads_in_key_order_however_it_was_shredded():
    # Declared fields and the residual's keys interleave in the Variant's
    # order (UTF-8 bytes), as the unshredded bytes have them.
    decl = {
        "type": "object",
        "fields": [{"name": "m", "type": "int64"}, {"name": "b", "type": "string"}],
    }
    row = {"z": 1, "m": 2, "a": 3, "b": "x", "\U0001f600": 4, "\uffff": 5}
    for shredding in (None, decl):
        (got,) = variant.to_python(
            variant.encode_python([row], shredding=shredding).array
        )
        assert list(got) == ["a", "b", "m", "z", "\uffff", "\U0001f600"]


def test_a_refusal_in_a_later_chunk_names_its_row_in_the_column():
    good = leaf_storage(pa.array([1, 2, 3], pa.int64()).view(pa.time64("us")))
    bad = leaf_storage(pa.array([86_400_000_000], pa.int64()).view(pa.time64("us")))
    for read in (variant.to_python, variant.verify):
        with pytest.raises(VariantEncodingError, match="time of day") as raised:
            read(pa.chunked_array([good, bad]))
        assert (raised.value.row, raised.value.path) == (3, "$")
        assert "row 3" in str(raised.value)


@pytest.mark.parametrize("read", [variant.to_python, variant.verify])
def test_storage_is_an_arrow_array(read):
    # Easy slips: the EncodedVariant, the table or batch around a column.
    encoded = variant.encode_json(["1"])
    with pytest.raises(TypeError, match=r"encoded\.array"):
        read(encoded)
    for wrong in (
        pa.table({"v": encoded.array}),
        pa.record_batch({"v": encoded.array}),
        encoded.array[0],
        [b"\x0c\x01"],
        None,
    ):
        with pytest.raises(TypeError, match="Array or ChunkedArray, not a"):
            read(wrong)


def _group_of(names: list[str], arrays: list[pa.Array]) -> pa.StructArray:
    return pa.StructArray.from_arrays(
        arrays, fields=[pa.field(n, a.type) for n, a in zip(names, arrays, strict=True)]
    )


@pytest.mark.parametrize(
    ("names", "repeated"),
    [
        (["metadata", "value", "value"], "value"),
        (["metadata", "metadata", "value"], "metadata"),
        (["metadata", "value", "typed_value", "typed_value"], "typed_value"),
    ],
)
def test_a_group_that_names_a_column_twice_is_refused(names, repeated):
    # Storage is read by name (VS:54): a name twice is no column to read.
    columns = {
        "metadata": pa.array([b"\x01\x00\x00"]),
        "value": pa.array([b"\x0c\x01"]),
        "typed_value": pa.array([None], pa.int64()),
    }
    array = _group_of(names, [columns[name] for name in names])
    for read in (variant.to_python, variant.verify):
        with pytest.raises(VariantEncodingError, match="two columns named") as raised:
            read(array)
        assert raised.value.reason == "invalid_shredding"
        assert repr(repeated) in str(raised.value)


def _field_group(number: int) -> pa.StructArray:
    return _group_of(
        ["value", "typed_value"],
        [pa.array([None], pa.binary()), pa.array([number], pa.int64())],
    )


@pytest.mark.parametrize(
    ("fields", "repeated"),
    [
        # Two groups for "a" would read as one key, the later winning.
        (_group_of(["a", "a"], [_field_group(1), _field_group(2)]), "a"),
        # A field group, below the top, of two value columns.
        (
            _group_of(
                ["a"],
                [
                    _group_of(
                        ["value", "value"],
                        [pa.array([b"\x0c\x01"]), pa.array([b"\x0c\x02"])],
                    )
                ],
            ),
            "value",
        ),
    ],
    ids=["field", "column-of-a-field"],
)
def test_an_object_that_shreds_a_name_twice_is_refused(fields, repeated):
    array = _group_of(
        ["metadata", "value", "typed_value"],
        [pa.array([KEY_A]), pa.array([None], pa.binary()), fields],
    )
    for read in (variant.to_python, variant.verify):
        with pytest.raises(
            VariantEncodingError, match=f"two columns named {repeated!r}"
        ) as raised:
            read(array)
        assert raised.value.reason == "invalid_shredding"


def test_only_the_top_group_has_metadata():
    # A field group's own dictionary would be read as the row's.
    group = pa.struct([("metadata", pa.binary()), ("value", pa.binary())])
    array = pa.array(
        [
            {
                "metadata": KEY_A,
                "value": None,
                "typed_value": {"a": {"metadata": KEY_A, "value": b"\x0c\x05"}},
            }
        ],
        pa.struct(
            [
                ("metadata", pa.binary()),
                ("value", pa.binary()),
                ("typed_value", pa.struct([("a", group)])),
            ]
        ),
    )
    for read in (variant.to_python, variant.verify):
        with pytest.raises(
            VariantEncodingError, match="a group has a column 'metadata'"
        ) as raised:
            read(array)
        assert raised.value.reason == "invalid_shredding"


def test_a_present_row_must_have_metadata():
    array = pa.StructArray.from_arrays(
        [pa.array([None], pa.binary()), pa.array([b"\x00"], pa.binary())],
        fields=list(variant.storage_type()),
    )
    for read in (variant.to_python, variant.verify):
        with pytest.raises(VariantEncodingError, match="no metadata") as raised:
            read(array)
        assert raised.value.reason == "invalid_shredding"


def test_a_declaration_must_match_the_storage():
    array = variant.encode_json(["1"], shredding=INT64).array
    for read, decl, where in (
        (variant.to_python, {"type": "int32"}, "typed_value"),
        (variant.verify, EVENT, "typed_value"),
        (variant.verify, LIST, "typed_value"),
        (variant.verify, {"type": "variant"}, "value"),
    ):
        _refused_as_another_form(read, array, decl, where)
    variant.verify(array, shredding=INT64)
    variant.verify(array)
    # A field of another name, whatever its type.
    array = variant.encode_json(['{"a":1}'], shredding=EVENT).array
    other = {"type": "object", "fields": [{"name": "b", "type": "int64"}]}
    _refused_as_another_form(variant.to_python, array, other, "typed_value.b")
    variant.verify(array, shredding=EVENT)


def _refused_as_another_form(read, array, decl, where):
    with pytest.raises(VariantEncodingError) as raised:
        read(array, shredding=decl)
    # A VariantEncodingError, as verify promises, and so a ValidationError
    # too.
    assert type(raised.value) is VariantEncodingError
    assert raised.value.reason == "invalid_shredding"
    assert (
        "neither the write nor the read form of the declaration (it first "
        f"differs at {where})" in str(raised.value)
    )


def test_a_form_refusal_names_a_column_not_the_whole_type():
    # The type of a wide declaration's storage runs to tens of kilobytes.
    decl = {
        "type": "object",
        "fields": [{"name": f"f{i}", "type": "int64"} for i in range(999)],
    }
    array = variant.encode_json(["{}"], shredding=decl).array
    with pytest.raises(VariantEncodingError) as raised:
        variant.verify(array, shredding=INT64)
    assert len(str(raised.value)) < 300
    narrower = {**decl, "fields": decl["fields"][:-1]}
    with pytest.raises(VariantEncodingError, match="first differs at typed_value\\)"):
        variant.verify(array, shredding=narrower)
    # Storage of the read form, refused at the int32 that is not its int64,
    # and by the write form at its decimal64.
    decl = {
        "type": "object",
        "fields": [{"name": "p", **DECIMAL8}, {"name": "q", "type": "int64"}],
    }
    other = {**decl, "fields": [decl["fields"][0], {"name": "q", "type": "int32"}]}
    read_form = pa.array([None], variant.storage_type(decl, form="read"))
    with pytest.raises(VariantEncodingError) as raised:
        variant.to_python(read_form, shredding=other)
    assert (
        "it first differs from the write form at typed_value.p.typed_value, and "
        "from the read form at typed_value.q.typed_value)" in str(raised.value)
    )


@pytest.mark.parametrize(
    ("name", "spelt"),
    [
        # Longer than the cap, which counts code points: UTF-16 units, as
        # the server counts, would cut a pair and leave a lone surrogate.
        ("\U0001f600" * 60, None),
        ("a\nb\u2028c", '["a\\nb\\u2028c"]'),
        # Not the spelling of a field b inside a field a.
        ("a.b", '["a.b"]'),
    ],
    ids=["astral", "line-breaks", "dotted"],
)
def test_a_form_refusal_spells_names_as_paths_do(name, spelt):
    def decl(kind):
        return {"type": "object", "fields": [{"name": name, "type": kind}]}

    array = variant.encode_python([{name: "x"}], shredding=decl("string")).array
    with pytest.raises(VariantEncodingError) as raised:
        variant.to_python(array, shredding=decl("int64"))
    message = str(raised.value)
    message.encode("utf-8")
    assert len(message.splitlines()) == 1
    if spelt is not None:
        assert f"it first differs at typed_value{spelt}.typed_value)" in message
    else:
        assert f"it first differs at typed_value.{name[:49]}...)" in message


DECIMAL8 = {"type": "decimal8", "precision": 18, "scale": 2}


def test_the_write_form_says_how_to_scale_its_decimals():
    # decimal4/decimal8 leaves are decimal32/decimal64, whose types say
    # how to scale them, with or without the declaration.
    decl = {
        "type": "object",
        "fields": [
            {"name": "p", **DECIMAL8},
            {"name": "q", "type": "decimal4", "precision": 9, "scale": 4},
            {
                "name": "l",
                "type": "array",
                "element": {"type": "decimal4", "precision": 3, "scale": 0},
            },
            # As many places as digits, the widest scale a precision takes.
            {"name": "r", "type": "decimal4", "precision": 2, "scale": 2},
            # Two-digit scales, at both widths.
            {"name": "s", "type": "decimal8", "precision": 18, "scale": 12},
            {"name": "t", "type": "decimal8", "precision": 10, "scale": 10},
        ],
    }
    rows = [
        {
            "p": Decimal("12.34"),
            "q": Decimal("-0.0001"),
            "l": [Decimal(7)],
            "r": Decimal("0.12"),
            "s": Decimal("-123456.789012345678"),
            "t": Decimal("0.0123456789"),
        }
    ]
    array = variant.encode_python(rows, shredding=decl).array
    typed = array.field("typed_value")
    assert typed.field("p").field("typed_value").view(pa.int64()).to_pylist() == [1234]
    assert typed.field("s").field("typed_value").type == pa.decimal64(18, 12)
    assert typed.field("q").field("typed_value").type == pa.decimal32(9, 4)
    assert variant.to_python(array) == rows
    assert variant.to_python(array, shredding=decl) == rows
    variant.verify(array)
    variant.verify(array, shredding=decl)
    report = variant.encode_python(rows, shredding=decl).report
    assert report.fallback and not any(report.fallback.values())


@pytest.mark.parametrize(
    ("source", "declared"),
    [
        ({"type": "int64"}, DECIMAL8),
        (DECIMAL8, {"type": "decimal8", "precision": 18, "scale": 6}),
        (DECIMAL8, {"type": "decimal8", "precision": 17, "scale": 2}),
        ({"type": "decimal4", "precision": 9, "scale": 2}, {"type": "int32"}),
        (
            {"type": "decimal4", "precision": 9, "scale": 2},
            {"type": "decimal8", "precision": 9, "scale": 2},
        ),
    ],
)
def test_the_write_form_of_another_declaration_is_refused(source, declared):
    array = variant.encode_python([Decimal("12.34")], shredding=source).array
    assert array.type != variant.storage_type(declared)
    for read in (variant.to_python, variant.verify):
        with pytest.raises(
            VariantEncodingError, match="neither the write nor the read"
        ):
            read(array, shredding=declared)
    assert codec.compile_plan(declared).form_of(array.type) is None
    assert codec.compile_plan(source).form_of(array.type) == "write"


def _in_object(leaf):
    return {"type": "object", "fields": [{"name": "p", **leaf}]}


def _in_array(leaf):
    return {"type": "array", "element": leaf}


def _in_both(leaf):
    return _in_array(_in_object(leaf))


NESTINGS = [
    (_in_object, lambda v: {"p": v}, "typed_value.p.typed_value"),
    (_in_array, lambda v: [v], "typed_value.element.typed_value"),
    (
        _in_both,
        lambda v: [{"p": v}],
        "typed_value.element.typed_value.p.typed_value",
    ),
]

OTHER_DECLARATIONS = [
    (DECIMAL8, {"type": "int64"}, Decimal("12.34")),
    ({"type": "int64"}, DECIMAL8, 1234),
    (DECIMAL8, {"type": "decimal8", "precision": 18, "scale": 6}, Decimal("12.34")),
    (DECIMAL8, {"type": "decimal8", "precision": 17, "scale": 2}, Decimal("12.34")),
]


@pytest.mark.parametrize("where", NESTINGS, ids=["object", "array", "array-of-objects"])
@pytest.mark.parametrize(("source", "declared", "value"), OTHER_DECLARATIONS)
def test_nested_decimal_leaves_tell_declarations_apart(where, source, declared, value):
    # A decimal4/decimal8 leaf below the root, in an object field or an
    # array element, is told apart by its type too.
    wrap, row, leaf = where
    array = variant.encode_python([row(value)], shredding=wrap(source)).array
    assert array.type != variant.storage_type(wrap(declared))
    assert codec.compile_plan(wrap(declared)).form_of(array.type) is None
    assert codec.compile_plan(wrap(source)).form_of(array.type) == "write"
    for read in (variant.to_python, variant.verify):
        with pytest.raises(
            VariantEncodingError, match="neither the write nor"
        ) as raised:
            read(array, shredding=wrap(declared))
        assert f"it first differs at {leaf})" in str(raised.value)
    assert variant.to_python(array, shredding=wrap(source)) == [row(value)]


def _rewrapped(array: pa.Array, kind: pa.DataType) -> pa.Array:
    return pa.StructArray.from_arrays(
        [array.field(i) for i in range(array.type.num_fields)], fields=list(kind)
    )


@pytest.mark.parametrize("where", NESTINGS, ids=["object", "array", "array-of-objects"])
@pytest.mark.parametrize(("source", "declared", "value"), OTHER_DECLARATIONS)
def test_arrow_keeps_two_declarations_out_of_one_column(where, source, declared, value):
    # Nothing in the storage is field metadata the reader relies on, which
    # Arrow drops or merges at will: a column of one declaration's storage
    # and another's cannot be built at all, so neither a combine, a take,
    # a sort or a write can rescale one by the other's decimals.
    wrap, row, _ = where
    mine = variant.encode_python([row(value)], shredding=wrap(source)).array
    theirs_type = variant.storage_type(wrap(declared))
    theirs = pa.array([None], theirs_type)
    with pytest.raises(pa.ArrowInvalid):
        pa.concat_arrays([theirs, mine])
    with pytest.raises(pa.ArrowTypeError):
        pa.chunked_array([theirs, mine])
    with pytest.raises((pa.ArrowInvalid, pa.ArrowTypeError)):
        pa.chunked_array([mine], type=theirs_type)
    with pytest.raises(pa.ArrowInvalid):
        pa.concat_tables([pa.table({"v": theirs}), pa.table({"v": mine})])
    with pytest.raises((pa.ArrowInvalid, pa.ArrowTypeError)):
        _rewrapped(mine, theirs_type)


def test_one_declarations_chunks_combine_as_they_read():
    def table(*values):
        encoded = variant.encode_python(
            [{"p": v} for v in values], shredding=_in_object(DECIMAL8)
        )
        return pa.table({"k": range(len(values)), "v": encoded.array})

    combined = pa.concat_tables(
        [table(Decimal("1.25"), Decimal("2.5")), table(Decimal(3), Decimal(-4))]
    )
    rows = [{"p": Decimal(v)} for v in ("1.25", "2.50", "3.00", "-4.00")]
    for column, expected in (
        (combined.column("v"), rows),
        (combined.combine_chunks().column("v"), rows),
        (combined.take([3, 0]).column("v"), [rows[3], rows[0]]),
        (combined.sort_by("k").column("v"), [rows[0], rows[2], rows[1], rows[3]]),
    ):
        assert variant.to_python(column, shredding=_in_object(DECIMAL8)) == expected
        variant.verify(column, shredding=_in_object(DECIMAL8))


def test_the_read_form_of_a_decimal_reads():
    decl = {"type": "decimal4", "precision": 9, "scale": 2}
    array = pa.array(
        [{"metadata": b"\x01\x00\x00", "value": None, "typed_value": Decimal("1.50")}],
        variant.storage_type(decl, form="read"),
    )
    assert codec.compile_plan(decl).form_of(array.type) == "read"
    assert variant.to_python(array, shredding=decl) == [Decimal("1.50")]


def test_field_metadata_in_storage_says_nothing():
    # Arrow drops and merges field metadata at will (take, concat,
    # from_arrays), so nothing in the storage's field metadata may change
    # how a leaf reads: an int64 leaf with decimal-looking metadata is still
    # an int64, and still the storage of an int64 declaration.
    kind = pa.struct(
        [
            pa.field("metadata", pa.binary(), nullable=False),
            pa.field("value", pa.binary()),
            pa.field(
                "typed_value",
                pa.int64(),
                metadata={b"pyhoglake:decimal": b"decimal8(18,2)"},
            ),
        ]
    )
    array = pa.array([{"metadata": b"\x01\x00\x00", "typed_value": 125}], kind)
    assert variant.to_python(array) == [125]
    assert codec.compile_plan(INT64).form_of(array.type) == "write"
    assert codec.compile_plan(DECIMAL8).form_of(array.type) is None


def test_duckdb_storage_reads():
    # tests/data/README.md: DuckDB 1.5.5, OPTIONAL field and element groups.
    table = pq.read_table(DATA / "native_variant.parquet")
    column = table.column("properties")
    assert variant.to_python(column) == [{"a": 42, "nested": [True, None]}]
    variant.verify(column, canonical=False)
    # DuckDB honours no declaration, so its files are no form of their
    # column's, even one that declares nothing: they read without it.
    undeclared = Column("properties", "variant", 2, 0, True, None)
    with pytest.raises(VariantEncodingError, match="read without the column$"):
        variant.to_python(column, undeclared)


def test_variant_null_survives_copies_and_pickles():
    assert repr(VARIANT_NULL) == "VARIANT_NULL"
    assert copy.copy(VARIANT_NULL) is VARIANT_NULL
    assert copy.deepcopy([VARIANT_NULL])[0] is VARIANT_NULL
    assert pickle.loads(pickle.dumps(VARIANT_NULL)) is VARIANT_NULL


def test_variant_encoding_errors_survive_pickles_and_copies():
    # A process pool hands an encode error back pickled.
    error = VariantEncodingError(
        "variant column 'p' row 3 at $.a: x",
        reason="duplicate_key",
        column="p",
        row=3,
        path="$.a",
    )
    for again in (
        pickle.loads(pickle.dumps(error)),
        copy.copy(error),
        copy.deepcopy(error),
    ):
        assert type(again) is VariantEncodingError
        assert (again.message, again.reason, again.column, again.row, again.path) == (
            error.message, "duplicate_key", "p", 3, "$.a",
        )  # fmt: skip
        assert str(again) == str(error) and again.status_code is None
    with pytest.raises(VariantEncodingError) as raised:
        variant.encode_json(['{"a":1,"a":1}'])
    assert pickle.loads(pickle.dumps(raised.value)).reason == "duplicate_key"


def test_encoded_variant_is_frozen():
    encoded = variant.encode_json(["1"])
    with pytest.raises(AttributeError):
        encoded.array = None  # type: ignore[misc]


def _nest(levels: int) -> bytes:
    """An empty array inside ``levels`` one-element arrays, by hand."""
    value = b"\x03\x00\x00"
    for _ in range(levels):
        width = 1 if len(value) < 0x100 else 2
        value = (
            bytes((0x03 | (width - 1) << 2, 1))
            + (0).to_bytes(width, "little")
            + len(value).to_bytes(width, "little")
            + value
        )
    return value


def test_the_decoder_bounds_depth_as_the_encoder_does():
    # An empty container may sit at depth 128 and nothing below it (D7),
    # whoever wrote the bytes; deeper bytes are refused, not recursed into.
    deepest = "[" * 129 + "]" * 129
    encoded = variant.encode_json([deepest]).array
    assert encoded.field("value").to_pylist() == [_nest(128)]
    too_deep = storage([{"metadata": b"\x01\x00\x00", "value": _nest(129)}], None)
    with pytest.raises(VariantEncodingError, match="nested more than 128") as raised:
        variant.to_python(too_deep)
    # Valid bytes, deeper than this reader goes: its limit, not malformed.
    assert raised.value.reason == "nesting_too_deep"


def _nested(levels: int) -> list[object]:
    """An empty list inside ``levels`` one-element lists."""
    value: list[object] = []
    for _ in range(levels):
        value = [value]
    return value


_BOX = {
    "type": "object",
    "fields": [
        {"name": "o", "type": "object", "fields": [{"name": "x", "type": "int64"}]}
    ],
}
_BOX_METADATA, _BOX_IDS = codec._metadata({"b", "o", "x"})


@pytest.mark.parametrize(
    ("decl", "depth", "row", "read"),
    [
        (
            {"type": "object", "fields": [{"name": "a", "type": "variant"}]},
            1,
            lambda value: {
                "metadata": KEY_A,
                "value": None,
                "typed_value": {"a": {"value": codec._encode(value, {})}},
            },
            lambda value: {"a": value},
        ),
        (
            {"type": "array", "element": {"type": "variant"}},
            1,
            lambda value: {
                "metadata": b"\x01\x00\x00",
                "value": None,
                "typed_value": [{"value": codec._encode(value, {})}],
            },
            lambda value: [value],
        ),
        # o's residual {"b": ...}: the residual is at o's depth, b below it.
        (
            _BOX,
            2,
            lambda value: {
                "metadata": _BOX_METADATA,
                "value": None,
                "typed_value": {
                    "o": {
                        "value": codec._encode({"b": value}, _BOX_IDS),
                        "typed_value": {"x": {"value": None, "typed_value": None}},
                    }
                },
            },
            lambda value: {"o": {"b": value}},
        ),
        # A list where the object o is declared, so in o's value.
        (
            _BOX,
            1,
            lambda value: {
                "metadata": _BOX_METADATA,
                "value": None,
                "typed_value": {
                    "o": {"value": codec._encode(value, {}), "typed_value": None}
                },
            },
            lambda value: {"o": value},
        ),
        # A list where an array is declared: in value, beside a null list.
        (
            {"type": "object", "fields": [{"name": "a", **LIST}]},
            1,
            lambda value: {
                "metadata": KEY_A,
                "value": None,
                "typed_value": {
                    "a": {"value": codec._encode(value, {}), "typed_value": None}
                },
            },
            lambda value: {"a": value},
        ),
    ],
    ids=[
        "object-field",
        "array-element",
        "residual",
        "unshredded-object-field",
        "unshredded-array-field",
    ],
)
def test_the_depth_bound_is_over_the_whole_value(decl, depth, row, read):
    # A value column below the root holds a value at ``depth`` of the whole
    # value, and the bound counts from the root, as the encoder does.
    deepest = _nested(MAX_VARIANT_DEPTH - depth)
    assert variant.to_python(storage([row(deepest)], decl)) == [read(deepest)]
    too_deep = storage([row(_nested(MAX_VARIANT_DEPTH - depth + 1))], decl)
    with pytest.raises(VariantEncodingError, match="nested more than 128") as raised:
        variant.to_python(too_deep)
    assert raised.value.reason == "nesting_too_deep"


def _shredded_nest(
    kind: str,
    levels: int,
    *,
    leaf: bool,
    null_group: bool = False,
    in_value: bytes | None = None,
) -> pa.StructArray:
    """Storage of ``levels`` objects ({"a": ...}) or one-element arrays,
    each a typed group of the next, the last holding the int 1 shredded,
    or nothing: no field, or no element. With ``null_group`` the last
    object's field group is null, as DuckDB's OPTIONAL ones may be, over
    the int; with ``in_value`` the last holds those Variant bytes in its
    value column instead. A ``kind`` of "large_array" nests large_lists, as
    pyarrow reads them with ``list_type=pa.LargeListType``. Built by hand,
    as a loop."""
    inner = pa.StructArray.from_arrays(
        [
            pa.array([in_value], pa.binary()),
            pa.array([1 if leaf and in_value is None else None], pa.int64()),
        ],
        names=["value", "typed_value"],
        mask=pa.array([null_group]),
    )
    for level in range(levels):
        last = level == 0 and not leaf
        if kind == "object":
            typed = pa.StructArray.from_arrays(
                [inner], fields=[pa.field("a", inner.type, nullable=null_group)]
            )
        else:
            large = kind == "large_array"
            typed = (pa.LargeListArray if large else pa.ListArray).from_arrays(
                pa.array([0, 0 if last else 1], pa.int64() if large else pa.int32()),
                inner.slice(0, 0) if last else inner,
                type=(pa.large_list if large else pa.list_)(
                    pa.field("element", inner.type, nullable=False)
                ),
            )
        inner = pa.StructArray.from_arrays(
            [pa.array([None], pa.binary()), typed], names=["value", "typed_value"]
        )
    metadata = pa.array([KEY_A if kind == "object" else b"\x01\x00\x00"])
    return pa.StructArray.from_arrays(
        [metadata, inner.field("value"), inner.field("typed_value")],
        names=["metadata", "value", "typed_value"],
    )


def _python_nest(kind: str, levels: int, *, leaf: bool, last: object = 1) -> object:
    value: object = last
    for level in range(levels):
        last = level == 0 and not leaf
        if kind == "object":
            value = {} if last else {"a": value}
        else:
            value = [] if last else [value]
    return value


def _value_only_nest(levels: int) -> pa.StructArray:
    """Storage of ``levels`` shredded objects {"a": ...} whose deepest
    field group holds a ``value`` column only, as a declared ``variant``
    field is laid out, with the int 1 in it."""
    inner = pa.StructArray.from_arrays(
        [pa.array([b"\x0c\x01"], pa.binary())], names=["value"]
    )
    for _ in range(levels):
        typed = pa.StructArray.from_arrays(
            [inner], fields=[pa.field("a", inner.type, nullable=False)]
        )
        inner = pa.StructArray.from_arrays(
            [pa.array([None], pa.binary()), typed], names=["value", "typed_value"]
        )
    return pa.StructArray.from_arrays(
        [pa.array([KEY_A]), inner.field("value"), inner.field("typed_value")],
        fields=[
            pa.field("metadata", pa.binary(), nullable=False),
            pa.field("value", pa.binary()),
            pa.field("typed_value", inner.field("typed_value").type),
        ],
    )


def test_a_value_only_field_group_at_the_depth_bound_is_refused_by_reason():
    """At the deepest level the reader asks each field group which rows
    hold its field, from the columns the group HAS: a declared ``variant``
    field has a ``value`` and no ``typed_value``. Asked for both, it raised
    a raw KeyError at depth 129 rather than refuse the row as
    nesting_too_deep."""
    at_bound = _value_only_nest(MAX_VARIANT_DEPTH)
    assert variant.to_python(at_bound) == [
        _python_nest("object", MAX_VARIANT_DEPTH, leaf=True)
    ]
    variant.verify(at_bound)
    too_deep = _value_only_nest(MAX_VARIANT_DEPTH + 1)
    for read in (variant.to_python, variant.verify):
        with pytest.raises(VariantEncodingError) as raised:
            read(too_deep)
        assert (raised.value.reason, raised.value.row) == ("nesting_too_deep", 0)
        assert raised.value.path == "$" + ".a" * MAX_VARIANT_DEPTH


@pytest.mark.parametrize("kind", ["object", "array", "large_array"])
def test_the_depth_bound_holds_through_shredded_nesting(kind):
    # Objects and arrays shredded as typed groups are levels of the value
    # as much as their bytes are: the bound and its refusal are the same.
    for levels, leaf in ((MAX_VARIANT_DEPTH, True), (MAX_VARIANT_DEPTH + 1, False)):
        # The int at depth 128; an empty container there.
        array = _shredded_nest(kind, levels, leaf=leaf)
        assert variant.to_python(array) == [_python_nest(kind, levels, leaf=leaf)]
        variant.verify(array)
    # A null field group is no field, whatever its columns hold.
    if kind == "object":
        array = _shredded_nest(kind, MAX_VARIANT_DEPTH + 1, leaf=True, null_group=True)
        assert variant.to_python(array) == [
            _python_nest(kind, MAX_VARIANT_DEPTH + 1, leaf=False)
        ]
    # A leaf held only in value is as much a value as a typed one.
    array = _shredded_nest(kind, MAX_VARIANT_DEPTH, leaf=True, in_value=b"\x05x")
    assert variant.to_python(array) == [
        _python_nest(kind, MAX_VARIANT_DEPTH, leaf=True, last="x")
    ]
    variant.verify(array)
    # One container at depth 128 that is not empty, refused at its row,
    # whether the value below it is typed or in its group's value column,
    # or is the array [1] in the value column of the deepest group.
    step = ".a" if kind == "object" else "[0]"
    for levels, in_value in (
        (MAX_VARIANT_DEPTH + 1, None),
        (MAX_VARIANT_DEPTH + 1, b"\x05x"),
        (MAX_VARIANT_DEPTH, b"\x03\x01\x00\x02\x0c\x01"),
    ):
        too_deep = _shredded_nest(kind, levels, leaf=True, in_value=in_value)
        for read in (variant.to_python, variant.verify):
            with pytest.raises(
                VariantEncodingError, match="nested more than 128"
            ) as raised:
                read(too_deep)
            assert (raised.value.reason, raised.value.row) == ("nesting_too_deep", 0)
            assert raised.value.path == "$" + step * MAX_VARIANT_DEPTH
    # Storage that nests deeper than Python recurses is refused, before
    # any row is read, as the same reason.
    deepest = _shredded_nest(kind, 1000, leaf=False)
    for read in (variant.to_python, variant.verify):
        with pytest.raises(
            VariantEncodingError, match="shreds values nested"
        ) as raised:
            read(deepest)
        assert raised.value.reason == "nesting_too_deep"
        # Checked against a declaration, it is no form of one, and the
        # check of its type does not recurse through it either.
        with pytest.raises(VariantEncodingError) as raised:
            read(deepest, shredding=LIST)
        assert raised.value.reason == "invalid_shredding"
