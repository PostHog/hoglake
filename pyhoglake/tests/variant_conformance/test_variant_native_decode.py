"""The Rust VARIANT decoder (pyhoglake-variant-codec) against the Python
one, its reference while both exist: storage read the same in each mode
(to_python's, verify's and verify's canonical default), as values tagged
by type (variant_decoding), or as the same fault: row, reason, path and
message. structure_fault finds the same null.

Inputs: the parquet-testing vectors, DuckDB's and native fixture files,
the golden storage, a value of every type and edge in value bytes and in
typed leaves, edge binaries, mutated bytes (100k), and the randomized
encode corpus read back (PYHOGLAKE_VARIANT_DIFFERENTIAL_ROWS rows, at
least 1M once per PR). conftest also reads every storage the other suites
read through both decoders.
"""

from __future__ import annotations

import json
import logging
import os
import random
import struct
import types
import uuid
from collections import Counter
from datetime import UTC, date, datetime, time
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from pyarrow import ipc
from test_parquet_testing import CASES, NAMES, READ_OPTIONS, SHREDDED, VARIANT
from test_variant_native_codec import (
    JSON_TEST_SUITE,
    MILLRACE,
    RANDOM,
    RANDOMIZED,
    _edges,
    _rows,
)
from variant_decoding import assert_reads_alike, rust_reads

from pyhoglake import VariantEncodingError, _variant_backend, variant
from pyhoglake import _variant_codec as codec
from pyhoglake.models import Column
from pyhoglake.parquet_schema import write_variant_parquet
from pyhoglake.variant import VARIANT_NULL, VariantReport

NATIVE, WHY = _variant_backend.native()
#: Taken before conftest swaps it for a failure, for the fallback tests.
REAL_DECODE_FALL_BACK = _variant_backend._decode_fall_back
pytestmark = pytest.mark.skipif(NATIVE is None, reason=WHY)

ROWS = int(os.environ.get("PYHOGLAKE_VARIANT_DIFFERENTIAL_ROWS", "20000"))
DATA = Path(__file__).parents[1] / "data"
MUTATIONS = int(os.environ.get("PYHOGLAKE_VARIANT_DECODE_MUTATIONS", "100000"))


def reads_alike(array, **kwargs):
    return assert_reads_alike(NATIVE, array, **kwargs)


def unshredded(pairs: list[tuple[bytes | None, bytes | None]]) -> pa.StructArray:
    """Storage of (metadata, value) rows, nullable, where None is a null."""
    metadata, values = zip(*pairs, strict=True) if pairs else ((), ())
    return pa.StructArray.from_arrays(
        [pa.array(metadata, pa.binary()), pa.array(values, pa.binary())],
        ["metadata", "value"],
    )


# -- the switch, the hook and the fallbacks -----------------------------------


def _faulty() -> pa.StructArray:
    return unshredded([(b"\x01\x00\x00", b"\x0c\x01"), (b"\x01\x00\x00", b"\x0c")])


def test_verify_reads_with_both_until_n6b(monkeypatch):
    seen = []
    real = codec.reassemble

    def counted(array, *args, **kwargs):
        seen.append(len(array))
        return real(array, *args, **kwargs)

    monkeypatch.setattr(codec, "reassemble", counted)
    variant.verify(variant.encode_json(['{"a":[1,"x"]}'] * 50).array)
    assert seen == [50]  # Python read it too
    with pytest.raises(VariantEncodingError) as raised:
        variant.verify(_faulty())
    assert (raised.value.row, raised.value.reason, raised.value.path) == (
        1,
        "malformed_variant",
        "$",
    )
    assert str(raised.value) == "variant value row 1 at $: a primitive is truncated"
    assert seen == [50, 2]


def _fake(**functions) -> types.SimpleNamespace:
    return types.SimpleNamespace(PanicException=NATIVE.PanicException, **functions)


@pytest.mark.parametrize("how", ["mismatch", "missed", "panic", "agreeing"])
@pytest.mark.parametrize("valid", [False, True])
def test_a_verdict_python_does_not_share_falls_back(monkeypatch, caplog, how, valid):
    def decode(chunks, **_):
        if how == "panic":
            raise NATIVE.PanicException("boom")
        if how == "mismatch":
            return [], (0, "malformed_variant", ["k"], ["made up"])
        if how == "missed":  # a false negative: no fault where there is one
            return [], None
        return [], (1, "malformed_variant", [], ["a primitive is truncated"])

    storage = variant.encode_json(["1", "2"]).array if valid else _faulty()
    monkeypatch.setattr(_variant_backend, "_decode_fall_back", REAL_DECODE_FALL_BACK)
    monkeypatch.setattr(_variant_backend, "native", lambda: (_fake(decode=decode), ""))
    before = Counter(_variant_backend.DECODE_FALLBACKS)
    with caplog.at_level(logging.WARNING):
        try:
            variant.verify(storage)
        except VariantEncodingError as exc:
            error: str | None = str(exc)
        else:
            error = None
    # Always Python's verdict until N6b: where Rust's differs either way, a
    # fallback to it.
    assert error == (
        None if valid else "variant value row 1 at $: a primitive is truncated"
    )
    # Logged, and counted apart from encode's fallbacks: a read has no report.
    fallbacks = [r.getMessage() for r in caplog.records if "fallback" in r.getMessage()]
    reason = {
        "mismatch": "mismatch",
        "missed": None if valid else "mismatch",
        "panic": "panic",
        "agreeing": "mismatch" if valid else None,
    }[how]
    assert [m.rsplit("reason=", 1)[1] for m in fallbacks] == [reason] * (
        reason is not None
    )
    counted = _variant_backend.DECODE_FALLBACKS - before
    assert counted == Counter({reason: 1} if reason else {})


@pytest.mark.parametrize("how", ["false fault", "missed", "panic", "agreeing"])
def test_structure_faults_are_pythons_where_rust_differs(monkeypatch, how):
    # A false fault from Rust would refuse every write of the storage (a
    # millrace group would stick), and a missed one write other bytes than
    # the row's: Python's verdict stands, counted.
    column = Column("v", "variant", 1, 0, True)
    storage = variant.encode_json(['{"a":1}', None], column).array
    if how in ("agreeing", "missed"):  # a null under a present parent, really
        children = [pa.array([None, None], pa.binary()), storage.field("value")]
        storage = pa.StructArray.from_arrays(children, fields=list(storage.type))
    field = pa.field("v", storage.type, metadata={b"PARQUET:field_id": b"1"})
    table = pa.Table.from_arrays([storage], schema=pa.schema([field]))

    def structure_fault(array):
        if how == "panic":
            raise NATIVE.PanicException("boom")
        if how == "missed":
            return None
        return [("column", ["metadata"]), " is null under a present parent"]

    fake = _fake(structure_fault=structure_fault)
    monkeypatch.setattr(_variant_backend, "_decode_fall_back", REAL_DECODE_FALL_BACK)
    monkeypatch.setattr(_variant_backend, "native", lambda: (fake, ""))
    before = Counter(_variant_backend.DECODE_FALLBACKS)
    if how in ("agreeing", "missed"):
        with pytest.raises(ValueError, match="metadata is null under a present"):
            write_variant_parquet(table, [column])
    else:
        assert write_variant_parquet(table, [column]).body
    reason = {"false fault": "mismatch", "missed": "mismatch", "panic": "panic"}.get(
        how
    )
    counted = _variant_backend.DECODE_FALLBACKS - before
    assert counted == Counter({reason: 1} if reason else {})


def test_the_switch_picks_the_decoder(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("Rust was used")

    storage = variant.encode_json(['{"a":1}']).array
    monkeypatch.setattr(_variant_backend, "ROUTED", Counter())
    fake = _fake(decode=refuse, structure_fault=refuse)
    monkeypatch.setattr(_variant_backend, "native", lambda: (fake, ""))
    _variant_backend.verify(
        storage, None, canonical=True, column=None, nullable=True, backend="python"
    )
    monkeypatch.setattr(_variant_backend, "MODE", "python")
    assert _variant_backend.structure_fault(storage) is None
    monkeypatch.setattr(_variant_backend, "native", lambda: (None, "it is gone"))
    for mode in ("native", "auto"):
        monkeypatch.setattr(_variant_backend, "MODE", mode)
        if mode == "native":
            with pytest.raises(RuntimeError, match="native, but it is gone"):
                variant.verify(storage)
            with pytest.raises(RuntimeError, match="native, but it is gone"):
                _variant_backend.structure_fault(storage)
        else:
            variant.verify(storage)
            assert _variant_backend.structure_fault(storage) is None
    # Only auto's route is counted, per process, for millrace to export.
    assert _variant_backend.ROUTED == Counter({("decode", "unavailable"): 2})


def test_storage_rust_does_not_import_is_read_by_python(monkeypatch):
    def decode(chunks, **_):
        raise TypeError("an Arrow type it does not know")

    storage = variant.encode_json(["1"]).array
    monkeypatch.setattr(_variant_backend, "native", lambda: (_fake(decode=decode), ""))
    monkeypatch.setattr(_variant_backend, "MODE", "auto")
    monkeypatch.setattr(_variant_backend, "ROUTED", Counter())
    variant.verify(storage)
    assert _variant_backend.ROUTED == Counter({("decode", "storage"): 1})
    monkeypatch.setattr(_variant_backend, "MODE", "native")
    with pytest.raises(RuntimeError, match="does not take this storage"):
        variant.verify(storage)


def test_struct_children_under_an_offset_are_imported_whole():
    # arrow-rs 59 slices the struct children of a struct twice where an
    # offset applies, and panics: importable copies such storage first.
    decl = {
        "type": "object",
        "fields": [
            {"name": "o", "type": "object", "fields": [{"name": "x", "type": "int64"}]},
            {
                "name": "l",
                "type": "array",
                "element": {
                    "type": "object",
                    "fields": [{"name": "y", "type": "string"}],
                },
            },
        ],
    }
    rows = [
        json.dumps({"o": {"x": i}, "l": [{"y": str(i)}] * (i % 3), "z": i})
        for i in range(9)
    ]
    storage = variant.encode_json(rows, shredding=decl).array
    assert _variant_backend.importable(storage) is storage
    for piece in (storage[3:7], pa.chunked_array([storage[:2], storage[2:]])):
        reads_alike(piece)
        variant.verify(piece)
    assert _variant_backend.structure_fault(storage[3:7]) is None
    values = storage.field("typed_value").field("l").field("typed_value").values
    nested = pa.ListArray.from_arrays(pa.array([0, 2, 3]), values[1:])
    assert _variant_backend.importable(nested) is not nested


# -- the vectors and fixture files ---------------------------------------------


@pytest.mark.parametrize("name", NAMES)
def test_the_variant_vectors(name):
    metadata = (VARIANT / f"{name}.metadata").read_bytes()
    value = (VARIANT / f"{name}.value").read_bytes()
    reads_alike(unshredded([(metadata, value)]))


@pytest.mark.parametrize("options", READ_OPTIONS.values(), ids=READ_OPTIONS.keys())
def test_the_shredded_cases(options):
    for case in CASES:
        reads_alike(
            pq.read_table(SHREDDED / case["parquet_file"], **options).column("var")
        )


@pytest.mark.parametrize("options", READ_OPTIONS.values(), ids=READ_OPTIONS.keys())
@pytest.mark.parametrize("name", sorted(p.name for p in DATA.glob("*.parquet")))
def test_the_fixture_files(name, options):
    table = pq.read_table(DATA / name, **options)
    for column in table.columns:
        kind = column.type
        if pa.types.is_struct(kind) and kind.get_field_index("metadata") >= 0:
            reads_alike(column)


def test_the_golden_storage():
    table = ipc.open_file(DATA / "variant" / "golden_storage.arrow").read_all()
    for column in table.columns:
        reads_alike(column)
        assert _variant_backend.structure_fault(column.chunk(0)) is None


# -- every type, in value bytes and in typed leaves ----------------------------


def _primitive(type_id: int, body: bytes) -> bytes:
    return bytes([type_id << 2]) + body


def _ints(fmt: str, *values: int) -> list[bytes]:
    return [struct.pack(fmt, v) for v in values]


#: A value of each primitive type and its edges: the bounds of each width,
#: decimals at every width and scale, signed zeros and NaN payloads, dates
#: and timestamps at and beyond Python's years 1 to 9999, nanoseconds,
#: times of day at and past their bounds, uuids, binary and long strings.
PRIMITIVES = [
    *(_primitive(t, b"") for t in (0, 1, 2, 21, 63)),
    *(_primitive(3, b) for b in _ints("<b", -128, -1, 0, 127)),
    *(_primitive(4, b) for b in _ints("<h", -32768, 5, 127, 32767)),
    *(_primitive(5, b) for b in _ints("<i", -(2**31), 7, 2**31 - 1)),
    *(_primitive(6, b) for b in _ints("<q", -(2**63), 9, 2**63 - 1)),
    *(
        _primitive(7, struct.pack("<Q", bits))
        for bits in (
            0,
            1 << 63,
            0x7FF0000000000001,
            0xFFF8000000000123,
            0x3FF0000000000000,
            1,
        )
    ),
    *(
        _primitive(14, struct.pack("<I", bits))
        for bits in (0, 1 << 31, 0x7F800001, 0xFFC00123, 0x7F800000, 1, 0x3FC00000)
    ),
    *(
        _primitive(11, b)
        for b in _ints("<i", 0, -719162, -719163, 2932896, 2932897, -(2**31), 2**31 - 1)
    ),
    *(
        _primitive(t, b)
        for t in (12, 13)
        for b in _ints(
            "<q",
            0,
            1,
            -1,
            -62135596800000000,
            -62135596800000001,
            253402300799999999,
            253402300800000000,
            2**63 - 1,
            -(2**63),
        )
    ),
    *(
        _primitive(t, b)
        for t in (18, 19)
        for b in _ints("<q", 0, -1, 2**63 - 1, 1_700_000_000_123_456_789)
    ),
    *(
        _primitive(17, b)
        for b in _ints("<q", 0, 1, 86_399_999_999, 86_400_000_000, -1, 43_200_000_000)
    ),
    *(
        _primitive(8, bytes([s]) + struct.pack("<i", u))
        for s in (0, 2, 9, 10, 38, 39)
        for u in (0, 1, -1, 123456789, -999999999, 10**9, 2**31 - 1)
    ),
    *(
        _primitive(9, bytes([s]) + struct.pack("<q", u))
        for s in (0, 7, 18, 19, 38)
        for u in (0, 5, -(10**18) + 1, 10**18, 10**17, 2**63 - 1)
    ),
    *(
        _primitive(10, bytes([s]) + u.to_bytes(16, "little", signed=True))
        for s in (0, 6, 7, 20, 38)
        for u in (0, 1, -(10**38) + 1, 10**38, 10**37, -(2**127))
    ),
    _primitive(15, b"\x00\x00\x00\x00"),
    _primitive(15, b"\x03\x00\x00\x00\x00\xff\x7f"),
    _primitive(16, (64).to_bytes(4, "little") + b"\xc3\xa9" * 32),
    _primitive(16, (3).to_bytes(4, "little") + b'a"\x01'),
    _primitive(16, (2).to_bytes(4, "little") + b"\xff\xfe"),
    _primitive(20, uuid.UUID("00112233-4455-6677-8899-aabbccddeeff").bytes),
    b"\x01",
    b"\x05a\x01\\",
    b"\x09\xe6\x97\xa5",
    b"\x05\xff",
    b"\xfd" + b"x" * 63,
]


def test_every_primitive_in_value_bytes():
    rows = [(b"\x01\x00\x00", value) for value in PRIMITIVES]
    for row in rows:  # one at a time: a fault ends a read
        reads_alike(unshredded([row]))
    # In an object and an array too, where a fault has a path.
    for value in PRIMITIVES:
        obj = b"\x02\x01\x00\x00" + bytes([len(value)]) + value
        reads_alike(
            unshredded(
                [
                    (b"\x11\x01\x00\x01k", obj),
                    (b"\x01\x00\x00", b"\x03\x01\x00" + bytes([len(value)]) + value),
                ]
            )
        )


#: A typed leaf of every Arrow type: what each holds, its out-of-range
#: values, and types no Variant shreds to (refused, with their types spelt).
LEAVES = [
    pa.array([True, False, None]),
    *(
        pa.array([lo, hi, None], kind)
        for kind, lo, hi in (
            (pa.int8(), -128, 127),
            (pa.int16(), -(2**15), 2**15 - 1),
            (pa.int32(), -(2**31), 2**31 - 1),
            (pa.int64(), -(2**63), 2**63 - 1),
        )
    ),
    pa.array([1.5, -0.0, float("nan"), None], pa.float64()),
    pa.array([0x7F800001, 0x80000000, 0x3FC00000], pa.uint32()).view(pa.float32()),
    *(
        pa.array([b"", b"\x00\xff", None], kind)
        for kind in (pa.binary(), pa.large_binary(), pa.binary_view())
    ),
    pa.array(["ok", "\xe9", "\u2028", None], pa.string()),
    pa.array([b"ok", b"\xff\xfe", None], pa.binary()).view(pa.string()),
    pa.array([b"x", b"\xc3", None], pa.large_binary()).view(pa.large_string()),
    pa.array([b"y" * 20, b"\xff" * 20, b"", None], pa.binary_view()).view(
        pa.string_view()
    ),
    *(
        pa.array([unscaled, -unscaled, 0, None], pa.int32()).view(pa.decimal32(p, s))
        for p, s, unscaled in (
            (9, 2, 123456789),
            (9, 0, 999999999),
            (5, 5, 10**5),
            (4, 1, 10**4 - 1),
        )
    ),
    *(
        pa.array([unscaled, None], pa.int64()).view(pa.decimal64(p, s))
        for p, s, unscaled in ((18, 9, 10**18 - 1), (10, 3, 10**10), (18, 18, 1))
    ),
    *(
        pa.array([Decimal(text), None], pa.decimal128(p, s))
        for p, s, text in (
            (38, 0, "9" * 38),
            (38, 38, "0." + "0" * 37 + "1"),
            (20, 7, "-1.0000000"),
            (38, 10, "0E-10"),
        )
    ),
    pa.array([Decimal("-" + "9" * 40), None], pa.decimal256(40, 0)),
    pa.array([Decimal("12.5"), None], pa.decimal256(38, 1)),
    pa.array([Decimal("1" * 69), Decimal(-(10**38)), None], pa.decimal256(76, 0)).view(
        pa.decimal256(38, 0)
    ),
    pa.Array.from_buffers(
        pa.decimal128(38, 0),
        1,
        [None, pa.py_buffer((10**38).to_bytes(16, "little", signed=True))],
    ),
    pa.array([0, -719162, -719163, 2932896, 2932897, None], pa.int32()).view(
        pa.date32()
    ),
    pa.array([0, 86_399_999_999, 86_400_000_000, -1, None], pa.int64()).view(
        pa.time64("us")
    ),
    *(
        pa.array(
            [0, -62135596800000001, 253402300799999999, 2**63 - 1, None], pa.int64()
        ).view(pa.timestamp(unit, tz))
        for unit in ("us", "ns")
        for tz in (None, "UTC", "America/New_York")
    ),
    pa.ExtensionArray.from_storage(
        pa.uuid(), pa.array([uuid.UUID(int=1).bytes, None], pa.binary(16))
    ),
    *(
        pa.nulls(2, kind)
        for kind in (
            pa.uint8(),
            pa.float16(),
            pa.time32("s"),
            pa.time64("ns"),
            pa.date64(),
            pa.timestamp("ms"),
            pa.null(),
            pa.binary(3),
            pa.duration("us"),
            pa.decimal128(5, -2),
            pa.list_view(pa.int64()),
        )
    ),
    pa.array(["a", None], pa.string()).dictionary_encode(),
    pa.ExtensionArray.from_storage(pa.json_(), pa.array(['"j"'])),
]


def _group(typed: pa.Array, value: pa.Array | None = None) -> pa.StructArray:
    value = pa.nulls(len(typed), pa.binary()) if value is None else value
    return pa.StructArray.from_arrays([value, typed], ["value", "typed_value"])


@pytest.mark.parametrize("leaf", LEAVES, ids=lambda leaf: str(leaf.type))
def test_every_typed_leaf(leaf):
    n = len(leaf)
    empty = pa.array([b"\x01\x00\x00"] * n, pa.binary())
    keyed = pa.array([b"\x11\x01\x00\x01f"] * n, pa.binary())
    top = _group(leaf)
    reads_alike(
        pa.StructArray.from_arrays(
            [empty, top.field(0), leaf], ["metadata", "value", "typed_value"]
        )
    )
    # As an object's field, beside a value in value bytes (both set, the
    # row is refused), and as an array's elements.
    for value in (None, pa.array([b"\x0c\x01"] * n, pa.binary())):
        field = _group(pa.StructArray.from_arrays([_group(leaf, value)], ["f"]))
        reads_alike(
            pa.StructArray.from_arrays(
                [keyed, field.field(0), field.field(1)],
                ["metadata", "value", "typed_value"],
            )
        )
    elements = pa.ListArray.from_arrays(pa.array([0, n], pa.int32()), _group(leaf))
    reads_alike(
        pa.StructArray.from_arrays(
            [empty[:1], pa.nulls(1, pa.binary()), elements],
            ["metadata", "value", "typed_value"],
        )
    )


@pytest.mark.parametrize(
    "decl",
    [
        {"type": t}
        for t in (
            "int8",
            "int64",
            "double",
            "boolean",
            "string",
            "binary",
            "date",
            "time",
            "timestamp",
            "timestamptz",
            "uuid",
        )
    ]
    + [
        {"type": "decimal8", "precision": 12, "scale": 2},
        {"type": "decimal16", "precision": 38, "scale": 0},
    ],
)
def test_a_value_its_leaf_takes_is_not_canonical(decl):
    # The same primitives in value beside each leaf: the ones the leaf
    # takes are a second spelling of the row (canonical only).
    plan = codec.compile_plan(decl)
    n = len(PRIMITIVES)
    typed = pa.nulls(n, plan.write_type.field("typed_value").type)
    values = pa.array(PRIMITIVES, pa.binary())
    storage = pa.StructArray.from_arrays(
        [pa.array([b"\x01\x00\x00"] * n, pa.binary()), values, typed],
        ["metadata", "value", "typed_value"],
    )
    for i in range(n):
        reads_alike(storage[i : i + 1])


# -- shapes of storage ---------------------------------------------------------

_B = pa.binary()


def _struct(**children: pa.Array) -> pa.StructArray:
    return pa.StructArray.from_arrays(list(children.values()), list(children))


SHAPES = [
    pa.array([1, 2]),
    pa.array([{"value": b"\x00"}]),
    pa.array([{"metadata": b"\x01\x00\x00"}], pa.struct([("metadata", _B)])),
    pa.array(
        [{"metadata": "x", "value": b"\x00"}],
        pa.struct([("metadata", pa.string()), ("value", _B)]),
    ),
    pa.array(
        [{"metadata": b"\x01\x00\x00", "value": b"\x00", "extra": 1}],
        pa.struct([("metadata", _B), ("value", _B), ("extra", pa.int8())]),
    ),
    pa.StructArray.from_arrays(
        [pa.array([b"\x01\x00\x00"]), pa.array([b"\x00"]), pa.array([b"\x00"])],
        ["metadata", "value", "value"],
    ),
    pa.StructArray.from_arrays(
        [pa.array([b"\x01\x00\x00"]), pa.array([1])], ["metadata", "typed_value"]
    ),
    _struct(
        metadata=pa.array([b"\x01\x00\x00"]),
        value=pa.array([b"\x00"]),
        typed_value=_struct(a=pa.array([1])),
    ),
    _struct(
        metadata=pa.array([b"\x01\x00\x00"]),
        value=pa.array([b"\x00"]),
        typed_value=_struct(a=_struct(typed_value=pa.array([1]))),
    ),
    _struct(
        metadata=pa.array([b"\x01\x00\x00"]),
        value=pa.array([b"\x00"]),
        typed_value=_struct(**{"a.b": _struct(value=pa.array([1]))}),
    ),
    _struct(
        metadata=pa.array([b"\x01\x00\x00"]),
        value=pa.array([b"\x00"]),
        typed_value=_struct(**{"\u2028": pa.array([b"x"])}),
    ),
    _struct(
        metadata=pa.array([None, b"\x01\x00\x00"], _B),
        value=pa.array([b"\x00", None], _B),
    ),
    _struct(
        metadata=pa.array([b"\x01\x00\x00"] * 3),
        value=pa.array([b"\x00", None, b"\x0c\x05"]),
    ),
    pa.nulls(0, pa.struct([("metadata", _B), ("value", _B)])),
    pa.array(
        [{"metadata": b"\x01\x00\x00", "value": b"\x00"}],
        pa.struct([("metadata", pa.large_binary()), ("value", pa.binary_view())]),
    ),
]


@pytest.mark.parametrize("storage", SHAPES, ids=range(len(SHAPES)))
def test_the_shapes_of_storage(storage):
    reads_alike(storage)
    reads_alike(storage, nullable=False)


def test_extension_storage_is_no_struct():
    storage = variant.encode_json(["1"]).array
    reads_alike(pa.ExtensionArray.from_storage(_Opaque(storage.type), storage))


class _Opaque(pa.ExtensionType):
    def __init__(self, storage: pa.DataType) -> None:
        super().__init__(storage, "pyhoglake.test_opaque")

    def __arrow_ext_serialize__(self) -> bytes:
        return b""

    @classmethod
    def __arrow_ext_deserialize__(cls, storage, serialized):
        return cls(storage)


# -- mutated bytes -------------------------------------------------------------


def _mutate(rng: random.Random, data: bytes) -> bytes:
    data = bytearray(data)
    for _ in range(rng.choice([1, 1, 1, 2, 3])):
        how, at = rng.random(), rng.randrange(len(data) + 1)
        if how < 0.3 and data:
            data[min(at, len(data) - 1)] ^= 1 << rng.randrange(8)
        elif how < 0.5 and data:
            data[min(at, len(data) - 1)] = rng.choice(
                [0x00, 0x01, 0x02, 0x03, 0x11, 0x7F, 0x80, 0xFF, rng.randrange(256)]
            )
        elif how < 0.65:
            data[at:at] = bytes([rng.randrange(256)])
        elif how < 0.8:
            del data[at : at + rng.randrange(1, 4)]
        elif how < 0.9:
            del data[at:]
        else:
            data[at:at] = data[: rng.randrange(1, 6)]
    return bytes(data)


def _sources(rng: random.Random, n: int) -> list[tuple[bytes, bytes]]:
    texts = [t.encode() for t in _edges() if len(t) < 3000] + _rows(rng, n)
    encoded = variant.encode_json(pa.array(texts, _B), on_invalid="null").array
    pairs = [
        (m, v)
        for m, v in zip(
            encoded.field(0).to_pylist(), encoded.field(1).to_pylist(), strict=True
        )
        if v
    ]
    for name in NAMES:
        pairs.append(
            (
                (VARIANT / f"{name}.metadata").read_bytes(),
                (VARIANT / f"{name}.value").read_bytes(),
            )
        )
    return pairs


def test_mutated_bytes():
    rng = random.Random(20261102)
    pairs = _sources(rng, 3000)
    mutated = []
    for m, v in (rng.choice(pairs) for _ in range(MUTATIONS)):
        mutated.append(
            (_mutate(rng, m), v) if rng.random() < 0.3 else (m, _mutate(rng, v))
        )
    storage = unshredded(mutated)
    faults: dict[str, int] = {}
    for i in range(len(storage)):  # one at a time: a fault ends a read
        for fault in reads_alike(storage[i : i + 1]):
            reason = fault[1] if fault else "read"
            faults[reason] = faults.get(reason, 0) + 1
    assert {"read", "malformed_variant", "non_canonical"} <= set(faults)
    assert sum(faults.values()) == 3 * MUTATIONS
    print(f"{MUTATIONS} mutated values, read in 3 modes: {faults}")


@pytest.mark.parametrize(
    "decl",
    [MILLRACE, RANDOM, {"type": "array", "element": RANDOM}],
    ids=["millrace", "random", "array"],
)
def test_mutated_bytes_under_shredded_storage(decl):
    # The row's metadata or residual mutated under every typed column:
    # field names looked up, residuals merged, keys checked.
    rng = random.Random(len(json.dumps(decl)))
    rows = pa.array(_rows(rng, 600, decl), _B)
    storage = variant.encode_json(rows, shredding=decl, on_invalid="null").array
    columns = [storage.field(i) for i in range(storage.type.num_fields)]
    for _ in range(MUTATIONS // 50):
        i, at = rng.randrange(len(storage)), rng.choice([0, 1])
        raw = columns[at][i].as_py()
        if raw is None:
            continue
        changed = list(columns)
        changed[at] = pa.array([_mutate(rng, raw)], _B)
        others = [c[i : i + 1] for c in columns]
        others[at] = changed[at]
        reads_alike(pa.StructArray.from_arrays(others, fields=list(storage.type)))


# -- the encode corpus read back -----------------------------------------------


def test_the_encode_corpus_reads_back():
    # Rust's encoding of the randomized rows under each declaration, read by
    # both: the same values and no fault, canonical included; and no
    # structural fault.
    rng = random.Random(20261103)
    seen = batch = 0
    modes = ((False, False), (True, True))
    while seen < ROWS:
        decl = RANDOMIZED[batch % len(RANDOMIZED)]
        rows = _rows(rng, min(5000, ROWS - seen), decl)
        stored = _variant_backend.encode(
            pa.array(rows, _B),
            codec.compile_plan(decl),
            VariantReport(),
            json_text=True,
            column=None,
            nullable=True,
            on_invalid="null",
            backend="native",
        )
        for chunk in stored:
            assert reads_alike(chunk, modes=modes) == [None, None]
            assert _variant_backend.structure_fault(chunk) is None
        seen, batch = seen + len(rows), batch + 1
    texts = [path.read_bytes() for path in sorted(JSON_TEST_SUITE.glob("*.json"))]
    for decl in (None, MILLRACE, RANDOM):
        reads_alike(
            variant.encode_json(
                pa.array(texts, _B), shredding=decl, on_invalid="null"
            ).array
        )


# -- structure_fault -----------------------------------------------------------


def _nulled(rng: random.Random, array: pa.Array) -> pa.Array:
    """``array`` with nulls set at random in it and below it."""
    kind = array.type
    if pa.types.is_struct(kind):
        children = [_nulled(rng, array.field(i)) for i in range(kind.num_fields)]
        mask = pc.or_(array.is_null(), pa.array([rng.random() < 0.05 for _ in array]))
        return pa.StructArray.from_arrays(children, fields=list(kind), mask=mask)
    if pa.types.is_list(kind):
        mask = pa.array([rng.random() < 0.05 for _ in array])
        offsets = array.offsets
        return pa.ListArray.from_arrays(
            offsets,
            _nulled(rng, array.values),
            type=kind,
            mask=pc.or_(mask, array.is_null()),
        )
    if rng.random() < 0.3 and not isinstance(kind, pa.BaseExtensionType):
        return pc.if_else(
            pa.array([rng.random() < 0.02 for _ in array]), pa.scalar(None, kind), array
        ).cast(kind)
    return array


@pytest.mark.parametrize(
    "decl",
    [None, MILLRACE, RANDOM, {"type": "array", "element": RANDOM}],
    ids=["unshredded", "millrace", "random", "array"],
)
def test_structure_fault_finds_the_same_null(decl):
    rng = random.Random(7)
    rows = pa.array(_rows(rng, 300, decl), _B)
    storage = variant.encode_json(rows, shredding=decl, on_invalid="null").array
    found = 0
    for _ in range(60):
        nulled = _nulled(rng, storage)
        python = codec.structure_fault(nulled)  # conftest checks Rust's too
        assert _variant_backend.structure_fault(nulled) == python
        found += python is not None
        for piece in (nulled[1:], nulled[5:9]):
            assert _variant_backend.structure_fault(piece) == codec.structure_fault(
                piece
            )
    assert found


def test_rust_reads_what_it_is_handed():
    # The tags themselves: Python's values, spelt by type.
    moment = datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)
    storage = variant.encode_python(
        [
            {"d": Decimal("-1.50"), "u": uuid.UUID(int=5), "t": time(0, 0, 1)},
            VARIANT_NULL,
            None,
            [None, b"\x00", date(1, 1, 1), moment, moment.replace(tzinfo=None)],
        ]
    ).array
    rows, fault = rust_reads(NATIVE, [storage], strict=True, canonical=True)
    assert fault is None
    assert rows == [
        [
            "dict",
            [
                ["d", ["decimal", -150, 2]],
                ["t", ["time", 10**6]],
                ["u", ["uuid", "0" * 31 + "5"]],
            ],
        ],
        ["variant_null"],
        None,
        [
            "list",
            [
                None,
                ["bytes", "00"],
                ["date", -719162],
                ["datetime", -1, True],
                ["datetime", -1, False],
            ],
        ],
    ]
