"""The Rust VARIANT codec (pyhoglake-variant-codec) against the Python one,
its reference while both exist: the same chunks, storage byte for byte
(every slot of every child, the placeholders under null parents too),
reports, reasons, paths and messages, for JSON text under any declaration:
unshredded, millrace's, every accepted one of the shredding vectors, and
ones the randomized rows' keys fill.

Skipped where the package is not installed: `uv sync --group
variant-codec` builds it, and CI's variant-codec job checks it imports
before it runs these. PYHOGLAKE_VARIANT_DIFFERENTIAL_ROWS sets the volume
of the randomized differential (a PR runs at least 1M rows once).
"""

from __future__ import annotations

import dataclasses
import json
import logging
import math
import os
import random
import subprocess
import sys
from collections import Counter
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from test_parquet_testing import BYTE_EQUAL, CASES, SHREDDED, _variant_bin
from variant_corpus import DECLARATIONS as CORPUS
from variant_corpus import JSON_ROWS
from variant_helpers import storage_digest, storage_slots

from pyhoglake import ValidationError, VariantEncodingError, _variant_backend, variant
from pyhoglake import _variant_codec as codec
from pyhoglake.variant import VARIANT_NULL, VariantReport

NATIVE, WHY = _variant_backend.native()
pytestmark = pytest.mark.skipif(NATIVE is None, reason=WHY)

#: Taken before conftest swaps it for a failure, for the fallback tests.
REAL_FALL_BACK = _variant_backend._fall_back
ROWS = int(os.environ.get("PYHOGLAKE_VARIANT_DIFFERENTIAL_ROWS", "20000"))
UNSHREDDED = [None, {"type": "variant"}]
PARQUET_TESTING = Path(__file__).parents[1] / "data" / "parquet-testing"
JSON_TEST_SUITE = Path(__file__).parents[1] / "data" / "JSONTestSuite" / "test_parsing"
VECTORS = Path(__file__).parents[1] / "vectors" / "variant_shredding_vectors.json"
#: Every declaration the shredding vectors accept.
ACCEPTED = [
    vector["declaration"]
    for vector in json.loads(VECTORS.read_text())["vectors"]
    if vector["expect"] == "accept"
]
#: millrace's properties_sv on ingest.events: 16 string fields.
MILLRACE_NAMES = ["$session_id", "$window_id", *(f"$group_{i}" for i in range(5)),
                  "$current_url", "$pathname", "$host", "$browser", "$os",
                  "$device_type", "$lib", "$referring_domain", "$device_id"]  # fmt: skip
MILLRACE = {"type": "object", "fields": [{"name": n, "type": "string"} for n in MILLRACE_NAMES]}  # fmt: skip
#: The randomized rows' keys (_KEYS, then "", "1" or "2"), at every type
#: JSON can fill and some it cannot, nested.
RANDOM = {
    "type": "object",
    "fields": [
        {"name": "a", "type": "int64"},
        {"name": "A2", "type": "string"},
        {"name": "a1", "type": "boolean"},
        {"name": "1", "type": "double"},
        {"name": "b.c", "type": "array", "element": {"type": "string"}},
        {"name": "x[0]", "type": "int8"},
        {"name": "\u00e9", "type": "decimal16", "precision": 20, "scale": 0},
        {"name": "\u65e5\u672c", "type": "int32"},
        {"name": "\x01", "type": "int16"},
        {"name": "$browser", "type": "variant"},
        {"name": 'q"', "type": "uuid"},
        {"name": "aa", "type": "array", "element": {"type": "array", "element": {"type": "double"}}},
        {
            "name": "$set",
            "type": "object",
            "fields": [
                {"name": "a", "type": "decimal16", "precision": 38, "scale": 0},
                {"name": "A2", "type": "variant"},
                {"name": "\u2028", "type": "decimal8", "precision": 18, "scale": 0},
                {"name": "$set", "type": "array", "element": {"type": "object", "fields": [
                    {"name": "aa", "type": "int16"}, {"name": "a", "type": "string"}]}},
            ],
        },
    ],
}  # fmt: skip
#: The randomized differential's declarations, a batch each in turn.
RANDOMIZED = [None, MILLRACE, RANDOM, {"type": "array", "element": RANDOM},
              {"type": "string"}, {"type": "int64"},
              {"type": "decimal16", "precision": 38, "scale": 0}]  # fmt: skip


def _encode(rows, backend, decl=None, on_invalid="null", **chunking):
    report = VariantReport()
    chunks = _variant_backend.encode(
        rows,
        codec.compile_plan(decl),
        report,
        json_text=True,
        column="c",
        nullable=True,
        on_invalid=on_invalid,
        backend=backend,
        **chunking,
    )
    report.encode_seconds = 0.0
    return chunks, report


def assert_same(rows, decl=None, **chunking):
    """Both backends give the same chunks, storage and report, but for the
    count of rows that took Python's verdict at a recursion bound."""
    python, python_report = _encode(rows, "python", decl, **chunking)
    native, native_report = _encode(rows, "native", decl, **chunking)
    native_report.recursion_bound_rows = 0
    assert native_report == python_report
    assert [len(c) for c in native] == [len(c) for c in python]
    for mine, theirs in zip(native, python, strict=True):
        assert mine.type == theirs.type == codec.compile_plan(decl).write_type
        # Every slot of every child, the placeholders under null parents too.
        if storage_digest(mine) != storage_digest(theirs):
            assert storage_slots(mine) == storage_slots(theirs)
    return python_report


def assert_same_faults(rows: list[bytes], decl=None) -> int:
    """Each row Rust refuses, Python refuses with the same reason, path and
    message (the message from Rust's detail); returns how many."""
    plan = codec.compile_plan(decl)
    _, _, _, invalid, _ = NATIVE.encode_json(
        [pa.array(rows, pa.binary())],
        NATIVE.Plan(plan.write_type),
        nullable=True,
        raise_on_invalid=False,
        chunk_rows=codec.CHUNK_ROWS,
        chunk_bytes=codec.CHUNK_BYTES,
        max_int_digits=sys.get_int_max_str_digits(),
    )
    for row, reason, path, detail in invalid:
        try:
            codec.encode(
                [rows[row]],
                plan,
                VariantReport(),
                json_text=True,
                column=None,
                nullable=True,
                on_invalid="raise",
            )
        except VariantEncodingError as exc:
            got = (exc.reason, exc.path, str(exc))
        else:
            got = None
        if (
            reason == "nesting_too_deep"
            and got
            and (path, got[:2])
            in (
                (None, got[:2]),
                (path, (reason, None)),
            )
        ):
            continue  # a recursion bound: the backend takes Python's verdict
        assert got == (reason, path, codec._message(None, 0, path, detail)), rows[row]
    return len(invalid)


# -- the switch, the hook and the fallbacks -----------------------------------


def test_the_switch_is_read_once_and_a_bad_one_fails_the_import(monkeypatch):
    monkeypatch.delenv(_variant_backend.ENV, raising=False)
    assert _variant_backend.mode() == "auto"
    monkeypatch.setenv(_variant_backend.ENV, "rust")
    with pytest.raises(ValueError, match="auto, native or python"):
        _variant_backend.mode()
    # Set after import, it changes nothing; malformed at import, it fails
    # there, not on every encode.
    assert variant.encode_json(["1"]).report.rows == 1
    imported = subprocess.run(
        [sys.executable, "-c", "import pyhoglake"],
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, _variant_backend.ENV: "rust"},
    )
    assert imported.returncode == 1
    assert "PYHOGLAKE_VARIANT_CODEC is auto, native or python" in imported.stderr


def test_auto_takes_rust_for_json_text(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("the Python codec was used")

    monkeypatch.setattr(codec, "encode", refuse)
    for decl in [*UNSHREDDED, *ACCEPTED, MILLRACE, RANDOM]:
        assert variant.encode_json(['{"a":1}'], shredding=decl).report.rows == 1
    with pytest.raises(AssertionError, match="Python codec"):
        variant.encode_python([{"a": 1}], shredding={"type": "string"})


@pytest.mark.parametrize(
    ("values", "why"),
    [(['"\ud800"'], "not UTF-8"), ([b"1", 1], "not UTF-8")],
)
def test_native_refuses_what_it_does_not_take(monkeypatch, values, why):
    monkeypatch.setattr(_variant_backend, "MODE", "native")
    with pytest.raises(RuntimeError, match=why):
        variant.encode_json(values)
    with pytest.raises(RuntimeError, match="encodes JSON text"):
        _variant_backend.encode(
            [1],
            codec.UNSHREDDED_PLAN,
            VariantReport(),
            json_text=False,
            column=None,
            nullable=True,
            on_invalid="raise",
            backend="native",
        )


def test_auto_counts_the_encodes_it_gives_python(monkeypatch):
    # Per process, by why, for millrace to export: auto's quiet route.
    monkeypatch.setattr(_variant_backend, "MODE", "auto")
    monkeypatch.setattr(_variant_backend, "ROUTED", Counter())

    def routed(why, values, json_text=True, backend=None):
        before = Counter(_variant_backend.ROUTED)
        plan, report = codec.UNSHREDDED_PLAN, VariantReport()
        flags = {"column": None, "nullable": True, "on_invalid": "null"}
        _variant_backend.encode(
            values, plan, report, json_text=json_text, backend=backend, **flags
        )
        counted = _variant_backend.ROUTED - before
        assert counted == Counter({("encode", why): 1} if why else {})

    routed("not_utf8", ['"\ud800"', None])
    routed("not_json", [1], json_text=False)
    routed(None, [1], json_text=False, backend="python")
    routed(None, ["1"])  # Rust's
    monkeypatch.setattr(_variant_backend, "_plan", lambda *_: (None, (), "no"))
    routed("plan", ["1"])
    monkeypatch.setattr(_variant_backend, "native", lambda: (None, "it is gone"))
    routed("unavailable", pa.array(["1"]))


def test_a_plan_rust_does_not_lay_out_is_refused():
    # Plans come from compile_plan, which Rust reads off the write form;
    # a type it does not know is refused rather than guessed at.
    plan = codec.compile_plan({"type": "string"})
    metadata, value, _ = plan.write_type
    odd = dataclasses.replace(
        plan,
        write_type=pa.struct(
            [metadata, value, pa.field("typed_value", pa.large_string())]
        ),
    )
    with pytest.raises(
        RuntimeError, match="does not take this plan .*typed_value of LargeUtf8"
    ):
        _variant_backend.encode(
            ['"x"'],
            odd,
            VariantReport(),
            json_text=True,
            column=None,
            nullable=True,
            on_invalid="raise",
            backend="native",
        )
    for kind in (
        pa.int64(),
        pa.struct([pa.field("value", pa.binary())]),
        pa.struct(
            [
                metadata,
                value,
                pa.field("typed_value", pa.struct([pa.field("a", pa.int8())])),
            ]
        ),
        pa.struct(
            [
                metadata,
                value,
                pa.field("typed_value", pa.list_(pa.field("element", value.type))),
            ]
        ),
        pa.struct(
            [
                metadata,
                pa.field("value", pa.binary(), nullable=False),
                pa.field("typed_value", pa.int8()),
            ]
        ),
    ):
        with pytest.raises(ValueError):
            NATIVE.Plan(kind)


def test_a_package_of_another_protocol_is_not_used(monkeypatch):
    # The codec has its own version, so PROTOCOL is the one runtime check.
    protocol = _variant_backend.PROTOCOL
    monkeypatch.setattr(NATIVE, "PROTOCOL", protocol + 1)
    _variant_backend.native.cache_clear()
    try:
        module, why = _variant_backend.native()
        assert module is None
        assert why == f"pyhoglake-variant-codec does not speak protocol {protocol}"
    finally:
        monkeypatch.undo()
        _variant_backend.native.cache_clear()
    assert _variant_backend.native()[0] is NATIVE


@pytest.mark.parametrize("reason", ["mismatch", "type", "panic"])
def test_a_result_that_fails_its_check_is_encoded_again_in_python(
    monkeypatch, caplog, reason
):
    monkeypatch.setattr(_variant_backend, "_fall_back", REAL_FALL_BACK)
    real = NATIVE.encode_json

    def broken(chunks, *args, **kwargs):
        if reason == "panic":
            raise NATIVE.PanicException("boom")
        out, rows, nulls, invalid, counts = real(chunks, *args, **kwargs)
        if reason == "type":
            return [pa.array([1])], rows, nulls, invalid, counts
        return out, rows, nulls, [(1, "duplicate_key", None, "key 'a' ...")], counts

    monkeypatch.setattr(NATIVE, "encode_json", broken)
    monkeypatch.setattr(_variant_backend, "ENCODE_FALLBACKS", Counter())
    rows = ["1", '{"a":[2]}', None]
    with caplog.at_level(logging.WARNING, logger=_variant_backend.__name__):
        encoded = variant.encode_json(rows, shredding={"type": "variant"})
    assert encoded.report.codec_fallbacks == {reason: 1}
    assert _variant_backend.ENCODE_FALLBACKS == Counter({reason: 1})
    row = 1 if reason == "mismatch" else None
    assert caplog.messages == [
        f"VARIANT codec fallback to Python: column=None row={row} reason={reason}"
    ]
    monkeypatch.setattr(NATIVE, "encode_json", real)
    again = variant.encode_json(rows, shredding={"type": "variant"})
    assert encoded.array.equals(again.array)
    assert again.report.codec_fallbacks == {}


# -- the shadow ----------------------------------------------------------------


def test_the_shadow_share_is_read_once_and_a_bad_one_fails_the_import(monkeypatch):
    for text, share in (("", 0.0), ("0", 0.0), ("0.05", 0.05), ("1", 1.0)):
        monkeypatch.setenv(_variant_backend.SHADOW_ENV, text)
        assert _variant_backend.shadow() == share
    for text in ("-0.1", "1.5", "nan", "half"):
        monkeypatch.setenv(_variant_backend.SHADOW_ENV, text)
        with pytest.raises(ValueError, match="a fraction from 0 to 1"):
            _variant_backend.shadow()
    # Set after import, it changes nothing; malformed at import, it fails
    # there, not on every encode.
    assert variant.encode_json(["1"]).report.shadow_seconds == 0
    imported = subprocess.run(
        [sys.executable, "-c", "import pyhoglake"],
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, _variant_backend.SHADOW_ENV: "half"},
    )
    assert imported.returncode == 1
    assert "PYHOGLAKE_VARIANT_SHADOW is a fraction from 0 to 1" in imported.stderr


#: Rows with typed doubles of both signs, integers under doubles, null lists
#: and absent fields under present and null parents: what the shadow must
#: compare slot for slot.
SHADOW_ROWS = [
    '{"b.c":"no","1":-0.0,"aa":[[1.5,-0.0],"x"],"$set":{"$set":[{"aa":1},2]}}',
    None,
    "[1,",
    '{"A2":null,"1":0.0}',
    '{"1":7,"aa":[[2]]}',
    '"x"',
]


def test_the_shadow_keeps_rusts_result_when_python_agrees(monkeypatch):
    monkeypatch.setattr(_variant_backend, "SHADOW", 1.0)
    real, out = NATIVE.encode_json, []

    def spy(*args, **kwargs):
        out.append(real(*args, **kwargs))
        return out[-1]

    monkeypatch.setattr(NATIVE, "encode_json", spy)
    monkeypatch.setattr(_variant_backend, "SHADOWED", Counter())
    encoded = variant.encode_json(SHADOW_ROWS, shredding=RANDOM, on_invalid="null")
    assert encoded.array is out[0][0][0]
    assert encoded.report.codec_fallbacks == {}
    assert encoded.report.shadow_seconds > 0
    monkeypatch.setattr(_variant_backend, "SHADOW", 0.0)
    plain = variant.encode_json(SHADOW_ROWS, shredding=RANDOM, on_invalid="null").report
    # The rate's denominators, per process: one call of six rows compared.
    assert _variant_backend.SHADOWED == Counter(calls=1, rows=len(SHADOW_ROWS))
    timing = {"encode_seconds": 0.0, "shadow_seconds": 0.0}
    assert dataclasses.replace(encoded.report, **timing) == dataclasses.replace(
        plain, **timing
    )


def _covered(chunk: pa.StructArray) -> pa.StructArray:
    """``chunk`` with its null list over one absent element: a placeholder
    no reader sees, Arrow's equality included."""
    typed = chunk.field("typed_value")
    element = pa.array([{"value": None, "typed_value": None}], typed.type.value_type)
    hidden = pa.ListArray.from_arrays(
        pa.array([0, 1], pa.int32()), element, typed.type, mask=pa.array([True])
    )
    return pa.StructArray.from_arrays(
        [chunk.field(0), chunk.field(1), hidden], fields=list(chunk.type)
    )


@pytest.mark.parametrize("how", ["storage", "counts", "raise", "sign", "placeholder"])
def test_a_shadow_mismatch_takes_pythons_result(monkeypatch, caplog, how):
    monkeypatch.setattr(_variant_backend, "_fall_back", REAL_FALL_BACK)
    monkeypatch.setattr(_variant_backend, "SHADOW", 1.0)
    real = NATIVE.encode_json
    decl, rows = MILLRACE, ['{"$browser":"x"}', '{"$os":1}', None]
    if how == "raise":
        rows[2] = "[1,"
    elif how == "sign":
        decl, rows = {"type": "double"}, ["-0.0"]
    elif how == "placeholder":
        decl, rows = {"type": "array", "element": {"type": "string"}}, ['"x"']

    def broken(chunks, *args, **kwargs):
        if how == "sign":  # 0.0 for -0.0: Arrow's equality takes them as one
            chunks = [pa.array([b"0.0"], pa.binary())]
        out, rows, nulls, invalid, counts = real(chunks, *args, **kwargs)
        if how == "storage":
            out = [chunk.take(list(reversed(range(len(chunk))))) for chunk in out]
        elif how == "counts":
            counts = [(typed + 1, fallback, null) for typed, fallback, null in counts]
        elif how == "raise":
            invalid = []  # the invalid row written as a null
        elif how == "placeholder":
            out = [_covered(chunk) for chunk in out]
        return out, rows, nulls, invalid, counts

    monkeypatch.setattr(NATIVE, "encode_json", broken)
    monkeypatch.setattr(_variant_backend, "ENCODE_FALLBACKS", Counter())
    monkeypatch.setattr(_variant_backend, "SHADOWED", Counter())
    with caplog.at_level(logging.WARNING, logger=_variant_backend.__name__):
        if how == "raise":
            with pytest.raises(VariantEncodingError, match="row 2"):
                variant.encode_json(rows, shredding=decl)
        else:
            encoded = variant.encode_json(rows, shredding=decl)
    assert caplog.messages == [
        "VARIANT codec fallback to Python: column=None row=None reason=shadow_mismatch"
    ]
    # Counted per process, a call that raised included (it has no report).
    assert _variant_backend.ENCODE_FALLBACKS == Counter(shadow_mismatch=1)
    assert _variant_backend.SHADOWED == Counter(calls=1, rows=len(rows))
    if how == "raise":
        return
    assert encoded.report.codec_fallbacks == {"shadow_mismatch": 1}
    monkeypatch.setattr(_variant_backend, "SHADOW", 0.0)
    wrong = variant.encode_json(rows, shredding=decl)
    monkeypatch.setattr(NATIVE, "encode_json", real)
    right = variant.encode_json(rows, shredding=decl)
    assert storage_slots(encoded.array) == storage_slots(right.array)
    assert encoded.report.typed == right.report.typed
    if how in ("sign", "placeholder"):
        assert wrong.array.equals(right.array)
        assert storage_slots(wrong.array) != storage_slots(right.array)


def test_the_digest_sees_placeholders_under_null_parents_and_types():
    # What Arrow's equality and Parquet skip, the differential compares.
    offsets = pa.array([0, 1, 1], pa.int32())
    a = pa.ListArray.from_arrays(offsets, pa.array([7]), mask=pa.array([True, False]))
    b = pa.ListArray.from_arrays(pa.array([0, 0, 0], pa.int32()), pa.array([], pa.int64()), mask=pa.array([True, False]))  # fmt: skip
    mask = pa.array([False, True])
    c = pa.StructArray.from_arrays([pa.array([1, 2])], names=["x"], mask=mask)
    d = pa.StructArray.from_arrays([pa.array([1, 3])], names=["x"], mask=mask)
    for x, y in ((a, b), (c, d)):
        assert x.equals(y)
        assert storage_digest(x) != storage_digest(y)
    raw = pa.array([b"\x00" * 16], pa.binary(16))
    assert storage_digest(raw) != storage_digest(
        pa.ExtensionArray.from_storage(pa.uuid(), raw)
    )


# -- the chunk budget through the hook ----------------------------------------


@pytest.mark.parametrize("decl", UNSHREDDED)
def test_chunks_close_at_the_same_rows(decl):
    rows = ['"' + "x" * 100 + '"', None, "[1,", '{"a":"' + "y" * 300 + '"}'] * 5
    for chunking in ({"chunk_rows": 3}, {"chunk_bytes": 216}, {"chunk_bytes": 250}):
        assert_same(rows, decl, **chunking)
    # 105 bytes of value and 3 of metadata a row: closes on reaching the cap.
    chunks, _ = _encode(['"' + "x" * 100 + '"'] * 7, "native", decl, chunk_bytes=216)
    assert [len(chunk) for chunk in chunks] == [2, 2, 2, 1]
    assert assert_same([], decl).rows == 0


#: A field of each kind that counts bytes against the budget as it is
#: placed: a residual, a typed string, a list's elements.
PLACED = {
    "type": "object",
    "fields": [
        {"name": "s", "type": "string"},
        {"name": "l", "type": "array", "element": {"type": "string"}},
        {"name": "z", "type": "int8"},
    ],
}


@pytest.mark.parametrize("decl", [PLACED, {"type": "array", "element": PLACED}])
def test_a_failed_row_keeps_the_bytes_it_placed(decl):
    # As Python's builders count them: a row that fails after placing values
    # leaves their bytes in the chunk's budget, so chunks close alike.
    rows = [
        '{"r":"' + "y" * 200 + '","z":' + "9" * 39 + "}",
        '{"s":"' + "x" * 150 + '","l":["a","\\ud800"]}',
        '{"s":"x","l":"no","z":300}',
        None,
        '"' + "w" * 120 + '"',
    ] * 4
    if decl["type"] == "array":
        rows = [row and f"[{row},{row}]" for row in rows]
    for chunk_bytes in (100, 200, 300, 450, 1000):
        assert_same(rows, decl, chunk_bytes=chunk_bytes)
    # The residual's 207 bytes close a chunk though its row is not written.
    chunks, report = _encode(rows[:1] * 3, "native", decl, chunk_bytes=200)
    assert report.invalid_rows == 3
    assert [len(chunk) for chunk in chunks] == [1, 1, 1]


def test_arrow_text_of_every_kind_and_chunking():
    rows = ['{"a":1}', None, "[1,", '"\\ud800"', "2"]
    for kind in (pa.string(), pa.large_string(), pa.string_view(), pa.binary(),
                 pa.large_binary(), pa.binary_view(), pa.json_()):  # fmt: skip
        whole = pa.array(rows, kind)
        assert_same(whole)
        assert_same(pa.chunked_array([whole.slice(0, 2), whole.slice(2)]))
    assert_same(pa.array([None, None], pa.null()))
    assert_same(pa.array([b"\xff", b'"\xc3"', b"1"], pa.binary()))


@pytest.mark.parametrize("decl", [None, RANDOM], ids=["unshredded", "random"])
@pytest.mark.parametrize("on_invalid", ["raise", "null"])
@pytest.mark.parametrize(
    ("text", "reason", "path"),
    [
        ('{"a":1,"\\u0061":2}', "duplicate_key", None),
        ('{"\\u00e9":1,"\u00e9":2}', "duplicate_key", None),
        ('{"x":{"\\ud800":1}}', "invalid_unicode", '$.x["\\ud800"]'),
        ('{"b":["ok","\\udc00"]}', "invalid_unicode", "$.b[1]"),
        ("[" * 128 + "[]" + "]" * 128, None, None),
        ("[" * 129 + "1" + "]" * 129, "nesting_too_deep", "$" + "[0]" * 128),
        ('{"n":' + "9" * 39 + "}", "integer_out_of_range", "$.n"),
        ("-" + "9" * 4301, "integer_out_of_range", None),
        ("[1e400]", "non_finite_number", None),
        ("[1,]", "invalid_json", None),
    ],
    ids=lambda value: value[:24] if isinstance(value, str) else None,
)
def test_invalid_rows_are_refused_alike(text, reason, path, on_invalid, decl):
    rows = ["1", text, "2"]
    if reason is None:
        assert assert_same(rows, decl).invalid_rows == 0
        return
    if on_invalid == "null":
        report = assert_same(rows, decl)
        assert report.first_invalid == ("c", 1, reason, path)
        return
    errors = []
    for backend in ("python", "native"):
        with pytest.raises(VariantEncodingError) as raised:
            _encode(rows, backend, decl, on_invalid="raise")
        error = raised.value
        errors.append((str(error), error.reason, error.row, error.path))
    assert errors[0] == errors[1] == (errors[0][0], reason, 1, path)


def test_rows_past_a_recursion_bound_take_pythons_verdict():
    # CPython's bound moves with the C stack in use (3.12, 3.13) or its size
    # (3.14); the Rust parser's is 10,000. Rows on either side of it agree.
    rows = ["[" * n + "]" * n for n in (9_990, 9_995, 9_999, 10_000, 20_000)]
    rows.append("[" * 20_000)
    python, python_report = _encode(rows, "python")
    native, native_report = _encode(rows, "native")
    assert native_report.recursion_bound_rows >= 1
    assert native_report.codec_fallbacks == {}
    native_report.recursion_bound_rows = 0
    assert native_report == python_report
    assert python_report.invalid_rows == len(rows)
    assert native[0].equals(python[0])
    for text in rows:
        errors = []
        for backend in ("python", "native"):
            with pytest.raises(VariantEncodingError) as raised:
                _encode([text], backend, on_invalid="raise")
            errors.append((str(raised.value), raised.value.path))
        assert errors[0] == errors[1]


@pytest.mark.parametrize("decl", [None, MILLRACE], ids=["unshredded", "millrace"])
@pytest.mark.parametrize("on_invalid", ["raise", "null"])
def test_sql_null_into_not_null_raises_alike(on_invalid, decl):
    for backend in ("python", "native"):
        with pytest.raises(VariantEncodingError, match="row 1: SQL NULL") as raised:
            _variant_backend.encode(
                ["1", None],
                codec.compile_plan(decl),
                VariantReport(),
                json_text=True,
                column="c",
                nullable=False,
                on_invalid=on_invalid,
                backend=backend,
            )
        assert raised.value.reason == "sql_null_in_not_null"


# -- apache/parquet-testing ---------------------------------------------------


def _json_values():
    """Every JSON-representable value of the vendored vectors, decoded."""
    for value in sorted((PARQUET_TESTING / "variant").glob("*.value")):
        metadata = value.with_suffix(".metadata").read_bytes()
        yield value.stem, metadata, value.read_bytes()
    for path in sorted((PARQUET_TESTING / "shredded_variant").glob("*.variant.bin")):
        data = path.read_bytes()
        width = (data[0] >> 6) + 1
        count = int.from_bytes(data[1 : 1 + width], "little")
        start = 1 + width
        last = int.from_bytes(
            data[start + count * width : start + (count + 1) * width], "little"
        )
        split = start + (count + 1) * width + last
        yield path.name, data[:split], data[split:]


def _json_like(value: object) -> bool:
    if type(value) is dict:
        return all(_json_like(v) for v in value.values())
    if type(value) is list:
        return all(_json_like(v) for v in value)
    if type(value) is float:
        return math.isfinite(value)
    return value is None or type(value) in (bool, int, str)


def test_every_json_value_of_the_vectors():
    taken = []
    for name, metadata, value in _json_values():
        storage = pa.StructArray.from_arrays(
            [pa.array([metadata], pa.binary()), pa.array([value], pa.binary())],
            fields=list(variant.storage_type()),
        )
        (decoded,) = variant.to_python(storage)
        decoded = None if decoded is VARIANT_NULL else decoded
        if not _json_like(decoded):
            continue
        taken.append(name)
        texts = [json.dumps(decoded), json.dumps(decoded, ensure_ascii=False)]
        assert_same(texts)
        stored = _encode(texts[:1], "native")[0][0].to_pylist()[0]
        if name in BYTE_EQUAL:  # pyhoglake writes these byte for byte
            assert (stored["metadata"], stored["value"]) == (metadata, value), name
    assert len(taken) == 95
    assert len(BYTE_EQUAL.intersection(taken)) == 14


def _json_texts() -> list[str]:
    """The JSON-representable values of the vectors, as text."""
    texts = []
    for _, metadata, value in _json_values():
        storage = pa.StructArray.from_arrays(
            [pa.array([metadata], pa.binary()), pa.array([value], pa.binary())],
            fields=list(variant.storage_type()),
        )
        (decoded,) = variant.to_python(storage)
        decoded = None if decoded is VARIANT_NULL else decoded
        if _json_like(decoded):
            texts += [json.dumps(decoded), json.dumps(decoded, ensure_ascii=False)]
    return texts


_LEAVES = {pa.int8(): "int8", pa.int16(): "int16", pa.int32(): "int32",
           pa.int64(): "int64", pa.float32(): "float", pa.float64(): "double",
           pa.bool_(): "boolean", pa.string(): "string", pa.binary(): "binary",
           pa.date32(): "date", pa.time64("us"): "time", pa.uuid(): "uuid",
           pa.timestamp("us"): "timestamp", pa.timestamp("ns"): "timestamp_ns",
           pa.timestamp("us", "UTC"): "timestamptz",
           pa.timestamp("ns", "UTC"): "timestamptz_ns"}  # fmt: skip


def _declaration(group: pa.DataType) -> dict:
    """The declaration of a shredded case's group; KeyError where the
    grammar has no type for a leaf."""
    if group.get_field_index("typed_value") < 0:
        return {"type": "variant"}
    kind = group.field("typed_value").type
    if pa.types.is_struct(kind):
        fields = [{"name": f.name, **_declaration(f.type)} for f in kind]
        return {"type": "object", "fields": fields}
    if pa.types.is_list(kind):
        return {"type": "array", "element": _declaration(kind.value_type)}
    if pa.types.is_decimal(kind):
        width = 4 if kind.precision <= 9 else 8 if kind.precision <= 18 else 16
        return {"type": f"decimal{width}", "precision": kind.precision, "scale": kind.scale}  # fmt: skip
    return {"type": _LEAVES[kind]}


def test_the_layouts_of_the_shredded_cases():
    # Each case's layout that the grammar can declare, over every
    # JSON-representable value of the vectors and the corpus's edge rows.
    layouts = {}
    for case in CASES:
        group = pq.read_schema(SHREDDED / case["parquet_file"]).field("var").type
        try:
            decl = _declaration(group)
            codec.compile_plan(decl)
        except (KeyError, ValidationError):
            continue
        layouts[json.dumps(decl, sort_keys=True)] = decl
    rows = [*_json_texts(), *JSON_ROWS]
    for case in CASES:
        for name in case.get("variant_files") or [case.get("variant_file")]:
            value = _variant_bin(name)
            if name and _json_like(None if value is VARIANT_NULL else value):
                rows.append(json.dumps(None if value is VARIANT_NULL else value))
    for decl in layouts.values():
        assert_same(rows, decl)
    assert len(layouts) == 27  # all but uint32 and fixed_size_binary[4]


@pytest.mark.parametrize(
    "decl",
    [*ACCEPTED, *CORPUS.values(), MILLRACE, RANDOM],
    ids=[f"vector-{i}" for i in range(len(ACCEPTED))] + [*CORPUS, "millrace", "random"],
)
def test_every_declaration_over_the_corpus(decl):
    rng = random.Random(len(json.dumps(decl)))
    fitted = [_fit(rng, decl) for _ in range(300)] if decl else []
    assert_same([*JSON_ROWS, *_json_texts(), *fitted], decl)


#: Strings cut short after escapes, controls and nothing, as text and keys:
#: the position and wording are the running CPython's.
UNTERMINATED = [
    b'"\\u0001\\\\\\u0001\\u000A',  # CI on 3.13.16, 2026-11-02
    *(b'"' + tail for tail in (b"", b"\\", b"\\u", b"\\u0", b"\\u004", b"\\u0041", b"ab\\u0041", b"\\n\\u0041",
                               b"\\\\\\u0041", b"\\u0041x", b"\\ud800\\udc00", b"\\ud800\\udc0", b"\\ud800\\u0041",
                               b"\\x", b"\x01", b"a\x01\\u0041", b"\\u0041\\u0042", b"\\/")),
    b'{"\\u0041', b'["\\u0041', b'{"a":"\\u0041', b'["x","\\ud83d\\ude00',
]  # fmt: skip


def test_strings_cut_short():
    report = assert_same(UNTERMINATED)
    assert assert_same_faults(UNTERMINATED) == report.invalid_rows == len(UNTERMINATED)


@pytest.mark.parametrize("decl", [None, MILLRACE, RANDOM, {"type": "string"},
                                  {"type": "array", "element": RANDOM}])  # fmt: skip
def test_json_test_suite(decl):
    # Every test_parsing file as one row, verdicts by Python's parser, not
    # the RFC's (Python takes NaN to its hook, refuses \f as whitespace).
    rows = [path.read_bytes() for path in sorted(JSON_TEST_SUITE.glob("*.json"))]
    report = assert_same(pa.array(rows, pa.binary()), decl)
    assert assert_same_faults(rows, decl) == report.invalid_rows
    assert (len(rows), report.invalid_rows) == (318, 221)


# -- randomized ---------------------------------------------------------------

_KEYS = ["$browser", "$set", "a", "A", "", "b.c", "x[0]", 'q"', "back\\slash",
         "\u00e9", "\u2028", "\x01", "\u65e5\u672c", "\U0001f600", "aa"]  # fmt: skip
_BAD = [b"{", b"}", b"[", b"]", b",", b":", b'"', b"\\", b" ", b"\x01", b"\xff",
        b"\xc3", b"\xe0\x80", b"\xed\xa0\x80", b"x", b"0", b"-", b".", b"e",
        b"N", b"\\u", b"\\ud800", b"\xf0\x9f"]  # fmt: skip


def _string(rng: random.Random, text: str) -> str:
    out = ['"']
    for char in text:
        code, r = ord(char), rng.random()
        if char in '"\\':
            out.append("\\" + char)
        elif code < 0x20 or r < 0.05 and code < 0x10000:
            out.append(rng.choice(["\\u{:04x}", "\\u{:04X}"]).format(code))
        elif r < 0.07 and code >= 0x10000:
            code -= 0x10000
            out.append(f"\\u{0xD800 | code >> 10:04x}\\u{0xDC00 | code & 0x3FF:04x}")
        else:
            out.append(char)
    if rng.random() < 0.01:
        out.append(rng.choice(["\\ud800", "\\udc00", "\\udfff\\ud800"]))
    return "".join(out) + '"'


def _number(rng: random.Random) -> str:
    r = rng.random()
    if r < 0.3:
        return str(rng.choice([0, 127, 128, -128, -129, 32767, 32768, -32769, 2**31,
                               -(2**31) - 1, 2**63 - 1, 2**63, -(2**63) - 1,
                               10**38 - 1, -(10**38) + 1, 10**38]))  # fmt: skip
    if r < 0.5:
        return str(rng.randrange(-(10**6), 10**6))
    if r < 0.6:
        digits = rng.choice([17, 18, 19, 20, 37, 38, 39, 40, 60])
        lead = str(rng.randrange(1, 10))
        return rng.choice(["", "-"]) + lead + str(rng.randrange(10 ** (digits - 1))).zfill(digits - 1)  # fmt: skip
    if r < 0.7:
        return rng.choice(["-0", "-0.0", "0e0", "5e-324", "-1e-310", "1e308", "1e309",
                           "-1E400", "1.5E+3", "123.456e-7", "1e22", "9007199254740993"])  # fmt: skip
    return repr(rng.uniform(-1e6, 1e6))


def _value(rng: random.Random, depth: int) -> str:
    ws = lambda: rng.choice(["", "", "", " ", "\n", "\t ", "\r\n"])
    r = rng.random()
    if depth > 4 or r < 0.55:
        kind = rng.random()
        if kind < 0.35:
            return _number(rng)
        if kind < 0.8:
            letters = rng.choice(
                ["abcdefgh /", "\u00e9\u65e5\U0001f600\u2028", 'a"\\\n\t\x01']
            )
            n = rng.choice([0, 1, 5, 20, 62, 63, 64, 65, 100])
            return _string(rng, "".join(rng.choice(letters) for _ in range(n)))
        return rng.choice(["true", "false", "null"])
    if r < 0.8:
        keys = [
            rng.choice(_KEYS) + rng.choice(["", "1", "2"])
            for _ in range(rng.randrange(7))
        ]
        if rng.random() > 0.03:
            keys = list(dict.fromkeys(keys))
        members = (ws() + _string(rng, key) + ws() + ":" + ws() + _value(rng, depth + 1) for key in keys)  # fmt: skip
        return "{" + ws() + ",".join(members) + ws() + "}"
    items = (ws() + _value(rng, depth + 1) + ws() for _ in range(rng.randrange(6)))
    return "[" + ",".join(items) + "]"


def _edges() -> list[str]:
    """The edges the 2026-10-27 probes found worth a row each."""
    rows = []
    for n in (127, 128, 129, 130):
        rows += ["[" * n + "1" + "]" * n, '{"k":' * n + "1" + "}" * n]
    for digits in (18, 19, 38, 39, 4300, 4301):
        rows += ["9" * digits, "-1" + "0" * (digits - 1)]
    rows += ["-0", "-0.0", "1e-310", "5e-324", '{"a":1,"A":2,"":3}', '{"a":1,"\\u0061":2}']  # fmt: skip
    for n in (63, 64):
        rows += [json.dumps("x" * n), json.dumps({"x" * n: "\u00e9" * (n // 2)})]
    for n in (255, 256):
        rows += ["{" + ",".join(f'"k{i}":{i}' for i in range(n)) + "}", json.dumps([0] * n)]  # fmt: skip
        rows.append(json.dumps({"k" * (n - 1): 1}))
    for total in (255, 256, 65_535, 65_536):
        rows += [json.dumps(["y" * (total - 5)]), json.dumps({"a": "y" * (total - 5)})]
    return rows


def _fit(rng: random.Random, decl: dict, depth: int = 0) -> str:
    """JSON text shaped like ``decl``: declared fields mostly there among
    other keys, values of the declared type mostly, of another now and then."""
    kind = decl["type"]
    if kind == "variant" or depth > 5 or rng.random() < 0.15:
        return _value(rng, depth)
    if kind == "object":
        members = {f["name"]: _fit(rng, f, depth + 1) for f in decl["fields"] if rng.random() < 0.8}  # fmt: skip
        for _ in range(rng.randrange(4)):
            members.setdefault(rng.choice(_KEYS) + rng.choice(["", "1"]), _value(rng, depth + 1))  # fmt: skip
        items = list(members.items())
        rng.shuffle(items)
        return "{" + ",".join(_string(rng, k) + ":" + v for k, v in items) + "}"
    if kind == "array":
        return "[" + ",".join(_fit(rng, decl["element"], depth + 1) for _ in range(rng.randrange(5))) + "]"  # fmt: skip
    if kind == "boolean":
        return rng.choice(["true", "false"])
    if kind == "string":
        return _string(
            rng, "".join(rng.choices("ab\u00e9 /\U0001f600", k=rng.randrange(70)))
        )
    if rng.random() < 0.5:
        return _number(rng)
    return (
        repr(rng.uniform(-1e6, 1e6))
        if kind == "double"
        else str(rng.randrange(-300, 300))
    )


def _rows(rng: random.Random, n: int, decl: dict | None = None) -> list[bytes]:
    rows = []
    for _ in range(n):
        text = _fit(rng, decl) if decl and rng.random() < 0.6 else _value(rng, 0)
        text = text.encode("utf-8", "surrogatepass")
        escapes = [i for i in range(len(text) - 1) if text[i : i + 2] == b"\\u"]
        if escapes and rng.random() < 0.03:
            # Cut inside or just after a \u escape: CPython's rule there
            # changed in 3.14 and 3.13.14 or so.
            text = text[: rng.choice(escapes) + rng.randrange(2, 8)]
        elif rng.random() < 0.3:
            at, kind = rng.randrange(len(text) + 1), rng.random()
            bad = rng.choice(_BAD)
            text = (text[:at] if kind < 0.25 else text[:at] + bad + text[at:] if kind < 0.6
                    else text[:at] + text[at + 1:] if kind < 0.8 else text[:at] + bad + text[at + 1:])  # fmt: skip
        rows.append(text)
    return rows


def test_randomized_differential():
    # Batches of rows under each declaration in turn: shredded ones get rows
    # shaped like them as well.
    rng = random.Random(20261102)
    for decl in RANDOMIZED:
        assert_same(_edges(), decl)
        assert_same_faults([text.encode() for text in _edges()], decl)
    invalid = seen = batch = 0
    while seen < ROWS:
        decl = RANDOMIZED[batch % len(RANDOMIZED)]
        rows = _rows(rng, min(5000, ROWS - seen), decl)
        report = assert_same(pa.array(rows, pa.binary()), decl)
        assert report.invalid_rows == assert_same_faults(rows, decl)
        invalid += report.invalid_rows
        seen, batch = seen + len(rows), batch + 1
    for decl in RANDOMIZED:
        assert_same(_rows(rng, 1000, decl), decl, chunk_rows=7, chunk_bytes=900)
    assert seen and 0.2 * seen < invalid < 0.6 * seen


@pytest.mark.parametrize("limit", [0, 640, 5000])
def test_the_int_digit_limit_is_read_at_call_time(limit):
    before = sys.get_int_max_str_digits()
    sys.set_int_max_str_digits(limit)
    try:
        rows = ["9" * n for n in (639, 640, 641, 4300, 4301, 5000, 5001, 7000)]
        assert_same(rows)
        assert assert_same_faults([row.encode() for row in rows]) == len(rows)
    finally:
        sys.set_int_max_str_digits(before)
