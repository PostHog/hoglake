"""The Rust VARIANT codec (pyhoglake-variant-codec) against the Python one,
its reference while both exist: the same chunks, storage byte for byte,
reports, reasons, paths and messages, for JSON text into an unshredded
plan, which is what it encodes so far.

Skipped where the package is not installed: `uv sync --group
variant-codec` builds it, and CI's variant-codec job checks it imports
before it runs these. PYHOGLAKE_VARIANT_DIFFERENTIAL_ROWS sets the volume
of the randomized differential (a PR runs at least 1M rows once).
"""

from __future__ import annotations

import json
import logging
import math
import os
import random
import sys
from pathlib import Path

import pyarrow as pa
import pytest
from test_parquet_testing import BYTE_EQUAL

from pyhoglake import VariantEncodingError, _variant_backend, variant
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
    native_report.codec_fallbacks.pop("recursion_bound", None)
    assert native_report == python_report
    assert [len(c) for c in native] == [len(c) for c in python]
    for mine, theirs in zip(native, python, strict=True):
        assert mine.type == theirs.type
        assert mine.is_valid().equals(theirs.is_valid())
        # Children whole, the placeholders under null rows too.
        assert mine.field(0).equals(theirs.field(0))
        assert mine.field(1).equals(theirs.field(1))
    return python_report


def assert_same_faults(rows: list[bytes]) -> int:
    """Each row Rust refuses, Python refuses with the same reason, path and
    message (the message from Rust's detail); returns how many."""
    _, _, _, invalid = NATIVE.encode_json(
        [pa.array(rows, pa.binary())],
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
                codec.UNSHREDDED_PLAN,
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


def test_the_switch(monkeypatch):
    monkeypatch.delenv(_variant_backend.ENV, raising=False)
    assert _variant_backend.mode() == "auto"
    monkeypatch.setenv(_variant_backend.ENV, "rust")
    with pytest.raises(ValueError, match="auto, native or python"):
        variant.encode_json(["1"])


def test_auto_takes_rust_for_json_into_an_unshredded_plan(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("the Python codec was used")

    monkeypatch.setattr(codec, "encode", refuse)
    for decl in UNSHREDDED:
        assert variant.encode_json(['{"a":1}'], shredding=decl).report.rows == 1
    with pytest.raises(AssertionError, match="Python codec"):
        variant.encode_json(['{"a":1}'], shredding={"type": "string"})


@pytest.mark.parametrize(
    ("values", "decl", "why"),
    [
        (['"x"'], {"type": "string"}, "unshredded plan only"),
        (['"\ud800"'], None, "not UTF-8"),
        ([b"1", 1], None, "not UTF-8"),
    ],
)
def test_native_refuses_what_it_does_not_take(monkeypatch, values, decl, why):
    monkeypatch.setenv(_variant_backend.ENV, "native")
    with pytest.raises(RuntimeError, match=why):
        variant.encode_json(values, shredding=decl)
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


def test_a_package_of_another_version_is_not_used(monkeypatch):
    versions = {"pyhoglake": "1.0.0", "pyhoglake-variant-codec": "1.0.1"}
    monkeypatch.setattr(_variant_backend.metadata, "version", versions.get)
    _variant_backend.native.cache_clear()
    try:
        module, why = _variant_backend.native()
        assert module is None
        assert why == "pyhoglake-variant-codec is 1.0.1, not pyhoglake's 1.0.0"
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

    def broken(chunks, **kwargs):
        if reason == "panic":
            raise NATIVE.PanicException("boom")
        out, rows, nulls, invalid = real(chunks, **kwargs)
        if reason == "type":
            return [pa.array([1])], rows, nulls, invalid
        return out, rows, nulls, [(1, "duplicate_key", None, "key 'a' ...")]

    monkeypatch.setattr(NATIVE, "encode_json", broken)
    rows = ["1", '{"a":[2]}', None]
    with caplog.at_level(logging.WARNING, logger=_variant_backend.__name__):
        encoded = variant.encode_json(rows, shredding={"type": "variant"})
    assert encoded.report.codec_fallbacks == {reason: 1}
    row = 1 if reason == "mismatch" else None
    assert caplog.messages == [
        f"VARIANT codec fallback to Python: column=None row={row} reason={reason}"
    ]
    monkeypatch.setattr(NATIVE, "encode_json", real)
    again = variant.encode_json(rows, shredding={"type": "variant"})
    assert encoded.array.equals(again.array)
    assert again.report.codec_fallbacks == {}


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


def test_arrow_text_of_every_kind_and_chunking():
    rows = ['{"a":1}', None, "[1,", '"\\ud800"', "2"]
    for kind in (pa.string(), pa.large_string(), pa.string_view(), pa.binary(),
                 pa.large_binary(), pa.binary_view(), pa.json_()):  # fmt: skip
        whole = pa.array(rows, kind)
        assert_same(whole)
        assert_same(pa.chunked_array([whole.slice(0, 2), whole.slice(2)]))
    assert_same(pa.array([None, None], pa.null()))
    assert_same(pa.array([b"\xff", b'"\xc3"', b"1"], pa.binary()))


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
def test_invalid_rows_are_refused_alike(text, reason, path, on_invalid):
    rows = ["1", text, "2"]
    if reason is None:
        assert assert_same(rows).invalid_rows == 0
        return
    if on_invalid == "null":
        report = assert_same(rows)
        assert report.first_invalid == ("c", 1, reason, path)
        return
    errors = []
    for backend in ("python", "native"):
        with pytest.raises(VariantEncodingError) as raised:
            _encode(rows, backend, on_invalid="raise")
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
    assert native_report.codec_fallbacks.pop("recursion_bound") >= 1
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


@pytest.mark.parametrize("on_invalid", ["raise", "null"])
def test_sql_null_into_not_null_raises_alike(on_invalid):
    for backend in ("python", "native"):
        with pytest.raises(VariantEncodingError, match="row 1: SQL NULL") as raised:
            _variant_backend.encode(
                ["1", None],
                codec.UNSHREDDED_PLAN,
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


def test_json_test_suite():
    # Every test_parsing file as one row, verdicts by Python's parser, not
    # the RFC's (Python takes NaN to its hook, refuses \f as whitespace).
    rows = [path.read_bytes() for path in sorted(JSON_TEST_SUITE.glob("*.json"))]
    report = assert_same(pa.array(rows, pa.binary()))
    assert assert_same_faults(rows) == report.invalid_rows
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


def _rows(rng: random.Random, n: int) -> list[bytes]:
    rows = []
    for _ in range(n):
        text = _value(rng, 0).encode("utf-8", "surrogatepass")
        if rng.random() < 0.3:
            at, kind = rng.randrange(len(text) + 1), rng.random()
            bad = rng.choice(_BAD)
            text = (text[:at] if kind < 0.25 else text[:at] + bad + text[at:] if kind < 0.6
                    else text[:at] + text[at + 1:] if kind < 0.8 else text[:at] + bad + text[at + 1:])  # fmt: skip
        rows.append(text)
    return rows


def test_randomized_differential():
    rng = random.Random(20261102)
    invalid = assert_same_faults([text.encode() for text in _edges()])
    seen = 0
    while seen < ROWS:
        rows = _rows(rng, min(5000, ROWS - seen))
        report = assert_same(pa.array(rows, pa.binary()))
        assert report.invalid_rows == assert_same_faults(rows)
        invalid += report.invalid_rows
        seen += len(rows)
    assert_same(_rows(rng, 2000), chunk_rows=7, chunk_bytes=900)
    assert seen and 0.2 * seen < invalid < 0.5 * seen


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
