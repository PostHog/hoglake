"""Which codec encodes VARIANT values: the Rust package
pyhoglake-variant-codec, or the Python one, its reference while both exist.
Private, and goes with the Python codec.

``PYHOGLAKE_VARIANT_CODEC``, read at import: ``auto`` (the default) uses
Rust when it imports with this PROTOCOL, whatever its own version, for what
it takes (JSON text, shredded or not, so far); ``native`` requires it, and
raises where it is missing or does not take the input or plan; ``python``
never uses it. A Rust result is checked first: an invalid row whose reason
or path Python does not give (``mismatch``), storage not of the plan's type
(``type``) or a panic (``panic``) re-encodes the call in Python rather than
raise, which would strand a millrace group in staging; each is counted in
``VariantReport.codec_fallbacks`` and logged. Where either parser stopped
at its recursion bound (CPython's moves with its stack), Python's verdict
on the row stands, counted apart in ``VariantReport.recursion_bound_rows``.

``PYHOGLAKE_VARIANT_SHADOW``, a fraction from 0 (the default) to 1 read
at import, encodes that share of the calls Rust took in Python too, in
``VariantReport.shadow_seconds``. Where the two differ, in counts or in any
slot of the storage (``shadow_mismatch``), Python's result is used.

``verify`` and ``structure_fault`` read storage through the same switch,
and until N6b removes the Python codec, Python reads it as well: where the
two verdicts differ either way (``mismatch``), or Rust panics (``panic``),
Python's stands, logged and counted in ``DECODE_FALLBACKS``, since a
read has no report. ``to_python`` stays Python until Rust builds its
objects (N5b).

Each process counts as well, for millrace: encode fallbacks, a call that
raises after one included (``ENCODE_FALLBACKS``); the calls and rows the
shadow compared (``SHADOWED``); and calls ``auto`` sent to Python, by op
and why (``ROUTED``).
"""

from __future__ import annotations

import logging
import os
import random
import sys
import threading
import time
from collections import Counter
from contextlib import suppress
from dataclasses import replace
from functools import cache, lru_cache, partial
from types import ModuleType
from typing import TYPE_CHECKING, Any, Literal

import pyarrow as pa

from . import _variant_codec as codec
from .errors import VariantEncodingError

if TYPE_CHECKING:
    from .variant import VariantReport

PROTOCOL = 3
ENV = "PYHOGLAKE_VARIANT_CODEC"
SHADOW_ENV = "PYHOGLAKE_VARIANT_SHADOW"
Backend = Literal["auto", "native", "python"]
_log = logging.getLogger(__name__)


def mode() -> Backend:
    chosen = os.environ.get(ENV) or "auto"
    if chosen not in ("auto", "native", "python"):
        raise ValueError(f"{ENV} is auto, native or python, not {chosen!r}")
    return chosen  # type: ignore[return-value]


def shadow() -> float:
    """The share of Rust's calls that Python encodes again to compare."""
    text = os.environ.get(SHADOW_ENV) or "0"
    with suppress(ValueError):
        if 0 <= (fraction := float(text)) <= 1:
            return fraction
    raise ValueError(f"{SHADOW_ENV} is a fraction from 0 to 1, not {text!r}")


#: Read once: a malformed value fails the import, not every encode, which
#: would strand a millrace group in staging.
MODE, SHADOW = mode(), shadow()


@cache
def native() -> tuple[ModuleType | None, str]:
    """The Rust module, or None and why it is not used."""
    try:
        import pyhoglake_variant_codec as module
    except ImportError as exc:
        return None, f"pyhoglake-variant-codec does not import ({exc})"
    if getattr(module, "PROTOCOL", None) != PROTOCOL:
        return None, f"pyhoglake-variant-codec does not speak protocol {PROTOCOL}"
    return module, ""


@lru_cache(maxsize=256)
def _plan(module: ModuleType, plan: codec.Plan) -> tuple[Any, tuple[str, ...], str]:
    """Rust's plan, from the write form, and the path of each node it counts
    (every node with a type, in the order Python's ``count`` visits them);
    or None and why it is not taken."""
    paths, nodes = [], [plan.root]
    while nodes:
        node = nodes.pop()
        if node is not None and node.kind != "variant":
            paths.append(node.path)
            nodes += [node.element, *reversed([child for _, child in node.fields])]
    try:
        return module.Plan(plan.write_type), tuple(paths), ""
    except ValueError as exc:
        return None, (), f"it does not take this plan ({exc})"


def encode(
    values: Any,
    plan: codec.Plan,
    report: VariantReport,
    *,
    json_text: bool,
    column: str | None,
    nullable: bool,
    on_invalid: Literal["raise", "null"],
    chunk_rows: int = codec.CHUNK_ROWS,
    chunk_bytes: int = codec.CHUNK_BYTES,
    backend: Backend | None = None,
) -> list[pa.StructArray]:
    """``codec.encode`` through ``backend``, or the switch's. JSON text
    comes as :func:`pyhoglake.variant.encode_json` takes it."""
    from .variant import VariantReport, _binary_chunks, _json_rows

    chosen = backend or MODE
    module, why = native() if chosen != "python" else (None, "")
    chunks, skipped = None, "unavailable"
    if module is not None:
        native_plan, paths, why = _plan(module, plan)
        skipped = "plan"
        if not json_text:
            why, skipped = "it encodes JSON text only, so far", "not_json"
        elif not why and isinstance(values, (pa.Array, pa.ChunkedArray)):
            chunks = _binary_chunks(values)
        elif not why:
            values = list(values)
            why, skipped = "a row is not UTF-8 text", "not_utf8"
            if all(type(v) in (str, bytes) or v is None for v in values):
                with suppress(UnicodeEncodeError):  # a str with a lone surrogate
                    chunks = [pa.array(values, pa.binary())]
    python = partial(
        codec.encode,
        plan=plan,
        json_text=json_text,
        column=column,
        nullable=nullable,
        on_invalid=on_invalid,
        chunk_rows=chunk_rows,
        chunk_bytes=chunk_bytes,
    )
    if module is None or chunks is None:
        if chosen == "native":
            raise RuntimeError(f"{ENV}=native, but {why}")
        if chosen == "auto":
            _count(ROUTED, ("encode", skipped))
        return python(_json_rows(values) if json_text else values, report=report)

    started = time.perf_counter()
    try:
        out, rows, sql_null_rows, invalid, counts = module.encode_json(
            chunks,
            native_plan,
            nullable=nullable,
            raise_on_invalid=on_invalid == "raise",
            chunk_rows=chunk_rows,
            chunk_bytes=chunk_bytes,
            max_int_digits=sys.get_int_max_str_digits(),
        )
    except module.PanicException:
        fault: tuple[str, int | None] | None = ("panic", None)
    else:
        fault, bounded = _check(out, invalid, chunks, plan)
    report.encode_seconds += time.perf_counter() - started
    if fault is not None:
        _fall_back(*fault, column, report)
        return python(_json_rows(values), report=report)
    mine = VariantReport(recursion_bound_rows=bounded)
    for row, reason, path, detail in invalid:
        if on_invalid == "raise" or reason == "sql_null_in_not_null":
            message = codec._message(column, row, path, detail)
            raise VariantEncodingError(
                message, column=column, row=row, path=path, reason=reason
            )
        mine.invalid_rows += 1
        mine.invalid_by_reason[reason] = mine.invalid_by_reason.get(reason, 0) + 1
        mine.first_invalid = mine.first_invalid or (column, row, reason, path)
    mine.rows, mine.sql_null_rows = rows, sql_null_rows
    for path, (typed, fallback, variant_null) in zip(paths, counts, strict=True):
        mine.typed[column, path] = typed
        mine.fallback[column, path] = fallback
        mine.variant_null[column, path] = variant_null
    if random.random() < SHADOW:
        return _shadow(out, mine, partial(python, _json_rows(values)), column, report)
    report.merge(mine)
    return out


def _shadow(
    out: list[pa.StructArray],
    mine: VariantReport,
    python: Any,
    column: str | None,
    report: VariantReport,
) -> list[pa.StructArray]:
    """Rust's result, or Python's where the two differ in counts, a raise
    or any slot of the storage (``Array.equals`` would take -0.0 for 0.0
    and skip the placeholders under null parents)."""
    from .variant import VariantReport, _storage_slots

    def counted(r: VariantReport) -> VariantReport:
        return replace(
            r, encode_seconds=0.0, codec_fallbacks={}, recursion_bound_rows=0
        )

    theirs = VariantReport()
    started = time.perf_counter()
    try:
        expected, error = python(report=theirs), None
    except VariantEncodingError as exc:
        expected, error = [], exc
    same = (
        error is None
        and counted(mine) == counted(theirs)
        and len(out) == len(expected)
        and all(
            _storage_slots(a) == _storage_slots(b)
            for a, b in zip(out, expected, strict=True)
        )
    )
    report.shadow_seconds += time.perf_counter() - started
    with _COUNTING:
        SHADOWED["calls"] += 1
        SHADOWED["rows"] += mine.rows
    if same:
        report.merge(mine)
        return out
    _fall_back("shadow_mismatch", None, column, report)
    if error is not None:
        raise error
    report.merge(replace(theirs, encode_seconds=0.0))
    return expected


def _fall_back(
    reason: str, row: int | None, column: str | None, report: VariantReport
) -> None:
    message = "VARIANT codec fallback to Python: column=%s row=%s reason=%s"
    _log.warning(message, column, row, reason)
    report.codec_fallbacks[reason] = report.codec_fallbacks.get(reason, 0) + 1
    _count(ENCODE_FALLBACKS, reason)


def _check(
    out: list[pa.StructArray],
    invalid: list[tuple[int, str, str | None, str]],
    chunks: list[pa.Array],
    plan: codec.Plan,
) -> tuple[tuple[str, int | None] | None, int]:
    """Why Rust's result is not to be used, and the row, or None; and how
    many invalid rows took Python's verdict (``invalid`` is updated)."""
    from .variant import VariantReport

    for chunk in out:
        if chunk.type != plan.write_type or structure_fault(chunk) is not None:
            return ("type", None), 0
    bounded = 0
    for i, (row, reason, path, _) in enumerate(invalid):
        if reason == "sql_null_in_not_null":
            continue
        at = row
        for chunk in chunks:  # the row, as the Python codec reads it
            if at < len(chunk):
                break
            at -= len(chunk)
        try:
            codec.encode(
                [chunk[at].as_py()],
                plan,
                VariantReport(),
                json_text=True,
                column=None,
                nullable=True,
                on_invalid="raise",
            )
        except VariantEncodingError as exc:
            if (exc.reason, exc.path) == (reason, path):
                continue
            if reason == "nesting_too_deep" and (
                path is None or (exc.reason, exc.path) == (reason, None)
            ):
                prefix = codec._message(None, 0, exc.path, "")
                invalid[i] = (row, exc.reason, exc.path, str(exc)[len(prefix) :])
                bounded += 1
                continue
        return ("mismatch", row), 0
    return None, bounded


def verify(
    array: pa.Array | pa.ChunkedArray,
    plan: codec.Plan | None,
    *,
    canonical: bool,
    column: str | None,
    nullable: bool,
    backend: Backend | None = None,
) -> None:
    """``codec.reassemble(strict=True)``'s verdict, through ``backend`` or
    the switch's; the GIL is released while Rust reads."""
    chosen = backend or MODE
    module, why = native() if chosen != "python" else (None, "")
    flags = {"strict": True, "canonical": canonical, "nullable": nullable}

    def python(storage: pa.Array | pa.ChunkedArray, plan: codec.Plan | None) -> None:
        codec.reassemble(storage, plan, column=column, **flags)

    if module is not None:
        chunks = list(map(importable, codec.storage_chunks(array, plan, column)))
        try:
            _, found = module.decode(chunks, tagged=False, **flags)
        except TypeError as exc:  # a type pyo3-arrow does not import
            why = f"it does not take this storage ({exc})"
        except module.PanicException:
            _decode_fall_back("panic", None, column)
            python(array, plan)
            return
        else:
            fault = found and error(column, *decoded_fault(found, chunks[0].type))
            # Until N6b, Python's verdict too, and it stands where they differ.
            try:
                python(array, plan)
            except VariantEncodingError as exc:
                theirs: VariantEncodingError | None = exc
            else:
                theirs = None
            spelt = [e and (e.row, e.reason, e.path, str(e)) for e in (fault, theirs)]
            if spelt[0] != spelt[1]:
                at = theirs.row if theirs is not None else fault.row
                _decode_fall_back("mismatch", at, column)
            if theirs is not None:
                raise theirs
            return
    if chosen == "native":
        raise RuntimeError(f"{ENV}=native, but {why}")
    if chosen == "auto":
        _count(ROUTED, ("decode", "unavailable" if module is None else "storage"))
    python(array, plan)


def structure_fault(array: pa.StructArray) -> str | None:
    """``codec.structure_fault``, from Rust where the switch uses it, and
    until N6b from Python too, whose verdict stands where they differ: a
    false fault would refuse every write of the storage (a millrace group
    sticks), and a missed one write a null pyarrow turns into other bytes."""
    module, why = native() if MODE != "python" else (None, "")
    if module is None and MODE == "native":
        raise RuntimeError(f"{ENV}=native, but {why}")
    if module is None:
        if MODE == "auto":
            _count(ROUTED, ("decode", "unavailable"))
        return codec.structure_fault(array)
    try:
        parts = module.structure_fault(importable(array))
    except module.PanicException:
        _decode_fall_back("panic", None, None)
        return codec.structure_fault(array)
    python = codec.structure_fault(array)
    if python != (parts and _detail(parts, array.type)):
        _decode_fall_back("mismatch", None, None)
    return python


#: Since import, under ``_COUNTING``, for millrace to export: encode and
#: decode fallbacks to Python by reason (``mismatch``, ``panic``; encode's
#: also ``type`` and ``shadow_mismatch``); calls ``auto`` sent to Python by
#: ``(op, why)``: ``encode`` for ``unavailable`` (no matching package),
#: ``plan``, ``not_utf8`` or, through the hook only, ``not_json``; ``decode``
#: for ``unavailable`` or ``storage``; and the shadow's ``calls`` and
#: ``rows`` compared.
ENCODE_FALLBACKS: Counter[str] = Counter()
DECODE_FALLBACKS: Counter[str] = Counter()
ROUTED: Counter[tuple[str, str]] = Counter()
SHADOWED: Counter[str] = Counter()
_COUNTING = threading.Lock()


def _count[K](counter: Counter[K], key: K) -> None:
    with _COUNTING:
        counter[key] += 1


def _decode_fall_back(reason: str, row: int | None, column: str | None) -> None:
    message = "VARIANT decode fallback to Python: column=%s row=%s reason=%s"
    _log.warning(message, column, row, reason)
    _count(DECODE_FALLBACKS, reason)


def importable(array: pa.Array) -> pa.Array:
    """``array``, or a copy without offsets where arrow-rs would not import
    it: it slices the struct children of a struct twice where an offset
    applies (arrow-rs 59 ``ArrayData::slice``), and panics there."""
    arrays = [array]
    while arrays:  # storage nests as deep as it likes: no recursion
        at = arrays.pop()
        if isinstance(at, pa.StructArray):
            arrays += [at.field(i) for i in range(at.type.num_fields)]
        elif isinstance(at, (pa.ListArray, pa.LargeListArray, pa.FixedSizeListArray)):
            arrays.append(at.values)
        if at.offset and isinstance(at, (pa.StructArray, pa.FixedSizeListArray)):
            return pa.concat_arrays([array])
    return array


def decoded_fault(
    found: tuple[Any, ...], kind: pa.DataType
) -> tuple[int | None, str, str | None, str]:
    """Rust's fault as ``(row, reason, path, detail)``, spelt by the
    Python codec's helpers: paths, quoted names and Arrow types."""
    row, reason, trail, parts = found
    located = codec._Invalid(reason, "")
    located.trail = trail or []
    return row, reason, None if trail is None else located.path(), _detail(parts, kind)


def _detail(parts: list[Any], kind: pa.DataType) -> str:
    out = []
    for part in parts:
        what, argument = ("text", part) if isinstance(part, str) else part
        if what in ("type", "raw_type"):  # the type at a child path of the storage
            at = kind
            for index in argument:
                at = at.field(index).type if pa.types.is_struct(at) else at.value_type
            argument = codec._type_text(at) if what == "type" else str(at)
        elif what == "quoted":
            argument = codec._quoted(argument)
        elif what == "column":
            argument = codec._column_path("".join(map(codec.member, argument)))
        out.append(argument)
    return "".join(out)


def error(
    column: str | None, row: int | None, reason: str, path: str | None, detail: str
) -> VariantEncodingError:
    """A fault as the Python codec raises it."""
    message = codec._message(column, row, path, detail)
    return VariantEncodingError(
        message, column=column, row=row, path=path, reason=reason
    )
