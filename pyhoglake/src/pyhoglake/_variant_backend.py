"""Which codec encodes VARIANT values: the Rust package
pyhoglake-variant-codec, or the Python one, its reference while both exist.
Private, and goes with the Python codec.

``PYHOGLAKE_VARIANT_CODEC``: ``auto`` (the default) uses Rust when it
imports with this PROTOCOL and pyhoglake's version, for what it takes (JSON
text into an unshredded plan, so far); ``native`` requires it, and raises
where it is missing or does not take the input or plan; ``python`` never
uses it. A Rust result is checked first: an invalid row whose reason or
path Python does not give (``mismatch``), storage not of the plan's type
(``type``) or a panic (``panic``) re-encodes the call in Python rather than
raise, which would strand a millrace group in staging; each is counted in
``VariantReport.codec_fallbacks`` and logged. Where either parser stopped
at its recursion bound (CPython's moves with its stack), Python's verdict
on the row stands, counted as ``recursion_bound``.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from contextlib import suppress
from functools import cache, partial
from importlib import metadata
from types import ModuleType
from typing import TYPE_CHECKING, Any, Literal

import pyarrow as pa

from . import _variant_codec as codec
from .errors import VariantEncodingError

if TYPE_CHECKING:
    from .variant import VariantReport

PROTOCOL = 1
ENV = "PYHOGLAKE_VARIANT_CODEC"
Backend = Literal["auto", "native", "python"]
_log = logging.getLogger(__name__)


def mode() -> Backend:
    chosen = os.environ.get(ENV) or "auto"
    if chosen not in ("auto", "native", "python"):
        raise ValueError(f"{ENV} is auto, native or python, not {chosen!r}")
    return chosen  # type: ignore[return-value]


@cache
def native() -> tuple[ModuleType | None, str]:
    """The Rust module, or None and why it is not used."""
    try:
        import pyhoglake_variant_codec as module
    except ImportError as exc:
        return None, f"pyhoglake-variant-codec does not import ({exc})"
    if getattr(module, "PROTOCOL", None) != PROTOCOL:
        return None, f"pyhoglake-variant-codec does not speak protocol {PROTOCOL}"
    mine, theirs = map(metadata.version, ("pyhoglake", "pyhoglake-variant-codec"))
    if mine != theirs:
        return None, f"pyhoglake-variant-codec is {theirs}, not pyhoglake's {mine}"
    return module, ""


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
    from .variant import _add_counts, _binary_chunks, _json_rows

    chosen = backend or mode()
    module, why = native() if chosen != "python" else (None, "")
    chunks = None
    if module is not None:
        if not json_text or plan.root is not None:
            why = "it encodes JSON text into an unshredded plan only, so far"
        elif isinstance(values, (pa.Array, pa.ChunkedArray)):
            chunks = _binary_chunks(values)
        else:
            values = list(values)
            why = "a row is not UTF-8 text"
            if all(type(v) in (str, bytes) or v is None for v in values):
                with suppress(UnicodeEncodeError):  # a str with a lone surrogate
                    chunks = [pa.array(values, pa.binary())]
    python = partial(
        codec.encode,
        plan=plan,
        report=report,
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
        return python(_json_rows(values) if json_text else values)

    started = time.perf_counter()
    try:
        out, rows, sql_null_rows, invalid = module.encode_json(
            chunks,
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
        return python(_json_rows(values))
    _add_counts(report.codec_fallbacks, {"recursion_bound": bounded} if bounded else {})
    for row, reason, path, detail in invalid:
        if on_invalid == "raise" or reason == "sql_null_in_not_null":
            message = codec._message(column, row, path, detail)
            raise VariantEncodingError(
                message, column=column, row=row, path=path, reason=reason
            )
        report.invalid_rows += 1
        report.invalid_by_reason[reason] = report.invalid_by_reason.get(reason, 0) + 1
        report.first_invalid = report.first_invalid or (column, row, reason, path)
    report.rows += rows
    report.sql_null_rows += sql_null_rows
    return out


def _fall_back(
    reason: str, row: int | None, column: str | None, report: VariantReport
) -> None:
    message = "VARIANT codec fallback to Python: column=%s row=%s reason=%s"
    _log.warning(message, column, row, reason)
    report.codec_fallbacks[reason] = report.codec_fallbacks.get(reason, 0) + 1


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
        if chunk.type != plan.write_type or codec.structure_fault(chunk) is not None:
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
