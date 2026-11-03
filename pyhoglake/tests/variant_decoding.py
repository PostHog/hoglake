"""The two VARIANT decoders held to each other: the Rust codec's reading of
storage against the Python codec's (``reassemble``, ``structure_fault``).

Values compare as tagged JSON (design-native-encoder §3, R5): one tag per
Python type ``to_python`` returns, so a ``Decimal`` keeps its exponent, a
float its bits, a datetime its zone, a pyarrow scalar its type, and a dict
its key order. Rust writes the tags itself; :func:`tagged` writes Python's.
A fault compares by row, reason, path and message.
"""

from __future__ import annotations

import json
import struct
import uuid
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any

import pyarrow as pa

from pyhoglake import VariantEncodingError, _variant_backend
from pyhoglake import _variant_codec as codec
from pyhoglake.variant import VARIANT_NULL


def tagged(value: Any) -> Any:
    """A decoded Python value as Rust's decoder tags it: dates, times and
    timestamps by days or microseconds since the epoch, a Decimal by its
    unscaled integer and scale, bytes and uuids in hex."""
    kind = type(value)
    if value is None:
        return None
    if value is VARIANT_NULL:
        return ["variant_null"]
    if kind in (bool, int, str):
        return [kind.__name__, value]
    if kind is float:
        return ["float", struct.unpack("<Q", struct.pack("<d", value))[0]]
    if kind is Decimal:
        sign, digits, exponent = value.as_tuple()
        return ["decimal", (-1) ** sign * int("".join(map(str, digits))), -exponent]
    if kind is bytes or kind is uuid.UUID:
        return [kind.__name__.lower(), value.hex() if kind is bytes else value.hex]
    if kind is date:
        return ["date", (value - date(1970, 1, 1)).days]
    if kind is time:
        seconds = (value.hour * 60 + value.minute) * 60 + value.second
        return ["time", seconds * 10**6 + value.microsecond]
    if kind is datetime:
        since = value.replace(tzinfo=None) - datetime(1970, 1, 1)
        return [
            "datetime",
            since // timedelta(microseconds=1),
            value.tzinfo is not None,
        ]
    if kind is dict:
        return ["dict", [[k, tagged(v)] for k, v in value.items()]]
    if kind is list:
        return ["list", [tagged(v) for v in value]]
    if isinstance(value, pa.Scalar):
        return ["scalar", str(value.type), value.value]
    raise TypeError(f"to_python gave a {kind.__name__}")


def rust_reads(
    module: Any,
    chunks: list[pa.Array],
    *,
    strict: bool,
    canonical: bool = False,
    nullable: bool = True,
    column: str | None = None,
) -> tuple[list[Any], tuple[Any, ...] | None]:
    """Rust's rows (tags) up to its fault, and the fault as ``(row, reason,
    path, message)``."""
    rows, found = module.decode(
        list(map(_variant_backend.importable, chunks)),
        strict=strict,
        canonical=canonical,
        nullable=nullable,
        tagged=True,
    )
    values = [json.loads(row) for row in rows]
    if found is None:
        return values, None
    row, reason, path, detail = _variant_backend.decoded_fault(found, chunks[0].type)
    message = str(_variant_backend.error(column, row, reason, path, detail))
    return values, (row, reason, path, message)


def python_reads(
    reassemble: Any,
    chunks: list[pa.Array],
    *,
    strict: bool,
    canonical: bool = False,
    nullable: bool = True,
    column: str | None = None,
) -> tuple[list[Any], tuple[Any, ...] | None]:
    """The same of the Python codec: on a fault, the rows before it are
    read again on their own."""
    options = {"strict": strict, "canonical": canonical, "nullable": nullable}
    array = pa.chunked_array(chunks, chunks[0].type)
    try:
        values = reassemble(array, None, column=column, **options)
    except VariantEncodingError as exc:
        fault = (exc.row, exc.reason, exc.path, str(exc))
        if exc.row is None:  # the storage's type: no row reads
            return [], fault
        values = reassemble(array.slice(0, exc.row), None, column=column, **options)
        return [tagged(v) for v in values], fault
    return [tagged(v) for v in values], None


def assert_reads_alike(
    module: Any,
    array: pa.Array | pa.ChunkedArray,
    *,
    modes: tuple[tuple[bool, bool], ...] = (
        (False, False),
        (True, False),
        (True, True),
    ),
    nullable: bool = True,
    reassemble: Any = None,
) -> list[tuple[Any, ...] | None]:
    """Both decoders read ``array`` alike in each ``(strict, canonical)``
    mode (to_python's, verify's and verify's default); the faults."""
    chunks = codec.storage_chunks(array, None, None)
    faults = []
    for strict, canonical in modes:
        options = {"strict": strict, "canonical": canonical, "nullable": nullable}
        mine = rust_reads(module, chunks, column="c", **options)
        # The Python codec's own, not conftest's, which reads with both.
        python = reassemble or getattr(
            codec.reassemble, "__wrapped__", codec.reassemble
        )
        theirs = python_reads(python, chunks, column="c", **options)
        assert mine == theirs, (strict, canonical)
        faults.append(mine[1])
    return faults


def agreeing(module: Any, reassemble: Any) -> Any:
    """``reassemble``, checking that Rust reads the storage alike: the same
    values, or the same fault."""

    def both(
        array: pa.Array | pa.ChunkedArray,
        plan: codec.Plan | None,
        *,
        strict: bool,
        canonical: bool = False,
        column: str | None = None,
        nullable: bool = True,
    ) -> list[object]:
        options = {"strict": strict, "canonical": canonical, "nullable": nullable}
        try:
            values, error = reassemble(array, plan, column=column, **options), None
        except VariantEncodingError as exc:
            values, error = [], exc
        try:
            chunks = codec.storage_chunks(array, plan, column)
        except VariantEncodingError:
            chunks = []  # the declaration's form, which Python checks
        if chunks:
            rows, fault = rust_reads(module, chunks, column=column, **options)
            if error is None:
                assert (fault, rows) == (None, [tagged(v) for v in values])
            else:
                assert fault == (error.row, error.reason, error.path, str(error))
        if error is not None:
            raise error
        return values

    both.__wrapped__ = reassemble  # type: ignore[attr-defined]
    return both


def agreeing_structure(module: Any, structure_fault: Any) -> Any:
    def both(array: pa.StructArray) -> str | None:
        found = structure_fault(array)
        parts = module.structure_fault(_variant_backend.importable(array))
        assert found == (parts and _variant_backend._detail(parts, array.type))
        return found

    return both
