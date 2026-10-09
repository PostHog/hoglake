"""Shared by the VARIANT conformance suites.

``same`` is the ground-truth comparison of the round-trip checks: a value
read back is compared with the SOURCE value, never with a second encoding,
so an encoder and decoder that agree on a mistake cannot pass. Integers are
exact (one beyond int64 reads back as a Decimal of scale 0, the decimal16
D1 writes), doubles bit-equal, Decimals exact with their scale, and object
key order is ignored.
"""

from __future__ import annotations

import struct
import uuid
from datetime import UTC, date, datetime, time
from decimal import Decimal

import pyarrow as pa

from pyhoglake import variant
from pyhoglake.variant import VARIANT_NULL

INT64_MIN, INT64_MAX = -(2**63), 2**63 - 1


def row(value: object, shredding=None) -> dict:
    """The storage struct of one Python value, as a dict."""
    return variant.encode_python([value], shredding=shredding).array.to_pylist()[0]


def json_row(text: str, shredding=None) -> dict:
    return variant.encode_json([text], shredding=shredding).array.to_pylist()[0]


def value_bytes(value: object) -> tuple[bytes, bytes]:
    """``(metadata, value)`` of one Python value, unshredded."""
    stored = row(value)
    return stored["metadata"], stored["value"]


def json_bytes(text: str) -> tuple[bytes, bytes]:
    stored = json_row(text)
    return stored["metadata"], stored["value"]


def unshredded(metadata: bytes, value: bytes) -> pa.StructArray:
    """Hand-made unshredded storage of one row."""
    return pa.StructArray.from_arrays(
        [pa.array([metadata], pa.binary()), pa.array([value], pa.binary())],
        fields=list(variant.storage_type()),
    )


def same(got: object, want: object, *, top: bool = True) -> bool:
    """Whether ``got``, read back, is exactly the source value ``want``."""
    if want is VARIANT_NULL or (want is None and not top):
        return got is (VARIANT_NULL if top else None)
    if want is None:
        return got is None
    if isinstance(want, bool):
        return type(got) is bool and got is want
    if isinstance(want, int):
        if INT64_MIN <= want <= INT64_MAX:
            return type(got) is int and got == want
        return type(got) is Decimal and got.as_tuple().exponent == 0 and got == want
    if isinstance(want, float):
        return type(got) is float and struct.pack("<d", got) == struct.pack("<d", want)
    if isinstance(want, Decimal):
        exponent = want.as_tuple().exponent
        scale = -exponent if exponent < 0 else 0
        return (
            type(got) is Decimal and got == want and got.as_tuple().exponent == -scale
        )
    if isinstance(want, str):
        return type(got) is str and got == want
    if isinstance(want, (bytes, bytearray, memoryview)):
        return type(got) is bytes and got == bytes(want)
    if isinstance(want, datetime):
        if want.utcoffset() is None:
            return type(got) is datetime and got.tzinfo is None and got == want
        return (
            type(got) is datetime
            and got.utcoffset() == UTC.utcoffset(None)
            and got == want
        )
    if isinstance(want, date):
        return type(got) is date and got == want
    if isinstance(want, time):
        return type(got) is time and got == want
    if isinstance(want, uuid.UUID):
        return type(got) is uuid.UUID and got == want
    if isinstance(want, dict):
        return (
            type(got) is dict
            and set(got) == set(want)
            and all(same(got[k], want[k], top=False) for k in want)
        )
    if isinstance(want, (list, tuple)):
        return (
            type(got) is list
            and len(got) == len(want)
            and all(same(g, w, top=False) for g, w in zip(got, want, strict=True))
        )
    raise TypeError(f"no comparison for {type(want).__name__}")


def ident(value: object) -> str:
    """A short test id; repr of an int of thousands of digits raises."""
    try:
        text = repr(value)
    except ValueError:
        return f"<{type(value).__name__}>"
    return text if len(text) <= 40 else text[:37] + "..."
