"""Deterministic value fabrication for generated schemas.

Vectorized where it matters (numpy draws, arrow casts); recursive for
containers. Bounds-stressing is built in:

- integer columns plant their type extremes in the first two rows, so
  footer bounds hit the domain edges (uint64 plants ``2**64 - 1`` —
  beyond int64, the decimal(20,0)-bounds path);
- float/double plant finite extremes and one NaN (the nan_count path);
- strings mix short tokens, a multi-KB value, and long-shared-prefix
  pairs that differ only past 16 bytes (bound-truncation territory);
- every nullable column with a positive rate is guaranteed at least one
  null (row 2), and rows 0-1 stay non-null so the planted extremes
  survive into the footer.

All draws come from the caller's Generator in a fixed order: same seed,
same schema, same table.
"""

from __future__ import annotations

import numpy as np
import pyarrow as pa

from .columns import ColSpec, to_arrow_schema

#: Long-shared-prefix marker for bound-stress strings (tests key on it).
BOUND_PREFIX = "bound-stress-prefix-0123456789abcdef-"

#: Null rate applied to nullable NESTED children (element/value/field).
CHILD_NULL_RATE = 0.15

_EPOCH_2000_S = 946_684_800
_EPOCH_2100_S = 4_102_444_800
_MICROS = 1_000_000

_JSON_DOCS = (
    '{"plan":"free","active":true}',
    '{"count":3,"tags":["a","b"]}',
    '{"nested":{"a":{"b":{"c":[1,2,3]}}},"f":0.5}',
    '{"empty":{}}',
    "[]",
    '{"text":"' + "x" * 512 + '"}',
    "null",
    '{"unicode":"\\u00fc\\u00f1\\u00ef"}',
)


_NP_INTS = {
    "int8": np.int8,
    "int16": np.int16,
    "int32": np.int32,
    "int64": np.int64,
    "uint8": np.uint8,
    "uint16": np.uint16,
    "uint32": np.uint32,
    "uint64": np.uint64,
}


def _null_mask(rng: np.random.Generator, n: int, null_rate: float) -> np.ndarray | None:
    """True = null. Rows 0-1 stay non-null (planted extremes must reach
    the footer); a positive rate guarantees at least one null (row 2)."""
    if null_rate <= 0 or n == 0:
        return None
    mask = rng.random(n) < null_rate
    mask[: min(2, n)] = False
    if n > 2:
        mask[2] = True
    return mask


def _plant(values: np.ndarray, extremes: tuple) -> np.ndarray:
    for i, v in enumerate(extremes[: len(values)]):
        values[i] = v
    return values


def _strings(rng: np.random.Generator, n: int) -> pa.Array:
    pool = [
        "alpha",
        "beta-2",
        "",
        "üñïçødé-véæłüê",
        "with space and, punctuation!",
        "z" * 64,
        "long-" + "ab" * 2048,  # multi-KB value
        BOUND_PREFIX + "aaaaaaaaaaaaaaaa",
        BOUND_PREFIX + "zzzzzzzzzzzzzzzz",
    ]
    idx = rng.integers(0, len(pool), n)
    idx[: min(2, n)] = [len(pool) - 2, len(pool) - 1][: min(2, n)]
    return pa.array([pool[i] for i in idx], pa.string())


def _scalar_array(rng: np.random.Generator, dtype: pa.DataType, n: int) -> pa.Array:
    if pa.types.is_boolean(dtype):
        return pa.array(rng.random(n) < 0.5)
    if pa.types.is_unsigned_integer(dtype):
        np_dtype = _NP_INTS[str(dtype)]
        info = np.iinfo(np_dtype)
        vals = rng.integers(0, int(info.max) + 1, n, dtype=np_dtype)
        return pa.array(_plant(vals, (info.min, info.max)), dtype)
    if pa.types.is_integer(dtype):
        np_dtype = _NP_INTS[str(dtype)]
        info = np.iinfo(np_dtype)
        vals = rng.integers(info.min, int(info.max) + 1, n, dtype=np_dtype)
        return pa.array(_plant(vals, (info.min, info.max)), dtype)
    if pa.types.is_floating(dtype):
        np_dtype = np.float32 if pa.types.is_float32(dtype) else np.float64
        info = np.finfo(np_dtype)
        vals = (rng.random(n) * 2e4 - 1e4).astype(np_dtype)
        vals = _plant(vals, (info.min, info.max))
        if n > 3:
            vals[3] = np.nan  # the nan_count path
        return pa.array(vals, dtype)
    if pa.types.is_decimal(dtype):
        # floats rounded to the scale, kept well inside the precision
        vals = np.round(rng.random(n) * 1e6 - 5e5, 2)
        return pa.array(vals).cast(dtype)
    if pa.types.is_date32(dtype):
        days = rng.integers(0, 40_000, n, dtype=np.int32)  # 1970..2079
        return pa.array(_plant(days, (0, 39_999)), pa.int32()).cast(dtype)
    if pa.types.is_time64(dtype):
        micros = rng.integers(0, 86_400 * _MICROS, n, dtype=np.int64)
        return pa.array(_plant(micros, (0, 86_400 * _MICROS - 1)), pa.int64()).cast(
            dtype
        )
    if pa.types.is_timestamp(dtype):
        scale = {"s": 1, "ms": 1_000, "us": _MICROS, "ns": 1_000_000_000}[dtype.unit]
        lo, hi = _EPOCH_2000_S * scale, _EPOCH_2100_S * scale
        vals = rng.integers(lo, hi, n, dtype=np.int64)
        return pa.array(_plant(vals, (lo, hi - 1)), pa.int64()).cast(dtype)
    if pa.types.is_fixed_size_binary(dtype):  # uuid: 16 random bytes
        width = dtype.byte_width
        raw = rng.bytes(n * width)
        return pa.array([raw[i * width : (i + 1) * width] for i in range(n)], dtype)
    if pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype):
        lengths = rng.integers(0, 48, n)
        raw = rng.bytes(int(lengths.sum()))
        out, pos = [], 0
        for ln in lengths:
            out.append(raw[pos : pos + int(ln)])
            pos += int(ln)
        return pa.array(out, dtype)
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return _strings(rng, n)
    # json extension (storage utf8): valid documents, annotation kept
    storage = getattr(dtype, "storage_type", None)
    if storage is not None and pa.types.is_string(storage):
        idx = rng.integers(0, len(_JSON_DOCS), n)
        docs = pa.array([_JSON_DOCS[i] for i in idx], pa.string())
        return docs.cast(dtype)
    raise NotImplementedError(f"no fabricator for arrow type {dtype!r}")


def _offsets(
    rng: np.random.Generator,
    n: int,
    max_len: int,
    null_rate: float,
) -> tuple[pa.Array, int]:
    """List/map offsets with nulls encoded as None entries."""
    lengths = rng.integers(0, max_len + 1, n)
    mask = _null_mask(rng, n, null_rate)
    offsets: list[int | None] = [0]
    total = 0
    for i in range(n):
        total += int(lengths[i])
        offsets.append(total)
    if mask is not None:
        # A null at offset position i marks LIST i as null; the final
        # offset (position n) must stay non-null.
        offsets = [
            None if (i < n and mask[i]) else off for i, off in enumerate(offsets)
        ]
    return pa.array(offsets, pa.int32()), total


def make_array(
    rng: np.random.Generator,
    dtype: pa.DataType,
    n: int,
    null_rate: float = 0.0,
) -> pa.Array:
    """One column's values for any generated type, recursively."""
    if pa.types.is_map(dtype):
        offsets, total = _offsets(rng, n, 3, null_rate)
        keys = _scalar_array(rng, dtype.key_type, total)  # keys: non-null
        items = make_array(rng, dtype.item_type, total, CHILD_NULL_RATE)
        return pa.MapArray.from_arrays(offsets, keys, items).cast(dtype)
    if pa.types.is_list(dtype):
        offsets, total = _offsets(rng, n, 4, null_rate)
        values = make_array(rng, dtype.value_type, total, CHILD_NULL_RATE)
        return pa.ListArray.from_arrays(offsets, values).cast(dtype)
    if pa.types.is_struct(dtype):
        children = [
            make_array(
                rng,
                dtype.field(i).type,
                n,
                CHILD_NULL_RATE if dtype.field(i).nullable else 0.0,
            )
            for i in range(dtype.num_fields)
        ]
        mask = _null_mask(rng, n, null_rate)
        return pa.StructArray.from_arrays(
            children,
            fields=[dtype.field(i) for i in range(dtype.num_fields)],
            mask=pa.array(mask) if mask is not None else None,
        )
    base = _scalar_array(rng, dtype, n)
    mask = _null_mask(rng, n, null_rate)
    if mask is None:
        return base
    # take() with a masked index array nulls the masked slots — no
    # python round trip (to_pylist chokes on e.g. extreme timestamp_ns)
    indices = pa.array(np.arange(n, dtype=np.int64), mask=mask)
    return base.take(indices)


def make_table(
    rng: np.random.Generator,
    specs: list[ColSpec] | tuple[ColSpec, ...],
    n: int,
) -> pa.Table:
    """A table of ``n`` rows for ``specs``, deterministic in ``rng``."""
    schema = to_arrow_schema(specs)
    arrays = [
        make_array(rng, s.dtype, n, s.null_rate if s.nullable else 0.0) for s in specs
    ]
    return pa.Table.from_arrays(arrays, schema=schema)
