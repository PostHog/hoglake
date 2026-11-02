"""Writes the server's VARIANT compaction fixtures, pyhoglake-written files
the rewriter copies through (server/src/test/kotlin/.../VariantPassthroughTest.kt):

    cd pyhoglake && flox activate -- uv run python tests/data/variant/regen_passthrough_fixtures.py

into server/src/test/resources/variant/passthrough/:

- columns.json: the live table, as the catalog's wire spells its columns.
- a.parquet, b.parquet: the declared layout, rows in no id order (the
  sorted rewrite has work to do), ``a`` in row groups of five.
- mismatch.parquet: ``props`` laid out from ANOTHER declaration, under the
  same field id: what a foreign writer, or the declaration of a dropped and
  re-added column, can leave in a table.
- duckdb.parquet: ``b`` with its footer's ``created_by`` overwritten, in
  place and at the same length, to DuckDB's.

Regenerating changes the bytes the server's tests compare row by row only
through what pyhoglake writes, so the tests need no change with it.
"""

from __future__ import annotations

import datetime as dt
import json
import uuid
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from pyhoglake import variant
from pyhoglake.models import Column
from pyhoglake.parquet_schema import write_variant_parquet
from pyhoglake.types import columns_to_arrow_schema

OUT = Path(__file__).parents[4] / "server/src/test/resources/variant/passthrough"

#: A subset of millrace's properties_sv declaration: every field a string.
PROPS = {
    "type": "object",
    "fields": [
        {"name": n, "type": "string"}
        for n in (
            "$session_id",
            "$window_id",
            "$group_0",
            "$current_url",
            "$browser",
            "$os",
            "$lib",
        )
    ],
}
PRIMITIVES = [
    *("boolean", "int8", "int16", "int32", "int64", "float", "double", "date", "time"),
    *(
        "timestamp",
        "timestamp_ns",
        "timestamptz",
        "timestamptz_ns",
        "binary",
        "string",
        "uuid",
    ),
]
#: Every leaf type, decimals of each width, an array of objects, a nested
#: object and an untyped field.
RICH = {
    "type": "object",
    "fields": [
        *({"name": t, "type": t} for t in PRIMITIVES),
        {"name": "d4", "type": "decimal4", "precision": 9, "scale": 3},
        {"name": "d8", "type": "decimal8", "precision": 18, "scale": 2},
        {"name": "d16", "type": "decimal16", "precision": 38, "scale": 4},
        {
            "name": "objs",
            "type": "array",
            "element": {"type": "object", "fields": [{"name": "k", "type": "string"}]},
        },
        {
            "name": "nested",
            "type": "object",
            "fields": [
                {"name": "a", "type": "int64"},
                {"name": "any", "type": "variant"},
            ],
        },
    ],
}
COLUMNS = [
    {"name": "id", "type": "long", "field_id": 1, "ordinal": 0, "nullable": False},
    {
        "name": "props",
        "type": "variant",
        "field_id": 2,
        "ordinal": 1,
        "type_params": {"shredding": PROPS},
    },
    {
        "name": "rich",
        "type": "variant",
        "field_id": 3,
        "ordinal": 2,
        "type_params": {"shredding": RICH},
    },
    {"name": "raw", "type": "variant", "field_id": 4, "ordinal": 3, "nullable": False},
]

PROPS_ROWS = [
    '{"$session_id": "s1", "$browser": "Chrome", "$os": "Mac", "$lib": "web"}',
    '{"$browser": "Safari"}',  # every other declared key missing
    '{"$browser": 5, "$os": {"v": 1}, "$lib": ["x"]}',  # type misses: value fallback
    '{"$current_url": "https://ex.am/ple?q=1", "extra": [1, null], "zz": null}',
    "{}",
    "null",  # a Variant null
    None,  # SQL NULL
    '"just text"',  # not objects
    "42",
    '[1, {"$browser": "no"}]',
    '{"$browser": "Fírefox ☃", "kéy": "v\\ud83d\\ude00"}',
    '{"$group_0": null, "$window_id": "w"}',
]
UTC = dt.UTC
RICH_ROWS = [
    {
        "boolean": True,
        "int8": -5,
        "int16": 300,
        "int32": 70000,
        "int64": 2**40,
        "float": 1.5,
        "double": 2.25,
        "date": dt.date(2026, 1, 2),
        "time": dt.time(1, 2, 3, 4),
        "timestamp": dt.datetime(2026, 1, 2, 3, 4, 5, 6),
        "timestamptz": dt.datetime(2026, 1, 2, 3, 4, 5, 6, tzinfo=UTC),
        "binary": b"\x00\xff",
        "string": "s",
        "uuid": uuid.UUID(int=7),
        "d4": Decimal("1.234"),
        "d8": Decimal("12.34"),
        "d16": Decimal("12345678901234567890.1234"),
        "objs": [{"k": "v"}, {"k": 1}, {"j": 2}],
        "nested": {"a": 1, "any": {"deep": [1, 2]}},
    },
    {"int8": 1000, "boolean": "no", "d4": Decimal("1.2345"), "f": float("nan")},
    {"uuid": "not-a-uuid", "objs": [], "nested": {}},
    None,
    [Decimal("1.5"), dt.date(1970, 1, 1), None],
    {"float": float("-inf"), "d16": Decimal("-0.0001"), "nested": {"a": "x"}},
]


def _table(columns: list[Column], ids: list[int]) -> pa.Table:
    n = len(ids)
    props = [PROPS_ROWS[i % len(PROPS_ROWS)] for i in ids]
    rich = [RICH_ROWS[i % len(RICH_ROWS)] for i in ids]
    raw = [json.dumps({"r": i, "s": [i]}) for i in ids]
    arrays = [
        pa.array(ids, pa.int64()),
        variant.encode_json(pa.array(props, pa.string()), columns[1]).array,
        variant.encode_python(rich, columns[2]).array,
        variant.encode_json(pa.array(raw, pa.string()), columns[3]).array,
    ]
    assert all(len(a) == n for a in arrays)
    return pa.table(arrays, schema=columns_to_arrow_schema(columns, variant="storage"))


def _write(name: str, columns: list[Column], ids: list[int], **kw: object) -> Path:
    path = OUT / name
    path.write_bytes(
        write_variant_parquet(_table(columns, ids), columns, **kw).body.to_pybytes()
    )
    return path


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    columns = [Column.from_wire(c) for c in COLUMNS]
    (OUT / "columns.json").write_text(
        json.dumps(COLUMNS, indent=1, ensure_ascii=False) + "\n"
    )
    # Even ids in a, odd in b, each in no order: the sorted merge interleaves them.
    _write(
        "a.parquet",
        columns,
        [(7 * i) % 24 for i in range(24) if (7 * i) % 24 % 2 == 0],
        row_group_size=5,
    )
    b = _write(
        "b.parquet", columns, [(5 * i) % 24 for i in range(24) if (5 * i) % 24 % 2 == 1]
    )
    other = dict(
        COLUMNS[1],
        type_params={
            "shredding": {
                "type": "object",
                "fields": [
                    {"name": "$browser", "type": "int64"},
                    {"name": "$os", "type": "string"},
                ],
            }
        },
    )
    _write(
        "mismatch.parquet",
        [columns[0], Column.from_wire(other), *columns[2:]],
        [30, 31, 32],
    )
    body = b.read_bytes()
    stamp = pq.ParquetFile(b).metadata.created_by.encode()
    duck = b"DuckDB version v1.5.6 (build x)".ljust(len(stamp), b" ")[: len(stamp)]
    assert body.count(stamp) == 1
    (OUT / "duckdb.parquet").write_bytes(body.replace(stamp, duck))
    assert pq.ParquetFile(OUT / "duckdb.parquet").metadata.created_by.startswith(
        "DuckDB"
    )
    print("wrote", sorted(p.name for p in OUT.iterdir()))


if __name__ == "__main__":
    main()
