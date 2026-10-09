"""The fixed corpus golden_storage.arrow pins (D19).

Declarations crossed with edge rows: every layout the encoder builds, and
every placement rule a row can exercise. tests/data/variant/
regen_golden_storage.py writes the encoder's output over it, and
test_golden_storage.py checks the encoder still reproduces it exactly, so a
change that moves one byte of output (a faster encoder, say) is seen.
Append to the corpus freely; regenerate, and say why in the commit.
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime, time, timedelta, timezone
from decimal import Decimal

from pyhoglake.variant import VARIANT_NULL

DECLARATIONS: dict[str, dict | None] = {
    "unshredded": None,
    "root_variant": {"type": "variant"},
    "int8": {"type": "int8"},
    "int64": {"type": "int64"},
    "double": {"type": "double"},
    "string": {"type": "string"},
    "decimal4": {"type": "decimal4", "precision": 9, "scale": 2},
    "decimal8": {"type": "decimal8", "precision": 18, "scale": 0},
    "decimal16": {"type": "decimal16", "precision": 38, "scale": 0},
    "strings": {"type": "array", "element": {"type": "string"}},
    "matrix": {
        "type": "array",
        "element": {"type": "array", "element": {"type": "double"}},
    },
    "posthog": {
        "type": "object",
        "fields": [
            {"name": "$browser", "type": "string"},
            {"name": "$screen_width", "type": "int64"},
            {"name": "$geoip_latitude", "type": "double"},
            {"name": "price", "type": "decimal8", "precision": 18, "scale": 2},
            {
                "name": "$active_feature_flags",
                "type": "array",
                "element": {"type": "string"},
            },
            {
                "name": "$set",
                "type": "object",
                "fields": [{"name": "email", "type": "string"}],
            },
            {"name": "payload", "type": "variant"},
        ],
    },
    "trino": {
        "type": "object",
        "fields": [
            {"name": "value", "type": "int32"},
            {"name": "typed_value", "type": "variant"},
            {"name": "metadata", "type": "boolean"},
            {"name": "a.b", "type": "string"},
            {
                "name": "a",
                "type": "object",
                "fields": [{"name": "b", "type": "string"}],
            },
        ],
    },
    "events": {
        "type": "array",
        "element": {
            "type": "object",
            "fields": [
                {"name": "type", "type": "string"},
                {"name": "ids", "type": "array", "element": {"type": "int16"}},
            ],
        },
    },
    "temporal": {
        "type": "object",
        "fields": [
            {"name": "d", "type": "date"},
            {"name": "t", "type": "time"},
            {"name": "ts", "type": "timestamp"},
            {"name": "tz", "type": "timestamptz"},
            {"name": "u", "type": "uuid"},
            {"name": "b", "type": "binary"},
            {"name": "w", "type": "decimal16", "precision": 20, "scale": 3},
            {"name": "f", "type": "float"},
            {"name": "ns", "type": "timestamptz_ns"},
        ],
    },
}

#: JSON rows. None is SQL NULL; the invalid ones are written as SQL NULL
#: (on_invalid="null"), which pins that too.
JSON_ROWS: list[str | None] = [
    None,
    "null",
    "true",
    "0",
    "-128",
    "128",
    "-32769",
    "2147483648",
    "9223372036854775807",
    "9223372036854775808",
    "-99999999999999999999999999999999999999",
    "-0.0",
    "1.5",
    "1e-400",
    '""',
    '"' + "x" * 63 + '"',
    '"' + "x" * 64 + '"',
    '"\\u00e9\\ud83d\\ude00"',
    "[]",
    "{}",
    '[1,null,"two",[3.5],{"k":[]}]',
    '[[1.5,2],[],null,[null,"x"],"no"]',
    '{"\\uffff":1,"\\ud83d\\ude00":2,"B":3,"a":4}',
    (
        '{"$browser":"Chrome","$Browser":"x","$screen_width":1920,"$geoip_latitude":51.5,'
        '"price":9.99,"$active_feature_flags":["a",null,1],"$set":{"email":"e@x","Email":1},'
        '"payload":{"deep":[{"z":null}]},"$lib":"web"}'
    ),
    '{"$browser":null,"$screen_width":"wide","$geoip_latitude":51,"$set":"none"}',
    '{"value":1,"typed_value":"t","metadata":true,"a.b":"dot","a":{"b":"nested","c":3}}',
    '[{"type":"click","ids":[1,-1,40000,null]},"x",null,{},{"TYPE":"case","ids":"no"}]',
    "[" + ",".join(f'{{"k{i:03d}":{i}}}' for i in range(300)) + "]",
    "{" + ",".join(f'"k{i:03d}":{i}' for i in range(256)) + "}",
    '{"a":1,"a":2}',
    '{"x":NaN}',
    '"\\ud800"',
    "1" + "0" * 38,
    "[" * 130 + "]" * 130,
    "{oops",
]

_EAST = timezone(timedelta(hours=9, minutes=30))

#: Python rows, under the declarations that type them.
PYTHON_ROWS: list[object] = [
    None,
    VARIANT_NULL,
    Decimal("12.34"),
    Decimal("-0.000001"),
    Decimal("12345678901234567890.123"),
    Decimal("1E+5"),
    2**63,
    float("nan"),
    float("-inf"),
    b"\x00\xff",
    date(1969, 12, 31),
    time(23, 59, 59, 999_999),
    datetime(1969, 12, 31, 23, 59, 59, 999_999),
    datetime(2025, 4, 16, 12, 34, 56, 780_000, tzinfo=_EAST),
    uuid.UUID("f24f9b64-81fa-49d1-b74e-8c09a6e31c56"),
    {
        "d": date(2024, 2, 29),
        "t": time(12, 0),
        "ts": datetime(2024, 1, 1),
        "tz": datetime(2024, 1, 1, tzinfo=UTC),
        "u": uuid.UUID(int=1),
        "b": b"",
        "w": Decimal("12345678901234567.890"),
        "f": 1.5,
        "ns": datetime(2024, 1, 1, tzinfo=UTC),
        "extra": (1, None, VARIANT_NULL),
    },
    {
        "d": "2024-02-29",
        "ts": datetime(2024, 1, 1, tzinfo=UTC),
        "w": Decimal("1.5"),
        "u": str(uuid.UUID(int=1)),
    },
    time(1, tzinfo=UTC),
    {1: "not a str key"},
]

#: The declarations the Python rows are encoded under.
PYTHON_DECLARATIONS = ["unshredded", "temporal", "decimal4", "decimal16", "double"]


def golden_table():
    """The encoder's output over the corpus, one column per declaration and
    input kind, named ``json/<name>`` and ``python/<name>``. The Python rows
    are padded with SQL NULLs to the length of the JSON rows, which a table
    needs."""
    import json

    import pyarrow as pa

    from pyhoglake import variant

    columns: dict[str, pa.Array] = {}
    for name, decl in DECLARATIONS.items():
        columns[f"json/{name}"] = variant.encode_json(
            JSON_ROWS, shredding=decl, on_invalid="null"
        ).array
    padded = PYTHON_ROWS + [None] * (len(JSON_ROWS) - len(PYTHON_ROWS))
    for name in PYTHON_DECLARATIONS:
        columns[f"python/{name}"] = variant.encode_python(
            padded, shredding=DECLARATIONS[name], on_invalid="null"
        ).array
    declarations = json.dumps(DECLARATIONS, sort_keys=True).encode()
    return pa.table(columns).replace_schema_metadata({"declarations": declarations})
