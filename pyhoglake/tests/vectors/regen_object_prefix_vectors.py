"""Writes object_prefix_vectors.json: the object-prefix layout cases the
server's ``ObjectPrefix.kt`` is held to (``ObjectPrefixVectorsTest``,
``ObjectPrefixTest``), and that a writer building those paths must match.

    cd pyhoglake && flox activate -- uv run python tests/vectors/regen_object_prefix_vectors.py

``test_object_prefix_vectors.py`` regenerates the file in memory and fails
when it differs, so a change to a case reds until the file moves with it.
"""

from __future__ import annotations

import json
import struct
from pathlib import Path
from typing import Any

OUT = Path(__file__).parent / "object_prefix_vectors.json"
DATA_PATH = "s3://hoglake-prod-us/lake"
ROOT = DATA_PATH + "/data/ingest/events/"
TAG = "${aws:PrincipalTag/hoglake-team-id}"


def _prefix(value: object) -> str:
    return ROOT + f"team_id={value}/"


def _prefix_case(
    col_type: str,
    value: str | None,
    expected: str | None = None,
    data_path: str = DATA_PATH,
) -> dict[str, Any]:
    case: dict[str, Any] = {
        "data_path": data_path,
        "namespace": "ingest",
        "table": "events",
        "field": "team_id",
        "col_type": col_type,
        "value": value,
    }
    if expected is None:
        case["error"] = "invalid_value"
    else:
        case["expected"] = expected
    return case


def _le4(value: int) -> str:
    return struct.pack("<i", value).hex()


def _le8(value: int) -> str:
    return struct.pack("<q", value).hex()


def _stats_case(
    col_type: str,
    value: int,
    lower: str | None,
    upper: str | None,
    null_count: int,
    verdict: str,
) -> dict[str, Any]:
    return {
        "col_type": col_type,
        "value": value,
        "verdict": verdict,
        "stats": {"lower_hex": lower, "upper_hex": upper, "null_count": null_count},
    }


def build() -> dict[str, Any]:
    prefix_cases = [
        _prefix_case("long", "4", _prefix(4)),
        _prefix_case("long", "4", _prefix(4), data_path=DATA_PATH + "/"),
        _prefix_case("long", "0", _prefix(0)),
        _prefix_case("long", "42", _prefix(42)),
        _prefix_case("long", "9223372036854775807", _prefix("9223372036854775807")),
        _prefix_case("long", "9223372036854775808"),
        *(
            _prefix_case("long", v)
            for v in ("042", "+4", "-4", "", "4 ", "4.0", "1e3", "٤٢", None)
        ),
        _prefix_case("int8", "127", _prefix(127)),
        _prefix_case("int8", "128"),
        _prefix_case("int16", "32767", _prefix(32767)),
        _prefix_case("int16", "32768"),
        _prefix_case("int", "2147483647", _prefix(2147483647)),
        _prefix_case("int", "2147483648"),
        _prefix_case("uint8", "255", _prefix(255)),
        _prefix_case("uint8", "256"),
        _prefix_case("uint16", "65535", _prefix(65535)),
        _prefix_case("uint16", "65536"),
        _prefix_case("uint32", "4294967295", _prefix(4294967295)),
        _prefix_case("uint32", "4294967296"),
        _prefix_case("uint64", "4"),
        _prefix_case("string", "4"),
    ]
    four = _prefix(4)
    containment = [
        (four + "a.parquet", True),
        (four + "sub/x.puffin", True),
        (_prefix(42) + "a.parquet", False),
        (ROOT + "team_id=4x/a.parquet", False),
        (four + "../team_id=42/a.parquet", False),
        (four + "/a.parquet", False),
        (four + "%2E%2E/a.parquet", False),
        (four + "%2f/a.parquet", False),
        (ROOT + "hoglake-dv-1.puffin", False),
        (four, False),
        (four + "a b.parquet", False),
        (four + "a\\b.parquet", False),
        (four + "./a.parquet", False),
        (four + "a\u0000.parquet", False),
    ]
    stats_cases = [
        _stats_case("long", 4, _le8(4), _le8(4), 0, "OK"),
        _stats_case("long", 4, _le4(4), _le4(4), 0, "BOUND_MISMATCH"),
        _stats_case("long", 4, _le8(4), _le8(42), 0, "BOUND_MISMATCH"),
        _stats_case("long", 4, _le8(2), _le8(4), 0, "BOUND_MISMATCH"),
        _stats_case("long", 4, _le8(42), _le8(42), 0, "BOUND_MISMATCH"),
        _stats_case("long", 4, _le8(4), _le8(4), 1, "NULLS_PRESENT"),
        _stats_case("long", 4, None, _le8(4), 0, "NULL_BOUNDS"),
        _stats_case("long", 4, _le8(4), None, 0, "NULL_BOUNDS"),
        {"col_type": "long", "value": 4, "verdict": "MISSING_STATS", "stats": None},
        _stats_case("int", 4, _le4(4), _le4(4), 0, "OK"),
        _stats_case("int", 4, _le4(2), _le4(4), 0, "BOUND_MISMATCH"),
        _stats_case("int", 4, _le8(4), _le8(4), 0, "BOUND_MISMATCH"),
        _stats_case("int8", 127, _le4(127), _le4(127), 0, "OK"),
        _stats_case("int16", 4, _le4(4), _le4(4), 0, "OK"),
        _stats_case("uint8", 255, _le4(255), _le4(255), 0, "OK"),
        _stats_case("uint16", 4, _le4(4), _le4(4), 0, "OK"),
        _stats_case("uint32", 4294967295, _le8(4294967295), _le8(4294967295), 0, "OK"),
        _stats_case("uint32", 4, _le4(4), _le4(4), 0, "BOUND_MISMATCH"),
    ]
    key_root = ROOT.removeprefix("s3://")
    iam = {
        "data_path": DATA_PATH,
        "namespace": "ingest",
        "table": "events",
        "field": "team_id",
        "col_type": "long",
        "resource_pattern": f"arn:aws:s3:::{key_root}team_id={TAG}/*",
        "values": ["0", "4", "42", "4294967295"],
        "foreign_keys": [
            key_root + "team_id=42/a.parquet",
            key_root + "team_id=4x/a.parquet",
            key_root + "hoglake-dv-1.puffin",
            key_root + "team_id=40/a.parquet",
        ],
    }
    return {
        "format": "hoglake-object-prefix-vectors",
        "version": 1,
        "description": (
            "Shared vectors for the object-prefix layout: a table with an object-prefix field keeps every data file "
            "and deletion vector under <data_path>/data/<ns>/<table>/<field>=<value>/, where value is the canonical "
            "decimal of the file's identity partition value. prefix_cases: build the prefix, or refuse the value. "
            "containment_cases: whether a path is an object under a prefix. stats_cases: whether a file's column "
            "stats for the field prove every row holds the value (Iceberg single-value bounds, lowercase hex). iam: "
            "resource_pattern with the tag replaced by each of values must equal 'arn:aws:s3:::' + that value's "
            "prefix without 's3://' + '*'; read as an IAM glob ('*' spans '/'), the pattern for value 4 must not "
            "match 'arn:aws:s3:::' + each of foreign_keys (bucket/key strings). The count of every case, iam values "
            "and foreign keys included, "
            "is pinned by the server's ObjectPrefixVectorsTest and by pyhoglake's test_object_prefix_vectors.py."
        ),
        "prefix_cases": prefix_cases,
        "containment_cases": [
            {"prefix": four, "path": p, "under": u} for p, u in containment
        ],
        "stats_cases": stats_cases,
        "iam": iam,
    }


def vector_count(doc: dict[str, Any]) -> int:
    return (
        len(doc["prefix_cases"])
        + len(doc["containment_cases"])
        + len(doc["stats_cases"])
        + len(doc["iam"]["values"])
        + len(doc["iam"]["foreign_keys"])
    )


def main() -> None:
    doc = build()
    OUT.write_text(
        json.dumps(doc, indent=2, ensure_ascii=True) + "\n", encoding="utf-8"
    )
    print(f"wrote {vector_count(doc)} vectors to {OUT.name}")


if __name__ == "__main__":
    main()
