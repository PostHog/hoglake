"""Golden bytes of the flush's parquet files: what ``build_flush_plan``
uploads and registers, pinned per file.

``data/flush_golden.json`` was captured from the build that wrote each
part with its own ``pq.write_table``, schema check and footer
measurement, before the write moved onto pyhoglake's writer core
(``write_part``). The build must keep writing it: the body (by sha256),
every registration field (path, record_count, file_size_bytes,
footer_size, column_stats, partition_values), and the commit's file
order. Set ``MILLRACE_RECAPTURE_FLUSH_GOLDEN=1`` to rewrite the fixture
from the current code instead of comparing, after checking that the
writer did not change.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from flushkit import DATA_PATH, TABLE_UUID, table_wire
from pyhoglake import TableInfo

import millrace.flush as flush_mod
from millrace import keyspace

GOLDEN = Path(__file__).parent / "data" / "flush_golden.json"
RECAPTURE = os.environ.get("MILLRACE_RECAPTURE_FLUSH_GOLDEN") == "1"

_DAY_US = 86_400_000_000
_JAN_US = 1_800_000_000_000_000  # 2027-01-15T08:00:00Z


def _c(name: str, type_: str, field_id: int, ordinal: int, **extra: Any) -> dict:
    return {
        "name": name,
        "type": type_,
        "field_id": field_id,
        "ordinal": ordinal,
        **extra,
    }


#: Every column type millrace fills, top level and nested, timestamp_s at
#: both depths (the s -> ms storage conversion).
COLUMNS_WIRE: list[dict[str, Any]] = [
    _c("team_id", "long", 1, 0, nullable=False),
    _c("ts", "timestamp", 2, 1, nullable=False),
    _c("b", "boolean", 3, 2),
    _c("i8", "int8", 4, 3),
    _c("i16", "int16", 5, 4),
    _c("i", "int", 6, 5),
    _c("u8", "uint8", 7, 6),
    _c("u16", "uint16", 8, 7),
    _c("u32", "uint32", 9, 8),
    _c("f", "float", 10, 9),
    _c("d", "double", 11, 10),
    _c("s", "string", 12, 11),
    _c("bin", "binary", 13, 12),
    _c("id", "uuid", 14, 13),
    _c("dec", "decimal", 15, 14, type_params={"precision": 10, "scale": 2}),
    _c("day", "date", 16, 15),
    _c("t", "time", 17, 16),
    _c("ts_s", "timestamp_s", 18, 17),
    _c("ts_ms", "timestamp_ms", 19, 18),
    _c("ts_ns", "timestamp_ns", 20, 19),
    _c("tz", "timestamptz", 21, 20),
    _c("j", "json", 22, 21),
    _c(
        "props",
        "struct",
        23,
        22,
        children=[
            _c("at", "timestamp_s", 24, 0),
            _c("name", "string", 25, 1),
            _c("tags", "list", 26, 2, children=[_c("element", "string", 27, 0)]),
        ],
    ),
    _c(
        "items",
        "list",
        28,
        23,
        children=[
            _c(
                "element",
                "struct",
                29,
                0,
                children=[_c("sku", "string", 30, 0), _c("at", "timestamp_s", 31, 1)],
            )
        ],
    ),
    _c(
        "attrs",
        "map",
        32,
        24,
        children=[
            _c("key", "string", 33, 0, nullable=False),
            _c("value", "double", 34, 1),
        ],
    ),
    _c("u64", "uint64", 35, 25),
    _c("u64s", "list", 36, 26, children=[_c("element", "uint64", 37, 0)]),
]
#: identity(team_id), day(ts): a fanout over teams and days.
SPEC_WIRE = {
    "spec_id": 5,
    "fields": [
        {"source_field_id": 1, "transform": "identity"},
        {"source_field_id": 2, "transform": "day"},
    ],
}
#: ts asc, then the struct leaf props.at desc nulls first.
SORT_WIRE = {
    "sort_id": 2,
    "fields": [
        {"source_field_id": 2, "direction": "asc", "null_order": "nulls_last"},
        {"source_field_id": 24, "direction": "desc", "null_order": "nulls_first"},
    ],
}
UNPARTITIONED_WIRE = {"spec_id": 0, "fields": []}


def _payload(row: int) -> bytes:
    """Row ``row``: three teams over two days, scan order not sort order,
    every fifth row's nullable columns null."""
    team = row % 3
    ts = _JAN_US + (row % 2) * _DAY_US - row * 7_000_000
    doc: dict[str, Any] = {"team_id": team, "ts": ts}
    if row % 5 != 4:
        doc |= {
            "b": row % 2 == 0,
            "i8": -row,
            "i16": row * 300,
            "i": -row * 70_000,
            "u8": row,
            "u16": row * 600,
            "u32": 4_000_000_000 - row,
            "f": row / 4,
            "d": row / 3,
            "s": f"event-{row}-é",
            "bin": "AAEC/w==",
            "id": f"0b8ee9ba-79a1-4f3e-b7e5-6a0b6ab6f0{row:02d}",
            "dec": f"{row}.{row % 100:02d}",
            "day": "2027-01-15",
            "t": f"0{row % 10}:30:00.123456",
            "ts_s": 1_800_000_000 + row,
            "ts_ms": 1_800_000_000_000 + row,
            "ts_ns": 1_800_000_000_123_456_789 + row,
            "tz": "2027-01-15T09:00:00+01:00",
            "j": {"k": [row, None, {"z": True}], "a": "x"},
            "props": {
                "at": None if row % 4 == 1 else 1_800_000_000 + row * 11,
                "name": f"p{row}",
                "tags": ["a", None, f"t{row}"],
            },
            "items": [{"sku": f"s{row}", "at": 1_800_000_000 - row}, {"sku": None}],
            "attrs": {"x": row * 1.5, "y": None},
            "u64": 18_000_000_000_000_000_000 - row,
            "u64s": [row, 2**64 - 1 - row, None],
        }
    return json.dumps(doc).encode()


ROWS = 24

#: case -> (partition spec, sort spec, footer_stats_min_bytes).
CASES: dict[str, tuple[dict, dict | None, int]] = {
    # The production gate: these small files defer their stats.
    "fanout-deferred-stats": (SPEC_WIRE, SORT_WIRE, flush_mod.FOOTER_STATS_MIN_BYTES),
    # The gate lowered so every file registers its footer stats.
    "fanout-footer-stats": (SPEC_WIRE, SORT_WIRE, 1),
    # One file, no partition_values, scan order.
    "unpartitioned": (UNPARTITIONED_WIRE, None, 1),
}


def _plan(case: str, *, footer_stats_min_bytes: int | None = None) -> Any:
    spec, sort, min_bytes = CASES[case]
    info = TableInfo.from_wire(table_wire(columns=COLUMNS_WIRE, spec=spec, sort=sort))
    ctx = flush_mod.FlushContext(
        topic="events",
        slot=3,
        group=(),
        group_fields=(),
        trigger="size",
        read_snapshot=88,
        table_uuid=TABLE_UUID,
        build="golden",
    )
    records = [(keyspace.RowKey((), row), _payload(row)) for row in range(ROWS)]
    return flush_mod.build_flush_plan(
        records,
        info,
        ctx=ctx,
        data_path=DATA_PATH,
        max_files_per_commit=512,
        footer_stats_min_bytes=(
            min_bytes if footer_stats_min_bytes is None else footer_stats_min_bytes
        ),
        version="golden",
    )


def _files(plan: Any) -> list[tuple[dict[str, Any], bytes]]:
    (commit,) = plan.commits
    regs = commit.payload["appends"][0]["files"]
    return [
        (reg, upload.body.to_pybytes())
        for reg, upload in zip(regs, commit.uploads, strict=True)
    ]


def _created_by(body: bytes) -> str:
    return str(pq.read_metadata(pa.BufferReader(body)).created_by)


def _recorded(body: bytes, golden: dict) -> bytes:
    """``body`` with this pyarrow's ``created_by`` (it carries pyarrow's
    version) put back to the one the fixture was captured under, so an
    upgrade that writes the same bytes is not a failure. A version string
    of another length moves the footer: recapture then."""
    now, then = _created_by(body), golden["created_by"]
    assert len(now) == len(then), (
        f"pyarrow's created_by {now!r} is not {then!r}'s length: recapture with "
        "MILLRACE_RECAPTURE_FLUSH_GOLDEN=1, after checking the writer did not change"
    )
    return body.replace(now.encode(), then.encode())


def _entry(reg: dict[str, Any], body: bytes) -> dict[str, Any]:
    return {"sha256": hashlib.sha256(body).hexdigest(), "registration": reg}


def _golden() -> dict:
    return json.loads(GOLDEN.read_text())


@pytest.mark.parametrize("case", sorted(CASES))
def test_the_flush_writes_the_golden_files(case: str) -> None:
    files = _files(_plan(case))
    if RECAPTURE:
        golden = _golden() if GOLDEN.exists() else {}
        golden["created_by"] = _created_by(files[0][1])
        golden.setdefault("cases", {})[case] = [_entry(r, b) for r, b in files]
        GOLDEN.parent.mkdir(exist_ok=True)
        GOLDEN.write_text(json.dumps(golden, indent=1, sort_keys=True) + "\n")
        return
    golden = _golden()
    assert [_entry(r, _recorded(b, golden)) for r, b in files] == golden["cases"][case]


def test_the_fixture_is_not_trivial() -> None:
    """Every case is present, the fanout cases fan out, and their files
    are not one file: the comparison cannot pass on a shrunken fixture."""
    cases = _golden()["cases"]
    assert sorted(cases) == sorted(CASES)
    fanout = cases["fanout-footer-stats"]
    assert len(fanout) == 6 and len(cases["unpartitioned"]) == 1
    assert len({f["sha256"] for f in fanout}) == 6
    assert sum(f["registration"]["record_count"] for f in fanout) == ROWS
    # The gate is the only difference between the two fanout cases.
    assert [f["sha256"] for f in fanout] == [
        f["sha256"] for f in cases["fanout-deferred-stats"]
    ]
    assert all("column_stats" in f["registration"] for f in fanout)
    assert not any(
        "column_stats" in f["registration"] for f in cases["fanout-deferred-stats"]
    )


def test_footer_stats_start_at_the_threshold() -> None:
    """A file of exactly ``footer_stats_min_bytes`` registers its footer
    stats; under it, the registration leaves them to the hydrator."""
    ((reg, _),) = _files(_plan("unpartitioned"))
    size = reg["file_size_bytes"]
    ((at, _),) = _files(_plan("unpartitioned", footer_stats_min_bytes=size))
    ((under, _),) = _files(_plan("unpartitioned", footer_stats_min_bytes=size + 1))
    assert at["column_stats"] == reg["column_stats"]
    assert "column_stats" not in under
