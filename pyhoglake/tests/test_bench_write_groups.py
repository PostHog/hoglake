"""The writer's fixed cost a partition group, with VARIANT columns: a
guard, not a test, run with ``PYHOGLAKE_BENCH=1 uv run pytest -s
tests/test_bench_write_groups.py``.

A millpond flush writes ~155k rows as tens to hundreds of (team, month)
groups on one thread, so what write_part costs a file, beyond the rows,
is what bounds the flush. This writes a seeded sample as 50, 500 and 2000
Zipf-sized groups and reports µs a row and ms a group, per codec in use,
beside the same rows written as plain strings; it asserts nothing about
the numbers, which are the machine's.
"""

from __future__ import annotations

import json
import os
import random
import time

import pyarrow as pa
import pytest

from pyhoglake import VariantReport, parquet_write_options, write_part
from pyhoglake import _variant_backend as backend
from pyhoglake.models import Column
from pyhoglake.types import columns_to_arrow_schema

pytestmark = pytest.mark.skipif(
    os.environ.get("PYHOGLAKE_BENCH") != "1", reason="set PYHOGLAKE_BENCH=1 to run"
)

ROWS = int(os.environ.get("PYHOGLAKE_BENCH_ROWS", "20000"))
GROUPS = (50, 500, 2000)
DECLARATION = {
    "type": "object",
    "fields": [
        *({"name": f"${name}", "type": "string"} for name in (
            "browser", "browser_version", "os", "os_version", "device_type",
            "lib", "lib_version", "session_id", "window_id", "geoip_city_name",
            "geoip_country_code", "geoip_time_zone", "timezone",
        )),
        {"name": "$feature_flags", "type": "variant"},
        {"name": "$active_feature_flags", "type": "array", "element": {"type": "string"}},
        {"name": "$screen_width", "type": "int64"},
        {"name": "$exception_list", "type": "array", "element": {"type": "object", "fields": [
            {"name": "type", "type": "string"}, {"name": "value", "type": "string"}]}},
    ],
}  # fmt: skip


def _row(rng: random.Random, i: int) -> str:
    props = {f"${k}": rng.choice(["a", "b", "c", "dd"]) for k in (
        "browser", "browser_version", "os", "os_version", "device_type", "lib",
        "lib_version", "geoip_city_name", "geoip_country_code", "geoip_time_zone",
        "timezone")}  # fmt: skip
    props["$session_id"] = f"0192{i:012x}"
    props["$window_id"] = f"0192{rng.randrange(1 << 40):012x}"
    flags = rng.sample(["f1", "f2", "f3", "f4", "f5", "f6"], rng.randrange(0, 5))
    props["$feature_flags"] = dict.fromkeys(flags, "control")
    props["$active_feature_flags"] = flags
    props["$screen_width"] = rng.choice([1280, 1440, 1920])
    if rng.random() < 0.02:
        props["$exception_list"] = [{"type": "TypeError", "value": "x is undefined"}]
    for k in range(rng.randrange(10, 40)):
        props[f"custom_{k}"] = rng.choice(
            [rng.random(), rng.randrange(10**6), "text " * 5]
        )
    return json.dumps(props, separators=(",", ":"))


def _columns(variant: bool) -> tuple[Column, ...]:
    kind = "variant" if variant else "string"
    params = {"shredding": DECLARATION} if variant else None
    return (
        Column("uuid", "string", 1, 0, False),
        Column("team_id", "long", 2, 1, False),
        Column("properties", kind, 3, 2, True, params),
        Column("person_properties", kind, 4, 3, True, params),
        Column("group0_properties", kind, 5, 4, True),
    )


def _sample(variant: bool) -> pa.Table:
    rng = random.Random(20261109)
    text = [_row(rng, i) for i in range(ROWS)]
    person = [_row(rng, i) for i in range(ROWS)]
    group = [_row(rng, i) if rng.random() < 0.1 else None for i in range(ROWS)]
    schema = columns_to_arrow_schema(_columns(variant), variant="json")
    arrays = [
        pa.array([f"0192{i:028x}" for i in range(ROWS)]),
        pa.array([2] * ROWS, pa.int64()),
        *(pa.array(v, pa.string()) for v in (text, person, group)),
    ]
    if variant:
        arrays[2:] = [pa.ExtensionArray.from_storage(pa.json_(), a) for a in arrays[2:]]
    return pa.Table.from_arrays(arrays, schema=schema)


def _groups(table: pa.Table, count: int) -> list[pa.Table]:
    weights = [1 / (k + 1) for k in range(count)]
    total = sum(weights)
    sizes = [max(1, int(ROWS * w / total)) for w in weights]
    sizes[0] += ROWS - sum(sizes)
    parts, at = [], 0
    for size in sizes:
        parts.append(table.slice(at, size))
        at += size
    return parts


def test_write_part_cost_a_group(record_property):
    options = parquet_write_options("zstd", 3)
    lines = [f"codec={backend.MODE} native={backend.native()[0] is not None} rows={ROWS}",
             "| groups | variant µs/row | variant ms/group | string µs/row | string ms/group |",
             "|---|---|---|---|---|"]  # fmt: skip
    tables = {variant: _sample(variant) for variant in (True, False)}
    for count in GROUPS:
        cells = []
        for variant in (True, False):
            columns, parts = _columns(variant), _groups(tables[variant], count)
            best = None
            for _ in range(2):
                report = VariantReport()
                started = time.perf_counter()
                for part in parts:
                    kwargs = (
                        {"variant_on_invalid": "null", "variant_report": report}
                        if variant
                        else {}
                    )
                    write_part(part, columns, write_options=options, **kwargs)  # type: ignore[arg-type]
                best = min(best or 1e9, time.perf_counter() - started)
            assert best is not None
            cells += [f"{best / ROWS * 1e6:.1f}", f"{best / count * 1e3:.2f}"]
            record_property(
                f"{'variant' if variant else 'string'}_{count}_ms_group", cells[-1]
            )
        lines.append(f"| {count} | " + " | ".join(cells) + " |")
    print("\n" + "\n".join(lines))
