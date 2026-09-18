"""The seeded warehouse's typed telemetry pair.

The star schema's original tables predate hoglake 1.1.0 and exercise
only the old scalar set. These two extend the warehouse across the FULL
type system, with realistic names on top of :mod:`hoglake_bench.datagen`
fabrication:

- ``telemetry.device_metrics`` — every writable scalar (unsigned ints
  with the uint64 upper range, all four timestamp precisions,
  timestamptz, json, uuid, binary, decimal, ...), plus a dense
  ``reading_id`` for row-lineage sanity checks;
- ``telemetry.app_events`` — the nested kinds: string- and
  non-string-keyed maps, list-of-struct spans, a struct-of-list/-struct
  context, list-of-list samples.

``variant`` is deliberately absent: see
:data:`hoglake_bench.datagen.VARIANT_EXCLUSION_REASON`.
"""

from __future__ import annotations

from datetime import UTC, datetime

import numpy as np
import pyarrow as pa

from ..datagen import ColSpec, make_table, to_arrow_schema
from ..datagen.columns import ARROW_BY_SCALAR
from .tables import TEAM_WEIGHTS, uuid_strings, weighted_idx

#: A fixed anchor for unit tests that need a stable history window.
ANCHOR_FOR_TESTS = datetime(2026, 6, 1, tzinfo=UTC)


def _scalar(name: str, coltype: str, **kw: object) -> ColSpec:
    return ColSpec(name=name, coltype=coltype, dtype=ARROW_BY_SCALAR[coltype], **kw)


#: Every writable scalar, under plausible device-telemetry names.
DEVICE_METRICS_SPECS: tuple[ColSpec, ...] = (
    _scalar("reading_id", "long", nullable=False),
    _scalar("device_uuid", "uuid", nullable=False),
    _scalar("is_active", "boolean", null_rate=0.05),
    _scalar("signal_db", "int8", null_rate=0.1),
    _scalar("fw_flags", "int16", null_rate=0.1),
    _scalar("status_code", "int", null_rate=0.05),
    _scalar("battery_pct", "uint8", null_rate=0.15),
    _scalar("error_count", "uint16", null_rate=0.1),
    _scalar("sample_rate_hz", "uint32", null_rate=0.1),
    _scalar("total_bytes", "uint64", null_rate=0.05),
    _scalar("cpu_temp", "float", null_rate=0.2),
    _scalar("pressure_kpa", "double", null_rate=0.15),
    _scalar("cost_per_mb", "decimal", null_rate=0.3),
    _scalar("reading_date", "date", null_rate=0.05),
    _scalar("reading_time", "time", null_rate=0.25),
    _scalar("recorded_s", "timestamp_s", null_rate=0.1),
    _scalar("recorded_ms", "timestamp_ms", null_rate=0.1),
    _scalar("recorded_at", "timestamp", nullable=False),
    _scalar("recorded_ns", "timestamp_ns", null_rate=0.1),
    _scalar("ingested_at", "timestamptz", null_rate=0.05),
    _scalar("firmware", "string", null_rate=0.2),
    _scalar("payload", "json", null_rate=0.35),
    _scalar("checksum", "binary", null_rate=0.4),
)
DEVICE_METRICS_SCHEMA = to_arrow_schema(DEVICE_METRICS_SPECS)

_SPAN = pa.struct(
    [
        pa.field("op", pa.string()),
        pa.field("dur_us", pa.int64()),
        pa.field("ok", pa.bool_()),
    ]
)
_CONTEXT = pa.struct(
    [
        pa.field(
            "app",
            pa.struct(
                [pa.field("version", pa.string()), pa.field("build", pa.int64())]
            ),
        ),
        pa.field("locale", pa.string()),
        pa.field("screens", pa.list_(pa.string())),
    ]
)

#: The nested kinds, under plausible product-analytics names.
APP_EVENTS_SPECS: tuple[ColSpec, ...] = (
    ColSpec("event_id", "string", pa.string(), nullable=False),
    _scalar("team_id", "long", nullable=False),
    _scalar("ts", "timestamptz", nullable=False),
    ColSpec("properties", "map", pa.map_(pa.string(), pa.string()), null_rate=0.2),
    ColSpec("metrics", "map", pa.map_(pa.string(), pa.float64()), null_rate=0.15),
    # non-string map key: numeric error-code -> message
    ColSpec("error_codes", "map", pa.map_(pa.int32(), pa.string()), null_rate=0.4),
    ColSpec("tags", "list", pa.list_(pa.string()), null_rate=0.25),
    ColSpec("spans", "list", pa.list_(_SPAN), null_rate=0.2),
    ColSpec("context", "struct", _CONTEXT, null_rate=0.1),
    ColSpec("samples", "list", pa.list_(pa.list_(pa.float64())), null_rate=0.3),
)
APP_EVENTS_SCHEMA = to_arrow_schema(APP_EVENTS_SPECS)


def device_metrics(rng: np.random.Generator, n: int, *, id_offset: int) -> pa.Table:
    """``n`` rows of full-scalar telemetry; ``reading_id`` continues
    densely from ``id_offset`` (like the dims' id columns)."""
    t = make_table(rng, DEVICE_METRICS_SPECS, n)
    ids = pa.array(np.arange(id_offset + 1, id_offset + n + 1, dtype=np.int64))
    idx = t.schema.get_field_index("reading_id")
    return t.set_column(idx, DEVICE_METRICS_SCHEMA.field("reading_id"), ids)


def app_events(
    rng: np.random.Generator,
    n: int,
    *,
    team_ids: tuple[int, ...],
    lo_us: int,
    hi_us: int,
) -> pa.Table:
    """``n`` rows of nested-typed app events: datagen fabrication for the
    containers, warehouse-real values for id/team/timestamp."""
    t = make_table(rng, APP_EVENTS_SPECS, n)

    def put(name: str, arr: pa.Array) -> None:
        nonlocal t
        idx = t.schema.get_field_index(name)
        t = t.set_column(idx, APP_EVENTS_SCHEMA.field(name), arr)

    put("event_id", pa.array(uuid_strings(rng, n)))
    put(
        "team_id",
        pa.array(
            np.asarray(team_ids, dtype=np.int64)[
                weighted_idx(rng, TEAM_WEIGHTS[: len(team_ids)], n)
            ]
        ),
    )
    micros = rng.integers(lo_us, max(hi_us, lo_us + 1), n, dtype=np.int64)
    micros.sort()
    put("ts", pa.array(micros, pa.int64()).cast(pa.timestamp("us", tz="UTC")))
    return t
