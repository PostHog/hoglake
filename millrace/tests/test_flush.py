"""Flush-path tests.

Layers: the decoder, the commit message and the plan builder are PURE
(unit, no I/O); everything past them is component-tested against a REAL
``PartitionStage`` on ``memory:///`` with a scripted hoglake server
(pytest-httpx) and a fake object store (tests/flushkit.py). The
at-least-once contract itself (drain, split commits, stuck groups,
ancient events, recreated table) lives in tests/test_flush_contract.py.

Pinned here:

- the wire contract: every commit carries the identity read's
  ``read_snapshot_id``, the pinned ``expected_table_uuid`` and a uuid4
  idempotency key; every table read is ``totals=false``; the shape
  cache's TTL is the catalog retention / 4;
- the staging group is the destination's partition tuple: one flush of
  a group keyed under the live spec writes one file, and a group keyed
  under an OLDER spec still fans out by the live one;
- the commit ladder: ambiguous or retryable answers resend the
  identical bytes; an answered 4xx fails the decision with the rows
  staged; a recreated table halts;
- files are sorted by the live sort spec before the partition fanout;
- footer stats ship only for files at least ``FOOTER_STATS_MIN_BYTES``;
- orphan accounting on a partial upload failure;
- unrepresentable records are dropped and counted, never batch-fatal;
- the runner: per-group containment, the concurrency bound, and a fenced
  stage latching :class:`~millrace.stage.FencedError`.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import re
import uuid
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from flushkit import (
    BASE,
    CATALOG,
    COLUMNS_WIRE,
    DATA_PATH,
    NAMESPACE,
    RECREATED_UUID,
    RETENTION_S,
    SORT_WIRE,
    SPEC_FIELDS,
    TABLE,
    TABLE_URL,
    TABLE_UUID,
    CommitCapture,
    FakeMonotonic,
    FakeObjectStore,
    Sleeper,
    commit_ok,
    commit_refusal,
    event_payload,
    flush_group,
    make_config,
    month_wire,
    parquet_rows,
    script_options,
    script_table_refresh,
    table_wire,
    team_group,
    team_label,
)
from pyhoglake import TableInfo
from pyhoglake.models import Column
from pyhoglake.parquet_schema import prepared_schema_matches
from slatedb.uniffi import CloseReason
from slatedb.uniffi import Error as SlateError
from stagekit import NOW, fast_flush_settings

import millrace.flush as flush_mod
from millrace import keyspace
from millrace.coerce import (
    REASON_UNCASTABLE_COLUMN,
    REASON_UNDECODABLE_PAYLOAD,
    DecodeError,
    UnsupportedColumnError,
)
from millrace.flush import (
    BatchReport,
    CommitExhausted,
    CommitRefused,
    FlushHalted,
    FlushReport,
    FlushRunner,
    FlushStartupError,
    HoglakeFlusher,
    JsonObjectDecoder,
    build_flush_plan,
    format_commit_message,
)
from millrace.stage import FencedError, StagedRecord, StageManager

component = pytest.mark.component

TEAM = 7
JAN_US = NOW  # 2027-01-15
DEC_US = NOW - 20 * 86_400_000_000  # 2026-12-26: one month boundary back


def columns(columns_wire: list[dict[str, Any]] | None = None) -> tuple[Column, ...]:
    return tuple(Column.from_wire(c) for c in (columns_wire or COLUMNS_WIRE))


async def staged(
    stage: Any,
    clock: Any,
    team: int,
    offsets_ts: list[tuple[int, int]],
    *,
    payloads: list[bytes] | None = None,
) -> None:
    """Stage one team's rows: (offset, ts_us) pairs with event payloads."""
    await stage.stage_batch(
        [
            StagedRecord(
                group=team_group(team),
                offset=offset,
                payload=(event_payload(team, ts) if payloads is None else payloads[i]),
            )
            for i, (offset, ts) in enumerate(offsets_ts)
        ],
        now_us=clock(),
    )


async def staged_rows(stage: Any, group: Any) -> list[tuple[Any, bytes]]:
    """Every staged row of ``group`` (an int names the team's default
    group)."""
    if isinstance(group, int):
        group = team_group(group)
    return (await stage.scan(group, max_bytes=1 << 40, now_us=0)).rows


def ts_micros(value: Any) -> int:
    """A readback timestamp (naive datetime from parquet) → epoch micros."""
    from datetime import datetime, timedelta

    return (value - datetime.fromisoformat("1970-01-01T00:00:00")) // timedelta(
        microseconds=1
    )


def runner(
    stages: StageManager,
    flusher: Any,
    cfg: Any,
    clock: Any,
    sleeper: Sleeper,
    *,
    mono: Any = None,
    concurrency: int = 4,
    batch_max_groups: int = 512,
) -> FlushRunner:
    extra = {} if mono is None else {"monotonic": mono}
    return FlushRunner(
        stages,
        flusher,
        cfg.planner_knobs(),
        now_us=clock,
        sleep=sleeper,
        sweep_s=60.0,
        concurrency=concurrency,
        batch_max_groups=batch_max_groups,
        **extra,
    )


def info_for(wire: dict[str, Any] | None = None) -> TableInfo:
    return TableInfo.from_wire(wire or table_wire())


def ctx_for(group: Any = None, *, build: str = "b0") -> flush_mod.FlushContext:
    group = team_group(TEAM) if group is None else group
    return flush_mod.FlushContext(
        topic="events",
        slot=0,
        group=group,
        group_fields=SPEC_FIELDS if len(group) == 2 else ("key_0",) * len(group),
        trigger="size",
        read_snapshot=88,
        table_uuid=TABLE_UUID,
        build=build,
    )


def plan_for(
    records: list[tuple[keyspace.RowKey, bytes]],
    info: TableInfo,
    *,
    ctx: flush_mod.FlushContext | None = None,
    max_files_per_commit: int = 512,
    footer_stats_min_bytes: int = 1,
) -> flush_mod.FlushPlan:
    return build_flush_plan(
        records,
        info,
        ctx=ctx or ctx_for(),
        data_path=DATA_PATH,
        max_files_per_commit=max_files_per_commit,
        footer_stats_min_bytes=footer_stats_min_bytes,
        version="test",
    )


# =====================================================================================
# The decoder (pure)
# =====================================================================================


class TestJsonDecoder:
    def test_round_trip_scalars(self) -> None:
        decoder = JsonObjectDecoder(columns())
        record = decoder.decode_record(
            b'{"team_id": 7, "timestamp": 1800000000123456, "event": "pageview"}'
        )
        assert record.values == {
            "team_id": 7,
            "timestamp": 1_800_000_000_123_456,
            "event": "pageview",
        }
        assert record.dropped_keys == ()

    def test_missing_nullable_is_null_and_missing_nonnullable_fails(self) -> None:
        decoder = JsonObjectDecoder(columns())
        record = decoder.decode_record(b'{"team_id": 7, "timestamp": 5}')
        assert record.values["event"] is None
        with pytest.raises(DecodeError) as excinfo:
            decoder.decode_record(b'{"timestamp": 5}')
        assert excinfo.value.reason == REASON_UNCASTABLE_COLUMN
        assert "team_id" in str(excinfo.value)

    def test_explicit_null_into_nonnullable_fails(self) -> None:
        decoder = JsonObjectDecoder(columns())
        with pytest.raises(DecodeError):
            decoder.decode_record(b'{"team_id": 7, "timestamp": null}')

    def test_unknown_keys_are_dropped_and_named(self) -> None:
        decoder = JsonObjectDecoder(columns())
        record = decoder.decode_record(
            b'{"team_id": 7, "timestamp": 5, "zz": 1, "aa": [1, 2]}'
        )
        assert record.dropped_keys == ("aa", "zz")
        assert "aa" not in record.values

    @pytest.mark.parametrize(
        "payload",
        [
            b"not json",
            b'"a string"',
            b"[1, 2]",
            b"42",
            b'{"x": NaN}',
            b'{"x": Infinity}',
        ],
    )
    def test_undecodable_payloads(self, payload: bytes) -> None:
        decoder = JsonObjectDecoder(columns())
        with pytest.raises(DecodeError) as excinfo:
            decoder.decode_record(payload)
        assert excinfo.value.reason == REASON_UNDECODABLE_PAYLOAD

    @pytest.mark.parametrize(
        "payload",
        [
            b'{"team_id": "7", "timestamp": 5}',  # string for long
            b'{"team_id": true, "timestamp": 5}',  # bool is not an int
            b'{"team_id": 7.5, "timestamp": 5}',  # float for long
            b'{"team_id": 7, "timestamp": 5, "event": 9}',  # int for string
        ],
    )
    def test_uncastable_columns(self, payload: bytes) -> None:
        decoder = JsonObjectDecoder(columns())
        with pytest.raises(DecodeError) as excinfo:
            decoder.decode_record(payload)
        assert excinfo.value.reason == REASON_UNCASTABLE_COLUMN

    def test_int_width_bounds(self) -> None:
        wire = [
            {
                "name": "v",
                "type": "int8",
                "field_id": 1,
                "ordinal": 0,
                "nullable": False,
            }
        ]
        decoder = JsonObjectDecoder(columns(wire))
        assert decoder.decode_record(b'{"v": 127}').values["v"] == 127
        with pytest.raises(DecodeError):
            decoder.decode_record(b'{"v": 128}')

    def test_timestamp_units_and_iso_forms(self) -> None:
        wire = [
            {"name": "s", "type": "timestamp_s", "field_id": 1, "ordinal": 0},
            {"name": "ms", "type": "timestamp_ms", "field_id": 2, "ordinal": 1},
            {"name": "us", "type": "timestamp", "field_id": 3, "ordinal": 2},
            {"name": "ns", "type": "timestamp_ns", "field_id": 4, "ordinal": 3},
            {"name": "tz", "type": "timestamptz", "field_id": 5, "ordinal": 4},
        ]
        decoder = JsonObjectDecoder(columns(wire))
        record = decoder.decode_record(
            b'{"s": 1800000000, "ms": 1800000000123, "us": 1800000000123456,'
            b' "ns": 1800000000123456789,'
            b' "tz": "2027-01-15T00:00:00Z"}'
        )
        assert record.values["s"] == 1_800_000_000
        assert record.values["ms"] == 1_800_000_000_123
        assert record.values["us"] == 1_800_000_000_123_456
        assert record.values["ns"] == 1_800_000_000_123_456_789
        # (1_800_000_000s past the epoch is 2027-01-15T08:00:00Z, so
        # midnight carries 8h less.)
        assert record.values["tz"] == 1_799_971_200_000_000
        # ISO into a naive column; +02:00 offset arithmetic on timestamptz.
        # (1_800_000_000s past the epoch is 2027-01-15T08:00:00Z.)
        record = decoder.decode_record(
            b'{"s": null, "ms": null, "us": "2027-01-15T08:00:00.123456",'
            b' "ns": null, "tz": "2027-01-15T10:00:00+02:00"}'
        )
        assert record.values["us"] == 1_800_000_000_123_456
        assert record.values["tz"] == 1_800_000_000_000_000

    def test_timestamp_iso_precision_is_never_truncated(self) -> None:
        wire = [
            {"name": "s", "type": "timestamp_s", "field_id": 1, "ordinal": 0},
        ]
        decoder = JsonObjectDecoder(columns(wire))
        with pytest.raises(DecodeError):
            # sub-second precision into a seconds column
            decoder.decode_record(b'{"s": "2027-01-15T00:00:00.5"}')
        wire_ns = [
            {"name": "ns", "type": "timestamp_ns", "field_id": 1, "ordinal": 0},
        ]
        decoder_ns = JsonObjectDecoder(columns(wire_ns))
        with pytest.raises(DecodeError):
            # sub-micro digits in a string are refused even for ns
            decoder_ns.decode_record(b'{"ns": "2027-01-15T00:00:00.123456789"}')
        # ...but trailing zeros are exactly representable and fine:
        assert (
            decoder_ns.decode_record(b'{"ns": "2027-01-15T08:00:00.123456000"}').values[
                "ns"
            ]
            == 1_800_000_000_123_456_000
        )

    @pytest.mark.parametrize(
        ("raw", "micros"),
        [
            # The ClickHouse wire shapes PostHog's producers emit: space
            # separator, 0/3/6 fractional digits, no offset, meaning UTC.
            ("2027-01-15 08:00:00", 1_800_000_000_000_000),
            ("2027-01-15 08:00:00.123", 1_800_000_000_123_000),
            ("2027-01-15 08:00:00.123456", 1_800_000_000_123_456),
            ("2027-01-15T08:00:00", 1_800_000_000_000_000),
            # Offsets still apply.
            ("2027-01-15T08:00:00Z", 1_800_000_000_000_000),
            ("2027-01-15 10:00:00+02:00", 1_800_000_000_000_000),
        ],
    )
    def test_timestamptz_reads_a_naive_string_as_utc(self, raw, micros) -> None:
        wire = [
            {"name": "tz", "type": "timestamptz", "field_id": 1, "ordinal": 0},
        ]
        decoder = JsonObjectDecoder(columns(wire))
        record = decoder.decode_record(json.dumps({"tz": raw}).encode())
        assert record.values["tz"] == micros

    def test_naive_column_still_refuses_an_offset(self) -> None:
        wire = [
            {"name": "us", "type": "timestamp", "field_id": 1, "ordinal": 0},
        ]
        decoder = JsonObjectDecoder(columns(wire))
        with pytest.raises(DecodeError):
            decoder.decode_record(b'{"us": "2027-01-15T08:00:00Z"}')

    def test_full_scalar_zoo_builds_arrow(self) -> None:
        """Every supported column type decodes and lands in Arrow at the
        catalog's type (the decoder/array-builder agreement, pinned)."""
        wire = [
            {"name": "b", "type": "boolean", "field_id": 1, "ordinal": 0},
            {"name": "i", "type": "int", "field_id": 2, "ordinal": 1},
            {"name": "l", "type": "long", "field_id": 3, "ordinal": 2},
            {"name": "u", "type": "uint32", "field_id": 4, "ordinal": 3},
            {"name": "f", "type": "double", "field_id": 5, "ordinal": 4},
            {"name": "s", "type": "string", "field_id": 6, "ordinal": 5},
            {"name": "bin", "type": "binary", "field_id": 7, "ordinal": 6},
            {"name": "id", "type": "uuid", "field_id": 8, "ordinal": 7},
            {
                "name": "d",
                "type": "decimal",
                "field_id": 9,
                "ordinal": 8,
                "type_params": {"precision": 10, "scale": 2},
            },
            {"name": "day", "type": "date", "field_id": 10, "ordinal": 9},
            {"name": "t", "type": "time", "field_id": 11, "ordinal": 10},
            {"name": "ts", "type": "timestamp", "field_id": 12, "ordinal": 11},
            {"name": "tz", "type": "timestamptz", "field_id": 13, "ordinal": 12},
        ]
        decoder = JsonObjectDecoder(columns(wire))
        record = decoder.decode_record(
            b'{"b": true, "i": -5, "l": -5000000000, "u": 4000000000, "f": 1.5,'
            b' "s": "x", "bin": "AAE=",'
            b' "id": "0b8ee9ba-79a1-4f3e-b7e5-6a0b6ab6f012",'
            b' "d": "12.34", "day": "2027-01-15", "t": "01:00:00",'
            b' "ts": 1800000000123456, "tz": 1800000000123456}'
        )
        assert record.values["bin"] == b"\x00\x01"
        table = flush_mod._build_arrow([record], decoder.columns)
        assert table.num_rows == 1
        assert table.column("id")[0].as_py().hex == "0b8ee9ba79a14f3eb7e56a0b6ab6f012"
        assert table.column("d")[0].as_py().as_integer_ratio() == (617, 50)
        assert table.column("t")[0].as_py().hour == 1
        assert table.column("tz")[0].as_py().tzinfo is not None

    def test_decimal_refuses_rounding_and_floats(self) -> None:
        wire = [
            {
                "name": "d",
                "type": "decimal",
                "field_id": 1,
                "ordinal": 0,
                "type_params": {"precision": 10, "scale": 2},
            }
        ]
        decoder = JsonObjectDecoder(columns(wire))
        with pytest.raises(DecodeError):
            decoder.decode_record(b'{"d": "1.234"}')  # would round
        with pytest.raises(DecodeError):
            decoder.decode_record(b'{"d": 1.5}')  # a float is not the decimal written
        with pytest.raises(DecodeError):
            decoder.decode_record(b'{"d": 12345678901}')  # precision 10 exceeded

    def test_unfillable_columns_are_a_startup_refusal(self) -> None:
        """What is still refused by shape: an unknown type, a variant (top
        level or below a container), a container whose children do not
        match its type, and a map whose key is a container or json (a
        JSON object key is a string, which no such key coerces — without
        the refusal every non-empty map would drop at flush time)."""
        variant_in_struct = {
            "name": "c",
            "type": "struct",
            "field_id": 1,
            "ordinal": 0,
            "children": [{"name": "v", "type": "variant", "field_id": 2, "ordinal": 0}],
        }
        bad_list = {
            "name": "c",
            "type": "list",
            "field_id": 1,
            "ordinal": 0,
            "children": [{"name": "item", "type": "long", "field_id": 2, "ordinal": 0}],
        }
        variant = {"name": "c", "type": "variant", "field_id": 1, "ordinal": 0}

        def map_keyed_by(key: dict[str, Any]) -> dict[str, Any]:
            return {
                "name": "c",
                "type": "map",
                "field_id": 1,
                "ordinal": 0,
                "children": [
                    {"name": "key", "field_id": 2, "ordinal": 0, "nullable": False}
                    | key,
                    {"name": "value", "type": "long", "field_id": 9, "ordinal": 1},
                ],
            }

        struct_key = map_keyed_by(
            {
                "type": "struct",
                "children": [
                    {"name": "x", "type": "long", "field_id": 3, "ordinal": 0}
                ],
            }
        )
        list_key = map_keyed_by(
            {
                "type": "list",
                "children": [
                    {"name": "element", "type": "long", "field_id": 3, "ordinal": 0}
                ],
            }
        )
        json_key = map_keyed_by({"type": "json"})
        unknown = {"name": "c", "type": "interval", "field_id": 1, "ordinal": 0}
        for wire in (
            variant_in_struct,
            bad_list,
            variant,
            unknown,
            struct_key,
            list_key,
            json_key,
        ):
            with pytest.raises(UnsupportedColumnError, match="'c[.']"):
                JsonObjectDecoder(columns([wire]))

    def test_timestamp_ints_outside_the_range_clamp_per_unit(self) -> None:
        """An epoch integer outside years 1-9999 (narrowed to the unit's
        int64 range) is clamped to the edge and counted, never dropped and
        never an Arrow-build OverflowError."""
        from millrace.coerce import timestamp_bounds

        for t in (
            "timestamp_s",
            "timestamp_ms",
            "timestamp",
            "timestamp_ns",
            "timestamptz",
        ):
            d = JsonObjectDecoder(
                columns([{"name": "v", "type": t, "field_id": 1, "ordinal": 0}])
            )
            lo, hi = timestamp_bounds(t)
            assert d.decode_record(b'{"v": %d}' % hi).values["v"] == hi
            assert d.decode_record(b'{"v": %d}' % lo).values["v"] == lo
            assert d.clamped == {}
            assert d.decode_record(b'{"v": %d}' % (hi + 1)).values["v"] == hi
            assert d.decode_record(b'{"v": %d}' % (lo - 1)).values["v"] == lo
            assert d.decode_record(b'{"v": %d}' % (2**80)).values["v"] == hi
            assert d.clamped == {"v": 3}
        # timestamp_ns spans only what int64 nanos hold.
        assert timestamp_bounds("timestamp_ns") == (-(2**63), 2**63 - 1)
        # Microsecond columns: years 1-9999 exactly.
        assert timestamp_bounds("timestamp") == (
            -62_135_596_800_000_000,
            253_402_300_799_999_999,
        )

    def test_timestamp_ns_string_past_int64_clamps(self) -> None:
        """The string path multiplies micros by 1000 for ns: the largest
        ISO-8601 datetime is past int64 nanoseconds and clamps; the SAME
        string is exact for a micros column."""
        ns = JsonObjectDecoder(
            columns(
                [{"name": "v", "type": "timestamp_ns", "field_id": 1, "ordinal": 0}]
            )
        )
        assert (
            ns.decode_record(b'{"v": "9999-12-31T23:59:59"}').values["v"] == 2**63 - 1
        )
        assert ns.clamped == {"v": 1}
        us = JsonObjectDecoder(
            columns([{"name": "v", "type": "timestamp", "field_id": 1, "ordinal": 0}])
        )
        from datetime import datetime as _dt
        from datetime import timedelta as _td

        expected = (
            _dt.fromisoformat("9999-12-31T23:59:59") - _dt.fromisoformat("1970-01-01")
        ) // _td(microseconds=1)
        assert us.decode_record(b'{"v": "9999-12-31T23:59:59"}').values["v"] == expected
        assert us.clamped == {}

    def test_iso_years_outside_1_to_9999_clamp(self) -> None:
        """Year 0, negative years and years past 9999 (which ``datetime``
        cannot parse) clamp to the edges when the rest of the string is
        well-formed ISO-8601; a malformed string is still refused."""
        from millrace.coerce import TIMESTAMP_MAX_US, TIMESTAMP_MIN_US

        for t in ("timestamp", "timestamptz"):
            d = JsonObjectDecoder(
                columns([{"name": "v", "type": t, "field_id": 1, "ordinal": 0}])
            )
            assert d.decode_record(b'{"v": "10000-01-01T00:00:00"}').values["v"] == (
                TIMESTAMP_MAX_US
            )
            assert d.decode_record(b'{"v": "0000-06-01T00:00:00"}').values["v"] == (
                TIMESTAMP_MIN_US
            )
            assert d.decode_record(b'{"v": "-0044-03-15 12:00:00"}').values["v"] == (
                TIMESTAMP_MIN_US
            )
            with pytest.raises(DecodeError):
                d.decode_record(b'{"v": "10000-13-45T00:00:00"}')
            assert d.clamped == {"v": 3}
        # An offset near the edge is integer arithmetic, not an overflow.
        tz = JsonObjectDecoder(
            columns([{"name": "v", "type": "timestamptz", "field_id": 1, "ordinal": 0}])
        )
        assert (
            tz.decode_record(b'{"v": "9999-12-31T23:59:59.999999-05:00"}').values["v"]
            == TIMESTAMP_MAX_US
        )
        assert tz.clamped == {"v": 1}

    def test_float_refuses_non_finite_and_unconvertible(self) -> None:
        """json.loads("1e400") yields inf WITHOUT tripping parse_constant
        (that hook sees only the NaN/Infinity literals), and
        float(10**400) raises OverflowError — both are per-record
        coercion failures, never a non-finite value in a column or a
        batch-fatal OverflowError."""
        decoder = JsonObjectDecoder(
            columns([{"name": "f", "type": "double", "field_id": 1, "ordinal": 0}])
        )
        assert decoder.decode_record(b'{"f": 1.5}').values["f"] == 1.5
        # the largest finite double stays legal
        assert (
            decoder.decode_record(b'{"f": 1.7976931348623157e308}').values["f"]
            == 1.7976931348623157e308
        )
        for payload in (
            b'{"f": 1e400}',  # parses to inf: a magnitude-overflow literal
            b'{"f": -1e400}',
            b'{"f": 1' + b"0" * 400 + b"}",  # 10**400: no double holds it
        ):
            with pytest.raises(DecodeError) as excinfo:
                decoder.decode_record(payload)
            assert excinfo.value.reason == REASON_UNCASTABLE_COLUMN

    def test_overflow_literal_in_an_unknown_key_is_dropped_harmlessly(self) -> None:
        # 1e400 under a key the table does not have: parsed (to inf),
        # dropped, never coerced — unknown keys stay drop-only.
        record = JsonObjectDecoder(columns()).decode_record(
            b'{"team_id": 7, "timestamp": 5, "surprise": 1e400}'
        )
        assert record.dropped_keys == ("surprise",)
        assert "surprise" not in record.values

    def test_deeply_nested_payload_is_a_per_record_failure(self) -> None:
        # The C scanner reports extreme nesting as RecursionError — NOT a
        # ValueError. It is dropped per record like any other
        # undecodable payload, or one such record wedges the flush.
        decoder = JsonObjectDecoder(columns())
        with pytest.raises(DecodeError) as excinfo:
            decoder.decode_record(b"[" * 100_000 + b"1" + b"]" * 100_000)
        assert excinfo.value.reason == REASON_UNDECODABLE_PAYLOAD

    def test_decimal_column_shape_is_a_startup_refusal(self) -> None:
        """A decimal column whose type_params are missing or outside
        decimal128's range is a DESTINATION defect — refused at decoder
        construction (startup validation), never a per-record failure at
        flush time (a KeyError/pa-decimal128 raise there would wedge the
        key identically forever)."""
        for params in (None, {}, {"precision": 76, "scale": 10}, {"precision": 10}):
            wire: list[dict[str, Any]] = [
                {"name": "d", "type": "decimal", "field_id": 1, "ordinal": 0}
            ]
            if params is not None:
                wire[0]["type_params"] = params
            with pytest.raises(UnsupportedColumnError, match="'d'"):
                JsonObjectDecoder(columns(wire))


# =====================================================================================
# The commit message (pure)
# =====================================================================================


class TestCommitMessage:
    def test_golden_shape(self) -> None:
        message = format_commit_message(
            table_uuid=TABLE_UUID,
            group=team_label(TEAM),
            trigger="size",
            records=5,
            dropped=1,
            files=2,
            commit_index=0,
            commits=3,
            topic="events",
            stage=3,
            version="0.1.0.dev0",
        )
        assert message.splitlines() == [
            (
                f"millrace=v2 table={TABLE_UUID} group={team_label(7)} trigger=size "
                "records=5 dropped=1 files=2 commit=1/3 millrace=0.1.0.dev0"
            ),
            "offsets topic=events stage=3",
        ]

    def test_no_value_holds_a_space(self) -> None:
        message = format_commit_message(
            table_uuid=TABLE_UUID,
            group="g",
            trigger="age",
            records=1,
            dropped=0,
            files=1,
            commit_index=0,
            commits=1,
            topic="events",
            stage=0,
            version="1.0 dev\tbuild",
        )
        assert "millrace=1.0_dev_build" in message.split()

    def test_the_plan_is_built_before_any_upload(self) -> None:
        """Source-order pin: the pure builder runs (and formats every
        message) before the first upload, so a build failure can never
        leave an uncounted orphan."""
        source = inspect.getsource(HoglakeFlusher.flush_batch)
        assert source.index("_build_plans(") < source.index("_publish_chunk(")
        assert "build_flush_plan" in inspect.getsource(HoglakeFlusher._build_plans)
        assert "_upload_and_commit(" in inspect.getsource(HoglakeFlusher._publish_chunk)
        upload = inspect.getsource(HoglakeFlusher._upload_and_commit)
        assert upload.index("self._upload") < upload.index("self._publish(")


# =====================================================================================
# The pure plan builder
# =====================================================================================


class TestPlanBuilder:
    def test_pre_epoch_month_and_null_partition_value(self) -> None:
        """1969-12 is month -1 (floored epoch months), and a null
        partition source groups into the null partition."""
        pre_epoch = -86_400_000_000  # 1969-12-31
        records = [
            (keyspace.RowKey(team_group(7), 0), event_payload(7, pre_epoch)),
            (
                keyspace.RowKey(team_group(7), 1),
                b'{"team_id": 7, "timestamp": null}',
            ),
        ]
        nullable = [COLUMNS_WIRE[0], {**COLUMNS_WIRE[1], "nullable": True}]
        plan = plan_for(records, info_for(table_wire(columns=nullable)))
        (commit,) = plan.commits
        files = commit.payload["appends"][0]["files"]
        assert {tuple(f["partition_values"]) for f in files} == {
            ("7", "-1"),
            ("7", None),
        }
        assert "idempotency_key" not in commit.payload  # minted at send time

    def test_the_same_inputs_build_identical_payloads(self) -> None:
        records = [
            (keyspace.RowKey(team_group(7), i), event_payload(7, JAN_US + i))
            for i in range(3)
        ]
        a = plan_for(records, info_for())
        b = plan_for(records, info_for())
        assert a.commits[0].payload == b.commits[0].payload
        assert [u.body for u in a.commits[0].uploads] == [
            u.body for u in b.commits[0].uploads
        ]

    def test_object_names_carry_the_build_token(self) -> None:
        records = [
            (keyspace.RowKey(team_group(7), 0), event_payload(7, JAN_US)),
            (keyspace.RowKey(team_group(7), 1), event_payload(7, DEC_US)),
        ]
        plan = plan_for(records, info_for(), ctx=ctx_for(build="tok"))
        (commit,) = plan.commits
        assert commit.uris == (
            f"{DATA_PATH}/data/{NAMESPACE}/{TABLE}/tok/0.parquet",
            f"{DATA_PATH}/data/{NAMESPACE}/{TABLE}/tok/1.parquet",
        )
        with pytest.raises(ValueError, match="path segment"):
            ctx_for(build="a/b")

    def test_a_fanout_over_the_cap_splits_into_commits_of_at_most_the_cap(
        self,
    ) -> None:
        months = [JAN_US, DEC_US, DEC_US - 40 * 86_400_000_000]
        records = [
            (keyspace.RowKey(team_group(7), i), event_payload(7, ts))
            for i, ts in enumerate(months)
        ]
        plan = plan_for(records, info_for(), max_files_per_commit=2)
        assert [len(c.uploads) for c in plan.commits] == [2, 1]
        assert plan.files == 3 and plan.records == 3
        assert sum(c.records for c in plan.commits) == 3
        messages = [c.payload["message"].split() for c in plan.commits]
        assert "commit=1/2" in messages[0] and "commit=2/2" in messages[1]
        with pytest.raises(ValueError):
            plan_for(records, info_for(), max_files_per_commit=0)

    def test_all_dropped_has_no_commits(self) -> None:
        records = [(keyspace.RowKey(team_group(7), 0), b"not json")]
        plan = plan_for(records, info_for())
        assert plan.commits == ()
        assert plan.dropped_records == (
            flush_mod.DroppedRecord(0, REASON_UNDECODABLE_PAYLOAD, partition=0),
        )

    def test_a_record_that_defeats_the_build_fails_the_plan_never_drops(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Encoder drift at the Arrow boundary is a millrace bug, not bad
        data: the plan raises (naming the record), so the decision fails
        with every row still staged instead of dropping a representable
        row."""
        real = flush_mod.JsonObjectDecoder.decode_record

        def drifting(self: Any, payload: bytes) -> Any:
            if payload == b"drift":
                return flush_mod.DecodedRecord(
                    values={"team_id": 2**63 + 5, "timestamp": JAN_US, "event": None},
                    dropped_keys=(),
                )
            return real(self, payload)

        monkeypatch.setattr(flush_mod.JsonObjectDecoder, "decode_record", drifting)
        records = [
            (keyspace.RowKey(team_group(7), 0), event_payload(7, JAN_US)),
            (keyspace.RowKey(team_group(7), 1), b"drift"),
            (keyspace.RowKey(team_group(7), 2), event_payload(7, JAN_US)),
        ]
        with pytest.raises(flush_mod.FlushError, match="offset 1 fails alone"):
            plan_for(records, info_for())

    def test_footer_stats_follow_the_realized_file_size(self) -> None:
        records = [(keyspace.RowKey(team_group(7), 0), event_payload(7, JAN_US))]
        big = plan_for(records, info_for(), footer_stats_min_bytes=1)
        small = plan_for(records, info_for(), footer_stats_min_bytes=1 << 30)
        (f_big,) = big.commits[0].payload["appends"][0]["files"]
        (f_small,) = small.commits[0].payload["appends"][0]["files"]
        assert {s["field_id"] for s in f_big["column_stats"]} == {1, 2, 3}
        assert "column_stats" not in f_small  # deferred: the hydrator backfills
        assert f_small["record_count"] == 1 and f_small["file_size_bytes"] > 0

    def test_files_are_zstd_compressed(self) -> None:
        """Compaction rewrites at zstd; writing snappy left millrace's
        files ~3x larger than their rewrite."""
        records = [(keyspace.RowKey(team_group(7), 0), event_payload(7, JAN_US))]
        (commit,) = plan_for(records, info_for()).commits
        (upload,) = commit.uploads
        meta = pq.ParquetFile(pa.BufferReader(upload.body)).metadata
        codecs = {
            meta.row_group(g).column(c).compression
            for g in range(meta.num_row_groups)
            for c in range(meta.num_columns)
        }
        assert codecs == {"ZSTD"}


class TestBatchBuild:
    """The Arrow batch build raises on encoder drift (coercion is total)."""

    def test_a_record_that_defeats_the_arrow_build_is_named(self) -> None:
        cols = columns()
        good = flush_mod.DecodedRecord(
            values={"team_id": 7, "timestamp": 5, "event": "x"}, dropped_keys=()
        )
        bad = flush_mod.DecodedRecord(
            values={"team_id": 2**63 + 5, "timestamp": 5, "event": None},
            dropped_keys=(),
        )
        rows = [
            (keyspace.RowKey(team_group(7), 0), b"payload-good", good),
            (keyspace.RowKey(team_group(7), 1), b"payload-bad", bad),
            (keyspace.RowKey(team_group(7), 2), b"payload-good-2", good),
        ]
        with pytest.raises(flush_mod.FlushError, match="offset 1 fails alone"):
            flush_mod._build_batch_or_raise(rows, cols)
        assert len(rows) == 3  # nothing is popped or dropped
        encoded = flush_mod._build_batch_or_raise([rows[0], rows[2]], cols)
        assert encoded.num_rows == 2

    def test_an_unisolatable_failure_is_a_loud_encoder_bug(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cols = columns()
        record = flush_mod.DecodedRecord(
            values={"team_id": 7, "timestamp": 5, "event": "x"}, dropped_keys=()
        )
        rows = [
            (keyspace.RowKey(team_group(7), 0), b"p", record),
            (keyspace.RowKey(team_group(7), 1), b"q", record),
        ]
        real = flush_mod._build_batch

        def flaky(records: Any, cols: Any) -> Any:
            if len(records) > 1:
                raise pa.ArrowInvalid("combination-only failure")
            return real(records, cols)

        monkeypatch.setattr(flush_mod, "_build_batch", flaky)
        with pytest.raises(flush_mod.FlushError, match="no single record"):
            flush_mod._build_batch_or_raise(rows, cols)


# =====================================================================================
# Component suite: real PartitionStage on memory:/// + scripted server
# =====================================================================================


@component
class TestHappyPath:
    async def test_flush_commits_the_wire_contract(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
    ) -> None:
        stage = await stage_factory()
        # Two months of one team's rows → one commit, two files.
        await staged(
            stage,
            fake_clock,
            TEAM,
            [
                (0, JAN_US),
                (1, JAN_US + 3_600_000_000),
                (2, JAN_US + 7_200_000_000),
                (3, DEC_US),
                (4, DEC_US + 3_600_000_000),
            ],
        )
        script_options(httpx_mock)
        capture = CommitCapture(httpx_mock, [commit_ok(50)])
        flusher = flusher_factory(store=store, cfg=make_config())

        report = await flush_group(flusher, stage, team_group(TEAM), "size")

        assert (report.outcome, report.records, report.files) == ("committed", 5, 2)
        assert (report.commits, report.snapshot_ids) == (1, (50,))
        assert report.staged_rows == 5
        assert report.complete

        (body,) = capture.payloads
        assert uuid.UUID(body["idempotency_key"]).version == 4
        assert body["read_snapshot"] == 88
        assert body["author"] == "millrace/events/0"
        append = body["appends"][0]
        assert (append["namespace"], append["table"]) == (NAMESPACE, TABLE)
        assert append["expected_table_uuid"] == TABLE_UUID
        files = append["files"]
        assert {tuple(f["partition_values"]) for f in files} == {
            (str(TEAM), month_wire(JAN_US)),
            (str(TEAM), month_wire(DEC_US)),
        }
        assert sum(f["record_count"] for f in files) == 5
        (build,) = {f["path"].split("/")[-2] for f in files}
        assert re.fullmatch(r"[0-9a-f]{32}", build)
        for f in files:
            assert f["path"].startswith(
                f"{DATA_PATH}/data/{NAMESPACE}/{TABLE}/{build}/"
            )
            assert f["file_size_bytes"] > 0 and f["footer_size"] > 0
        summary, offsets_line = body["message"].splitlines()
        assert summary.split(" ")[:4] == [
            "millrace=v2",
            f"table={TABLE_UUID}",
            f"group={team_label(7)}",
            "trigger=size",
        ]
        assert {"records=5", "dropped=0", "files=2", "commit=1/1"} <= set(
            summary.split(" ")
        )
        assert offsets_line == "offsets topic=events stage=0"

        # Settlement: the scanned rows are gone and so is the group.
        assert await staged_rows(stage, TEAM) == []
        assert stage.snapshot() == []

        jan = parquet_rows(store, files[0]["path"])
        assert [ts_micros(r["timestamp"]) for r in jan.to_pylist()] == [
            JAN_US,
            JAN_US + 3_600_000_000,
            JAN_US + 7_200_000_000,
        ]
        stats = flusher.stats()
        assert (stats.decisions_committed, stats.commits) == (1, 1)
        assert (stats.rows_published, stats.files_written) == (5, 2)
        assert (stats.commit_attempts, stats.commit_retries) == (1, 0)
        assert stats.bytes_written == sum(f["file_size_bytes"] for f in files)

    async def test_flush_via_runner_sweeps_claimed_partitions(
        self,
        httpx_mock: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        cfg = make_config()
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage = await manager.open_partition("events", 0)
            await staged(stage, fake_clock, TEAM, [(0, JAN_US), (1, JAN_US)])
            script_options(httpx_mock)
            capture = CommitCapture(httpx_mock, [commit_ok(50)])
            flusher = flusher_factory(store=store, cfg=cfg)

            sweep = await runner(manager, flusher, cfg, fake_clock, sleeper).run_once(
                drain=True
            )

            assert (sweep.decisions, sweep.committed, sweep.failed) == (1, 1, 0)
            assert len(capture.bodies) == 1
            assert await staged_rows(stage, TEAM) == []

    async def test_a_group_that_is_not_due_is_left_alone(
        self,
        httpx_mock: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        cfg = make_config()
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage = await manager.open_partition("events", 0)
            await staged(stage, fake_clock, TEAM, [(0, JAN_US)])
            flusher = flusher_factory(store=store, cfg=cfg)
            r = runner(manager, flusher, cfg, fake_clock, sleeper)
            assert (await r.run_once()).decisions == 0
            assert r.oldest_due_staged_ts is None
            # Past the slow lane it is due, whatever its size.
            fake_clock.advance(cfg.slow_lane_deadline_s * 1_000_000)
            script_options(httpx_mock)
            CommitCapture(httpx_mock, [commit_ok(50)])
            assert (await r.run_once()).committed == 1


# =====================================================================================
# Staging groups are partition tuples
# =====================================================================================


def _keyed_records(keyer: Any, rows: list[tuple[int, int, int]]) -> list[Any]:
    """Stage-ready records for (offset, team, payload ts_us) triples,
    grouped by ``keyer`` exactly as the consumer does."""
    from millrace.consumer import classify_message

    out = []
    for offset, team, ts in rows:
        record = classify_message(keyer, offset=offset, value=event_payload(team, ts))
        assert isinstance(record, StagedRecord)
        out.append(record)
    return out


@component
class TestPartitionTupleGroups:
    async def test_one_flush_per_group_writes_exactly_one_file_under_a_matching_spec(
        self,
        httpx_mock: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """Two teams × two months, keyed by the live spec's keyer: four
        groups, four decisions, and every commit registers exactly ONE
        file whose ``partition_values`` ARE the staged group."""
        from millrace.grouping import GroupKeyer

        keyer = GroupKeyer(info_for())
        assert keyer.fields == SPEC_FIELDS
        cfg = make_config()
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage = await manager.open_partition("events", 0)
            await stage.stage_batch(
                _keyed_records(
                    keyer,
                    [
                        (0, 11, JAN_US),
                        (1, 22, DEC_US),
                        (2, 11, DEC_US),
                        (3, 22, JAN_US),
                        (4, 11, JAN_US + 1),
                        (5, 22, DEC_US + 1),
                    ],
                ),
                now_us=fake_clock(),
            )
            groups = {s.group for s in stage.snapshot()}
            assert groups == {
                ("11", month_wire(JAN_US)),
                ("11", month_wire(DEC_US)),
                ("22", month_wire(JAN_US)),
                ("22", month_wire(DEC_US)),
            }
            script_options(httpx_mock)
            capture = CommitCapture(httpx_mock, [commit_ok(50)])
            flusher = flusher_factory(store=store, cfg=cfg)
            sweep = await runner(manager, flusher, cfg, fake_clock, sleeper).run_once(
                drain=True
            )
            assert (sweep.decisions, sweep.committed, sweep.failed) == (4, 4, 0)
            # One batched commit: one file per group.
            (body,) = capture.payloads
            files = body["appends"][0]["files"]
            assert len(files) == 4
            committed = {tuple(f["partition_values"]) for f in files}
            assert "groups=4" in body["message"].split()
            assert committed == groups
            assert stage.snapshot() == []

    async def test_a_group_keyed_under_an_older_spec_still_fans_out_by_the_live_one(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
    ) -> None:
        """Rows keyed under ``identity(team_id)`` alone (an older spec)
        flush as ONE decision whose fanout splits them by the live spec,
        and the group is labelled positionally."""
        from millrace.grouping import GroupKeyer

        old = info_for(
            table_wire(
                spec={
                    "spec_id": 2,
                    "fields": [{"source_field_id": 1, "transform": "identity"}],
                }
            )
        )
        stage = await stage_factory()
        await stage.stage_batch(
            _keyed_records(GroupKeyer(old), [(0, TEAM, JAN_US), (1, TEAM, DEC_US)]),
            now_us=fake_clock(),
        )
        assert [s.group for s in stage.snapshot()] == [(str(TEAM),)]
        script_options(httpx_mock)
        capture = CommitCapture(httpx_mock, [commit_ok(50)])
        flusher = flusher_factory(store=store, cfg=make_config())
        report = await flush_group(flusher, stage, (str(TEAM),), "size")
        assert (report.outcome, report.records, report.files) == ("committed", 2, 2)
        body = capture.payloads[0]
        assert {tuple(f["partition_values"]) for f in body["appends"][0]["files"]} == {
            (str(TEAM), month_wire(JAN_US)),
            (str(TEAM), month_wire(DEC_US)),
        }
        assert f"group=key_0={TEAM}" in body["message"].split()
        assert stage.snapshot() == []


# =====================================================================================
# The commit ladder
# =====================================================================================


@component
class TestCommitLadder:
    async def _setup(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        steps: list[Any],
    ) -> tuple[Any, CommitCapture, HoglakeFlusher]:
        stage = await stage_factory()
        await staged(stage, fake_clock, TEAM, [(0, JAN_US), (1, JAN_US), (2, JAN_US)])
        script_options(httpx_mock)
        capture = CommitCapture(httpx_mock, steps)
        flusher = flusher_factory(store=store, cfg=make_config())
        return stage, capture, flusher

    async def test_commit_conflict_resends_the_identical_request(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        stage, capture, flusher = await self._setup(
            httpx_mock,
            stage_factory,
            fake_clock,
            flusher_factory,
            store,
            [
                commit_refusal(409, "commit_conflict", "the read set moved"),
                commit_ok(50),
            ],
        )
        report = await flush_group(flusher, stage, team_group(TEAM), "size")
        assert report.outcome == "committed"
        assert len(capture.bodies) == 2
        assert capture.bodies[0] == capture.bodies[1]  # same key, same bytes
        assert sleeper.calls == [0.5]
        stats = flusher.stats()
        assert (stats.commit_retries, stats.commit_attempts) == (1, 2)
        assert await staged_rows(stage, TEAM) == []

    async def test_commit_conflict_exhaustion_fails_the_decision_rows_stay(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        stage, capture, flusher = await self._setup(
            httpx_mock,
            stage_factory,
            fake_clock,
            flusher_factory,
            store,
            [commit_refusal(409, "commit_conflict", "moved")] * 5,
        )
        with pytest.raises(CommitExhausted):
            await flush_group(flusher, stage, team_group(TEAM), "size")
        assert len(capture.bodies) == 5
        assert len(set(capture.bodies)) == 1
        assert sleeper.calls == [0.5, 1.0, 2.0, 4.0]
        assert len(await staged_rows(stage, TEAM)) == 3
        assert stage.has_group(team_group(TEAM))
        # The ladder ran out: the commit may have landed, so its objects
        # are ambiguous, never counted as orphans.
        assert flusher.stats().orphaned_uploads == 0
        assert flusher.stats().ambiguous_uploads == 1

    async def test_transient_answers_resend_identically_until_the_ladder_ends(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        from millrace.flush import _PUBLISH_MAX_ATTEMPTS

        steps: list[Any] = [
            commit_refusal(500, "internal", "boom"),
            commit_refusal(408, "timeout", "slow"),
            commit_refusal(429, "throttled", "slow down"),
            httpx.ConnectError("reset"),
        ]
        steps += [httpx.ReadTimeout("lost")] * (_PUBLISH_MAX_ATTEMPTS - len(steps))
        stage, capture, flusher = await self._setup(
            httpx_mock, stage_factory, fake_clock, flusher_factory, store, steps
        )
        with pytest.raises(CommitExhausted):
            await flush_group(flusher, stage, team_group(TEAM), "size")
        assert len(capture.bodies) == _PUBLISH_MAX_ATTEMPTS
        assert len(set(capture.bodies)) == 1
        assert len(await staged_rows(stage, TEAM)) == 3
        assert (
            flusher.stats().ambiguous_uploads,
            flusher.stats().orphaned_uploads,
        ) == (1, 0)

    async def test_503_backs_off_per_retry_after(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        busy = commit_refusal(
            503,
            "commit_queue_timeout",
            "admission timed out",
            headers={"Retry-After": "2"},
        )
        stage, capture, flusher = await self._setup(
            httpx_mock,
            stage_factory,
            fake_clock,
            flusher_factory,
            store,
            [busy, busy, commit_ok(50)],
        )
        report = await flush_group(flusher, stage, team_group(TEAM), "size")
        assert report.outcome == "committed"
        assert len(set(capture.bodies)) == 1
        # Retry-After floors the backoff above the curve's 0.5s/1.0s.
        assert sleeper.calls == [2.0, 2.0]

    async def test_a_stray_retry_after_on_another_status_is_ignored(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        stage, _capture, flusher = await self._setup(
            httpx_mock,
            stage_factory,
            fake_clock,
            flusher_factory,
            store,
            [
                commit_refusal(500, "internal", "boom", headers={"Retry-After": "99"}),
                commit_ok(50),
            ],
        )
        await flush_group(flusher, stage, team_group(TEAM), "size")
        assert sleeper.calls == [0.5]

    @pytest.mark.parametrize(
        ("status", "code"),
        [
            (400, "bad_request"),
            (401, "unauthorized"),
            (403, "forbidden"),
            (404, "not_found"),
            (413, "too_large"),
            (422, "validation"),
            (422, "record_validation"),
        ],
    )
    async def test_an_answered_4xx_fails_the_decision_with_rows_staged(
        self,
        status: int,
        code: str,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """No answered refusal deletes staged rows and none is retried in
        place (the runner's per-group backoff owns the retry)."""
        stage, capture, flusher = await self._setup(
            httpx_mock,
            stage_factory,
            fake_clock,
            flusher_factory,
            store,
            [commit_refusal(status, code, "answered")],
        )
        with pytest.raises(CommitRefused) as excinfo:
            await flush_group(flusher, stage, team_group(TEAM), "size")
        assert excinfo.value.status == status
        assert len(capture.bodies) == 1
        assert sleeper.calls == []
        assert len(await staged_rows(stage, TEAM)) == 3
        stats = flusher.stats()
        assert stats.commit_refusals == {str(status): 1}
        assert stats.records_dropped == {}
        assert stats.orphaned_uploads == 1

    async def test_table_dropped_409_is_a_refusal_not_contention(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        stage, capture, flusher = await self._setup(
            httpx_mock,
            stage_factory,
            fake_clock,
            flusher_factory,
            store,
            [commit_refusal(409, "table_dropped", "dropped at snapshot 91")],
        )
        with pytest.raises(CommitRefused, match="dropped"):
            await flush_group(flusher, stage, team_group(TEAM), "size")
        assert len(capture.bodies) == 1
        assert sleeper.calls == []
        assert len(await staged_rows(stage, TEAM)) == 3

    async def test_a_commit_answered_table_recreated_halts(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        stage, capture, flusher = await self._setup(
            httpx_mock,
            stage_factory,
            fake_clock,
            flusher_factory,
            store,
            [commit_refusal(409, "table_recreated", "new incarnation")],
        )
        with pytest.raises(FlushHalted) as excinfo:
            await flush_group(flusher, stage, team_group(TEAM), "size")
        assert excinfo.value.reason == "table_recreated"
        assert len(capture.bodies) == 1
        assert len(await staged_rows(stage, TEAM)) == 3

    async def test_a_refused_basis_three_times_fails_the_decision(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        ddl = commit_refusal(409, "ddl_since_read_snapshot", "spec moved")
        stage, capture, flusher = await self._setup(
            httpx_mock,
            stage_factory,
            fake_clock,
            flusher_factory,
            store,
            [ddl, ddl, ddl],
        )
        script_table_refresh(httpx_mock, table=table_wire(read_snapshot_id=95))
        script_table_refresh(httpx_mock, table=table_wire(read_snapshot_id=96))
        with pytest.raises(flush_mod.FlushError, match="read basis refused 3 times"):
            await flush_group(flusher, stage, team_group(TEAM), "size")
        assert [p["read_snapshot"] for p in capture.payloads] == [88, 95, 96]
        assert len({p["idempotency_key"] for p in capture.payloads}) == 3
        assert len(await staged_rows(stage, TEAM)) == 3


# =====================================================================================
# Orphan accounting, drops and dropped keys
# =====================================================================================


@component
class TestOrphansAndDrops:
    async def test_mid_fanout_upload_failure_counts_and_names_orphans(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        stage = await stage_factory()
        await staged(stage, fake_clock, TEAM, [(0, JAN_US), (1, DEC_US)])
        script_options(httpx_mock)
        capture = CommitCapture(httpx_mock, [])  # no commit is reached
        flusher = flusher_factory(store=store, cfg=make_config())
        store.fail_at = 1

        with (
            caplog.at_level(logging.WARNING),
            pytest.raises(OSError, match="object store refused"),
        ):
            await flush_group(flusher, stage, team_group(TEAM), "size")

        assert flusher.stats().orphaned_uploads == 1
        (orphaned,) = [k for k in store.files if k.endswith("/0.parquet")]
        assert any(
            "orphaned 1 uploaded parquet" in r.getMessage()
            and orphaned in r.getMessage()
            for r in caplog.records
        )
        assert capture.bodies == []
        assert len(await staged_rows(stage, TEAM)) == 2

        # The retry rebuilds with FRESH object names; the orphan is untouched.
        store.fail_at = None
        capture.extend([commit_ok(50)])
        report = await flush_group(flusher, stage, team_group(TEAM), "size")
        assert report.outcome == "committed"
        paths = [f["path"] for f in capture.payloads[0]["appends"][0]["files"]]
        assert f"s3://{orphaned}" not in paths
        assert orphaned in store.files
        assert len(store.opened) == len(set(store.opened))
        assert await staged_rows(stage, TEAM) == []

    async def test_bad_records_are_dropped_counted_and_logged_good_rows_flush(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        stage = await stage_factory()
        payloads = [
            event_payload(TEAM, JAN_US),  # good
            b"this is not json",  # undecodable
            event_payload(TEAM, JAN_US, event=None),  # null into nullable is fine
            b'{"timestamp": 5}',  # team_id missing (non-nullable)
            b'{"team_id": "seven", "timestamp": 5}',  # wrong type
            b'{"team_id": 7, "timestamp": 5, "event": "x", "junk": NaN}',  # strict JSON
        ]
        await staged(
            stage,
            fake_clock,
            TEAM,
            [(i, JAN_US + i) for i in range(len(payloads))],
            payloads=payloads,
        )
        script_options(httpx_mock)
        capture = CommitCapture(
            httpx_mock, [commit_ok(50), commit_ok(51), commit_ok(52)]
        )
        flusher = flusher_factory(store=store, cfg=make_config())

        with caplog.at_level(logging.WARNING):
            report = await flush_group(flusher, stage, team_group(TEAM), "size")

        assert (report.outcome, report.records, report.dropped) == ("committed", 2, 4)
        assert flusher.stats().records_dropped == {
            REASON_UNDECODABLE_PAYLOAD: 2,
            REASON_UNCASTABLE_COLUMN: 2,
        }
        lines = [
            r.getMessage() for r in caplog.records if "flush dropped" in r.getMessage()
        ]
        assert len(lines) == 2  # one line per reason per decision
        assert any("first_path=team_id (offset 0:3)" in line for line in lines)
        files = capture.payloads[0]["appends"][0]["files"]
        assert sum(f["record_count"] for f in files) == 2
        assert await staged_rows(stage, TEAM) == []

        # An unknown payload key is dropped from the row, warned once.
        surprise = (
            b'{"team_id": 7, "timestamp": ' + str(JAN_US).encode() + b', "surprise": 1}'
        )
        for offset in (6, 7):
            await staged(
                stage, fake_clock, TEAM, [(offset, JAN_US)], payloads=[surprise]
            )
            with caplog.at_level(logging.WARNING):
                await flush_group(flusher, stage, team_group(TEAM), "size")
        assert flusher.stats().dropped_payload_keys == 2
        warned = [
            r for r in caplog.records if "'surprise' is not a column" in r.getMessage()
        ]
        assert len(warned) == 1
        assert "surprise" not in str(capture.payloads[1])

    async def test_magnitude_junk_clamps_timestamps_and_drops_floats(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
    ) -> None:
        """A timestamp carrying 2**63+5 is clamped to 9999-12-31 and
        written; a float carrying the overflow literal 1e400 or 10**400
        cannot be represented and is dropped and counted."""
        wire = COLUMNS_WIRE + [
            {
                "name": "t2",
                "type": "timestamp",
                "field_id": 4,
                "ordinal": 3,
                "nullable": True,
            },
            {
                "name": "score",
                "type": "double",
                "field_id": 5,
                "ordinal": 4,
                "nullable": True,
            },
        ]
        stage = await stage_factory()
        good = event_payload(TEAM, JAN_US, t2=946_684_800_000_000, score=1.5)
        payloads = [
            good,
            event_payload(TEAM, JAN_US, t2=2**63 + 5),
            b'{"team_id":7,"timestamp":'
            + str(JAN_US).encode()
            + b',"event":"x","score":1e400}',
            event_payload(TEAM, JAN_US, score=10**400),
        ]
        await staged(
            stage,
            fake_clock,
            TEAM,
            [(i, JAN_US + i) for i in range(len(payloads))],
            payloads=payloads,
        )
        script_options(httpx_mock)
        capture = CommitCapture(httpx_mock, [commit_ok(50)])
        flusher = flusher_factory(
            store=store, cfg=make_config(), table=table_wire(columns=wire)
        )

        report = await flush_group(flusher, stage, team_group(TEAM), "size")

        from millrace.coerce import TIMESTAMP_MAX_US

        assert (report.outcome, report.records, report.dropped) == ("committed", 2, 2)
        assert flusher.stats().records_dropped == {REASON_UNCASTABLE_COLUMN: 2}
        assert flusher.stats().values_clamped == {"t2": 1}
        (f,) = capture.payloads[0]["appends"][0]["files"]
        rows = parquet_rows(store, f["path"]).to_pylist()
        assert sorted(r["score"] or 0 for r in rows) == [0, 1.5]
        assert sorted(ts_micros(r["t2"]) for r in rows) == [
            946_684_800_000_000,
            TIMESTAMP_MAX_US,
        ]
        assert await staged_rows(stage, TEAM) == []

    async def test_footer_stats_ship_for_files_at_the_threshold(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        stage = await stage_factory()
        await staged(stage, fake_clock, TEAM, [(0, JAN_US), (1, JAN_US + 1)])
        await staged(stage, fake_clock, TEAM + 1, [(2, JAN_US)])
        script_options(httpx_mock)
        capture = CommitCapture(httpx_mock, [commit_ok(50), commit_ok(51)])
        flusher = flusher_factory(store=store, cfg=make_config())
        # A small file at the production threshold registers deferred.
        await flush_group(flusher, stage, team_group(TEAM + 1), "age")
        (small,) = capture.payloads[0]["appends"][0]["files"]
        assert "column_stats" not in small
        # Lower the threshold below the realized size: full footer stats.
        monkeypatch.setattr(flush_mod, "FOOTER_STATS_MIN_BYTES", 1)
        await flush_group(flusher, stage, team_group(TEAM), "slow")
        (big,) = capture.payloads[1]["appends"][0]["files"]
        stats = {s["field_id"]: s for s in big["column_stats"]}
        assert set(stats) == {1, 2, 3}
        import base64

        assert base64.b64decode(stats[1]["lower_bound"]) == (7).to_bytes(8, "little")
        key = big["path"].removeprefix("s3://")
        metadata = pq.read_metadata(pa.BufferReader(store.files[key]))
        assert metadata.row_group(0).column(0).statistics.min == 7


# =====================================================================================
# Wire discipline: TTL, totals=false, mid-flush arrivals
# =====================================================================================


@component
class TestWireDiscipline:
    async def test_shape_cache_ttl_is_a_quarter_of_retention(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        mono: FakeMonotonic,
    ) -> None:
        """retention 3600s → TTL 900s: inside it no table read; past it a
        totals=false re-read whose read_snapshot the next commit carries.
        The retention options are read once."""
        stage = await stage_factory()
        script_options(httpx_mock)
        capture = CommitCapture(
            httpx_mock, [commit_ok(50), commit_ok(51), commit_ok(52)]
        )
        flusher = flusher_factory(store=store, cfg=make_config())
        offset = 0
        for advance in (0, 0, RETENTION_S / 4 + 1):
            await staged(
                stage, fake_clock, TEAM, [(offset, JAN_US), (offset + 1, JAN_US)]
            )
            offset += 2
            if advance:
                mono.advance(advance)
                script_table_refresh(httpx_mock, table=table_wire(read_snapshot_id=95))
            report = await flush_group(flusher, stage, team_group(TEAM), "size")
            assert report.outcome == "committed"
        assert [p["read_snapshot"] for p in capture.payloads] == [88, 88, 95]
        table_reads = [
            r for r in httpx_mock.get_requests() if str(r.url).startswith(TABLE_URL)
        ]
        assert len(table_reads) == 2
        assert all(r.url.params.get("totals") == "false" for r in table_reads)
        assert (
            len(httpx_mock.get_requests(url=f"{BASE}/v1/catalogs/{CATALOG}/options"))
            == 1
        )
        for request in httpx_mock.get_requests():
            assert request.url.params.get("totals", "false") == "false"

    async def test_mid_flush_arrivals_stay_out_of_the_window(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A batch landing after the scan is neither flushed nor deleted
        (exact-key settle), and the group stays in the index for it."""
        stage = await stage_factory()
        await staged(stage, fake_clock, TEAM, [(0, JAN_US), (1, JAN_US), (2, JAN_US)])
        script_options(httpx_mock)
        capture = CommitCapture(httpx_mock, [commit_ok(50), commit_ok(51)])
        flusher = flusher_factory(store=store, cfg=make_config())

        real_scan = stage.scan
        arrived = False

        async def scan_then_arrival(group: Any, **kwargs: Any) -> Any:
            nonlocal arrived
            scan = await real_scan(group, **kwargs)
            if not arrived:
                arrived = True
                await staged(stage, fake_clock, TEAM, [(3, JAN_US), (4, JAN_US)])
            return scan

        monkeypatch.setattr(stage, "scan", scan_then_arrival)
        report = await flush_group(flusher, stage, team_group(TEAM), "size")
        assert report.staged_rows == 3  # offsets 0-2: the arrivals stay out
        assert capture.payloads[0]["message"].splitlines()[1] == (
            "offsets topic=events stage=0"
        )
        assert [rk.offset for rk, _ in await staged_rows(stage, TEAM)] == [3, 4]
        assert stage.has_group(team_group(TEAM))  # a later write kept it indexed
        report2 = await flush_group(flusher, stage, team_group(TEAM), "size")
        assert report2.staged_rows == 2
        assert await staged_rows(stage, TEAM) == []
        assert not stage.has_group(team_group(TEAM))

    async def test_a_capped_scan_flushes_an_exact_offset_prefix(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
    ) -> None:
        stage = await stage_factory()
        await staged(stage, fake_clock, TEAM, [(o, JAN_US + o) for o in range(5)])
        (_, one), *_ = await staged_rows(stage, TEAM)
        script_options(httpx_mock)
        capture = CommitCapture(httpx_mock, [commit_ok(50 + i) for i in range(3)])
        # Room for two rows per decision.
        flusher = flusher_factory(
            store=store, cfg=make_config(flush_bytes=2 * len(one))
        )
        windows = []
        while stage.has_group(team_group(TEAM)):
            before = [rk.offset for rk, _ in await staged_rows(stage, TEAM)]
            report = await flush_group(flusher, stage, team_group(TEAM), "size")
            after = [rk.offset for rk, _ in await staged_rows(stage, TEAM)]
            windows.append((sorted(set(before) - set(after)), report.complete))
        assert windows == [([0, 1], False), ([2, 3], False), ([4], True)]
        assert len(capture.payloads) == 3


# =====================================================================================
# The runner
# =====================================================================================


@component
class TestRunner:
    async def test_one_groups_failure_never_wedges_the_sweep(
        self,
        httpx_mock: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
        mono: FakeMonotonic,
    ) -> None:
        """Team 11's ladder exhausts (contained, backed off); team 22
        commits in the same sweep; team 11 retries once its backoff ends
        and lands (its first attempts may have landed too: a duplicate)."""
        from millrace.flush import _PUBLISH_MAX_ATTEMPTS

        cfg = make_config(flush_concurrency=1)
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage = await manager.open_partition("events", 0)
            await staged(stage, fake_clock, 11, [(0, JAN_US)])
            fake_clock.advance(1_000_000)
            await staged(stage, fake_clock, 22, [(1, JAN_US)])
            script_options(httpx_mock)
            capture = CommitCapture(
                httpx_mock,
                [httpx.ConnectError("lost")] * _PUBLISH_MAX_ATTEMPTS
                + [commit_ok(50), commit_ok(51)],
            )
            flusher = flusher_factory(store=store, cfg=cfg)
            r = runner(
                manager,
                flusher,
                cfg,
                fake_clock,
                sleeper,
                mono=mono,
                concurrency=1,
                batch_max_groups=1,
            )

            sweep = await r.run_once(drain=True)
            assert (sweep.decisions, sweep.committed, sweep.failed) == (2, 1, 1)
            assert [rk.offset for rk, _ in await staged_rows(stage, 11)] == [0]
            assert await staged_rows(stage, 22) == []
            assert flusher.stats().decisions_failed == 1

            # Inside the backoff nothing is retried...
            assert (await r.run_once(drain=True)).decisions == 0
            # ...past it the group rebuilds under a fresh key and lands.
            mono.advance(60)
            sweep2 = await r.run_once()
            assert (sweep2.decisions, sweep2.committed) == (1, 1)
            assert (
                capture.payloads[-1]["idempotency_key"]
                != (capture.payloads[0]["idempotency_key"])
            )
            assert await staged_rows(stage, 11) == []
            assert r.stuck_groups == 0

    async def test_halt_is_sticky(
        self,
        httpx_mock: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        cfg = make_config()
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage = await manager.open_partition("events", 0)
            await staged(stage, fake_clock, TEAM, [(0, JAN_US)])
            script_options(httpx_mock)
            capture = CommitCapture(
                httpx_mock, [commit_refusal(409, "table_recreated", "recreated")]
            )
            flusher = flusher_factory(store=store, cfg=cfg)
            r = runner(manager, flusher, cfg, fake_clock, sleeper)
            with pytest.raises(FlushHalted) as first:
                await r.run_once(drain=True)
            assert r.halted is first.value
            with pytest.raises(FlushHalted) as again:
                await r.run_once(drain=True)
            assert again.value is first.value
            with pytest.raises(FlushHalted):
                await r.drain()
            with pytest.raises(FlushHalted):
                await r.run()
            assert len(capture.bodies) == 1
            assert [rk.offset for rk, _ in await staged_rows(stage, TEAM)] == [0]

    async def test_run_sweeps_until_stopped(
        self,
        httpx_mock: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
    ) -> None:
        cfg = make_config(flush_bytes=1)
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage = await manager.open_partition("events", 0)
            await staged(stage, fake_clock, TEAM, [(0, JAN_US)])
            script_options(httpx_mock)
            capture = CommitCapture(httpx_mock, [commit_ok(50)])
            flusher = flusher_factory(store=store, cfg=cfg)
            cadence: list[float] = []
            r: FlushRunner | None = None

            async def stop_after_first_tick(seconds: float) -> None:
                cadence.append(seconds)
                assert r is not None
                r.stop()

            r = FlushRunner(
                manager,
                flusher,
                cfg.planner_knobs(),
                now_us=fake_clock,
                sleep=stop_after_first_tick,
                sweep_s=60.0,
            )
            await r.run()  # in-flight flushes finish on the way out
            assert r.sweeps == 1
            assert cadence == [60.0]
            assert len(capture.bodies) == 1
            assert await staged_rows(stage, TEAM) == []

    @pytest.mark.parametrize("concurrency", [1, 3])
    async def test_the_semaphore_bounds_decisions_in_flight(
        self,
        concurrency: int,
        fake_clock: Any,
        sleeper: Sleeper,
    ) -> None:
        cfg = make_config()
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            for p in range(3):
                stage = await manager.open_partition("events", p)
                for t in range(4):
                    await staged(stage, fake_clock, p * 4 + t, [(t, JAN_US)])
            gate = asyncio.Event()
            inflight = peak = 0
            calls: list[Any] = []

            class Gated:
                def note_failure(self) -> None:
                    raise AssertionError("no failure expected")

                async def flush_batch(
                    self, stage: Any, scans: Any, trigger: str
                ) -> Any:
                    nonlocal inflight, peak
                    inflight += 1
                    peak = max(peak, inflight)
                    calls.extend((stage.partition, scan.group) for scan in scans)
                    try:
                        await gate.wait()
                    finally:
                        inflight -= 1
                    return BatchReport(
                        tuple(
                            FlushReport(
                                stage.topic,
                                stage.partition,
                                scan.group,
                                trigger,
                                "committed",
                            )
                            for scan in scans
                        )
                    )

            r = runner(
                manager,
                Gated(),
                cfg,
                fake_clock,
                sleeper,
                concurrency=concurrency,
                batch_max_groups=1,
            )
            task = asyncio.create_task(r.run_once(drain=True))
            for _ in range(200):
                await asyncio.sleep(0.005)
                if peak >= concurrency:
                    break
            await asyncio.sleep(0.02)
            assert peak == concurrency
            gate.set()
            sweep = await asyncio.wait_for(task, timeout=30)
            assert (sweep.decisions, sweep.committed) == (12, 12)
            assert len(set(calls)) == 12


# =====================================================================================
# Fencing: a newer writer owns a stage's path
# =====================================================================================


@component
class TestFencedFlusher:
    async def test_a_fence_surfacing_mid_decision_latches_fenced_error(
        self, fake_clock: Any, sleeper: Sleeper
    ) -> None:
        cfg = make_config()

        class Fenced:
            calls = 0

            def note_failure(self) -> None:
                raise AssertionError("a fence is not a failure")

            async def flush_batch(self, stage: Any, scans: Any, trigger: str) -> Any:
                Fenced.calls += 1
                raise SlateError.Closed(CloseReason.FENCED, "detected newer DB client")

        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage = await manager.open_partition("events", 0)
            await staged(stage, fake_clock, TEAM, [(0, JAN_US)])
            r = runner(manager, Fenced(), cfg, fake_clock, sleeper)
            with pytest.raises(FencedError) as first:
                await r.run_once(drain=True)
            assert first.value.partitions == (("events", 0),)
            assert r.halted is first.value
            with pytest.raises(FencedError):
                await r.run_once(drain=True)
            assert Fenced.calls == 1  # terminal: never planned again

    async def test_a_non_fence_close_is_an_ordinary_failure(
        self, fake_clock: Any, sleeper: Sleeper, mono: FakeMonotonic
    ) -> None:
        cfg = make_config()
        failures: list[int] = []

        class Closed:
            def note_failure(self) -> None:
                failures.append(1)

            async def flush_batch(self, stage: Any, scans: Any, trigger: str) -> Any:
                raise SlateError.Closed(CloseReason.CLEAN, "shut down")

        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage = await manager.open_partition("events", 0)
            await staged(stage, fake_clock, TEAM, [(0, JAN_US)])
            r = runner(manager, Closed(), cfg, fake_clock, sleeper, mono=mono)
            sweep = await r.run_once(drain=True)
            assert sweep.failed == 1 and failures == [1]
            assert r.halted is None

    @pytest.mark.integration
    async def test_a_real_contested_open_fences_the_runner(
        self,
        tmp_path: Any,
        httpx_mock: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """End to end on ``file:///``: a second writer opens partition 0's
        path; the runner latches FencedError naming it, and partition 0's
        staged row survives for the new owner."""
        from slatedb.uniffi import DbBuilder, KeyRange, ObjectStore

        cfg = make_config()
        url = "file:///"
        base = str(tmp_path / "slatedb")
        async with StageManager(url, base, settings=fast_flush_settings()) as mgr:
            s0 = await mgr.open_partition("events", 0)
            await staged(s0, fake_clock, TEAM, [(0, JAN_US)])
            from flushkit import OPTIONS_URL, options_wire

            httpx_mock.add_response(
                method="GET", url=OPTIONS_URL, json=options_wire(), is_optional=True
            )
            # Whether the fenced stage's scan or its settle detects the
            # fence, at most one commit goes out (a duplicate at worst).
            CommitCapture(httpx_mock, [commit_ok(50)])
            flusher = flusher_factory(store=store, cfg=cfg)
            r = runner(mgr, flusher, cfg, fake_clock, sleeper)

            rival = await DbBuilder(
                mgr.path_for("events", 0), ObjectStore.resolve(url)
            ).build()
            with pytest.raises(SlateError.Closed):
                await s0.stage_batch(
                    [StagedRecord(group=team_group(TEAM), offset=1, payload=b"x")],
                    now_us=fake_clock(),
                )
            with pytest.raises(FencedError) as fenced:
                await r.run_once(drain=True)
            assert fenced.value.partitions == (("events", 0),)

            it = await rival.scan_prefix(
                b"rows/",
                KeyRange(
                    start=None, start_inclusive=False, end=None, end_inclusive=False
                ),
            )
            survivors = []
            while batch := await it.next_batch(16):
                survivors.extend(kv.key for kv in batch)
            assert len(survivors) >= 1
            await rival.shutdown()


@component
@component
class TestShapeReader:
    """The consumer's view of the destination (``flush.ShapeReader``):
    pinned to the flusher's incarnation, on the flusher's TTL."""

    async def test_a_reread_returns_the_live_shape_and_the_ttl_is_retention_over_4(
        self, httpx_mock: Any, flusher_factory: Any, store: FakeObjectStore
    ) -> None:
        flusher = flusher_factory(store=store)
        reader = flusher.shape_reader()
        script_options(httpx_mock)
        assert reader.ttl_s() == RETENTION_S / 4
        moved = {
            "spec_id": 4,
            "fields": [{"source_field_id": 1, "transform": "identity"}],
        }
        script_table_refresh(
            httpx_mock, table=table_wire(read_snapshot_id=95, spec=moved)
        )
        info = reader.read()
        assert (info.read_snapshot_id, info.partition_spec.spec_id) == (95, 4)
        # The flusher's own shape cache is untouched by the reader.
        assert flusher.resolved_info.partition_spec.spec_id == 3

    async def test_another_incarnation_halts_table_recreated(
        self, httpx_mock: Any, flusher_factory: Any, store: FakeObjectStore
    ) -> None:
        reader = flusher_factory(store=store).shape_reader()
        script_table_refresh(httpx_mock, table=table_wire(uuid=RECREATED_UUID))
        with pytest.raises(FlushHalted) as excinfo:
            reader.read()
        assert excinfo.value.reason == "table_recreated"
        assert RECREATED_UUID in excinfo.value.detail

    async def test_a_read_without_read_snapshot_id_halts_server_capability(
        self, httpx_mock: Any, flusher_factory: Any, store: FakeObjectStore
    ) -> None:
        reader = flusher_factory(store=store).shape_reader()
        script_table_refresh(httpx_mock, table=table_wire(read_snapshot_id=None))
        with pytest.raises(FlushHalted) as excinfo:
            reader.read()
        assert excinfo.value.reason == "server_capability"

    async def test_a_failed_read_is_transient_and_the_next_read_succeeds(
        self, httpx_mock: Any, flusher_factory: Any, store: FakeObjectStore
    ) -> None:
        """A transport-level refusal is an ordinary error (not a halt —
        the consumer keeps its old keyer on it); the reader holds no
        state that a failure could poison."""
        from pyhoglake import HoglakeError

        reader = flusher_factory(store=store).shape_reader()
        httpx_mock.add_response(
            method="GET",
            url=f"{TABLE_URL}?totals=false",
            status_code=404,
            json={"error": "not_found", "detail": "briefly gone"},
        )
        with pytest.raises(HoglakeError) as excinfo:
            reader.read()
        assert not isinstance(excinfo.value, FlushHalted)
        script_table_refresh(httpx_mock)
        assert reader.read().table_uuid == TABLE_UUID


# =====================================================================================
# Files are sorted by the catalog's sort spec (fields, directions, null
# placement) before the partition fanout: scan order is Kafka offset order,
# the spec is payload-event time
# =====================================================================================


@component
class TestSortSpec:
    def test_sort_keys_triple_form_is_supported_by_the_pyarrow_floor(self):
        """API-conformance pin for the pyarrow>=25.0 floor (pyproject.toml):
        pc.sort_indices accepts (name, direction, null_placement) triples.
        On pyarrow 21–24 this raises ValueError("too many values to
        unpack"), which would fail every sorted flush."""
        import pyarrow as pa
        import pyarrow.compute as pc

        table = pa.table({"a": [2, 1], "b": [3, 4]})
        order = pc.sort_indices(table, sort_keys=[("a", "ascending", "at_start")])
        assert table.take(order)["a"].to_pylist() == [1, 2]

    async def test_inverted_kafka_and_payload_timestamps_flush_key_nondecreasing(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """Offsets (the scan order) ASCEND through the batch while the
        payloads' event times DESCEND (late arrivals): the committed file
        is key-nondecreasing under the live spec (timestamp asc), not in
        scan order."""
        stage = await stage_factory()
        n = 6
        record_ts = [JAN_US + i * 1_000_000 for i in range(n)]  # ascending
        event_ts = [JAN_US + (n - 1 - i) * 1_000_000 for i in range(n)]  # descending
        await staged(
            stage,
            fake_clock,
            TEAM,
            list(enumerate(record_ts)),
            payloads=[event_payload(TEAM, ts) for ts in event_ts],
        )
        script_options(httpx_mock)
        capture = CommitCapture(httpx_mock, [commit_ok(50)])
        flusher = flusher_factory(
            store=store,
            table=table_wire(sort=SORT_WIRE),  # timestamp asc, nulls_first
            cfg=make_config(),
        )

        report = await flush_group(flusher, stage, team_group(TEAM), "size")

        assert report.outcome == "committed"
        assert report.files == 1  # one month partition
        (file,) = capture.payloads[0]["appends"][0]["files"]
        written = [
            ts_micros(r["timestamp"])
            for r in parquet_rows(store, file["path"]).to_pylist()
        ]
        assert written == sorted(event_ts)  # spec order, not scan order
        # The scan window and settlement are unaffected: all rows gone.
        assert await staged_rows(stage, TEAM) == []

    async def test_direction_and_null_placement_come_from_the_spec(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """desc + nulls_first: the exact file order pins BOTH mappings
        (a direction flip reds; a placement flip reds)."""
        stage = await stage_factory()
        nullable_ts = [
            COLUMNS_WIRE[0],
            {**COLUMNS_WIRE[1], "nullable": True},
            COLUMNS_WIRE[2],
        ]
        wire = table_wire(
            columns=nullable_ts,
            spec={"spec_id": 0, "fields": []},  # unpartitioned
            sort={
                "sort_id": 9,
                "fields": [
                    {
                        "source_field_id": 2,
                        "direction": "desc",
                        "null_order": "nulls_first",
                    }
                ],
            },
        )
        staged_ts = [JAN_US, None, JAN_US + 1_000_000, JAN_US - 1_000_000]
        payloads = [
            b'{"team_id": 7, "timestamp": null, "event": "x"}'
            if ts is None
            else event_payload(TEAM, ts)
            for ts in staged_ts
        ]
        # Scan order (record ts) is staging order — already neither
        # ascending nor descending, so the sort is what orders the file.
        await staged(
            stage,
            fake_clock,
            TEAM,
            [(i, JAN_US + i) for i in range(len(staged_ts))],
            payloads=payloads,
        )
        script_options(httpx_mock)
        capture = CommitCapture(httpx_mock, [commit_ok(50)])
        flusher = flusher_factory(store=store, table=wire, cfg=make_config())

        report = await flush_group(flusher, stage, team_group(TEAM), "size")

        assert report.outcome == "committed"
        (file,) = capture.payloads[0]["appends"][0]["files"]
        written = [
            None if r["timestamp"] is None else ts_micros(r["timestamp"])
            for r in parquet_rows(store, file["path"]).to_pylist()
        ]
        assert written == [None, JAN_US + 1_000_000, JAN_US, JAN_US - 1_000_000]

    async def test_multiple_sort_fields_apply_in_spec_order(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """Lexicographic over (event asc nulls_last, timestamp desc
        nulls_first): the second key only breaks ties."""
        stage = await stage_factory()
        wire = table_wire(
            sort={
                "sort_id": 10,
                "fields": [
                    {
                        "source_field_id": 3,
                        "direction": "asc",
                        "null_order": "nulls_last",
                    },
                    {
                        "source_field_id": 2,
                        "direction": "desc",
                        "null_order": "nulls_first",
                    },
                ],
            }
        )
        rows = [  # (event, payload ts) in staging order
            ("b", JAN_US),
            ("a", JAN_US + 1_000_000),
            ("b", JAN_US - 1_000_000),
            ("a", JAN_US + 2_000_000),
            (None, JAN_US + 3_000_000),
        ]
        payloads = [
            b'{"team_id": 7, "timestamp": %d, "event": %s}'
            % (ts, b"null" if event is None else b'"%b"' % event.encode())
            for event, ts in rows
        ]
        await staged(
            stage,
            fake_clock,
            TEAM,
            [(i, JAN_US + i) for i in range(len(rows))],
            payloads=payloads,
        )
        script_options(httpx_mock)
        capture = CommitCapture(httpx_mock, [commit_ok(50)])
        flusher = flusher_factory(store=store, table=wire, cfg=make_config())

        report = await flush_group(flusher, stage, team_group(TEAM), "size")

        assert report.outcome == "committed"
        (file,) = capture.payloads[0]["appends"][0]["files"]
        written = [
            (r["event"], ts_micros(r["timestamp"]))
            for r in parquet_rows(store, file["path"]).to_pylist()
        ]
        assert written == [
            ("a", JAN_US + 2_000_000),
            ("a", JAN_US + 1_000_000),
            ("b", JAN_US),
            ("b", JAN_US - 1_000_000),
            (None, JAN_US + 3_000_000),
        ]

    async def test_uuid_sort_keys_sort_on_the_storage_bytes(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """Arrow has no sort kernel for the uuid extension type: the
        sorter falls back to the STORAGE (fixed-size binary, unsigned
        lexicographic — the server comparator's own order for those
        bytes)."""
        import uuid as pyuuid

        stage = await stage_factory()
        columns = COLUMNS_WIRE + [
            {
                "name": "uid",
                "type": "uuid",
                "field_id": 4,
                "ordinal": 3,
                "nullable": False,
            }
        ]
        wire = table_wire(
            columns=columns,
            sort={
                "sort_id": 11,
                "fields": [
                    {
                        "source_field_id": 4,
                        "direction": "asc",
                        "null_order": "nulls_first",
                    }
                ],
            },
        )
        uids = [pyuuid.UUID(int=v) for v in (10, 2, 30)]
        payloads = [
            b'{"team_id": 7, "timestamp": %d, "event": "x", "uid": "%s"}'
            % (JAN_US, str(u).encode())
            for u in uids
        ]
        await staged(
            stage,
            fake_clock,
            TEAM,
            [(i, JAN_US + i) for i in range(len(uids))],
            payloads=payloads,
        )
        script_options(httpx_mock)
        capture = CommitCapture(httpx_mock, [commit_ok(50)])
        flusher = flusher_factory(store=store, table=wire, cfg=make_config())

        report = await flush_group(flusher, stage, team_group(TEAM), "size")

        assert report.outcome == "committed"
        (file,) = capture.payloads[0]["appends"][0]["files"]
        written = [r["uid"] for r in parquet_rows(store, file["path"]).to_pylist()]
        assert written == [pyuuid.UUID(int=2), pyuuid.UUID(int=10), pyuuid.UUID(int=30)]

    async def test_a_sort_spec_the_writer_cannot_honor_is_contained_not_a_halt(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
        mono: FakeMonotonic,
    ) -> None:
        """A shape refresh that serves a sort spec outside the wire
        vocabulary fails the DECISION loudly (FlushError — contained per
        decision, retried next sweep), never silently writes unsorted
        files and never halts the pipeline."""
        stage = await stage_factory()
        await staged(stage, fake_clock, TEAM, [(0, JAN_US)])
        script_options(httpx_mock)
        flusher = flusher_factory(
            store=store,
            table=table_wire(sort=SORT_WIRE),
            cfg=make_config(),
        )
        mono.advance(901)  # past the shape TTL
        script_table_refresh(
            httpx_mock,
            table=table_wire(
                sort={
                    "sort_id": 12,
                    "fields": [
                        {
                            "source_field_id": 2,
                            "direction": "sideways",
                            "null_order": "nulls_first",
                        }
                    ],
                }
            ),
        )
        with pytest.raises(flush_mod.FlushError, match="sideways") as excinfo:
            await flush_group(flusher, stage, team_group(TEAM), "size")
        assert not isinstance(excinfo.value, FlushHalted)  # contained, not a halt
        assert [rk.offset for rk, _ in await staged_rows(stage, TEAM)] == [0]


# =====================================================================================
# Startup resolution and validation
# =====================================================================================


@component
class TestStartupValidation:
    async def test_unsupported_column_type_refuses_at_resolve(
        self,
        httpx_mock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
    ) -> None:
        wire = table_wire(
            columns=COLUMNS_WIRE
            + [
                {
                    "name": "props",
                    "type": "struct",
                    "field_id": 4,
                    "ordinal": 3,
                    "children": [
                        {"name": "v", "type": "variant", "field_id": 5, "ordinal": 0}
                    ],
                }
            ]
        )
        with pytest.raises(FlushStartupError, match="'props.v'"):
            flusher_factory(store=store, table=wire)

    async def test_missing_read_snapshot_id_refuses_at_resolve(
        self,
        httpx_mock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
    ) -> None:
        with pytest.raises(FlushStartupError, match="read_snapshot_id"):
            flusher_factory(store=store, table=table_wire(read_snapshot_id=None))

    async def test_partition_source_must_be_a_live_top_level_column(
        self,
        httpx_mock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
    ) -> None:
        wire = table_wire(
            spec={
                "spec_id": 5,
                "fields": [{"source_field_id": 99, "transform": "identity"}],
            }
        )
        with pytest.raises(FlushStartupError, match="field_id 99"):
            flusher_factory(store=store, table=wire)

    async def test_unknown_transform_refuses_at_resolve(
        self,
        httpx_mock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
    ) -> None:
        wire = table_wire(
            spec={"spec_id": 5, "fields": [{"source_field_id": 1, "transform": "void"}]}
        )
        with pytest.raises(FlushStartupError, match="void"):
            flusher_factory(store=store, table=wire)

    async def test_sort_source_must_be_a_live_top_level_column(
        self,
        httpx_mock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
    ) -> None:
        """The live sort spec must be honorable at boot — the flusher
        sorts every batch by it, so an unresolvable source is a startup
        refusal, not a per-decision surprise."""
        wire = table_wire(
            sort={
                "sort_id": 5,
                "fields": [
                    {
                        "source_field_id": 99,
                        "direction": "asc",
                        "null_order": "nulls_first",
                    }
                ],
            }
        )
        with pytest.raises(FlushStartupError, match="field_id 99"):
            flusher_factory(store=store, table=wire)

    async def test_sort_field_vocabulary_is_checked_at_resolve(
        self,
        httpx_mock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
    ) -> None:
        wire = table_wire(
            sort={
                "sort_id": 5,
                "fields": [
                    {
                        "source_field_id": 2,
                        "direction": "up",
                        "null_order": "nulls_first",
                    }
                ],
            }
        )
        with pytest.raises(FlushStartupError, match="'up'"):
            flusher_factory(store=store, table=wire)

    async def test_missing_namespace_refuses_at_resolve(
        self,
        httpx_mock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
    ) -> None:
        with pytest.raises(FlushStartupError, match="cannot resolve"):
            flusher_factory(store=store, namespaces=[], table=False)

    async def test_unreachable_catalog_refuses_at_resolve(
        self,
        httpx_mock: Any,
        fake_clock: Any,
        sleeper: Sleeper,
        mono: FakeMonotonic,
        store: FakeObjectStore,
    ) -> None:
        httpx_mock.add_response(
            method="GET",
            url=f"{BASE}/v1/catalogs/{CATALOG}",
            status_code=404,
            json={"error": "not_found", "detail": "no such catalog"},
        )
        from pyhoglake import HoglakeClient

        client = HoglakeClient(BASE)
        client.s3 = store
        with pytest.raises(FlushStartupError, match="cannot resolve"):
            HoglakeFlusher.resolve(
                client, make_config(), now_us=fake_clock, sleep=sleeper, monotonic=mono
            )
        client.close()


# =====================================================================================
# Nested and json columns end to end
# =====================================================================================

#: A destination with every column shape the decoder fills beyond scalars:
#: a struct (holding the partition and sort sources), a list of structs, a
#: map and a json column.
NESTED_COLUMNS_WIRE: list[dict[str, Any]] = [
    {"name": "id", "type": "long", "field_id": 1, "ordinal": 0, "nullable": False},
    {
        "name": "properties",
        "type": "struct",
        "field_id": 2,
        "ordinal": 1,
        "children": [
            {"name": "team_id", "type": "long", "field_id": 3, "ordinal": 0},
            {"name": "ts", "type": "timestamp", "field_id": 4, "ordinal": 1},
            {"name": "u", "type": "uuid", "field_id": 5, "ordinal": 2},
            {"name": "tz", "type": "timestamptz", "field_id": 6, "ordinal": 3},
        ],
    },
    {
        "name": "items",
        "type": "list",
        "field_id": 7,
        "ordinal": 2,
        "children": [
            {
                "name": "element",
                "type": "struct",
                "field_id": 8,
                "ordinal": 0,
                "children": [
                    {"name": "sku", "type": "string", "field_id": 9, "ordinal": 0},
                    {"name": "qty", "type": "int", "field_id": 10, "ordinal": 1},
                ],
            }
        ],
    },
    {
        "name": "attrs",
        "type": "map",
        "field_id": 11,
        "ordinal": 3,
        "children": [
            {
                "name": "key",
                "type": "string",
                "field_id": 12,
                "ordinal": 0,
                "nullable": False,
            },
            {"name": "value", "type": "double", "field_id": 13, "ordinal": 1},
        ],
    },
    {"name": "payload", "type": "json", "field_id": 14, "ordinal": 4},
]
#: identity(properties.team_id) — a struct-leaf partition source.
NESTED_SPEC_WIRE: dict[str, Any] = {
    "spec_id": 9,
    "fields": [{"source_field_id": 3, "transform": "identity"}],
}
#: properties.ts asc, nulls first — a struct-leaf sort source.
NESTED_SORT_WIRE: dict[str, Any] = {
    "sort_id": 4,
    "fields": [{"source_field_id": 4, "direction": "asc", "null_order": "nulls_first"}],
}
HOUR_US = 3_600_000_000


def nested_payload(
    row: int, team: int | None, ts_us: int | None, **extra: Any
) -> bytes:
    doc: dict[str, Any] = {
        "id": row,
        "properties": None
        if team is None
        else {
            "team_id": team,
            "ts": ts_us,
            "u": "12345678-1234-5678-1234-56781234567" + str(row % 10),
            "tz": "2027-01-15T01:00:00+01:00",
        },
        "items": [{"sku": f"s{row}", "qty": row}, {"sku": None}],
        "attrs": {"a": row / 2, "b": None},
        "payload": {"row": row, "nested": [True, None, "é"]},
    }
    doc.update(extra)
    return json.dumps(doc).encode()


@component
class TestNestedJsonColumns:
    async def test_nested_and_json_columns_flush_end_to_end(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
        tmp_path: Any,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Rows staged under an older UNPARTITIONED spec (group ``()``)
        fan out by the live struct-leaf source ``properties.team_id`` —
        one file per team plus the null tuple of a row whose
        ``properties`` is null — each sorted by the struct-leaf sort
        source ``properties.ts``. A row whose list is not a JSON array is
        dropped as ``uncastable_column`` and the batch lands. Every file
        is readable at the catalog schema (field ids on every level),
        passes ``prepared_schema_matches``, and ships leaf-only column
        stats with nothing for a container."""
        monkeypatch.setattr(flush_mod, "FOOTER_STATS_MIN_BYTES", 1)
        from millrace.stage import StagedRecord

        wire = table_wire(
            columns=NESTED_COLUMNS_WIRE, spec=NESTED_SPEC_WIRE, sort=NESTED_SORT_WIRE
        )
        rows = [
            nested_payload(0, 1, JAN_US + 3 * HOUR_US),
            nested_payload(1, 2, JAN_US + 1 * HOUR_US),
            nested_payload(2, 1, JAN_US + 2 * HOUR_US),
            nested_payload(3, None, None),
            nested_payload(4, 1, JAN_US, items={"not": "a list"}),
            nested_payload(5, 2, JAN_US, payload="scalar"),
            nested_payload(6, 1, None),
        ]
        stage = await stage_factory()
        await stage.stage_batch(
            [StagedRecord(group=(), offset=i, payload=p) for i, p in enumerate(rows)],
            now_us=fake_clock(),
        )
        script_options(httpx_mock)
        capture = CommitCapture(httpx_mock, [commit_ok(50)])
        flusher = flusher_factory(store=store, cfg=make_config(), table=wire)

        report = await flush_group(flusher, stage, (), "size")

        assert (report.outcome, report.records, report.files, report.dropped) == (
            "committed",
            6,
            3,
            1,
        )
        assert flusher.stats().records_dropped == {REASON_UNCASTABLE_COLUMN: 1}
        files = capture.payloads[0]["appends"][0]["files"]
        by_team = {f["partition_values"][0]: f for f in files}
        assert set(by_team) == {"1", "2", None}

        info = TableInfo.from_wire(wire)
        want_ids = {3, 4, 5, 6, 9, 10, 12, 13, 14, 1}
        ids_by_team: dict[str | None, list[int]] = {}
        for team, registration in by_team.items():
            key = registration["path"].removeprefix("s3://")
            path = tmp_path / f"{team}.parquet"
            path.write_bytes(store.files[key])
            with pq.ParquetFile(path) as parquet:
                assert prepared_schema_matches(
                    flush_mod.file_schema(parquet.schema_arrow),
                    flush_mod._encode_schema(flush_mod.arrow_schema(info.columns)),
                )
                table = parquet.read()
            props = table.column("properties").to_pylist()
            assert {None if p is None else p["team_id"] for p in props} == {
                None if team is None else int(team)
            }
            ts = [None if p is None else p["ts"] for p in props]
            # nulls first, then nondecreasing payload time
            present = [t for t in ts if t is not None]
            assert ts == [None] * (len(ts) - len(present)) + sorted(present)
            stats = registration["column_stats"]
            ids_by_team[team] = sorted(s["field_id"] for s in stats)
            # leaves only: no container (2, 7, 8, 11)
            assert set(ids_by_team[team]) == want_ids
            ids_by_team[team] = [r["id"] for r in table.to_pylist()]
            if team == "1":
                assert ids_by_team[team] == [6, 2, 0]  # row 4 dropped
                first = table.to_pylist()[2]
                assert first["items"] == [
                    {"sku": "s0", "qty": 0},
                    {"sku": None, "qty": None},
                ]
                assert first["attrs"] == [("a", 0.0), ("b", None)]
                assert first["payload"] == '{"row":0,"nested":[true,null,"é"]}'
                assert first["properties"]["tz"].isoformat() == (
                    "2027-01-15T00:00:00+00:00"
                )
            if team == "2":
                assert ids_by_team[team] == [5, 1]
                assert table.column("payload").to_pylist()[0] == '"scalar"'
            if team is None:
                assert ids_by_team[team] == [3]
        assert stage.snapshot() == []

    async def test_a_nested_drop_logs_the_dotted_path_never_the_value(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A value refused deep inside a container is dropped with its
        dotted catalog path in the drop log line (the operator's only view
        of it); the refused payload value never reaches the log."""
        from millrace.stage import StagedRecord

        wire = table_wire(columns=NESTED_COLUMNS_WIRE, spec=NESTED_SPEC_WIRE)
        bad = nested_payload(1, 1, JAN_US, attrs={"a": "secret-value-xyz"})
        stage = await stage_factory()
        await stage.stage_batch(
            [
                StagedRecord(
                    group=(),
                    offset=0,
                    payload=nested_payload(0, 1, JAN_US),
                ),
                StagedRecord(group=(), offset=1, payload=bad),
            ],
            now_us=fake_clock(),
        )
        script_options(httpx_mock)
        CommitCapture(httpx_mock, [commit_ok(50)])
        flusher = flusher_factory(store=store, table=wire)
        with caplog.at_level(logging.WARNING):
            report = await flush_group(flusher, stage, (), "size")
        assert (report.outcome, report.records, report.dropped) == ("committed", 1, 1)
        (line,) = [
            r.getMessage() for r in caplog.records if "flush dropped" in r.getMessage()
        ]
        assert "reason=uncastable_column" in line
        assert "first_path=attrs.value (offset 0:1)" in line
        assert not [r for r in caplog.records if "secret-value-xyz" in r.getMessage()]

    async def test_a_nested_partition_source_keyed_by_the_consumer_flushes_one_file(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """The steady state: the consumer keyed each row by
        ``properties.team_id``; one group flushes as exactly one file
        under that tuple."""
        from millrace.consumer import classify_message
        from millrace.grouping import GroupKeyer
        from millrace.stage import StagedRecord

        wire = table_wire(
            columns=NESTED_COLUMNS_WIRE, spec=NESTED_SPEC_WIRE, sort=NESTED_SORT_WIRE
        )
        keyer = GroupKeyer(TableInfo.from_wire(wire))
        records = []
        for offset, (team, ts) in enumerate(
            [(1, JAN_US + HOUR_US), (2, JAN_US), (1, JAN_US)]
        ):
            record = classify_message(
                keyer,
                offset=offset,
                value=nested_payload(offset, team, ts),
            )
            assert isinstance(record, StagedRecord)
            records.append(record)
        assert [r.group for r in records] == [("1",), ("2",), ("1",)]
        stage = await stage_factory()
        await stage.stage_batch(records, now_us=fake_clock())
        script_options(httpx_mock)
        capture = CommitCapture(httpx_mock, [commit_ok(50)])
        flusher = flusher_factory(store=store, table=wire)

        report = await flush_group(flusher, stage, ("1",), "size")

        assert (report.outcome, report.records, report.files) == ("committed", 2, 1)
        (registration,) = capture.payloads[0]["appends"][0]["files"]
        assert registration["partition_values"] == ["1"]
        table = parquet_rows(store, registration["path"])
        assert table.column("id").to_pylist() == [2, 0]  # sorted by properties.ts


@component
class TestNestedSourceStartupRefusals:
    @pytest.mark.parametrize(
        ("spec", "sort", "match"),
        [
            # a partition source under a list
            (
                {
                    "spec_id": 5,
                    "fields": [{"source_field_id": 9, "transform": "identity"}],
                },
                None,
                "'items.element.sku' sits under 'items'",
            ),
            # a map value
            (
                {
                    "spec_id": 5,
                    "fields": [{"source_field_id": 13, "transform": "identity"}],
                },
                None,
                "sits under 'attrs'",
            ),
            # a json partition source
            (
                {
                    "spec_id": 5,
                    "fields": [{"source_field_id": 14, "transform": "identity"}],
                },
                None,
                "'payload' is a 'json' column",
            ),
            # a container partition source
            (
                {
                    "spec_id": 5,
                    "fields": [{"source_field_id": 2, "transform": "identity"}],
                },
                None,
                "'properties' is a 'struct' column",
            ),
            # a json sort source
            (
                None,
                {
                    "sort_id": 1,
                    "fields": [
                        {
                            "source_field_id": 14,
                            "direction": "asc",
                            "null_order": "nulls_first",
                        }
                    ],
                },
                "'payload' is a 'json' column",
            ),
            # a sort source under a list
            (
                None,
                {
                    "sort_id": 1,
                    "fields": [
                        {
                            "source_field_id": 10,
                            "direction": "asc",
                            "null_order": "nulls_first",
                        }
                    ],
                },
                "'items.element.qty' sits under 'items'",
            ),
        ],
    )
    async def test_an_unreadable_source_refuses_at_resolve(
        self,
        spec: dict[str, Any] | None,
        sort: dict[str, Any] | None,
        match: str,
        httpx_mock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
    ) -> None:
        wire = table_wire(
            columns=NESTED_COLUMNS_WIRE,
            spec=spec if spec is not None else NESTED_SPEC_WIRE,
            sort=sort,
        )
        with pytest.raises(FlushStartupError, match=match):
            flusher_factory(store=store, table=wire)


# =====================================================================================
# Batched commits: several groups' files in one commit
# =====================================================================================


@component
class TestBatchedFlush:
    async def _stage_teams(
        self, manager: StageManager, clock: Any, teams: list[int]
    ) -> tuple[Any, list[Any]]:
        stage = await manager.open_partition("events", 0)
        for i, team in enumerate(teams):
            await staged(stage, clock, team, [(i, JAN_US)])
        scans = [
            await stage.scan(team_group(t), max_bytes=1 << 30, now_us=clock())
            for t in teams
        ]
        return stage, scans

    async def test_many_groups_land_in_one_commit_and_settle_together(
        self,
        httpx_mock: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
    ) -> None:
        cfg = make_config()
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage, scans = await self._stage_teams(manager, fake_clock, [1, 2, 3, 4])
            script_options(httpx_mock)
            capture = CommitCapture(httpx_mock, [commit_ok(70)])
            flusher = flusher_factory(store=store, cfg=cfg)
            report = await flusher.flush_batch(stage, scans, "slow")

            (body,) = capture.payloads
            files = body["appends"][0]["files"]
            assert sorted(f["partition_values"][0] for f in files) == [
                "1",
                "2",
                "3",
                "4",
            ]
            summary = body["message"].splitlines()[0].split()
            assert {"groups=4", "trigger=slow", "files=4", "commit=1/1"} <= set(summary)
            assert report.commits == 1 and report.failed == ()
            assert [r.outcome for r in report.reports] == ["committed"] * 4
            assert all(r.snapshot_ids == (70,) for r in report.reports)
            assert stage.snapshot() == []
            for team in (1, 2, 3, 4):
                assert await staged_rows(stage, team) == []
            stats = flusher.stats()
            assert (stats.commits, stats.decisions_committed, stats.files_written) == (
                1,
                4,
                4,
            )

    async def test_a_batch_wider_than_the_file_cap_splits_and_settles_per_commit(
        self,
        httpx_mock: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
    ) -> None:
        """Five groups at two files per commit: three commits. The second
        is lost: the first commit's groups are published and settled, the
        rest stay staged and indexed for the retry."""
        from millrace.flush import _PUBLISH_MAX_ATTEMPTS, CommitExhausted

        cfg = make_config(max_files_per_commit=2)
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage, scans = await self._stage_teams(manager, fake_clock, [1, 2, 3, 4, 5])
            script_options(httpx_mock)
            CommitCapture(
                httpx_mock,
                [commit_ok(80)] + [httpx.ConnectError("lost")] * _PUBLISH_MAX_ATTEMPTS,
            )
            flusher = flusher_factory(store=store, cfg=cfg)
            with pytest.raises(CommitExhausted):
                await flusher.flush_batch(stage, scans, "slow")
            assert await staged_rows(stage, 1) == []
            assert await staged_rows(stage, 2) == []
            for team in (3, 4, 5):
                assert len(await staged_rows(stage, team)) == 1
            assert sorted(s.group for s in stage.snapshot()) == sorted(
                team_group(t) for t in (3, 4, 5)
            )

    async def test_a_group_whose_build_fails_leaves_the_batch(
        self,
        httpx_mock: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        cfg = make_config()
        real = flush_mod.build_flush_plan

        def flaky(records: Any, info: Any, *, ctx: Any, **kwargs: Any) -> Any:
            if ctx.group == team_group(2):
                raise flush_mod.FlushError("encoder bug")
            return real(records, info, ctx=ctx, **kwargs)

        monkeypatch.setattr(flush_mod, "build_flush_plan", flaky)
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage, scans = await self._stage_teams(manager, fake_clock, [1, 2, 3])
            script_options(httpx_mock)
            capture = CommitCapture(httpx_mock, [commit_ok(90)])
            flusher = flusher_factory(store=store, cfg=cfg)
            report = await flusher.flush_batch(stage, scans, "slow")

            (body,) = capture.payloads
            assert sorted(
                f["partition_values"][0] for f in body["appends"][0]["files"]
            ) == ["1", "3"]
            assert [g for g, _ in report.failed] == [team_group(2)]
            assert isinstance(report.failed[0][1], flush_mod.FlushError)
            assert "encoder bug" in str(report.failed[0][1])
            assert [s.group for s in stage.snapshot()] == [team_group(2)]
            assert len(await staged_rows(stage, 2)) == 1

    async def test_a_refused_batch_names_its_unpublished_groups(
        self,
        httpx_mock: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
    ) -> None:
        cfg = make_config()
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage, scans = await self._stage_teams(manager, fake_clock, [1, 2])
            script_options(httpx_mock)
            CommitCapture(
                httpx_mock, [commit_refusal(422, "record_validation", "bad value")]
            )
            flusher = flusher_factory(store=store, cfg=cfg)
            report = await flusher.flush_batch(stage, scans, "slow")
            assert sorted(report.refused) == sorted([team_group(1), team_group(2)])
            assert isinstance(report.refusal, flush_mod.CommitRefused)
            assert report.reports == ()
            assert len(stage.snapshot()) == 2

    async def test_a_sweep_batches_due_groups_and_flushes_a_whale_alone(
        self,
        httpx_mock: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """Three small groups past the slow lane share ONE commit; a group
        at the size lane commits on its own."""
        cfg = make_config(
            flush_bytes=3_000, flush_deadline_s=60, slow_lane_deadline_s=60
        )
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage = await manager.open_partition("events", 0)
            for i, team in enumerate((1, 2, 3)):
                await staged(stage, fake_clock, team, [(i, JAN_US)])
            fake_clock.advance(61 * 1_000_000)
            await staged(stage, fake_clock, 9, [(10 + o, JAN_US) for o in range(60)])
            assert stage.snapshot()[-1].staged_bytes >= cfg.flush_bytes
            script_options(httpx_mock)
            capture = CommitCapture(httpx_mock, [commit_ok(1), commit_ok(2)])
            flusher = flusher_factory(store=store, cfg=cfg)
            sweep = await runner(manager, flusher, cfg, fake_clock, sleeper).run_once()

            assert sweep.decisions == 4 and sweep.failed == 0
            teams = sorted(
                sorted(f["partition_values"][0] for f in body["appends"][0]["files"])
                for body in capture.payloads
            )
            assert teams == [["1", "2", "3"], ["9"]]
            for team in (1, 2, 3):
                assert await staged_rows(stage, team) == []

    async def test_a_batch_caps_each_scan_so_the_caps_sum_to_the_budget(
        self,
        httpx_mock: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """Each group of a batch may read its indexed estimate plus an
        even share of the unused budget, so a batch never reads more
        than ``flush_bytes`` however the index under-counts: the
        under-counted group is capped (no sliver for the others) and its
        rest flushes as backlog."""
        cfg = make_config(flush_bytes=10_000)
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage = await manager.open_partition("events", 0)
            for i, team in enumerate((1, 2, 3)):
                await staged(stage, fake_clock, team, [(i, JAN_US)])
            # Team 1 really holds far more than the index says.
            stage._groups[team_group(1)].staged_bytes = 1
            for o in range(20):
                await stage._db.put(
                    keyspace.encode_row_key(keyspace.RowKey(team_group(1), 100 + o)),
                    event_payload(1, JAN_US, "x" * 300),
                )
            script_options(httpx_mock)
            capture = CommitCapture(httpx_mock, [commit_ok(1), commit_ok(2)])
            flusher = flusher_factory(store=store, cfg=cfg)
            r = runner(manager, flusher, cfg, fake_clock, sleeper)
            first = await r.run_once(drain=True)
            assert (first.committed, first.failed) == (3, 0)
            (body,) = capture.payloads
            records = {
                f["partition_values"][0]: f["record_count"]
                for f in body["appends"][0]["files"]
            }
            assert records["2"] == records["3"] == 1  # whole, not slivers
            assert records["1"] < 21  # capped near its share of the budget
            assert stage.has_group(team_group(1))
            assert stage.snapshot()[0].backlog
            second = await r.run_once()
            assert second.committed == 1
            assert stage.snapshot() == []

    async def test_a_batchs_scans_run_concurrently_with_small_read_ahead(
        self,
        httpx_mock: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A batch's group scans overlap (a gate opens only once all three
        are in flight; sequential scans would time out), and a group the
        index puts under 1 MiB is scanned with the small read-ahead."""
        cfg = make_config()
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage = await manager.open_partition("events", 0)
            for i, team in enumerate((1, 2, 3)):
                await staged(stage, fake_clock, team, [(i, JAN_US)])
            gate = asyncio.Event()
            in_flight: list[Any] = []
            hints: list[Any] = []
            real = stage.scan

            async def gated(group: Any, **kwargs: Any) -> Any:
                in_flight.append(group)
                hints.append(kwargs.get("size_hint"))
                if len(in_flight) == 3:
                    gate.set()
                await asyncio.wait_for(gate.wait(), timeout=5)
                return await real(group, **kwargs)

            monkeypatch.setattr(stage, "scan", gated)
            script_options(httpx_mock)
            CommitCapture(httpx_mock, [commit_ok(1)])
            flusher = flusher_factory(store=store, cfg=cfg)
            sweep = await runner(manager, flusher, cfg, fake_clock, sleeper).run_once(
                drain=True
            )
            assert sweep.committed == 3
            assert all(h is not None and h < 1024 * 1024 for h in hints)

    async def test_a_rebuild_inside_a_batch_republishes_only_the_unpublished(
        self,
        httpx_mock: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
    ) -> None:
        """Two commits (two files each at most): the first lands, the
        second's read basis is refused. Only the unpublished group is
        rebuilt on the fresh shape; a dropped row is settled once, right
        after the build, and counted once."""
        cfg = make_config(max_files_per_commit=2)
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage = await manager.open_partition("events", 0)
            await staged(stage, fake_clock, 1, [(0, JAN_US)])
            await staged(
                stage,
                fake_clock,
                2,
                [(1, JAN_US), (2, JAN_US)],
                payloads=[event_payload(2, JAN_US), b"not json"],
            )
            await staged(stage, fake_clock, 3, [(3, JAN_US)])
            scans = [
                await stage.scan(team_group(t), max_bytes=1 << 30, now_us=fake_clock())
                for t in (1, 2, 3)
            ]
            script_options(httpx_mock)
            ddl = commit_refusal(409, "ddl_since_read_snapshot", "spec moved")
            capture = CommitCapture(httpx_mock, [commit_ok(1), ddl, commit_ok(2)])
            script_table_refresh(httpx_mock, table=table_wire(read_snapshot_id=95))
            flusher = flusher_factory(store=store, cfg=cfg)
            report = await flusher.flush_batch(stage, scans, "slow")

            files = [
                sorted(f["partition_values"][0] for f in p["appends"][0]["files"])
                for p in capture.payloads
            ]
            assert files == [["1", "2"], ["3"], ["3"]]
            # One read basis for the first build, a fresh one for the rebuild.
            bases = [p["read_snapshot"] for p in capture.payloads]
            assert bases[0] == bases[1] != bases[2]
            assert [r.outcome for r in report.reports] == ["committed"] * 3
            assert [r.dropped for r in report.reports] == [0, 1, 0]
            assert sum(flusher.stats().records_dropped.values()) == 1
            assert stage.snapshot() == []
            for team in (1, 2, 3):
                assert await staged_rows(stage, team) == []

    async def test_a_refusal_after_a_landed_commit_reports_both(
        self,
        httpx_mock: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
    ) -> None:
        """The first commit lands, the second is refused: the landed
        groups report committed (and are settled), the rest are named in
        ``refused`` and stay staged."""
        cfg = make_config(max_files_per_commit=2)
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage, scans = await self._stage_teams(manager, fake_clock, [1, 2, 3])
            script_options(httpx_mock)
            CommitCapture(
                httpx_mock,
                [commit_ok(1), commit_refusal(422, "record_validation", "bad value")],
            )
            flusher = flusher_factory(store=store, cfg=cfg)
            report = await flusher.flush_batch(stage, scans, "slow")
            assert [(r.group, r.outcome) for r in report.reports] == [
                (team_group(1), "committed"),
                (team_group(2), "committed"),
            ]
            assert report.refused == (team_group(3),)
            assert isinstance(report.refusal, flush_mod.CommitRefused)
            assert [s.group for s in stage.snapshot()] == [team_group(3)]
            assert flusher.stats().rows_published == 2

    async def test_the_runner_splits_refused_and_build_failed_groups(
        self,
        httpx_mock: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
        mono: FakeMonotonic,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A refused batch with a build failure in it: every group it did
        not publish is split out (retries alone), the landed ones carry
        no failure state."""
        real = flush_mod.build_flush_plan

        def flaky(records: Any, info: Any, *, ctx: Any, **kwargs: Any) -> Any:
            if ctx.group == team_group(2):
                raise flush_mod.FlushError("encoder bug")
            return real(records, info, ctx=ctx, **kwargs)

        monkeypatch.setattr(flush_mod, "build_flush_plan", flaky)
        cfg = make_config()
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage = await manager.open_partition("events", 0)
            for i, team in enumerate((1, 2, 3)):
                await staged(stage, fake_clock, team, [(i, JAN_US)])
            script_options(httpx_mock)
            CommitCapture(
                httpx_mock, [commit_refusal(422, "record_validation", "bad value")]
            )
            flusher = flusher_factory(store=store, cfg=cfg)
            r = runner(manager, flusher, cfg, fake_clock, sleeper, mono=mono)
            sweep = await r.run_once(drain=True)
            assert (sweep.decisions, sweep.failed) == (3, 3)
            assert {k[2]: f.split for k, f in r._failures.items()} == {
                team_group(1): True,
                team_group(2): True,
                team_group(3): True,
            }

    async def test_a_decisions_scans_are_capped_and_bounded_in_flight(
        self,
        httpx_mock: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """At most eight scans run at once; each group's cap is its
        indexed estimate plus an even share of the unused budget; small
        groups read with the small scan options."""
        cfg = make_config(flush_bytes=100_000)
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage = await manager.open_partition("events", 0)
            teams = list(range(1, 13))
            for i, team in enumerate(teams):
                await staged(stage, fake_clock, team, [(i, JAN_US)])
            hints = {s.group: s.staged_bytes for s in stage.snapshot()}
            in_flight = peak = 0
            caps: dict[Any, int] = {}
            real = stage.scan

            async def counted(group: Any, **kwargs: Any) -> Any:
                nonlocal in_flight, peak
                in_flight += 1
                peak = max(peak, in_flight)
                caps[group] = kwargs["max_bytes"]
                await asyncio.sleep(0.01)
                try:
                    return await real(group, **kwargs)
                finally:
                    in_flight -= 1

            monkeypatch.setattr(stage, "scan", counted)
            options: list[Any] = []
            real_scan = stage._db.scan_with_options

            async def spied(key_range: Any, opts: Any) -> Any:
                options.append(opts)
                return await real_scan(key_range, opts)

            monkeypatch.setattr(stage._db, "scan_with_options", spied)
            script_options(httpx_mock)
            CommitCapture(httpx_mock, [commit_ok(1)])
            flusher = flusher_factory(store=store, cfg=cfg)
            sweep = await runner(manager, flusher, cfg, fake_clock, sleeper).run_once(
                drain=True
            )
            assert sweep.committed == 12
            assert 1 < peak <= flush_mod._SCAN_CONCURRENCY
            slack = (cfg.flush_bytes - sum(hints.values())) // len(teams)
            assert caps == {g: h + slack for g, h in hints.items()}
            assert sum(caps.values()) <= cfg.flush_bytes
            assert {o.read_ahead_bytes for o in options} == {64 * 1024}
            assert {o.max_fetch_tasks for o in options} == {1}

    async def test_failure_state_is_pruned_when_its_stage_closes(
        self,
        fake_clock: Any,
        sleeper: Sleeper,
    ) -> None:
        cfg = make_config()
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage = await manager.open_partition("events", 0)
            await staged(stage, fake_clock, 1, [(0, JAN_US)])
            r = runner(manager, object(), cfg, fake_clock, sleeper)
            r._failures[("events", 0, team_group(1))] = flush_mod._Failures(1, 1e18)
            r._prune_failures()
            assert len(r._failures) == 1  # still indexed: kept
            await manager.close_partition("events", 0)
            r._prune_failures()
            assert r._failures == {}
