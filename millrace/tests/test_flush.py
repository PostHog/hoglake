"""Flush-path tests (docs/kafka-ingestion-plan.md Phase 4).

Layers: the decoder, the commit message and the request builder are PURE
(unit, no I/O); everything past them is component-tested against a REAL
``PartitionStage`` on ``memory:///`` with a scripted hoglake server
(pytest-httpx, pyhoglake's own pattern) and a fake object store
(tests/flushkit.py).

Pinned here:

- the wire contract, from day one: every commit carries the identity
  read's ``read_snapshot_id`` and the startup-pinned
  ``expected_table_uuid``; EVERY table read is ``totals=false``; the
  shape cache's TTL is the catalog retention / 4;
- commit identity is scoped by ``(table, topic, partition, team,
  offsets)``: two partitions flushing one team over the same offset
  range produce different idempotency keys AND different object URIs,
  and a persisted entry whose v3 envelope names another partition is
  loud corruption at recovery, never a replay (B1);
- the full refusal taxonomy, each recovery in its own scripted test
  (retry counts, re-prepare vs verbatim replay, halt vs continue,
  metric/log emitted) — including B2's: ONLY a 422 coded
  ``record_validation`` quarantines the window (whole payloads, never
  capped); every other answered 4xx halts with the rows staged;
- files are sorted by the live sort spec (fields, directions, null
  placement) before the partition fanout — inverted record-vs-payload
  timestamps flush key-nondecreasing (B3);
- crash discipline: a dropped commit response replays the PERSISTED
  bytes (in-process and across a stage close/reopen), and a commit that
  landed without its settle is receipt-settled with ZERO republication;
- determinism: same staged range → byte-identical request, in a fresh
  flusher too; the message is formatted before any upload (source-order
  pin); object names derive from the idempotency key;
- stats asymmetry: size-triggered flushes ship footer stats, age/slow
  ship ``record_count`` only;
- orphan accounting: a mid-fanout upload failure counts and names the
  abandoned objects;
- junk ``event_time``: quarantine lands the original payloads WHOLE in
  ``poison/`` (never capped); clamp rewrites only the decoded values;
  the scripted server never receives a junk partition value;
- poisoned payloads inside a staged range route to ``poison/`` and the
  good rows still flush;
- a fenced stage (SlateDB ``Error.Closed`` on a contested open) is a
  first-class terminal condition for that partition's decisions —
  logged once, surfaced in the runner's stats, other partitions
  unaffected — pinned against a real ``file:///`` contested open (B5);
- the recovery handoff: ``recover()``-surfaced ``prepared/`` entries are
  reconciled by receipt lookup (200 → local settle, 404 → verbatim
  republish INSIDE the receipt horizon, a loud halt past it or on a
  legacy unknown-age envelope);
- the runner: per-decision containment (one key's failure never wedges a
  sweep), the halt taxonomy stopping the pipeline loudly, chunked sweep
  processing, and the redelivery-inflation stats repair.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
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
    RETENTION_S,
    SORT_WIRE,
    SPEC_WIRE,
    TABLE,
    TABLE_URL,
    TABLE_UUID,
    CommitCapture,
    FakeMonotonic,
    FakeObjectStore,
    Sleeper,
    commit_ok,
    commit_refusal,
    decision,
    event_payload,
    make_config,
    month_wire,
    parquet_rows,
    read_markers,
    script_bootstrap,
    script_options,
    script_receipt,
    script_table_refresh,
    table_wire,
)
from pyhoglake.models import Column
from slatedb.uniffi import CloseReason
from slatedb.uniffi import Error as SlateError
from stagekit import NOW, fast_flush_settings

import millrace.flush as flush_mod
from millrace import keyspace
from millrace.config import EventTimePolicy
from millrace.flush import (
    REASON_EVENT_TIME_OUT_OF_WINDOW,
    REASON_FLUSH_REFUSED,
    REASON_UNCASTABLE_COLUMN,
    REASON_UNDECODABLE_PAYLOAD,
    DecodeError,
    FlushHalted,
    FlushPublishExhausted,
    FlushReport,
    FlushRunner,
    FlushStartupError,
    HoglakeFlusher,
    JsonObjectDecoder,
    StageCorruptionError,
    UnsupportedColumnError,
    build_prepared_plan,
    format_commit_message,
    parse_message_offsets,
)
from millrace.keyspace import FlushedKey, OffsetRange, PreparedRequest
from millrace.stage import PoisonedRecord, StageManager

component = pytest.mark.component

TEAM = 7
JAN_US = NOW  # 2027-01-15
# One month boundary BACKWARD (the default junk window is now−30d … now+1d,
# so a later month would be out-of-window junk): 2026-12-26.
DEC_US = NOW - 20 * 86_400_000_000


def columns(columns_wire: list[dict[str, Any]] | None = None) -> tuple[Column, ...]:
    return tuple(Column.from_wire(c) for c in (columns_wire or COLUMNS_WIRE))


@pytest.fixture
def store() -> FakeObjectStore:
    return FakeObjectStore()


@pytest.fixture
def sleeper() -> Sleeper:
    return Sleeper()


@pytest.fixture
def mono() -> FakeMonotonic:
    return FakeMonotonic()


@pytest.fixture
def flusher_factory(
    httpx_mock: Any, fake_clock: Any, sleeper: Sleeper, mono: FakeMonotonic
):
    """Boot HoglakeFlushers against the scripted server; close them all at
    teardown. Registers the bootstrap reads ONLY (the retention options
    read is per-flush — tests that flush register it themselves)."""
    created: list[HoglakeFlusher] = []

    def make(
        *,
        cfg: Any = None,
        store: FakeObjectStore,
        table: dict[str, Any] | bool | None = None,
        namespaces: list[str] | None = None,
    ) -> HoglakeFlusher:
        script_bootstrap(httpx_mock, table=table, namespaces=namespaces)
        from pyhoglake import HoglakeClient

        client = HoglakeClient(BASE)
        client.s3 = store
        flusher = HoglakeFlusher.resolve(
            client,
            cfg if cfg is not None else make_config(),
            now_us=fake_clock,
            sleep=sleeper,
            monotonic=mono,
        )
        created.append(flusher)
        return flusher

    yield make
    for flusher in created:
        flusher.close()


async def staged(
    stage: Any,
    clock: Any,
    team: int,
    offsets_ts: list[tuple[int, int]],
    *,
    payloads: list[bytes] | None = None,
) -> None:
    """Stage one team's rows: (offset, ts_us) pairs with event payloads."""
    from millrace.stage import StagedRecord

    records = []
    for i, (offset, ts) in enumerate(offsets_ts):
        records.append(
            StagedRecord(
                team_id=team,
                event_ts_us=ts,
                offset=offset,
                payload=(event_payload(team, ts) if payloads is None else payloads[i]),
            )
        )
    await stage.stage_batch(records, now_us=clock())


async def staged_rows(stage: Any, team: int) -> list[tuple[Any, bytes]]:
    return [kv async for kv in stage.scan_team_rows(team)]


def ts_micros(value: Any) -> int:
    """A readback timestamp (naive datetime from parquet) → epoch micros,
    without float arithmetic."""
    from datetime import datetime, timedelta

    return (value - datetime.fromisoformat("1970-01-01T00:00:00")) // timedelta(
        microseconds=1
    )


def runner(
    stages: StageManager,
    flusher: HoglakeFlusher,
    cfg: Any,
    clock: Any,
    sleeper: Sleeper,
) -> FlushRunner:
    return FlushRunner(
        stages,
        flusher,
        cfg.planner_knobs(),
        now_us=clock,
        sleep=sleeper,
        sweep_s=60.0,
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

    def test_timestamptz_requires_an_offset(self) -> None:
        wire = [
            {"name": "tz", "type": "timestamptz", "field_id": 1, "ordinal": 0},
        ]
        decoder = JsonObjectDecoder(columns(wire))
        with pytest.raises(DecodeError):
            decoder.decode_record(b'{"tz": "2027-01-15T00:00:00"}')

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

    def test_unsupported_column_types_are_a_startup_refusal(self) -> None:
        for t in ("json", "list", "struct", "map", "variant"):
            wire = [{"name": "c", "type": t, "field_id": 1, "ordinal": 0}]
            if t in ("list", "struct", "map"):
                wire[0]["children"] = [
                    {"name": "element", "type": "long", "field_id": 2, "ordinal": 0}
                ]
                if t == "struct":
                    wire[0]["children"] = [
                        {"name": "leaf", "type": "long", "field_id": 2, "ordinal": 0}
                    ]
                if t == "map":
                    wire[0]["children"] = [
                        {"name": "key", "type": "string", "field_id": 2, "ordinal": 0},
                        {"name": "value", "type": "long", "field_id": 3, "ordinal": 1},
                    ]
            with pytest.raises(UnsupportedColumnError, match="'c'"):
                JsonObjectDecoder(columns(wire))

    def test_timestamp_int_passthrough_is_bounded_per_unit(self) -> None:
        """An epoch integer outside the column unit's int64 range is a
        per-record coercion failure. Pre-fix it passed coercion and
        detonated the batch's Arrow build with OverflowError — one
        malformed record wedging the key's flush forever. timestamp_s is
        bounded TIGHTER than int64: it is STORED as milliseconds, so its
        range is the ms-representable one (the s→ms cast is safe and
        raises ArrowInvalid past it)."""
        decoder = JsonObjectDecoder(
            columns([{"name": "s", "type": "timestamp_s", "field_id": 1, "ordinal": 0}])
        )
        hi = (2**63 - 1) // 1000  # 9223372036854775: *1000 fits int64
        lo = -hi  # symmetric here: -9223372036854775 * 1000 >= INT64_MIN
        assert decoder.decode_record(f'{{"s": {hi}}}'.encode()).values["s"] == hi
        assert decoder.decode_record(f'{{"s": {lo}}}'.encode()).values["s"] == lo
        for bad in (hi + 1, lo - 1):
            with pytest.raises(DecodeError) as excinfo:
                decoder.decode_record(f'{{"s": {bad}}}'.encode())
            assert excinfo.value.reason == REASON_UNCASTABLE_COLUMN
        # ms / us / ns / ptz carry the full int64 range in their own unit
        for t in ("timestamp_ms", "timestamp", "timestamp_ns", "timestamptz"):
            d = JsonObjectDecoder(
                columns([{"name": "v", "type": t, "field_id": 1, "ordinal": 0}])
            )
            assert d.decode_record(b'{"v": %d}' % (2**63 - 1)).values["v"] == (
                2**63 - 1
            )
            assert d.decode_record(b'{"v": %d}' % -(2**63)).values["v"] == -(2**63)
            for bad in (2**63, -(2**63) - 1):
                with pytest.raises(DecodeError) as excinfo:
                    d.decode_record(b'{"v": %d}' % bad)
                assert excinfo.value.reason == REASON_UNCASTABLE_COLUMN

    def test_timestamp_ns_string_overflow_is_refused(self) -> None:
        """The string path multiplies micros by 1000 for ns: the largest
        ISO-8601 datetime overflows int64 nanoseconds — a coercion
        failure, not an Arrow-build death. The SAME string is legal for
        a micros column (its result fits)."""
        ns = JsonObjectDecoder(
            columns(
                [{"name": "v", "type": "timestamp_ns", "field_id": 1, "ordinal": 0}]
            )
        )
        with pytest.raises(DecodeError, match="outside the int64"):
            ns.decode_record(b'{"v": "9999-12-31T23:59:59"}')
        us = JsonObjectDecoder(
            columns([{"name": "v", "type": "timestamp", "field_id": 1, "ordinal": 0}])
        )
        from datetime import datetime as _dt

        expected = (
            _dt.fromisoformat("9999-12-31T23:59:59") - _dt.fromisoformat("1970-01-01")
        ) // flush_mod.timedelta(microseconds=1)
        assert us.decode_record(b'{"v": "9999-12-31T23:59:59"}').values["v"] == expected

    def test_float_refuses_non_finite_and_unconvertible(self) -> None:
        """json.loads("1e400") yields inf WITHOUT tripping parse_constant
        (that hook sees only the NaN/Infinity literals), and
        float(10**400) raises OverflowError — both are per-record
        coercion failures now. Pre-fix, inf landed in float columns and
        their stats, and the OverflowError escaped the per-record path
        and killed the batch's Arrow build."""
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
        # ValueError. It is quarantined per record like any other
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
# The deterministic commit message (pure)
# =====================================================================================


class TestCommitMessage:
    def test_golden_shape(self) -> None:
        message = format_commit_message(
            table_uuid=TABLE_UUID,
            team_id=TEAM,
            trigger="size",
            records=5,
            quarantined=1,
            files=2,
            partitions=2,
            staged_bytes=123,
            arrow_bytes=456,
            topic="events",
            partition=3,
            offset_ranges=[
                OffsetRange("events", 3, 10, 14),
                OffsetRange("events", 3, 20, 24),
            ],
            version="0.1.0.dev0",
        )
        assert message.splitlines() == [
            (
                f"millrace=v1 table={TABLE_UUID} team=7 trigger=size records=5 "
                "quarantined=1 files=2 partitions=2 staged_bytes=123 arrow_bytes=456 "
                "millrace=0.1.0.dev0"
            ),
            "offsets topic=events partition=3 ranges=10-14,20-24",
        ]

    def test_truncation_keeps_first_and_last_range_verbatim(self) -> None:
        ranges = [OffsetRange("events", 0, i * 10, i * 10 + 4) for i in range(50)]
        message = format_commit_message(
            table_uuid=TABLE_UUID,
            team_id=TEAM,
            trigger="age",
            records=250,
            quarantined=0,
            files=1,
            partitions=1,
            staged_bytes=1,
            arrow_bytes=1,
            topic="events",
            partition=0,
            offset_ranges=ranges,
            version="x",
            limit=120,
        )
        line = message.splitlines()[1]
        assert (
            line
            == "offsets topic=events partition=0 ranges=0-4,...(+48_elided),490-494"
        )
        # ...and the window still parses back out of it.
        assert parse_message_offsets(message, topic="events", partition=0) == (0, 494)

    def test_parse_round_trip(self) -> None:
        message = format_commit_message(
            table_uuid=TABLE_UUID,
            team_id=TEAM,
            trigger="size",
            records=1,
            quarantined=0,
            files=1,
            partitions=1,
            staged_bytes=1,
            arrow_bytes=1,
            topic="events",
            partition=2,
            offset_ranges=[OffsetRange("events", 2, 41, 90)],
            version="x",
        )
        assert parse_message_offsets(message, topic="events", partition=2) == (41, 90)

    def test_parse_refuses_corruption(self) -> None:
        with pytest.raises(StageCorruptionError):
            parse_message_offsets("no offsets here", topic="events", partition=0)
        with pytest.raises(StageCorruptionError):
            # a different partition's line is not ours to settle
            parse_message_offsets(
                "offsets topic=events partition=1 ranges=0-4",
                topic="events",
                partition=0,
            )
        with pytest.raises(StageCorruptionError):
            parse_message_offsets(
                "offsets topic=events partition=0 ranges=garbage",
                topic="events",
                partition=0,
            )

    def test_message_is_formatted_before_any_upload(self) -> None:
        """Source-order pin (millpond's rule): the message is built inside
        the pure builder BEFORE the first Upload object exists, and the
        flusher's upload runs only after the builder returns — a
        formatting failure can never leave an uncounted orphan."""
        builder = inspect.getsource(flush_mod.build_prepared_plan)
        assert builder.index("format_commit_message(") < builder.index(
            "uploads.append(Upload"
        )
        flusher = inspect.getsource(flush_mod.HoglakeFlusher.flush_key)
        assert flusher.index("build_prepared_plan(") < flusher.index(
            "self._upload(plan)"
        )


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
        sleeper: Sleeper,
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
        flusher = flusher_factory(store=store, cfg=make_config(target_output_bytes=1))

        report = await flusher.flush_key(
            stage,
            decision(TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US),
        )

        assert report.outcome == "committed"
        assert report.records == 5
        assert report.files == 2
        assert report.snapshot_id == 50

        # ONE commit, and it carries the whole wire contract.
        assert len(capture.bodies) == 1
        body = capture.payloads[0]
        key = keyspace.idempotency_key(TABLE_UUID, "events", 0, TEAM, 0, 4)
        assert body["idempotency_key"] == key
        assert body["read_snapshot"] == 88  # the identity read's snapshot
        assert body["author"] == "millrace/events/0"
        assert body["appends"][0]["namespace"] == NAMESPACE
        assert body["appends"][0]["table"] == TABLE
        assert body["appends"][0]["expected_table_uuid"] == TABLE_UUID
        files = body["appends"][0]["files"]
        assert len(files) == 2
        assert {tuple(f["partition_values"]) for f in files} == {
            (str(TEAM), month_wire(JAN_US)),
            (str(TEAM), month_wire(DEC_US)),
        }
        assert sum(f["record_count"] for f in files) == 5
        # size trigger: full footer stats ride along.
        assert all("column_stats" in f for f in files)
        assert {s["field_id"] for f in files for s in f["column_stats"]} == {1, 2, 3}
        for f in files:
            assert f["path"].startswith(f"{DATA_PATH}/data/{NAMESPACE}/{TABLE}/{key}/")
            assert f["file_size_bytes"] > 0 and f["footer_size"] > 0
        # The deterministic message: line 1 fixed key=value, line 2 the
        # staged offsets window.
        summary, offsets_line = body["message"].splitlines()
        tokens = summary.split(" ")
        assert tokens[:4] == [
            "millrace=v1",
            f"table={TABLE_UUID}",
            "team=7",
            "trigger=size",
        ]
        assert "records=5" in tokens
        assert "quarantined=0" in tokens
        assert "files=2" in tokens
        assert "partitions=2" in tokens
        assert any(t.startswith("staged_bytes=") for t in tokens)
        assert any(t.startswith("arrow_bytes=") for t in tokens)
        assert any(t.startswith("millrace=") for t in tokens)
        assert offsets_line == "offsets topic=events partition=0 ranges=0-4"

        # Settlement: the staged range is deleted, stats and offsets are
        # gone, the prepared entry is dropped, and NO marker is written —
        # the settle is atomic with the receipt in hand, so a marker
        # could only restate both (the flushed/ prefix stays empty).
        assert await staged_rows(stage, TEAM) == []
        assert await stage.iter_key_stats() == []
        assert await stage.read_offsets(TEAM) == ()
        assert await stage.load_prepared() == []
        assert await read_markers(stage) == []

        # The uploaded parquet is real and carries the rows, in scan
        # ((timestamp, offset)) order: the older December rows group first.
        assert len(store.files) == 2
        assert sorted(store.files) == [
            f"bkt/lake/data/{NAMESPACE}/{TABLE}/{key}/{i}.parquet" for i in (0, 1)
        ]
        dec = parquet_rows(
            store, f"s3://bkt/lake/data/{NAMESPACE}/{TABLE}/{key}/0.parquet"
        )
        assert [r["team_id"] for r in dec.to_pylist()] == [7, 7]
        assert [ts_micros(r["timestamp"]) for r in dec.to_pylist()] == [
            DEC_US,
            DEC_US + 3_600_000_000,
        ]
        jan = parquet_rows(
            store, f"s3://bkt/lake/data/{NAMESPACE}/{TABLE}/{key}/1.parquet"
        )
        assert [ts_micros(r["timestamp"]) for r in jan.to_pylist()] == [
            JAN_US,
            JAN_US + 3_600_000_000,
            JAN_US + 7_200_000_000,
        ]

        stats = flusher.stats()
        assert stats.flushes_committed == 1
        assert stats.rows_published == 5
        assert stats.files_written == 2
        assert stats.commit_attempts == 1
        assert stats.commit_retries == 0

    async def test_flush_via_runner_sweeps_claimed_partitions(
        self,
        httpx_mock: Any,
        memory_store: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
        mono: FakeMonotonic,
    ) -> None:
        cfg = make_config(target_output_bytes=1)
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            opened = await manager.open_partition("events", 0)
            await staged(opened.stage, fake_clock, TEAM, [(0, JAN_US), (1, JAN_US)])
            script_options(httpx_mock)
            capture = CommitCapture(httpx_mock, [commit_ok(50)])
            flusher = flusher_factory(store=store, cfg=cfg)

            sweep = await runner(manager, flusher, cfg, fake_clock, sleeper).run_once()

            assert sweep.partitions == 1
            assert sweep.decisions == 1
            assert sweep.committed == 1
            assert sweep.failed == 0
            assert len(capture.bodies) == 1
            assert await staged_rows(opened.stage, TEAM) == []


# =====================================================================================
# The refusal taxonomy (each recovery pinned)
# =====================================================================================


@component
class TestRefusalTaxonomy:
    async def _stage_and_flush(
        self,
        httpx_mock,
        stage_factory,
        fake_clock,
        flusher_factory,
        store,
        sleeper,
        steps,
        *,
        cfg=None,
    ):
        stage = await stage_factory()
        await staged(stage, fake_clock, TEAM, [(0, JAN_US), (1, JAN_US), (2, JAN_US)])
        script_options(httpx_mock)
        capture = CommitCapture(httpx_mock, steps)
        flusher = flusher_factory(
            store=store, cfg=cfg or make_config(target_output_bytes=1)
        )
        return stage, capture, flusher

    async def test_commit_conflict_retries_the_identical_request(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        stage, capture, flusher = await self._stage_and_flush(
            httpx_mock,
            stage_factory,
            fake_clock,
            flusher_factory,
            store,
            sleeper,
            [
                commit_refusal(409, "commit_conflict", "the read set moved"),
                commit_ok(50),
            ],
        )
        report = await flusher.flush_key(
            stage,
            decision(TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US),
        )
        assert report.outcome == "committed"
        # Bounded, backed-off, BYTE-IDENTICAL retry of the same payload.
        assert len(capture.bodies) == 2
        assert capture.bodies[0] == capture.bodies[1]
        assert sleeper.calls == [0.5]
        stats = flusher.stats()
        assert stats.conflict_retries == 1
        assert stats.commit_attempts == 2
        assert await staged_rows(stage, TEAM) == []

    async def test_commit_conflict_exhaustion_keeps_the_prepared_entry(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        stage, capture, flusher = await self._stage_and_flush(
            httpx_mock,
            stage_factory,
            fake_clock,
            flusher_factory,
            store,
            sleeper,
            [commit_refusal(409, "commit_conflict", "moved")] * 5,
        )
        with pytest.raises(FlushPublishExhausted):
            await flusher.flush_key(
                stage,
                decision(
                    TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US
                ),
            )
        assert len(capture.bodies) == 5
        assert len(set(capture.bodies)) == 1
        assert sleeper.calls == [0.5, 1.0, 2.0, 4.0]
        # The persisted request survives for the next sweep; the rows stay.
        pending = await stage.load_prepared()
        assert len(pending) == 1
        assert pending[0].request.body == capture.bodies[0]
        assert len(await staged_rows(stage, TEAM)) == 3

    async def test_ddl_since_read_snapshot_rebuilds_never_replays(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        stage, capture, flusher = await self._stage_and_flush(
            httpx_mock,
            stage_factory,
            fake_clock,
            flusher_factory,
            store,
            sleeper,
            [
                commit_refusal(
                    409,
                    "ddl_since_read_snapshot",
                    "concurrent DDL since snapshot 88 on table(s): ns1.events",
                )
            ],
        )
        key = keyspace.idempotency_key(TABLE_UUID, "events", 0, TEAM, 0, 2)
        report = await flusher.flush_key(
            stage,
            decision(TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US),
        )
        # The refusal is definitive: the persisted entry is DROPPED (a
        # replay would livelock — the basis is frozen in the payload), the
        # abandoned upload is counted, the shape cache is dropped, and the
        # rows stay staged for a RE-PREPARE against a fresh read.
        assert report.outcome == "reprepare"
        assert await stage.load_prepared() == []
        assert len(await staged_rows(stage, TEAM)) == 3
        stats = flusher.stats()
        assert stats.reprepares == 1
        assert stats.orphaned_uploads == 1
        assert stats.commit_attempts == 1  # no retry of the refused payload

        # The next sweep re-reads the table (the cache was dropped) and
        # re-prepares: SAME idempotency key (same rows), NEW read_snapshot.
        script_table_refresh(httpx_mock, table=table_wire(read_snapshot_id=95))
        capture.extend([commit_ok(51)])
        report2 = await flusher.flush_key(
            stage,
            decision(TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US),
        )
        assert report2.outcome == "committed"
        assert report2.snapshot_id == 51
        assert capture.payloads[1]["read_snapshot"] == 95
        assert capture.payloads[1]["idempotency_key"] == key
        # Rebuilt, not replayed: the fresh basis changes the bytes...
        assert capture.bodies[1] != capture.bodies[0]
        # ...and the deterministic object names are the same, so the
        # rebuild rewrites the orphaned objects rather than doubling them.
        assert (
            capture.payloads[1]["appends"][0]["files"][0]["path"]
            == capture.payloads[0]["appends"][0]["files"][0]["path"]
        )
        assert await staged_rows(stage, TEAM) == []

    async def test_table_recreated_halts_loudly_and_keeps_state(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        stage, _capture, flusher = await self._stage_and_flush(
            httpx_mock,
            stage_factory,
            fake_clock,
            flusher_factory,
            store,
            sleeper,
            [
                commit_refusal(
                    409,
                    "table_recreated",
                    "expected_table_uuid does not match the live table",
                )
            ],
        )
        with pytest.raises(FlushHalted) as excinfo:
            await flusher.flush_key(
                stage,
                decision(
                    TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US
                ),
            )
        assert excinfo.value.reason == "table_recreated"
        assert TABLE_UUID in excinfo.value.detail
        # The incarnation guard: nothing is settled, dropped or deleted —
        # the staged state stays for an operator, the upload is accounted.
        assert len(await staged_rows(stage, TEAM)) == 3
        assert len(await stage.load_prepared()) == 1
        assert flusher.stats().orphaned_uploads == 1

    async def test_410_halts_with_the_servers_reconcile_instructions(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        detail = (
            "read_snapshot 88 is below the expiry floor 90 (reached at "
            "2027-01-15T01:00:00Z); reconcile from a current read of the table"
        )
        stage, _capture, flusher = await self._stage_and_flush(
            httpx_mock,
            stage_factory,
            fake_clock,
            flusher_factory,
            store,
            sleeper,
            [commit_refusal(410, "expired", detail)],
        )
        with pytest.raises(FlushHalted) as excinfo:
            await flusher.flush_key(
                stage,
                decision(
                    TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US
                ),
            )
        assert excinfo.value.reason == "read_snapshot_expired"
        assert detail in excinfo.value.detail
        # A stop sign, never a skip: the rows stay staged.
        assert len(await staged_rows(stage, TEAM)) == 3

    async def test_503_commit_queue_timeout_backs_off_per_retry_after(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        stage, capture, flusher = await self._stage_and_flush(
            httpx_mock,
            stage_factory,
            fake_clock,
            flusher_factory,
            store,
            sleeper,
            [
                commit_refusal(
                    503,
                    "commit_queue_timeout",
                    "admission timed out",
                    headers={"Retry-After": "2"},
                ),
                commit_refusal(
                    503,
                    "commit_queue_timeout",
                    "admission timed out",
                    headers={"Retry-After": "2"},
                ),
                commit_ok(50),
            ],
        )
        report = await flusher.flush_key(
            stage,
            decision(TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US),
        )
        assert report.outcome == "committed"
        # IDENTICAL request each time; the Retry-After header floors the
        # backoff above the exponential curve's 0.5s/1.0s.
        assert len(capture.bodies) == 3
        assert len(set(capture.bodies)) == 1
        assert sleeper.calls == [2.0, 2.0]
        assert flusher.stats().queue_retries == 2

    async def test_422_request_shape_halts_with_rows_staged(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """B2: a request-shape 422 (``error="validation"`` — the only 422
        the current server can produce; it never opens the parquet) HALTS
        with the rows staged: nothing about the refusal says the ROWS can
        never publish. The pre-fix behavior quarantined — truncated and
        deleted — the whole window on this answer."""
        stage, capture, flusher = await self._stage_and_flush(
            httpx_mock,
            stage_factory,
            fake_clock,
            flusher_factory,
            store,
            sleeper,
            [commit_refusal(422, "validation", "column_stats name a dropped field_id")],
        )
        with caplog.at_level(logging.WARNING), pytest.raises(FlushHalted) as excinfo:
            await flusher.flush_key(
                stage,
                decision(
                    TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US
                ),
            )
        assert excinfo.value.reason == "commit_refused"
        assert "422" in excinfo.value.detail
        assert len(capture.bodies) == 1  # an answered 4xx is never retried
        # Nothing is settled, quarantined or deleted: the rows stay
        # staged, the prepared entry stays for an operator, the upload is
        # orphan-accounted — the idempotency_key_reused posture, verbatim.
        assert len(await staged_rows(stage, TEAM)) == 3
        assert await stage.scan_poison() == []
        assert len(await stage.load_prepared()) == 1
        stats = flusher.stats()
        assert stats.decisions_quarantined == 0
        assert stats.orphaned_uploads == 1
        assert any("orphaned 1 uploaded parquet" in r.message for r in caplog.records)
        assert not any(
            "flush decision quarantined" in r.message for r in caplog.records
        )

    @pytest.mark.parametrize("status", [400, 401, 403, 413])
    async def test_answered_4xx_halts_with_rows_staged(
        self,
        status: int,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """B2: a proxy's 401/403, an ingress's 413, a plain 400 — each
        ANSWERED refusal used to truncate the window to the poison cap
        and delete the rows. Each now halts with the rows staged."""
        stage, _capture, flusher = await self._stage_and_flush(
            httpx_mock,
            stage_factory,
            fake_clock,
            flusher_factory,
            store,
            sleeper,
            [commit_refusal(status, "proxy_says_no", "answered by an intermediary")],
        )
        with pytest.raises(FlushHalted) as excinfo:
            await flusher.flush_key(
                stage,
                decision(
                    TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US
                ),
            )
        assert excinfo.value.reason == "commit_refused"
        assert str(status) in excinfo.value.detail
        assert len(await staged_rows(stage, TEAM)) == 3
        assert await stage.scan_poison() == []
        assert len(await stage.load_prepared()) == 1
        assert flusher.stats().decisions_quarantined == 0

    async def test_422_record_validation_quarantines_uncapped(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """B2's ONE quarantine arm: a 422 whose error code is the
        reserved ``record_validation`` — the server judged RECORDS, not
        the request. The window's rows land in ``poison/`` WHOLE: a
        flush-time quarantine is never capped, because settlement deletes
        the staged rows and the poison entry is the only surviving copy.
        The payloads here exceed ``poison.value_max_bytes`` to pin that
        (the pre-fix cap truncated them)."""
        stage = await stage_factory()
        big = 2 * 1024 * 1024  # twice the default poison value cap (1 MiB)
        payloads = [event_payload(TEAM, JAN_US, blob="x" * big) for _ in range(2)]
        assert all(len(p) > make_config().poison.value_max_bytes for p in payloads)
        await staged(
            stage, fake_clock, TEAM, [(0, JAN_US), (1, JAN_US)], payloads=payloads
        )
        script_options(httpx_mock)
        capture = CommitCapture(
            httpx_mock,
            [
                commit_refusal(
                    422, "record_validation", "record 1 of 2 fails the schema"
                )
            ],
        )
        flusher = flusher_factory(store=store, cfg=make_config(target_output_bytes=1))
        with caplog.at_level(logging.WARNING):
            report = await flusher.flush_key(
                stage,
                decision(
                    TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US
                ),
            )
        assert report.outcome == "quarantined"
        assert report.quarantined == 2
        # The whole window quarantined with the ORIGINAL payloads, WHOLE
        # (value_bytes_original == the stored length: nothing was cut)...
        poisoned = await stage.scan_poison()
        assert [(p.offset, p.reason) for p in poisoned] == [
            (0, REASON_FLUSH_REFUSED),
            (1, REASON_FLUSH_REFUSED),
        ]
        assert [p.value for p in poisoned] == payloads
        assert all(p.value_bytes_original == len(p.value or b"") for p in poisoned)
        # ...and the range settled WITHOUT a commit: rows and stats gone,
        # no prepared entry, the refused upload orphan-accounted.
        assert await staged_rows(stage, TEAM) == []
        assert await stage.iter_key_stats() == []
        assert await stage.load_prepared() == []
        assert await read_markers(stage) == []
        stats = flusher.stats()
        assert stats.decisions_quarantined == 1
        assert stats.records_quarantined == 2
        assert stats.orphaned_uploads == 1
        assert any("flush decision quarantined" in r.message for r in caplog.records)

        # The loop is not wedged: the next decision flushes fine. (The 422
        # dropped the shape cache — the refusal may name a stale shape —
        # so this flush re-reads the table first.)
        await staged(stage, fake_clock, TEAM, [(3, JAN_US), (4, JAN_US)])
        script_table_refresh(httpx_mock)
        capture.extend([commit_ok(51)])
        report2 = await flusher.flush_key(
            stage,
            decision(TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US),
        )
        assert report2.outcome == "committed"
        assert capture.payloads[1]["appends"][0]["files"][0]["record_count"] == 2

    async def test_422_record_validation_with_an_unknown_shape_still_halts(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """Fail-safe: a 422 whose code is NOT exactly ``record_validation``
        is an unknown shape — halt, never quarantine. (The matcher is the
        (status, code) pair, never a substring of the detail prose.)"""
        stage, _capture, flusher = await self._stage_and_flush(
            httpx_mock,
            stage_factory,
            fake_clock,
            flusher_factory,
            store,
            sleeper,
            # The marker phrase in the DETAIL must not matter either way
            # here (only the reused-key halt reads it, on the
            # "validation" code).
            [commit_refusal(422, "record_validation_v2", "a newer, unknown shape")],
        )
        with pytest.raises(FlushHalted) as excinfo:
            await flusher.flush_key(
                stage,
                decision(
                    TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US
                ),
            )
        assert excinfo.value.reason == "commit_refused"
        assert len(await staged_rows(stage, TEAM)) == 3
        assert await stage.scan_poison() == []

    async def test_recovery_replay_answered_plain_422_halts(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """B2 on the recovery path: a persisted prepared/ entry whose
        verbatim replay draws a request-shape 422 halts with the entry
        and the rows intact (pre-fix: the replay quarantine-settled the
        window and DELETED the rows)."""
        stage = await stage_factory()
        await staged(stage, fake_clock, TEAM, [(0, JAN_US), (1, JAN_US)])
        script_options(httpx_mock)
        from millrace.flush import _PUBLISH_MAX_ATTEMPTS

        capture = CommitCapture(
            httpx_mock, [httpx.ConnectError("lost")] * _PUBLISH_MAX_ATTEMPTS
        )
        flusher = flusher_factory(store=store, cfg=make_config(target_output_bytes=1))
        with pytest.raises(FlushPublishExhausted):
            await flusher.flush_key(
                stage,
                decision(
                    TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US
                ),
            )
        pending = await stage.load_prepared()
        assert len(pending) == 1

        # Reopen (the claim moved) and reconcile: receipt 404 → replay →
        # answered 422 "validation" → HALT, entry and rows intact.
        await stage.close()
        stage2 = await stage_factory()
        key = keyspace.idempotency_key(TABLE_UUID, "events", 0, TEAM, 0, 1)
        script_receipt(httpx_mock, key, snapshot_id=None)
        capture.extend(
            [commit_refusal(422, "validation", "column_stats name a dropped field_id")]
        )
        with pytest.raises(FlushHalted) as excinfo:
            await flusher.reconcile_prepared(
                stage2, (await stage2.recover()).pending_prepared[0]
            )
        assert excinfo.value.reason == "commit_refused"
        assert len(await staged_rows(stage2, TEAM)) == 2
        assert len(await stage2.load_prepared()) == 1
        assert await stage2.scan_poison() == []
        assert capture.bodies[-1] == capture.bodies[0]  # the replay was verbatim
        await stage2.close()

    def test_record_validation_refusal_matcher_is_precise(self) -> None:
        """The B2 matcher: exactly (status 422, code "record_validation")
        — a locally-raised ValidationError (no status), a request-shape
        "validation" 422, and other statuses all fail safe (halt)."""
        from pyhoglake import ValidationError as _VE

        assert flush_mod._is_record_validation_refusal(
            _VE("record_validation", status_code=422, detail="row 3: bad")
        )
        assert not flush_mod._is_record_validation_refusal(
            _VE("validation", status_code=422, detail="row 3: bad")
        )
        assert not flush_mod._is_record_validation_refusal(
            _VE("record_validation", status_code=None)  # local, never on the wire
        )
        assert not flush_mod._is_record_validation_refusal(
            flush_mod.HoglakeError("record_validation", status_code=422)
        )
        assert not flush_mod._is_record_validation_refusal(
            _VE("record_validation", status_code=400)
        )

    async def test_422_reused_idempotency_key_halts(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        stage, _capture, flusher = await self._stage_and_flush(
            httpx_mock,
            stage_factory,
            fake_clock,
            flusher_factory,
            store,
            sleeper,
            [
                commit_refusal(
                    422, "validation", "idempotency_key reused with a different request"
                )
            ],
        )
        with pytest.raises(FlushHalted) as excinfo:
            await flusher.flush_key(
                stage,
                decision(
                    TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US
                ),
            )
        # A receipt exists for THIS key with a DIFFERENT payload: with
        # deterministic derivation that means a foreign writer or broken
        # determinism — halt, never settle rows a stranger may not have
        # published.
        assert excinfo.value.reason == "idempotency_key_reused"
        assert len(await staged_rows(stage, TEAM)) == 3

    async def test_table_dropped_409_halts(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """The spec's fourth 409 code is untyped in pyhoglake (it maps to
        plain CommitConflictError); the commit_conflict ladder must not
        retry it — a dropped destination is a halt, not contention."""
        stage, capture, flusher = await self._stage_and_flush(
            httpx_mock,
            stage_factory,
            fake_clock,
            flusher_factory,
            store,
            sleeper,
            [
                commit_refusal(
                    409, "table_dropped", "the table was dropped at snapshot 91"
                )
            ],
        )
        with pytest.raises(FlushHalted) as excinfo:
            await flusher.flush_key(
                stage,
                decision(
                    TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US
                ),
            )
        assert excinfo.value.reason == "table_dropped"
        assert capture.bodies and len(capture.bodies) == 1  # no retries
        assert sleeper.calls == []


# =====================================================================================
# Crash discipline
# =====================================================================================


@component
class TestCrashDiscipline:
    async def test_dropped_commit_response_replays_persisted_bytes_in_process(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        stage = await stage_factory()
        await staged(stage, fake_clock, TEAM, [(0, JAN_US), (1, JAN_US)])
        script_options(httpx_mock)
        capture = CommitCapture(
            httpx_mock, [httpx.ConnectError("connection lost"), commit_ok(50)]
        )
        flusher = flusher_factory(store=store, cfg=make_config(target_output_bytes=1))

        report = await flusher.flush_key(
            stage,
            decision(TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US),
        )

        # The ambiguous response replays the PERSISTED bytes, the receipt
        # dedupes, and the rows publish exactly once.
        assert report.outcome == "committed"
        assert len(capture.bodies) == 2
        assert capture.bodies[0] == capture.bodies[1]
        assert flusher.stats().commit_retries == 1
        # ...and the retry re-uploaded nothing (the objects persisted).
        assert len(store.opened) == len(set(store.opened)) == 1

    async def test_crash_between_upload_and_commit_recovers_byte_identically(
        self,
        httpx_mock: Any,
        memory_store: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
        mono: FakeMonotonic,
    ) -> None:
        """The whole publish ladder dies in a 'crash'; a fresh flusher
        (a new process) recovers the partition and replays the persisted
        request BYTE-IDENTICALLY — receipt dedupe means one logical
        commit."""
        stage = await stage_factory()
        await staged(stage, fake_clock, TEAM, [(0, JAN_US), (1, JAN_US), (2, JAN_US)])
        script_options(httpx_mock)
        from millrace.flush import _PUBLISH_MAX_ATTEMPTS

        capture1 = CommitCapture(
            httpx_mock,
            [httpx.ConnectError("lost")] * _PUBLISH_MAX_ATTEMPTS,
        )
        flusher1 = flusher_factory(store=store, cfg=make_config(target_output_bytes=1))
        with pytest.raises(FlushPublishExhausted):
            await flusher1.flush_key(
                stage,
                decision(
                    TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US
                ),
            )
        # The persisted request survived, byte-identical to every attempt.
        pending = (await stage.load_prepared())[0]
        assert pending.request.body == capture1.bodies[0]
        assert len(set(capture1.bodies)) == 1
        uploads_before = list(store.opened)
        await stage.close()

        # "Restart": the partition's stage reopens from the same store;
        # recovery surfaces the prepared entry; the receipt lookup misses;
        # the republished bytes are the persisted ones.
        stage2 = await stage_factory()
        recovery = await stage2.recover()
        assert recovery.legacy_markers_collected == 0
        assert len(recovery.pending_prepared) == 1

        key = pending.request.idempotency_key
        script_receipt(httpx_mock, key, snapshot_id=None)
        capture1.extend([commit_ok(50)])
        flusher2 = flusher_factory(store=store, cfg=make_config(target_output_bytes=1))
        report = await flusher2.reconcile_prepared(stage2, recovery.pending_prepared[0])

        assert report.outcome == "replayed"
        assert report.snapshot_id == 50
        assert capture1.bodies[-1] == pending.request.body
        assert capture1.bodies[-1] == capture1.bodies[0]
        # The replay re-uploads nothing (the objects landed before the
        # 'crash') and settles the range — no marker left behind either.
        assert store.opened == uploads_before
        assert await staged_rows(stage2, TEAM) == []
        assert await stage2.load_prepared() == []
        assert await read_markers(stage2) == []
        assert flusher2.stats().flushes_replayed == 1
        assert flusher2.stats().receipt_lookups == 1

    async def test_crash_between_commit_and_settle_is_receipt_settled(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Commit landed, ``commit_flushed`` never ran: recovery settles
        from the receipt and the rows are NOT re-flushed (zero POSTs)."""
        stage = await stage_factory()
        await staged(stage, fake_clock, TEAM, [(0, JAN_US), (1, JAN_US)])
        script_options(httpx_mock)
        capture = CommitCapture(httpx_mock, [commit_ok(50)])
        flusher = flusher_factory(store=store, cfg=make_config(target_output_bytes=1))

        async def crash_before_settle(*args: Any, **kwargs: Any) -> None:
            raise RuntimeError("the pod dies between the receipt and the settle")

        monkeypatch.setattr(stage, "commit_flushed", crash_before_settle)
        with pytest.raises(RuntimeError, match="the pod dies"):
            await flusher.flush_key(
                stage,
                decision(
                    TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US
                ),
            )
        assert len(capture.bodies) == 1  # the commit landed
        assert len(await staged_rows(stage, TEAM)) == 2  # the rows are staged
        await stage.close()

        stage2 = await stage_factory()
        recovery = await stage2.recover()
        assert len(recovery.pending_prepared) == 1
        key = recovery.pending_prepared[0].request.idempotency_key
        script_receipt(httpx_mock, key, snapshot_id=50)
        flusher2 = flusher_factory(store=store, cfg=make_config(target_output_bytes=1))

        report = await flusher2.reconcile_prepared(stage2, recovery.pending_prepared[0])

        assert report.outcome == "receipt_settled"
        assert report.records == 0  # THIS process published nothing
        assert len(capture.bodies) == 1  # zero republication
        assert await staged_rows(stage2, TEAM) == []
        assert await read_markers(stage2) == []  # the settle writes no marker
        assert flusher2.stats().receipt_settlements == 1


# =====================================================================================
# Determinism
# =====================================================================================


@component
class TestDeterminism:
    async def test_same_staged_range_flushes_byte_identically_across_processes(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """Flush, settle, re-stage the IDENTICAL rows, flush again through a
        FRESH flusher (a new process): the two prepared requests are
        byte-identical — object names included, since they derive from the
        idempotency key. (A real server would answer the second with the
        first's receipt; the scripted one re-accepts.)"""
        rows = [(0, JAN_US), (1, JAN_US + 3_600_000_000), (2, DEC_US)]
        capture = CommitCapture(httpx_mock, [commit_ok(50), commit_ok(51)])
        for _snapshot in (50, 51):
            stage = await stage_factory()
            await staged(stage, fake_clock, TEAM, rows)
            script_options(httpx_mock)
            flusher = flusher_factory(
                store=store, cfg=make_config(target_output_bytes=1)
            )
            report = await flusher.flush_key(
                stage,
                decision(
                    TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US
                ),
            )
            assert report.outcome == "committed"
            await stage.close()
        assert capture.bodies[0] == capture.bodies[1]

    async def test_prepared_body_is_the_wire_body(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """The persisted request IS the wire body (httpx's exact JSON
        encoding of the same dict), before and after a replay."""
        stage = await stage_factory()
        await staged(stage, fake_clock, TEAM, [(0, JAN_US), (1, JAN_US)])
        script_options(httpx_mock)
        from millrace.flush import _PUBLISH_MAX_ATTEMPTS

        capture = CommitCapture(
            httpx_mock, [httpx.ConnectError("lost")] * _PUBLISH_MAX_ATTEMPTS
        )
        flusher = flusher_factory(store=store, cfg=make_config(target_output_bytes=1))
        with pytest.raises(FlushPublishExhausted):
            await flusher.flush_key(
                stage,
                decision(
                    TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US
                ),
            )
        pending = await stage.load_prepared()
        assert len(pending) == 1
        assert pending[0].request.body == capture.bodies[0]
        # And the envelope round-trips the byte-exact body (the keyspace
        # codec's whole job for this prefix).
        decoded = keyspace.decode_prepared_value(
            keyspace.encode_prepared_value(pending[0].request)
        )
        assert decoded.body == capture.bodies[0]


# =====================================================================================
# B1: commit identity is scoped by (topic, partition) — offsets are per partition
# =====================================================================================


@component
class TestPartitionScopedIdentity:
    async def test_two_partitions_same_team_same_offsets_distinct_keys_and_uris(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """Two partitions flushing one team over the SAME offset range:
        different idempotency keys AND different object URIs (the key is
        in the path), so the second partition's upload can never
        overwrite the first partition's committed objects ahead of the
        server's key-reuse refusal (PR #331 review — the v1 derivation
        omitted topic/partition and collided exactly here)."""
        stage0 = await stage_factory(partition=0)
        stage1 = await stage_factory(partition=1)
        rows = [(0, JAN_US), (1, JAN_US)]
        await staged(stage0, fake_clock, TEAM, rows)
        await staged(stage1, fake_clock, TEAM, rows)
        script_options(httpx_mock)
        capture = CommitCapture(httpx_mock, [commit_ok(50), commit_ok(51)])
        flusher = flusher_factory(store=store, cfg=make_config(target_output_bytes=1))

        r0 = await flusher.flush_key(
            stage0,
            decision(TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US),
        )
        r1 = await flusher.flush_key(
            stage1,
            decision(TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US),
        )

        assert r0.outcome == r1.outcome == "committed"
        # Identical (table, team, first, last); the partition makes the key.
        assert r0.idempotency_key == keyspace.idempotency_key(
            TABLE_UUID, "events", 0, TEAM, 0, 1
        )
        assert r1.idempotency_key == keyspace.idempotency_key(
            TABLE_UUID, "events", 1, TEAM, 0, 1
        )
        assert r0.idempotency_key != r1.idempotency_key
        # ...and the object names derive from the key, so they are
        # disjoint too — every uploaded object distinct and present.
        uris0 = [f["path"] for f in capture.payloads[0]["appends"][0]["files"]]
        uris1 = [f["path"] for f in capture.payloads[1]["appends"][0]["files"]]
        assert uris0 and uris1
        assert set(uris0).isdisjoint(uris1)
        assert all(f"/{r0.idempotency_key}/" in u for u in uris0)
        assert all(f"/{r1.idempotency_key}/" in u for u in uris1)
        assert set(store.files) == {u.removeprefix("s3://") for u in [*uris0, *uris1]}
        # The v3 envelopes are self-describing — but both settled
        # post-commit, so the decode is exercised on the NEXT crash-path
        # tests; here the commits themselves carried the distinct keys.
        assert capture.payloads[0]["idempotency_key"] == r0.idempotency_key
        assert capture.payloads[1]["idempotency_key"] == r1.idempotency_key

    async def test_a_prepared_entry_naming_another_partition_is_corruption(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """The v3 envelope's derivation inputs are checked against the
        stage's identity at recovery: an entry under events[0]'s SlateDB
        naming partition 1 is foreign state — loud corruption, NEVER a
        replay under another partition's identity. (Nothing here produces
        such an entry; the guard is for foreign writes and bugs.)"""
        stage = await stage_factory(partition=0)
        await staged(stage, fake_clock, TEAM, [(0, JAN_US)])
        key = keyspace.idempotency_key(TABLE_UUID, "events", 1, TEAM, 0, 0)
        body = flush_mod.canonical_body(
            {"idempotency_key": key, "message": "millrace=v1 synthetic"}
        )
        await stage.persist_prepared(
            TEAM,
            0,
            PreparedRequest(key, body, NOW, topic="events", partition=1),
        )
        flusher = flusher_factory(store=store, cfg=make_config(target_output_bytes=1))
        (pending,) = (await stage.recover()).pending_prepared
        with pytest.raises(StageCorruptionError, match=r"events\[1\]"):
            await flusher.reconcile_prepared(stage, pending)
        # The entry is NOT replayed, dropped or settled: it stays for an
        # operator, and no receipt lookup even happened (the scripted
        # server registers none — a lookup would have errored the test).
        assert len(await stage.load_prepared()) == 1
        assert [rk.offset for rk, _ in await staged_rows(stage, TEAM)] == [0]


# =====================================================================================
# B3: files are sorted by the catalog's sort spec (fields, directions, null
# placement) before the partition fanout — scan order is Kafka-record time,
# the spec is payload-event time
# =====================================================================================


@component
class TestSortSpec:
    async def test_inverted_kafka_and_payload_timestamps_flush_key_nondecreasing(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """The review's red test: record timestamps (the row key's ts)
        ASCEND through the batch while the payloads' event times DESCEND
        — late arrivals. The committed file must be key-nondecreasing
        under the live spec (timestamp asc); pre-fix it followed scan
        order and inverted."""
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
            cfg=make_config(target_output_bytes=1),
        )

        report = await flusher.flush_key(
            stage,
            decision(TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US),
        )

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
        flusher = flusher_factory(
            store=store, table=wire, cfg=make_config(target_output_bytes=1)
        )

        report = await flusher.flush_key(
            stage,
            decision(TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US),
        )

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
        flusher = flusher_factory(
            store=store, table=wire, cfg=make_config(target_output_bytes=1)
        )

        report = await flusher.flush_key(
            stage,
            decision(TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US),
        )

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
        flusher = flusher_factory(
            store=store, table=wire, cfg=make_config(target_output_bytes=1)
        )

        report = await flusher.flush_key(
            stage,
            decision(TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US),
        )

        assert report.outcome == "committed"
        (file,) = capture.payloads[0]["appends"][0]["files"]
        written = [r["uid"] for r in parquet_rows(store, file["path"]).to_pylist()]
        assert written == [pyuuid.UUID(int=2), pyuuid.UUID(int=10), pyuuid.UUID(int=30)]

    async def test_a_sort_spec_change_resets_the_ratio(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
        mono: FakeMonotonic,
    ) -> None:
        """The sort spec is in the shape fingerprint (a sort-order change
        rearranges every row's neighbors, so realized compression under
        the old order is a different quantity): a changed spec resets the
        EWMA, the SAME spec does not."""
        from millrace.planner import DEFAULT_COMPRESSION_RATIO

        stage = await stage_factory()
        await staged(stage, fake_clock, TEAM, [(0, JAN_US)])
        script_options(httpx_mock)
        CommitCapture(httpx_mock, [commit_ok(50)])
        flusher = flusher_factory(
            store=store,
            table=table_wire(sort=SORT_WIRE),
            cfg=make_config(target_output_bytes=1),
        )
        await flusher.flush_key(
            stage,
            decision(TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US),
        )
        observed = flusher.compression_ratio
        assert observed != DEFAULT_COMPRESSION_RATIO

        # A refresh carrying the SAME sort spec does not reset.
        mono.advance(901)  # past the TTL (retention 3600 / 4)
        script_table_refresh(httpx_mock, table=table_wire(sort=SORT_WIRE))
        await flusher._io(flusher._shape_sync)
        assert flusher.compression_ratio == observed

        # A refresh with a CHANGED sort order resets.
        mono.advance(901)
        changed = {
            "sort_id": 7,
            "fields": [
                {"source_field_id": 2, "direction": "desc", "null_order": "nulls_first"}
            ],
        }
        script_table_refresh(httpx_mock, table=table_wire(sort=changed))
        await flusher._io(flusher._shape_sync)
        assert flusher.compression_ratio == DEFAULT_COMPRESSION_RATIO

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
            cfg=make_config(target_output_bytes=1),
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
            await flusher.flush_key(
                stage,
                decision(
                    TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US
                ),
            )
        assert not isinstance(excinfo.value, FlushHalted)  # contained, not a halt
        assert [rk.offset for rk, _ in await staged_rows(stage, TEAM)] == [0]


# =====================================================================================
# Stats asymmetry
# =====================================================================================


@component
class TestStatsAsymmetry:
    async def test_size_triggered_flush_ships_full_footer_stats(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        stage = await stage_factory()
        await staged(
            stage, fake_clock, TEAM, [(0, JAN_US), (1, JAN_US + 3_600_000_000)]
        )
        script_options(httpx_mock)
        capture = CommitCapture(httpx_mock, [commit_ok(50)])
        flusher = flusher_factory(store=store, cfg=make_config(target_output_bytes=1))

        await flusher.flush_key(
            stage,
            decision(TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US),
        )

        files = capture.payloads[0]["appends"][0]["files"]
        assert len(files) == 1
        stats = files[0]["column_stats"]
        assert {s["field_id"] for s in stats} == {1, 2, 3}
        by_id = {s["field_id"]: s for s in stats}
        assert by_id[1]["value_count"] == 2
        assert by_id[1]["null_count"] == 0
        # Bounds are Iceberg single-value binary, base64'd — team_id 7
        # (Iceberg serializes longs little-endian).
        import base64 as b64

        assert b64.b64decode(by_id[1]["lower_bound"]) == (7).to_bytes(8, "little")
        assert files[0]["record_count"] == 2
        # The parquet footer carries real statistics either way.
        key = files[0]["path"].removeprefix("s3://")
        metadata = pq.read_metadata(pa.BufferReader(store.files[key]))
        assert metadata.row_group(0).column(0).statistics is not None
        assert metadata.row_group(0).column(0).statistics.min == 7

    @pytest.mark.parametrize("trigger", ["age", "slow"])
    async def test_age_and_slow_triggered_flushes_defer_stats(
        self,
        trigger: str,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """A SMALL age/slow-triggered file ships deferred stats. (m10:
        the rule keys on the REALIZED file size, not the trigger — with
        the default 128 MiB target this two-row file is far below the
        half-target threshold.)"""
        stage = await stage_factory()
        await staged(
            stage, fake_clock, TEAM, [(0, JAN_US), (1, JAN_US + 3_600_000_000)]
        )
        script_options(httpx_mock)
        capture = CommitCapture(httpx_mock, [commit_ok(50)])
        flusher = flusher_factory(store=store, cfg=make_config())

        await flusher.flush_key(
            stage,
            decision(TEAM, trigger=trigger, staged_bytes=100, first_staged_ts=JAN_US),
        )

        files = capture.payloads[0]["appends"][0]["files"]
        assert len(files) == 1
        # stats_mode=deferred on the wire: NO column_stats key — the file
        # registers as `pending` and the hydrator backfills off the commit
        # path. record_count (and the sizes) still ship.
        assert "column_stats" not in files[0]
        assert files[0]["record_count"] == 2
        assert files[0]["file_size_bytes"] > 0
        # The footer still has them — the hydrator reads it.
        key = files[0]["path"].removeprefix("s3://")
        metadata = pq.read_metadata(pa.BufferReader(store.files[key]))
        assert metadata.row_group(0).column(0).statistics is not None

    @pytest.mark.parametrize("trigger", ["age", "slow"])
    async def test_big_age_and_slow_flushes_ship_footer_stats(
        self,
        trigger: str,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """m10: the stats asymmetry must NOT invert on catch-up flushes —
        a big age/slow-triggered file (realized size at or above half
        the target output) ships full footer stats exactly like a
        size-triggered one."""
        stage = await stage_factory()
        await staged(
            stage, fake_clock, TEAM, [(0, JAN_US), (1, JAN_US + 3_600_000_000)]
        )
        script_options(httpx_mock)
        capture = CommitCapture(httpx_mock, [commit_ok(50)])
        # The realized file is ~2-3 KB; a 1 KiB target puts the
        # half-target threshold at 512 B, well below it — the file is
        # "large" by the deployment's own sizing.
        flusher = flusher_factory(
            store=store, cfg=make_config(target_output_bytes=1024)
        )

        await flusher.flush_key(
            stage,
            decision(TEAM, trigger=trigger, staged_bytes=100, first_staged_ts=JAN_US),
        )

        files = capture.payloads[0]["appends"][0]["files"]
        assert len(files) == 1
        assert files[0]["file_size_bytes"] >= 512  # the premise: realized >= threshold
        assert {s["field_id"] for s in files[0]["column_stats"]} == {1, 2, 3}


# =====================================================================================
# Orphan accounting
# =====================================================================================


@component
class TestOrphanAccounting:
    async def test_mid_fanout_upload_failure_counts_and_names_orphans(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        stage = await stage_factory()
        # Two months → a two-file fanout; the second upload dies.
        await staged(stage, fake_clock, TEAM, [(0, JAN_US), (1, DEC_US)])
        script_options(httpx_mock)
        capture = CommitCapture(httpx_mock, [])  # no commit is reached
        flusher = flusher_factory(store=store, cfg=make_config(target_output_bytes=1))
        store.fail_at = 1

        with pytest.raises(OSError, match="object store refused"):
            await flusher.flush_key(
                stage,
                decision(
                    TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US
                ),
            )

        stats = flusher.stats()
        assert stats.orphaned_uploads == 1
        key = keyspace.idempotency_key(TABLE_UUID, "events", 0, TEAM, 0, 1)
        orphaned = f"bkt/lake/data/{NAMESPACE}/{TABLE}/{key}/0.parquet"
        with caplog.at_level(logging.WARNING):
            # re-run the accounting read: the warning was logged at failure
            pass
        assert any(
            "orphaned 1 uploaded parquet" in r.message and orphaned in r.message
            for r in caplog.records
        ), [r.message for r in caplog.records]
        # The completed upload landed; nothing was persisted or published.
        assert orphaned in store.files
        assert await stage.load_prepared() == []
        assert len(await staged_rows(stage, TEAM)) == 2
        assert capture.bodies == []

        # The retry regenerates the identical request and REWRITES the
        # same object names (deterministic paths reclaim the orphan). The
        # upload failure was not a HoglakeError, so the shape cache
        # survived and this flush makes no table read.
        store.fail_at = None
        capture.extend([commit_ok(50)])
        report = await flusher.flush_key(
            stage,
            decision(TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US),
        )
        assert report.outcome == "committed"
        assert (
            capture.payloads[0]["appends"][0]["files"][0]["path"] == f"s3://{orphaned}"
        )
        assert await staged_rows(stage, TEAM) == []

    async def test_persist_failure_counts_the_whole_fanout(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        stage = await stage_factory()
        await staged(stage, fake_clock, TEAM, [(0, JAN_US), (1, DEC_US)])
        script_options(httpx_mock)
        CommitCapture(httpx_mock, [])
        flusher = flusher_factory(store=store, cfg=make_config(target_output_bytes=1))

        async def fail_persist(*args: Any, **kwargs: Any) -> None:
            raise ConnectionError("slatedb hiccup")

        monkeypatch.setattr(stage, "persist_prepared", fail_persist)
        with pytest.raises(ConnectionError):
            await flusher.flush_key(
                stage,
                decision(
                    TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US
                ),
            )
        # Upload done, persist failed: BOTH objects are orphaned.
        assert len(store.files) == 2
        assert flusher.stats().orphaned_uploads == 2


# =====================================================================================
# Junk event_time policy
# =====================================================================================


@component
class TestJunkEventTime:
    FUTURE_JUNK = NOW + 10 * 86_400_000_000  # 10 days past the future edge
    PAST_JUNK = NOW - 100 * 86_400_000_000  # 100 days back, past the 30d edge

    async def test_quarantine_mode_lands_original_payloads_in_poison(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        stage = await stage_factory()
        payloads = [
            event_payload(TEAM, JAN_US),
            event_payload(TEAM, self.FUTURE_JUNK),
            event_payload(TEAM, self.PAST_JUNK),
        ]
        await staged(
            stage,
            fake_clock,
            TEAM,
            [(0, JAN_US), (1, self.FUTURE_JUNK), (2, self.PAST_JUNK)],
            payloads=payloads,
        )
        script_options(httpx_mock)
        capture = CommitCapture(httpx_mock, [commit_ok(50)])
        flusher = flusher_factory(store=store, cfg=make_config(target_output_bytes=1))

        report = await flusher.flush_key(
            stage,
            decision(TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US),
        )

        assert report.outcome == "committed"
        assert report.records == 1
        assert report.quarantined == 2
        # The staged rows are NEVER mutated; the junk ones land in poison/
        # with their ORIGINAL payloads...
        poisoned = await stage.scan_poison()
        assert [(p.offset, p.reason) for p in poisoned] == [
            (1, REASON_EVENT_TIME_OUT_OF_WINDOW),
            (2, REASON_EVENT_TIME_OUT_OF_WINDOW),
        ]
        assert poisoned[0].value == payloads[1]
        assert poisoned[1].value == payloads[2]
        assert all(p.value_bytes_original == len(p.value or b"") for p in poisoned)
        # ...and the server never receives a junk partition value: the
        # only tuple committed is the in-window month.
        files = capture.payloads[0]["appends"][0]["files"]
        assert [f["partition_values"] for f in files] == [
            [str(TEAM), month_wire(JAN_US)]
        ]
        # The window covers every settled row: all three staged rows are
        # gone (the quarantined ones via the poison path).
        assert await staged_rows(stage, TEAM) == []
        assert flusher.stats().records_quarantined == 2

    async def test_clamp_mode_rewrites_timestamps_to_the_window_edge(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        stage = await stage_factory()
        await staged(
            stage,
            fake_clock,
            TEAM,
            [(0, JAN_US), (1, self.FUTURE_JUNK), (2, self.PAST_JUNK)],
        )
        script_options(httpx_mock)
        capture = CommitCapture(httpx_mock, [commit_ok(50)])
        cfg = make_config(
            target_output_bytes=1, event_time_policy=EventTimePolicy.CLAMP
        )
        flusher = flusher_factory(store=store, cfg=cfg)

        report = await flusher.flush_key(
            stage,
            decision(TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US),
        )

        assert report.outcome == "committed"
        assert report.records == 3
        assert await stage.scan_poison() == []
        # The clamped timestamps, read back from the parquet: the future
        # junk sits at now+max_future, the past junk at now-max_past.
        files = capture.payloads[0]["appends"][0]["files"]
        key_prefix = f"bkt/lake/data/{NAMESPACE}/{TABLE}/"
        months = sorted(f["partition_values"][1] for f in files)
        max_us = fake_clock() + cfg.event_time_max_future_s * 1_000_000
        min_us = fake_clock() - cfg.event_time_max_past_s * 1_000_000
        assert months == sorted(
            {month_wire(JAN_US), month_wire(max_us), month_wire(min_us)}
        )
        written: list[int] = []
        for k in sorted(store.files):
            if k.startswith(key_prefix.removeprefix("s3://")):
                written += [
                    ts_micros(r["timestamp"])
                    for r in parquet_rows(store, f"s3://{k}").to_pylist()
                ]
        assert sorted(written) == sorted([JAN_US, max_us, min_us])

    async def test_a_quarantined_payload_is_never_capped(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """B2's other half: the FLUSH-TIME quarantine (a junk event_time
        here) stores the WHOLE payload — the consumer's
        ``poison_value_max_bytes`` cap does not apply, because settlement
        deletes the staged row and the poison entry is the only surviving
        copy. (The consume-time path keeps its cap: a record that can
        never become a row is forensic state, capped by design.)"""
        stage = await stage_factory()
        # A payload larger than the consumer's poison cap (1 MiB in these
        # configs), out-of-window junk.
        big = event_payload(TEAM, self.PAST_JUNK, blob="x" * (2 * 1024 * 1024))
        assert len(big) > make_config().poison.value_max_bytes
        await staged(
            stage,
            fake_clock,
            TEAM,
            [(0, self.PAST_JUNK)],
            payloads=[big],
        )
        script_options(httpx_mock)
        CommitCapture(httpx_mock, [])  # every row junk: no commit at all
        flusher = flusher_factory(store=store, cfg=make_config(target_output_bytes=1))

        report = await flusher.flush_key(
            stage,
            decision(TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US),
        )

        assert report.outcome == "quarantined"  # nothing to commit
        poisoned = await stage.scan_poison()
        assert len(poisoned) == 1
        assert poisoned[0].value == big  # WHOLE — the pre-fix cap cut it
        assert poisoned[0].value_bytes_original == len(big)


# =====================================================================================
# Poisoned payloads inside a staged range
# =====================================================================================


@component
class TestPoisonedPayloads:
    async def test_bad_records_route_to_poison_and_good_rows_flush(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
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
        capture = CommitCapture(httpx_mock, [commit_ok(50)])
        flusher = flusher_factory(store=store, cfg=make_config(target_output_bytes=1))

        report = await flusher.flush_key(
            stage,
            decision(TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US),
        )

        assert report.outcome == "committed"
        assert report.records == 2
        assert report.quarantined == 4
        poisoned = await stage.scan_poison()
        assert [(p.offset, p.reason) for p in poisoned] == [
            (1, REASON_UNDECODABLE_PAYLOAD),
            (3, REASON_UNCASTABLE_COLUMN),
            (4, REASON_UNCASTABLE_COLUMN),
            (5, REASON_UNDECODABLE_PAYLOAD),
        ]
        # Original payloads preserved for forensics.
        assert poisoned[0].value == payloads[1]
        # The good two flushed; every staged row is settled (no marker
        # is written anywhere: settlement is atomic, receipt in hand).
        files = capture.payloads[0]["appends"][0]["files"]
        assert sum(f["record_count"] for f in files) == 2
        assert await staged_rows(stage, TEAM) == []
        assert await read_markers(stage) == []
        # An unknown payload key is dropped (counted + logged once), never
        # fatal: the NEXT range carries one.
        await staged(
            stage,
            fake_clock,
            TEAM,
            [(6, JAN_US)],
            payloads=[
                b'{"team_id": 7, "timestamp": '
                + str(JAN_US).encode()
                + b', "surprise": 1}'
            ],
        )
        capture.extend([commit_ok(51)])
        report2 = await flusher.flush_key(
            stage,
            decision(TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US),
        )
        assert report2.outcome == "committed"
        assert flusher.stats().dropped_payload_keys == 1
        assert "surprise" not in str(capture.payloads[1])


# =====================================================================================
# Magnitude junk: one malformed record must never wedge the flush (C1)
# =====================================================================================


class TestBatchBuildIsolation:
    """The Arrow batch build's per-record failure routing. Coercion is
    total (every bad value is a DecodeError first — the decoder tests
    above pin that), so this layer only fires on encoder drift; these
    tests drive it directly with records no decoder would emit."""

    def test_a_record_that_defeats_the_arrow_build_is_isolated(self) -> None:
        cols = columns()
        good = flush_mod.DecodedRecord(
            values={"team_id": 7, "timestamp": 5, "event": "x"}, dropped_keys=()
        )
        # team_id is a long: 2**63 + 5 passes NO coercion, but a record
        # carrying it (hand-built here) must still cost only itself.
        bad = flush_mod.DecodedRecord(
            values={"team_id": 2**63 + 5, "timestamp": 5, "event": None},
            dropped_keys=(),
        )
        rows = [
            (keyspace.RowKey(7, 100, 0), b"payload-good", good),
            (keyspace.RowKey(7, 101, 1), b"payload-bad", bad),
            (keyspace.RowKey(7, 102, 2), b"payload-good-2", good),
        ]
        table, encoded, guilty = flush_mod._build_batch_isolating(rows, cols)
        assert table.num_rows == 2
        assert encoded.num_rows == 2
        assert [(k.offset, payload) for k, payload, _ in guilty] == [
            (1, b"payload-bad")
        ]
        # survivors keep scan order (the ingest-sort contract)
        assert table.column("timestamp").to_pylist() is not None
        assert [r.values["team_id"] for _, _, r in rows] == [7, 7]

    def test_an_unisolatable_failure_is_a_loud_encoder_bug(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A batch failure no single record reproduces is a bug, not
        data: a loud FlushError, never a silent skip or an infinite
        isolation loop."""
        cols = columns()
        record = flush_mod.DecodedRecord(
            values={"team_id": 7, "timestamp": 5, "event": "x"}, dropped_keys=()
        )
        rows = [
            (keyspace.RowKey(7, 100, 0), b"p", record),
            (keyspace.RowKey(7, 101, 1), b"q", record),
        ]
        real = flush_mod._build_batch

        def flaky(records: Any, cols: Any) -> Any:
            if len(records) > 1:
                raise pa.ArrowInvalid("combination-only failure")
            return real(records, cols)

        monkeypatch.setattr(flush_mod, "_build_batch", flaky)
        with pytest.raises(flush_mod.FlushError, match="encoder bug"):
            flush_mod._build_batch_isolating(rows, cols)


@component
class TestMagnitudeOverflow:
    async def test_magnitude_junk_poisons_the_record_never_the_flush(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """The C1 regression at flush level: a SECOND timestamp column
        (not a partition source, so the junk window never sees it)
        carrying 2**63+5, and a float column carrying the overflow
        literal 1e400 (parses to inf without tripping strict-JSON) and
        10**400 (no double holds it). Each bad record routes to poison/
        with its original payload; the good row still flushes. Pre-fix,
        the first OverflowError escaped the per-record path and killed
        the batch's Arrow build — the key retried identically every
        sweep, wedging the partition."""
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
        # The good row's t2 is 2000-01-01 — far OUTSIDE the junk window:
        # a non-partition-source temporal column is not window-checked at
        # all (the window covers the live spec's sources only), and this
        # pins that the coercion bound — not the window — covers them.
        good = event_payload(TEAM, JAN_US, t2=946_684_800_000_000, score=1.5)
        payloads = [
            good,
            event_payload(TEAM, JAN_US, t2=2**63 + 5),  # int64 overflow, timestamp
            b'{"team_id":7,"timestamp":'
            + str(JAN_US).encode()
            + b',"event":"x","score":1e400}',  # inf via overflow literal
            event_payload(TEAM, JAN_US, score=10**400),  # no double holds this int
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
            store=store,
            cfg=make_config(target_output_bytes=1),
            table=table_wire(columns=wire),
        )

        report = await flusher.flush_key(
            stage,
            decision(TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US),
        )

        assert report.outcome == "committed"
        assert (report.records, report.quarantined) == (1, 3)
        assert [(p.offset, p.reason) for p in await stage.scan_poison()] == [
            (1, REASON_UNCASTABLE_COLUMN),
            (2, REASON_UNCASTABLE_COLUMN),
            (3, REASON_UNCASTABLE_COLUMN),
        ]
        # The good row landed whole; nothing non-finite or clamped-away.
        (f,) = capture.payloads[0]["appends"][0]["files"]
        rows = parquet_rows(store, f["path"]).to_pylist()
        assert [r["score"] for r in rows] == [1.5]
        assert [ts_micros(r["t2"]) for r in rows] == [946_684_800_000_000]
        assert await staged_rows(stage, TEAM) == []


# =====================================================================================
# The receipt horizon (C3): a receipt 404 past the horizon is a halt, never
# a blind replay
# =====================================================================================


@component
class TestReceiptHorizon:
    async def _crashed_pending(
        self,
        httpx_mock: Any,
        stage: Any,
        fake_clock: Any,
        flusher: Any,
    ) -> tuple[str, bytes, Any]:
        """A real flush whose whole publish ladder dies: the partition
        keeps a v2 persisted entry (persisted_at = the fake clock) and
        its two staged rows. Returns (idempotency key, persisted body,
        the RecoveredPrepared)."""
        from millrace.flush import _PUBLISH_MAX_ATTEMPTS

        await staged(stage, fake_clock, TEAM, [(0, JAN_US), (1, JAN_US)])
        script_options(httpx_mock)
        capture = CommitCapture(
            httpx_mock, [httpx.ConnectError("lost")] * _PUBLISH_MAX_ATTEMPTS
        )
        with pytest.raises(FlushPublishExhausted):
            await flusher.flush_key(
                stage,
                decision(
                    TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US
                ),
            )
        (pending,) = await stage.load_prepared()
        assert pending.request.persisted_at == fake_clock()
        return pending.request.idempotency_key, capture.bodies[0], pending

    async def test_entry_inside_the_horizon_replays_byte_identically(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        stage = await stage_factory()
        flusher = flusher_factory(store=store, cfg=make_config(target_output_bytes=1))
        key, crashed_body, pending = await self._crashed_pending(
            httpx_mock, stage, fake_clock, flusher
        )

        script_receipt(httpx_mock, key, snapshot_id=None)
        capture = CommitCapture(httpx_mock, [commit_ok(50)])
        report = await flusher.reconcile_prepared(stage, pending)

        assert report.outcome == "replayed"
        assert capture.bodies == [crashed_body]
        assert await staged_rows(stage, TEAM) == []

    async def test_entry_at_exactly_the_horizon_still_replays(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """The boundary: age == horizon is INSIDE it (the guard is
        `age > horizon`; flipped to `>=` this test reds)."""
        stage = await stage_factory()
        flusher = flusher_factory(
            store=store, cfg=make_config(target_output_bytes=1, receipt_horizon_s=100)
        )
        key, crashed_body, pending = await self._crashed_pending(
            httpx_mock, stage, fake_clock, flusher
        )
        fake_clock.advance(100 * 1_000_000)  # exactly the horizon, in micros

        script_receipt(httpx_mock, key, snapshot_id=None)
        capture = CommitCapture(httpx_mock, [commit_ok(50)])
        report = await flusher.reconcile_prepared(stage, pending)

        assert report.outcome == "replayed"
        assert capture.bodies == [crashed_body]

    async def test_entry_past_the_horizon_halts_without_replaying(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """One microsecond past the horizon: the receipt may have been
        purged, so the 404 is ambiguous and the pipeline HALTS for
        operator reconciliation — no republish (duplication), nothing
        settled, the entry and rows stay put."""
        stage = await stage_factory()
        flusher = flusher_factory(
            store=store, cfg=make_config(target_output_bytes=1, receipt_horizon_s=100)
        )
        key, _body, pending = await self._crashed_pending(
            httpx_mock, stage, fake_clock, flusher
        )
        fake_clock.advance(100 * 1_000_000 + 1)  # one microsecond past

        script_receipt(httpx_mock, key, snapshot_id=None)
        capture = CommitCapture(httpx_mock, [])  # no commit may be sent
        with pytest.raises(FlushHalted) as excinfo:
            await flusher.reconcile_prepared(stage, pending)

        assert excinfo.value.reason == "receipt_horizon_exceeded"
        assert key in excinfo.value.detail  # the operator's lookup handle
        assert capture.bodies == []
        # nothing settled: the prepared entry and the staged rows survive
        assert len(await stage.load_prepared()) == 1
        assert len(await staged_rows(stage, TEAM)) == 2
        # the possibly-live uploads are NOT orphan-accounted (they may be
        # referenced by a landed commit — naming them for deletion could
        # lose data)
        assert flusher.stats().orphaned_uploads == 0

    async def test_unknown_age_legacy_entry_halts_on_a_404(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """A v1 envelope carries no persisted_at: age unknown ⇒ halt-safe
        on a receipt 404 (never a blind replay), even fresh off the
        clock."""
        stage = await stage_factory()
        flusher = flusher_factory(store=store, cfg=make_config(target_output_bytes=1))
        key, body, pending = await self._crashed_pending(
            httpx_mock, stage, fake_clock, flusher
        )
        # Re-encode the SAME request as a legacy v1 envelope (no
        # persisted_at) — what a pre-v2 build would have left behind.
        raw_key = key.encode("utf-8")
        v1_envelope = (
            b"\x01"
            + len(raw_key).to_bytes(2, "big")
            + raw_key
            + len(body).to_bytes(4, "big")
            + body
        )
        legacy = keyspace.decode_prepared_value(v1_envelope)
        assert legacy.persisted_at is None
        legacy_pending = type(pending)(key=pending.key, request=legacy)

        script_receipt(httpx_mock, key, snapshot_id=None)
        capture = CommitCapture(httpx_mock, [])  # no commit may be sent
        with pytest.raises(FlushHalted) as excinfo:
            await flusher.reconcile_prepared(stage, legacy_pending)
        assert excinfo.value.reason == "receipt_horizon_exceeded"
        assert capture.bodies == []
        assert len(await staged_rows(stage, TEAM)) == 2

    async def test_receipt_200_settles_regardless_of_age(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """The horizon gates only the BLIND replay: a 200 receipt is
        ground truth and settles at ANY age (a receipt still standing
        past its retention is purge lag — the safe direction)."""
        stage = await stage_factory()
        flusher = flusher_factory(
            store=store, cfg=make_config(target_output_bytes=1, receipt_horizon_s=100)
        )
        key, _body, pending = await self._crashed_pending(
            httpx_mock, stage, fake_clock, flusher
        )
        fake_clock.advance(10_000 * 1_000_000)  # 100x past the horizon

        script_receipt(httpx_mock, key, snapshot_id=50)
        capture = CommitCapture(httpx_mock, [])  # no commit may be sent
        report = await flusher.reconcile_prepared(stage, pending)

        assert report.outcome == "receipt_settled"
        assert await staged_rows(stage, TEAM) == []
        assert capture.bodies == []

    async def test_the_horizon_comes_from_the_server_when_exposed(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """The wire exposes no receipt retention today, but a pyhoglake
        that grows ``Catalog._receipt_retention_seconds`` is honored when
        present: its (positive) value, not the knob, is the horizon."""

        class CatalogWithReceiptRetention:
            def _receipt_retention_seconds(self) -> float:
                return 100.0  # far below the knob's 6-day default

        stage = await stage_factory()
        flusher = flusher_factory(store=store, cfg=make_config(target_output_bytes=1))
        key, _body, pending = await self._crashed_pending(
            httpx_mock, stage, fake_clock, flusher
        )
        flusher._catalog = CatalogWithReceiptRetention()  # type: ignore[assignment]
        fake_clock.advance(200 * 1_000_000)  # inside the knob, past the server

        script_receipt(httpx_mock, key, snapshot_id=None)
        with pytest.raises(FlushHalted) as excinfo:
            await flusher.reconcile_prepared(stage, pending)
        assert excinfo.value.reason == "receipt_horizon_exceeded"

    async def test_server_horizon_inf_and_knob_fallback(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """inf from the server means receipts are kept forever: no
        horizon at all (a 404 can only mean never-landed). A raising
        accessor falls back to the knob."""
        stage = await stage_factory()
        flusher = flusher_factory(
            store=store, cfg=make_config(target_output_bytes=1, receipt_horizon_s=60)
        )
        key, crashed_body, pending = await self._crashed_pending(
            httpx_mock, stage, fake_clock, flusher
        )

        class CatalogWithForeverReceipts:
            def _receipt_retention_seconds(self) -> float:
                return float("inf")

        flusher._catalog = CatalogWithForeverReceipts()  # type: ignore[assignment]
        fake_clock.advance(120 * 1_000_000)  # past the knob, "forever" says fine
        script_receipt(httpx_mock, key, snapshot_id=None)
        capture = CommitCapture(httpx_mock, [commit_ok(50)])
        report = await flusher.reconcile_prepared(stage, pending)
        assert report.outcome == "replayed"
        assert capture.bodies == [crashed_body]

        # A broken accessor: the knob governs. Fresh entry, age past the
        # 60s knob → halt.
        stage2 = await stage_factory(partition=1)
        flusher2 = flusher_factory(
            store=store, cfg=make_config(target_output_bytes=1, receipt_horizon_s=60)
        )
        key2, _body2, pending2 = await self._crashed_pending(
            httpx_mock, stage2, fake_clock, flusher2
        )

        class CatalogWithABrokenAccessor:
            def _receipt_retention_seconds(self) -> float:
                raise OSError("the options endpoint is down")

        flusher2._catalog = CatalogWithABrokenAccessor()  # type: ignore[assignment]
        fake_clock.advance(120 * 1_000_000)
        script_receipt(httpx_mock, key2, snapshot_id=None)
        with pytest.raises(FlushHalted) as excinfo:
            await flusher2.reconcile_prepared(stage2, pending2)
        assert excinfo.value.reason == "receipt_horizon_exceeded"

    async def test_the_runner_halts_and_stays_halted_past_the_horizon(
        self,
        httpx_mock: Any,
        memory_store: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """At the loop level: a recovery survivor past the horizon stops
        the pipeline loudly (the halt is sticky), never republishes."""
        cfg = make_config(target_output_bytes=1, receipt_horizon_s=100)
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage = (await manager.open_partition("events", 0)).stage
            flusher = flusher_factory(store=store, cfg=cfg)
            key, _body, _pending = await self._crashed_pending(
                httpx_mock, stage, fake_clock, flusher
            )
            fake_clock.advance(101 * 1_000_000)

            script_receipt(httpx_mock, key, snapshot_id=None)
            r = runner(manager, flusher, cfg, fake_clock, sleeper)
            with pytest.raises(FlushHalted) as first:
                await r.run_once()
            assert r.halted is not None
            assert first.value.reason == "receipt_horizon_exceeded"
            with pytest.raises(FlushHalted) as again:
                await r.run_once()
            assert again.value is first.value  # halted is sticky


# =====================================================================================
# Chunking and the fanout bound
# =====================================================================================


@component
class TestChunking:
    async def test_sweep_commits_stay_within_the_file_bound(
        self,
        httpx_mock: Any,
        memory_store: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """Five teams decide in one sweep (age-triggered, oldest first):
        one commit per (team, window), every commit within
        max_files_per_commit, every team settled."""
        cfg = make_config(target_output_bytes=1, max_files_per_commit=2)
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            opened = await manager.open_partition("events", 0)
            stage = opened.stage
            offset = 0
            for team in (11, 22, 33, 44, 55):
                await staged(
                    stage, fake_clock, team, [(offset, JAN_US), (offset + 1, JAN_US)]
                )
                offset += 2
                fake_clock.advance(1_000_000)  # distinct first_staged_ts per team
            fake_clock.advance(901 * 1_000_000)  # past the age deadline
            script_options(httpx_mock)
            capture = CommitCapture(httpx_mock, [commit_ok(50 + i) for i in range(5)])
            flusher = flusher_factory(store=store, cfg=cfg)

            sweep = await runner(manager, flusher, cfg, fake_clock, sleeper).run_once()

            assert sweep.decisions == 5
            assert sweep.committed == 5
            assert len(capture.bodies) == 5
            # Oldest first (planner order, preserved through chunking)...
            assert [
                p["message"].split(" team=")[1].split(" ")[0] for p in capture.payloads
            ] == [
                "11",
                "22",
                "33",
                "44",
                "55",
            ]
            # ...and every commit honors the file bound.
            assert all(len(p["appends"][0]["files"]) <= 2 for p in capture.payloads)
            assert await stage.iter_key_stats() == []

    async def test_fanout_past_the_bound_halts_before_any_io(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """identity(event) fans one decision out past the bound: a
        spec/bound disagreement is a deployment bug, halted loudly BEFORE
        the first upload (the builder is pure)."""
        spec = {
            "spec_id": 4,
            "fields": [{"source_field_id": 3, "transform": "identity"}],
        }
        cfg = make_config(target_output_bytes=1, max_files_per_commit=2)
        stage = await stage_factory()
        await staged(
            stage,
            fake_clock,
            TEAM,
            [(0, JAN_US), (1, JAN_US), (2, JAN_US)],
            payloads=[
                event_payload(TEAM, JAN_US, event="a"),
                event_payload(TEAM, JAN_US, event="b"),
                event_payload(TEAM, JAN_US, event="c"),
            ],
        )
        script_options(httpx_mock)
        capture = CommitCapture(httpx_mock, [])
        flusher = flusher_factory(store=store, cfg=cfg, table=table_wire(spec=spec))

        with pytest.raises(FlushHalted) as excinfo:
            await flusher.flush_key(
                stage,
                decision(
                    TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US
                ),
            )
        assert excinfo.value.reason == "fanout_exceeds_commit_bound"
        assert store.opened == []  # no upload
        assert capture.bodies == []  # no commit
        assert await stage.load_prepared() == []  # nothing persisted
        assert len(await staged_rows(stage, TEAM)) == 3  # nothing settled


# =====================================================================================
# Recovery handoff through the runner
# =====================================================================================


@component
class TestRecoveryHandoff:
    async def _crashed_partition(
        self,
        httpx_mock: Any,
        manager: StageManager,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> tuple[str, bytes]:
        """A flush whose whole publish ladder dies; the partition keeps a
        persisted prepared entry, unpublished. Returns (key, body)."""
        from millrace.flush import _PUBLISH_MAX_ATTEMPTS

        stage = manager.stage("events", 0)
        await staged(stage, fake_clock, TEAM, [(0, JAN_US), (1, JAN_US)])
        script_options(httpx_mock)
        capture = CommitCapture(
            httpx_mock, [httpx.ConnectError("lost")] * _PUBLISH_MAX_ATTEMPTS
        )
        flusher = flusher_factory(store=store, cfg=make_config(target_output_bytes=1))
        cfg = make_config(target_output_bytes=1)
        sweep = await runner(manager, flusher, cfg, fake_clock, sleeper).run_once()
        assert sweep.failed == 1 and sweep.committed == 0
        pending = await stage.load_prepared()
        assert len(pending) == 1
        assert len(await staged_rows(stage, TEAM)) == 2
        return pending[0].request.idempotency_key, capture.bodies[0]

    async def test_receipt_200_settles_without_republication(
        self,
        httpx_mock: Any,
        memory_store: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            await manager.open_partition("events", 0)
            key, crashed_body = await self._crashed_partition(
                httpx_mock, manager, fake_clock, flusher_factory, store, sleeper
            )

            # "Restart": a NEW runner over the same manager with a fresh
            # flusher; the receipt says the crash-time commit LANDED.
            script_receipt(httpx_mock, key, snapshot_id=50)
            capture = CommitCapture(httpx_mock, [])  # no commit may be sent
            cfg = make_config(target_output_bytes=1)
            flusher2 = flusher_factory(store=store, cfg=cfg)
            sweep = await runner(manager, flusher2, cfg, fake_clock, sleeper).run_once()

            assert sweep.receipt_settled == 1
            assert sweep.decisions == 0  # nothing left to plan
            assert capture.bodies == []
            stage = manager.stage("events", 0)
            assert await staged_rows(stage, TEAM) == []
            assert await stage.load_prepared() == []
            assert await read_markers(stage) == []  # the settle writes no marker
            assert flusher2.stats().receipt_settlements == 1
            del crashed_body

    async def test_receipt_404_republishes_the_persisted_bytes(
        self,
        httpx_mock: Any,
        memory_store: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            await manager.open_partition("events", 0)
            key, crashed_body = await self._crashed_partition(
                httpx_mock, manager, fake_clock, flusher_factory, store, sleeper
            )

            script_receipt(httpx_mock, key, snapshot_id=None)
            capture = CommitCapture(httpx_mock, [commit_ok(50)])
            cfg = make_config(target_output_bytes=1)
            flusher2 = flusher_factory(store=store, cfg=cfg)
            sweep = await runner(manager, flusher2, cfg, fake_clock, sleeper).run_once()

            assert sweep.replayed == 1
            # Byte-identical replay of the persisted request — the
            # idempotency key makes the server dedupe a landing replay.
            assert capture.bodies == [crashed_body]
            stage = manager.stage("events", 0)
            assert await staged_rows(stage, TEAM) == []
            assert flusher2.stats().flushes_replayed == 1


# =====================================================================================
# Wire discipline: TTL, totals=false, read-before-scan
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
        sleeper: Sleeper,
        mono: FakeMonotonic,
    ) -> None:
        """retention 3600s → TTL 900s. Flush B inside the TTL makes NO
        table read; flush C past it re-reads (totals=false) and carries
        the NEW read_snapshot. The retention options are read once."""
        stage = await stage_factory()
        script_options(httpx_mock)
        capture = CommitCapture(
            httpx_mock, [commit_ok(50), commit_ok(51), commit_ok(52)]
        )
        flusher = flusher_factory(store=store, cfg=make_config(target_output_bytes=1))

        offset = 0
        snapshots: list[int] = []
        for advance in (0, 0, RETENTION_S / 4 + 1):
            await staged(
                stage, fake_clock, TEAM, [(offset, JAN_US), (offset + 1, JAN_US)]
            )
            offset += 2
            if advance:
                mono.advance(advance)
                # The refresh returns a moved basis.
                script_table_refresh(httpx_mock, table=table_wire(read_snapshot_id=95))
            report = await flusher.flush_key(
                stage,
                decision(
                    TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US
                ),
            )
            assert report.outcome == "committed"
        snapshots = [p["read_snapshot"] for p in capture.payloads]
        assert snapshots == [88, 88, 95]

        # Exactly two table reads ever (bootstrap + the TTL refresh), both
        # totals=false; the retention read happened exactly once.
        table_reads = [
            r for r in httpx_mock.get_requests() if str(r.url).startswith(TABLE_URL)
        ]
        assert len(table_reads) == 2
        assert all(r.url.params.get("totals") == "false" for r in table_reads)
        assert (
            len(httpx_mock.get_requests(url=f"{BASE}/v1/catalogs/{CATALOG}/options"))
            == 1
        )

    async def test_identity_reads_are_totals_false_everywhere(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """Across the whole flusher surface (bootstrap, refresh), no
        request ever aggregates the manifest."""
        stage = await stage_factory()
        await staged(stage, fake_clock, TEAM, [(0, JAN_US)])
        script_options(httpx_mock)
        CommitCapture(httpx_mock, [commit_ok(50)])
        flusher = flusher_factory(store=store, cfg=make_config(target_output_bytes=1))
        await flusher.flush_key(
            stage,
            decision(TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US),
        )
        for request in httpx_mock.get_requests():
            assert (
                "totals" not in request.url.params
                or request.url.params["totals"] == "false"
            )

    async def test_the_shape_read_precedes_the_staged_scan(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
        mono: FakeMonotonic,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Invariant 12's ordering: the commit's read_snapshot is taken
        BEFORE the staged range is read — even on a cold shape cache, the
        table GET precedes the first row scan."""
        stage = await stage_factory()
        await staged(stage, fake_clock, TEAM, [(0, JAN_US)])
        script_options(httpx_mock)
        journal: list[str] = []

        def table_get(request: httpx.Request) -> httpx.Response:
            journal.append("table_get")
            return httpx.Response(200, json=table_wire(read_snapshot_id=95))

        capture = CommitCapture(httpx_mock, [commit_ok(50)])
        flusher = flusher_factory(store=store, cfg=make_config(target_output_bytes=1))
        mono.advance(RETENTION_S)  # past the TTL: the flush re-reads
        # Registered only after the bootstrap's read was consumed — the
        # scripted server hands the MOVED basis to the refresh.
        httpx_mock.add_callback(
            table_get, method="GET", url=f"{TABLE_URL}?totals=false"
        )

        original_scan = stage.scan_team_rows_bounded

        async def scanning(team_id: int, **kwargs: Any) -> Any:
            journal.append("scan")
            return await original_scan(team_id, **kwargs)

        monkeypatch.setattr(stage, "scan_team_rows_bounded", scanning)
        report = await flusher.flush_key(
            stage,
            decision(TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US),
        )
        assert report.outcome == "committed"
        assert journal == ["table_get", "scan"]
        assert capture.payloads[0]["read_snapshot"] == 95

    async def test_mid_scan_arrivals_are_clipped_out_of_the_window(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A batch landing between the scan and the offsets read is NOT
        flushed (the scan's snapshot predates it) and NOT deleted (the
        window clips to what was scanned): it flushes next time."""
        stage = await stage_factory()
        await staged(stage, fake_clock, TEAM, [(0, JAN_US), (1, JAN_US), (2, JAN_US)])
        script_options(httpx_mock)
        capture = CommitCapture(httpx_mock, [commit_ok(50), commit_ok(51)])
        flusher = flusher_factory(store=store, cfg=make_config(target_output_bytes=1))

        original_read_offsets = stage.read_offsets

        async def read_offsets_with_arrival(team_id: int) -> Any:
            # The consumer lands a new batch in the scan/offsets gap.
            await staged(stage, fake_clock, TEAM, [(3, JAN_US), (4, JAN_US)])
            return await original_read_offsets(team_id)

        monkeypatch.setattr(stage, "read_offsets", read_offsets_with_arrival)
        report = await flusher.flush_key(
            stage,
            decision(TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US),
        )
        assert report.outcome == "committed"
        assert (report.first_offset, report.last_offset) == (0, 2)
        body = capture.payloads[0]
        assert body["message"].splitlines()[1] == (
            "offsets topic=events partition=0 ranges=0-2"
        )
        assert sum(f["record_count"] for f in body["appends"][0]["files"]) == 3
        # The younger rows survived the settle...
        remaining = await staged_rows(stage, TEAM)
        assert [rk.offset for rk, _ in remaining] == [3, 4]
        # ...and flush next, with their own window.
        report2 = await flusher.flush_key(
            stage,
            decision(TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US),
        )
        assert (report2.first_offset, report2.last_offset) == (3, 4)
        assert capture.payloads[1]["message"].splitlines()[1] == (
            "offsets topic=events partition=0 ranges=3-4"
        )
        assert await staged_rows(stage, TEAM) == []


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
            + [{"name": "props", "type": "json", "field_id": 4, "ordinal": 3}]
        )
        with pytest.raises(FlushStartupError, match="'props'"):
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
        """B3: the live sort spec must be honorable at boot — the flusher
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

    async def test_incarnation_flip_at_refresh_halts_before_any_io(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
        mono: FakeMonotonic,
    ) -> None:
        """The pinned incarnation never changes hands mid-run: a refresh
        resolving the name to a new uuid halts before upload or commit."""
        stage = await stage_factory()
        await staged(stage, fake_clock, TEAM, [(0, JAN_US)])
        script_options(httpx_mock)
        capture = CommitCapture(httpx_mock, [])
        from flushkit import RECREATED_UUID

        flusher = flusher_factory(store=store, cfg=make_config(target_output_bytes=1))
        mono.advance(RETENTION_S)  # force the refresh
        script_table_refresh(httpx_mock, table=table_wire(uuid=RECREATED_UUID))
        with pytest.raises(FlushHalted) as excinfo:
            await flusher.flush_key(
                stage,
                decision(
                    TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US
                ),
            )
        assert excinfo.value.reason == "table_recreated"
        assert RECREATED_UUID in excinfo.value.detail
        assert store.opened == [] and capture.bodies == []  # zero I/O happened
        assert len(await staged_rows(stage, TEAM)) == 1  # nothing settled


# =====================================================================================
# The runner: containment, halt, stats repair, prefix replay
# =====================================================================================


@component
class TestRunner:
    async def test_one_decisions_failure_never_wedges_the_sweep(
        self,
        httpx_mock: Any,
        memory_store: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """Team 11's publish ladder exhausts (contained); team 22 commits
        in the same tick; team 11's persisted entry replays next tick."""
        from millrace.flush import _PUBLISH_MAX_ATTEMPTS

        cfg = make_config(target_output_bytes=1)
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage = (await manager.open_partition("events", 0)).stage
            await staged(stage, fake_clock, 11, [(0, JAN_US)])
            fake_clock.advance(1_000_000)
            await staged(stage, fake_clock, 22, [(1, JAN_US)])
            script_options(httpx_mock)
            capture = CommitCapture(
                httpx_mock,
                [httpx.ConnectError("lost")] * _PUBLISH_MAX_ATTEMPTS + [commit_ok(50)],
            )
            flusher = flusher_factory(store=store, cfg=cfg)

            sweep = await runner(manager, flusher, cfg, fake_clock, sleeper).run_once()
            assert (sweep.decisions, sweep.committed, sweep.failed) == (2, 1, 1)
            assert [rk.offset for rk, _ in await staged_rows(stage, 11)] == [0]
            assert await staged_rows(stage, 22) == []

            # Next tick replays team 11's PERSISTED request (receipt 404 →
            # republish), byte-identical to the crashed attempts.
            key = keyspace.idempotency_key(TABLE_UUID, "events", 0, 11, 0, 0)
            script_receipt(httpx_mock, key, snapshot_id=None)
            capture.extend([commit_ok(51)])
            sweep2 = await runner(manager, flusher, cfg, fake_clock, sleeper).run_once()
            assert (sweep2.replayed, sweep2.failed) == (1, 0)
            assert capture.bodies[-1] == capture.bodies[0]
            assert await staged_rows(stage, 11) == []

    async def test_halt_stops_the_pipeline_and_stays_halted(
        self,
        httpx_mock: Any,
        memory_store: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        cfg = make_config(target_output_bytes=1)
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage = (await manager.open_partition("events", 0)).stage
            await staged(stage, fake_clock, TEAM, [(0, JAN_US)])
            script_options(httpx_mock)
            capture = CommitCapture(
                httpx_mock,
                [
                    commit_refusal(
                        409, "table_recreated", "the table was dropped and recreated"
                    )
                ],
            )
            flusher = flusher_factory(store=store, cfg=cfg)
            r = runner(manager, flusher, cfg, fake_clock, sleeper)
            with pytest.raises(FlushHalted) as first:
                await r.run_once()
            assert r.halted is not None
            with pytest.raises(FlushHalted) as again:
                await r.run_once()
            assert again.value is first.value  # halted is sticky
            assert len(capture.bodies) == 1  # the halt gates before any retry
            assert [rk.offset for rk, _ in await staged_rows(stage, TEAM)] == [0]

    async def test_redelivery_inflated_stats_are_repaired_at_the_next_sweep(
        self,
        httpx_mock: Any,
        memory_store: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """A redelivered batch re-puts the same rows (idempotent) but
        inflates the stats counters; the flush settles ACTUALS, and the
        next tick's empty scan repairs the residue instead of letting the
        stale first_staged_ts pin the backpressure age gauge forever."""
        cfg = make_config(target_output_bytes=1)
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage = (await manager.open_partition("events", 0)).stage
            await staged(stage, fake_clock, TEAM, [(0, JAN_US), (1, JAN_US)])
            # The crash-free redelivery: same records, same keys.
            await staged(stage, fake_clock, TEAM, [(0, JAN_US), (1, JAN_US)])
            assert (await stage.iter_key_stats())[0].row_count == 4  # inflated

            script_options(httpx_mock)
            capture = CommitCapture(httpx_mock, [commit_ok(50)])
            flusher = flusher_factory(store=store, cfg=cfg)
            r = runner(manager, flusher, cfg, fake_clock, sleeper)
            sweep = await r.run_once()
            assert (sweep.decisions, sweep.committed) == (1, 1)
            # Two real rows flushed; the inflated counters survive the settle.
            assert (
                sum(
                    f["record_count"]
                    for f in capture.payloads[0]["appends"][0]["files"]
                )
                == 2
            )
            assert (await stage.iter_key_stats())[0].row_count == 2  # residue
            assert await staged_rows(stage, TEAM) == []

            # Next tick: the residue still decides the key, the scan finds
            # nothing, and the stats entry is repaired away — no commit.
            sweep2 = await r.run_once()
            assert (sweep2.decisions, sweep2.committed, sweep2.empty) == (1, 0, 1)
            assert await stage.iter_key_stats() == []
            assert len(capture.bodies) == 1
            assert flusher.stats().stats_repaired == 1

    async def test_a_persisted_entry_settles_its_own_prefix_window(
        self,
        httpx_mock: Any,
        memory_store: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """The persisted entry is authoritative for ITS window: rows that
        arrived after the crash are not covered by the replay and flush
        under their own window afterwards."""
        from millrace.flush import _PUBLISH_MAX_ATTEMPTS

        cfg = make_config(target_output_bytes=1)
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage = (await manager.open_partition("events", 0)).stage
            await staged(stage, fake_clock, TEAM, [(0, JAN_US), (1, JAN_US)])
            script_options(httpx_mock)
            capture = CommitCapture(
                httpx_mock,
                [httpx.ConnectError("lost")] * _PUBLISH_MAX_ATTEMPTS,
            )
            flusher = flusher_factory(store=store, cfg=cfg)
            with pytest.raises(FlushPublishExhausted):
                await flusher.flush_key(
                    stage,
                    decision(
                        TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US
                    ),
                )
            # Newer rows arrive while the entry is unpublished.
            await staged(stage, fake_clock, TEAM, [(2, JAN_US), (3, JAN_US)])

            # The next flush of the team finds the persisted entry and
            # replays it — the entry's window is [0,1], a strict prefix.
            key = keyspace.idempotency_key(TABLE_UUID, "events", 0, TEAM, 0, 1)
            script_receipt(httpx_mock, key, snapshot_id=None)
            capture.extend([commit_ok(50), commit_ok(51)])
            report = await flusher.flush_key(
                stage,
                decision(
                    TEAM, trigger="size", staged_bytes=200, first_staged_ts=JAN_US
                ),
            )
            assert report.outcome == "replayed"
            assert (report.first_offset, report.last_offset) == (0, 1)
            assert capture.bodies[-1] == capture.bodies[0]  # the persisted bytes
            assert [rk.offset for rk, _ in await staged_rows(stage, TEAM)] == [2, 3]

            # ...which flush under their own window next.
            report2 = await flusher.flush_key(
                stage,
                decision(
                    TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US
                ),
            )
            assert report2.outcome == "committed"
            assert (report2.first_offset, report2.last_offset) == (2, 3)
            assert capture.payloads[-1]["message"].splitlines()[1] == (
                "offsets topic=events partition=0 ranges=2-3"
            )
            assert await staged_rows(stage, TEAM) == []


# =====================================================================================
# The stage's flush-time quarantine primitives (stage.py's Phase 4 additions)
# =====================================================================================


@component
class TestStageQuarantinePrimitives:
    async def test_poison_records_are_durable_across_reopen(
        self,
        memory_store: Any,
        stage_factory: Any,
        fake_clock: Any,
    ) -> None:
        stage = await stage_factory()
        await stage.stage_batch(
            [  # one real row so the partition has live state
                __import__("millrace.stage", fromlist=["StagedRecord"]).StagedRecord(
                    team_id=TEAM, event_ts_us=JAN_US, offset=0, payload=b"ok"
                )
            ],
            now_us=fake_clock(),
        )
        written = await stage.poison_records(
            [
                PoisonedRecord(
                    offset=1,
                    reason=REASON_EVENT_TIME_OUT_OF_WINDOW,
                    key=None,
                    value=b"junk",
                    value_bytes_original=4,
                )
            ]
        )
        assert written == 1
        await stage.close()

        stage2 = await stage_factory()
        poisoned = await stage2.scan_poison()
        assert [(p.offset, p.reason, p.value) for p in poisoned] == [
            (1, REASON_EVENT_TIME_OUT_OF_WINDOW, b"junk")
        ]
        # Idempotent under redelivery of the same write.
        await stage2.poison_records(
            [
                PoisonedRecord(
                    offset=1,
                    reason=REASON_EVENT_TIME_OUT_OF_WINDOW,
                    key=None,
                    value=b"junk",
                    value_bytes_original=4,
                )
            ]
        )
        assert len(await stage2.scan_poison()) == 1
        await stage2.close()

    async def test_settle_quarantined_deletes_cleanly(
        self,
        stage_factory: Any,
        fake_clock: Any,
    ) -> None:
        stage = await stage_factory()
        await staged(stage, fake_clock, TEAM, [(0, JAN_US), (1, JAN_US), (2, JAN_US)])
        from millrace.keyspace import PreparedRequest

        await stage.persist_prepared(
            TEAM, 0, PreparedRequest("k" * 36, b"{}", NOW, topic="events", partition=0)
        )

        report = await stage.settle_quarantined(FlushedKey(TEAM, 0, 2))
        assert (report.rows_deleted, report.team_drained) == (3, True)
        assert await staged_rows(stage, TEAM) == []
        assert await stage.iter_key_stats() == []
        assert await stage.load_prepared() == []
        assert await read_markers(stage) == []  # nothing writes flushed/ any more
        # Idempotent replay...
        again = await stage.settle_quarantined(FlushedKey(TEAM, 0, 2))
        assert again.rows_deleted == 0
        # ...and recovery finds nothing to do.
        recovery = await stage.recover()
        assert (
            recovery.pending_prepared == () and recovery.legacy_markers_collected == 0
        )
        await stage.close()


# =====================================================================================
# The new config knobs
# =====================================================================================


class TestFlushConfigKnobs:
    def test_event_time_window_and_sweep_knobs_parse(self) -> None:
        from millrace.config import load_config

        env = {
            "MILLRACE_KAFKA_BOOTSTRAP_SERVERS": "b:9092",
            "MILLRACE_KAFKA_TOPIC": "events",
            "MILLRACE_KAFKA_PARTITIONS": "0",
            "MILLRACE_CATALOG": "prod",
            "MILLRACE_NAMESPACE": "default",
            "MILLRACE_TABLE": "events",
            "MILLRACE_STAGE_URL": "memory:///",
            "MILLRACE_EVENT_TIME_MAX_PAST_S": "3600",
            "MILLRACE_EVENT_TIME_MAX_FUTURE_S": "60",
            "MILLRACE_FLUSH_SWEEP_S": "10",
            "MILLRACE_RECEIPT_HORIZON_S": "432000",
        }
        cfg = load_config(env)
        assert cfg.event_time_max_past_s == 3600
        assert cfg.event_time_max_future_s == 60
        assert cfg.flush_sweep_s == 10
        assert cfg.receipt_horizon_s == 432000

    def test_event_time_window_defaults_and_validation(self) -> None:
        from millrace.config import ConfigError, load_config

        env = {
            "MILLRACE_KAFKA_BOOTSTRAP_SERVERS": "b:9092",
            "MILLRACE_KAFKA_TOPIC": "events",
            "MILLRACE_KAFKA_PARTITIONS": "0",
            "MILLRACE_CATALOG": "prod",
            "MILLRACE_NAMESPACE": "default",
            "MILLRACE_TABLE": "events",
            "MILLRACE_STAGE_URL": "memory:///",
        }
        cfg = load_config(env)
        assert cfg.event_time_max_past_s == 30 * 86400
        assert cfg.event_time_max_future_s == 86400
        assert cfg.flush_sweep_s == 5
        assert cfg.receipt_horizon_s == 6 * 86400
        with pytest.raises(ConfigError):
            load_config(env | {"MILLRACE_EVENT_TIME_MAX_FUTURE_S": "0"})
        with pytest.raises(ConfigError, match="MILLRACE_RECEIPT_HORIZON_S"):
            load_config(env | {"MILLRACE_RECEIPT_HORIZON_S": "0"})

    def test_scan_budget_ratio_and_concurrency_knobs(self) -> None:
        from millrace.config import ConfigError, load_config

        env = {
            "MILLRACE_KAFKA_BOOTSTRAP_SERVERS": "b:9092",
            "MILLRACE_KAFKA_TOPIC": "events",
            "MILLRACE_KAFKA_PARTITIONS": "0",
            "MILLRACE_CATALOG": "prod",
            "MILLRACE_NAMESPACE": "default",
            "MILLRACE_TABLE": "events",
            "MILLRACE_STAGE_URL": "memory:///",
        }
        cfg = load_config(env)
        assert cfg.flush_scan_min_bytes == 64 * 1024 * 1024
        assert cfg.flush_scan_max_rows == 8_000_000
        assert cfg.compression_ratio_halflife == 20
        assert cfg.flush_concurrency == 4

        cfg = load_config(
            env
            | {
                "MILLRACE_FLUSH_SCAN_MIN_BYTES": "1024",
                "MILLRACE_FLUSH_SCAN_MAX_ROWS": "5000",
                "MILLRACE_COMPRESSION_RATIO_HALFLIFE": "7",
                "MILLRACE_FLUSH_CONCURRENCY": "1",
            }
        )
        assert cfg.flush_scan_min_bytes == 1024
        assert cfg.flush_scan_max_rows == 5000
        assert cfg.compression_ratio_halflife == 7
        assert cfg.flush_concurrency == 1

        for knob in (
            "MILLRACE_FLUSH_SCAN_MIN_BYTES",
            "MILLRACE_FLUSH_SCAN_MAX_ROWS",
            "MILLRACE_COMPRESSION_RATIO_HALFLIFE",
            "MILLRACE_FLUSH_CONCURRENCY",
        ):
            with pytest.raises(ConfigError, match=knob):
                load_config(env | {knob: "0"})


# =====================================================================================
# Adversarial pins: the settlement guard, the junk window boundary, the
# pure builder's edge semantics
# =====================================================================================


@component
class TestSettlementGuard:
    async def test_a_settlement_that_disagrees_with_the_scan_halts(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The guard is the last check against losing an unpublished row:
        a settle deleting a row count the flush did not see is a staging-
        invariant violation, and the pipeline halts rather than proceed.
        (Removing the guard turns this test red: no halt is raised.)"""
        stage = await stage_factory()
        await staged(stage, fake_clock, TEAM, [(0, JAN_US), (1, JAN_US)])
        script_options(httpx_mock)
        CommitCapture(httpx_mock, [commit_ok(50)])
        flusher = flusher_factory(store=store, cfg=make_config(target_output_bytes=1))

        real_commit_flushed = stage.commit_flushed

        async def doctored_settle(key: Any) -> Any:
            report = await real_commit_flushed(key)
            return type(report)(
                rows_deleted=report.rows_deleted + 1,  # a row the scan never saw
                bytes_deleted=report.bytes_deleted,
                team_drained=report.team_drained,
            )

        monkeypatch.setattr(stage, "commit_flushed", doctored_settle)
        with pytest.raises(FlushHalted) as excinfo:
            await flusher.flush_key(
                stage,
                decision(
                    TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US
                ),
            )
        assert excinfo.value.reason == "settlement_mismatch"


class TestJunkWindowBoundaries:
    def test_window_bounds_are_inclusive(self) -> None:
        window = flush_mod.JunkWindow(min_us=100, max_us=200)
        assert window.contains(100)
        assert window.contains(200)
        assert not window.contains(99)
        assert not window.contains(201)

    def test_pure_builder_pre_epoch_month_and_null_partition_value(self) -> None:
        """The pure builder: 1969-12 is month -1 (the floored epoch-month
        rule, negative pre-1970), and a null partition source groups into
        the null partition (Iceberg)."""
        from pyhoglake import TableInfo

        spec = {
            "spec_id": 3,
            "fields": [
                {"source_field_id": 1, "transform": "identity"},
                {"source_field_id": 2, "transform": "month"},
            ],
        }
        info = TableInfo.from_wire(
            {
                "name": TABLE,
                "namespace": NAMESPACE,
                "table_uuid": TABLE_UUID,
                "columns": [
                    {"name": "team_id", "type": "long", "field_id": 1, "ordinal": 0},
                    {
                        "name": "timestamp",
                        "type": "timestamp",
                        "field_id": 2,
                        "ordinal": 1,
                    },
                ],
                "partition_spec": spec,
                "read_snapshot_id": 88,
            }
        )
        pre_epoch = -86_400_000_000  # 1969-12-31
        records = [
            (keyspace.RowKey(7, pre_epoch, 0), event_payload(7, pre_epoch)),
            (keyspace.RowKey(7, pre_epoch, 1), b'{"team_id": 7, "timestamp": null}'),
        ]
        ctx = flush_mod.FlushContext(
            topic="events",
            partition=0,
            team_id=7,
            trigger="size",
            first_offset=0,
            last_offset=1,
            offset_ranges=(OffsetRange("events", 0, 0, 1),),
            idempotency_key=keyspace.idempotency_key(TABLE_UUID, "events", 0, 7, 0, 1),
            read_snapshot=88,
            table_uuid=TABLE_UUID,
        )
        # A window wide enough to hold both (the policy is not under test here).
        plan = build_prepared_plan(
            records,
            info,
            ctx=ctx,
            event_time_policy=EventTimePolicy.QUARANTINE,
            event_time_window_us=(-(1 << 62), 1 << 62),
            data_path=DATA_PATH,
            max_files_per_commit=512,
            footer_stats_min_bytes=1,
            version="test",
        )
        assert plan.payload is not None
        files = plan.payload["appends"][0]["files"]
        # team_id "7" with month -1, and the null-timestamp row in the
        # null month partition — the server treats them as opaque strings.
        assert {tuple(f["partition_values"]) for f in files} == {
            ("7", "-1"),
            ("7", None),
        }
        # Determinism: the same inputs build byte-identical payloads.
        plan2 = build_prepared_plan(
            records,
            info,
            ctx=ctx,
            event_time_policy=EventTimePolicy.QUARANTINE,
            event_time_window_us=(-(1 << 62), 1 << 62),
            data_path=DATA_PATH,
            max_files_per_commit=512,
            footer_stats_min_bytes=1,
            version="test",
        )
        assert flush_mod.canonical_body(plan2.payload) == flush_mod.canonical_body(
            plan.payload
        )

    def test_pure_builder_all_quarantined_has_no_payload(self) -> None:
        from pyhoglake import TableInfo

        info = TableInfo.from_wire(
            {
                "name": TABLE,
                "namespace": NAMESPACE,
                "table_uuid": TABLE_UUID,
                "columns": COLUMNS_WIRE,
                "partition_spec": SPEC_WIRE,
                "read_snapshot_id": 88,
            }
        )
        records = [(keyspace.RowKey(7, JAN_US, 0), b"not json")]
        ctx = flush_mod.FlushContext(
            topic="events",
            partition=0,
            team_id=7,
            trigger="age",
            first_offset=0,
            last_offset=0,
            offset_ranges=(OffsetRange("events", 0, 0, 0),),
            idempotency_key=keyspace.idempotency_key(TABLE_UUID, "events", 0, 7, 0, 0),
            read_snapshot=88,
            table_uuid=TABLE_UUID,
        )
        plan = build_prepared_plan(
            records,
            info,
            ctx=ctx,
            event_time_policy=EventTimePolicy.QUARANTINE,
            event_time_window_us=(-(1 << 62), 1 << 62),
            data_path=DATA_PATH,
            max_files_per_commit=512,
            footer_stats_min_bytes=1,
            version="test",
        )
        assert plan.payload is None
        assert len(plan.quarantined) == 1
        assert plan.quarantined[0].reason == REASON_UNDECODABLE_PAYLOAD


@component
class TestRunnerLoop:
    async def test_run_sweeps_until_stopped(
        self,
        httpx_mock: Any,
        memory_store: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """The loop shape: run() ticks until stop(); each tick is one
        planning pass; the injected sleep backs the cadence."""
        cfg = make_config(target_output_bytes=1)
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage = (await manager.open_partition("events", 0)).stage
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
            await r.run()
            assert r.sweeps == 1
            assert cadence == [60.0]
            assert r.last_report.committed == 1
            assert capture.bodies
            assert await staged_rows(stage, TEAM) == []


# =====================================================================================
# M4: the bounded unit of work (post-outage catch-up) + the EWMA ratio feedback
# =====================================================================================


def sized_payload(team: int, ts: int, size: int) -> bytes:
    """A decodable event payload of EXACTLY ``size`` bytes (the scan-cap
    arithmetic in these tests is byte-exact)."""
    import json as _json

    doc: dict[str, Any] = {"team_id": team, "timestamp": ts, "event": ""}
    base = _json.dumps(doc, separators=(",", ":")).encode()
    doc["event"] = "x" * (size - len(base))
    out = _json.dumps(doc, separators=(",", ":")).encode()
    assert len(out) == size
    return out


async def stage_sized_rows(
    stage: Any,
    team: int,
    offsets_ts: list[tuple[int, int]],
    *,
    size: int,
    staged_at: int = JAN_US,
) -> None:
    """Stage rows whose payloads are exactly ``size`` bytes each.
    ``staged_at`` is the staging clock (drives ``first_staged_ts``);
    the payload timestamps are the (ts, offset) key order material."""
    from millrace.stage import StagedRecord

    await stage.stage_batch(
        [
            StagedRecord(
                team_id=team,
                event_ts_us=ts,
                offset=offset,
                payload=sized_payload(team, ts, size),
            )
            for offset, ts in offsets_ts
        ],
        now_us=staged_at,
    )


def message_window(payload: dict[str, Any]) -> tuple[int, int]:
    """The flush window out of a captured commit message."""
    from millrace.flush import parse_message_offsets

    return parse_message_offsets(payload["message"], topic="events", partition=0)


@component
class TestBoundedFlush:
    async def test_catchup_flush_is_bounded_and_drains_over_sweeps(
        self,
        httpx_mock: Any,
        memory_store: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """Stage 10x the byte budget in one team: the first sweep flushes
        exactly the budget's worth (at the 0.2 fallback ratio the budget
        is ceil(800 / 0.2 x 1.25) = 5000 staged bytes -> 50 rows of
        100 B), later sweeps take the next bounded windows — contiguous,
        disjoint, sized by the CURRENT observed ratio — until the slice
        drains. The window identity comes from the scanned rows, never
        the stats snapshot."""
        cfg = make_config(target_output_bytes=800, flush_scan_min_bytes=1)
        total = 500  # 10x the first budget's 50 rows
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage = (await manager.open_partition("events", 0)).stage
            await stage_sized_rows(
                stage, TEAM, [(i, JAN_US + i) for i in range(total)], size=100
            )
            script_options(httpx_mock)
            capture = CommitCapture(httpx_mock, [commit_ok(100 + i) for i in range(40)])
            flusher = flusher_factory(store=store, cfg=cfg)
            r = runner(manager, flusher, cfg, fake_clock, sleeper)

            sweep1 = await r.run_once()
            assert (sweep1.decisions, sweep1.committed, sweep1.failed) == (1, 1, 0)
            # The window is the SCANNED prefix's — the stats snapshot's
            # whole 500-row slice would read offsets 0-499 here.
            assert message_window(capture.payloads[0]) == (0, 49)
            assert capture.payloads[0]["idempotency_key"] == keyspace.idempotency_key(
                TABLE_UUID, "events", 0, TEAM, 0, 49
            )
            assert len(await staged_rows(stage, TEAM)) == total - 50

            windows = [(0, 49)]
            for _ in range(40):
                remaining = await staged_rows(stage, TEAM)
                if not remaining:
                    break
                staged_bytes_left = sum(len(p) for _, p in remaining)
                estimate = flusher.compression_ratio
                if staged_bytes_left * estimate < 800:
                    # The size lane can no longer fire on the residue;
                    # the slow lane drains it (any size).
                    fake_clock.advance(6 * 3600 * 1_000_000)
                sweep = await r.run_once()
                assert sweep.failed == 0
                assert sweep.committed == 1
                # The budget tracks the CURRENT estimate; the window is
                # the next contiguous chunk and never exceeds it.
                budget = flusher._scan_budget_bytes()
                first, last = message_window(capture.payloads[len(windows)])
                assert first == windows[-1][1] + 1  # contiguous, disjoint
                assert 0 < last - first < budget // 100 + 1
                windows.append((first, last))
            assert windows[-1][1] == total - 1  # drained to the end
            assert await stage.iter_key_stats() == []

    async def test_a_timestamp_inversion_at_the_cap_boundary_narrows_the_window(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """The capped scan is a prefix in (timestamp, offset) order, and
        record timestamps are not offset-monotone: row 5 here carries
        the LATEST timestamp, so the 50-row cap leaves it unscanned while
        its offset sits inside the scanned span [0, 50]. Settling the
        naive [min, max] window would delete that unpublished row (the
        guard would halt): the window must NARROW to the covered prefix
        [0, 4], and the leftover re-decides later."""
        cfg = make_config(target_output_bytes=800, flush_scan_min_bytes=1)
        stage = await stage_factory()
        rows = [(i, JAN_US + i) for i in range(51) if i != 5]
        rows.append((5, JAN_US + 1_000_000))  # the inversion: sorts last
        await stage_sized_rows(stage, TEAM, rows, size=100)
        script_options(httpx_mock)
        capture = CommitCapture(httpx_mock, [commit_ok(100 + i) for i in range(10)])
        flusher = flusher_factory(store=store, cfg=cfg)

        report = await flusher.flush_key(
            stage,
            decision(TEAM, trigger="size", staged_bytes=5100, first_staged_ts=JAN_US),
        )
        assert report.outcome == "committed"
        assert report.records == 5  # the covered prefix, not the 50 scanned
        assert message_window(capture.payloads[0]) == (0, 4)
        assert capture.payloads[0]["idempotency_key"] == keyspace.idempotency_key(
            TABLE_UUID, "events", 0, TEAM, 0, 4
        )
        assert len(await staged_rows(stage, TEAM)) == 46

        # Drain: each flush publishes exactly the rows its window covers
        # AT SETTLE TIME (earlier windows' rows are already gone, so a
        # later window may SPAN their offsets — the identity material in
        # the message records the true ranges). The check that matters:
        # the published row sets tile the slice exactly, once each.
        published_ts: list[int] = []
        total_records = 5
        for _ in range(10):
            if not await staged_rows(stage, TEAM):
                break
            report = await flusher.flush_key(
                stage,
                decision(
                    TEAM, trigger="size", staged_bytes=4600, first_staged_ts=JAN_US
                ),
            )
            assert report.outcome == "committed"
            total_records += report.records
        assert total_records == 51  # every row published exactly once
        assert await staged_rows(stage, TEAM) == []
        for payload in capture.payloads:
            for f in payload["appends"][0]["files"]:
                table = parquet_rows(store, f["path"])
                published_ts.extend(
                    ts_micros(row["timestamp"]) for row in table.to_pylist()
                )
        assert sorted(published_ts) == sorted(
            [JAN_US + i for i in range(51) if i != 5] + [JAN_US + 1_000_000]
        )

    async def test_interleaved_teams_flush_full_windows_when_untruncated(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """Two teams staged in ONE interleaved batch: the team's
        ``offsets/`` span (batch min..max) names the other team's
        offsets too, so the record is NOT the exact staged set — an
        UNtruncated scan must not narrow against it (that shrunk every
        flush to a one-row window, the live-suite trickle). The window
        of a complete scan is the scan's own span; settlement deletes
        exactly this team's rows inside it."""
        from millrace.stage import StagedRecord

        stage = await stage_factory()
        # One batch, teams alternating per offset — the consumer's
        # per-poll shape on any multi-tenant partition.
        await stage.stage_batch(
            [
                StagedRecord(
                    team_id=TEAM + (i % 2),
                    event_ts_us=JAN_US + i,
                    offset=i,
                    payload=event_payload(TEAM + (i % 2), JAN_US + i),
                )
                for i in range(8)
            ],
            now_us=fake_clock(),
        )
        # The arrival-structure record over-covers by construction.
        assert await stage.read_offsets(TEAM) == (OffsetRange("events", 0, 0, 6),)
        script_options(httpx_mock)
        capture = CommitCapture(httpx_mock, [commit_ok(100), commit_ok(101)])
        flusher = flusher_factory(store=store, cfg=make_config())

        report = await flusher.flush_key(
            stage,
            decision(TEAM, trigger="age", staged_bytes=400, first_staged_ts=JAN_US),
        )
        assert report.outcome == "committed"
        assert report.records == 4  # every staged row of the team, not a prefix
        assert (report.first_offset, report.last_offset) == (0, 6)
        assert message_window(capture.payloads[0]) == (0, 6)
        assert await staged_rows(stage, TEAM) == []  # drained in one flush
        assert [rk.offset for rk, _ in await staged_rows(stage, TEAM + 1)] == [
            1,
            3,
            5,
            7,
        ]

        report2 = await flusher.flush_key(
            stage,
            decision(TEAM + 1, trigger="age", staged_bytes=400, first_staged_ts=JAN_US),
        )
        assert report2.records == 4
        assert (report2.first_offset, report2.last_offset) == (1, 7)
        assert await staged_rows(stage, TEAM + 1) == []

    async def test_interleaved_truncated_scans_drain_exactly(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """The narrowing's remaining job: a scan a cap cut short on an
        interleaved partition. The over-covering record makes the
        covered-prefix walk conservative (windows shrink below the true
        prefix — sound, never settling an unscanned row), and the drain
        still publishes every row exactly once."""
        cfg = make_config(target_output_bytes=100, flush_scan_min_bytes=1)
        # budget = ceil(100 / 0.2 x 1.25) = 625 staged bytes -> 6 rows of
        # 100 B per scan; each team has 12 interleaved rows.
        stage = await stage_factory()
        records = []
        for i in range(24):
            records.append((TEAM + (i % 2), i, JAN_US + i))
        for team in (TEAM, TEAM + 1):
            await stage_sized_rows(
                stage,
                team,
                [(o, t) for t2, o, t in records if t2 == team],
                size=100,
            )
        script_options(httpx_mock)
        capture = CommitCapture(httpx_mock, [commit_ok(100 + i) for i in range(60)])
        flusher = flusher_factory(store=store, cfg=cfg)

        for team in (TEAM, TEAM + 1):
            published: list[int] = []
            for _ in range(60):
                if not await staged_rows(stage, team):
                    break
                report = await flusher.flush_key(
                    stage,
                    decision(
                        team, trigger="size", staged_bytes=625, first_staged_ts=JAN_US
                    ),
                )
                assert report.outcome == "committed"
                assert report.records >= 1
                for f in capture.payloads[-1]["appends"][0]["files"]:
                    for row in parquet_rows(store, f["path"]).to_pylist():
                        published.append(ts_micros(row["timestamp"]))
            # Every row published exactly once, nothing staged left.
            assert sorted(published) == [
                JAN_US + i for i in range(24) if i % 2 == team - TEAM
            ]
            assert await staged_rows(stage, team) == []
            assert await stage.read_team_stats(team) is None

    async def test_a_single_row_larger_than_the_byte_budget_still_flushes(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """The byte cap must not wedge a key whose FIRST row alone
        exceeds it: the flush probes one row (a staged record is bounded
        by the broker's message limit) instead of reporting a spurious
        empty scan."""
        cfg = make_config(target_output_bytes=800, flush_scan_min_bytes=1)
        stage = await stage_factory()
        await stage_sized_rows(stage, TEAM, [(0, JAN_US), (1, JAN_US + 1)], size=6000)
        script_options(httpx_mock)
        capture = CommitCapture(httpx_mock, [commit_ok(50), commit_ok(51)])
        flusher = flusher_factory(store=store, cfg=cfg)

        report = await flusher.flush_key(
            stage,
            decision(TEAM, trigger="size", staged_bytes=12000, first_staged_ts=JAN_US),
        )
        assert report.outcome == "committed"  # not the spurious "empty"
        assert report.records == 1
        assert (report.first_offset, report.last_offset) == (0, 0)
        assert len(await staged_rows(stage, TEAM)) == 1
        report2 = await flusher.flush_key(
            stage,
            decision(TEAM, trigger="size", staged_bytes=6000, first_staged_ts=JAN_US),
        )
        assert (report2.first_offset, report2.last_offset) == (1, 1)
        assert len(capture.bodies) == 2
        assert await staged_rows(stage, TEAM) == []

    def test_covered_window_pure_cases(self) -> None:
        """The narrowing arithmetic, without I/O: full coverage returns
        the scan's own span; a hole ends the window below it; ranges
        outside the span clip away; gaps between ranges (other teams,
        quarantines) need no coverage."""
        r = lambda a, b: OffsetRange("events", 0, a, b)
        # No inversion: the window is the scan's span.
        assert flush_mod._covered_window([3, 1, 2], [r(1, 3)]) == (1, 3)
        # A hole at 4 (staged, unscanned): the window ends just below
        # it — K=3 names no row (a gap between ranges needs no
        # coverage), and settling [1, 3] deletes exactly {1, 2, 3}.
        assert flush_mod._covered_window([1, 2, 3, 5, 6], [r(1, 6)]) == (1, 3)
        # The hole can be the first offset of a later range.
        assert flush_mod._covered_window([1, 2, 7], [r(1, 2), r(4, 5), r(7, 7)]) == (
            1,
            3,
        )
        # Gaps BETWEEN ranges are not staged rows and need no coverage.
        assert flush_mod._covered_window([2, 3, 7], [r(2, 3), r(7, 9)]) == (2, 7)
        # A range clipped by the span contributes only its overlap.
        assert flush_mod._covered_window([5, 6], [r(0, 100)]) == (5, 6)
        with pytest.raises(ValueError):
            flush_mod._covered_window([], [r(0, 1)])

    async def test_an_outstanding_prepared_entry_wins_over_a_moved_scan_prefix(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """The M4 safety pin: with a BOUNDED scan, the prefix's minimum
        offset can move under timestamp-inverted arrivals — here rows 2
        and 3 arrive after the crash with an EARLIER timestamp, so the
        capped scan starts at offset 2 while the persisted entry's
        window [0, 1] is still outstanding. Keying the
        outstanding-entry lookup by the scan's first offset would miss
        it and build a window overlapping it (row 0 or 1 published
        twice). The lookup is per-team: the entry is authoritative."""
        from millrace.flush import _PUBLISH_MAX_ATTEMPTS

        # budget = ceil(40 / 0.2 x 1.25) = 300 staged bytes = 3 rows.
        cfg = make_config(target_output_bytes=40, flush_scan_min_bytes=1)
        stage = await stage_factory()
        await stage_sized_rows(stage, TEAM, [(0, JAN_US), (1, JAN_US)], size=100)
        script_options(httpx_mock)
        capture = CommitCapture(
            httpx_mock, [httpx.ConnectError("lost")] * _PUBLISH_MAX_ATTEMPTS
        )
        flusher = flusher_factory(store=store, cfg=cfg)
        # The crash: uploaded, persisted, never answered — entry (7, 0)
        # covers the window [0, 1].
        with pytest.raises(FlushPublishExhausted):
            await flusher.flush_key(
                stage,
                decision(
                    TEAM, trigger="size", staged_bytes=200, first_staged_ts=JAN_US
                ),
            )
        assert len(capture.bodies) == _PUBLISH_MAX_ATTEMPTS

        # Post-crash arrivals with EARLIER event timestamps: the capped
        # scan's prefix no longer starts at the entry's offset.
        await stage_sized_rows(
            stage, TEAM, [(2, JAN_US - 10), (3, JAN_US - 9)], size=100
        )
        scanned = [
            rk.offset async for rk, _ in stage.scan_team_rows(TEAM, max_bytes=250)
        ]
        assert scanned[:2] == [2, 3]  # the moved prefix (white-box check)

        key = keyspace.idempotency_key(TABLE_UUID, "events", 0, TEAM, 0, 1)
        script_receipt(httpx_mock, key, snapshot_id=77)  # it landed
        report = await flusher.flush_key(
            stage,
            decision(TEAM, trigger="size", staged_bytes=400, first_staged_ts=JAN_US),
        )
        # The entry won: receipt-settled, zero republication, and NO new
        # window was built over the moved scan's rows.
        assert report.outcome == "receipt_settled"
        assert (report.first_offset, report.last_offset) == (0, 1)
        assert len(capture.bodies) == _PUBLISH_MAX_ATTEMPTS  # no new publish
        assert [rk.offset for rk, _ in await staged_rows(stage, TEAM)] == [2, 3]

        # ...and the leftovers flush under their own window next.
        capture.extend([commit_ok(78)])
        report2 = await flusher.flush_key(
            stage,
            decision(TEAM, trigger="size", staged_bytes=200, first_staged_ts=JAN_US),
        )
        assert report2.outcome == "committed"
        assert (report2.first_offset, report2.last_offset) == (2, 3)


# =====================================================================================
# The observed compression ratio (EWMA feedback into the planner's size lane)
# =====================================================================================


class TestCompressionRatioEwma:
    def test_fallback_until_the_first_observation(self) -> None:
        from millrace.planner import DEFAULT_COMPRESSION_RATIO

        ewma = flush_mod.CompressionRatioEwma(halflife=20)
        assert ewma.estimate == DEFAULT_COMPRESSION_RATIO
        assert ewma.observations == 0

    def test_first_observation_replaces_the_fallback(self) -> None:
        ewma = flush_mod.CompressionRatioEwma(halflife=20)
        assert ewma.observe(parquet_bytes=50, staged_bytes=100) == 0.5
        assert ewma.estimate == 0.5  # no blending with a made-up constant
        assert ewma.observations == 1

    def test_converges_geometrically_toward_a_constant_truth(self) -> None:
        ewma = flush_mod.CompressionRatioEwma(halflife=10)
        ewma.observe(parquet_bytes=90, staged_bytes=100)  # start at 0.9
        for _ in range(10):
            ewma.observe(parquet_bytes=10, staged_bytes=100)
        # After one halflife the initial error (0.8) halves.
        assert ewma.estimate == pytest.approx(0.1 + 0.8 * 0.5, rel=1e-9)
        for _ in range(10):
            ewma.observe(parquet_bytes=10, staged_bytes=100)
        assert ewma.estimate == pytest.approx(0.1 + 0.8 * 0.25, rel=1e-9)
        for _ in range(40):
            ewma.observe(parquet_bytes=10, staged_bytes=100)
        assert abs(ewma.estimate - 0.1) < 0.05  # bounded error, converging

    def test_broken_observations_never_poison_the_estimate(self) -> None:
        ewma = flush_mod.CompressionRatioEwma(halflife=20)
        assert ewma.observe(parquet_bytes=0, staged_bytes=100) is None
        assert ewma.observe(parquet_bytes=100, staged_bytes=0) is None
        assert ewma.observations == 0
        ewma.observe(parquet_bytes=30, staged_bytes=100)
        assert ewma.observe(parquet_bytes=0, staged_bytes=0) is None
        assert ewma.estimate == 0.3

    def test_a_ratio_above_one_clamps_into_the_planners_domain(self) -> None:
        # Parquet larger than the staged bytes (incompressible payload +
        # format overhead): the planner refuses r > 1, so the estimate
        # must never carry one.
        ewma = flush_mod.CompressionRatioEwma(halflife=20)
        assert ewma.observe(parquet_bytes=400, staged_bytes=100) == 1.0
        assert ewma.estimate == 1.0

    def test_reset_returns_to_the_fallback(self) -> None:
        from millrace.planner import DEFAULT_COMPRESSION_RATIO

        ewma = flush_mod.CompressionRatioEwma(halflife=20)
        ewma.observe(parquet_bytes=80, staged_bytes=100)
        ewma.reset()
        assert ewma.estimate == DEFAULT_COMPRESSION_RATIO
        assert ewma.observations == 0
        ewma.observe(parquet_bytes=10, staged_bytes=100)
        assert ewma.estimate == 0.1  # and the first observation sets again

    def test_halflife_validation(self) -> None:
        with pytest.raises(ValueError):
            flush_mod.CompressionRatioEwma(halflife=0)


@component
class TestCompressionRatioFeedback:
    async def test_the_estimate_is_the_fallback_until_the_first_flush(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        from millrace.planner import DEFAULT_COMPRESSION_RATIO

        stage = await stage_factory()
        await staged(stage, fake_clock, TEAM, [(0, JAN_US)])
        script_options(httpx_mock)
        CommitCapture(httpx_mock, [commit_ok(50)])
        flusher = flusher_factory(store=store, cfg=make_config(target_output_bytes=1))
        assert flusher.compression_ratio == DEFAULT_COMPRESSION_RATIO
        await flusher.flush_key(
            stage,
            decision(TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US),
        )
        # One real observation landed; the estimate left the fallback.
        assert flusher.compression_ratio != DEFAULT_COMPRESSION_RATIO

    async def test_the_observed_ratio_is_parquet_over_staged_bytes(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        stage = await stage_factory()
        await stage_sized_rows(
            stage, TEAM, [(i, JAN_US + i) for i in range(5)], size=100
        )
        script_options(httpx_mock)
        capture = CommitCapture(httpx_mock, [commit_ok(50), commit_ok(51)])
        flusher = flusher_factory(
            store=store,
            cfg=make_config(target_output_bytes=1, compression_ratio_halflife=20),
        )
        await flusher.flush_key(
            stage,
            decision(TEAM, trigger="size", staged_bytes=500, first_staged_ts=JAN_US),
        )
        # First observation: the estimate IS the realized ratio.
        r1 = min(1.0, sum(len(v) for v in store.files.values()) / 500)
        assert flusher.compression_ratio == pytest.approx(r1)

        # Second flush blends in with the halflife decay.
        files_after_1 = dict(store.files)
        await stage_sized_rows(
            stage, TEAM, [(i, JAN_US + i) for i in range(5, 10)], size=100
        )
        await flusher.flush_key(
            stage,
            decision(TEAM, trigger="size", staged_bytes=500, first_staged_ts=JAN_US),
        )
        assert len(capture.bodies) == 2
        r2 = min(
            1.0,
            sum(len(v) for k, v in store.files.items() if k not in files_after_1) / 500,
        )
        decay = 0.5 ** (1 / 20)
        assert flusher.compression_ratio == pytest.approx(decay * r1 + (1 - decay) * r2)

    async def test_a_shape_change_resets_the_estimate(
        self,
        httpx_mock: Any,
        stage_factory: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
        mono: FakeMonotonic,
    ) -> None:
        """A schema/spec change makes the old ratio observations a
        different quantity: the EWMA resets to the fallback. (An
        INCARNATION change halts instead — the fingerprint change here
        is a column added.)"""
        from millrace.planner import DEFAULT_COMPRESSION_RATIO

        stage = await stage_factory()
        await staged(stage, fake_clock, TEAM, [(0, JAN_US)])
        script_options(httpx_mock)
        CommitCapture(httpx_mock, [commit_ok(50)])
        flusher = flusher_factory(store=store, cfg=make_config(target_output_bytes=1))
        await flusher.flush_key(
            stage,
            decision(TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US),
        )
        observed = flusher.compression_ratio
        assert observed != DEFAULT_COMPRESSION_RATIO

        # A refresh with the SAME shape does not reset.
        mono.advance(901)  # past the TTL (retention 3600 / 4)
        script_table_refresh(httpx_mock)
        await flusher._io(flusher._shape_sync)
        assert flusher.compression_ratio == observed

        # A refresh with a CHANGED shape (a new column) resets.
        mono.advance(901)
        script_table_refresh(
            httpx_mock,
            table=table_wire(
                columns=COLUMNS_WIRE
                + [
                    {
                        "name": "extra",
                        "type": "string",
                        "field_id": 4,
                        "ordinal": 3,
                        "nullable": True,
                    }
                ]
            ),
        )
        await flusher._io(flusher._shape_sync)
        assert flusher.compression_ratio == DEFAULT_COMPRESSION_RATIO

    async def test_the_sweep_feeds_the_observed_ratio_to_the_planner(
        self,
        httpx_mock: Any,
        memory_store: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from millrace.planner import DEFAULT_COMPRESSION_RATIO

        cfg = make_config(target_output_bytes=1)
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage = (await manager.open_partition("events", 0)).stage
            await staged(stage, fake_clock, TEAM, [(0, JAN_US)])
            await staged(stage, fake_clock, TEAM + 1, [(1, JAN_US)])
            script_options(httpx_mock)
            CommitCapture(httpx_mock, [commit_ok(50), commit_ok(51)])
            flusher = flusher_factory(store=store, cfg=cfg)
            # Move the estimate off the fallback with a direct flush.
            await flusher.flush_key(
                stage,
                decision(
                    TEAM, trigger="size", staged_bytes=100, first_staged_ts=JAN_US
                ),
            )
            moved = flusher.compression_ratio
            assert moved != DEFAULT_COMPRESSION_RATIO

            captured: dict[str, Any] = {}
            real_plan_flush = flush_mod.plan_flush

            def spy_plan_flush(stats: Any, now_us: int, knobs: Any, **kwargs: Any):
                captured.update(kwargs)
                return real_plan_flush(stats, now_us, knobs, **kwargs)

            monkeypatch.setattr(flush_mod, "plan_flush", spy_plan_flush)
            sweep = await runner(manager, flusher, cfg, fake_clock, sleeper).run_once()
            assert sweep.committed == 1  # team TEAM+1 flushes
            assert captured["estimated_compression_ratio"] == moved


# =====================================================================================
# M6: the age/slow readiness path reads the sched_age/ index; the sweep's
# stats scan is shared with the gauges
# =====================================================================================


@component
class TestReadinessRouting:
    async def test_the_sweep_reads_age_readiness_through_the_index(
        self,
        httpx_mock: Any,
        memory_store: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The age/slow lanes' candidate set is the ``sched_age/`` prefix
        scan through the age-lane cutoff, with a point-read per
        candidate; the size lane keeps the full stats scan. Team A is
        age-ready (old, past the floor), team B size-ready (big, fresh),
        team C neither."""
        cfg = make_config(
            target_output_bytes=100_000,  # B's 2 MiB x 0.2 clears it
            min_flush_bytes=10,  # A's 100 B clears the age lane's floor
        )
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage = (await manager.open_partition("events", 0)).stage
            team_a, team_b, team_c = 11, 22, 33
            await staged(stage, fake_clock, team_a, [(0, JAN_US)])
            fake_clock.advance(901 * 1_000_000)  # past the age deadline
            await stage_sized_rows(
                stage,
                team_b,
                [(1, JAN_US + 1), (2, JAN_US + 2)],
                size=1 << 20,
                staged_at=fake_clock(),
            )
            await staged(stage, fake_clock, team_c, [(3, JAN_US + 3)])

            index_calls: list[int] = []
            point_reads: list[int] = []
            real_index = stage.iter_sched_age_through
            real_read = stage.read_team_stats

            async def spy_index(cutoff: int):
                index_calls.append(cutoff)
                return await real_index(cutoff)

            async def spy_read(team_id: int):
                point_reads.append(team_id)
                return await real_read(team_id)

            monkeypatch.setattr(stage, "iter_sched_age_through", spy_index)
            monkeypatch.setattr(stage, "read_team_stats", spy_read)

            script_options(httpx_mock)
            capture = CommitCapture(httpx_mock, [commit_ok(50), commit_ok(51)])
            flusher = flusher_factory(store=store, cfg=cfg)
            sweep = await runner(manager, flusher, cfg, fake_clock, sleeper).run_once()

            assert sweep.committed == 2
            assert sweep.failed == 0
            # The index scan ran once, through the age-lane cutoff...
            assert index_calls == [fake_clock() - 900 * 1_000_000]
            # ...and only the age-eligible team was point-read.
            assert point_reads == [team_a]
            # A rode the age lane, B the size lane, C is still staged.
            by_team = {
                int(p["message"].split(" team=")[1].split(" ")[0]): p
                for p in capture.payloads
            }
            assert "trigger=age" in by_team[team_a]["message"]
            assert "trigger=size" in by_team[team_b]["message"]
            assert len(await staged_rows(stage, team_c)) == 1

    async def test_a_size_only_sweep_makes_no_index_point_reads(
        self,
        httpx_mock: Any,
        memory_store: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """No age-eligible candidates -> no point reads at all (the
        index scan still runs — it is how the sweep KNOWS there are
        none)."""
        cfg = make_config(target_output_bytes=1)
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage = (await manager.open_partition("events", 0)).stage
            await staged(stage, fake_clock, TEAM, [(0, JAN_US)])
            point_reads: list[int] = []
            real_read = stage.read_team_stats

            async def spy_read(team_id: int):
                point_reads.append(team_id)
                return await real_read(team_id)

            monkeypatch.setattr(stage, "read_team_stats", spy_read)
            script_options(httpx_mock)
            CommitCapture(httpx_mock, [commit_ok(50)])
            flusher = flusher_factory(store=store, cfg=cfg)
            sweep = await runner(manager, flusher, cfg, fake_clock, sleeper).run_once()
            assert sweep.committed == 1
            assert point_reads == []

    async def test_a_candidate_without_stats_is_corruption_not_a_skip(
        self,
        httpx_mock: Any,
        memory_store: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A ``sched_age/`` entry with no ``stats/`` entry is drift the
        stage never produces (the two are written and removed in one
        batch) — the sweep refuses loudly rather than silently skipping
        the key."""
        cfg = make_config(target_output_bytes=1)
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage = (await manager.open_partition("events", 0)).stage
            await staged(stage, fake_clock, TEAM, [(0, JAN_US)])
            fake_clock.advance(901 * 1_000_000)  # age-eligible
            # No HTTP scripted: the assembly must refuse BEFORE any
            # flush needs the shape or the commit path.
            flusher = flusher_factory(store=store, cfg=cfg)

            async def drifted_read(team_id: int):
                return None

            monkeypatch.setattr(stage, "read_team_stats", drifted_read)
            with pytest.raises(StageCorruptionError, match="sched_age"):
                await runner(manager, flusher, cfg, fake_clock, sleeper).run_once()
            assert await staged_rows(stage, TEAM) != []  # nothing flushed away

    async def test_a_partition_skipped_by_reconciliation_still_publishes_its_gauges(
        self,
        httpx_mock: Any,
        memory_store: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
        mono: FakeMonotonic,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A partition whose reconciliation fails transiently is skipped
        for the tick, but its staged bytes must NOT vanish from the
        published gauges — the consumer's backpressure reads them."""
        cfg = make_config(target_output_bytes=1)
        async with StageManager(
            "memory:///",
            "millrace",
            settings=fast_flush_settings(),
            monotonic=mono,
        ) as manager:
            stage = (await manager.open_partition("events", 0)).stage
            await staged(stage, fake_clock, TEAM, [(0, JAN_US)])

            async def broken_recover():
                raise ConnectionError("stage store hiccup")

            monkeypatch.setattr(stage, "recover", broken_recover)
            flusher = flusher_factory(store=store, cfg=cfg)
            sweep = await runner(manager, flusher, cfg, fake_clock, sleeper).run_once()
            assert sweep.failed == 1 and sweep.committed == 0
            served = await manager.gauges(max_staleness_s=1000.0)
            assert served.staged_rows == 1  # the skipped partition's row
            assert ("events", 0) in served.per_partition

    async def test_the_sweep_publishes_the_gauges_from_its_own_scan(
        self,
        httpx_mock: Any,
        memory_store: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
        mono: FakeMonotonic,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """M6(b): the sweep's planner scan doubles as the gauge scan —
        after one sweep the manager serves the published fold without
        rescanning, until the snapshot ages out."""
        cfg = make_config(target_output_bytes=1)
        async with StageManager(
            "memory:///",
            "millrace",
            settings=fast_flush_settings(),
            monotonic=mono,
        ) as manager:
            stage = (await manager.open_partition("events", 0)).stage
            await staged(stage, fake_clock, TEAM, [(0, JAN_US)])
            scans = 0
            real_scan = stage.iter_key_stats

            async def spy_scan():
                nonlocal scans
                scans += 1
                return await real_scan()

            monkeypatch.setattr(stage, "iter_key_stats", spy_scan)
            script_options(httpx_mock)
            CommitCapture(httpx_mock, [commit_ok(50)])
            flusher = flusher_factory(store=store, cfg=cfg)
            sweep = await runner(manager, flusher, cfg, fake_clock, sleeper).run_once()
            assert sweep.committed == 1
            assert scans == 1  # the sweep's one scan

            # The published fold is the sweep's planner input (the row
            # was still staged when scanned — execution follows the
            # publish by construction of run_once).
            served = await manager.gauges(max_staleness_s=1000.0)
            assert scans == 1  # no rescan: the published snapshot served
            assert served.staged_rows == 1
            # Two rows staged after the sweep are INVISIBLE while the
            # snapshot is fresh (the staleness bound is the price of
            # the sharing)...
            await staged(stage, fake_clock, TEAM + 1, [(1, JAN_US), (2, JAN_US)])
            assert (await manager.gauges(max_staleness_s=1000.0)).staged_rows == 1
            # ...and visible once it ages out (the live fallback).
            mono.advance(1001.0)
            assert (await manager.gauges(max_staleness_s=1000.0)).staged_rows == 2
            assert scans == 2


# =====================================================================================
# M5: bound-parallel flush execution
# =====================================================================================


@component
class TestParallelSweep:
    @pytest.mark.parametrize("concurrency", [2, 4])
    async def test_the_process_wide_cap_bounds_in_flight(
        self,
        concurrency: int,
        httpx_mock: Any,
        memory_store: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The sweep's global bound is 4x the per-partition cap: with 5
        partitions x 8 teams (per-partition caps would allow 5xC), at
        most 4xC decisions are ever in flight — each holds its bounded
        scan in memory, so the bound is what ties sweep memory to
        config. Everything commits once the gate opens."""
        cfg = make_config(target_output_bytes=1)
        partitions, teams_per = 5, 8
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stages = []
            for p in range(partitions):
                stages.append((await manager.open_partition("events", p)).stage)
            for p, stage in enumerate(stages):
                for t in range(teams_per):
                    await staged(
                        stage,
                        fake_clock,
                        p * teams_per + t,
                        [(t, JAN_US)],
                    )
            script_options(httpx_mock)
            capture = CommitCapture(httpx_mock, [commit_ok(50 + i) for i in range(40)])
            flusher = flusher_factory(store=store, cfg=cfg)

            gate = asyncio.Event()
            inflight = 0
            peak = 0
            real_flush_key = flusher.flush_key

            async def gated_flush_key(stage: Any, dec: Any):
                nonlocal inflight, peak
                inflight += 1
                peak = max(peak, inflight)
                try:
                    await gate.wait()
                    return await real_flush_key(stage, dec)
                finally:
                    inflight -= 1

            monkeypatch.setattr(flusher, "flush_key", gated_flush_key)
            r = FlushRunner(
                manager,
                flusher,
                cfg.planner_knobs(),
                now_us=fake_clock,
                sleep=sleeper,
                sweep_s=60.0,
                concurrency=concurrency,
            )
            sweep_task = asyncio.create_task(r.run_once())
            # Let the pool saturate against the gate.
            for _ in range(500):
                await asyncio.sleep(0.01)
                if peak >= 4 * concurrency:
                    break
            assert peak == 4 * concurrency  # the global bound, not 5xC
            gate.set()
            sweep = await asyncio.wait_for(sweep_task, timeout=30)
            assert sweep.committed == 40 and sweep.failed == 0
            assert len(capture.bodies) == 40

    async def test_decisions_overlap_within_the_per_partition_cap(
        self,
        httpx_mock: Any,
        memory_store: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """One team's flush blocks; with concurrency=2 the same
        partition's second team (and the other partition's team) still
        commit, and the in-flight count per partition never exceeds the
        cap. (Run serially this test deadlocks on the blocked team.)"""
        cfg = make_config(target_output_bytes=1)
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage0 = (await manager.open_partition("events", 0)).stage
            stage1 = (await manager.open_partition("events", 1)).stage
            await staged(stage0, fake_clock, 11, [(0, JAN_US)])
            await staged(stage0, fake_clock, 12, [(1, JAN_US)])
            await staged(stage1, fake_clock, 21, [(0, JAN_US)])
            script_options(httpx_mock)
            capture = CommitCapture(
                httpx_mock, [commit_ok(50), commit_ok(51), commit_ok(52)]
            )
            flusher = flusher_factory(store=store, cfg=cfg)

            gate = asyncio.Event()
            started = asyncio.Event()
            inflight: dict[tuple[str, int], int] = {}
            peak: dict[tuple[str, int], int] = {}
            real_flush_key = flusher.flush_key

            async def gated_flush_key(stage: Any, dec: Any):
                key = (stage.topic, stage.partition)
                inflight[key] = inflight.get(key, 0) + 1
                peak[key] = max(peak.get(key, 0), inflight[key])
                try:
                    if dec.team_id == 11:
                        started.set()
                        await gate.wait()
                    return await real_flush_key(stage, dec)
                finally:
                    inflight[key] -= 1

            monkeypatch.setattr(flusher, "flush_key", gated_flush_key)
            r = FlushRunner(
                manager,
                flusher,
                cfg.planner_knobs(),
                now_us=fake_clock,
                sleep=sleeper,
                sweep_s=60.0,
                concurrency=2,
            )
            sweep_task = asyncio.create_task(r.run_once())
            await asyncio.wait_for(started.wait(), timeout=5)
            # Team 11 is parked inside its flush; the others must still
            # land (bounded overlap, not a pile-up behind it).
            for _ in range(200):
                if len(capture.bodies) == 2:
                    break
                await asyncio.sleep(0.01)
            assert len(capture.bodies) == 2  # teams 12 and 21 committed
            gate.set()
            sweep = await asyncio.wait_for(sweep_task, timeout=5)
            assert (sweep.decisions, sweep.committed, sweep.failed) == (3, 3, 0)
            assert len(capture.bodies) == 3
            assert peak[("events", 0)] == 2  # the cap, exactly
            assert peak[("events", 1)] == 1

    async def test_a_failure_is_contained_under_parallelism(
        self,
        httpx_mock: Any,
        memory_store: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """One decision's failure (an encoder-bug-shaped exception)
        neither wedges nor aborts the sweep: the sibling commits, the
        failed team keeps its rows and replays next sweep."""
        cfg = make_config(target_output_bytes=1)
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage = (await manager.open_partition("events", 0)).stage
            await staged(stage, fake_clock, 11, [(0, JAN_US)])
            await staged(stage, fake_clock, 12, [(1, JAN_US)])
            script_options(httpx_mock)
            capture = CommitCapture(httpx_mock, [commit_ok(50)])
            flusher = flusher_factory(store=store, cfg=cfg)
            real_flush_key = flusher.flush_key

            async def broken_flush_key(stage: Any, dec: Any):
                if dec.team_id == 11:
                    raise RuntimeError("encoder bug")
                return await real_flush_key(stage, dec)

            monkeypatch.setattr(flusher, "flush_key", broken_flush_key)
            sweep = await FlushRunner(
                manager,
                flusher,
                cfg.planner_knobs(),
                now_us=fake_clock,
                sleep=sleeper,
                sweep_s=60.0,
                concurrency=4,
            ).run_once()
            assert (sweep.decisions, sweep.committed, sweep.failed) == (2, 1, 1)
            assert len(capture.bodies) == 1
            assert len(await staged_rows(stage, 11)) == 1  # still staged
            assert await staged_rows(stage, 12) == []

    async def test_a_halt_under_parallelism_is_sticky_and_settles_nothing_past_it(
        self,
        httpx_mock: Any,
        memory_store: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
    ) -> None:
        """A halt out of any in-flight decision stops the runner loudly;
        every later tick re-raises. (Both scripted steps lead to a halt
        or an exhausted retry, so the commit order is free.)"""
        cfg = make_config(target_output_bytes=1)
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage = (await manager.open_partition("events", 0)).stage
            await staged(stage, fake_clock, 11, [(0, JAN_US)])
            await staged(stage, fake_clock, 12, [(1, JAN_US)])
            script_options(httpx_mock)
            CommitCapture(
                httpx_mock,
                [
                    commit_refusal(
                        409, "table_recreated", "the table was dropped and recreated"
                    )
                ]
                + [commit_ok(50)],
            )
            flusher = flusher_factory(store=store, cfg=cfg)
            r = FlushRunner(
                manager,
                flusher,
                cfg.planner_knobs(),
                now_us=fake_clock,
                sleep=sleeper,
                sweep_s=60.0,
                concurrency=4,
            )
            with pytest.raises(FlushHalted):
                await r.run_once()
            assert r.halted is not None
            with pytest.raises(FlushHalted) as again:
                await r.run_once()
            assert again.value is r.halted
            assert [rk.offset for rk, _ in await staged_rows(stage, 11)] == [0]

    async def test_serial_mode_dispatches_oldest_first(
        self,
        httpx_mock: Any,
        memory_store: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """concurrency=1 keeps a partition's decisions strictly in the
        planner's oldest-first order (the dispatch contract the serial
        sweep always had)."""
        cfg = make_config(target_output_bytes=1)
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            stage = (await manager.open_partition("events", 0)).stage
            for i, team in enumerate((44, 22, 33, 11)):
                await staged(stage, fake_clock, team, [(i, JAN_US)])
                fake_clock.advance(1_000_000)
            starts: list[int] = []
            script_options(httpx_mock)
            CommitCapture(httpx_mock, [commit_ok(50 + i) for i in range(4)])
            flusher = flusher_factory(store=store, cfg=cfg)
            real_flush_key = flusher.flush_key

            async def recording_flush_key(stage: Any, dec: Any):
                starts.append(dec.team_id)
                return await real_flush_key(stage, dec)

            monkeypatch.setattr(flusher, "flush_key", recording_flush_key)
            sweep = await runner(manager, flusher, cfg, fake_clock, sleeper).run_once()
            assert sweep.committed == 4
            # Staged oldest-first: 44 first, 11 last, regardless of the
            # team-number order they were written in.
            assert starts == [44, 22, 33, 11]


# =====================================================================================
# B5: a fenced stage (SlateDB Error.Closed — the contested open) is a
# first-class terminal condition for that partition's flush decisions
# =====================================================================================


class _FencedOutFlusher:
    """Duck-typed HoglakeFlusher stand-in whose flush_key raises SlateDB's
    ``Error.Closed(FENCED)`` for the partitions in ``fenced_on`` — the
    contested open surfacing MID-DECISION (the binding's lazy fencing
    pins that any stage call can be the detecting one, flush.py's worker
    arm included) — and reports a plain commit elsewhere."""

    def __init__(self, fenced_on: set[int]) -> None:
        self._fenced_on = fenced_on
        self.calls: list[int] = []

    @property
    def compression_ratio(self) -> float:
        from millrace.planner import DEFAULT_COMPRESSION_RATIO

        return DEFAULT_COMPRESSION_RATIO

    async def flush_key(self, stage: Any, decision: Any) -> Any:
        self.calls.append(stage.partition)
        if stage.partition in self._fenced_on:
            raise SlateError.Closed(CloseReason.FENCED, "detected newer DB client")
        return FlushReport(
            topic=stage.topic,
            partition=stage.partition,
            team_id=decision.team_id,
            trigger=decision.trigger,
            outcome="committed",
            first_offset=0,
            last_offset=0,
        )


@component
class TestFencedFlusher:
    async def test_a_fence_detected_mid_decision_is_logged_once_and_terminal(
        self,
        memory_store: Any,
        fake_clock: Any,
        sleeper: Sleeper,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The decision arm: flush_key raises ``Error.Closed(FENCED)``.
        The runner fences the partition out of scheduling, logs ONCE (no
        stack trace per decision per tick — the pre-fix loop), surfaces
        the fence in its stats, and leaves the other partition alone."""
        cfg = make_config(target_output_bytes=1)
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            s0 = (await manager.open_partition("events", 0)).stage
            s1 = (await manager.open_partition("events", 1)).stage
            # TWO teams on the fenced partition: sweep 1 queues two p0
            # decisions, so the worker's fenced-skip (not a second
            # flush_key call) is what the second one meets.
            await staged(s0, fake_clock, TEAM, [(0, JAN_US), (1, JAN_US)])
            await staged(s0, fake_clock, TEAM + 9, [(2, JAN_US)])
            await staged(s1, fake_clock, TEAM, [(2, JAN_US)])
            flusher = _FencedOutFlusher(fenced_on={0})
            r = FlushRunner(
                manager,
                flusher,  # type: ignore[arg-type]
                cfg.planner_knobs(),
                now_us=fake_clock,
                sleep=sleeper,
                sweep_s=60.0,
            )
            with caplog.at_level(logging.WARNING):
                sweep1 = await r.run_once()
                sweep2 = await r.run_once()

            # The fence surfaced on partition 0's FIRST decision of sweep
            # 1; neither sweep planned it again, and the second decision
            # queued behind it was skipped without another call.
            # Partition 1 flushes (through the fake) on both sweeps.
            assert flusher.calls == [0, 1, 1]
            assert sweep1.committed == 1 and sweep2.committed == 1
            assert r.fenced_partitions == (("events", 0),)
            assert (sweep1.fenced, sweep2.fenced) == (1, 1)
            assert sweep1.failed == 0  # a fence is not a decision failure
            fence_logs = [
                rec
                for rec in caplog.records
                if "fenced out of flush scheduling" in rec.message
            ]
            assert len(fence_logs) == 1
            assert "events" in fence_logs[0].message and "[0]" in fence_logs[0].message
            # Never a stack trace: nothing logged with an exception
            # attached, on either sweep.
            assert not [rec for rec in caplog.records if rec.exc_info]
            # Both partitions' staged rows are untouched (the fenced
            # partition's state is its new owner's; the fake never
            # settled partition 1 either — nothing was published).
            assert [rk.offset for rk, _ in await staged_rows(s0, TEAM)] == [0, 1]
            assert [rk.offset for rk, _ in await staged_rows(s0, TEAM + 9)] == [2]
            assert [rk.offset for rk, _ in await staged_rows(s1, TEAM)] == [2]

    async def test_a_revoked_partition_s_fence_clears(
        self,
        memory_store: Any,
        fake_clock: Any,
        sleeper: Sleeper,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The fence mark follows the CLAIM, not the path: a revoke drops
        it, so a later re-claim (a fresh writer on the path) is planned
        and reconciled afresh."""
        cfg = make_config(target_output_bytes=1)
        async with StageManager(
            "memory:///", "millrace", settings=fast_flush_settings()
        ) as manager:
            s0 = (await manager.open_partition("events", 0)).stage
            await staged(s0, fake_clock, TEAM, [(0, JAN_US)])
            flusher = _FencedOutFlusher(fenced_on={0})
            r = FlushRunner(
                manager,
                flusher,  # type: ignore[arg-type]
                cfg.planner_knobs(),
                now_us=fake_clock,
                sleep=sleeper,
                sweep_s=60.0,
            )
            with caplog.at_level(logging.WARNING):
                await r.run_once()
                assert r.fenced_partitions == (("events", 0),)
                # Revoke: the mark clears...
                await manager.close_partition("events", 0)
                sweep = await r.run_once()
                assert r.fenced_partitions == ()
                assert sweep.fenced == 0
                # ...and a re-claim reconciles afresh (a fresh writer on
                # the path is planned and its decision attempted — the
                # fake fences THAT one too, logging again: one log per
                # claim, not per decision).
                await manager.open_partition("events", 0)
                await r.run_once()
                assert r.fenced_partitions == (("events", 0),)
            fence_logs = [
                rec
                for rec in caplog.records
                if "fenced out of flush scheduling" in rec.message
            ]
            assert len(fence_logs) == 2  # one per claim
            assert not [rec for rec in caplog.records if rec.exc_info]

    @pytest.mark.integration
    async def test_a_real_contested_open_fences_the_live_flusher(
        self,
        tmp_path: Any,
        httpx_mock: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """End to end against a real ``file:///`` stage: a contested open
        on partition 0's path (a second writer — the rollover shape the
        review cites) fences the runner's stage LAZILY; the runner treats
        it as terminal for that partition: one log, no repeated
        exceptions, partition 1 unaffected, partition 0's staged rows
        left for the new owner."""
        from slatedb.uniffi import DbBuilder, KeyRange, ObjectStore

        cfg = make_config(target_output_bytes=1)
        url = "file:///"
        base = str(tmp_path / "slatedb")
        async with StageManager(url, base, settings=fast_flush_settings()) as mgr:
            s0 = (await mgr.open_partition("events", 0)).stage
            s1 = (await mgr.open_partition("events", 1)).stage
            await staged(s0, fake_clock, TEAM, [(0, JAN_US)])
            await staged(s1, fake_clock, TEAM + 1, [(0, JAN_US)])
            script_options(httpx_mock)
            capture = CommitCapture(httpx_mock, [commit_ok(50)])
            flusher = flusher_factory(store=store, cfg=cfg)
            r = runner(mgr, flusher, cfg, fake_clock, sleeper)

            # The contested open: a second writer takes partition 0's
            # path (two file:/// resolves share the backing — the parity
            # suite's fencing topology). Force the detection through the
            # write path: the binding pins "no later than the first
            # durability wait after losing the path"
            # (tests/test_slatedb_parity.py).
            rival = await DbBuilder(
                mgr.path_for("events", 0), ObjectStore.resolve(url)
            ).build()
            from millrace.stage import StagedRecord

            with pytest.raises(SlateError.Closed):
                await s0.stage_batch(
                    [
                        StagedRecord(
                            team_id=TEAM, event_ts_us=JAN_US, offset=1, payload=b"x"
                        )
                    ],
                    now_us=fake_clock(),
                )

            with caplog.at_level(logging.WARNING):
                sweep1 = await r.run_once()
                sweep2 = await r.run_once()

            # Partition 1 committed in both... once (its row settled in
            # sweep 1; sweep 2 has no work for it). Partition 0 never
            # planned again after the fence surfaced.
            assert len(capture.bodies) == 1
            assert sweep1.committed == 1
            assert r.fenced_partitions == (("events", 0),)
            assert (sweep1.fenced, sweep2.fenced) == (1, 1)
            fence_logs = [
                rec
                for rec in caplog.records
                if "fenced out of flush scheduling" in rec.message
            ]
            assert len(fence_logs) == 1
            assert not [rec for rec in caplog.records if rec.exc_info]

            # Partition 0's staged row SURVIVES under the new owner (the
            # fenced writer never settled it) — read back through the
            # rival writer, not the dead handle.
            it = await rival.scan_prefix(
                b"rows/",
                KeyRange(
                    start=None, start_inclusive=False, end=None, end_inclusive=False
                ),
            )
            survivors = []
            while batch := await it.next_batch(16):
                survivors.extend(kv.key for kv in batch)
            assert len(survivors) == 1
            await rival.shutdown()

    @pytest.mark.integration
    async def test_a_fence_surfacing_at_planning_is_terminal(
        self,
        tmp_path: Any,
        httpx_mock: Any,
        fake_clock: Any,
        flusher_factory: Any,
        store: FakeObjectStore,
        sleeper: Sleeper,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Same terminal handling on the OTHER detection point: the
        partition reconciled cleanly LAST sweep, the contested open lands
        between sweeps, and the fence surfaces in the sweep's planning
        scan (``_assemble_stats``). Fenced out of scheduling from then
        on, one log, no stack traces."""
        from slatedb.uniffi import DbBuilder, KeyRange, ObjectStore

        cfg = make_config(target_output_bytes=1)
        url = "file:///"
        base = str(tmp_path / "slatedb")
        async with StageManager(url, base, settings=fast_flush_settings()) as mgr:
            s0 = (await mgr.open_partition("events", 0)).stage
            s1 = (await mgr.open_partition("events", 1)).stage
            script_options(httpx_mock)
            capture = CommitCapture(httpx_mock, [commit_ok(50)])
            flusher = flusher_factory(store=store, cfg=cfg)
            r = runner(mgr, flusher, cfg, fake_clock, sleeper)

            # Sweep 1: both partitions reconcile; nothing staged, no work.
            sweep1 = await r.run_once()
            assert (sweep1.decisions, sweep1.fenced) == (0, 0)

            await staged(s0, fake_clock, TEAM, [(0, JAN_US)])
            await staged(s1, fake_clock, TEAM + 1, [(0, JAN_US)])

            # The contested open lands between sweeps; detection is
            # forced through the write path (the sacrificial record
            # never lands — the parity suite's pin).
            rival = await DbBuilder(
                mgr.path_for("events", 0), ObjectStore.resolve(url)
            ).build()
            from millrace.stage import StagedRecord

            with pytest.raises(SlateError.Closed):
                await s0.stage_batch(
                    [
                        StagedRecord(
                            team_id=TEAM, event_ts_us=JAN_US, offset=1, payload=b"x"
                        )
                    ],
                    now_us=fake_clock(),
                )

            with caplog.at_level(logging.WARNING):
                sweep2 = await r.run_once()
                sweep3 = await r.run_once()

            # Partition 1 committed; partition 0 fenced at PLANNING and
            # never planned again.
            assert len(capture.bodies) == 1
            assert sweep2.committed == 1
            assert r.fenced_partitions == (("events", 0),)
            assert (sweep2.fenced, sweep3.fenced) == (1, 1)
            fence_logs = [
                rec
                for rec in caplog.records
                if "fenced out of flush scheduling" in rec.message
            ]
            assert len(fence_logs) == 1
            assert not [rec for rec in caplog.records if rec.exc_info]

            it = await rival.scan_prefix(
                b"rows/",
                KeyRange(
                    start=None, start_inclusive=False, end=None, end_inclusive=False
                ),
            )
            survivors = []
            while batch := await it.next_batch(16):
                survivors.extend(kv.key for kv in batch)
            assert len(survivors) == 1  # the sacrificial record never landed
            await rival.shutdown()
