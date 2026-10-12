"""Consume-loop tests (docs/kafka-ingestion-plan.md Phase 3).

Layers: the PURE decisions (record classification — the value parsed
once as a JSON object and keyed by the destination's partition tuple —
and the client-config pins) are unit tests without I/O;
the loop itself is driven component-style — a scripted FakeConsumer
(tests/kafkakit.py, hedgerow's fakes.py shape) over a REAL
StageManager on ``memory:///``, so poll → stage → ack → commit ordering,
assignment open/recover/revoke, drops and shutdown are all pinned
against durable staged state with no broker anywhere.

Pinned here:

- the call-order invariant: a commit is NEVER observed before the
  durable ack it covers (rows-then-offset, hedgerow's rule), and
  commits are monotone per partition;
- static assignment uses OFFSET_STORED and opens+recovers the stages
  BEFORE assign; cooperative assignment drives sync_assignment from the
  rebalance callbacks — assign opens+recovers, revoke/lost close
  cleanly with zero staged-data loss and no commit for the revoked
  partition's in-flight messages;
- the client-config pins: ``auto.offset.reset`` from the config knob
  (default ``latest``, the fleet policy — millpond's whole-retention
  replay; ``earliest`` stays selectable), ``enable.auto.commit`` /
  ``enable.auto.offset.store`` off, and the whale-shape fetch tuning
  (``max.partition.fetch.bytes`` / ``fetch.max.bytes`` /
  ``queued.max.messages.kbytes``);
- one poll stages its partitions CONCURRENTLY: a gated spy proves the
  per-partition ``stage_batch`` calls overlap in flight (not one WAL
  round trip per partition in series), and the single commit still
  follows every ack;
- the per-poll byte cap clamps the next consume's message count to
  budget ÷ observed average record size, floored at 1;
- the staging group is the destination's partition tuple: an
  unpartitioned table stages everything under one group,
  ``identity(team_id)`` and ``(identity(team_id), month(timestamp))``
  group by the payload's values, a nested source (``properties.team_id``)
  is read by path, the Kafka key is never read, and a malformed source
  drops with a reason naming the field;
- a spec refresh mid-stream: past the shape TTL the consumer re-reads
  the destination and new records stage under the new spec's tuples
  while the old group stays staged and flushable; a failed re-read keeps
  the old keyer;
- a fenced partition (a REAL contested open on the stage's path — the
  parity suite's FENCED shape) closes locally and halts the loop with
  the distinct ConsumerFencedError, without committing the failed batch;
- malformed records are dropped: nothing staged, counted per reason
  label (the partition-source path collapsed), logged at WARNING
  rate-limited per (partition, reason), and covered by the partition's
  commit;
  The gauges are alert inputs, published by the flush sweep;
- commit retry ladder and its exhaustion; a failed stage commits
  nothing;
- graceful shutdown: polling stops, the Kafka side closes, committed
  offsets equal what was durably staged, and the caller-owned
  StageManager stays open.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Collection
from typing import Any

import pytest
from confluent_kafka import OFFSET_STORED, KafkaError, KafkaException
from kafkakit import FakeConsumer, error_event, msg, team_msg, team_payload
from pyhoglake import TableInfo
from slatedb.uniffi import CloseReason, DbBuilder
from slatedb.uniffi import Error as SlateError
from stagekit import NOW, fast_flush_settings, prepared, rec

import millrace.consumer as consumer_mod
from millrace.config import (
    AssignmentMode,
    AutoOffsetReset,
    Config,
    EventTimePolicy,
)
from millrace.consumer import (
    DROP_LOG_INTERVAL_S,
    REASON_INVALID_OFFSET,
    REASON_INVALID_TIMESTAMP,
    REASON_MALFORMED_PARTITION_SOURCE,
    REASON_MALFORMED_VALUE,
    REASON_MISSING_TIMESTAMP,
    REASON_MISSING_VALUE,
    ConsumerFencedError,
    ConsumerStats,
    MillraceConsumer,
    classify_message,
    create_kafka_consumer,
    drop_reason_label,
    kafka_consumer_config,
)
from millrace.flush import FlushHalted
from millrace.grouping import GroupKeyer
from millrace.planner import PlannerKnobs, plan_flush
from millrace.stage import (
    DroppedRecord,
    PartitionStage,
    StageClosedError,
    StagedRecord,
    StageManager,
)

component = pytest.mark.component


# -- builders and drivers ----------------------------------------------------------


def make_config(**overrides) -> Config:
    base: dict[str, Any] = {
        "kafka_bootstrap_servers": "broker:9092",
        "kafka_topic": "events",
        "kafka_group_id": "millrace-events",
        "assignment_mode": AssignmentMode.STATIC,
        "static_partitions": (0,),
        "catalog": "prod",
        "namespace": "default",
        "table": "events",
        "stage_store_url": "memory:///",
        "stage_base_path": "millrace",
        "target_output_bytes": 1000,
        "flush_deadline_s": 900,
        "slow_lane_deadline_s": 21600,
        "min_flush_bytes": 1024 * 1024,
        "max_files_per_commit": 512,
        "consume_batch_size": 100,
        "poll_timeout_ms": 50,
        "event_time_policy": EventTimePolicy.DROP,
        "metrics_port": 8000,
    }
    return Config(**{**base, **overrides})


class Sleeper:
    """The injected commit-retry sleep: records, never waits."""

    def __init__(self) -> None:
        self.calls: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)


def make_manager(journal: list[tuple]) -> StageManager:
    return StageManager(
        "memory:///",
        "millrace",
        settings=fast_flush_settings(),
        ack_hook=lambda ack: journal.append(("ack", ack)),
    )


#: The destination's columns: a top-level ``team_id``, the event time,
#: free text, and a ``properties`` struct whose ``team_id`` leaf is a
#: legal (nested) partition source.
COLUMNS_WIRE: list[dict[str, Any]] = [
    {"name": "team_id", "type": "long", "field_id": 1, "ordinal": 0},
    {"name": "timestamp", "type": "timestamp", "field_id": 2, "ordinal": 1},
    {"name": "event", "type": "string", "field_id": 3, "ordinal": 2},
    {
        "name": "properties",
        "type": "struct",
        "field_id": 4,
        "ordinal": 3,
        "children": [
            {"name": "team_id", "type": "long", "field_id": 5, "ordinal": 0},
        ],
    },
]
IDENTITY_TEAM: dict[str, Any] = {"source_field_id": 1, "transform": "identity"}
MONTH_TIMESTAMP: dict[str, Any] = {"source_field_id": 2, "transform": "month"}
IDENTITY_PROPERTIES_TEAM: dict[str, Any] = {
    "source_field_id": 5,
    "transform": "identity",
}


def table_info(spec: list[dict[str, Any]] | None, *, spec_id: int = 1) -> TableInfo:
    """The destination's shape with ``spec`` as its live partition spec
    (None: unpartitioned)."""
    wire: dict[str, Any] = {
        "name": "events",
        "namespace": "default",
        "table_uuid": "0b8ee9ba-79a1-4f3e-b7e5-6a0b6ab6f012",
        "columns": COLUMNS_WIRE,
        "read_snapshot_id": 88,
    }
    if spec is not None:
        wire["partition_spec"] = {"spec_id": spec_id, "fields": spec}
    return TableInfo.from_wire(wire)


#: The component tests' keyer: ``identity(team_id)``, so a team's rows
#: stage under the one-field tuple ``(str(team),)``.
TEAM_KEYER = GroupKeyer(table_info([IDENTITY_TEAM]))


class FakeShape:
    """The consumer's ``ShapeSource``: scripted re-reads (a TableInfo or
    an exception per call) and a fixed TTL."""

    def __init__(self, ttl_s: float = 450.0) -> None:
        self.ttl = ttl_s
        self.reads: list[TableInfo | Exception] = []
        self.calls = 0

    def read(self) -> TableInfo:
        self.calls += 1
        step = self.reads.pop(0)
        if isinstance(step, Exception):
            raise step
        return step

    def ttl_s(self) -> float:
        return self.ttl


class FakeMonotonic:
    def __init__(self) -> None:
        self.now = 1_000.0

    def __call__(self) -> float:
        return self.now


def make_consumer(
    fake: FakeConsumer,
    manager: StageManager,
    cfg: Config,
    clock,
    sleeper: Sleeper | None = None,
    *,
    keyer: GroupKeyer = TEAM_KEYER,
    shape: FakeShape | None = None,
    monotonic: FakeMonotonic | None = None,
) -> MillraceConsumer:
    return MillraceConsumer(
        fake,
        manager,
        cfg,
        keyer=keyer,
        shape=shape,
        monotonic=monotonic if monotonic is not None else FakeMonotonic(),
        now_us=clock,
        sleep=sleeper if sleeper is not None else Sleeper(),
    )


async def run_steps(
    consumer: MillraceConsumer, fake: FakeConsumer, extra: int = 1
) -> None:
    """Drive the loop until the fake has nothing deliverable and no
    scripted rebalance pending, plus ``extra`` idle steps (the idle
    steps matter: a trailing error event or a rebalance callback can
    land on an otherwise-empty poll, so the drain condition is
    re-checked after them)."""
    while True:
        while fake.deliverable() or fake.has_pending_rebalance():
            await consumer._step()
        for _ in range(extra):
            await consumer._step()
        if not (fake.deliverable() or fake.has_pending_rebalance()):
            return


def assert_commits_follow_acks(
    journal: list[tuple], dropped: Collection[tuple[str, int, int]] = ()
) -> None:
    """The rows-then-offset invariant over the shared journal: every
    commit is preceded by durable acks covering it — every offset below
    the committed one and above the last acked one must be a DROPPED
    record (``dropped``: ``(topic, partition, offset)``) — and
    per-partition commits are strictly increasing."""
    drops = set(dropped)
    acked_through: dict[tuple[str, int], int] = {}
    last_commit: dict[tuple[str, int], int] = {}
    for entry in journal:
        if entry[0] == "ack":
            ack = entry[1]
            key = (ack.topic, ack.partition)
            acked_through[key] = max(ack.last_offset, acked_through.get(key, -1))
        elif entry[0] == "commit":
            for topic, partition, offset in entry[1]:
                key = (topic, partition)
                floor = max(acked_through.get(key, -1), last_commit.get(key, 0) - 1)
                uncovered = [
                    o
                    for o in range(floor + 1, offset)
                    if (topic, partition, o) not in drops
                ]
                assert not uncovered, (
                    f"commit {topic}[{partition}]@{offset} precedes durable "
                    f"staging of offsets {uncovered[:5]} (acked through "
                    f"{acked_through.get(key)})"
                )
                prev = last_commit.get(key)
                assert prev is None or offset > prev, "commits must be monotone"
                last_commit[key] = offset


async def team_offsets(stage: PartitionStage, team: int) -> list[int]:
    """The staged offsets of team ``team``'s ``identity(team_id)`` group."""
    return [rk.offset async for rk, _ in stage.scan_group_rows((str(team),))]


# ==================================================================================
# Pure: record classification under the destination's partition spec
# ==================================================================================


def _classify(
    keyer: GroupKeyer,
    value: bytes | None,
    *,
    offset: int = 41,
    timestamp: tuple[int, int] = (1, 1_234_567),
) -> StagedRecord | DroppedRecord:
    return classify_message(keyer, offset=offset, value=value, timestamp=timestamp)


def _group(keyer: GroupKeyer, doc: dict[str, Any]) -> tuple[str | None, ...]:
    out = _classify(keyer, json.dumps(doc).encode())
    assert isinstance(out, StagedRecord), out
    return out.group


#: 2027-01-15T08:00:00Z in epoch micros; month 684 (2027-01) since 1970-01.
JAN_TS = 1_800_000_000_000_000
JAN_MONTH = "684"


def test_an_unpartitioned_table_stages_every_record_under_one_group():
    keyer = GroupKeyer(table_info(None))
    assert keyer.fields == ()
    assert _group(keyer, {"team_id": 7}) == ()
    assert _group(keyer, {"anything": "at all"}) == ()
    assert _group(keyer, {}) == ()


def test_identity_team_id_groups_by_the_teams_wire_string():
    keyer = GroupKeyer(table_info([IDENTITY_TEAM]))
    assert keyer.fields == ("team_id",)
    out = _classify(keyer, b'{"team_id": 7, "v": 1}', offset=41)
    assert out == StagedRecord(
        group=("7",),
        event_ts_us=1_234_567_000,
        offset=41,
        payload=b'{"team_id": 7, "v": 1}',
    )
    assert _group(keyer, {"team_id": 2**63 - 1}) == (str(2**63 - 1),)
    assert _group(keyer, {"team_id": -3}) == ("-3",)


def test_identity_team_and_month_timestamp_group_by_both_components():
    keyer = GroupKeyer(table_info([IDENTITY_TEAM, MONTH_TIMESTAMP]))
    assert keyer.fields == ("team_id", "timestamp_month")
    assert _group(keyer, {"team_id": 7, "timestamp": JAN_TS}) == ("7", JAN_MONTH)
    # The ISO form of the same instant lands in the same tuple: the tuple
    # is computed from the COERCED value, exactly as the flush writes it.
    iso = {"team_id": 7, "timestamp": "2027-01-15T08:00:00"}
    assert _group(keyer, iso) == ("7", JAN_MONTH)
    # One month earlier is another group; a pre-epoch instant floors.
    assert _group(keyer, {"team_id": 7, "timestamp": "2026-12-31T23:59:59"}) == (
        "7",
        "683",
    )
    assert _group(keyer, {"team_id": 7, "timestamp": -1}) == ("7", "-1")


def test_a_missing_or_null_source_is_a_null_component():
    keyer = GroupKeyer(table_info([IDENTITY_TEAM, MONTH_TIMESTAMP]))
    assert _group(keyer, {"timestamp": JAN_TS}) == (None, JAN_MONTH)
    assert _group(keyer, {"team_id": None, "timestamp": None}) == (None, None)


def test_a_nested_source_is_read_by_its_path():
    keyer = GroupKeyer(table_info([IDENTITY_PROPERTIES_TEAM]))
    assert keyer.fields == ("properties.team_id",)
    assert _group(keyer, {"properties": {"team_id": 9, "x": 1}}) == ("9",)
    # The top-level team_id is a different column: not this source.
    assert _group(keyer, {"team_id": 7, "properties": {"team_id": 9}}) == ("9",)
    # Absent or null anywhere along the path is a null component.
    assert _group(keyer, {"team_id": 7}) == (None,)
    assert _group(keyer, {"properties": None}) == (None,)
    assert _group(keyer, {"properties": {}}) == (None,)
    # A parent that is not an object is a malformed source.
    out = _classify(keyer, b'{"properties": [1, 2]}')
    assert isinstance(out, DroppedRecord)
    assert out.reason == f"{REASON_MALFORMED_PARTITION_SOURCE}:properties.team_id"


@pytest.mark.parametrize(
    "payload",
    [
        b'{"team_id": "7"}',  # a string, never coerced to a long
        b'{"team_id": 7.5}',
        b'{"team_id": true}',  # bool IS int in Python; refused explicitly
        b'{"team_id": %d}' % 2**63,  # past the long column
        b'{"team_id": {"id": 7}}',
    ],
)
def test_a_malformed_source_is_dropped_naming_the_field(payload):
    keyer = GroupKeyer(table_info([IDENTITY_TEAM]))
    out = _classify(keyer, payload, offset=9)
    assert out == DroppedRecord(9, "malformed_partition_source:team_id")
    # The metric label drops the path (bounded cardinality).
    assert drop_reason_label(out.reason) == REASON_MALFORMED_PARTITION_SOURCE


def test_a_malformed_temporal_source_is_dropped_naming_the_field():
    keyer = GroupKeyer(table_info([IDENTITY_TEAM, MONTH_TIMESTAMP]))
    for raw in (b'"not a date"', b'"2027-01-15T08:00:00+02:00"', b"1e3"):
        out = _classify(keyer, b'{"team_id": 7, "timestamp": ' + raw + b"}")
        assert isinstance(out, DroppedRecord)
        assert out.reason == "malformed_partition_source:timestamp"


@pytest.mark.parametrize(
    "payload",
    [b"not json", b"\xff\xfe{}", b"[1, 2]", b"42", b'"team_id"', b'{"x": NaN}'],
)
def test_a_payload_that_is_not_a_strict_json_object_is_malformed_value(payload):
    # Every spec parses the value first — the unpartitioned one too.
    for spec in (None, [IDENTITY_TEAM]):
        out = _classify(GroupKeyer(table_info(spec)), payload)
        assert out == DroppedRecord(41, REASON_MALFORMED_VALUE)


def test_the_kafka_key_is_never_read():
    """Classification has no key input at all: the payload alone decides
    the tuple (the loop-level pin is
    test_a_keyless_topic_stages_by_the_payloads_partition_tuple)."""
    import inspect

    assert "key" not in inspect.signature(classify_message).parameters
    keyer = GroupKeyer(table_info([IDENTITY_TEAM]))
    assert _group(keyer, {"team_id": 7}) == ("7",)


def test_classify_drop_reasons():
    keyer = GroupKeyer(table_info([IDENTITY_TEAM]))
    out = _classify(keyer, b'{"team_id": 7}', timestamp=(0, 0))  # NOT_AVAILABLE
    assert out == DroppedRecord(41, REASON_MISSING_TIMESTAMP)
    assert _classify(keyer, None) == DroppedRecord(41, REASON_MISSING_VALUE)


def test_classify_out_of_range_offset_and_timestamp():
    keyer = GroupKeyer(table_info([IDENTITY_TEAM]))
    out = _classify(keyer, b'{"team_id": 7}', offset=-1)
    assert out == DroppedRecord(-1, REASON_INVALID_OFFSET)
    # *1000 overflows int64 µs
    out = _classify(keyer, b'{"team_id": 7}', timestamp=(1, 10**18))
    assert out == DroppedRecord(41, REASON_INVALID_TIMESTAMP)


def test_classify_names_the_FIRST_failing_check():
    keyer = GroupKeyer(table_info([IDENTITY_TEAM]))
    # A tombstone with no timestamp drops as missing_timestamp; a
    # tombstone is missing_value, never a parse error.
    out = _classify(keyer, None, timestamp=(0, 0))
    assert out == DroppedRecord(41, REASON_MISSING_TIMESTAMP)
    assert _classify(keyer, None) == DroppedRecord(41, REASON_MISSING_VALUE)


@pytest.mark.parametrize(
    ("reason", "label"),
    [
        (REASON_INVALID_OFFSET, REASON_INVALID_OFFSET),
        (REASON_MALFORMED_VALUE, REASON_MALFORMED_VALUE),
        ("malformed_partition_source:a.b.c", REASON_MALFORMED_PARTITION_SOURCE),
        (REASON_MALFORMED_PARTITION_SOURCE, REASON_MALFORMED_PARTITION_SOURCE),
    ],
)
def test_drop_reason_label_collapses_only_the_partition_source_path(reason, label):
    assert drop_reason_label(reason) == label


# ==================================================================================
# Pure: the librdkafka client config pins
# ==================================================================================


def test_consumer_config_defaults_to_latest_and_explicit_commits():
    conf = kafka_consumer_config(make_config())
    # The fleet policy default (millpond's whole-retention replay
    # incident): a fresh group starts at the head. The removal-by-
    # omission tension (a no-offset partition's backlog is never
    # consumed) is the documented price — config.AutoOffsetReset.
    assert conf["auto.offset.reset"] == "latest"
    assert make_config().kafka_auto_offset_reset is AutoOffsetReset.LATEST
    assert conf["enable.auto.commit"] is False
    assert conf["enable.auto.offset.store"] is False
    assert conf["group.id"] == "millrace-events"
    # Static assignment: no group rebalancing strategy on the wire.
    assert "partition.assignment.strategy" not in conf


def test_consumer_config_earliest_stays_selectable():
    conf = kafka_consumer_config(
        make_config(kafka_auto_offset_reset=AutoOffsetReset.EARLIEST)
    )
    assert conf["auto.offset.reset"] == "earliest"


def test_consumer_config_wires_the_fetch_tuning():
    cfg = make_config(
        kafka_max_partition_fetch_bytes=4 * 1024 * 1024,
        kafka_fetch_max_bytes=64 * 1024 * 1024,
        kafka_queued_max_messages_kbytes=131072,
    )
    conf = kafka_consumer_config(cfg)
    assert conf["max.partition.fetch.bytes"] == 4 * 1024 * 1024
    assert conf["fetch.max.bytes"] == 64 * 1024 * 1024
    assert conf["queued.max.messages.kbytes"] == 131072


def test_consumer_config_cooperative_sets_the_assignor():
    conf = kafka_consumer_config(
        make_config(assignment_mode=AssignmentMode.COOPERATIVE, static_partitions=None)
    )
    assert conf["partition.assignment.strategy"] == "cooperative-sticky"
    assert conf["auto.offset.reset"] == "latest"


def test_real_consumer_constructs_from_the_config():
    """librdkafka validates the config keys eagerly at construction; no
    broker is contacted (a construction-only smoke of the pinned dict)."""
    consumer = create_kafka_consumer(make_config())
    consumer.close()


# ==================================================================================
# Component: the loop over a scripted fake and a real StageManager
# ==================================================================================


@component
async def test_static_start_opens_and_recovers_stages_before_assign(fake_clock):
    journal: list[tuple] = []
    manager = make_manager(journal)
    original_sync = manager.sync_assignment

    async def spied_sync(claimed):
        journal.append(("sync_assignment", frozenset(claimed)))
        return await original_sync(claimed)

    manager.sync_assignment = spied_sync  # type: ignore[method-assign]
    fake = FakeConsumer(journal=journal)
    consumer = make_consumer(
        fake, manager, make_config(static_partitions=(0, 2)), fake_clock
    )

    await consumer.start()
    assert manager.partitions() == (("events", 0), ("events", 2))
    assert (
        "assign",
        [("events", 0, OFFSET_STORED), ("events", 2, OFFSET_STORED)],
    ) in journal
    kinds = [e[0] for e in journal]
    assert kinds.index("sync_assignment") < kinds.index("assign")
    await manager.close()


@component
async def test_static_start_rides_out_coordinator_warmup(fake_clock):
    """A fresh broker answers the first committed-offset probe with
    NOT_COORDINATOR while __consumer_offsets elects its leaders; the
    static start retries the probe and assigns only after it answers
    (librdkafka resolves OFFSET_STORED exactly once, at assign time — a
    fetch refused there leaves the partition assigned but never
    fetching)."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    fake.fail_next_committed(2)  # the broker's coordinator warms up
    sleeper = Sleeper()
    consumer = make_consumer(fake, manager, make_config(), fake_clock, sleeper)

    await consumer.start()
    assert (
        "assign",
        [("events", 0, OFFSET_STORED)],
    ) in journal
    probes = [e for e in journal if e[0] == "committed"]
    assert len(probes) == 3  # two warm-up refusals, then the answer
    assert sleeper.calls == [0.5, 0.5]
    await manager.close()


@component
async def test_static_start_fails_loudly_when_the_coordinator_never_answers(
    fake_clock,
):
    """The warmup is bounded: a broker that keeps refusing fails startup
    (the supervisor restarts) instead of idling in OFFSET_STORED limbo."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    fake.fail_next_committed(100)  # more than the attempt bound
    sleeper = Sleeper()
    consumer = make_consumer(fake, manager, make_config(), fake_clock, sleeper)

    with pytest.raises(KafkaException):
        await consumer.start()
    assert not any(e[0] == "assign" for e in journal)
    probes = [e for e in journal if e[0] == "committed"]
    assert len(probes) == consumer_mod._OFFSET_STORAGE_WARMUP_MAX_ATTEMPTS
    await manager.close()


@component
async def test_claim_recovers_the_previous_owners_leftovers(fake_clock):
    """Claiming a partition runs recovery, and the recovery report flows
    to the claimant: a persisted ``prepared/`` entry left by a previous
    owner (crashed between persist and publication) is handed back as
    the flusher's replay set."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    # A previous owner staged rows and persisted a prepared request, then
    # went away (close = the claim moved; the state stays on the store).
    previous = await manager.open_partition("events", 0)
    await previous.stage.stage_batch([rec(1, 100, 0), rec(1, 200, 1)], now_us=NOW)
    await previous.stage.persist_prepared(("1",), 0, prepared("idem-1", team=1))
    await manager.close_partition("events", 0)

    captured = []

    original_sync = manager.sync_assignment

    async def spied_sync(claimed):
        result = await original_sync(claimed)
        captured.append(result)
        return result

    manager.sync_assignment = spied_sync  # type: ignore[method-assign]
    fake = FakeConsumer(journal=journal)
    consumer = make_consumer(
        fake, manager, make_config(static_partitions=(0,)), fake_clock
    )
    await consumer.start()

    (sync,) = captured
    recovery = sync.opened[("events", 0)].recovery
    assert recovery.legacy_markers_collected == 0
    assert [(p.key.group, p.key.first_offset) for p in recovery.pending_prepared] == [
        (("1",), 0)
    ]
    assert recovery.pending_prepared[0].request.idempotency_key == "idem-1"
    # ...and the staged rows are claimable state, not stranded bytes.
    stage = manager.stage("events", 0)
    assert await team_offsets(stage, 1) == [0, 1]
    await manager.close()


@component
async def test_end_to_end_staged_rows_and_committed_offsets_cover(fake_clock):
    """Produced rows are all staged (byte-exact, key-ordered) and the
    committed offsets exactly cover the produced ranges."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    cfg = make_config(static_partitions=(0, 1), consume_batch_size=7)
    consumer = make_consumer(fake, manager, cfg, fake_clock)
    await consumer.start()

    produced: dict[tuple[int, int], bytes] = {}  # (partition, offset) -> payload
    for offset in range(20):
        m = team_msg(0, offset, team=1 + offset % 3, tag=f"row-0-{offset}")
        fake.feed(m)
        produced[(0, offset)] = m.value()
    for offset in range(10):
        m = team_msg(1, offset, team=7, tag=f"row-1-{offset}")
        fake.feed(m)
        produced[(1, offset)] = m.value()

    await run_steps(consumer, fake)

    assert_commits_follow_acks(journal)
    assert fake.committed_store == {("events", 0): 20, ("events", 1): 10}
    assert consumer.committed_offsets() == fake.committed_store
    stats = consumer.stats()
    assert stats.messages_consumed == 30
    assert stats.records_staged == 30
    assert stats.records_dropped == {}

    # Every produced row is staged, byte-exact, in (group, ts, offset)
    # key order; the stats the planner reads reconcile exactly; the
    # offsets/ identity ranges cover the produced range contiguously.
    total_rows = 0
    for partition, team_count in ((0, 3), (1, 1)):
        stage = manager.stage("events", partition)
        ranges = []
        for ks in await stage.iter_key_stats():
            rows = [(rk, v) async for rk, v in stage.scan_group_rows(ks.group)]
            assert [(rk.timestamp_us, rk.offset) for rk, _ in rows] == sorted(
                (rk.timestamp_us, rk.offset) for rk, _ in rows
            )
            for rk, value in rows:
                assert value == produced[(partition, rk.offset)]
                assert rk.group == ks.group
            assert ks.row_count == len(rows)
            assert ks.staged_bytes == sum(len(v) for _, v in rows)
            total_rows += len(rows)
            ranges.extend(await stage.read_offsets(ks.group))
        ordered = sorted(ranges, key=lambda r: r.first_offset)
        assert ordered[0].first_offset == 0
        assert max(r.last_offset for r in ordered) == (19 if partition == 0 else 9)
        # the union covers the produced range contiguously (a sweep, not
        # a pairwise check — earlier ranges bridge later gaps)
        covered_through = ordered[0].last_offset
        for r in ordered[1:]:
            assert r.first_offset <= covered_through + 1
            covered_through = max(covered_through, r.last_offset)
    assert total_rows == 30

    gauges = await manager.gauges()
    assert gauges.staged_rows == 30
    assert gauges.staged_bytes == sum(len(v) for v in produced.values())
    assert gauges.oldest_first_staged_ts == NOW  # the injected staging clock
    await manager.close()


@component
async def test_error_events_are_counted_and_never_staged_or_committed(fake_clock):
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    consumer = make_consumer(fake, manager, make_config(), fake_clock)
    await consumer.start()

    fake.feed(
        error_event(0, 0, KafkaError._PARTITION_EOF),
        team_msg(0, 0, team=1, tag="good"),
        error_event(0, 1, KafkaError._TRANSPORT),
        team_msg(0, 1, team=1, tag="good2"),
    )
    await run_steps(consumer, fake)

    stats = consumer.stats()
    assert stats.kafka_error_events == 2
    assert stats.records_staged == 2
    assert fake.committed_store == {("events", 0): 2}
    stage = manager.stage("events", 0)
    assert await team_offsets(stage, 1) == [0, 1]
    assert stats.records_dropped == {}
    await manager.close()


async def stage_key_prefixes(stage: PartitionStage) -> set[bytes]:
    """Every key family present in the stage (the bytes up to the first
    ``/``) — what a drop must never add to."""
    return {
        key.split(b"/", 1)[0] + b"/"
        async for key, _ in stage._iter_range(b"\x00", b"\xff")
    }


@component
async def test_malformed_records_are_dropped_counted_and_never_block(
    fake_clock, caplog
):
    """Malformed records stage nothing, are counted per reason and logged
    at WARNING with topic/partition/offset/reason, and the partition's
    commit covers them — they never wedge the loop."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    consumer = make_consumer(fake, manager, make_config(), fake_clock)
    await consumer.start()

    fake.feed(
        team_msg(0, 0, team=3, tag="good-0"),
        msg(0, 1, key=b"not-a-team", value=b"junk" * 20),  # not JSON
        msg(0, 2, key=b"3", ts_ms=None),  # no timestamp
        msg(0, 3, key=b"3", value=None),  # tombstone
        team_msg(0, 4, team=3, tag="good-4"),
    )
    with caplog.at_level(logging.WARNING, logger="millrace.consumer"):
        await run_steps(consumer, fake)

    assert_commits_follow_acks(journal, dropped={("events", 0, o) for o in (1, 2, 3)})
    # The commit covers the dropped records' offsets.
    assert fake.committed_store == {("events", 0): 5}
    stats = consumer.stats()
    assert stats.records_staged == 2
    assert stats.records_dropped == {
        REASON_MALFORMED_VALUE: 1,
        REASON_MISSING_TIMESTAMP: 1,
        REASON_MISSING_VALUE: 1,
    }

    stage = manager.stage("events", 0)
    assert await team_offsets(stage, 3) == [0, 4]
    assert await stage_key_prefixes(stage) == {
        b"rows/",
        b"stats/",
        b"sched_age/",
        b"offsets/",
    }
    lines = [r.getMessage() for r in caplog.records if "record dropped" in r.message]
    assert [line.split(" (")[0] for line in lines] == [
        "record dropped: topic=events partition=0 offset=1 reason=malformed_value",
        "record dropped: topic=events partition=0 offset=2 reason=missing_timestamp",
        "record dropped: topic=events partition=0 offset=3 reason=missing_value",
    ]
    await manager.close()


@component
async def test_a_poll_of_only_dropped_records_still_commits(fake_clock):
    """Nothing to stage is still handled: the offset advances past a
    poll whose every record was dropped, with no stage_batch call."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    consumer = make_consumer(fake, manager, make_config(), fake_clock)
    await consumer.start()

    fake.feed(msg(0, 0, key=None, value=b"x"), msg(0, 1, key=None, value=None))
    await run_steps(consumer, fake)

    assert_commits_follow_acks(journal, dropped={("events", 0, 0), ("events", 0, 1)})
    assert not [e for e in journal if e[0] == "ack"]  # nothing was staged
    assert fake.committed_store == {("events", 0): 2}
    stats = consumer.stats()
    assert stats.records_staged == 0
    assert sum(stats.records_dropped.values()) == 2
    assert await manager.stage("events", 0).iter_key_stats() == []
    await manager.close()


@component
async def test_drop_logging_is_rate_limited_per_partition_and_reason(
    fake_clock, caplog
):
    """At most one line per (partition, reason) per DROP_LOG_INTERVAL_S:
    the first drop of a reason always logs, a different reason or
    partition logs independently, and the next line after the interval
    names the suppressed count. The counter counts every drop."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    mono = FakeMonotonic()
    consumer = make_consumer(
        fake,
        manager,
        make_config(static_partitions=(0, 1)),
        fake_clock,
        monotonic=mono,
    )
    await consumer.start()

    def dropped_lines() -> list[str]:
        return [r.getMessage() for r in caplog.records if "record dropped" in r.message]

    with caplog.at_level(logging.WARNING, logger="millrace.consumer"):
        fake.feed(
            *[msg(0, o, key=None, value=b"x") for o in range(5)],  # malformed_value
            msg(0, 5, key=None, value=None),  # missing_value: its own first line
            msg(1, 0, key=None, value=b"x"),  # another partition: its own line
            msg(0, 6, key=None, value=b'{"team_id": "7"}'),  # path reason
            msg(0, 7, key=None, value=b'{"team_id": "8"}'),
        )
        await run_steps(consumer, fake)
        first = dropped_lines()
        assert [line.split(" (")[0] for line in first] == [
            "record dropped: topic=events partition=0 offset=0 reason=malformed_value",
            "record dropped: topic=events partition=0 offset=5 reason=missing_value",
            "record dropped: topic=events partition=1 offset=0 reason=malformed_value",
            (
                "record dropped: topic=events partition=0 offset=6 "
                "reason=malformed_partition_source:team_id"
            ),
        ]
        assert all("(0 more" in line for line in first)

        # Still inside the interval: suppressed.
        mono.now += DROP_LOG_INTERVAL_S - 1
        fake.feed(msg(0, 8, key=None, value=b"x"))
        await run_steps(consumer, fake)
        assert dropped_lines() == first

        # Past it: one line, carrying what was suppressed (offsets 1-4, 8).
        mono.now += 2
        fake.feed(msg(0, 9, key=None, value=b"x"))
        await run_steps(consumer, fake)
        (late,) = dropped_lines()[len(first) :]
        assert late.startswith(
            "record dropped: topic=events partition=0 offset=9 reason=malformed_value"
        )
        assert "(5 more" in late

    assert consumer.stats().records_dropped == {
        REASON_MALFORMED_VALUE: 8,
        REASON_MISSING_VALUE: 1,
        REASON_MALFORMED_PARTITION_SOURCE: 2,
    }
    assert fake.committed_store == {("events", 0): 10, ("events", 1): 1}
    await manager.close()


@component
async def test_a_keyless_topic_stages_by_the_payloads_partition_tuple(fake_clock):
    """The Kafka key is never read: keyless records (and records with
    junk keys) stage under the tuple their PAYLOAD gives; a payload that
    is not a JSON object, or whose source scalar is malformed, drops
    with the field named; the commit covers all of it."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    consumer = make_consumer(fake, manager, make_config(), fake_clock)
    await consumer.start()

    fake.feed(
        msg(0, 0, key=None, value=b'{"team_id": 7, "event": "a"}'),
        msg(0, 1, key=None, value=b'{"team_id": 9, "event": "b"}'),
        msg(0, 2, key=None, value=b'{"team_id": 7, "event": "c"}'),
        msg(0, 3, key=b"junk-never-consulted", value=b'{"team_id": 9}'),
        msg(0, 4, key=None, value=b'{"event": "no team field"}'),
        msg(0, 5, key=None, value=b'{"team_id": "7"}'),
        msg(0, 6, key=None, value=b"not json"),
    )
    await run_steps(consumer, fake)

    assert_commits_follow_acks(journal, dropped={("events", 0, 5), ("events", 0, 6)})
    assert fake.committed_store == {("events", 0): 7}
    stats = consumer.stats()
    assert stats.records_staged == 5
    assert stats.records_dropped == {
        REASON_MALFORMED_PARTITION_SOURCE: 1,
        REASON_MALFORMED_VALUE: 1,
    }
    stage = manager.stage("events", 0)
    assert await team_offsets(stage, 7) == [0, 2]
    assert await team_offsets(stage, 9) == [1, 3]
    # An absent source is a null component, not a drop.
    assert [rk.offset async for rk, _ in stage.scan_group_rows((None,))] == [4]
    # The staged payload is the raw value bytes — classify parsed the
    # JSON once to read the tuple and never rewrote the record.
    rows = [(rk.offset, v) async for rk, v in stage.scan_group_rows(("7",))]
    assert rows[0] == (0, b'{"team_id": 7, "event": "a"}')
    await manager.close()


@component
async def test_a_spec_refresh_mid_stream_starts_new_groups_and_keeps_the_old(
    fake_clock,
):
    """Past the shape TTL the consumer re-reads the destination: records
    after the refresh stage under the NEW spec's tuples, the group staged
    under the old spec stays staged — and the planner still decides it,
    so it drains by age (the flush's live-spec fanout splits it; pinned
    in test_flush.py)."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    shape = FakeShape(ttl_s=450.0)
    mono = FakeMonotonic()
    consumer = make_consumer(
        fake,
        manager,
        make_config(),
        fake_clock,
        keyer=GroupKeyer(table_info([IDENTITY_TEAM], spec_id=1)),
        shape=shape,
        monotonic=mono,
    )
    await consumer.start()
    payload = team_payload(7, "x")[:-1] + b',"timestamp":%d}' % JAN_TS

    fake.feed(msg(0, 0, key=None, value=payload))
    await run_steps(consumer, fake, extra=0)
    assert shape.calls == 0  # inside the TTL: no re-read

    shape.reads.append(table_info([IDENTITY_TEAM, MONTH_TIMESTAMP], spec_id=2))
    mono.now += 450.0
    fake.feed(msg(0, 1, key=None, value=payload))
    await run_steps(consumer, fake, extra=0)
    assert shape.calls == 1
    assert consumer.keyer.spec_id == 2
    assert consumer.keyer.fields == ("team_id", "timestamp_month")

    stage = manager.stage("events", 0)
    stats = {ks.group: ks for ks in await stage.iter_key_stats()}
    assert set(stats) == {("7",), ("7", JAN_MONTH)}
    assert [rk.offset async for rk, _ in stage.scan_group_rows(("7",))] == [0]
    assert [rk.offset async for rk, _ in stage.scan_group_rows(("7", JAN_MONTH))] == [1]
    # Both groups are flushable: past the slow-lane deadline the planner
    # decides the old group exactly like the new one.
    knobs = PlannerKnobs(min_flush_bytes=1)
    later = NOW + knobs.slow_lane_deadline_s * 1_000_000
    decided = {d.group for d in plan_flush(stats.values(), later, knobs)}
    assert decided == {("7",), ("7", JAN_MONTH)}
    assert fake.committed_store == {("events", 0): 2}
    await manager.close()


@component
async def test_a_failed_shape_reread_keeps_the_old_keyer(fake_clock, caplog):
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    shape = FakeShape(ttl_s=450.0)
    mono = FakeMonotonic()
    keyer = GroupKeyer(table_info([IDENTITY_TEAM]))
    consumer = make_consumer(
        fake,
        manager,
        make_config(),
        fake_clock,
        keyer=keyer,
        shape=shape,
        monotonic=mono,
    )
    await consumer.start()
    await consumer._step()  # arms the first re-read one TTL out
    shape.reads.append(RuntimeError("hoglake unreachable"))
    mono.now += 451.0
    fake.feed(team_msg(0, 0, team=7))
    with caplog.at_level(logging.WARNING, logger="millrace.consumer"):
        await run_steps(consumer, fake, extra=0)
    assert shape.calls == 1
    assert consumer.keyer is keyer
    assert "keeps grouping under partition spec 1" in caplog.text
    assert await team_offsets(manager.stage("events", 0), 7) == [0]
    # The next attempt waits a full TTL: no retry storm against hoglake.
    await consumer._step()
    assert shape.calls == 1
    await manager.close()


@component
async def test_a_recreated_table_at_reread_halts_the_loop_without_committing(
    fake_clock,
):
    """``ShapeReader.read`` refuses another incarnation with
    ``FlushHalted("table_recreated")``: that is not a transient read
    failure, so the consume loop halts BEFORE polling — nothing further
    is staged or committed against a table the pipeline no longer
    writes (main.py maps the halt to exit 3)."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    shape = FakeShape(ttl_s=450.0)
    mono = FakeMonotonic()
    consumer = make_consumer(
        fake, manager, make_config(), fake_clock, shape=shape, monotonic=mono
    )
    await consumer.start()
    fake.feed(team_msg(0, 0, team=7))
    await run_steps(consumer, fake, extra=0)
    assert fake.committed_store == {("events", 0): 1}

    shape.reads.append(FlushHalted("table_recreated", "another incarnation"))
    mono.now += 450.0
    fake.feed(team_msg(0, 1, team=7))
    consume_calls = fake.consume_calls
    with pytest.raises(FlushHalted, match="table_recreated"):
        await consumer._step()
    assert fake.consume_calls == consume_calls  # never polled
    assert fake.committed_store == {("events", 0): 1}
    assert await team_offsets(manager.stage("events", 0), 7) == [0]
    await manager.close()


@component
async def test_cooperative_assignment_opens_and_recovers_stages(fake_clock):
    """Group assignment drives sync_assignment: the on_assign callback
    (fired inside consume(), as librdkafka does) opens and recovers the
    claimed partitions' stages before their first record is staged."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    cfg = make_config(
        assignment_mode=AssignmentMode.COOPERATIVE, static_partitions=None
    )
    consumer = make_consumer(fake, manager, cfg, fake_clock)
    await consumer.start()
    assert fake.subscribed_topics == ["events"]
    assert manager.partitions() == ()

    # The group hands us partitions 0 and 1; a record rides the same poll.
    fake.feed(team_msg(0, 0, team=1, tag="first"))
    fake.script_rebalance(assign=[0, 1])
    await consumer._step()

    assert ("rebalance_cb", "assign", [("events", 0), ("events", 1)]) in journal
    assert manager.partitions() == (("events", 0), ("events", 1))
    assert fake.committed_store == {("events", 0): 1}
    stage = manager.stage("events", 0)
    assert await team_offsets(stage, 1) == [0]
    await manager.close()


@component
async def test_revoke_closes_cleanly_and_loses_nothing(fake_clock):
    """On revoke the partition's stage closes (flush nothing, abandon
    nothing — staged state is durable on the store), its not-yet-
    delivered messages are never staged or committed, and a later
    reclaim finds the previous owner's rows intact."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    cfg = make_config(
        assignment_mode=AssignmentMode.COOPERATIVE, static_partitions=None
    )
    consumer = make_consumer(fake, manager, cfg, fake_clock)
    await consumer.start()
    fake.script_rebalance(assign=[0, 1])
    await consumer._step()

    fake.feed(*[team_msg(0, o, team=1, tag=f"a{o}") for o in range(3)])
    fake.feed(*[team_msg(1, o, team=2, tag=f"b{o}") for o in range(2)])
    await run_steps(consumer, fake)
    assert fake.committed_store == {("events", 0): 3, ("events", 1): 2}
    stage0 = manager.stage("events", 0)

    # Revoke 0 while two more of its records sit in the fetch buffer.
    fake.feed(*[team_msg(0, o, team=1, tag=f"a{o}") for o in (3, 4)])
    fake.script_rebalance(revoke=[0])
    await consumer._step()

    # The stage is closed; nothing of the revoked partition was staged
    # or committed past 3; partition 1 is unaffected.
    assert manager.partitions() == (("events", 1),)
    assert fake.committed_store[("events", 0)] == 3
    with pytest.raises(StageClosedError):
        await stage0.gauges()
    stats = consumer.stats()
    assert stats.messages_skipped_revoked == 0  # held, not delivered

    # Zero loss: reopening the partition's path shows exactly the
    # previously committed rows — the new owner's starting point.
    reopened = await PartitionStage.open_store(
        manager.store,
        manager.path_for("events", 0),
        topic="events",
        partition=0,
        settings=fast_flush_settings(),
    )
    assert await reopened.recover() is not None
    assert await team_offsets(reopened, 1) == [0, 1, 2]
    await reopened.close()

    # Reclaim (assignment changed between deploys): the stage reopens
    # with the old rows intact, and the records that were held while
    # unassigned now fetch, stage and commit.
    fake.script_rebalance(assign=[0])
    await consumer._step()
    assert manager.partitions() == (("events", 0), ("events", 1))
    reclaimed = manager.stage("events", 0)
    assert set(await team_offsets(reclaimed, 1)) >= {0, 1, 2}
    await run_steps(consumer, fake)
    assert await team_offsets(reclaimed, 1) == [0, 1, 2, 3, 4]
    assert fake.committed_store[("events", 0)] == 5
    assert_commits_follow_acks(journal)
    await manager.close()


@component
async def test_messages_delivered_with_a_same_poll_revoke_are_skipped(fake_clock):
    """The other real ordering: librdkafka returns already-queued
    messages from the call whose callbacks revoke their partition. The
    loop must skip them (never stage, never commit) — the new owner
    replays from the last commit."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    cfg = make_config(
        assignment_mode=AssignmentMode.COOPERATIVE, static_partitions=None
    )
    consumer = make_consumer(fake, manager, cfg, fake_clock)
    await consumer.start()
    fake.script_rebalance(assign=[0])
    await consumer._step()
    fake.feed(*[team_msg(0, o, team=1) for o in range(3)])
    await run_steps(consumer, fake)
    assert fake.committed_store == {("events", 0): 3}

    fake.feed(*[team_msg(0, o, team=1) for o in (3, 4)])
    fake.script_rebalance(revoke=[0], after_delivery=True)
    await consumer._step()

    assert fake.committed_store == {("events", 0): 3}
    assert consumer.stats().messages_skipped_revoked == 2
    assert manager.partitions() == ()
    await manager.close()


@component
async def test_lost_partitions_close_like_revoked_ones(fake_clock):
    """on_lost is the ungraceful revoke: the new owner may already hold
    the path (SlateDB fences); the local stage still closes cleanly and
    nothing further is staged."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    cfg = make_config(
        assignment_mode=AssignmentMode.COOPERATIVE, static_partitions=None
    )
    consumer = make_consumer(fake, manager, cfg, fake_clock)
    await consumer.start()
    fake.script_rebalance(assign=[0])
    await consumer._step()
    fake.feed(team_msg(0, 0, team=1))
    await run_steps(consumer, fake)
    assert fake.committed_store == {("events", 0): 1}

    fake.script_rebalance(lost=[0])
    await consumer._step()
    assert ("rebalance_cb", "lost", [("events", 0)]) in journal
    assert manager.partitions() == ()
    assert fake.committed_store == {("events", 0): 1}
    await manager.close()


@component
async def test_commit_retries_then_succeeds(fake_clock):
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    sleeper = Sleeper()
    consumer = make_consumer(fake, manager, make_config(), fake_clock, sleeper)
    await consumer.start()

    fake.feed(*[team_msg(0, o, team=1) for o in range(3)])
    fake.fail_next_commits(2)
    await run_steps(consumer, fake)

    assert fake.committed_store == {("events", 0): 3}
    assert sleeper.calls == [0.5, 1.0]  # the ladder: 0.5 * 2**attempt
    stats = consumer.stats()
    assert stats.commit_attempts == 3 and stats.commit_retries == 2
    assert_commits_follow_acks(journal)
    await manager.close()


@component
async def test_commit_exhaustion_raises_without_committing(fake_clock):
    """The rows stay durable; the offsets never advanced — the restart
    replays and restages idempotently."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    sleeper = Sleeper()
    consumer = make_consumer(fake, manager, make_config(), fake_clock, sleeper)
    await consumer.start()

    fake.feed(*[team_msg(0, o, team=1) for o in range(3)])
    fake.fail_next_commits(3)
    with pytest.raises(KafkaException):
        await consumer._step()

    assert fake.committed_store == {}
    assert consumer.committed_offsets() == {}
    assert not [e for e in journal if e[0] == "commit"]
    # ...but the batch IS durable (acks precede any commit attempt).
    stage = manager.stage("events", 0)
    assert await team_offsets(stage, 1) == [0, 1, 2]
    await manager.close()


@component
async def test_a_failed_batch_commits_nothing(fake_clock):
    """No commit for a failed batch: a stage failure propagates and the
    cycle's offsets stay uncommitted."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    consumer = make_consumer(fake, manager, make_config(), fake_clock)
    await consumer.start()
    fake.feed(team_msg(0, 0, team=1))

    # Sabotage: the stage is closed underneath the loop.
    await manager.stage("events", 0).close()
    with pytest.raises(StageClosedError):
        await consumer._step()
    assert fake.committed_store == {}
    assert not [e for e in journal if e[0] == "commit"]
    assert not [e for e in journal if e[0] == "ack"]
    await manager.close()


# -- fencing (B5): a newer writer owns the path ---------------------------------------


@component
async def test_a_fenced_partition_closes_locally_and_halts_distinctly(fake_clock):
    """A REAL contested open fences a live consumer. A second writer on
    partition 0's path (an EKS rollout double-assigning the ordinal —
    the open itself succeeds; the fencing is lazy, pinned by
    tests/test_slatedb_parity.py) makes this pod's next staged write
    raise ``Error.Closed(FENCED)``. The loop catches it, closes the
    fenced stage locally, and raises the DISTINCT ConsumerFencedError —
    the anti-flap semantics (a crash-loop restart would re-open the
    path and fence the NEW owner back)."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    consumer = make_consumer(
        fake, manager, make_config(static_partitions=(0, 1)), fake_clock
    )
    await consumer.start()
    fake.feed(team_msg(0, 0, team=1), team_msg(1, 0, team=2))
    await run_steps(consumer, fake)
    assert fake.committed_store == {("events", 0): 1, ("events", 1): 1}

    # The rollout's replacement pod opens the same path (SlateDB lets
    # the open through; the OLD writer — us — is fenced from here).
    new_owner = await DbBuilder(manager.path_for("events", 0), manager.store).build()
    try:
        fake.feed(team_msg(0, 1, team=1), team_msg(1, 1, team=2))
        with pytest.raises(ConsumerFencedError) as excinfo:
            await consumer._step()
        assert excinfo.value.partitions == (("events", 0),)

        # The fenced stage is closed locally — the manager no longer
        # holds it, so this process cannot touch the contested path
        # again (the anti-flap invariant).
        assert manager.partitions() == (("events", 1),)
        # NOTHING committed past the fenced batch — not even partition
        # 1's sibling batch, which IS durable (its rows replay
        # idempotently): offsets move only after EVERY ack.
        assert fake.committed_store == {("events", 0): 1, ("events", 1): 1}
        assert consumer.committed_offsets() == {
            ("events", 0): 1,
            ("events", 1): 1,
        }
        stage1 = manager.stage("events", 1)
        assert await team_offsets(stage1, 2) == [0, 1]
    finally:
        await new_owner.shutdown()
    await manager.close()


@component
async def test_a_non_fenced_closed_stage_error_is_not_the_fenced_path(fake_clock):
    """Only FENCED takes the distinct halt: a ``Closed`` with any other
    reason (a local shutdown racing an in-flight call) propagates as
    the generic error — no local relinquish, no ConsumerFencedError.
    Pins the reason predicate (``== CloseReason.FENCED``) against its
    mutants."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    consumer = make_consumer(fake, manager, make_config(), fake_clock)
    await consumer.start()
    fake.feed(team_msg(0, 0, team=1))

    stage = manager.stage("events", 0)

    async def clean_closed(records, *, now_us):
        raise SlateError.Closed(CloseReason.CLEAN, "scripted local shutdown")

    stage.stage_batch = clean_closed  # type: ignore[method-assign]
    with pytest.raises(SlateError.Closed):
        await consumer._step()
    # NOT the fenced path: the stage stays open in the manager, nothing
    # committed.
    assert manager.partitions() == (("events", 0),)
    assert fake.committed_store == {}
    await manager.close()


# -- T7: per-partition staging overlaps within one poll ------------------------------


@component
async def test_partitions_stage_concurrently_within_one_poll(fake_clock):
    """One poll stages its partitions CONCURRENTLY — the per-partition
    WAL writes and durability waits overlap — and the single commit
    still follows EVERY ack (the invariant is "all acks before the
    commit", not "acks in sequence"; serial per-partition waits would
    cap the pod at one remote round trip per partition per poll).

    The gate makes overlap deterministic: each partition's spied
    stage_batch records its start and then waits on a gate that opens
    only once BOTH partitions are in flight. Sequential staging can
    never open the gate (partition 1 cannot start before partition 0
    returns) and the step times out instead of passing.
    """
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    consumer = make_consumer(
        fake, manager, make_config(static_partitions=(0, 1)), fake_clock
    )
    await consumer.start()

    gate = asyncio.Event()
    in_flight: set[tuple[str, int]] = set()
    order: list[tuple[str, tuple[str, int]]] = []

    def make_spy(key, original):
        async def spy(records, *, now_us):
            order.append(("start", key))
            in_flight.add(key)
            if len(in_flight) == 2:
                gate.set()
            await asyncio.wait_for(gate.wait(), timeout=5)
            ack = await original(records, now_us=now_us)
            order.append(("end", key))
            return ack

        return spy

    for key in (("events", 0), ("events", 1)):
        stage = manager.stage(*key)
        stage.stage_batch = make_spy(key, stage.stage_batch)  # type: ignore[method-assign]

    fake.feed(team_msg(0, 0, team=1), team_msg(1, 0, team=2))
    await consumer._step()

    # Both partitions' batches were in flight together: every start
    # precedes the first end.
    kinds = [kind for kind, _ in order]
    assert kinds == ["start", "start", "end", "end"]
    assert {key for kind, key in order if kind == "start"} == {
        ("events", 0),
        ("events", 1),
    }
    assert fake.committed_store == {("events", 0): 1, ("events", 1): 1}
    assert_commits_follow_acks(journal)
    await manager.close()


@component
async def test_the_byte_cap_clamps_the_next_polls_batch_size(fake_clock):
    """The per-poll byte cap shapes what the NEXT consume asks for:
    ``batch_max_bytes ÷ observed average record size`` (key + value
    bytes). The first poll has no observation and is bounded by the
    record cap alone."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    cfg = make_config(consume_batch_size=1000, consume_batch_max_bytes=1000)
    consumer = make_consumer(fake, manager, cfg, fake_clock)
    await consumer.start()

    # 20 records of 121 observed bytes each (a 120-byte JSON value + a
    # 1-byte key): avg 121, so every later poll requests 1000 // 121 == 8.
    messages = [team_msg(0, o, team=1, tag="v" * 100) for o in range(20)]
    assert {len(m.value() or b"") + len(m.key() or b"") for m in messages} == {121}
    fake.feed(*messages)
    await run_steps(consumer, fake)
    assert fake.requested_batch_sizes[0] == 1000
    assert fake.requested_batch_sizes[1:] and set(fake.requested_batch_sizes[1:]) == {8}
    assert fake.committed_store == {("events", 0): 20}
    assert_commits_follow_acks(journal)
    await manager.close()


@component
async def test_the_byte_cap_never_stalls_an_oversized_record(fake_clock):
    """The clamp floors at ONE message per poll: records larger than
    the whole byte budget still flow (one per poll) — a 0-message
    request would idle forever with the record sitting in the fetch
    queue. (Without the floor the last three records never commit:
    bounded steps, not a drain loop, so that mutant reds instead of
    hanging.)"""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    cfg = make_config(consume_batch_size=5, consume_batch_max_bytes=10)
    consumer = make_consumer(fake, manager, cfg, fake_clock)
    await consumer.start()

    # 8 records of 121 observed bytes: poll 1 takes 5 (the record cap),
    # then avg 121 > the 10-byte budget, so every later poll asks for
    # exactly 1.
    fake.feed(*[team_msg(0, o, team=1, tag="v" * 100) for o in range(8)])
    for _ in range(8):
        await consumer._step()
    assert fake.requested_batch_sizes[:2] == [5, 1]
    assert fake.committed_store == {("events", 0): 8}
    assert_commits_follow_acks(journal)
    await manager.close()


# -- shutdown ------------------------------------------------------------------------


@component
async def test_graceful_shutdown_stops_polling_with_offsets_committed(fake_clock):
    """run() + stop(): polling stops, the Kafka consumer closes, and the
    committed offsets already reflect exactly what was durably staged
    (every staged batch commits inside its cycle). The StageManager is
    caller-owned and is NOT closed by the consumer."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    consumer = make_consumer(fake, manager, make_config(poll_timeout_ms=20), fake_clock)
    fake.feed(*[team_msg(0, o, team=1, tag=f"r{o}") for o in range(5)])

    task = asyncio.create_task(consumer.run())
    for _ in range(500):
        if fake.committed_store.get(("events", 0)) == 5:
            break
        await asyncio.sleep(0.005)
    else:
        consumer.stop()
        raise AssertionError("never committed the five records")
    consumer.stop()
    await asyncio.wait_for(task, timeout=10)

    consume_calls_at_stop = fake.consume_calls
    assert fake.closed
    assert fake.committed_store == {("events", 0): 5}
    # No polling after close — ever.
    await asyncio.sleep(0.05)
    assert fake.consume_calls == consume_calls_at_stop
    # The caller-owned manager is untouched: its stage is still open,
    # and a reopen-equivalent read shows the durable rows.
    stage = manager.stage("events", 0)
    assert await team_offsets(stage, 1) == [0, 1, 2, 3, 4]
    await manager.close()


@component
async def test_stop_before_the_first_poll_still_closes_cleanly(fake_clock):
    """stop() ahead of run(): the loop starts, sees the flag, never
    consumes, and closes the Kafka side."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    consumer = make_consumer(fake, manager, make_config(), fake_clock)
    consumer.stop()
    await asyncio.wait_for(consumer.run(), timeout=10)
    assert fake.consume_calls == 0
    assert fake.closed
    # The static claim still happened: start() runs before the stop check.
    assert ("assign", [("events", 0, OFFSET_STORED)]) in journal
    await manager.close()


@component
async def test_run_propagates_a_fatal_error_but_still_closes_kafka(fake_clock):
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    consumer = make_consumer(fake, manager, make_config(), fake_clock)
    await consumer.start()

    async def broken(records, *, now_us):
        raise RuntimeError("scripted staging failure")

    manager.stage("events", 0).stage_batch = broken  # type: ignore[method-assign]
    fake.feed(team_msg(0, 0, team=1))
    with pytest.raises(RuntimeError, match="scripted staging failure"):
        await consumer.run()
    assert fake.closed
    assert fake.committed_store == {}
    await manager.close()


# -- stats snapshot shape -------------------------------------------------------------


@component
async def test_stats_snapshot_counts_the_run(fake_clock):
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    consumer = make_consumer(fake, manager, make_config(), fake_clock)
    await consumer.start()
    fake.feed(
        team_msg(0, 0, team=1),
        msg(0, 1, key=b"bad", value=b"not json"),
        error_event(0, 2, KafkaError._PARTITION_EOF),
        team_msg(0, 2, team=1),
    )
    await run_steps(consumer, fake)
    assert consumer.stats() == ConsumerStats(
        messages_consumed=4,
        records_staged=2,
        records_dropped={REASON_MALFORMED_VALUE: 1},
        kafka_error_events=1,
        messages_skipped_revoked=0,
        commit_attempts=1,
        commit_retries=0,
    )
    await manager.close()
