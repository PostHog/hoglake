"""Consume-loop tests (docs/kafka-ingestion-plan.md Phase 3).

Layers: the PURE decisions (record classification — the value parsed
once as a JSON object and keyed by the destination's partition tuple —
and the client-config pins) are unit tests without I/O;
the loop itself is driven component-style — a scripted FakeConsumer
(tests/kafkakit.py, hedgerow's fakes.py shape) over a REAL
StageManager on ``memory:///``, so poll → stage → ack → commit ordering,
assignment open/discover, the startup drain, drops and shutdown are all pinned
against durable staged state with no broker anywhere.

Pinned here:

- the call-order invariant: a commit is NEVER observed before the
  durable ack it covers (rows-then-offset, hedgerow's rule), and
  commits are monotone per partition;
- the assignment is derived ONCE at start: the topic's partition count
  from one ``list_topics`` metadata read, split by millpond's rule
  (``p % replica_count == ordinal``, uneven counts included); the stages
  open (their staged groups discovered), the caller's drain runs, and
  only then ``assign()`` with OFFSET_STORED; an unknown topic
  or an ordinal with no partitions refuses to start with nothing
  claimed;
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
  the distinct FencedError, without committing the failed batch;
- malformed records are dropped: nothing staged, counted per reason
  label (the partition-source path collapsed), logged at WARNING
  rate-limited per (partition, reason), and covered by the partition's
  commit;
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
from stagekit import NOW, fast_flush_settings, rec

import millrace.consumer as consumer_mod
from millrace.config import AutoOffsetReset, Config
from millrace.consumer import (
    DROP_LOG_INTERVAL_S,
    REASON_INVALID_OFFSET,
    REASON_MALFORMED_PARTITION_SOURCE,
    REASON_MALFORMED_VALUE,
    REASON_MISSING_VALUE,
    AssignmentError,
    ConsumerStats,
    MillraceConsumer,
    classify_message,
    compute_assignment,
    create_kafka_consumer,
    drop_reason_label,
    kafka_consumer_config,
)
from millrace.flush import FlushHalted
from millrace.grouping import GroupKeyer
from millrace.planner import PlannerKnobs, decide
from millrace.stage import (
    DroppedRecord,
    FencedError,
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
        # One replica, ordinal 0: with the fake's default one-partition
        # topic the consumer claims exactly partition 0. A test wanting
        # partitions 0 and 1 builds ``FakeConsumer(partitions=2)``.
        "replica_count": 1,
        "ordinal": 0,
        "catalog": "prod",
        "namespace": "default",
        "table": "events",
        "stage_store_url": "memory:///",
        "stage_base_path": "millrace",
        "consume_batch_size": 100,
        "poll_timeout_ms": 50,
    }
    return Config(**{**base, **overrides})


class Sleeper:
    """The injected commit-retry sleep: records, never waits."""

    def __init__(self) -> None:
        self.calls: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)


class JournalingStageManager(StageManager):
    """A real StageManager whose stages journal every durable
    ``stage_batch`` as ``("ack", StageAck)`` once it returns: the
    rows-then-offset ordering is checked against the same journal the
    fake consumer writes its commits to."""

    def __init__(self, journal: list[tuple], slot_prober: Any | None = None) -> None:
        super().__init__(
            "memory:///",
            "millrace",
            settings=fast_flush_settings(),
            slot_prober=slot_prober,
        )
        self.journal = journal

    async def open_partition(self, topic: str, partition: int) -> PartitionStage:
        stage = await super().open_partition(topic, partition)
        real = stage.stage_batch

        async def journaled(records: Any, *, now_us: int) -> Any:
            ack = await real(records, now_us=now_us)
            self.journal.append(("ack", ack))
            return ack

        stage.stage_batch = journaled  # type: ignore[method-assign]
        return stage


def make_manager(journal: list[tuple], slot_prober: Any | None = None) -> StageManager:
    return JournalingStageManager(journal, slot_prober)


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
    """Drive the loop until the fake has nothing deliverable, plus
    ``extra`` idle steps (the idle steps matter: a trailing error event
    can land on an otherwise-empty poll, so the drain condition is
    re-checked after them)."""
    while True:
        while fake.deliverable():
            await consumer._step()
        for _ in range(extra):
            await consumer._step()
        if not fake.deliverable():
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
            for partition, last in ack.last_offsets.items():
                key = (ack.topic, partition)
                acked_through[key] = max(last, acked_through.get(key, -1))
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
    return await group_offsets(stage, (str(team),))


async def group_rows(
    stage: PartitionStage, group: tuple[str | None, ...]
) -> list[tuple[Any, bytes]]:
    """Every staged row of ``group`` in key order."""
    return (await stage.scan(group, max_bytes=1 << 40, now_us=0)).rows


async def group_offsets(
    stage: PartitionStage, group: tuple[str | None, ...]
) -> list[int]:
    return [rk.offset for rk, _ in await group_rows(stage, group)]


# ==================================================================================
# Pure: record classification under the destination's partition spec
# ==================================================================================


def _classify(
    keyer: GroupKeyer,
    value: bytes | None,
    *,
    offset: int = 41,
) -> StagedRecord | DroppedRecord:
    return classify_message(keyer, offset=offset, value=value)


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
    assert _classify(keyer, None) == DroppedRecord(41, REASON_MISSING_VALUE)


def test_the_record_timestamp_is_never_read():
    """Rows are keyed (group, offset) and the stats use the staging
    clock: classification has no timestamp input, so a record without
    one (TIMESTAMP_NOT_AVAILABLE) stages like any other (the loop-level
    pin is test_malformed_records_are_dropped_counted_and_never_block)."""
    import inspect

    assert "timestamp" not in inspect.signature(classify_message).parameters
    keyer = GroupKeyer(table_info([IDENTITY_TEAM]))
    out = _classify(keyer, b'{"team_id": 7}')
    assert out == StagedRecord(("7",), 41, b'{"team_id": 7}')


def test_classify_out_of_range_offset():
    keyer = GroupKeyer(table_info([IDENTITY_TEAM]))
    out = _classify(keyer, b'{"team_id": 7}', offset=-1)
    assert out == DroppedRecord(-1, REASON_INVALID_OFFSET)
    out = _classify(keyer, b'{"team_id": 7}', offset=2**63)
    assert out == DroppedRecord(2**63, REASON_INVALID_OFFSET)
    assert _classify(keyer, b'{"team_id": 7}', offset=2**63 - 1) == StagedRecord(
        ("7",), 2**63 - 1, b'{"team_id": 7}'
    )


def test_classify_names_the_FIRST_failing_check():
    keyer = GroupKeyer(table_info([IDENTITY_TEAM]))
    # An invalid offset outranks everything; a tombstone is
    # missing_value, never a parse error.
    out = _classify(keyer, None, offset=-1)
    assert out == DroppedRecord(-1, REASON_INVALID_OFFSET)
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
    # The consumer never joins the group: no assignment strategy on the
    # wire. The client id carries the ordinal (millpond's shape) so the
    # broker side can tell replicas apart.
    assert "partition.assignment.strategy" not in conf
    assert conf["client.id"] == "millrace-events-0"


def test_consumer_config_is_exactly_the_documented_dict_without_passthrough():
    """No MILLRACE_KAFKA_CONSUMER_* set: the client config is exactly
    millrace's own keys, nothing added, nothing renamed."""
    assert kafka_consumer_config(make_config()) == {
        "bootstrap.servers": "broker:9092",
        "client.id": "millrace-events-0",
        "group.id": "millrace-events",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "max.partition.fetch.bytes": 32 * 1024 * 1024,
        "fetch.max.bytes": 256 * 1024 * 1024,
        "queued.max.messages.kbytes": 262144,
    }


def test_consumer_config_applies_the_passthrough_last():
    """The operator's librdkafka passthrough lands AFTER millrace's own
    keys: it can add security/rack settings and override client.id
    (WarpStream's ws_az= zone pinning), verbatim strings."""
    cfg = make_config(
        kafka_consumer_overrides=(
            ("client.id", "ws_az=use1-az1,millrace-events-0"),
            ("client.rack", "use1-az1"),
            ("security.protocol", "SSL"),
        )
    )
    conf = kafka_consumer_config(cfg)
    assert conf["client.id"] == "ws_az=use1-az1,millrace-events-0"
    assert conf["client.rack"] == "use1-az1"
    assert conf["security.protocol"] == "SSL"
    # Everything millrace owns is untouched.
    assert conf["enable.auto.commit"] is False
    assert conf["group.id"] == "millrace-events"


def test_consumer_config_earliest_stays_selectable():
    conf = kafka_consumer_config(
        make_config(kafka_auto_offset_reset=AutoOffsetReset.EARLIEST)
    )
    assert conf["auto.offset.reset"] == "earliest"


def test_consumer_config_fetch_defaults_are_overridable_by_the_passthrough():
    assert consumer_mod.KAFKA_FETCH_DEFAULTS == {
        "max.partition.fetch.bytes": 32 * 1024 * 1024,
        "fetch.max.bytes": 256 * 1024 * 1024,
        "queued.max.messages.kbytes": 262144,
    }
    cfg = make_config(
        kafka_consumer_overrides=(
            ("fetch.max.bytes", 64 * 1024 * 1024),
            ("max.partition.fetch.bytes", 4 * 1024 * 1024),
            ("queued.max.messages.kbytes", 131072),
        )
    )
    conf = kafka_consumer_config(cfg)
    assert conf["max.partition.fetch.bytes"] == 4 * 1024 * 1024
    assert conf["fetch.max.bytes"] == 64 * 1024 * 1024
    assert conf["queued.max.messages.kbytes"] == 131072


def test_real_consumer_constructs_from_the_config():
    """librdkafka validates the config keys eagerly at construction; no
    broker is contacted (a construction-only smoke of the pinned dict)."""
    consumer = create_kafka_consumer(make_config())
    consumer.close()


# ==================================================================================
# Component: the loop over a scripted fake and a real StageManager
# ==================================================================================


@component
async def test_static_start_opens_the_home_stage_before_assign(fake_clock):
    """The home stage is the slot of the smallest claimed partition; it
    is opened before any partition is assigned."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    original_claim = manager.claim

    async def spied_claim(topic, home, slots):
        journal.append(("claim", home, tuple(slots)))
        return await original_claim(topic, home, slots)

    manager.claim = spied_claim  # type: ignore[method-assign]
    fake = FakeConsumer(journal=journal, partitions=3)
    consumer = make_consumer(
        fake, manager, make_config(replica_count=2, ordinal=0), fake_clock
    )

    await consumer.start()
    assert manager.partitions() == (("events", 0),)
    assert manager.home_stage().partition == 0
    assert (
        "assign",
        [("events", 0, OFFSET_STORED), ("events", 2, OFFSET_STORED)],
    ) in journal
    kinds = [e[0] for e in journal]
    assert kinds.index("claim") < kinds.index("assign")
    await manager.close()


@component
async def test_static_start_opens_existing_slots_of_this_ordinal_for_draining(
    fake_clock,
):
    """Every existing slot ``j`` with ``j % R == ordinal`` is opened (to
    be drained) beside the home; other ordinals' slots are never
    touched. Scale-down case: R=2, ordinal 1, slots left by R=8."""
    journal: list[tuple] = []
    on_disk = {0, 1, 3, 5, 6, 7}
    probed: list[int] = []

    async def prober(topic: str, slots: list[int]) -> list[int]:
        probed.extend(slots)
        return [s for s in slots if s in on_disk]

    manager = make_manager(journal, slot_prober=prober)
    fake = FakeConsumer(journal=journal, partitions=4)
    consumer = make_consumer(
        fake, manager, make_config(replica_count=2, ordinal=1), fake_clock
    )

    await consumer.start()
    assert manager.partitions() == (
        ("events", 1),
        ("events", 3),
        ("events", 5),
        ("events", 7),
    )
    assert manager.home_stage().partition == 1
    assert [p for t, p in manager.partitions() if manager.is_foreign(t, p)] == [3, 5, 7]
    # Only this ordinal's slots are probed: 1, 3, 5, … below the limit.
    assert probed == list(range(1, consumer_mod.STAGE_SLOT_SCAN_LIMIT, 2))
    await manager.close()


@component
async def test_a_slot_probe_failure_fails_the_start(fake_clock):
    """A slot holding committed rows must never be skipped as absent."""

    async def broken(topic: str, slots: list[int]) -> list[int]:
        raise OSError("GetObject: access denied")

    manager = make_manager([], slot_prober=broken)
    consumer = make_consumer(FakeConsumer(), manager, make_config(), fake_clock)
    with pytest.raises(OSError, match="access denied"):
        await consumer.start()
    assert manager.partitions() == ()
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
async def test_start_drains_previously_staged_rows_before_assign_and_consume(
    fake_clock,
):
    """The startup drain: the stages are opened, the caller's drain runs
    over what they hold, and only then is the partition assigned and
    consumed."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    # A previous process staged rows and went away without flushing.
    previous = await manager.open_partition("events", 0)
    await previous.stage_batch([rec(1, 100, 0), rec(1, 200, 1)], now_us=NOW)
    await manager.close_partition("events", 0)

    fake = FakeConsumer(journal=journal)
    fake.feed(team_msg(0, 2, team=1))
    consumer = make_consumer(fake, manager, make_config(), fake_clock)
    seen: list[Any] = []

    async def drain() -> None:
        journal.append(("drain",))
        stage = manager.stage("events", 0)
        # Opened before the drain runs, with the previous rows on disk
        # (the drain's row stream reads them; the index starts empty).
        seen.append([s.group for s in stage.snapshot()])
        seen.append(await team_offsets(stage, 1))
        assert not any(e[0] in ("assign", "consume") for e in journal)

    await consumer.start(drain=drain)
    assert seen == [[], [0, 1]]
    kinds = [e[0] for e in journal]
    assert kinds.index("drain") < kinds.index("assign")
    assert consumer.started
    await run_steps(consumer, fake)
    assert fake.committed_store == {("events", 0): 3}
    await manager.close()


@component
async def test_a_failed_drain_never_assigns_and_the_start_sticks(fake_clock):
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    consumer = make_consumer(fake, manager, make_config(), fake_clock)

    async def drain() -> None:
        raise FlushHalted("table_recreated", "scripted")

    with pytest.raises(FlushHalted):
        await consumer.start(drain=drain)
    assert not any(e[0] == "assign" for e in journal)
    assert not consumer.started
    # Sticky: a second start re-raises without claiming or assigning.
    with pytest.raises(FlushHalted):
        await consumer.start()
    assert not any(e[0] == "assign" for e in journal)
    await manager.close()


@component
async def test_a_stop_during_the_drain_never_assigns_or_consumes(fake_clock):
    """SIGTERM during the startup drain: start() returns without
    assigning, and run() closes without consuming anything."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    fake.feed(team_msg(0, 0, team=1))
    consumer = make_consumer(fake, manager, make_config(), fake_clock)

    async def drain() -> None:
        consumer.stop()  # the signal handler, mid-drain

    await consumer.start(drain=drain)
    assert not consumer.started
    assert not any(e[0] == "assign" for e in journal)
    await asyncio.wait_for(consumer.run(), timeout=10)
    assert fake.consume_calls == 0
    assert fake.closed
    assert not any(e[0] == "assign" for e in journal)
    await manager.close()


@component
async def test_end_to_end_staged_rows_and_committed_offsets_cover(fake_clock):
    """Produced rows are all staged (byte-exact, key-ordered) and the
    committed offsets exactly cover the produced ranges."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal, partitions=2)
    cfg = make_config(consume_batch_size=7)
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

    # Every produced row is staged in the ONE home stage, byte-exact, in
    # (group, partition, offset) key order; the stats the planner reads
    # reconcile exactly; the groups' coordinates together are exactly
    # the produced ranges.
    assert manager.partitions() == (("events", 0),)
    stage = manager.home_stage()
    seen: list[tuple[int, int]] = []
    for ks in stage.snapshot():
        rows = await group_rows(stage, ks.group)
        coords = [(rk.partition, rk.offset) for rk, _ in rows]
        assert coords == sorted(coords)
        for rk, value in rows:
            assert value == produced[(rk.partition, rk.offset)]
            assert rk.group == ks.group
        assert ks.rows == len(rows)
        assert ks.staged_bytes == sum(len(v) for _, v in rows)
        seen.extend(coords)
    assert sorted(seen) == sorted(produced)
    # teams 1, 2, 3 from partition 0 and team 7 from partition 1
    assert len(stage.snapshot()) == 4

    gauges = manager.gauges()
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
    from slatedb.uniffi import KeyRange

    it = await stage._db.scan(
        KeyRange(start=None, start_inclusive=False, end=None, end_inclusive=False)
    )
    out: set[bytes] = set()
    while chunk := await it.next_batch(1024):
        out.update(kv.key.split(b"/", 1)[0] + b"/" for kv in chunk)
    return out


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
        team_msg(0, 2, team=3, tag="no-ts", ts_ms=None),  # no timestamp: staged
        msg(0, 3, key=b"3", value=None),  # tombstone
        team_msg(0, 4, team=3, tag="good-4"),
    )
    with caplog.at_level(logging.WARNING, logger="millrace.consumer"):
        await run_steps(consumer, fake)

    assert_commits_follow_acks(journal, dropped={("events", 0, o) for o in (1, 3)})
    # The commit covers the dropped records' offsets.
    assert fake.committed_store == {("events", 0): 5}
    stats = consumer.stats()
    assert stats.records_staged == 3
    assert stats.records_dropped == {
        REASON_MALFORMED_VALUE: 1,
        REASON_MISSING_VALUE: 1,
    }

    stage = manager.stage("events", 0)
    assert await team_offsets(stage, 3) == [0, 2, 4]
    assert await stage_key_prefixes(stage) == {b"rows/"}
    lines = [r.getMessage() for r in caplog.records if "record dropped" in r.message]
    assert [line.split(" (")[0] for line in lines] == [
        "record dropped: topic=events partition=0 offset=1 reason=malformed_value",
        "record dropped: topic=events partition=0 offset=3 reason=missing_value",
    ]
    await manager.close()


def _journal_probes(manager: StageManager, journal: list[tuple]) -> None:
    """Spy every open stage's fence probe into the journal, after its
    durable write returns."""
    for key in manager.partitions():
        stage = manager.stage(*key)

        def make_spy(key, original):
            async def spy(offset):
                await original(offset)
                journal.append(("probe", key, offset))

            return spy

        stage.probe_fence = make_spy(key, stage.probe_fence)  # type: ignore[method-assign]


@component
async def test_a_poll_of_only_dropped_records_still_commits(fake_clock):
    """Nothing to stage is still handled: the offsets advance past a
    poll whose every record was dropped — after ONE durable fence probe
    on the home stage (no stage_batch, no row, no stats), so a fenced
    owner would have halted first. A poll that staged any row is not
    probed: its batch is the durable write."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal, partitions=2)
    consumer = make_consumer(fake, manager, make_config(), fake_clock)
    await consumer.start()
    _journal_probes(manager, journal)

    fake.feed(
        msg(0, 0, key=None, value=b"x"),
        msg(0, 1, key=None, value=None),
        msg(1, 0, key=None, value=b"x"),
    )
    await consumer._step()

    kinds = [e[0] for e in journal if e[0] in ("ack", "probe", "commit")]
    assert kinds == ["probe", "commit"]
    assert [e for e in journal if e[0] == "probe"] == [("probe", ("events", 0), 1)]
    assert fake.committed_store == {("events", 0): 2, ("events", 1): 1}
    stage = manager.home_stage()
    assert stage.snapshot() == []
    assert await stage_key_prefixes(stage) == {b"meta/"}

    # A mixed poll: the staged row's batch covers the drop beside it.
    fake.feed(team_msg(1, 1, team=2), msg(1, 2, key=None, value=b"x"))
    await run_steps(consumer, fake)
    assert_commits_follow_acks(
        journal,
        dropped={
            ("events", 0, 0),
            ("events", 0, 1),
            ("events", 1, 0),
            ("events", 1, 2),
        },
    )
    assert [e[0] for e in journal if e[0] == "probe"] == ["probe"]
    assert fake.committed_store == {("events", 0): 2, ("events", 1): 3}
    stats = consumer.stats()
    assert stats.records_staged == 1
    assert sum(stats.records_dropped.values()) == 4
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
    fake = FakeConsumer(journal=journal, partitions=2)
    mono = FakeMonotonic()
    consumer = make_consumer(
        fake,
        manager,
        make_config(),
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
    assert await group_offsets(stage, (None,)) == [4]
    # The staged payload is the raw value bytes — classify parsed the
    # JSON once to read the tuple and never rewrote the record.
    rows = [(rk.offset, v) for rk, v in await group_rows(stage, ("7",))]
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
    stats = {ks.group: ks for ks in stage.snapshot()}
    assert set(stats) == {("7",), ("7", JAN_MONTH)}
    assert await group_offsets(stage, ("7",)) == [0]
    assert await group_offsets(stage, ("7", JAN_MONTH)) == [1]
    # Both groups are flushable: past the slow-lane deadline the planner
    # decides the old group exactly like the new one.
    knobs = PlannerKnobs(min_flush_bytes=1)
    later = NOW + knobs.slow_lane_deadline_s * 1_000_000
    decided = {
        g
        for g, ks in stats.items()
        if decide(
            staged_bytes=ks.staged_bytes,
            first_staged_ts=ks.first_staged_ts,
            now_us=later,
            knobs=knobs,
        )
    }
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
    assert "keeping partition spec 1" in caplog.text
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


@pytest.mark.parametrize(
    ("ordinal", "replica_count", "partition_count", "expected"),
    [
        (0, 1, 3, (0, 1, 2)),
        (0, 2, 4, (0, 2)),
        (1, 2, 4, (1, 3)),
        # Uneven: the low ordinals get one more.
        (0, 2, 5, (0, 2, 4)),
        (1, 2, 5, (1, 3)),
        (2, 3, 7, (2, 5)),
        # More replicas than partitions: the high ordinals get nothing.
        (3, 4, 3, ()),
        (2, 3, 2, ()),
    ],
)
def test_compute_assignment_is_millponds_modulo_rule(
    ordinal, replica_count, partition_count, expected
):
    """``p % replica_count == ordinal`` over ``range(partition_count)``
    — millpond's rule verbatim, so a millpond deployment's
    ordinal→partition map carries over."""
    assert compute_assignment(partition_count, replica_count, ordinal) == expected


def test_compute_assignment_partitions_the_topic_exactly_once():
    """Every partition belongs to exactly one ordinal, whatever the
    counts (the union over ordinals is the topic, disjointly)."""
    for partition_count in range(1, 12):
        for replica_count in range(1, 6):
            sets = [
                compute_assignment(partition_count, replica_count, o)
                for o in range(replica_count)
            ]
            flat = sorted(p for s in sets for p in s)
            assert flat == list(range(partition_count)), (
                partition_count,
                replica_count,
            )


@component
@pytest.mark.parametrize(
    ("ordinal", "replica_count", "partition_count", "expected"),
    [
        (0, 1, 2, (0, 1)),
        (1, 2, 4, (1, 3)),
        (0, 2, 5, (0, 2, 4)),
        (2, 3, 7, (2, 5)),
    ],
)
async def test_derived_assignment_claims_exactly_the_expected_partitions(
    fake_clock, ordinal, replica_count, partition_count, expected
):
    """start() reads the topic's partition count from the metadata
    ONCE, claims this ordinal's share and nothing else — the
    OFFSET_STORED assign and the warmup probe name the same set, the
    home stage is the smallest of it — and never re-reads the metadata
    on later polls."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal, partitions=partition_count)
    cfg = make_config(replica_count=replica_count, ordinal=ordinal)
    consumer = make_consumer(fake, manager, cfg, fake_clock)

    await consumer.start()
    claimed = tuple(("events", p) for p in expected)
    assert manager.partitions() == (("events", expected[0]),)
    assert ("list_topics", "events", 30.0) in journal
    assert ("assign", [("events", p, OFFSET_STORED) for p in expected]) in journal
    assert ("committed", list(claimed)) in journal
    kinds = [e[0] for e in journal]
    assert kinds.index("list_topics") < kinds.index("committed") < kinds.index("assign")

    # A record on every claimed partition stages and commits; the
    # metadata is not consulted again.
    fake.feed(*[team_msg(p, 0, team=1 + p) for p in expected])
    await run_steps(consumer, fake)
    assert fake.committed_store == {("events", p): 1 for p in expected}
    assert [e for e in journal if e[0] == "list_topics"] == [
        ("list_topics", "events", 30.0)
    ]
    await manager.close()


@component
async def test_an_unknown_topic_refuses_startup_with_nothing_claimed(fake_clock):
    """The metadata answers UNKNOWN_TOPIC_OR_PART for the configured
    topic: start() raises AssignmentError carrying the broker's error
    before any stage opens or anything is assigned (main.py: exit 2,
    one line). Not retried — a missing topic does not come back."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal, topics={"other": 4})
    consumer = make_consumer(fake, manager, make_config(), fake_clock)

    with pytest.raises(AssignmentError, match="'events'.*UNKNOWN_TOPIC_OR_PART"):
        await consumer.start()
    assert manager.partitions() == ()
    assert [e[0] for e in journal] == ["list_topics"]
    await manager.close()


@component
async def test_a_topic_absent_from_the_metadata_refuses_startup(fake_clock):
    """The other shape of "unknown": the answer simply omits the topic."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal, topics={}, strict_metadata=True)
    consumer = make_consumer(fake, manager, make_config(), fake_clock)

    with pytest.raises(AssignmentError, match="unknown to the cluster"):
        await consumer.start()
    assert manager.partitions() == ()
    assert [e[0] for e in journal] == ["list_topics"]
    await manager.close()


@component
async def test_a_topic_with_no_partitions_refuses_startup(fake_clock):
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal, partitions=0)
    consumer = make_consumer(fake, manager, make_config(), fake_clock)

    with pytest.raises(AssignmentError, match="reports no partitions"):
        await consumer.start()
    assert manager.partitions() == ()
    assert [e[0] for e in journal] == ["list_topics"]
    await manager.close()


@component
@pytest.mark.parametrize("topic_level", [False, True])
async def test_partition_discovery_rides_out_a_transient_metadata_failure(
    fake_clock, topic_level
):
    """A transport error (raised KafkaException) or a fresh broker's
    LEADER_NOT_AVAILABLE on the topic clears on the second probe: the
    start succeeds with exactly two metadata reads, one sleep between
    them, and the derived assignment unchanged."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal, partitions=2)
    if topic_level:
        fake.fail_next_list_topics(1, KafkaError.LEADER_NOT_AVAILABLE, topic_level=True)
    else:
        fake.fail_next_list_topics(1)
    sleeper = Sleeper()
    consumer = make_consumer(fake, manager, make_config(), fake_clock, sleeper)

    await consumer.start()
    assert [e for e in journal if e[0] == "list_topics"] == [
        ("list_topics", "events", 30.0),
        ("list_topics", "events", 30.0),
    ]
    assert sleeper.calls == [consumer_mod._METADATA_RETRY_S]
    assert manager.partitions() == (("events", 0),)
    assert ("assign", [("events", 0, OFFSET_STORED), ("events", 1, OFFSET_STORED)]) in (
        journal
    )
    await manager.close()


@component
async def test_persistent_metadata_failure_refuses_startup_and_sticks(fake_clock):
    """Every probe fails: start() raises the last KafkaException with
    nothing claimed and nothing assigned — and the failure is sticky:
    run() raises it again instead of polling an unassigned consumer."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal, partitions=2)
    fake.fail_next_list_topics(consumer_mod._METADATA_MAX_ATTEMPTS)
    sleeper = Sleeper()
    consumer = make_consumer(fake, manager, make_config(), fake_clock, sleeper)

    with pytest.raises(KafkaException) as first:
        await consumer.start()
    assert len([e for e in journal if e[0] == "list_topics"]) == (
        consumer_mod._METADATA_MAX_ATTEMPTS
    )
    assert len(sleeper.calls) == consumer_mod._METADATA_MAX_ATTEMPTS - 1
    assert manager.partitions() == ()
    assert not any(e[0] == "assign" for e in journal)

    with pytest.raises(KafkaException) as again:
        await consumer.run()
    assert again.value is first.value
    assert fake.consume_calls == 0
    await manager.close()


@component
async def test_an_ordinal_with_no_partitions_refuses_startup(fake_clock):
    """Four replicas over a three-partition topic: ordinal 3's share is
    empty, and an idle replica that claims nothing is a topology
    mistake, not a pipeline — refused, naming every number."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal, partitions=3)
    cfg = make_config(replica_count=4, ordinal=3)
    consumer = make_consumer(fake, manager, cfg, fake_clock)

    with pytest.raises(AssignmentError) as excinfo:
        await consumer.start()
    text = str(excinfo.value)
    assert "ordinal 3" in text and "3 partition" in text
    assert "MILLRACE_REPLICA_COUNT=4" in text
    assert manager.partitions() == ()
    assert not any(e[0] == "assign" for e in journal)
    # Not a transient: refused on the first probe, no retry.
    assert len([e for e in journal if e[0] == "list_topics"]) == 1
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
    the home stage's path (an EKS rollout double-assigning the ordinal —
    the open itself succeeds; the fencing is lazy, pinned by
    tests/test_slatedb_parity.py) makes this pod's next staged write
    raise ``Error.Closed(FENCED)``. The loop catches it, closes the
    fenced stage locally, and raises the DISTINCT FencedError —
    the anti-flap semantics (a crash-loop restart would re-open the
    path and fence the NEW owner back)."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal, partitions=2)
    consumer = make_consumer(fake, manager, make_config(), fake_clock)
    await consumer.start()
    fake.feed(team_msg(0, 0, team=1), team_msg(1, 0, team=2))
    await run_steps(consumer, fake)
    assert fake.committed_store == {("events", 0): 1, ("events", 1): 1}

    # The rollout's replacement pod opens the same path (SlateDB lets
    # the open through; the OLD writer — us — is fenced from here).
    new_owner = await DbBuilder(manager.path_for("events", 0), manager.store).build()
    try:
        fake.feed(team_msg(0, 1, team=1), team_msg(1, 1, team=2))
        with pytest.raises(FencedError) as excinfo:
            await consumer._step()
        assert excinfo.value.partitions == (("events", 0),)

        # The fenced stage is closed locally — the manager no longer
        # holds it, so this process cannot touch the contested path
        # again (the anti-flap invariant).
        assert manager.partitions() == ()
        # NOTHING committed past the fenced batch, on either partition.
        assert fake.committed_store == {("events", 0): 1, ("events", 1): 1}
        assert consumer.committed_offsets() == {
            ("events", 0): 1,
            ("events", 1): 1,
        }
    finally:
        await new_owner.shutdown()
    await manager.close()


@component
async def test_an_all_drops_poll_on_a_fenced_stage_halts_without_committing(
    fake_clock,
):
    """Under static assignment a fenced old owner whose stream is 100 %
    drops has no staging write to fail — the fence probe is that write:
    the poll raises FencedError and commits NOTHING against the
    new owner, and the fenced stage is closed locally."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    consumer = make_consumer(fake, manager, make_config(), fake_clock)
    await consumer.start()
    fake.feed(team_msg(0, 0, team=1))
    await run_steps(consumer, fake)
    assert fake.committed_store == {("events", 0): 1}

    new_owner = await DbBuilder(manager.path_for("events", 0), manager.store).build()
    try:
        fake.feed(msg(0, 1, key=None, value=b"x"), msg(0, 2, key=None, value=None))
        with pytest.raises(FencedError) as excinfo:
            await consumer._step()
        assert excinfo.value.partitions == (("events", 0),)
        assert manager.partitions() == ()
        assert fake.committed_store == {("events", 0): 1}
        assert consumer.committed_offsets() == {("events", 0): 1}
        assert len([e for e in journal if e[0] == "commit"]) == 1
    finally:
        await new_owner.shutdown()
    await manager.close()


@component
async def test_a_non_fenced_closed_stage_error_is_not_the_fenced_path(fake_clock):
    """Only FENCED takes the distinct halt: a ``Closed`` with any other
    reason (a local shutdown racing an in-flight call) propagates as
    the generic error — no local relinquish, no FencedError.
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
async def test_one_poll_is_one_durable_write_for_every_partition(fake_clock):
    """One poll stages every partition's records in ONE home-stage
    batch (one WAL write and durability wait per poll, not one per
    partition), and the single commit follows that ack."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal, partitions=2)
    consumer = make_consumer(fake, manager, make_config(), fake_clock)
    await consumer.start()

    fake.feed(team_msg(0, 0, team=1), team_msg(1, 0, team=2), team_msg(1, 1, team=1))
    await consumer._step()

    acks = [e[1] for e in journal if e[0] == "ack"]
    assert len(acks) == 1
    assert acks[0].rows == 3
    assert dict(acks[0].last_offsets) == {0: 0, 1: 1}
    assert fake.committed_store == {("events", 0): 1, ("events", 1): 2}
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
    # A stopped consumer is never assigned.
    assert not any(entry[0] == "assign" for entry in journal)
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
        commit_attempts=1,
        commit_retries=0,
    )
    await manager.close()
