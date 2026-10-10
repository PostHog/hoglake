"""Consume-loop tests (docs/kafka-ingestion-plan.md Phase 3).

Layers: the PURE decisions (team-id extraction — key codecs and the
``value-json:<field>`` payload codec —, record classification, the
backpressure latch, the client-config pins) are unit tests without I/O;
the loop itself is driven component-style — a scripted FakeConsumer
(tests/kafkakit.py, hedgerow's fakes.py shape) over a REAL
StageManager on ``memory:///``, so poll → stage → ack → commit ordering,
assignment open/recover/revoke, quarantine and shutdown are all pinned
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
- the ``value-json`` codec: a topic NOT keyed by team stages under the
  payload's team field, keyless records and all; a payload lacking or
  mistyping the field quarantines;
- a fenced partition (a REAL contested open on the stage's path — the
  parity suite's FENCED shape) closes locally and halts the loop with
  the distinct ConsumerFencedError, without committing the failed batch;
- poison records quarantine durably (readable after reopen), are
  counted, and never block the partition's commits; a poison FLOOD
  halts loudly without committing past it;
- backpressure is two-level: a partition whose staged bytes age past
  the pause age pauses ALONE (the others keep flowing), while the
  aggregate staged bytes crossing the pod ceiling pauses everything —
  hysteresis on both levels, exactly-once transitions, no flapping
  (hypothesis over gauge sequences), partitions assigned while the pod
  ceiling is engaged start paused;
- commit retry ladder and its exhaustion; a failed stage commits
  nothing;
- graceful shutdown: polling stops, the Kafka side closes, committed
  offsets equal what was durably staged, and the caller-owned
  StageManager stays open.
"""

from __future__ import annotations

import asyncio
from itertools import pairwise
from typing import Any

import pytest
from confluent_kafka import OFFSET_STORED, KafkaError, KafkaException
from hypothesis import given
from hypothesis import strategies as st
from kafkakit import FakeConsumer, error_event, keyed_msg, msg
from slatedb.uniffi import CloseReason, DbBuilder
from slatedb.uniffi import Error as SlateError
from stagekit import NOW, fast_flush_settings, rec, window

import millrace.consumer as consumer_mod
from millrace.config import (
    AssignmentMode,
    AutoOffsetReset,
    BackpressureConfig,
    Config,
    EventTimePolicy,
    PoisonConfig,
    TeamKeyCodec,
)
from millrace.consumer import (
    REASON_INVALID_OFFSET,
    REASON_INVALID_TIMESTAMP,
    REASON_MALFORMED_KEY,
    REASON_MALFORMED_TEAM_FIELD,
    REASON_MALFORMED_VALUE,
    REASON_MISSING_KEY,
    REASON_MISSING_TEAM_FIELD,
    REASON_MISSING_TIMESTAMP,
    REASON_MISSING_VALUE,
    ConsumerFencedError,
    ConsumerStats,
    MillraceConsumer,
    PoisonLimitExceeded,
    backpressure_target,
    classify_message,
    create_kafka_consumer,
    decode_team_key,
    decode_team_value,
    kafka_consumer_config,
)
from millrace.keyspace import PreparedRequest
from millrace.stage import (
    PartitionStage,
    PoisonedRecord,
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
        "team_key_codec": TeamKeyCodec.UTF8_DECIMAL,
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
        "backpressure": BackpressureConfig(
            pause_staged_bytes=10**9,
            resume_staged_bytes=5 * 10**8,
            pause_oldest_age_s=3600,
            resume_oldest_age_s=1800,
        ),
        "poison": PoisonConfig(max_records_per_run=1000, value_max_bytes=64),
        "consume_batch_size": 100,
        "poll_timeout_ms": 50,
        "event_time_policy": EventTimePolicy.QUARANTINE,
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


def make_consumer(
    fake: FakeConsumer,
    manager: StageManager,
    cfg: Config,
    clock,
    sleeper: Sleeper | None = None,
) -> MillraceConsumer:
    return MillraceConsumer(
        fake,
        manager,
        cfg,
        now_us=clock,
        sleep=sleeper if sleeper is not None else Sleeper(),
    )


async def run_steps(
    consumer: MillraceConsumer, fake: FakeConsumer, extra: int = 1
) -> None:
    """Drive the loop until the fake has nothing deliverable and no
    scripted rebalance pending, plus ``extra`` idle steps (the idle
    steps matter: backpressure re-evaluates on every step, and a resume
    during an idle step can make held messages deliverable again — so
    the drain condition is re-checked after them)."""
    while True:
        while fake.deliverable() or fake.has_pending_rebalance():
            await consumer._step()
        for _ in range(extra):
            await consumer._step()
        if not (fake.deliverable() or fake.has_pending_rebalance()):
            return


def assert_commits_follow_acks(journal: list[tuple]) -> None:
    """The rows-then-offset invariant over the shared journal: every
    commit is preceded by a durable ack covering it (same partition,
    acked last_offset >= committed offset - 1), and per-partition
    commits are strictly increasing."""
    acks: list = []
    last_commit: dict[tuple[str, int], int] = {}
    for entry in journal:
        if entry[0] == "ack":
            acks.append(entry[1])
        elif entry[0] == "commit":
            assert acks, "commit observed before any durable ack"
            for topic, partition, offset in entry[1]:
                covering = [
                    a for a in acks if a.topic == topic and a.partition == partition
                ]
                assert covering, f"commit {topic}[{partition}]@{offset} has no ack"
                assert covering[-1].last_offset >= offset - 1, (
                    f"commit {topic}[{partition}]@{offset} precedes durable "
                    f"staging (last acked offset {covering[-1].last_offset})"
                )
                prev = last_commit.get((topic, partition))
                assert prev is None or offset > prev, "commits must be monotone"
                last_commit[(topic, partition)] = offset


async def settle_ranges(
    manager: StageManager, partitions: tuple[int, ...], teams: tuple[int, ...]
) -> None:
    """The flusher's settlement, test-driven: delete exactly the
    staged ranges of the given teams (one atomic settle per range)."""
    for partition in partitions:
        stage = manager.stage("events", partition)
        for team in teams:
            for r in await stage.read_offsets(team):
                await stage.commit_flushed(window(team, r.first_offset, r.last_offset))


async def team_offsets(stage: PartitionStage, team: int) -> list[int]:
    return [rk.offset async for rk, _ in stage.scan_team_rows(team)]


# ==================================================================================
# Pure: the team key codec
# ==================================================================================


def test_decode_team_key_utf8_decimal():
    assert decode_team_key(TeamKeyCodec.UTF8_DECIMAL, b"12345") == 12345
    assert decode_team_key(TeamKeyCodec.UTF8_DECIMAL, b"0") == 0
    assert decode_team_key(TeamKeyCodec.UTF8_DECIMAL, b"007") == 7


@pytest.mark.parametrize(
    "raw", [b"", b" 12", b"+12", b"1_000", b"12.5", b"abc", b"\xff\xfe"]
)
def test_decode_team_key_utf8_decimal_refusals(raw):
    with pytest.raises(ValueError, match="utf8-decimal"):
        decode_team_key(TeamKeyCodec.UTF8_DECIMAL, raw)


def test_decode_team_key_utf8_decimal_u64_ceiling():
    assert (
        decode_team_key(TeamKeyCodec.UTF8_DECIMAL, str(2**64 - 1).encode()) == 2**64 - 1
    )
    with pytest.raises(ValueError, match="u64"):
        decode_team_key(TeamKeyCodec.UTF8_DECIMAL, str(2**64).encode())


def test_decode_team_key_be64():
    assert decode_team_key(TeamKeyCodec.BE64, (12345).to_bytes(8, "big")) == 12345
    assert decode_team_key(TeamKeyCodec.BE64, b"\xff" * 8) == 2**64 - 1
    with pytest.raises(ValueError, match="8 bytes"):
        decode_team_key(TeamKeyCodec.BE64, b"\x00\x00\x00\x01")


def test_value_json_has_no_key_decoding():
    with pytest.raises(ValueError, match="unsupported"):
        decode_team_key(TeamKeyCodec.VALUE_JSON, b"7")


# ==================================================================================
# Pure: the value-json payload codec (B4 — topics not keyed by team)
# ==================================================================================


def test_decode_team_value_reads_the_configured_field():
    assert decode_team_value(b'{"team_id": 42, "event": "x"}', "team_id") == 42
    assert decode_team_value(b'{"team_id": 0}', "team_id") == 0
    assert decode_team_value(b'{"team_id": %d}' % (2**64 - 1), "team_id") == 2**64 - 1
    # A differently-named field is the operator's choice.
    assert decode_team_value(b'{"tenant": 7}', "tenant") == 7


@pytest.mark.parametrize(
    "payload,reason",
    [
        (b"not json", REASON_MALFORMED_VALUE),
        (b"\xff\xfe{}", REASON_MALFORMED_VALUE),
        (b"[1, 2]", REASON_MALFORMED_VALUE),  # JSON, but not an object
        (b"42", REASON_MALFORMED_VALUE),
        (b'"team_id"', REASON_MALFORMED_VALUE),
        (b'{"other": 1}', REASON_MISSING_TEAM_FIELD),  # field absent
        (b'{"team_id": null}', REASON_MISSING_TEAM_FIELD),  # null is absent
        (b'{"team_id": "42"}', REASON_MALFORMED_TEAM_FIELD),  # never coerced
        (b'{"team_id": 42.0}', REASON_MALFORMED_TEAM_FIELD),  # float, not int
        (b'{"team_id": true}', REASON_MALFORMED_TEAM_FIELD),  # bool IS int
        (b'{"team_id": -1}', REASON_MALFORMED_TEAM_FIELD),  # negative
        (b'{"team_id": %d}' % 2**64, REASON_MALFORMED_TEAM_FIELD),  # > u64
    ],
)
def test_decode_team_value_refusals_carry_the_reason(payload, reason):
    with pytest.raises(ValueError) as excinfo:
        decode_team_value(payload, "team_id")
    # The bounded quarantine reason rides the exception (the poison
    # envelope stores it); the message detail is for the logs.
    assert excinfo.value.reason == reason  # type: ignore[attr-defined]


def test_classify_value_json_ignores_the_key_entirely():
    # The topic is not keyed by team: a KEYLESS record stages fine (a
    # keyed codec would quarantine it as missing_key), and even a junk
    # key is never consulted.
    out = classify_message(
        TeamKeyCodec.VALUE_JSON,
        offset=41,
        key=None,
        value=b'{"team_id": 7, "v": 1}',
        timestamp=(1, 1_234_567),
        poison_value_max_bytes=64,
        value_field="team_id",
    )
    assert out == StagedRecord(
        team_id=7,
        event_ts_us=1_234_567_000,
        offset=41,
        payload=b'{"team_id": 7, "v": 1}',
    )
    out = classify_message(
        TeamKeyCodec.VALUE_JSON,
        offset=42,
        key=b"junk-not-a-team",
        value=b'{"team_id": 9}',
        timestamp=(1, 1_234_567),
        poison_value_max_bytes=64,
        value_field="team_id",
    )
    assert isinstance(out, StagedRecord) and out.team_id == 9


def test_classify_value_json_poisons_a_payload_without_the_field():
    out = classify_message(
        TeamKeyCodec.VALUE_JSON,
        offset=41,
        key=None,
        value=b'{"not_team": 7}',
        timestamp=(1, 1_234_567),
        poison_value_max_bytes=64,
        value_field="team_id",
    )
    assert isinstance(out, PoisonedRecord)
    assert out.reason == REASON_MISSING_TEAM_FIELD
    out = classify_message(
        TeamKeyCodec.VALUE_JSON,
        offset=41,
        key=None,
        value=b'{"team_id": "7"}',
        timestamp=(1, 1_234_567),
        poison_value_max_bytes=64,
        value_field="team_id",
    )
    assert isinstance(out, PoisonedRecord)
    assert out.reason == REASON_MALFORMED_TEAM_FIELD
    out = classify_message(
        TeamKeyCodec.VALUE_JSON,
        offset=41,
        key=None,
        value=b"not json",
        timestamp=(1, 1_234_567),
        poison_value_max_bytes=64,
        value_field="team_id",
    )
    assert isinstance(out, PoisonedRecord)
    assert out.reason == REASON_MALFORMED_VALUE


def test_classify_value_json_tombstone_is_missing_value_not_a_parse_error():
    # The FIRST failing check names the reason: a tombstone is
    # missing_value, exactly like under the keyed codecs.
    out = classify_message(
        TeamKeyCodec.VALUE_JSON,
        offset=41,
        key=None,
        value=None,
        timestamp=(1, 1_234_567),
        poison_value_max_bytes=64,
        value_field="team_id",
    )
    assert isinstance(out, PoisonedRecord) and out.reason == REASON_MISSING_VALUE


def test_classify_value_json_without_a_field_is_a_config_bug_not_poison():
    # Config refuses the pair at construction; a hand-built path must
    # not silently quarantine every record.
    with pytest.raises(ValueError, match="value_field"):
        classify_message(
            TeamKeyCodec.VALUE_JSON,
            offset=41,
            key=None,
            value=b'{"team_id": 7}',
            timestamp=(1, 1_234_567),
            poison_value_max_bytes=64,
        )


# ==================================================================================
# Pure: record classification (total — never raises)
# ==================================================================================


def classify(
    codec, *, offset=41, key=b"7", value=b"payload", timestamp=(1, 1_234_567), cap=64
):
    return classify_message(
        codec,
        offset=offset,
        key=key,
        value=value,
        timestamp=timestamp,
        poison_value_max_bytes=cap,
    )


def test_classify_a_wellformed_record():
    assert classify(TeamKeyCodec.UTF8_DECIMAL) == StagedRecord(
        team_id=7, event_ts_us=1_234_567_000, offset=41, payload=b"payload"
    )


def test_classify_poison_reasons():
    codec = TeamKeyCodec.UTF8_DECIMAL
    out = classify(codec, key=None)
    assert isinstance(out, PoisonedRecord) and out.reason == REASON_MISSING_KEY
    out = classify(codec, key=b"not-a-team")
    assert isinstance(out, PoisonedRecord) and out.reason == REASON_MALFORMED_KEY
    out = classify(codec, timestamp=(0, 0))  # TIMESTAMP_NOT_AVAILABLE
    assert isinstance(out, PoisonedRecord) and out.reason == REASON_MISSING_TIMESTAMP
    out = classify(codec, value=None)
    assert isinstance(out, PoisonedRecord) and out.reason == REASON_MISSING_VALUE
    assert out.value is None and out.value_bytes_original == 0


def test_classify_out_of_range_offset_and_timestamp():
    codec = TeamKeyCodec.UTF8_DECIMAL
    out = classify(codec, offset=-1)
    assert isinstance(out, PoisonedRecord) and out.reason == REASON_INVALID_OFFSET
    # *1000 overflows int64 µs
    out = classify(codec, timestamp=(1, 10**18))
    assert isinstance(out, PoisonedRecord) and out.reason == REASON_INVALID_TIMESTAMP


def test_classify_poison_keeps_the_raw_key_and_truncates_the_value():
    out = classify(TeamKeyCodec.UTF8_DECIMAL, key=b"bad-key", value=b"x" * 100, cap=10)
    assert isinstance(out, PoisonedRecord)
    assert out.key == b"bad-key"
    assert out.value == b"x" * 10
    assert out.value_bytes_original == 100


def test_classify_names_the_FIRST_failing_check():
    # A tombstone with an undecodable key quarantines as malformed_key.
    out = classify(TeamKeyCodec.UTF8_DECIMAL, key=b"bad", value=None)
    assert isinstance(out, PoisonedRecord) and out.reason == REASON_MALFORMED_KEY


# ==================================================================================
# Pure: the backpressure latch
# ==================================================================================

KNOBS = BackpressureConfig(
    pause_staged_bytes=1000,
    resume_staged_bytes=500,
    pause_oldest_age_s=100,
    resume_oldest_age_s=50,
)


def target(*, staged_bytes, oldest=None, now_us, paused, knobs=KNOBS):
    return backpressure_target(
        staged_bytes=staged_bytes,
        oldest_first_staged_ts=oldest,
        now_us=now_us,
        currently_paused=paused,
        knobs=knobs,
    )


def test_backpressure_byte_boundaries():
    # Trips AT the high water, not below it.
    assert target(staged_bytes=1000, now_us=0, paused=False) is True
    assert target(staged_bytes=999, now_us=0, paused=False) is False
    # While paused, the low water is STRICT: at it, the latch holds.
    assert target(staged_bytes=500, now_us=0, paused=True) is True
    assert target(staged_bytes=499, now_us=0, paused=True) is False
    # Inside the band the state persists in both directions.
    assert target(staged_bytes=750, now_us=0, paused=False) is False
    assert target(staged_bytes=750, now_us=0, paused=True) is True


def test_backpressure_age_boundaries():
    now = 10**12
    pause_us, resume_us = 100 * 10**6, 50 * 10**6
    # Trips AT the pause age.
    assert (
        target(staged_bytes=0, oldest=now - pause_us, now_us=now, paused=False) is True
    )
    assert (
        target(staged_bytes=0, oldest=now - pause_us + 1, now_us=now, paused=False)
        is False
    )
    # The resume age is strict: at it, the latch holds.
    assert (
        target(staged_bytes=0, oldest=now - resume_us, now_us=now, paused=True) is True
    )
    assert (
        target(staged_bytes=0, oldest=now - resume_us + 1, now_us=now, paused=True)
        is False
    )


def test_backpressure_nothing_staged_clears_the_age_arm():
    assert target(staged_bytes=10**9, oldest=None, now_us=0, paused=False) is True
    assert target(staged_bytes=0, oldest=None, now_us=0, paused=True) is False


def test_backpressure_clock_skew_trips_nothing():
    # A snapshot whose oldest timestamp is AHEAD of now (skew) has a
    # negative age and trips no deadline, by construction.
    assert target(staged_bytes=0, oldest=10**12, now_us=0, paused=False) is False
    assert target(staged_bytes=0, oldest=10**12, now_us=0, paused=True) is False


def test_backpressure_monotone_ramps_transition_once():
    knobs = KNOBS
    paused = False
    seen: list[bool] = []
    for staged_bytes in range(2001):  # ramp up through both bands
        paused = target(staged_bytes=staged_bytes, now_us=0, paused=paused, knobs=knobs)
        seen.append(paused)
    transitions = sum(a != b for a, b in pairwise(seen))
    assert transitions == 1 and seen[0] is False and seen[-1] is True
    for staged_bytes in range(2000, -1, -1):  # ramp back down
        paused = target(staged_bytes=staged_bytes, now_us=0, paused=paused, knobs=knobs)
        seen.append(paused)
    assert sum(a != b for a, b in pairwise(seen)) == 2  # one pause, one resume


@given(
    snapshots=st.lists(
        st.tuples(
            st.integers(min_value=0, max_value=10**12),
            st.one_of(st.none(), st.integers(min_value=-(2**62), max_value=2**62)),
            st.integers(min_value=-(2**62), max_value=2**62),
        ),
        max_size=200,
    ),
    initially_paused=st.booleans(),
    gap_bytes=st.integers(min_value=1, max_value=10**6),
    gap_age_s=st.integers(min_value=1, max_value=10**6),
    base_bytes=st.integers(min_value=1, max_value=10**6),
    base_age_s=st.integers(min_value=1, max_value=10**6),
)
def test_backpressure_never_flaps_over_arbitrary_gauge_sequences(
    snapshots, initially_paused, gap_bytes, gap_age_s, base_bytes, base_age_s
):
    """No-flapping as a property: over ANY gauge sequence, a transition
    is always justified by a full crossing (a pause only on a trip, a
    resume only on a clear), and the decision is idempotent on a fixed
    snapshot — so pause/resume each apply exactly once per crossing."""
    knobs = BackpressureConfig(
        pause_staged_bytes=base_bytes + gap_bytes,
        resume_staged_bytes=base_bytes,
        pause_oldest_age_s=base_age_s + gap_age_s,
        resume_oldest_age_s=base_age_s,
    )
    paused = initially_paused
    for staged_bytes, oldest, now_us in snapshots:
        target_now = backpressure_target(
            staged_bytes=staged_bytes,
            oldest_first_staged_ts=oldest,
            now_us=now_us,
            currently_paused=paused,
            knobs=knobs,
        )
        # Idempotence: a fixed snapshot has a stable answer, so applying
        # the decision twice changes nothing (no double-pause).
        assert (
            backpressure_target(
                staged_bytes=staged_bytes,
                oldest_first_staged_ts=oldest,
                now_us=now_us,
                currently_paused=target_now,
                knobs=knobs,
            )
            == target_now
        )
        if target_now != paused:
            age_us = None if oldest is None else now_us - oldest
            trip = staged_bytes >= knobs.pause_staged_bytes or (
                age_us is not None and age_us >= knobs.pause_oldest_age_s * 1_000_000
            )
            clear = staged_bytes < knobs.resume_staged_bytes and (
                age_us is None or age_us < knobs.resume_oldest_age_s * 1_000_000
            )
            if target_now:
                assert trip, "paused without either high-water trip"
            else:
                assert clear, "resumed without both low-water clears"
        paused = target_now


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
    await previous.stage.persist_prepared(
        1,
        0,
        PreparedRequest(
            idempotency_key="idem-1",
            body=b"{}",
            persisted_at=NOW,
            topic="events",
            partition=0,
        ),
    )
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
    assert [(p.key.team_id, p.key.first_offset) for p in recovery.pending_prepared] == [
        (1, 0)
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
        value = f"row-0-{offset}".encode()
        fake.feed(keyed_msg(0, offset, team=1 + offset % 3, value=value))
        produced[(0, offset)] = value
    for offset in range(10):
        value = f"row-1-{offset}".encode()
        fake.feed(keyed_msg(1, offset, team=7, value=value))
        produced[(1, offset)] = value

    await run_steps(consumer, fake)

    assert_commits_follow_acks(journal)
    assert fake.committed_store == {("events", 0): 20, ("events", 1): 10}
    assert consumer.committed_offsets() == fake.committed_store
    stats = consumer.stats()
    assert stats.messages_consumed == 30
    assert stats.records_staged == 30
    assert stats.records_poisoned == 0

    # Every produced row is staged, byte-exact, in (team, ts, offset)
    # key order; the stats the planner reads reconcile exactly; the
    # offsets/ identity ranges cover the produced range contiguously.
    total_rows = 0
    for partition, team_count in ((0, 3), (1, 1)):
        stage = manager.stage("events", partition)
        ranges = []
        for ks in await stage.iter_key_stats():
            rows = [(rk, v) async for rk, v in stage.scan_team_rows(ks.team_id)]
            assert [(rk.timestamp_us, rk.offset) for rk, _ in rows] == sorted(
                (rk.timestamp_us, rk.offset) for rk, _ in rows
            )
            for rk, value in rows:
                assert value == produced[(partition, rk.offset)]
                assert rk.team_id == ks.team_id
            assert ks.row_count == len(rows)
            assert ks.staged_bytes == sum(len(v) for _, v in rows)
            total_rows += len(rows)
            ranges.extend(await stage.read_offsets(ks.team_id))
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
        keyed_msg(0, 0, team=1, value=b"good"),
        error_event(0, 1, KafkaError._TRANSPORT),
        keyed_msg(0, 1, team=1, value=b"good2"),
    )
    await run_steps(consumer, fake)

    stats = consumer.stats()
    assert stats.kafka_error_events == 2
    assert stats.records_staged == 2
    assert fake.committed_store == {("events", 0): 2}
    stage = manager.stage("events", 0)
    assert await team_offsets(stage, 1) == [0, 1]
    assert await stage.scan_poison() == []
    await manager.close()


@component
async def test_poison_records_quarantine_durably_and_never_block(fake_clock):
    """Malformed records land in the quarantine (durable across reopen),
    are counted and logged, and the partition's commit covers them —
    they never wedge the loop."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    cfg = make_config(poison=PoisonConfig(max_records_per_run=100, value_max_bytes=8))
    consumer = make_consumer(fake, manager, cfg, fake_clock)
    await consumer.start()

    fake.feed(
        keyed_msg(0, 0, team=3, value=b"good-0"),
        msg(0, 1, key=b"not-a-team", value=b"junk" * 20),  # malformed key, truncated
        msg(0, 2, key=b"3", ts_ms=None),  # no timestamp
        msg(0, 3, key=b"3", value=None),  # tombstone
        keyed_msg(0, 4, team=3, value=b"good-4"),
    )
    await run_steps(consumer, fake)

    assert_commits_follow_acks(journal)
    # The commit covers the poison records' offsets — they are skipped
    # without blocking the partition.
    assert fake.committed_store == {("events", 0): 5}
    stats = consumer.stats()
    assert stats.records_staged == 2 and stats.records_poisoned == 3

    stage = manager.stage("events", 0)
    assert await team_offsets(stage, 3) == [0, 4]
    poisoned = await stage.scan_poison()
    assert [(p.offset, p.reason) for p in poisoned] == [
        (1, REASON_MALFORMED_KEY),
        (2, REASON_MISSING_TIMESTAMP),
        (3, REASON_MISSING_VALUE),
    ]
    assert poisoned[0].key == b"not-a-team"
    assert poisoned[0].value == b"junkjunk"  # truncated at the 8-byte cap
    assert poisoned[0].value_bytes_original == 80
    assert poisoned[2].value is None and poisoned[2].value_bytes_original == 0

    # Durable: a reopen (new owner, crash recovery) still reads them.
    await manager.close_partition("events", 0)
    reopened = await PartitionStage.open_store(
        manager.store,
        manager.path_for("events", 0),
        topic="events",
        partition=0,
        settings=fast_flush_settings(),
    )
    assert [(p.offset, p.reason) for p in await reopened.scan_poison()] == [
        (1, REASON_MALFORMED_KEY),
        (2, REASON_MISSING_TIMESTAMP),
        (3, REASON_MISSING_VALUE),
    ]
    await reopened.close()
    await manager.close()


@component
async def test_missing_key_quarantines_too(fake_clock):
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    consumer = make_consumer(fake, manager, make_config(), fake_clock)
    await consumer.start()
    fake.feed(msg(0, 0, key=None, value=b"orphan"))
    await run_steps(consumer, fake)
    stage = manager.stage("events", 0)
    assert [(p.offset, p.reason) for p in await stage.scan_poison()] == [
        (0, REASON_MISSING_KEY)
    ]
    assert fake.committed_store == {("events", 0): 1}
    await manager.close()


@component
async def test_value_json_consumes_an_unkeyed_topic(fake_clock):
    """B4: a topic NOT keyed by team — the team id rides the payload.
    Keyless records (and records with junk keys — the key is never
    consulted) stage under their payload's team field; a payload
    lacking or mistyping the configured field quarantines; the commit
    covers all of it."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    cfg = make_config(team_key_codec=TeamKeyCodec.VALUE_JSON, team_id_field="team_id")
    consumer = make_consumer(fake, manager, cfg, fake_clock)
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

    assert_commits_follow_acks(journal)
    assert fake.committed_store == {("events", 0): 7}
    stats = consumer.stats()
    assert stats.records_staged == 4 and stats.records_poisoned == 3
    stage = manager.stage("events", 0)
    assert await team_offsets(stage, 7) == [0, 2]
    assert await team_offsets(stage, 9) == [1, 3]
    assert [(p.offset, p.reason) for p in await stage.scan_poison()] == [
        (4, REASON_MISSING_TEAM_FIELD),
        (5, REASON_MALFORMED_TEAM_FIELD),
        (6, REASON_MALFORMED_VALUE),
    ]
    # The staged payload is the raw value bytes — classify parsed the
    # JSON once to read the team and never rewrote the record (the
    # flush parses with its own schema logic).
    rows = [(rk.offset, v) async for rk, v in stage.scan_team_rows(7)]
    assert rows[0] == (0, b'{"team_id": 7, "event": "a"}')
    await manager.close()


@component
async def test_poison_flood_halts_loudly_without_committing_past_it(fake_clock):
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    cfg = make_config(poison=PoisonConfig(max_records_per_run=2, value_max_bytes=64))
    consumer = make_consumer(fake, manager, cfg, fake_clock)
    await consumer.start()

    fake.feed(keyed_msg(0, 0, team=1, value=b"fine"))
    await run_steps(consumer, fake)
    assert fake.committed_store == {("events", 0): 1}

    # A flood: three bad records against a limit of two. The cycle
    # raises BEFORE anything in it is staged or committed.
    fake.feed(
        msg(0, 1, key=b"bad"),
        msg(0, 2, key=b"bad"),
        msg(0, 3, key=b"bad"),
    )
    with pytest.raises(PoisonLimitExceeded, match="MILLRACE_POISON_MAX_RECORDS=2"):
        await consumer._step()
    assert fake.committed_store == {("events", 0): 1}
    assert consumer.committed_offsets() == {("events", 0): 1}
    stage = manager.stage("events", 0)
    assert await stage.scan_poison() == []
    assert await team_offsets(stage, 1) == [0]
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
    fake.feed(keyed_msg(0, 0, team=1, value=b"first"))
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

    fake.feed(*[keyed_msg(0, o, team=1, value=f"a{o}".encode()) for o in range(3)])
    fake.feed(*[keyed_msg(1, o, team=2, value=f"b{o}".encode()) for o in range(2)])
    await run_steps(consumer, fake)
    assert fake.committed_store == {("events", 0): 3, ("events", 1): 2}
    stage0 = manager.stage("events", 0)

    # Revoke 0 while two more of its records sit in the fetch buffer.
    fake.feed(*[keyed_msg(0, o, team=1, value=f"a{o}".encode()) for o in (3, 4)])
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
    fake.feed(*[keyed_msg(0, o, team=1) for o in range(3)])
    await run_steps(consumer, fake)
    assert fake.committed_store == {("events", 0): 3}

    fake.feed(*[keyed_msg(0, o, team=1) for o in (3, 4)])
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
    fake.feed(keyed_msg(0, 0, team=1))
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

    fake.feed(*[keyed_msg(0, o, team=1) for o in range(3)])
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

    fake.feed(*[keyed_msg(0, o, team=1) for o in range(3)])
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
    fake.feed(keyed_msg(0, 0, team=1))

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
    fake.feed(keyed_msg(0, 0, team=1), keyed_msg(1, 0, team=2))
    await run_steps(consumer, fake)
    assert fake.committed_store == {("events", 0): 1, ("events", 1): 1}

    # The rollout's replacement pod opens the same path (SlateDB lets
    # the open through; the OLD writer — us — is fenced from here).
    new_owner = await DbBuilder(manager.path_for("events", 0), manager.store).build()
    try:
        fake.feed(keyed_msg(0, 1, team=1), keyed_msg(1, 1, team=2))
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
async def test_a_fence_surfacing_at_the_gauge_read_halts_distinctly(fake_clock):
    """An IDLE fenced partition has no write to raise from — the fence
    surfaces at the backpressure gauge scan instead. Same semantics:
    local close of the held stages, distinct halt. (Detection is forced
    first by a write — the parity suite pins WHICH call detects a fence
    as timing-dependent, and that once detected every later operation
    raises.)"""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    consumer = make_consumer(fake, manager, make_config(), fake_clock)
    await consumer.start()
    fake.feed(keyed_msg(0, 0, team=1))
    await run_steps(consumer, fake)
    assert fake.committed_store == {("events", 0): 1}

    new_owner = await DbBuilder(manager.path_for("events", 0), manager.store).build()
    try:
        # Force the detection onto the stage's handle without involving
        # the consumer: the raw stage call raises SlateDB's Closed, and
        # the handle keeps raising from here (the parity pin).
        with pytest.raises(SlateError.Closed):
            await manager.stage("events", 0).stage_batch([rec(1, NOW, 1)], now_us=NOW)
        # The next step has nothing to stage; the live gauge scan (no
        # flush sweep is publishing in this test) hits the fence.
        with pytest.raises(ConsumerFencedError) as excinfo:
            await consumer._step()
        assert excinfo.value.partitions == (("events", 0),)
        assert manager.partitions() == ()
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
    fake.feed(keyed_msg(0, 0, team=1))

    stage = manager.stage("events", 0)

    async def clean_closed(records, *, now_us, poison=()):
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
        async def spy(records, *, now_us, poison=()):
            order.append(("start", key))
            in_flight.add(key)
            if len(in_flight) == 2:
                gate.set()
            await asyncio.wait_for(gate.wait(), timeout=5)
            ack = await original(records, now_us=now_us, poison=poison)
            order.append(("end", key))
            return ack

        return spy

    for key in (("events", 0), ("events", 1)):
        stage = manager.stage(*key)
        stage.stage_batch = make_spy(key, stage.stage_batch)  # type: ignore[method-assign]

    fake.feed(keyed_msg(0, 0, team=1), keyed_msg(1, 0, team=2))
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

    # 20 records of 101 observed bytes each (100 value + 1 key):
    # avg 101, so every later poll requests 1000 // 101 == 9.
    fake.feed(*[keyed_msg(0, o, team=1, value=b"v" * 100) for o in range(20)])
    await run_steps(consumer, fake)
    assert fake.requested_batch_sizes[0] == 1000
    assert fake.requested_batch_sizes[1:] and set(fake.requested_batch_sizes[1:]) == {9}
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

    # 8 records of 101 observed bytes: poll 1 takes 5 (the record cap),
    # then avg 101 > the 10-byte budget, so every later poll asks for
    # exactly 1.
    fake.feed(*[keyed_msg(0, o, team=1, value=b"v" * 100) for o in range(8)])
    for _ in range(8):
        await consumer._step()
    assert fake.requested_batch_sizes[:2] == [5, 1]
    assert fake.committed_store == {("events", 0): 8}
    assert_commits_follow_acks(journal)
    await manager.close()


# -- backpressure through the loop ---------------------------------------------------


@component
async def test_backpressure_pauses_and_resumes_exactly_once(fake_clock):
    """High-water pause, low-water resume, exactly once per crossing,
    with the band between holding the state (no flapping). While paused
    the fake delivers nothing for the paused partition."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    cfg = make_config(
        backpressure=BackpressureConfig(
            pause_staged_bytes=100,
            resume_staged_bytes=50,
            pause_oldest_age_s=3600,
            resume_oldest_age_s=1800,
        )
    )
    consumer = make_consumer(fake, manager, cfg, fake_clock)
    await consumer.start()
    pauses = lambda: [e for e in journal if e[0] == "pause"]
    resumes = lambda: [e for e in journal if e[0] == "resume"]

    fake.feed(keyed_msg(0, 0, team=1, value=b"x" * 60))
    await run_steps(consumer, fake)
    assert fake.committed_store == {("events", 0): 1}
    assert not consumer.paused and pauses() == []

    fake.feed(keyed_msg(0, 1, team=1, value=b"x" * 60))
    await run_steps(consumer, fake)
    # 120 staged bytes >= 100: paused, once, over the whole assignment.
    assert consumer.paused
    assert pauses() == [("pause", [("events", 0)])]
    assert fake.paused == {("events", 0)}

    # More data offered; a paused partition delivers nothing (the record
    # is held in the fetch queue), and further steps neither stage nor
    # re-pause.
    fake.feed(keyed_msg(0, 2, team=1, value=b"x" * 60))
    await run_steps(consumer, fake)
    assert fake.deliverable() == 0 and fake.queued() == 1
    assert fake.committed_store == {("events", 0): 2}
    assert pauses() == [("pause", [("events", 0)])]

    # Settle 60 bytes: 60 remains — inside the (50, 100) band, so the
    # latch HOLDS (this is the hysteresis half of no-flapping).
    stage = manager.stage("events", 0)
    await stage.commit_flushed(window(1, 0, 0))
    await run_steps(consumer, fake)
    assert consumer.paused and resumes() == []

    # Settle the rest: staged drops to 0 < 50 — resumed, exactly once.
    await stage.commit_flushed(window(1, 1, 1))
    await run_steps(consumer, fake)
    assert not consumer.paused
    assert resumes() == [("resume", [("events", 0)])]
    assert fake.paused == set()

    # And consumption continues: the held record stages and commits.
    assert fake.committed_store == {("events", 0): 3}
    assert_commits_follow_acks(journal)
    await manager.close()


@component
async def test_backpressure_age_trigger_and_clear_on_drain(fake_clock):
    """The age arm: the oldest staged byte crossing the pause age pauses
    even at tiny byte counts (the flusher is not keeping up); draining
    every key clears it."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    cfg = make_config(
        backpressure=BackpressureConfig(
            pause_staged_bytes=10**9,
            resume_staged_bytes=5 * 10**8,
            pause_oldest_age_s=100,
            resume_oldest_age_s=50,
        )
    )
    consumer = make_consumer(fake, manager, cfg, fake_clock)
    await consumer.start()

    fake.feed(keyed_msg(0, 0, team=1, value=b"tiny"))
    await run_steps(consumer, fake)
    assert not consumer.paused

    fake_clock.advance(100 * 10**6)  # oldest staged age reaches the pause water
    await consumer._step()
    assert consumer.paused

    # Drained (the flusher settled the key): nothing staged, so the age
    # arm is inert and the loop resumes.
    await settle_ranges(manager, (0,), (1,))
    await consumer._step()
    assert not consumer.paused
    await manager.close()


@component
async def test_partitions_assigned_while_paused_start_paused(fake_clock):
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    cfg = make_config(
        assignment_mode=AssignmentMode.COOPERATIVE,
        static_partitions=None,
        backpressure=BackpressureConfig(
            pause_staged_bytes=100,
            resume_staged_bytes=50,
            pause_oldest_age_s=3600,
            resume_oldest_age_s=1800,
        ),
    )
    consumer = make_consumer(fake, manager, cfg, fake_clock)
    await consumer.start()
    fake.script_rebalance(assign=[0])
    await consumer._step()

    fake.feed(keyed_msg(0, 0, team=1, value=b"x" * 120))
    await run_steps(consumer, fake)
    assert consumer.paused and fake.paused == {("events", 0)}

    fake.script_rebalance(assign=[1])
    await consumer._step()
    # The new partition inherits the pod-level pause, exactly once.
    assert ("pause", [("events", 1)]) in [e for e in journal if e[0] == "pause"]
    assert fake.paused == {("events", 0), ("events", 1)}
    assert len([e for e in journal if e[0] == "pause"]) == 2
    await manager.close()


# -- T10: two-level backpressure — per-partition age latches + pod byte ceiling -------


@component
async def test_a_stuck_partition_pauses_alone(fake_clock):
    """The age latch is PER PARTITION: a partition whose oldest staged
    byte ages past the pause age (its flusher is stuck) pauses ALONE —
    the pod's other partitions keep flowing and committing. (The
    pre-fix policy folded the oldest age into one pod-wide latch and
    stopped all 63 healthy partitions with the stuck one.)"""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    cfg = make_config(
        static_partitions=(0, 1),
        backpressure=BackpressureConfig(
            pause_staged_bytes=10**9,
            resume_staged_bytes=5 * 10**8,
            pause_oldest_age_s=100,
            resume_oldest_age_s=50,
        ),
    )
    consumer = make_consumer(fake, manager, cfg, fake_clock)
    await consumer.start()

    fake.feed(keyed_msg(0, 0, team=1, value=b"a"), keyed_msg(1, 0, team=2, value=b"b"))
    await run_steps(consumer, fake)
    assert fake.paused == set()

    # Partition 1's flusher keeps up (its ranges settle); partition 0's
    # staged byte then ages past the pause age.
    await settle_ranges(manager, (1,), (2,))
    fake_clock.advance(100 * 10**6)  # p0's oldest reaches the pause age
    await consumer._step()
    # The stuck partition pauses ALONE.
    assert fake.paused == {("events", 0)}
    assert consumer.paused
    assert consumer.paused_partitions() == frozenset({("events", 0)})

    # Partition 1 keeps flowing while 0 is latched.
    fake.feed(keyed_msg(1, 1, team=2, value=b"c"))
    await run_steps(consumer, fake)
    assert fake.committed_store[("events", 1)] == 2
    assert fake.paused == {("events", 0)}

    # Partition 0's flush unsticks: its ranges settle, the age arm goes
    # inert, exactly one resume fires.
    await settle_ranges(manager, (0,), (1,))
    await consumer._step()
    assert fake.paused == set()
    assert not consumer.paused
    assert [e for e in journal if e[0] == "resume"] == [("resume", [("events", 0)])]
    assert_commits_follow_acks(journal)
    await manager.close()


@component
async def test_per_partition_age_latch_holds_inside_the_band(fake_clock):
    """The per-partition latch keeps the hysteresis discipline: draining
    the oldest key so the NEW oldest lands inside the (resume, pause)
    band HOLDS the pause; only a full drain below the resume age
    resumes."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    cfg = make_config(
        backpressure=BackpressureConfig(
            pause_staged_bytes=10**9,
            resume_staged_bytes=5 * 10**8,
            pause_oldest_age_s=100,
            resume_oldest_age_s=50,
        )
    )
    consumer = make_consumer(fake, manager, cfg, fake_clock)
    await consumer.start()

    fake.feed(keyed_msg(0, 0, team=1, value=b"old"))
    await run_steps(consumer, fake)
    fake_clock.advance(30 * 10**6)
    fake.feed(keyed_msg(0, 1, team=3, value=b"newer"))
    await run_steps(consumer, fake)
    fake_clock.advance(70 * 10**6)  # team1 age 100 (TRIP), team3 age 70
    await consumer._step()
    assert fake.paused == {("events", 0)}

    # Drain the old key: the oldest staged byte is now team3's at 70 s —
    # inside the (50, 100) band, and a latched latch HOLDS there
    # (strictly-below-resume is the only clear).
    await settle_ranges(manager, (0,), (1,))
    await consumer._step()
    assert fake.paused == {("events", 0)}
    assert [e for e in journal if e[0] == "resume"] == []

    # Drain the rest: nothing staged, the age arm goes inert, and the
    # latch clears with exactly one resume.
    await settle_ranges(manager, (0,), (3,))
    await consumer._step()
    assert fake.paused == set()
    assert [e for e in journal if e[0] == "resume"] == [("resume", [("events", 0)])]
    await manager.close()


@component
async def test_the_pod_byte_ceiling_pauses_everything(fake_clock):
    """Level 2: the AGGREGATE staged bytes crossing the pod ceiling
    pauses EVERY assigned partition — the legitimate pod-wide limit
    (total staged volume is the catch-up working set this pod is owed)
    — even when no single partition is near the threshold on its own.
    Hysteresis is the aggregate's: partial drain into the band holds,
    drain below the resume water resumes all."""
    journal: list[tuple] = []
    manager = make_manager(journal)
    fake = FakeConsumer(journal=journal)
    cfg = make_config(
        static_partitions=(0, 1),
        backpressure=BackpressureConfig(
            pause_staged_bytes=100,
            resume_staged_bytes=50,
            pause_oldest_age_s=3600,
            resume_oldest_age_s=1800,
        ),
    )
    consumer = make_consumer(fake, manager, cfg, fake_clock)
    await consumer.start()

    # 60 bytes per partition: neither partition is near 100 alone, but
    # the 120-byte aggregate crosses the pod ceiling.
    fake.feed(
        keyed_msg(0, 0, team=1, value=b"x" * 60),
        keyed_msg(1, 0, team=2, value=b"y" * 60),
    )
    await run_steps(consumer, fake)
    assert fake.paused == {("events", 0), ("events", 1)}
    assert consumer.paused
    assert consumer.paused_partitions() == frozenset({("events", 0), ("events", 1)})
    # One pause call covering the assignment, exactly once.
    assert [e for e in journal if e[0] == "pause"] == [
        ("pause", [("events", 0), ("events", 1)])
    ]

    # Partial drain into the (50, 100) band: the pod latch HOLDS.
    await settle_ranges(manager, (1,), (2,))
    await consumer._step()
    assert fake.paused == {("events", 0), ("events", 1)}
    assert not [e for e in journal if e[0] == "resume"]

    # Drain below the resume water: one resume call for the assignment.
    await settle_ranges(manager, (0,), (1,))
    await consumer._step()
    assert fake.paused == set()
    assert [e for e in journal if e[0] == "resume"] == [
        ("resume", [("events", 0), ("events", 1)])
    ]
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
    fake.feed(*[keyed_msg(0, o, team=1, value=f"r{o}".encode()) for o in range(5)])

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
    cfg = make_config(poison=PoisonConfig(max_records_per_run=1, value_max_bytes=64))
    consumer = make_consumer(fake, manager, cfg, fake_clock)
    fake.feed(msg(0, 0, key=b"bad"), msg(0, 1, key=b"bad"))
    with pytest.raises(PoisonLimitExceeded):
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
        keyed_msg(0, 0, team=1),
        msg(0, 1, key=b"bad"),
        error_event(0, 2, KafkaError._PARTITION_EOF),
        keyed_msg(0, 2, team=1),
    )
    await run_steps(consumer, fake)
    assert consumer.stats() == ConsumerStats(
        messages_consumed=4,
        records_staged=2,
        records_poisoned=1,
        kafka_error_events=1,
        messages_skipped_revoked=0,
        commit_attempts=1,
        commit_retries=0,
    )
    await manager.close()


# -- M6(b): backpressure reads the sweep's shared gauge snapshot --------------------


class _Mono:
    def __init__(self) -> None:
        self.now = 10_000.0

    def __call__(self) -> float:
        return self.now


@component
async def test_backpressure_reads_the_sweep_snapshot_and_falls_back_when_stale(
    fake_clock,
):
    """The consumer's per-poll gauge read serves the flush sweep's
    published fold while it is fresh (younger than two sweep
    intervals) — here the published snapshot says 150 staged bytes
    while the (empty) stage would scan 0, so the pause PROVES the
    shared read — and falls back to a live scan once the sweep stops
    publishing, so a halted flush loop never freezes backpressure."""
    from stagekit import fast_flush_settings

    from millrace.stage import StageGauges, StageManager

    journal: list[tuple] = []
    mono = _Mono()
    manager = StageManager(
        "memory:///",
        "millrace",
        settings=fast_flush_settings(),
        ack_hook=lambda ack: journal.append(("ack", ack)),
        monotonic=mono,
    )
    fake = FakeConsumer(journal=journal)
    cfg = make_config(
        backpressure=BackpressureConfig(
            pause_staged_bytes=100,
            resume_staged_bytes=50,
            pause_oldest_age_s=3600,
            resume_oldest_age_s=1800,
        )
    )  # flush_sweep_s default 5 -> staleness bound 10 s
    consumer = make_consumer(fake, manager, cfg, fake_clock)
    await consumer.start()

    manager.publish_gauges(
        {
            ("events", 0): StageGauges(
                staged_bytes=150,
                staged_rows=1,
                staged_teams=1,
                oldest_first_staged_ts=None,
            )
        }
    )
    await consumer._step()
    assert consumer.paused  # the snapshot's 150 bytes, not the live 0

    # The snapshot ages past two sweep intervals: the consumer falls
    # back to a live scan (0 bytes staged) and resumes.
    mono.now += 11.0
    await consumer._step()
    assert not consumer.paused
    await manager.close()
