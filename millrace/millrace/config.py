"""Environment-driven configuration, validated at startup.

Fail-fast at boot (docs/kafka-ingestion.md §Deployment): the catalog,
table and incarnation are resolved and mismatches refused before any
consume begins; the junk ``event_time`` policy (clamp vs quarantine) is
config, validated here. One process = one topic → one table.

Every knob is a ``MILLRACE_*`` environment variable; every validation
failure names the knob and the problem, and :func:`load_config` reports
ALL problems at once rather than failing one restart at a time.

Knobs
-----

| variable | default | meaning |
|---|---|---|
| ``MILLRACE_KAFKA_BOOTSTRAP_SERVERS`` | required | librdkafka bootstrap list |
| ``MILLRACE_KAFKA_TOPIC`` | required | the one topic this pipeline consumes |
| ``MILLRACE_KAFKA_GROUP_ID`` | ``millrace-{topic}`` | offset-storage group (static mode has no group semantics — it namespaces ``__consumer_offsets``; changing it loses the committed offsets, and where consumption then starts is ``MILLRACE_KAFKA_AUTO_OFFSET_RESET``'s call) |
| ``MILLRACE_KAFKA_ASSIGNMENT`` | ``static`` | ``static`` (explicit ``assign()`` list, millpond's model) or ``cooperative`` (group assignment with cooperative-sticky rebalancing) |
| ``MILLRACE_KAFKA_PARTITIONS`` | required iff static | comma-separated partition ids, e.g. ``0,1,2``; forbidden under ``cooperative`` (the group hands partitions out) |
| ``MILLRACE_KAFKA_AUTO_OFFSET_RESET`` | ``latest`` | where a partition with NO committed offset starts: ``latest`` (fleet policy — the head) or ``earliest`` (the log start). The tension is real and documented below the table |
| ``MILLRACE_KAFKA_MAX_PARTITION_FETCH_BYTES`` | 33554432 (32 MiB) | librdkafka ``max.partition.fetch.bytes`` — per-partition fetch response cap. The 1 MiB library default capped millpond's hot partition at ~50 MB/s (dozens of serialized fetch round trips per poll); the whale shape is ~55 MB/s, i.e. ~25 MB per hot partition per 500 ms poll, so one response must hold a poll's worth with headroom |
| ``MILLRACE_KAFKA_FETCH_MAX_BYTES`` | 268435456 (256 MiB) | librdkafka ``fetch.max.bytes`` — the per-fetch aggregate across the pod's partitions. Must be ≥ the per-partition cap (one hot partition's response must fit) and cover a poll's aggregate when several partitions run hot at once |
| ``MILLRACE_KAFKA_QUEUED_MAX_MESSAGES_KBYTES`` | 262144 (256 MiB) | librdkafka ``queued.max.messages.kbytes`` — the local fetch queue. Sized at ~5 s of a 50 MB/s pod inflow so a staging-latency spike doesn't stall fetches; past it librdkafka throttles fetches — the only fetch-side throttle there is (nothing pauses the consumer downstream; the staging gauges and alerts carry that job) |
| ``MILLRACE_TEAM_KEY_CODEC`` | ``utf8-decimal`` | how a record's ``team_id`` is extracted: ``utf8-decimal`` (the key is ``b"12345"``) or ``be64`` (the key is the 8-byte big-endian unsigned) for team-keyed topics, or ``value-json:<field>`` (e.g. ``value-json:team_id``) reading a top-level field of the JSON payload for topics NOT keyed by team — then every partition's instance can hold every team, and a team spread over K partitions flushes up to K files per window (docs/kafka-ingestion.md §Staging layout) |
| ``MILLRACE_CATALOG`` / ``MILLRACE_NAMESPACE`` / ``MILLRACE_TABLE`` | required | the one destination table's identity |
| ``MILLRACE_STAGE_URL`` | required | staging root: ``s3://bucket/millrace``, ``file:///path`` or ``memory:///`` (tests). One SlateDB instance per claimed partition lives at ``<base>/<topic>/<partition>`` |
| ``MILLRACE_TARGET_OUTPUT_BYTES`` | 500 MiB | planner size trigger — the estimated OUTPUT (parquet) size, compression roughly accounted (docs/kafka-ingestion.md §The flush planner) |
| ``MILLRACE_FLUSH_DEADLINE_S`` | 900 | planner age trigger — the tail's freshness SLA |
| ``MILLRACE_SLOW_LANE_DEADLINE_S`` | 21600 (6 h) | slow lane: flush any key this old regardless of size; accepted range 6–24 h |
| ``MILLRACE_MIN_FLUSH_BYTES`` | 1 MiB | the age lane's minimum size — a smaller key at the age deadline waits for the slow lane |
| ``MILLRACE_MAX_FILES_PER_COMMIT`` | 512 | commit chunking bound |
| ``MILLRACE_POISON_MAX_RECORDS`` | 1000 | per-run quarantine bound; exceeding it halts the loop loudly instead of committing past a flood |
| ``MILLRACE_POISON_VALUE_MAX_BYTES`` | 1 MiB | per-record cap on the quarantined value bytes (original length recorded) |
| ``MILLRACE_POISON_RETENTION_S`` | 604800 (7 d) | how long a quarantined record is kept for forensics before the poison sweeper deletes it (stage.py ``purge_poison_expired``). Entries older than this are deleted in bounded batches; entries WITHOUT a quarantine stamp (v1 envelopes — pre-retention builds, and flush-path quarantines until the flusher passes its clock) are kept forever: unknown age is never deleted. Poison is forensic — nothing depends on its presence, so a pod that is down at expiry simply purges at the next sweep. 7 d matches the server's default receipt retention: the incidents poison matters to are replay/settlement questions, all inside that window |
| ``MILLRACE_POISON_SWEEP_S`` | 3600 | the poison sweeper's cadence (per partition, off the consume/flush path — main.py's side task) |
| ``MILLRACE_SLATEDB_GC_ENABLED`` | ``false`` | the writer-internal SlateDB GC loop. DEFAULT OFF because the external maintenance service (``python -m millrace.maintenance``) is deployed beside every millrace fleet and owns collection — doubled GC is wasted LIST/DELETE traffic on the replicas (docs/kafka-ingestion.md §Maintenance services; GC itself is idempotent and overlap-safe, so a mixed or misconfigured deployment is wasteful, never corrupt). Boot logs a loud warning when off: the setting ASSUMES the maintenance service exists — without it, garbage accumulates forever. There is deliberately NO knob for ``compactor_options``: disabling the embedded compactor stalls memtable→L0 flushes at ``l0_max_ssts`` with nothing to drain them (parity-pinned, tests/test_slatedb_parity.py) |
| ``MILLRACE_SLATEDB_MAX_UNFLUSHED_BYTES`` | 268435456 (256 MiB) | per-instance bound on unflushed memtable+WAL bytes before SlateDB stalls writes (the library default is 1 GiB — per INSTANCE, so 64 partitions per pod could hold 64 GiB of unflushed state; the README carries the per-pod memory arithmetic). The binding requires it to exceed ``l0_sst_size_bytes`` (never overridden; 64 MiB), which the validation enforces |
| ``MILLRACE_SLATEDB_L0_MAX_SSTS`` | 8 | L0 depth at which a writer's memtable flush stalls until the embedded compactor drains (SlateDB's default; the whale-shape stall margin is a bench topic — tests/test_stage_bench.py) |
| ``MILLRACE_CONSUME_BATCH_SIZE`` | 65536 | max messages per ``consume()`` call — the RECORD cap, a backstop for degenerate tiny-payload floods; at the design's ~1 KB record shape the byte cap below is the operative bound. Per-partition durability waits overlap within a poll (consumer.py), so a large batch still stages in ~one WAL round trip |
| ``MILLRACE_CONSUME_BATCH_MAX_BYTES`` | 268435456 (256 MiB) | per-poll byte bound, enforced by clamping the poll's message count to this ÷ the run's observed average record size (key + value bytes, cumulative; floored at 1 message so an oversized record still flows). The first poll after a start has no observation and is bounded by the record cap alone |
| ``MILLRACE_POLL_TIMEOUT_MS`` | 500 | per-poll block bound; also the stop() latency |
| ``MILLRACE_EVENT_TIME_POLICY`` | ``quarantine`` | flush-time junk ``event_time`` handling (``clamp`` or ``quarantine``; consumed by the flush phase) |
| ``MILLRACE_EVENT_TIME_MAX_PAST_S`` | 2592000 (30 d) | junk window lower bound: an ``event_time`` older than ``now − this`` is junk (flush phase) |
| ``MILLRACE_EVENT_TIME_MAX_FUTURE_S`` | 86400 (1 d) | junk window upper bound: an ``event_time`` newer than ``now + this`` is junk (flush phase) |
| ``MILLRACE_RECEIPT_HORIZON_S`` | 518400 (6 d) | receipt-horizon guard: a persisted ``prepared/`` entry whose receipt lookup 404s is republished ONLY while younger than this; past it the server's receipt may have been purged and a blind replay could duplicate an executed commit, so the pipeline HALTS for operator reconciliation. THE DEPENDENCY IS NOT ON THE WIRE: the server never reports its receipt retention (``CatalogOptions`` carries snapshot retention only; ``GET /v1/info`` is instance identity — verified against hoglake.yaml), so this knob CARRIES the assumption that it stays below the server's effective receipt purge floor. That floor is ``max(HOGLAKE_RECEIPT_RETENTION_SECONDS, 2 × the catalog's snapshot retention)`` capped at 30 d and never below 1 h (server-side ``MIN_RECEIPT_RETENTION_SECONDS``): the 6 d default undercuts the 7 d server default; an operator who lowers the server knob (or runs a snapshot retention above ~3.5 d, which lifts the floor) must lower this knob with it. If pyhoglake grows a ``Catalog._receipt_retention_seconds`` accessor the flusher honors it instead of the knob (the guarded-accessor seam, flush.py ``_receipt_horizon_s``) |
| ``MILLRACE_FLUSH_SWEEP_S`` | 5 | pause between flush sweeps (one tick = one planning pass over every claimed partition) |
| ``MILLRACE_FLUSH_SCAN_MIN_BYTES`` | 67108864 (64 MiB) | FLOOR for the flush's staged-byte scan budget (flush.py §Bounded unit of work): the budget itself is derived from the size lane (``target_output_bytes ÷ observed compression ratio``), and the floor keeps toy/miniature targets from fragmenting every flush into single-row windows. The budget's job is bounding post-outage catch-up memory, not sizing steady-state files |
| ``MILLRACE_FLUSH_SCAN_MAX_ROWS`` | 8000000 | row-count half of the scan budget: bounds the per-row Arrow-build work of one decision against degenerate tiny payloads (at the design's ~1 KB/row the byte budget binds long before this) |
| ``MILLRACE_COMPRESSION_RATIO_HALFLIFE`` | 20 | halflife, in completed flushes, of the EWMA of observed ``parquet bytes ÷ staged bytes`` that feeds the planner's size lane (flush.py ``CompressionRatioEwma``); the estimate is in-memory only (a restart falls back to ``DEFAULT_COMPRESSION_RATIO``, which errs toward over-staging) and resets on a table shape/incarnation change |
| ``MILLRACE_FLUSH_CONCURRENCY`` | 4 | per-partition in-flight flush decisions (1 = the serial sweep); the process-wide in-flight bound is ``4 ×`` this (flush.py ``FlushRunner``) |
| ``MILLRACE_METRICS_PORT`` | 8000 | the operational HTTP surface's port (0 = ephemeral) |

Renamed knobs — the pre-three-lane names ``MILLRACE_TARGET_FILE_BYTES``
and ``MILLRACE_REAP_DEADLINE_S`` are REFUSED at boot, each refusal
naming its successor. There is no deprecation window and no silent
alias: the semantics NARROWED at the rename (what was the
raw-staged-bytes trigger now sizes the estimated parquet OUTPUT, and
the downtime reaper became a first-class lane with a bounded range), so
carrying an old value under the new name would deploy a policy the
operator did not ask for, and no deployment predates the rename.

``MILLRACE_KAFKA_AUTO_OFFSET_RESET`` carries a real tension, so the
default is stated rather than inherited: ``earliest`` never silently
drops a partition's existing data (with ``latest``, a partition with no
committed offset — a new partition, a changed ``group.id``, offsets aged
out of ``__consumer_offsets`` — starts at the head and the backlog is
simply never consumed: removal-by-omission, millpond AGENT.md), but a
fresh group replays the topic's WHOLE retention window — the 7-day
replay recorded in the millpond chart's comments, the incident that made
``latest`` the fleet policy. Millrace bounds the damage either way: a
replay is idempotent while the range is still staged, and the
settled-floor boundary where a replay CAN duplicate (already flushed,
receipt-confirmed, deleted from staging) is the same under both settings
— docs/kafka-ingestion.md §Delivery semantics names it, and the
operators' levers there (group-id stability, offset retention) apply
unchanged. The default follows the fleet: ``latest``.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Final

from .planner import (
    FLUSH_DEADLINE_S_DEFAULT,
    MAX_FILES_PER_COMMIT_DEFAULT,
    MIN_FLUSH_BYTES_DEFAULT,
    SLOW_LANE_DEADLINE_S_DEFAULT,
    TARGET_OUTPUT_BYTES_DEFAULT,
    PlannerKnobs,
)


class ConfigError(Exception):
    """Startup configuration is invalid. ``problems`` carries every
    problem found, each naming the knob at fault — fail-fast at boot
    means ALL of them reported on the first boot, not one per restart."""

    def __init__(self, problems: list[str]) -> None:
        self.problems = tuple(problems)
        super().__init__(
            f"invalid millrace configuration ({len(problems)} problem(s)):\n"
            + "\n".join(f"  - {p}" for p in problems)
        )


class AssignmentMode(Enum):
    """How partitions are claimed (docs/kafka-ingestion.md §Deployment)."""

    STATIC = "static"
    COOPERATIVE = "cooperative"


class TeamKeyCodec(Enum):
    """How a record's ``team_id`` is extracted (the codec is a
    pipeline-wide constant: a wrong choice quarantines every record, and
    the poison limit turns that flood into a loud halt).

    ``UTF8_DECIMAL``: the message key is the ASCII decimal rendering,
    e.g. ``b"12345"``. ``BE64``: the key is the team id as an 8-byte
    big-endian unsigned integer. Both require a team-keyed topic.

    ``VALUE_JSON``: the topic is NOT keyed by team (e.g.
    ``clickhouse_events_json``); the team id is read from a configured
    top-level field of the JSON payload (``Config.team_id_field``, set
    via ``MILLRACE_TEAM_KEY_CODEC=value-json:<field>``). The message key
    is not consulted. Consequence to design: with an unkeyed topic every
    partition's SlateDB instance can hold every team, so a team spread
    over K partitions is up to K files per flush window — the
    per-partition staging topology is unchanged, and the flush identity
    (topic, partition, offset range) scopes each window, so the files
    never collide.
    """

    UTF8_DECIMAL = "utf8-decimal"
    BE64 = "be64"
    VALUE_JSON = "value-json"


class AutoOffsetReset(Enum):
    """Where a partition with no committed offset starts consuming.

    ``LATEST`` (the default): the head — fleet policy since the millpond
    chart's 7-day-replay incident (a fresh group under ``earliest``
    replays the topic's whole retention window). ``EARLIEST``: the log
    start — no silent skip of existing data (under ``latest`` a
    partition with no committed offset never consumes its backlog:
    removal-by-omission, millpond AGENT.md), at the price of the
    whole-retention replay. Millrace's per-partition staged state makes
    a replay idempotent above the settled floor, and the receipt horizon
    bounds it below — the boundary is the same under both settings
    (docs/kafka-ingestion.md §Delivery semantics), which is what makes
    the fleet default safe to follow here.
    """

    EARLIEST = "earliest"
    LATEST = "latest"


class EventTimePolicy(Enum):
    """What the flush does with outlier ``event_time`` values."""

    CLAMP = "clamp"
    QUARANTINE = "quarantine"


@dataclass(frozen=True)
class PoisonConfig:
    """Quarantine bounds and retention. ``max_records_per_run`` is the
    loud-halt bound: a flood of unstageable records means a misconfigured
    key codec or a producer bug, and committing past it would hide the
    damage — the consumer raises instead. ``value_max_bytes`` caps each
    quarantined record's stored value; the original length is recorded.
    ``retention_s`` / ``sweep_s`` drive the poison sweeper (main.py):
    entries older than ``retention_s`` are purged per partition every
    ``sweep_s`` — poison is forensic state with a bounded lifetime, and
    entries whose age is unknowable (v1 envelopes) are kept forever.
    """

    max_records_per_run: int
    value_max_bytes: int
    retention_s: int = 7 * 86400
    sweep_s: int = 3600

    def __post_init__(self) -> None:
        if self.max_records_per_run < 1:
            raise ValueError(
                f"MILLRACE_POISON_MAX_RECORDS must be >= 1, "
                f"got {self.max_records_per_run}"
            )
        if self.value_max_bytes < 1:
            raise ValueError(
                f"MILLRACE_POISON_VALUE_MAX_BYTES must be >= 1, "
                f"got {self.value_max_bytes}"
            )
        if self.retention_s < 1:
            raise ValueError(
                f"MILLRACE_POISON_RETENTION_S must be >= 1, got {self.retention_s}"
            )
        if self.sweep_s < 1:
            raise ValueError(
                f"MILLRACE_POISON_SWEEP_S must be >= 1, got {self.sweep_s}"
            )


#: The binding validates ``max_unflushed_bytes > l0_sst_size_bytes`` at
#: build time; we never set ``l0_sst_size_bytes``, so the pinned 0.17.x
#: default (64 MiB) is the effective floor for the knob.
SLATEDB_L0_SST_SIZE_BYTES: Final = 64 * 1024 * 1024


@dataclass(frozen=True)
class SlateDbConfig:
    """Per-instance SlateDB tuning for the writer's partition stages
    (T9 — the library defaults are wrong for this shape: 1 GiB of
    unflushed state PER INSTANCE and an 8-deep L0, times K partitions
    per pod, with no pod-level cap). Applied through
    :func:`millrace.stage.build_slatedb_settings`.

    ``gc_enabled`` defaults to False: the external maintenance service
    is part of every millrace deployment and owns collection — see the
    module docstring's knob table for the assumption and its warning
    posture. ``compactor_options`` is not represented here on purpose:
    disabling the writer's embedded compactor is a parity-pinned stall
    (stage.build_slatedb_settings says why).
    """

    gc_enabled: bool = False
    max_unflushed_bytes: int = 256 * 1024 * 1024
    l0_max_ssts: int = 8

    def __post_init__(self) -> None:
        if self.max_unflushed_bytes <= SLATEDB_L0_SST_SIZE_BYTES:
            raise ValueError(
                f"MILLRACE_SLATEDB_MAX_UNFLUSHED_BYTES must be greater than "
                f"the (library-default, never overridden) l0_sst_size_bytes "
                f"of {SLATEDB_L0_SST_SIZE_BYTES}, got {self.max_unflushed_bytes}"
            )
        if self.l0_max_ssts < 1:
            raise ValueError(
                f"MILLRACE_SLATEDB_L0_MAX_SSTS must be >= 1, got {self.l0_max_ssts}"
            )


_DEFAULT_STAGE_BASE: Final = "millrace"

#: Flush-phase knob defaults (the dataclass fields and the env parsing
#: share them, so the two can never disagree).
EVENT_TIME_MAX_PAST_S_DEFAULT: Final = 30 * 86400
EVENT_TIME_MAX_FUTURE_S_DEFAULT: Final = 86400
FLUSH_SWEEP_S_DEFAULT: Final = 5
#: The flush scan budget's floor (module docstring): the derived budget
#: (``target_output_bytes ÷ ratio``) only gets to shrink a decision's
#: unit of work down to this; below it the windows fragment for no
#: memory saving anyone needs.
FLUSH_SCAN_MIN_BYTES_DEFAULT: Final = 64 * 1024 * 1024
#: The row-count half of the scan budget (module docstring).
FLUSH_SCAN_MAX_ROWS_DEFAULT: Final = 8_000_000
#: Halflife (in completed flushes) of the compression-ratio EWMA.
COMPRESSION_RATIO_HALFLIFE_DEFAULT: Final = 20
#: Per-partition in-flight flush decisions (FlushRunner; 1 = serial).
FLUSH_CONCURRENCY_DEFAULT: Final = 4
#: Consume-loop knob defaults (module docstring). The record cap is the
#: backstop; the byte cap is the operative bound at the design's ~1 KB
#: record shape (256 MiB ÷ 1 KB = 262144 records, so 65536 records ≈
#: 64 MiB binds first — a poll's staging burst stays WAL-sized, and at a
#: 500 ms poll cadence the record cap still allows ~131k records/s per
#: pod, well past a pod's whale share).
CONSUME_BATCH_SIZE_DEFAULT: Final = 65536
CONSUME_BATCH_MAX_BYTES_DEFAULT: Final = 256 * 1024 * 1024
#: librdkafka fetch tuning (module docstring): the whale's hot partition
#: at ~50 MB/s moves ~25 MB per 500 ms poll; the 1 MiB library default
#: for ``max.partition.fetch.bytes`` serialized ~25 fetch round trips
#: per poll and capped the partition there (millpond's observation).
KAFKA_MAX_PARTITION_FETCH_BYTES_DEFAULT: Final = 32 * 1024 * 1024
KAFKA_FETCH_MAX_BYTES_DEFAULT: Final = 256 * 1024 * 1024
#: In KiB, as librdkafka names it: 262144 KiB = 256 MiB ≈ 5 s of a
#: 50 MB/s pod inflow.
KAFKA_QUEUED_MAX_MESSAGES_KBYTES_DEFAULT: Final = 262144
#: One day BELOW the server's default receipt retention
#: (``HOGLAKE_RECEIPT_RETENTION_SECONDS``, 7 days — purged in bounded
#: pages by the cleanup sweep, V24). The dependency is NOT on the wire
#: (the server exposes snapshot retention only, never receipt retention
#: — verified against hoglake.yaml's ``CatalogOptions`` and ``/v1/info``),
#: so this knob is the guard and its value must stay below the server's
#: EFFECTIVE purge floor: ``max(HOGLAKE_RECEIPT_RETENTION_SECONDS, 2 x
#: snapshot retention)``, capped at 30 d and floored at 1 h server-side.
#: Within the horizon a receipt 404 can only mean "the commit never
#: landed" and replaying the persisted request is safe; past it the 404
#: is ambiguous with a purged receipt and the flusher halts. A pyhoglake
#: that exposes ``Catalog._receipt_retention_seconds`` overrides this
#: knob (flush.py ``_receipt_horizon_s``).
RECEIPT_HORIZON_S_DEFAULT: Final = 6 * 86400

# Kafka topic names: https://kafka.apache.org/documentation/#topicnames
_TOPIC_NAME: Final = re.compile(r"^[a-zA-Z0-9._-]{1,249}$")
# hoglake identifier rules (millpond config.py's, kept in step).
_CATALOG_NAME: Final = re.compile(r"^[a-z][a-z0-9_-]{0,62}$")
_IDENTIFIER: Final = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]{0,127}$")
# S3 bucket names (millpond's _S3_URI bucket piece).
_S3_BUCKET: Final = re.compile(r"^[a-z0-9][a-z0-9.\-]{1,61}[a-z0-9]$")
#: The ``value-json:<field>`` payload field: a top-level JSON object key,
#: identifier-shaped (the grammar is deliberately narrower than "any
#: string" so an operator typo — whitespace, a dot that implies the
#: nesting we do not support — fails at boot, not as a poison flood).
_VALUE_JSON_FIELD: Final = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]{0,127}$")


#: The slow lane's accepted range, 6–24 h (docs/kafka-ingestion.md §The
#: flush planner): below 6 h the lane duplicates the age lane's job;
#: above 24 h churned keys stand unflushed for over a day.
SLOW_LANE_DEADLINE_S_MIN: Final = 6 * 3600
SLOW_LANE_DEADLINE_S_MAX: Final = 24 * 3600

#: Pre-three-lane knob names → their successors, REFUSED at boot (module
#: docstring): the rename narrowed the semantics, so there is no alias.
_RENAMED_ENV: Final = {
    "MILLRACE_TARGET_FILE_BYTES": "MILLRACE_TARGET_OUTPUT_BYTES",
    "MILLRACE_REAP_DEADLINE_S": "MILLRACE_SLOW_LANE_DEADLINE_S",
}


@dataclass(frozen=True)
class Config:
    """Validated startup configuration for one millrace pipeline.

    ``static_partitions`` is set iff ``assignment_mode`` is
    :class:`AssignmentMode.STATIC`. ``stage_store_url`` /
    ``stage_base_path`` are the parsed halves of ``MILLRACE_STAGE_URL``,
    ready for :class:`millrace.stage.StageManager`: one SlateDB instance
    per claimed partition lives at ``{stage_base_path}/{topic}/{partition}``
    on the store, and staged state follows the partition to its new
    owner, never the pod.
    """

    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_group_id: str
    assignment_mode: AssignmentMode
    static_partitions: tuple[int, ...] | None
    team_key_codec: TeamKeyCodec
    catalog: str
    namespace: str
    table: str
    stage_store_url: str
    stage_base_path: str
    target_output_bytes: int
    flush_deadline_s: int
    slow_lane_deadline_s: int
    min_flush_bytes: int
    max_files_per_commit: int
    poison: PoisonConfig
    consume_batch_size: int
    poll_timeout_ms: int
    event_time_policy: EventTimePolicy
    metrics_port: int
    # Defaulted so existing constructors stay valid; load_config always
    # passes them explicitly.
    #: The payload field ``value-json`` reads (set iff the codec is
    #: :attr:`TeamKeyCodec.VALUE_JSON`; enforced by ``__post_init__``).
    team_id_field: str | None = None
    kafka_auto_offset_reset: AutoOffsetReset = AutoOffsetReset.LATEST
    consume_batch_max_bytes: int = CONSUME_BATCH_MAX_BYTES_DEFAULT
    kafka_max_partition_fetch_bytes: int = KAFKA_MAX_PARTITION_FETCH_BYTES_DEFAULT
    kafka_fetch_max_bytes: int = KAFKA_FETCH_MAX_BYTES_DEFAULT
    kafka_queued_max_messages_kbytes: int = KAFKA_QUEUED_MAX_MESSAGES_KBYTES_DEFAULT
    # Flush-phase knobs (Phase 4).
    event_time_max_past_s: int = EVENT_TIME_MAX_PAST_S_DEFAULT
    event_time_max_future_s: int = EVENT_TIME_MAX_FUTURE_S_DEFAULT
    flush_sweep_s: int = FLUSH_SWEEP_S_DEFAULT
    receipt_horizon_s: int = RECEIPT_HORIZON_S_DEFAULT
    flush_scan_min_bytes: int = FLUSH_SCAN_MIN_BYTES_DEFAULT
    flush_scan_max_rows: int = FLUSH_SCAN_MAX_ROWS_DEFAULT
    compression_ratio_halflife: int = COMPRESSION_RATIO_HALFLIFE_DEFAULT
    flush_concurrency: int = FLUSH_CONCURRENCY_DEFAULT
    # Per-instance SlateDB writer tuning (T9).
    slatedb: SlateDbConfig = SlateDbConfig()

    def __post_init__(self) -> None:
        # The codec/field pair is one invariant: value-json is
        # meaningless without its field, and a field under a key codec
        # would be silently ignored (an operator's typo deploying a
        # policy nobody reads).
        if self.team_key_codec is TeamKeyCodec.VALUE_JSON:
            if not self.team_id_field:
                raise ValueError(
                    "MILLRACE_TEAM_KEY_CODEC=value-json requires a payload "
                    "field: value-json:<field> (e.g. value-json:team_id)"
                )
        elif self.team_id_field is not None:
            raise ValueError(
                f"team_id_field {self.team_id_field!r} is set but "
                f"MILLRACE_TEAM_KEY_CODEC is {self.team_key_codec.value} — "
                f"the field only applies to value-json"
            )

    def planner_knobs(self) -> PlannerKnobs:
        """The planner's view of the flush-policy knobs."""
        return PlannerKnobs(
            target_output_bytes=self.target_output_bytes,
            flush_deadline_s=self.flush_deadline_s,
            slow_lane_deadline_s=self.slow_lane_deadline_s,
            min_flush_bytes=self.min_flush_bytes,
            max_files_per_commit=self.max_files_per_commit,
        )

    @property
    def team_key_codec_env(self) -> str:
        """The ``MILLRACE_TEAM_KEY_CODEC`` value that reproduces this
        configuration — the parser's inverse. ``value-json`` carries its
        payload field inline (``value-json:<field>``): dumping the bare
        enum value would boot-refuse the child ("names no payload
        field"), which is exactly the dump bug this property exists to
        make impossible (the live harness renders env from a Config)."""
        if self.team_key_codec is TeamKeyCodec.VALUE_JSON:
            assert self.team_id_field is not None  # __post_init__'s invariant
            return f"{TeamKeyCodec.VALUE_JSON.value}:{self.team_id_field}"
        return str(self.team_key_codec.value)


def _required(env: Mapping[str, str], name: str, problems: list[str]) -> str | None:
    raw = env.get(name, "").strip()
    if not raw:
        problems.append(f"{name} is required and is not set (or is empty)")
        return None
    return raw


def _positive_int(
    env: Mapping[str, str], name: str, default: int, problems: list[str]
) -> int | None:
    """A positive-integer knob, or its default. Zero is refused for the
    same reason millpond refuses it: an operator misrendering an unset
    value as "0" should fail loudly, not silently deploy a pipeline with
    the knob turned off."""
    raw = env.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        problems.append(f"{name}={raw!r} must be a positive integer")
        return None
    if value <= 0:
        problems.append(f"{name}={value} must be a positive integer")
        return None
    return value


def _enum[E: Enum](
    env: Mapping[str, str],
    name: str,
    choices: Sequence[E],
    default: E,
    problems: list[str],
) -> E | None:
    raw = env.get(name, "").strip()
    if not raw:
        return default
    for member in choices:
        if member.value == raw:
            return member
    valid = ", ".join(str(m.value) for m in choices)
    problems.append(f"{name}={raw!r} must be one of: {valid}")
    return None


def _bool(
    env: Mapping[str, str], name: str, default: bool, problems: list[str]
) -> bool:
    """A boolean knob (true/false/1/0/yes/no), or its default."""
    raw = env.get(name, "").strip().lower()
    if not raw:
        return default
    if raw in ("true", "1", "yes"):
        return True
    if raw in ("false", "0", "no"):
        return False
    problems.append(f"{name}={raw!r} must be a boolean (true/false)")
    return default


def _parse_partitions(raw: str, problems: list[str]) -> tuple[int, ...] | None:
    """``0,1,2`` → (0, 1, 2): comma-separated, unique, non-negative."""
    out: list[int] = []
    ok = True
    for token in raw.split(","):
        token = token.strip()
        if not token or not all("0" <= c <= "9" for c in token):
            problems.append(
                f"MILLRACE_KAFKA_PARTITIONS entry {token!r} is not a "
                f"non-negative integer (expected e.g. 0,1,2)"
            )
            ok = False
            continue
        value = int(token)
        if value in out:
            problems.append(f"MILLRACE_KAFKA_PARTITIONS lists partition {value} twice")
            ok = False
            continue
        out.append(value)
    if not out and ok:
        problems.append("MILLRACE_KAFKA_PARTITIONS is empty")
        return None
    return tuple(sorted(out)) if ok else None


def _parse_stage_url(raw: str, problems: list[str]) -> tuple[str, str] | None:
    """Split ``MILLRACE_STAGE_URL`` into (object-store URL, base path).

    slatedb's ``ObjectStore.resolve`` accepts NO path component — the
    path belongs to the DbBuilder — so the split is config's job:
    ``s3://bucket/millrace`` → (``s3://bucket``, ``millrace``),
    ``file:///var/lib/millrace`` → (``file:///``, ``/var/lib/millrace``),
    ``memory:///`` → (``memory:///``, ``millrace``).
    """
    if raw.startswith("memory://"):
        rest = raw[len("memory://") :].strip("/")
        return "memory:///", rest or _DEFAULT_STAGE_BASE
    if raw.startswith("file://"):
        rest = raw[len("file://") :].rstrip("/")
        if not rest.strip("/"):
            problems.append(
                f"MILLRACE_STAGE_URL {raw!r} names no directory; "
                f"expected e.g. file:///var/lib/millrace"
            )
            return None
        return "file:///", rest
    if raw.startswith("s3://"):
        rest = raw[len("s3://") :]
        bucket, _, prefix = rest.partition("/")
        if not _S3_BUCKET.match(bucket):
            problems.append(
                f"MILLRACE_STAGE_URL {raw!r}: {bucket!r} is not a valid s3 "
                f"bucket name (expected s3://bucket/prefix)"
            )
            return None
        if not prefix.strip("/"):
            problems.append(
                f"MILLRACE_STAGE_URL {raw!r} names no prefix below the "
                f"bucket; the staging root is per-pipeline state, "
                f"e.g. s3://{bucket}/millrace"
            )
            return None
        return f"s3://{bucket}", prefix.strip("/")
    problems.append(
        f"MILLRACE_STAGE_URL {raw!r} must be an s3://bucket/prefix, "
        f"file:///path or memory:/// URL"
    )
    return None


def _parse_team_key_codec(
    env: Mapping[str, str], problems: list[str]
) -> tuple[TeamKeyCodec, str | None] | None:
    """``MILLRACE_TEAM_KEY_CODEC`` → (codec, payload field).

    The keyed codecs are bare enum values; ``value-json`` carries its
    payload field inline (``value-json:team_id``), validated here so a
    typo fails at boot instead of quarantining every record at runtime.
    """
    raw = env.get("MILLRACE_TEAM_KEY_CODEC", "").strip()
    if not raw:
        return TeamKeyCodec.UTF8_DECIMAL, None
    prefix = f"{TeamKeyCodec.VALUE_JSON.value}:"
    if raw.startswith(prefix):
        field = raw[len(prefix) :].strip()
        if not _VALUE_JSON_FIELD.match(field):
            problems.append(
                f"MILLRACE_TEAM_KEY_CODEC {raw!r}: the value-json payload "
                f"field must be a top-level identifier-shaped JSON key "
                f"(must match [A-Za-z_][A-Za-z0-9_-]{{0,127}}), got {field!r}"
            )
            return None
        return TeamKeyCodec.VALUE_JSON, field
    if raw == TeamKeyCodec.VALUE_JSON.value:
        problems.append(
            f"MILLRACE_TEAM_KEY_CODEC {raw!r} names no payload field — "
            f"expected value-json:<field> (e.g. value-json:team_id)"
        )
        return None
    for member in (TeamKeyCodec.UTF8_DECIMAL, TeamKeyCodec.BE64):
        if member.value == raw:
            return member, None
    problems.append(
        f"MILLRACE_TEAM_KEY_CODEC={raw!r} must be one of: utf8-decimal, "
        f"be64, value-json:<field>"
    )
    return None


def load_config(env: Mapping[str, str]) -> Config:
    """Parse and validate the ``MILLRACE_*`` environment variables.

    Invalid or missing values are a startup failure, never a runtime
    one: every problem is collected and reported by one
    :class:`ConfigError`, each entry naming the knob and the problem.
    """
    problems: list[str] = []

    # Renamed knobs are refused FIRST, so an operator migrating an
    # environment meets the rename before any other complaint.
    for old_name, new_name in _RENAMED_ENV.items():
        if env.get(old_name, "").strip():
            problems.append(
                f"{old_name} was renamed {new_name} and is refused — the "
                f"rename narrowed the knob's semantics (see the module "
                f"docstring); set {new_name}"
            )

    bootstrap = _required(env, "MILLRACE_KAFKA_BOOTSTRAP_SERVERS", problems)
    topic = _required(env, "MILLRACE_KAFKA_TOPIC", problems)
    if topic is not None and not _TOPIC_NAME.match(topic):
        problems.append(
            f"MILLRACE_KAFKA_TOPIC {topic!r} is not a valid Kafka topic "
            f"name (must match [a-zA-Z0-9._-]{{1,249}})"
        )
        topic = None
    group_id = env.get("MILLRACE_KAFKA_GROUP_ID", "").strip() or (
        f"millrace-{topic}" if topic is not None else None
    )

    mode = _enum(
        env,
        "MILLRACE_KAFKA_ASSIGNMENT",
        list(AssignmentMode),
        AssignmentMode.STATIC,
        problems,
    )
    partitions_raw = env.get("MILLRACE_KAFKA_PARTITIONS", "").strip()
    partitions: tuple[int, ...] | None = None
    if mode is AssignmentMode.STATIC:
        if not partitions_raw:
            problems.append(
                "MILLRACE_KAFKA_PARTITIONS is required when "
                "MILLRACE_KAFKA_ASSIGNMENT=static (the partitions this "
                "replica claims, e.g. 0,1,2)"
            )
        else:
            partitions = _parse_partitions(partitions_raw, problems)
    elif mode is AssignmentMode.COOPERATIVE and partitions_raw:
        problems.append(
            "MILLRACE_KAFKA_PARTITIONS must not be set when "
            "MILLRACE_KAFKA_ASSIGNMENT=cooperative — the consumer group "
            "hands partitions out"
        )

    codec_pair = _parse_team_key_codec(env, problems)
    codec = codec_pair[0] if codec_pair is not None else None
    team_id_field = codec_pair[1] if codec_pair is not None else None

    auto_offset_reset = _enum(
        env,
        "MILLRACE_KAFKA_AUTO_OFFSET_RESET",
        list(AutoOffsetReset),
        AutoOffsetReset.LATEST,
        problems,
    )
    max_partition_fetch_bytes = _positive_int(
        env,
        "MILLRACE_KAFKA_MAX_PARTITION_FETCH_BYTES",
        KAFKA_MAX_PARTITION_FETCH_BYTES_DEFAULT,
        problems,
    )
    fetch_max_bytes = _positive_int(
        env,
        "MILLRACE_KAFKA_FETCH_MAX_BYTES",
        KAFKA_FETCH_MAX_BYTES_DEFAULT,
        problems,
    )
    queued_max_messages_kbytes = _positive_int(
        env,
        "MILLRACE_KAFKA_QUEUED_MAX_MESSAGES_KBYTES",
        KAFKA_QUEUED_MAX_MESSAGES_KBYTES_DEFAULT,
        problems,
    )
    if (
        max_partition_fetch_bytes is not None
        and fetch_max_bytes is not None
        and fetch_max_bytes < max_partition_fetch_bytes
    ):
        problems.append(
            f"MILLRACE_KAFKA_FETCH_MAX_BYTES ({fetch_max_bytes}) must be "
            f">= MILLRACE_KAFKA_MAX_PARTITION_FETCH_BYTES "
            f"({max_partition_fetch_bytes}) — one partition's fetch "
            f"response must fit in the per-fetch aggregate"
        )

    catalog = _required(env, "MILLRACE_CATALOG", problems)
    if catalog is not None and not _CATALOG_NAME.match(catalog):
        problems.append(
            f"MILLRACE_CATALOG {catalog!r} is not a valid hoglake catalog "
            f"name (must match [a-z][a-z0-9_-]{{0,62}})"
        )
        catalog = None
    namespace = _required(env, "MILLRACE_NAMESPACE", problems)
    if namespace is not None and not _IDENTIFIER.match(namespace):
        problems.append(
            f"MILLRACE_NAMESPACE {namespace!r} is not a valid hoglake "
            f"identifier (must match [A-Za-z_][A-Za-z0-9_-]{{0,127}})"
        )
        namespace = None
    table = _required(env, "MILLRACE_TABLE", problems)
    if table is not None and not _IDENTIFIER.match(table):
        problems.append(
            f"MILLRACE_TABLE {table!r} is not a valid hoglake identifier "
            f"(must match [A-Za-z_][A-Za-z0-9_-]{{0,127}})"
        )
        table = None

    stage_raw = _required(env, "MILLRACE_STAGE_URL", problems)
    stage: tuple[str, str] | None = None
    if stage_raw is not None:
        stage = _parse_stage_url(stage_raw, problems)

    target_output_bytes = _positive_int(
        env, "MILLRACE_TARGET_OUTPUT_BYTES", TARGET_OUTPUT_BYTES_DEFAULT, problems
    )
    flush_deadline_s = _positive_int(
        env, "MILLRACE_FLUSH_DEADLINE_S", FLUSH_DEADLINE_S_DEFAULT, problems
    )
    slow_lane_deadline_s = _positive_int(
        env, "MILLRACE_SLOW_LANE_DEADLINE_S", SLOW_LANE_DEADLINE_S_DEFAULT, problems
    )
    if slow_lane_deadline_s is not None and not (
        SLOW_LANE_DEADLINE_S_MIN <= slow_lane_deadline_s <= SLOW_LANE_DEADLINE_S_MAX
    ):
        problems.append(
            f"MILLRACE_SLOW_LANE_DEADLINE_S={slow_lane_deadline_s} must be "
            f"within 6-24 h ({SLOW_LANE_DEADLINE_S_MIN}-"
            f"{SLOW_LANE_DEADLINE_S_MAX} s)"
        )
        slow_lane_deadline_s = None
    min_flush_bytes = _positive_int(
        env, "MILLRACE_MIN_FLUSH_BYTES", MIN_FLUSH_BYTES_DEFAULT, problems
    )
    max_files_per_commit = _positive_int(
        env, "MILLRACE_MAX_FILES_PER_COMMIT", MAX_FILES_PER_COMMIT_DEFAULT, problems
    )

    poison: PoisonConfig | None = None
    poison_max = _positive_int(env, "MILLRACE_POISON_MAX_RECORDS", 1000, problems)
    poison_value_max = _positive_int(
        env, "MILLRACE_POISON_VALUE_MAX_BYTES", 1024 * 1024, problems
    )
    poison_retention_s = _positive_int(
        env, "MILLRACE_POISON_RETENTION_S", 7 * 86400, problems
    )
    poison_sweep_s = _positive_int(env, "MILLRACE_POISON_SWEEP_S", 3600, problems)
    if (
        poison_max is not None
        and poison_value_max is not None
        and poison_retention_s is not None
        and poison_sweep_s is not None
    ):
        try:
            poison = PoisonConfig(
                max_records_per_run=poison_max,
                value_max_bytes=poison_value_max,
                retention_s=poison_retention_s,
                sweep_s=poison_sweep_s,
            )
        except ValueError as exc:
            problems.append(str(exc))

    slatedb: SlateDbConfig | None = None
    slatedb_gc = _bool(env, "MILLRACE_SLATEDB_GC_ENABLED", False, problems)
    slatedb_max_unflushed = _positive_int(
        env, "MILLRACE_SLATEDB_MAX_UNFLUSHED_BYTES", 256 * 1024 * 1024, problems
    )
    slatedb_l0_max_ssts = _positive_int(
        env, "MILLRACE_SLATEDB_L0_MAX_SSTS", 8, problems
    )
    if slatedb_max_unflushed is not None and slatedb_l0_max_ssts is not None:
        try:
            slatedb = SlateDbConfig(
                gc_enabled=slatedb_gc,
                max_unflushed_bytes=slatedb_max_unflushed,
                l0_max_ssts=slatedb_l0_max_ssts,
            )
        except ValueError as exc:
            problems.append(str(exc))

    consume_batch_size = _positive_int(
        env, "MILLRACE_CONSUME_BATCH_SIZE", CONSUME_BATCH_SIZE_DEFAULT, problems
    )
    consume_batch_max_bytes = _positive_int(
        env,
        "MILLRACE_CONSUME_BATCH_MAX_BYTES",
        CONSUME_BATCH_MAX_BYTES_DEFAULT,
        problems,
    )
    poll_timeout_ms = _positive_int(env, "MILLRACE_POLL_TIMEOUT_MS", 500, problems)

    event_time_policy = _enum(
        env,
        "MILLRACE_EVENT_TIME_POLICY",
        list(EventTimePolicy),
        EventTimePolicy.QUARANTINE,
        problems,
    )
    event_time_max_past_s = _positive_int(
        env,
        "MILLRACE_EVENT_TIME_MAX_PAST_S",
        EVENT_TIME_MAX_PAST_S_DEFAULT,
        problems,
    )
    event_time_max_future_s = _positive_int(
        env,
        "MILLRACE_EVENT_TIME_MAX_FUTURE_S",
        EVENT_TIME_MAX_FUTURE_S_DEFAULT,
        problems,
    )
    flush_sweep_s = _positive_int(
        env, "MILLRACE_FLUSH_SWEEP_S", FLUSH_SWEEP_S_DEFAULT, problems
    )
    receipt_horizon_s = _positive_int(
        env, "MILLRACE_RECEIPT_HORIZON_S", RECEIPT_HORIZON_S_DEFAULT, problems
    )
    flush_scan_min_bytes = _positive_int(
        env, "MILLRACE_FLUSH_SCAN_MIN_BYTES", FLUSH_SCAN_MIN_BYTES_DEFAULT, problems
    )
    flush_scan_max_rows = _positive_int(
        env, "MILLRACE_FLUSH_SCAN_MAX_ROWS", FLUSH_SCAN_MAX_ROWS_DEFAULT, problems
    )
    compression_ratio_halflife = _positive_int(
        env,
        "MILLRACE_COMPRESSION_RATIO_HALFLIFE",
        COMPRESSION_RATIO_HALFLIFE_DEFAULT,
        problems,
    )
    flush_concurrency = _positive_int(
        env, "MILLRACE_FLUSH_CONCURRENCY", FLUSH_CONCURRENCY_DEFAULT, problems
    )

    port_raw = env.get("MILLRACE_METRICS_PORT", "").strip() or "8000"
    metrics_port: int | None
    try:
        metrics_port = int(port_raw)
    except ValueError:
        problems.append(f"MILLRACE_METRICS_PORT {port_raw!r} is not an integer")
        metrics_port = None
    else:
        if not 0 <= metrics_port <= 65535:
            problems.append(
                f"MILLRACE_METRICS_PORT {metrics_port} is out of range "
                f"(0-65535; 0 = ephemeral)"
            )
            metrics_port = None

    if problems:
        raise ConfigError(problems)

    # Flow invariant: every reader above either produced a value or
    # recorded a problem, so past the raise everything is present. The
    # asserts pin that for the type checker.
    assert bootstrap is not None
    assert topic is not None
    assert group_id is not None
    assert isinstance(mode, AssignmentMode)
    assert isinstance(codec, TeamKeyCodec)
    assert isinstance(auto_offset_reset, AutoOffsetReset)
    assert max_partition_fetch_bytes is not None
    assert fetch_max_bytes is not None
    assert queued_max_messages_kbytes is not None
    assert catalog is not None and namespace is not None and table is not None
    assert stage is not None
    assert target_output_bytes is not None
    assert flush_deadline_s is not None
    assert slow_lane_deadline_s is not None
    assert min_flush_bytes is not None
    assert max_files_per_commit is not None
    assert poison is not None
    assert slatedb is not None
    assert consume_batch_size is not None
    assert consume_batch_max_bytes is not None
    assert poll_timeout_ms is not None
    assert isinstance(event_time_policy, EventTimePolicy)
    assert metrics_port is not None
    assert event_time_max_past_s is not None
    assert event_time_max_future_s is not None
    assert flush_sweep_s is not None
    assert receipt_horizon_s is not None
    assert flush_scan_min_bytes is not None
    assert flush_scan_max_rows is not None
    assert compression_ratio_halflife is not None
    assert flush_concurrency is not None
    if mode is AssignmentMode.STATIC:
        assert partitions is not None
    return Config(
        kafka_bootstrap_servers=bootstrap,
        kafka_topic=topic,
        kafka_group_id=group_id,
        assignment_mode=mode,
        static_partitions=partitions,
        team_key_codec=codec,
        team_id_field=team_id_field,
        kafka_auto_offset_reset=auto_offset_reset,
        kafka_max_partition_fetch_bytes=max_partition_fetch_bytes,
        kafka_fetch_max_bytes=fetch_max_bytes,
        kafka_queued_max_messages_kbytes=queued_max_messages_kbytes,
        catalog=catalog,
        namespace=namespace,
        table=table,
        stage_store_url=stage[0],
        stage_base_path=stage[1],
        target_output_bytes=target_output_bytes,
        flush_deadline_s=flush_deadline_s,
        slow_lane_deadline_s=slow_lane_deadline_s,
        min_flush_bytes=min_flush_bytes,
        max_files_per_commit=max_files_per_commit,
        poison=poison,
        consume_batch_size=consume_batch_size,
        consume_batch_max_bytes=consume_batch_max_bytes,
        poll_timeout_ms=poll_timeout_ms,
        event_time_policy=event_time_policy,
        metrics_port=metrics_port,
        event_time_max_past_s=event_time_max_past_s,
        event_time_max_future_s=event_time_max_future_s,
        flush_sweep_s=flush_sweep_s,
        receipt_horizon_s=receipt_horizon_s,
        flush_scan_min_bytes=flush_scan_min_bytes,
        flush_scan_max_rows=flush_scan_max_rows,
        compression_ratio_halflife=compression_ratio_halflife,
        flush_concurrency=flush_concurrency,
        slatedb=slatedb,
    )
