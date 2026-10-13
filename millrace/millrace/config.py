"""Environment-driven configuration, validated at startup.

Fail-fast at boot (docs/kafka-ingestion.md §Deployment): the catalog,
namespace and table are resolved before any consume begins and the
table's incarnation (``table_uuid``) is pinned there — a later change
halts the flush (``table_recreated``); the junk ``event_time`` policy (clamp vs drop) is
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
| ``MILLRACE_KAFKA_GROUP_ID`` | ``millrace-{topic}`` | offset-storage group (static assignment has no group semantics — it namespaces ``__consumer_offsets``; changing it loses the committed offsets, and where consumption then starts is ``MILLRACE_KAFKA_AUTO_OFFSET_RESET``'s call) |
| ``MILLRACE_REPLICA_COUNT`` | required | the StatefulSet's replica count (positive). Partitions are assigned millpond's way: this replica claims every partition ``p`` of the topic with ``p % replica_count == ordinal`` (consumer.py ``compute_assignment``), the topic's partition count read from Kafka metadata ONCE at startup. Replicas dividing the partition count is the sensible shape; an ordinal that works out to no partitions refuses to start |
| ``POD_NAME`` (fallback ``HOSTNAME``) | required | the pod's name from the downward API; the ordinal is its trailing ``-<n>`` (``millrace-events-3`` → 3). Must be below ``MILLRACE_REPLICA_COUNT`` |
| ``MILLRACE_KAFKA_CONSUMER_<KEY>`` | none | librdkafka passthrough, millpond's ``KAFKA_CONSUMER_*``: ``<KEY>`` lowercased with ``_`` → ``.`` becomes the librdkafka key, the value passed verbatim, applied AFTER millrace's own keys (``MILLRACE_KAFKA_CONSUMER_SECURITY_PROTOCOL=SSL`` → ``security.protocol=SSL``; the motivating case is WarpStream zone pinning, ``MILLRACE_KAFKA_CONSUMER_CLIENT_ID=ws_az=use1-az1,millrace-events-0`` or ``..._CLIENT_RACK``). Keys millrace owns for correctness are REFUSED at boot (``bootstrap.servers``, ``group.id``, ``auto.offset.reset`` — each has its own knob — and ``enable.auto.commit``, ``enable.auto.offset.store``, ``partition.assignment.strategy``); an empty value is refused too |
| ``MILLRACE_KAFKA_AUTO_OFFSET_RESET`` | ``latest`` | where a partition with NO committed offset starts: ``latest`` (fleet policy — the head) or ``earliest`` (the log start). The tension is real and documented below the table |
| ``MILLRACE_KAFKA_MAX_PARTITION_FETCH_BYTES`` | 33554432 (32 MiB) | librdkafka ``max.partition.fetch.bytes`` — per-partition fetch response cap. The 1 MiB library default capped millpond's hot partition at ~50 MB/s (dozens of serialized fetch round trips per poll); the whale shape is ~55 MB/s, i.e. ~25 MB per hot partition per 500 ms poll, so one response must hold a poll's worth with headroom |
| ``MILLRACE_KAFKA_FETCH_MAX_BYTES`` | 268435456 (256 MiB) | librdkafka ``fetch.max.bytes`` — the per-fetch aggregate across the pod's partitions. Must be ≥ the per-partition cap (one hot partition's response must fit) and cover a poll's aggregate when several partitions run hot at once |
| ``MILLRACE_KAFKA_QUEUED_MAX_MESSAGES_KBYTES`` | 262144 (256 MiB) | librdkafka ``queued.max.messages.kbytes`` — the local fetch queue. Sized at ~5 s of a 50 MB/s pod inflow so a staging-latency spike doesn't stall fetches; past it librdkafka throttles fetches — the only fetch-side throttle there is (nothing pauses the consumer downstream; the staging gauges and alerts carry that job) |
| ``MILLRACE_CATALOG`` / ``MILLRACE_NAMESPACE`` / ``MILLRACE_TABLE`` | required | the one destination table's identity |
| ``MILLRACE_STAGE_URL`` | required | staging root: ``s3://bucket/millrace``, ``file:///path`` or ``memory:///`` (tests). One SlateDB instance per claimed partition lives at ``<base>/<topic>/<partition>`` |
| ``MILLRACE_TARGET_OUTPUT_BYTES`` | 500 MiB | planner size trigger — the estimated OUTPUT (parquet) size, compression roughly accounted (docs/kafka-ingestion.md §The flush planner) |
| ``MILLRACE_FLUSH_DEADLINE_S`` | 900 | planner age trigger — the tail's freshness SLA |
| ``MILLRACE_SLOW_LANE_DEADLINE_S`` | 21600 (6 h) | slow lane: flush any key this old regardless of size. Must be ≥ ``MILLRACE_FLUSH_DEADLINE_S`` (below it the age lane and ``MILLRACE_MIN_FLUSH_BYTES`` are dead letters). 6–24 h is the production shape; a dev table with a long tail of tiny keys can run it at minutes |
| ``MILLRACE_MIN_FLUSH_BYTES`` | 1 MiB | the age lane's minimum size — a smaller key at the age deadline waits for the slow lane |
| ``MILLRACE_MAX_FILES_PER_COMMIT`` | 512 | the most files one flush decision may fan out to under the live partition spec; a decision past it HALTS the pipeline (``fanout_exceeds_commit_bound``) before any upload — there is no commit chunking |
| ``MILLRACE_SLATEDB_GC_ENABLED`` | ``false`` | the writer-internal SlateDB GC loop. DEFAULT OFF because the external maintenance service (``python -m millrace.maintenance``) is deployed beside every millrace fleet and owns collection — doubled GC is wasted LIST/DELETE traffic on the replicas (docs/kafka-ingestion.md §Maintenance services; GC itself is idempotent and overlap-safe, so a mixed or misconfigured deployment is wasteful, never corrupt). Boot logs a loud warning when off: the setting ASSUMES the maintenance service exists — without it, garbage accumulates forever. There is deliberately NO knob for ``compactor_options``: disabling the embedded compactor stalls memtable→L0 flushes at ``l0_max_ssts`` with nothing to drain them (parity-pinned, tests/test_slatedb_parity.py) |
| ``MILLRACE_SLATEDB_MAX_UNFLUSHED_BYTES`` | 268435456 (256 MiB) | per-instance bound on unflushed memtable+WAL bytes before SlateDB stalls writes (the library default is 1 GiB — per INSTANCE, so 64 partitions per pod could hold 64 GiB of unflushed state; the README carries the per-pod memory arithmetic). The binding requires it to exceed ``l0_sst_size_bytes`` (never overridden; 64 MiB), which the validation enforces |
| ``MILLRACE_SLATEDB_L0_MAX_SSTS`` | 8 | L0 depth at which a writer's memtable flush stalls until the embedded compactor drains (SlateDB's default; the whale-shape stall margin is a bench topic — tests/test_stage_bench.py) |
| ``MILLRACE_CONSUME_BATCH_SIZE`` | 65536 | max messages per ``consume()`` call — the RECORD cap, a backstop for degenerate tiny-payload floods; at the design's ~1 KB record shape the byte cap below is the operative bound. Per-partition durability waits overlap within a poll (consumer.py), so a large batch still stages in ~one WAL round trip |
| ``MILLRACE_CONSUME_BATCH_MAX_BYTES`` | 268435456 (256 MiB) | per-poll byte bound, enforced by clamping the poll's message count to this ÷ the run's observed average record size (key + value bytes, cumulative; floored at 1 message so an oversized record still flows). The first poll after a start has no observation and is bounded by the record cap alone |
| ``MILLRACE_POLL_TIMEOUT_MS`` | 500 | per-poll block bound; also the stop() latency |
| ``MILLRACE_EVENT_TIME_POLICY`` | ``drop`` | flush-time junk ``event_time`` handling (``clamp`` or ``drop``; consumed by the flush phase — a dropped record is logged and counted, ``millrace_records_dropped_total{reason="event_time_out_of_window"}``) |
| ``MILLRACE_EVENT_TIME_MAX_PAST_S`` | 2592000 (30 d) | junk window lower bound: an ``event_time`` older than ``now − this`` is junk (flush phase) |
| ``MILLRACE_EVENT_TIME_MAX_FUTURE_S`` | 86400 (1 d) | junk window upper bound: an ``event_time`` newer than ``now + this`` is junk (flush phase) |
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


class AutoOffsetReset(Enum):
    """Where a partition with no committed offset starts consuming.

    ``LATEST`` (the default): the head — fleet policy since the millpond
    chart's 7-day-replay incident (a fresh group under ``earliest``
    replays the topic's whole retention window). ``EARLIEST``: the log
    start — no silent skip of existing data (under ``latest`` a
    partition with no committed offset never consumes its backlog:
    removal-by-omission, millpond AGENT.md), at the price of the
    whole-retention replay. Millrace's per-partition staged state makes
    a replay idempotent above the settled floor; below it re-consumed
    records publish under new keys — the boundary is the same under
    both settings (docs/kafka-ingestion.md §Delivery semantics), which
    is what makes the fleet default safe to follow here.
    """

    EARLIEST = "earliest"
    LATEST = "latest"


class EventTimePolicy(Enum):
    """What the flush does with outlier ``event_time`` values."""

    CLAMP = "clamp"
    DROP = "drop"


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

# Kafka topic names: https://kafka.apache.org/documentation/#topicnames
_TOPIC_NAME: Final = re.compile(r"^[a-zA-Z0-9._-]{1,249}$")
# hoglake identifier rules (millpond config.py's, kept in step).
_CATALOG_NAME: Final = re.compile(r"^[a-z][a-z0-9_-]{0,62}$")
_IDENTIFIER: Final = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]{0,127}$")
# S3 bucket names (millpond's _S3_URI bucket piece).
_S3_BUCKET: Final = re.compile(r"^[a-z0-9][a-z0-9.\-]{1,61}[a-z0-9]$")
# A StatefulSet pod name's trailing ordinal (millpond's _parse_ordinal).
_POD_ORDINAL: Final = re.compile(r"-(\d+)$")


#: The librdkafka passthrough prefix (module docstring).
_KAFKA_CONSUMER_PREFIX: Final = "MILLRACE_KAFKA_CONSUMER_"
#: librdkafka keys the passthrough may NOT set: millrace owns them for
#: correctness. The value names the dedicated knob where one exists;
#: None means there is deliberately no knob at all.
_OWNED_KAFKA_KEYS: Final[dict[str, str | None]] = {
    "bootstrap.servers": "MILLRACE_KAFKA_BOOTSTRAP_SERVERS",
    "group.id": "MILLRACE_KAFKA_GROUP_ID",
    "auto.offset.reset": "MILLRACE_KAFKA_AUTO_OFFSET_RESET",
    "enable.auto.commit": None,
    "enable.auto.offset.store": None,
    "partition.assignment.strategy": None,
}

#: Pre-three-lane knob names → their successors, REFUSED at boot (module
#: docstring): the rename narrowed the semantics, so there is no alias.
_RENAMED_ENV: Final = {
    "MILLRACE_TARGET_FILE_BYTES": "MILLRACE_TARGET_OUTPUT_BYTES",
    "MILLRACE_REAP_DEADLINE_S": "MILLRACE_SLOW_LANE_DEADLINE_S",
}


@dataclass(frozen=True)
class Config:
    """Validated startup configuration for one millrace pipeline.

    ``replica_count`` and ``ordinal`` (``0 <= ordinal < replica_count``)
    are the two halves of the static partition assignment: the consumer
    reads the topic's partition count from Kafka metadata once at
    startup and claims the partitions millpond's rule hands this
    ordinal (consumer.py ``compute_assignment``). ``stage_store_url`` /
    ``stage_base_path`` are the parsed halves of ``MILLRACE_STAGE_URL``,
    ready for :class:`millrace.stage.StageManager`: one SlateDB instance
    per claimed partition lives at ``{stage_base_path}/{topic}/{partition}``
    on the store, and staged state follows the partition to its new
    owner, never the pod.
    """

    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_group_id: str
    replica_count: int
    ordinal: int
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
    consume_batch_size: int
    poll_timeout_ms: int
    event_time_policy: EventTimePolicy
    metrics_port: int
    # Defaulted so existing constructors stay valid; load_config always
    # passes them explicitly.
    kafka_auto_offset_reset: AutoOffsetReset = AutoOffsetReset.LATEST
    consume_batch_max_bytes: int = CONSUME_BATCH_MAX_BYTES_DEFAULT
    kafka_max_partition_fetch_bytes: int = KAFKA_MAX_PARTITION_FETCH_BYTES_DEFAULT
    kafka_fetch_max_bytes: int = KAFKA_FETCH_MAX_BYTES_DEFAULT
    kafka_queued_max_messages_kbytes: int = KAFKA_QUEUED_MAX_MESSAGES_KBYTES_DEFAULT
    #: The ``MILLRACE_KAFKA_CONSUMER_*`` passthrough as sorted
    #: (librdkafka key, verbatim value) pairs, applied last by
    #: ``consumer.kafka_consumer_config``.
    kafka_consumer_overrides: tuple[tuple[str, str], ...] = ()
    # Flush-phase knobs (Phase 4).
    event_time_max_past_s: int = EVENT_TIME_MAX_PAST_S_DEFAULT
    event_time_max_future_s: int = EVENT_TIME_MAX_FUTURE_S_DEFAULT
    flush_sweep_s: int = FLUSH_SWEEP_S_DEFAULT
    flush_scan_min_bytes: int = FLUSH_SCAN_MIN_BYTES_DEFAULT
    flush_scan_max_rows: int = FLUSH_SCAN_MAX_ROWS_DEFAULT
    compression_ratio_halflife: int = COMPRESSION_RATIO_HALFLIFE_DEFAULT
    flush_concurrency: int = FLUSH_CONCURRENCY_DEFAULT
    # Per-instance SlateDB writer tuning (T9).
    slatedb: SlateDbConfig = SlateDbConfig()

    def planner_knobs(self) -> PlannerKnobs:
        """The planner's view of the flush-policy knobs."""
        return PlannerKnobs(
            target_output_bytes=self.target_output_bytes,
            flush_deadline_s=self.flush_deadline_s,
            slow_lane_deadline_s=self.slow_lane_deadline_s,
            min_flush_bytes=self.min_flush_bytes,
            max_files_per_commit=self.max_files_per_commit,
        )


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


def _parse_ordinal(pod_name: str) -> int:
    """The StatefulSet ordinal from a pod name: its trailing ``-<n>``
    (``millrace-events-3`` → 3). millpond's ``_parse_ordinal`` verbatim
    (millpond/config.py), so the two services read the same pod name
    the same way."""
    match = _POD_ORDINAL.search(pod_name)
    if not match:
        raise ValueError(f"cannot parse an ordinal from pod name {pod_name!r}")
    return int(match.group(1))


def _replica_and_ordinal(
    env: Mapping[str, str], problems: list[str]
) -> tuple[int | None, int | None]:
    """``MILLRACE_REPLICA_COUNT`` and the pod ordinal (``POD_NAME``,
    falling back to ``HOSTNAME`` — the downward API sets the former,
    Kubernetes the latter), each a problem on its own, plus the one
    relation between them: the ordinal names a replica that exists."""
    replica_count: int | None = None
    if _required(env, "MILLRACE_REPLICA_COUNT", problems) is not None:
        # Present, so the default is never used: _positive_int only
        # parses and refuses zero/negative/garbage.
        replica_count = _positive_int(env, "MILLRACE_REPLICA_COUNT", 1, problems)
    pod_var = "POD_NAME" if env.get("POD_NAME", "").strip() else "HOSTNAME"
    pod_name = env.get(pod_var, "").strip()
    # A refusal of the fallback must say so: the fix is almost always
    # "wire POD_NAME from the downward API", not "rename the host".
    fallback = (
        " (HOSTNAME was the fallback because POD_NAME is unset; set POD_NAME "
        "from the downward API, metadata.name)"
        if pod_var == "HOSTNAME"
        else ""
    )
    ordinal: int | None = None
    if not pod_name:
        problems.append(
            "POD_NAME (or its fallback HOSTNAME) is required and is not set: "
            "the replica's ordinal is the pod name's trailing -<n>, "
            "e.g. millrace-events-3; set POD_NAME from the downward API "
            "(metadata.name)"
        )
    else:
        try:
            ordinal = _parse_ordinal(pod_name)
        except ValueError:
            problems.append(
                f"{pod_var}={pod_name!r} carries no trailing -<n> ordinal "
                f"(expected a StatefulSet pod name, e.g. millrace-events-3)" + fallback
            )
    if ordinal is not None and replica_count is not None and ordinal >= replica_count:
        problems.append(
            f"{pod_var}={pod_name!r} ordinal {ordinal} >= "
            f"MILLRACE_REPLICA_COUNT={replica_count}: the ordinal must name "
            f"one of the replicas (0..{replica_count - 1})" + fallback
        )
        ordinal = None
    return replica_count, ordinal


def _kafka_consumer_overrides(
    env: Mapping[str, str], problems: list[str]
) -> tuple[tuple[str, str], ...]:
    """``MILLRACE_KAFKA_CONSUMER_<KEY>=value`` → ``(<key>, value)`` with
    ``<KEY>`` lowercased and ``_`` → ``.`` (millpond's ``KAFKA_CONSUMER_*``
    rule), sorted. A key millrace owns is a problem naming the knob
    that sets it (or saying there is none on purpose); an empty value
    is a problem (librdkafka would take the empty string literally)."""
    out: list[tuple[str, str]] = []
    for name in sorted(env):
        if not name.startswith(_KAFKA_CONSUMER_PREFIX):
            continue
        key = name[len(_KAFKA_CONSUMER_PREFIX) :].lower().replace("_", ".")
        value = env[name]
        if not key:
            problems.append(
                f"{name} names no librdkafka key (expected e.g. "
                f"{_KAFKA_CONSUMER_PREFIX}SECURITY_PROTOCOL)"
            )
            continue
        if key in _OWNED_KAFKA_KEYS:
            knob = _OWNED_KAFKA_KEYS[key]
            problems.append(
                f"{name} is refused: millrace owns librdkafka {key!r} — "
                + (
                    f"set {knob} instead"
                    if knob is not None
                    else "it is pinned for correctness (offsets move only "
                    "through the loop's explicit commits; there is no "
                    "group membership) and has no knob on purpose"
                )
            )
            continue
        if not value.strip():
            problems.append(f"{name} is empty (librdkafka {key!r} needs a value)")
            continue
        out.append((key, value))
    return tuple(out)


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

    replica_count, ordinal = _replica_and_ordinal(env, problems)
    kafka_consumer_overrides = _kafka_consumer_overrides(env, problems)

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
    # The three lanes are independent knobs (docs/kafka-ingestion.md
    # §The flush planner): size for the whales, deadline + minimum
    # bytes for the mediums, the slow lane for the tail. The one
    # relation that has to hold is slow ≥ flush deadline — a slow lane
    # below the age lane flushes every key at the shorter cadence and
    # the minimum-bytes floor never applies. No fixed range otherwise:
    # 6–24 h is the production shape, and a dev table whose tail is
    # most of its keys runs the slow lane at minutes.
    if (
        slow_lane_deadline_s is not None
        and flush_deadline_s is not None
        and slow_lane_deadline_s < flush_deadline_s
    ):
        problems.append(
            f"MILLRACE_SLOW_LANE_DEADLINE_S={slow_lane_deadline_s} must be >= "
            f"MILLRACE_FLUSH_DEADLINE_S={flush_deadline_s}: below the age "
            "lane's deadline the slow lane flushes every key at the shorter "
            "cadence and MILLRACE_MIN_FLUSH_BYTES never applies"
        )
        slow_lane_deadline_s = None
    min_flush_bytes = _positive_int(
        env, "MILLRACE_MIN_FLUSH_BYTES", MIN_FLUSH_BYTES_DEFAULT, problems
    )
    max_files_per_commit = _positive_int(
        env, "MILLRACE_MAX_FILES_PER_COMMIT", MAX_FILES_PER_COMMIT_DEFAULT, problems
    )

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
        EventTimePolicy.DROP,
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
    assert replica_count is not None
    assert ordinal is not None
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
    assert slatedb is not None
    assert consume_batch_size is not None
    assert consume_batch_max_bytes is not None
    assert poll_timeout_ms is not None
    assert isinstance(event_time_policy, EventTimePolicy)
    assert metrics_port is not None
    assert event_time_max_past_s is not None
    assert event_time_max_future_s is not None
    assert flush_sweep_s is not None
    assert flush_scan_min_bytes is not None
    assert flush_scan_max_rows is not None
    assert compression_ratio_halflife is not None
    assert flush_concurrency is not None
    return Config(
        kafka_bootstrap_servers=bootstrap,
        kafka_topic=topic,
        kafka_group_id=group_id,
        replica_count=replica_count,
        ordinal=ordinal,
        kafka_auto_offset_reset=auto_offset_reset,
        kafka_max_partition_fetch_bytes=max_partition_fetch_bytes,
        kafka_fetch_max_bytes=fetch_max_bytes,
        kafka_queued_max_messages_kbytes=queued_max_messages_kbytes,
        kafka_consumer_overrides=kafka_consumer_overrides,
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
        consume_batch_size=consume_batch_size,
        consume_batch_max_bytes=consume_batch_max_bytes,
        poll_timeout_ms=poll_timeout_ms,
        event_time_policy=event_time_policy,
        metrics_port=metrics_port,
        event_time_max_past_s=event_time_max_past_s,
        event_time_max_future_s=event_time_max_future_s,
        flush_sweep_s=flush_sweep_s,
        flush_scan_min_bytes=flush_scan_min_bytes,
        flush_scan_max_rows=flush_scan_max_rows,
        compression_ratio_halflife=compression_ratio_halflife,
        flush_concurrency=flush_concurrency,
        slatedb=slatedb,
    )
