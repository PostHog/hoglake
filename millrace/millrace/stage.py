"""Per-partition SlateDB staging (docs/kafka-ingestion.md §Staging layout).

ONE SlateDB instance per claimed Kafka partition, at an object-storage
path keyed by the partition (``s3://…/millrace/{topic}/{partition}``):
the consumer assignment *is* the claim, the instance *is* the partition,
and staged state follows the partition to its new owner, never the pod.
This module owns every byte written to or read from that instance — all
keys and values go through :mod:`millrace.keyspace` codecs; nothing here
hand-encodes a key.

Write path — :meth:`PartitionStage.stage_batch`: ONE write batch per
call: the row puts (keyed ``(group, offset)``), the per-tuple
``stats/`` merge and the ``sched_age/`` readiness entry land
atomically, so readiness accounting cannot drift from the data, not
even on a hard kill that lands between them. Records the consumer DROPPED (malformed, counted
and logged there) never reach this module. ``first_staged_ts`` bounds
the staging time of the group's OLDEST staged row from below: staging
never advances it (an existing stats entry keeps its value — the tail's
freshness SLA stands on this and the planner relies on it); it is born
when the group stands empty, and a settle that leaves rows behind lifts
it to the instant the settled window's scan started, when that scan saw
the group's whole staged slice (every row left behind was staged after
it). Without that lift a group under steady inflow never drains, its
``first_staged_ts`` stays at its birth, and it reads age-eligible on
every sweep after its first deadline.

Durability ordering: ``stage_batch`` awaits the batch's REMOTE
durability (the WAL on the object store, via the write handle's
``await_durable``) before its ack exists — Kafka offsets commit strictly
after durable staging (rows-then-offset, AGENT.md invariant 10 restated
for a staging area). The optional ``ack_hook`` is invoked after
durability and before return, so an observed hook call implies the batch
is recoverable, and a failed batch never invokes it. Note the binding's
shape: durability latency tracks SlateDB's ``flush_interval`` setting
(the WAL flush ticker), so ack latency sits at up to one interval above
the write itself.

Flush settlement — :meth:`PartitionStage.commit_flushed`: ONE write
batch: exact-key deletes of the window's rows, the stats subtraction
(or removal, with its ``sched_age/`` entry, when the tuple drains) and
the drop of the window's persisted ``prepared/`` request. A fresh flush
settles EXACTLY the row keys its scan read (hoglake#351): a row that
entered the window ``[first, last]`` after the scan (a replay below the
settled floor, a re-key) is not deleted and flushes in a later window —
no guard, nothing to disagree with. A recovered ``prepared/`` entry has
no keys in memory: the window's keys (``keyspace.window_rows_range``)
are pre-scanned, the count is checked against the entry's persisted
``row_count`` (a mismatch raises before anything is written), and
exactly the pre-scanned keys are deleted. NO scan runs under the write
lock: the lock covers the ``stats/`` point read and the batch write.
NO ``flushed/`` marker is
written: the settlement is atomic, so "marker landed, deletes didn't" —
the state a marker existed to repair — is unreachable, and the commit's
receipt is durable server-side (and in the caller's hand) before settle
is even called, so a marker could only ever restate it. A crash before
the settle leaves the ``prepared/`` entry, which recovery replays
receipt-first. Deletes and bookkeeping commit atomically, so a crash
lands them together or not at all.

Recovery — :meth:`PartitionStage.recover`: returns every
still-unpublished ``prepared/`` entry (the flusher's replay set) and
collects any LEGACY ``flushed/`` markers — written by builds whose
operators ran them; settled garbage by the argument above — in bounded
pages: one delete batch per page, the write lock taken PER PAGE, so a
partition that accumulated markers for days is collected without one
long lock hold (the consumer keeps staging between pages) and without a
per-marker transaction.

Concurrency: one writer thread of control per instance — an asyncio
lock serializes this process's mutations (the consume loop and the flush
loop share the stage). Across processes SlateDB's single-writer rule is
the guard; the binding-parity suite pins what a contested open actually
does on slatedb 0.17.x (the second OPEN succeeds; the older writer then
fails with ``Error.Closed(FENCED)`` no later than its next write's
durability wait, and nothing it wrote after the contest lands). Read
paths (stats and row scans, gauges) take no lock: scan iterators are
consistent snapshots as of creation (also pinned by the parity suite).
No scan runs while the lock is held — at object-store latency a scan
under it stalls staging and the consumer's offset commits behind it;
lock holders do point reads and one batch write.

Gauges — :meth:`PartitionStage.gauges`: derived from the ``stats/``
prefix, the same snapshot the flush planner reads, so the published
numbers can never drift from flush decisions — across restarts and
owner changes included. At the pod level the sweep's planner scan is
SHARED with the gauges (``StageManager.publish_gauges`` /
``StageManager.gauges(max_staleness_s=...)``): the ``/metrics`` scrape
serves the sweep's published snapshot while it is fresh and falls back
to a live scan when the sweep stops publishing — one ``stats/`` scan
per partition per sweep, not two independent ones (M6). The gauges are
ALERT INPUTS, never control flow: nothing here pauses the consumer
(there is no reason to stop consuming except on error — SlateDB on
object storage is the unbounded buffer, and pausing would move the
backlog into Kafka, whose retention is the only data-loss cliff). The
alertable backlog signal is the oldest ELIGIBLE staged key
(``ManagerGauges.oldest_eligible_staged_ts``), computed and published
by the flush sweep with the planner's own knobs and policy — a tiny
key waiting out its slow lane is not backlog and must not trip the
alert.

SlateDB metrics: every instance carries a ``DefaultMetricsRecorder``
(``PartitionStage.metrics_recorder``; one is created unless the caller
hands one in via ``metrics_recorder=``). The operational surface
(``millrace.slatedb_metrics``) snapshots the recorders at scrape time
and aggregates them ACROSS the pod's instances — never per-partition
series (cardinality; docs/kafka-ingestion.md §Deployment). The recorder
is passive by construction: SlateDB writes atomics, and nothing on the
write/flush path reads it back.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from collections.abc import AsyncIterator, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Final, Literal, Self, overload

from slatedb.uniffi import (
    CloseReason,
    Db,
    DbBuilder,
    DbCache,
    DefaultMetricsRecorder,
    DurabilityLevel,
    KeyRange,
    MokaCacheOptions,
    ObjectStore,
    ScanOptions,
    Settings,
    WriteBatch,
)
from slatedb.uniffi import Error as SlateError

from . import keyspace
from .config import (
    SLATEDB_BLOCK_CACHE_BYTES_DEFAULT,
    SLATEDB_META_CACHE_BYTES_DEFAULT,
    validate_slatedb_cache_bytes,
)
from .keyspace import (
    FlushedKey,
    PartitionTuple,
    PreparedKey,
    PreparedRequest,
    RowKey,
    SchedAgeKey,
    StatsValue,
)
from .planner import KeyStats

log = logging.getLogger(__name__)

_SCAN_CHUNK: Final = 1024
"""Rows pulled per ``DbIterator.next_batch`` round trip on scans."""

_BULK_SCAN_READ_AHEAD_BYTES: Final = 4 * 1024 * 1024
_BULK_SCAN_FETCH_TASKS: Final = 4


def _bulk_scan_options() -> ScanOptions:
    """Scan options for every range scan over row-sized or unbounded key
    ranges: the flush window (:meth:`PartitionStage._iter_range`), the
    recovery settlement's pre-scan, the reconcile emptiness probe (all
    outside the write lock) and recovery's legacy-marker sweep.

    SlateDB's default ScanOptions fetch ONE block (~4 KiB) per object-store
    GET with one fetch task, so a range that lives in L0/sorted-run SSTs
    reads at block_size / GET-latency (~50 KB/s at S3 latencies) — a
    window of a few MiB takes minutes (and, when the settlement scan ran
    under the write lock, stalled staging for as long). Read ahead in multi-MiB ranged GETs with a few concurrent
    fetches instead (the fetch never runs past the range's last block);
    blocks stay uncached (a one-pass scan would only churn the shared
    cache). The small, re-read ``stats/``/``prepared/`` prefix scans
    (:meth:`PartitionStage._scan_prefix`) keep the defaults, so their
    blocks stay cached across sweeps."""
    return ScanOptions(
        durability_filter=DurabilityLevel.MEMORY,
        dirty=False,
        read_ahead_bytes=_BULK_SCAN_READ_AHEAD_BYTES,
        cache_blocks=False,
        max_fetch_tasks=_BULK_SCAN_FETCH_TASKS,
    )


_RECOVERY_PAGE: Final = 1000
"""Legacy ``flushed/`` markers deleted per write batch in
:meth:`PartitionStage.recover` — bounded pages, one batch (and one
write-lock acquisition) per page, so collecting a partition's markers
never holds the lock across the whole sweep and never opens a
per-marker transaction."""

AckHook = Callable[["StageAck"], None]
"""Callback invoked once per successfully staged batch, after the batch
is durable and before ``stage_batch`` returns (the offset-commit seam —
the consumer's commit callback belongs here or later, never earlier)."""


class StageError(Exception):
    """Base for staging-layer failures."""


class StageClosedError(StageError):
    """An operation was attempted on a closed stage or manager."""


class SettlementMismatchError(StageError):
    """A recovered window's range held a different number of rows than
    its ``prepared/`` entry published (``commit_flushed(expected_rows=...)``).
    Raised by the pre-scan, BEFORE anything is written — no row,
    ``stats/`` entry or ``prepared/`` entry changed."""

    def __init__(self, key: FlushedKey, expected: int, found: int) -> None:
        self.key = key
        self.expected = expected
        self.found = found
        super().__init__(
            f"settling group {key.label} offsets {key.first_offset}-"
            f"{key.last_offset}: the window holds {found} staged rows but "
            f"the flush published {expected}; the settlement was not applied"
        )


class StageCorruptionError(StageError):
    """Persisted staging state violates a layout invariant (rows whose
    ``stats/`` entry is gone, stats that underflow a settled window).

    Never raised by a state this module can produce — every mutation is
    one atomic batch/transaction — so it means foreign writes or a bug;
    it is deliberately loud rather than clamped.
    """


@dataclass(frozen=True, slots=True)
class StagedRecord:
    """One consumed Kafka record to stage, under ``group`` — the
    destination's partition tuple for it (wire strings, one per live
    spec field; :class:`millrace.grouping.GroupKeyer`).

    Rows are keyed ``(group, offset)``: ``offset`` is the record's
    Kafka offset, validated by the keyspace codec at encode time. The
    Kafka record timestamp is not carried (the staging stats use the
    staging clock, ``stage_batch``'s ``now_us``).
    """

    group: PartitionTuple
    offset: int
    payload: bytes

    def __post_init__(self) -> None:
        if not isinstance(self.payload, bytes):
            raise TypeError(f"payload must be bytes, got {type(self.payload).__name__}")


@dataclass(frozen=True, slots=True)
class DroppedRecord:
    """One consumed Kafka record that can never become a staged row — a
    payload that is not a JSON object, a malformed partition source, a
    tombstone. No row is written for it: the
    consumer logs and counts the drop, and the offset commit that follows
    covers it (after the partition's staged batch, or its fence probe
    when the poll staged nothing there). ``reason`` is the consumer's bounded rejection
    vocabulary (``malformed_partition_source:<path>`` names the field).
    The flush drops records with the same shape; ``path`` is the dotted
    catalog path of the column that refused the value (a nested child's
    included, e.g. ``properties.inner.id``) when the decoder knows it —
    catalog names only, never a payload value — and the flush's drop log
    names it."""

    offset: int
    reason: str
    path: str | None = None


@dataclass(frozen=True, slots=True)
class StageAck:
    """What a successful :meth:`PartitionStage.stage_batch` stands for.

    The batch is remotely durable when this exists. ``first_offset`` /
    ``last_offset`` are the batch's offset bounds; the consumer commits
    the poll's last handled offset + 1 for the partition (dropped records
    included), strictly after receiving this.
    """

    topic: str
    partition: int
    rows: int
    staged_bytes: int
    first_offset: int
    last_offset: int


@dataclass(frozen=True, slots=True)
class BoundedScan:
    """:meth:`PartitionStage.scan_window`'s result: the rows (an exact
    offset prefix of the tuple's staged rows) and whether the scan saw
    the tuple's whole staged slice."""

    rows: list[tuple[RowKey, bytes]]
    complete: bool


@dataclass(frozen=True, slots=True)
class FlushCommitReport:
    """Accounting of one :meth:`PartitionStage.commit_flushed` call.

    ``group_drained`` is true when the settle removed the tuple's last
    staged row (its ``stats/`` and ``sched_age/`` entries went with it,
    so the next stage starts the tuple's ``first_staged_ts`` fresh).
    A replayed call reports zeros and ``group_drained=False``.
    """

    rows_deleted: int
    bytes_deleted: int
    group_drained: bool


@dataclass(frozen=True, slots=True)
class RecoveredPrepared:
    """One persisted ``prepared/`` entry — the byte-exact request and the
    key it sits under, for the flusher's replay logic."""

    key: PreparedKey
    request: PreparedRequest


@dataclass(frozen=True, slots=True)
class RecoveryReport:
    """What :meth:`PartitionStage.recover` found and did.

    ``pending_prepared``: every ``prepared/`` entry that remains —
    persisted before publication and never settled, so each is a replay
    candidate (reconciled receipt-first by the flusher, and republished
    verbatim on a receipt 404, whatever its age).
    ``legacy_markers_collected``: ``flushed/`` markers deleted. Nothing
    writes that prefix any more — a settlement is one atomic
    batch, so a marker was always fully-settled state with its
    receipt already in hand — and recovery removes leftover markers in
    bounded pages without decoding them.
    """

    pending_prepared: tuple[RecoveredPrepared, ...]
    legacy_markers_collected: int = 0


@dataclass(frozen=True, slots=True)
class StageGauges:
    """Staging gauges for one partition (docs/kafka-ingestion.md
    §Deployment: staged-bytes and oldest-staged-age are alert inputs,
    never control flow). ``oldest_first_staged_ts`` is None when
    nothing is staged."""

    staged_bytes: int
    staged_rows: int
    staged_groups: int
    oldest_first_staged_ts: int | None


def gauges_from_stats(stats: Iterable[KeyStats]) -> StageGauges:
    """One partition's gauges folded from a ``stats/`` snapshot — the
    SAME fold whether the snapshot was just scanned live or assembled
    by the flush sweep for sharing (``StageManager.publish_gauges``),
    so the two readers can never drift apart."""
    entries = list(stats)
    return StageGauges(
        staged_bytes=sum(s.staged_bytes for s in entries),
        staged_rows=sum(s.row_count for s in entries),
        staged_groups=len(entries),
        oldest_first_staged_ts=min((s.first_staged_ts for s in entries), default=None),
    )


def build_slatedb_settings(
    *, gc_enabled: bool, max_unflushed_bytes: int, l0_max_ssts: int
) -> Settings:
    """The SlateDB ``Settings`` every millrace writer opens its partition
    instances with (main.py wires the values from ``MILLRACE_SLATEDB_*``;
    config.py documents the knobs).

    Two knobs bound per-instance memory/stall shape:
    ``max_unflushed_bytes`` caps the memtable+WAL backlog before writes
    stall (the pod-level worst case is K instances × this value — the
    README carries the arithmetic), and ``l0_max_ssts`` is the L0 depth
    at which memtable flushes stall when the embedded compactor falls
    behind.

    ``compactor_options`` is deliberately NEVER touched: disabling the
    writer's embedded compactor stalls memtable→L0 flushes at
    ``l0_max_ssts`` with nothing left to drain them (pinned by
    tests/test_slatedb_parity.py on slatedb 0.17.x), and the Python
    binding exposes no external compactor runner. ``gc_enabled=False``
    (the default) sheds the writer-internal GC loop's LIST/DELETE
    traffic because the external maintenance service
    (millrace/maintenance.py, deployed beside every millrace fleet) owns
    collection — GC is idempotent and overlap-safe, so an operator
    flipping this back on is safe either way.
    """
    if max_unflushed_bytes < 1:
        raise ValueError(f"max_unflushed_bytes must be >= 1, got {max_unflushed_bytes}")
    if l0_max_ssts < 1:
        raise ValueError(f"l0_max_ssts must be >= 1, got {l0_max_ssts}")
    settings = Settings.default()
    settings.set("max_unflushed_bytes", str(max_unflushed_bytes))
    settings.set("l0_max_ssts", str(l0_max_ssts))
    if not gc_enabled:
        settings.set("garbage_collector_options", "null")
    return settings


def build_slatedb_cache(
    *, block_cache_bytes: int, meta_cache_bytes: int
) -> DbCache | None:
    """The ONE pod-level SST cache every partition instance shares
    (``MILLRACE_SLATEDB_BLOCK_CACHE_BYTES`` / ``..._META_CACHE_BYTES``;
    config.py's table), or ``None`` for the 0/0 "disabled" pair — the
    caller then opens each instance with ``with_db_cache_disabled()``.

    Shape: a split cache (data blocks in one half, index/filter/footer
    blocks in the other) of two moka caches. ``max_capacity`` is in
    BYTES — moka's weigher is the cached entry's byte size (slatedb
    0.17 ``db_cache/moka.rs``), so the pair bounds resident cache
    memory for the whole pod, not an entry count. The library's own
    default is the same split shape but PER INSTANCE (512 MiB + 128
    MiB), which is why K unconfigured partitions grow K × 640 MiB.

    A cache passed to ``DbBuilder.with_db_cache`` stays owned by the
    caller: ``Db.close`` never closes it, so one object serves every
    open/close over the manager's lifetime; ``db_cache_id`` keeps the
    instances' entries apart (:func:`db_cache_id_for`). No TTL/TTI —
    capacity eviction is the only policy, same as the library default.
    """
    validate_slatedb_cache_bytes(block_cache_bytes, meta_cache_bytes)
    if block_cache_bytes == 0:
        return None
    block = DbCache.new_moka_cache(
        MokaCacheOptions(
            max_capacity=block_cache_bytes, time_to_live=None, time_to_idle=None
        )
    )
    meta = DbCache.new_moka_cache(
        MokaCacheOptions(
            max_capacity=meta_cache_bytes, time_to_live=None, time_to_idle=None
        )
    )
    return DbCache.new_split_cache(block, meta)


def db_cache_id_for(topic: str, partition: int) -> int:
    """The ``db_cache_id`` one partition instance uses in the shared
    cache: the first 8 bytes of SHA-256 over ``"{topic}\\0{partition}"``
    as a u64. The binding wants the id unique among the instances
    sharing a cache and stable across reopens; one process consumes one
    topic, so the bare partition number would already be unique within
    a pod — hashing the pair costs nothing and stays unique if a
    process ever opens two topics' stages against one cache."""
    if partition < 0:
        raise ValueError(f"partition must be >= 0, got {partition}")
    digest = hashlib.sha256(f"{topic}\0{partition}".encode()).digest()
    return int.from_bytes(digest[:8], "big")


@dataclass(frozen=True, slots=True)
class _SettleDeletes:
    """One settlement's batch, built before the write lock is taken: the
    exact row-key deletes plus their accounting (``rows``, ``nbytes`` —
    the deleted values' sizes, what ``stats/`` subtracts). The
    bookkeeping writes join it under the lock."""

    batch: WriteBatch
    rows: int
    nbytes: int
    edges: tuple[bytes, bytes] | None = None
    """The first and last row keys of the ``rows`` form — the replay
    probe :meth:`PartitionStage.commit_flushed` point-reads under the
    lock. None for the window form (a pre-scan is its own probe)."""


class PartitionStage:
    """Owns ONE SlateDB instance staging ONE (topic, partition).

    Open via :meth:`open` (object-store URL) or :meth:`open_store`
    (pre-resolved store, for sharing one backend across handles — the
    :class:`StageManager` and ``memory:///`` tests, whose per-resolve
    stores are isolated). Opening a path another live writer holds is
    NOT refused by the binding; the older writer is fenced on its next
    flush/read (module docstring). Use :meth:`close` (or the async
    context manager) for a clean shutdown that flushes and releases the
    instance.
    """

    def __init__(
        self,
        db: Db,
        *,
        path: str,
        topic: str,
        partition: int,
        ack_hook: AckHook | None,
        metrics_recorder: DefaultMetricsRecorder,
    ) -> None:
        self._db = db
        self._path = path
        self._topic = topic
        self._partition = partition
        self._ack_hook = ack_hook
        self._metrics_recorder = metrics_recorder
        self._write_lock = asyncio.Lock()
        self._closed = False

    @overload
    @classmethod
    async def open(
        cls,
        object_store_url: str,
        path: str,
        *,
        topic: str,
        partition: int,
        settings: Settings | None = None,
        ack_hook: AckHook | None = None,
        metrics_recorder: DefaultMetricsRecorder | None = None,
        db_cache: DbCache,
        db_cache_id: int,
    ) -> PartitionStage: ...

    @overload
    @classmethod
    async def open(
        cls,
        object_store_url: str,
        path: str,
        *,
        topic: str,
        partition: int,
        settings: Settings | None = None,
        ack_hook: AckHook | None = None,
        metrics_recorder: DefaultMetricsRecorder | None = None,
        cache_disabled: Literal[True],
    ) -> PartitionStage: ...

    @classmethod
    async def open(
        cls,
        object_store_url: str,
        path: str,
        *,
        topic: str,
        partition: int,
        settings: Settings | None = None,
        ack_hook: AckHook | None = None,
        metrics_recorder: DefaultMetricsRecorder | None = None,
        db_cache: DbCache | None = None,
        db_cache_id: int | None = None,
        cache_disabled: bool = False,
    ) -> PartitionStage:
        """Open (or create) the partition's SlateDB at ``path`` on the
        store ``object_store_url`` resolves to (``memory:///``,
        ``file:///``, ``s3://bucket``).

        NOTE on ``memory:///``: each ``resolve`` is an isolated store —
        reopening "the same" memory path requires a shared store, which
        is what :meth:`open_store` is for.

        The SST cache choice is REQUIRED (:meth:`open_store`).
        """
        return await cls._open_store(
            ObjectStore.resolve(object_store_url),
            path,
            topic=topic,
            partition=partition,
            settings=settings,
            ack_hook=ack_hook,
            metrics_recorder=metrics_recorder,
            db_cache=db_cache,
            db_cache_id=db_cache_id,
            cache_disabled=cache_disabled,
        )

    @overload
    @classmethod
    async def open_store(
        cls,
        store: ObjectStore,
        path: str,
        *,
        topic: str,
        partition: int,
        settings: Settings | None = None,
        ack_hook: AckHook | None = None,
        metrics_recorder: DefaultMetricsRecorder | None = None,
        db_cache: DbCache,
        db_cache_id: int,
    ) -> PartitionStage: ...

    @overload
    @classmethod
    async def open_store(
        cls,
        store: ObjectStore,
        path: str,
        *,
        topic: str,
        partition: int,
        settings: Settings | None = None,
        ack_hook: AckHook | None = None,
        metrics_recorder: DefaultMetricsRecorder | None = None,
        cache_disabled: Literal[True],
    ) -> PartitionStage: ...

    @classmethod
    async def open_store(
        cls,
        store: ObjectStore,
        path: str,
        *,
        topic: str,
        partition: int,
        settings: Settings | None = None,
        ack_hook: AckHook | None = None,
        metrics_recorder: DefaultMetricsRecorder | None = None,
        db_cache: DbCache | None = None,
        db_cache_id: int | None = None,
        cache_disabled: bool = False,
    ) -> PartitionStage:
        """Like :meth:`open` but on a pre-resolved object store.

        ``metrics_recorder`` plugs into SlateDB's ``DbBuilder``: the DB
        records its internals into it, and the operational surface
        snapshots it at scrape time (module docstring). One is created
        per stage when not handed in, so a stage always HAS a recorder —
        the collector never has to know whether wiring happened.

        The SST cache choice is REQUIRED — exactly one of: ``db_cache``
        + ``db_cache_id`` (the pod-level cache this instance shares with
        every other, :func:`build_slatedb_cache`, scoped by
        :func:`db_cache_id_for`), or ``cache_disabled=True`` (the
        config's 0/0 pair). There is no third path: the binding's own
        default is a 512 MiB + 128 MiB split cache PER INSTANCE, the
        shape the pod-level knobs exist to replace, so it is never taken
        silently (the overloads pin this for mypy; the checks below for
        everyone else).
        """
        return await cls._open_store(
            store,
            path,
            topic=topic,
            partition=partition,
            settings=settings,
            ack_hook=ack_hook,
            metrics_recorder=metrics_recorder,
            db_cache=db_cache,
            db_cache_id=db_cache_id,
            cache_disabled=cache_disabled,
        )

    @classmethod
    async def _open_store(
        cls,
        store: ObjectStore,
        path: str,
        *,
        topic: str,
        partition: int,
        settings: Settings | None,
        ack_hook: AckHook | None,
        metrics_recorder: DefaultMetricsRecorder | None,
        db_cache: DbCache | None,
        db_cache_id: int | None,
        cache_disabled: bool,
    ) -> PartitionStage:
        if not topic:
            raise ValueError("topic must be non-empty")
        if partition < 0:
            raise ValueError(f"partition must be >= 0, got {partition}")
        if db_cache is not None and cache_disabled:
            raise ValueError("db_cache and cache_disabled are mutually exclusive")
        if db_cache is not None and db_cache_id is None:
            raise ValueError("db_cache_id is required with a shared db_cache")
        if db_cache is None and db_cache_id is not None:
            raise ValueError("db_cache_id is meaningless without a db_cache")
        if db_cache is None and not cache_disabled:
            raise ValueError(
                "an SST cache choice is required: db_cache + db_cache_id, or "
                "cache_disabled=True (the binding's per-instance default is "
                "never taken silently)"
            )
        recorder = (
            metrics_recorder
            if metrics_recorder is not None
            else DefaultMetricsRecorder()
        )
        builder = DbBuilder(path, store)
        builder.with_metrics_recorder(recorder)
        if settings is not None:
            builder.with_settings(settings)
        if db_cache is not None:
            assert db_cache_id is not None  # the checks above
            builder.with_db_cache(db_cache, db_cache_id)
        else:
            builder.with_db_cache_disabled()
        db = await builder.build()
        return cls(
            db,
            path=path,
            topic=topic,
            partition=partition,
            ack_hook=ack_hook,
            metrics_recorder=recorder,
        )

    @property
    def path(self) -> str:
        """The object-storage path this instance owns."""
        return self._path

    @property
    def topic(self) -> str:
        return self._topic

    @property
    def partition(self) -> int:
        return self._partition

    @property
    def metrics_recorder(self) -> DefaultMetricsRecorder:
        """The recorder this instance's SlateDB writes its internal
        counters/gauges/histograms into (wired at open; module
        docstring). Snapshotted at scrape time by
        :mod:`millrace.slatedb_metrics` — never read on any write or
        flush path."""
        return self._metrics_recorder

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    def _ensure_open(self) -> None:
        if self._closed:
            raise StageClosedError(f"stage {self._path} is closed")

    async def _scan_prefix(self, prefix: bytes) -> list[tuple[bytes, bytes]]:
        """Materialize a whole prefix scan (used for the bounded prefixes:
        ``stats/``, ``prepared/``)."""
        it = await self._db.scan_prefix(
            prefix,
            KeyRange(start=None, start_inclusive=False, end=None, end_inclusive=False),
        )
        out: list[tuple[bytes, bytes]] = []
        while True:
            chunk = await it.next_batch(_SCAN_CHUNK)
            if not chunk:
                return out
            out.extend((kv.key, kv.value) for kv in chunk)

    async def _iter_range(
        self, start: bytes, end: bytes
    ) -> AsyncIterator[tuple[bytes, bytes]]:
        """Stream the half-open key range ``[start, end)`` in key order."""
        it = await self._db.scan_with_options(
            KeyRange(start=start, start_inclusive=True, end=end, end_inclusive=False),
            _bulk_scan_options(),
        )
        while True:
            chunk = await it.next_batch(_SCAN_CHUNK)
            if not chunk:
                break
            for kv in chunk:
                yield kv.key, kv.value

    async def _read_stats(self, group: PartitionTuple) -> StatsValue | None:
        raw = await self._db.get(keyspace.stats_key(group))
        return keyspace.decode_stats_value(raw) if raw is not None else None

    # -- staging --------------------------------------------------------------

    async def stage_batch(
        self,
        records: Iterable[StagedRecord],
        *,
        now_us: int,
    ) -> StageAck:
        """Stage one consumed microbatch — ONE write batch, remotely durable
        before return.

        Per partition tuple in the batch: row puts, the ``stats/`` merge
        (``first_staged_ts`` kept from the entry that already exists —
        staging never advances it; created with ``now_us`` when the tuple
        stands empty) and
        the ``sched_age/`` entry on first stage. ``now_us`` is the
        injected staging clock — never read from the wall here.

        Rejects an empty batch and a batch carrying one row key twice
        (both are consumer bugs; redelivery ACROSS batches is fine — the
        row key is derived from the record, so the re-put is idempotent and only the stats counters inflate
        conservatively until the next flush). The ack hook fires after
        durability, never for a rejected or failed batch.
        """
        self._ensure_open()
        keyed: list[tuple[bytes, StagedRecord]] = []
        seen: set[bytes] = set()
        for rec in records:
            row_key = keyspace.encode_row_key(RowKey(rec.group, rec.offset))
            if row_key in seen:
                raise ValueError(
                    f"duplicate row key in batch: group {rec.group} offset {rec.offset}"
                )
            seen.add(row_key)
            keyed.append((row_key, rec))

        if not keyed:
            raise ValueError("stage_batch requires at least one record")

        by_group: dict[PartitionTuple, list[tuple[bytes, StagedRecord]]] = {}
        for row_key, rec in keyed:
            by_group.setdefault(rec.group, []).append((row_key, rec))

        total_bytes = 0
        async with self._write_lock:
            batch = WriteBatch()
            for group in sorted(by_group, key=keyspace.encode_group):
                rows = by_group[group]
                group_bytes = 0
                for row_key, rec in rows:
                    batch.put(row_key, rec.payload)
                    group_bytes += len(rec.payload)

                current = await self._read_stats(group)
                if current is None:
                    merged = StatsValue(
                        staged_bytes=group_bytes,
                        first_staged_ts=now_us,
                        last_staged_ts=now_us,
                        row_count=len(rows),
                    )
                    batch.put(
                        keyspace.encode_sched_age_key(SchedAgeKey(now_us, group)),
                        b"",
                    )
                else:
                    merged = StatsValue(
                        staged_bytes=current.staged_bytes + group_bytes,
                        # Arrivals never advance it (only a settle lifts it).
                        first_staged_ts=current.first_staged_ts,
                        last_staged_ts=now_us,
                        row_count=current.row_count + len(rows),
                    )
                batch.put(
                    keyspace.stats_key(group), keyspace.encode_stats_value(merged)
                )
                total_bytes += group_bytes

            handle = await self._db.write(batch)
            await handle.await_durable()

        all_offsets = [rec.offset for _, rec in keyed]
        ack = StageAck(
            topic=self._topic,
            partition=self._partition,
            rows=len(keyed),
            staged_bytes=total_bytes,
            first_offset=min(all_offsets),
            last_offset=max(all_offsets),
        )
        if self._ack_hook is not None:
            self._ack_hook(ack)
        return ack

    async def probe_fence(self, offset: int) -> None:
        """One durable no-op write for a poll that handled records on
        this partition but staged none (every record dropped): a single
        put of the fence-probe key (``keyspace.FENCE_PROBE_KEY``,
        holding ``offset``), awaited to remote durability exactly like
        :meth:`stage_batch`. A fenced writer gets SlateDB's
        ``Error.Closed(FENCED)`` here, BEFORE the consumer commits the
        poll's offsets against the new owner (pinned by
        tests/test_slatedb_parity.py). The key is not a row — no scan,
        stats entry or gauge reads the ``meta/`` prefix — and is
        overwritten by the next probe."""
        self._ensure_open()
        batch = WriteBatch()
        batch.put(keyspace.FENCE_PROBE_KEY, keyspace.encode_fence_probe_value(offset))
        async with self._write_lock:
            handle = await self._db.write(batch)
            await handle.await_durable()

    # -- flush inputs -----------------------------------------------------------

    async def iter_key_stats(self) -> list[KeyStats]:
        """The flush planner's input: every partition tuple's decoded
        ``stats/`` entry as a :class:`millrace.planner.KeyStats`, in key
        order."""
        self._ensure_open()
        out: list[KeyStats] = []
        for key, value in await self._scan_prefix(keyspace.STATS_PREFIX):
            stats = keyspace.decode_stats_value(value)
            out.append(
                KeyStats(
                    group=keyspace.decode_stats_key(key),
                    staged_bytes=stats.staged_bytes,
                    first_staged_ts=stats.first_staged_ts,
                    last_staged_ts=stats.last_staged_ts,
                    row_count=stats.row_count,
                )
            )
        return out

    async def read_group_stats(self, group: PartitionTuple) -> KeyStats | None:
        """Point-read one partition tuple's ``stats/`` entry as a
        :class:`millrace.planner.KeyStats` (None when the tuple has none).

        The age/slow readiness path's exact decision input
        (M6): the sweep enumerates age-eligible candidate keys from the
        ``sched_age/`` index and point-reads each candidate's stats,
        rather than trusting a shared scan's older snapshot for the
        deadline call."""
        self._ensure_open()
        stats = await self._read_stats(group)
        if stats is None:
            return None
        return KeyStats(
            group=group,
            staged_bytes=stats.staged_bytes,
            first_staged_ts=stats.first_staged_ts,
            last_staged_ts=stats.last_staged_ts,
            row_count=stats.row_count,
        )

    async def iter_sched_age_through(self, cutoff_ts: int) -> list[SchedAgeKey]:
        """The age-ordered readiness index scan: every ``sched_age/``
        entry with ``first_staged_ts <= cutoff_ts``, oldest first."""
        self._ensure_open()
        start, end = keyspace.sched_age_range_through(cutoff_ts)
        return [
            keyspace.decode_sched_age_key(k)
            async for k, _ in self._iter_range(start, end)
        ]

    def scan_group_rows(
        self,
        group: PartitionTuple,
        *,
        max_rows: int | None = None,
        max_bytes: int | None = None,
    ) -> AsyncIterator[tuple[RowKey, bytes]]:
        """Stream one partition tuple's staged rows in key order —
        offset order within the tuple — a flush's input.

        The optional caps bound the read (an offset prefix of the
        slice): iteration stops before exceeding either. The iterator
        is a consistent snapshot as of creation, so rows staged while
        it streams are not seen.
        """
        if max_rows is not None and max_rows < 1:
            raise ValueError(f"max_rows must be >= 1, got {max_rows}")
        if max_bytes is not None and max_bytes < 1:
            raise ValueError(f"max_bytes must be >= 1, got {max_bytes}")
        return self._scan_group_rows(group, max_rows=max_rows, max_bytes=max_bytes)

    async def _scan_group_rows(
        self, group: PartitionTuple, *, max_rows: int | None, max_bytes: int | None
    ) -> AsyncIterator[tuple[RowKey, bytes]]:
        self._ensure_open()
        start, end = keyspace.group_rows_range(group)
        rows = 0
        nbytes = 0
        async for key, value in self._iter_range(start, end):
            if max_rows is not None and rows >= max_rows:
                break
            if max_bytes is not None and nbytes + len(value) > max_bytes:
                break
            rows += 1
            nbytes += len(value)
            yield keyspace.decode_row_key(key), value

    async def scan_group_rows_bounded(
        self, group: PartitionTuple, *, max_rows: int, max_bytes: int
    ) -> list[tuple[RowKey, bytes]]:
        """:meth:`scan_window`'s rows alone."""
        return (
            await self.scan_window(group, max_rows=max_rows, max_bytes=max_bytes)
        ).rows

    async def scan_window(
        self, group: PartitionTuple, *, max_rows: int, max_bytes: int
    ) -> BoundedScan:
        """The flush's bounded scan, collected: one partition tuple's
        staged rows in offset order, capped in rows and bytes (the row
        that would exceed a cap is NOT included).

        Row keys sort by (group, offset), so the result is an exact
        offset PREFIX of the tuple's staged rows as of the scan's
        snapshot: every staged row of the tuple with an offset between
        the first and the last scanned one was scanned, capped or not.
        The flush window is therefore ``[first scanned, last scanned]``
        (flush.py §Bounded unit of work). ``complete`` is True when the
        scan reached the end of the tuple's slice — no cap cut it short —
        so every row of the tuple left after the window's settle was
        staged after the scan began.
        """
        if max_rows < 1:
            raise ValueError(f"max_rows must be >= 1, got {max_rows}")
        if max_bytes < 1:
            raise ValueError(f"max_bytes must be >= 1, got {max_bytes}")
        self._ensure_open()
        start, end = keyspace.group_rows_range(group)
        rows: list[tuple[RowKey, bytes]] = []
        nbytes = 0
        async for key, value in self._iter_range(start, end):
            if len(rows) >= max_rows or nbytes + len(value) > max_bytes:
                return BoundedScan(rows, complete=False)
            rows.append((keyspace.decode_row_key(key), value))
            nbytes += len(value)
        return BoundedScan(rows, complete=True)

    # -- prepared/ lifecycle ----------------------------------------------------

    async def persist_prepared(
        self, group: PartitionTuple, first_offset: int, request: PreparedRequest
    ) -> None:
        """Persist the COMPLETE prepared commit request BEFORE publication
        (durable before return). It is replayed byte-identically after an
        ambiguous response and never regenerated
        (docs/kafka-ingestion.md §The wire contract). The envelope must
        name the partition tuple its key sits under."""
        self._ensure_open()
        if request.group != group:
            raise ValueError(
                f"prepared request names partition tuple {request.group}, "
                f"but is persisted under {group}"
            )
        batch = WriteBatch()
        batch.put(
            keyspace.prepared_key(group, first_offset),
            keyspace.encode_prepared_value(request),
        )
        async with self._write_lock:
            handle = await self._db.write(batch)
            await handle.await_durable()

    async def get_prepared(
        self, group: PartitionTuple, first_offset: int
    ) -> PreparedRequest | None:
        """Point-read one persisted prepared request."""
        self._ensure_open()
        key = keyspace.prepared_key(group, first_offset)
        raw = await self._db.get(key)
        return self._decode_prepared(key, raw).request if raw is not None else None

    async def list_prepared(self, group: PartitionTuple) -> list[RecoveredPrepared]:
        """Every persisted ``prepared/`` entry of ONE partition tuple, in
        ``first_offset`` order (the key sorts numerically).

        The flush path's outstanding-entry gate: at most one entry per
        tuple exists by construction (a new request is persisted only
        when the tuple has none outstanding), so this is how the flush
        finds that entry REGARDLESS of where the current scan's window
        starts — a rewind that re-stages offsets below the entry's
        window moves the scan's first offset, and a missed entry would
        publish an overlapping window. The prefix holds at most the tuple's
        in-flight entries, so the scan is tiny."""
        self._ensure_open()
        start, end = keyspace.group_prepared_range(group)
        return [
            self._decode_prepared(key, value)
            async for key, value in self._iter_range(start, end)
        ]

    async def load_prepared(self) -> list[RecoveredPrepared]:
        """Every persisted ``prepared/`` entry, in (group, first_offset)
        order — the flusher's replay set."""
        self._ensure_open()
        return [
            self._decode_prepared(key, value)
            for key, value in await self._scan_prefix(keyspace.PREPARED_PREFIX)
        ]

    def _decode_prepared(self, key: bytes, value: bytes) -> RecoveredPrepared:
        """One ``prepared/`` entry, decoded — or, for an entry that does
        not decode (corruption), a :class:`StageCorruptionError` naming
        the partition and the raw key rather than a bare ``ValueError``."""
        try:
            return RecoveredPrepared(
                key=keyspace.decode_prepared_key(key),
                request=keyspace.decode_prepared_value(value),
            )
        except ValueError as e:
            raise StageCorruptionError(
                f"{self._topic}[{self._partition}]: the prepared/ entry at key "
                f"{bytes(key)!r} is corrupt: {e}"
            ) from e

    async def drop_prepared(self, group: PartitionTuple, first_offset: int) -> None:
        """Delete one ``prepared/`` entry on its own — for abandoning a
        refused request before a rebuild. Settled windows need no call:
        :meth:`commit_flushed` drops the entry in the settlement
        batch."""
        self._ensure_open()
        batch = WriteBatch()
        batch.delete(keyspace.prepared_key(group, first_offset))
        async with self._write_lock:
            handle = await self._db.write(batch)
            await handle.await_durable()

    async def _range_holds_a_key(self, start: bytes, end: bytes) -> bool:
        """True when ``[start, end)`` holds at least one key — a one-key
        probe with the bulk options, never run under the write lock."""
        it = await self._db.scan_with_options(
            KeyRange(start=start, start_inclusive=True, end=end, end_inclusive=False),
            _bulk_scan_options(),
        )
        return bool(await it.next_batch(1))

    async def reconcile_stats(self, group: PartitionTuple) -> bool:
        """Remove a partition tuple's ``stats/`` + ``sched_age/`` entries when its rows
        are gone — the redelivery-inflation residue.

        ``stage_batch`` is idempotent at the ROW level but its stats merge
        inflates counters on redelivery (documented there), and a flush
        settles ACTUALS: afterwards a tuple can hold ``row_count > 0`` with
        zero staged rows. Left alone the residue re-decides the key every
        sweep and — worse — its stale ``first_staged_ts`` pins the
        staged-age gauge forever. The flush path calls this when a
        decided key scans empty.

        The emptiness probe runs OUTSIDE the write lock (no scan ever
        holds it — module docstring). Race-freedom comes from the stats
        entry: it is read BEFORE the probe, and the delete commits only
        if the entry is byte-identical under the lock. Every
        ``stage_batch`` touching the tuple rewrites the entry with a
        larger ``row_count`` (and only this decision's flush settles the
        tuple), so an unchanged entry means no row landed after the
        probe's basis, and a row landing before it was seen by the probe.
        Returns True when it repaired."""
        self._ensure_open()
        stats_key = keyspace.stats_key(group)
        basis = await self._db.get(stats_key)
        if basis is None:
            return False
        if await self._range_holds_a_key(*keyspace.group_rows_range(group)):
            return False
        stats = keyspace.decode_stats_value(basis)
        batch = WriteBatch()
        batch.delete(stats_key)
        batch.delete(
            keyspace.encode_sched_age_key(SchedAgeKey(stats.first_staged_ts, group))
        )
        async with self._write_lock:
            if await self._db.get(stats_key) != basis:
                return False  # a batch staged into the tuple since the probe
            handle = await self._db.write(batch)
            await handle.await_durable()
        return True

    # -- flush settlement ---------------------------------------------------------

    async def commit_flushed(
        self,
        key: FlushedKey,
        *,
        rows: Sequence[tuple[RowKey, bytes]] | None = None,
        expected_rows: int | None = None,
        scanned_at_us: int | None = None,
    ) -> FlushCommitReport:
        """Settle one committed flush — ONE write batch: delete the
        window's rows by EXACT key, subtract (or remove, with the
        ``sched_age/`` entry, when drained) the tuple's ``stats/``, and
        drop the window's ``prepared/`` entry.

        Which keys (hoglake#351):

        - ``rows`` given (a fresh flush): exactly those rows — the ones
          the flush scanned, which ARE the published set by construction.
          A row that entered ``[first_offset, last_offset]`` after the
          scan (a replay or re-key) is not among them, survives, and is
          flushed by a later decision. No count guard: there is nothing
          to disagree with. Each row must be of the window's tuple and
          inside it, in strictly increasing offset order (a scan's shape).
        - ``rows`` None (a recovered ``prepared/`` entry, nothing in
          memory): every key in ``keyspace.window_rows_range``, read by a
          pre-scan. ``expected_rows`` (the entry's persisted
          ``row_count``) guards it: a different count raises
          :class:`SettlementMismatchError` before anything is written. A
          row arriving between the pre-scan and the write is not
          deleted.

        Every scan — the pre-scan — runs BEFORE the write lock is taken;
        under the lock only the ``stats/`` point read and the batch write
        run, so a settlement never stalls staging behind object-store
        reads.

        No ``flushed/`` marker is written. The caller holds the commit's
        receipt before calling (the server commits it in the same
        transaction as the snapshot), and this settle is atomic, so the
        two states a marker could distinguish — "delete pending" and
        "receipt unconfirmed" — cannot exist. A crash before this call
        leaves the ``prepared/`` entry; recovery replays it
        receipt-first (flush.py's recovery handoff).

        The same settlement retires a window that will NEVER be committed
        because the flush dropped every row of it (undecodable or junk
        ``event_time``): there is no receipt and nothing to reconcile, so
        the settle is a plain delete.

        ``scanned_at_us`` is the staging-clock instant taken just before
        the window's scan, given only when that scan saw the tuple's
        whole staged slice: every row left after this settle was then
        staged after it, so when rows remain the tuple's
        ``first_staged_ts`` is lifted to ``max(first_staged_ts,
        scanned_at_us)`` (never past ``last_staged_ts``) and its
        ``sched_age/`` entry is re-keyed, in the same batch. None (a
        capped scan, whose leftovers may predate it) keeps the old value
        — the flush-earlier direction.

        Idempotence: the window form is idempotent (a replay pre-scans
        no rows and reports zeros; with ``expected_rows`` it raises
        instead — the count no longer matches). The ``rows`` form
        point-reads the first and last given keys under the lock: both
        gone means the rows were already settled (a replayed call) — a
        WARNING, zeros reported, nothing but a still-present
        ``prepared/`` entry deleted, so a replay never subtracts the
        same rows twice and drains accounting from rows still staged;
        exactly one gone is :class:`StageCorruptionError`. Nothing in
        the flusher replays it (a failed settle is retried by a new
        decision with a new scan); the probe pins the contract.

        Lock hold time scales with the tombstones written (the batch
        write and its durability wait run under the lock) — far below
        the old range scan's object-store stall, but not constant.
        """
        self._ensure_open()
        if rows is not None:
            if expected_rows is not None:
                raise ValueError("expected_rows guards the window form only")
            if not rows:
                raise ValueError("an exact-key settle needs at least one row")
            deletes = await self._deletes_for_rows(key, rows)
        else:
            deletes = await self._deletes_for_window(key)
            if expected_rows is not None and deletes.rows != expected_rows:
                raise SettlementMismatchError(key, expected_rows, deletes.rows)
        async with self._write_lock:
            if deletes.edges is not None and not await self._edges_staged(
                key, deletes.edges
            ):
                await self._settle_replayed(key)
                return FlushCommitReport(
                    rows_deleted=0, bytes_deleted=0, group_drained=False
                )
            drained = await self._settle_stats(deletes, key, scanned_at_us)
            # Recovery's replay set is exactly what remains under
            # prepared/, so the entry leaves with the window.
            deletes.batch.delete(keyspace.prepared_key(key.group, key.first_offset))
            handle = await self._db.write(deletes.batch)
            await handle.await_durable()
        return FlushCommitReport(
            rows_deleted=deletes.rows,
            bytes_deleted=deletes.nbytes,
            group_drained=drained,
        )

    async def _edges_staged(self, key: FlushedKey, edges: tuple[bytes, bytes]) -> bool:
        """The ``rows`` form's replay probe (under the lock; point reads
        only): True when the first and last given rows are both staged,
        False when both are gone (already settled)."""
        first, last = edges
        first_staged = await self._db.get(first) is not None
        last_staged = (
            first_staged if last == first else await self._db.get(last) is not None
        )
        if first_staged == last_staged:
            return first_staged
        staged, gone = ("first", "last") if first_staged else ("last", "first")
        raise StageCorruptionError(
            f"{self._topic}[{self._partition}] group {key.label} window "
            f"[{key.first_offset}, {key.last_offset}]: the {staged} settled row "
            f"is still staged but the {gone} is gone — a partial settle no "
            "write path produces"
        )

    async def _settle_replayed(self, key: FlushedKey) -> None:
        """A replayed exact-key settle (its rows already gone): delete the
        window's ``prepared/`` entry if one still stands, touch nothing
        else (under the lock)."""
        log.warning(
            "%s[%d]: an exact-key settle of group %s offsets %d-%d found its "
            "rows already settled; replay ignored (stats untouched)",
            self._topic,
            self._partition,
            key.label,
            key.first_offset,
            key.last_offset,
        )
        prepared = keyspace.prepared_key(key.group, key.first_offset)
        if await self._db.get(prepared) is not None:
            batch = WriteBatch()
            batch.delete(prepared)
            handle = await self._db.write(batch)
            await handle.await_durable()

    async def _deletes_for_rows(
        self, key: FlushedKey, rows: Sequence[tuple[RowKey, bytes]]
    ) -> _SettleDeletes:
        """The exact-key deletes of a fresh flush's scanned rows (no I/O;
        yields to the loop every ``_SCAN_CHUNK`` rows)."""
        batch = WriteBatch()
        nbytes = 0
        previous: int | None = None
        for i, (row, payload) in enumerate(rows):
            if row.group != key.group or not keyspace.row_in_flushed_range(row, key):
                raise ValueError(
                    f"row {row} is outside the settled window {key.label} "
                    f"[{key.first_offset}, {key.last_offset}]"
                )
            if previous is not None and row.offset <= previous:
                raise ValueError(
                    f"settled rows must be in strictly increasing offset "
                    f"order: {row.offset} after {previous}"
                )
            previous = row.offset
            batch.delete(keyspace.encode_row_key(row))
            nbytes += len(payload)
            if i % _SCAN_CHUNK == _SCAN_CHUNK - 1:
                await asyncio.sleep(0)
        return _SettleDeletes(
            batch,
            len(rows),
            nbytes,
            (
                keyspace.encode_row_key(rows[0][0]),
                keyspace.encode_row_key(rows[-1][0]),
            ),
        )

    async def _deletes_for_window(self, key: FlushedKey) -> _SettleDeletes:
        """The exact-key deletes of every row now in the window (keyspace.py
        §The settlement window: one contiguous byte range of the tuple's
        slice) — a bulk pre-scan, outside the write lock."""
        start, end = keyspace.window_rows_range(key)
        batch = WriteBatch()
        rows = 0
        nbytes = 0
        async for row_key, value in self._iter_range(start, end):
            batch.delete(row_key)
            rows += 1
            nbytes += len(value)
        return _SettleDeletes(batch, rows, nbytes)

    async def _settle_stats(
        self, deletes: _SettleDeletes, key: FlushedKey, scanned_at_us: int | None
    ) -> bool:
        """Add the ``stats/`` update to the settlement batch: subtract the
        deleted rows, or remove the entry (with its ``sched_age/`` entry)
        when the tuple drained. While rows remain, ``first_staged_ts`` is
        lifted to the scan's start (``scanned_at_us``, when the scan was
        complete — every survivor was staged after it) and the
        ``sched_age/`` entry re-keyed; without it the older value stays,
        which can only flush the survivors EARLIER than their own
        deadline — the freshness SLA's safe direction. The lift never
        passes ``last_staged_ts`` (a batch whose clock was read before
        the scan began but which landed after the scan's snapshot stays
        bounded by its own stamp). Runs under the write lock: the point
        read sees every batch staged before the settle commits."""
        batch, rows_deleted, bytes_deleted = deletes.batch, deletes.rows, deletes.nbytes
        current = await self._read_stats(key.group)
        if current is None:
            if rows_deleted:
                raise StageCorruptionError(
                    f"group {key.label}: {rows_deleted} staged rows in the "
                    "settled window but no stats entry"
                )
            return False
        if rows_deleted > current.row_count or bytes_deleted > current.staged_bytes:
            raise StageCorruptionError(
                f"group {key.label}: settling {rows_deleted} rows / "
                f"{bytes_deleted} bytes against stats holding "
                f"{current.row_count} rows / {current.staged_bytes} bytes"
            )
        if rows_deleted == 0:
            return False  # idempotent replay: the landing commit already settled
        remaining = current.row_count - rows_deleted
        if remaining == 0:
            batch.delete(keyspace.stats_key(key.group))
            batch.delete(
                keyspace.encode_sched_age_key(
                    SchedAgeKey(current.first_staged_ts, key.group)
                )
            )
            return True
        first = current.first_staged_ts
        if scanned_at_us is not None:
            first = min(max(first, scanned_at_us), current.last_staged_ts)
        if first != current.first_staged_ts:
            batch.delete(
                keyspace.encode_sched_age_key(
                    SchedAgeKey(current.first_staged_ts, key.group)
                )
            )
            batch.put(keyspace.encode_sched_age_key(SchedAgeKey(first, key.group)), b"")
        batch.put(
            keyspace.stats_key(key.group),
            keyspace.encode_stats_value(
                StatsValue(
                    staged_bytes=current.staged_bytes - bytes_deleted,
                    first_staged_ts=first,
                    last_staged_ts=current.last_staged_ts,
                    row_count=remaining,
                )
            ),
        )
        return False

    # -- recovery ---------------------------------------------------------------

    async def recover(self) -> RecoveryReport:
        """Bring a (re)opened instance to its crash-safe boundary.

        Two bounded jobs:

        1. Collect LEGACY ``flushed/`` markers. Nothing writes that
           prefix any more (settlement is one atomic batch, so a
           marker was always settled state with its receipt already in
           hand — never a repair instruction). Collection is a paged
           delete: one write batch per ``_RECOVERY_PAGE`` keys, the write
           lock taken per page, NO per-marker transaction and no
           group-slice scan — a partition that accumulated markers for
           days is collected without stalling staging.
        2. Return every surviving ``prepared/`` entry — the ambiguity
           set the flusher reconciles receipt-first (flush.py).

        Safe to run any number of times; a clean instance costs one
        empty page-read and the ``prepared/`` scan.
        """
        self._ensure_open()
        collected = 0
        start, end = keyspace.flushed_range()
        it = await self._db.scan_with_options(
            KeyRange(start=start, start_inclusive=True, end=end, end_inclusive=False),
            _bulk_scan_options(),
        )
        while True:
            chunk = await it.next_batch(_RECOVERY_PAGE)
            if not chunk:
                break
            batch = WriteBatch()
            for kv in chunk:
                batch.delete(kv.key)
            async with self._write_lock:
                handle = await self._db.write(batch)
                await handle.await_durable()
            collected += len(chunk)
        pending = await self.load_prepared()
        return RecoveryReport(
            pending_prepared=tuple(pending), legacy_markers_collected=collected
        )

    # -- gauges and lifecycle ---------------------------------------------------

    async def gauges(self) -> StageGauges:
        """Staging gauges derived from the ``stats/`` prefix — the
        same snapshot the planner reads, so gauges cannot drift from
        flush decisions (restart-safe by construction)."""
        return gauges_from_stats(await self.iter_key_stats())

    async def close(self) -> None:
        """Flush and shut the instance down. Idempotent. A fenced writer
        has already yielded the path; SlateDB completes shutdown before
        reporting the fence. Other shutdown failures propagate."""
        if self._closed:
            return
        self._closed = True
        try:
            await self._db.shutdown()
        except SlateError.Closed as exc:
            if exc.reason != CloseReason.FENCED:
                raise


@dataclass(frozen=True, slots=True)
class OpenedPartition:
    """A freshly opened partition stage plus its recovery report."""

    stage: PartitionStage
    recovery: RecoveryReport


@dataclass(frozen=True, slots=True)
class AssignmentSync:
    """The diff one :meth:`StageManager.sync_assignment` applied."""

    opened: Mapping[tuple[str, int], OpenedPartition]
    kept: tuple[tuple[str, int], ...]
    closed: tuple[tuple[str, int], ...]


@dataclass(frozen=True, slots=True)
class ManagerGauges:
    """Staging gauges aggregated over the manager's open partitions.

    ``oldest_eligible_staged_ts`` is the alertable backlog signal: the
    oldest ``first_staged_ts`` among keys the flush policy found
    ELIGIBLE (size, age or slow lane) at the sweep that published the
    snapshot — computed by the FlushRunner with the planner's own knobs
    and ratio, because the stage layer has neither. It is None when
    nothing was eligible and ALWAYS None on a live fallback scan (a
    scan has no policy input), so a halted sweep's eligible-age series
    goes absent rather than silently frozen — the halt itself is the
    louder signal (/healthz)."""

    partitions: int
    staged_bytes: int
    staged_rows: int
    staged_groups: int
    oldest_first_staged_ts: int | None
    oldest_eligible_staged_ts: int | None
    per_partition: Mapping[tuple[str, int], StageGauges]


class StageManager:
    """Opens/closes :class:`PartitionStage`\\ s for the claimed partition
    set — one SlateDB instance per (topic, partition), at
    ``{base_path}/{topic}/{partition}`` on ONE shared object store.

    The claim follows the Kafka consumer assignment; assignments may
    change between deploys, so a manager must open an existing path and
    recover cleanly (``open_partition`` always runs
    :meth:`PartitionStage.recover` and reports what it found). One store
    resolution per manager: ``memory:///`` resolves to an isolated store
    per call, so a single manager is also how tests reopen memory-backed
    partitions coherently.

    Shared gauges (M6): the flush sweep's per-partition planner scan is
    the one ``stats/`` scan the pod needs per sweep iteration —
    :meth:`publish_gauges` stores the sweep's fold (stamped with the
    injected ``monotonic``), and :meth:`gauges` serves it to callers
    passing ``max_staleness_s`` while it is fresh, falling back to a
    live scan when the sweep stops publishing — a halted or slow flush
    loop therefore degrades to the old per-reader scans, never to
    silently frozen gauges. A live fallback scan is NOT
    itself published: a reader in fallback needs a fresh answer on
    every call.

    One SST cache per manager (= per pod): built ONCE at construction
    from the ``block_cache_bytes`` / ``meta_cache_bytes`` pair
    (:func:`build_slatedb_cache`; defaults are config.py's) and handed
    to every instance this manager opens with
    ``db_cache_id_for(topic, partition)`` as its scope — the 0/0 pair
    opens every instance with the cache disabled instead. The library
    default would be one 640 MiB split cache PER INSTANCE. A revoked
    partition's entries stay in the shared cache until capacity
    eviction pushes them out — bounded by the pair, and harmless (no
    other instance's id can address them; a reclaim reuses its id).
    """

    def __init__(
        self,
        object_store_url: str,
        base_path: str,
        *,
        settings: Settings | None = None,
        ack_hook: AckHook | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        block_cache_bytes: int = SLATEDB_BLOCK_CACHE_BYTES_DEFAULT,
        meta_cache_bytes: int = SLATEDB_META_CACHE_BYTES_DEFAULT,
    ) -> None:
        if not base_path.strip("/"):
            raise ValueError("base_path must be non-empty")
        self._store = ObjectStore.resolve(object_store_url)
        self._base_path = base_path.strip("/")
        self._settings = settings
        self._ack_hook = ack_hook
        self._db_cache = build_slatedb_cache(
            block_cache_bytes=block_cache_bytes, meta_cache_bytes=meta_cache_bytes
        )
        self._cache_capacity = (block_cache_bytes, meta_cache_bytes)
        self._monotonic = monotonic
        self._stages: dict[tuple[str, int], PartitionStage] = {}
        self._published: tuple[float, ManagerGauges] | None = None
        self._closed = False

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    @property
    def store(self) -> ObjectStore:
        return self._store

    @property
    def db_cache(self) -> DbCache | None:
        """The pod-level SST cache every open instance shares — ``None``
        when the manager opens instances with the cache disabled."""
        return self._db_cache

    @property
    def cache_capacity_bytes(self) -> tuple[int, int]:
        """The shared cache's configured ``(block, meta)`` capacities in
        bytes — ``(0, 0)`` when disabled. Configuration, not occupancy:
        the binding exposes no cache size."""
        return self._cache_capacity

    def path_for(self, topic: str, partition: int) -> str:
        """The SlateDB path of one claimed partition."""
        return f"{self._base_path}/{topic}/{partition}"

    def partitions(self) -> tuple[tuple[str, int], ...]:
        """The currently open (topic, partition) set."""
        return tuple(sorted(self._stages))

    def stage(self, topic: str, partition: int) -> PartitionStage:
        """The open stage for one partition (KeyError if not open)."""
        return self._stages[(topic, partition)]

    async def open_partition(self, topic: str, partition: int) -> OpenedPartition:
        """Open (or create) one partition's stage and recover it —
        the deploy-time "assignment changed" path included: an existing
        DB at the partition's path is opened as-is."""
        if self._closed:
            raise StageClosedError("stage manager is closed")
        key = (topic, partition)
        if key in self._stages:
            raise StageError(f"partition already open: {key}")
        path = self.path_for(topic, partition)
        if self._db_cache is not None:
            stage = await PartitionStage.open_store(
                self._store,
                path,
                topic=topic,
                partition=partition,
                settings=self._settings,
                ack_hook=self._ack_hook,
                db_cache=self._db_cache,
                db_cache_id=db_cache_id_for(topic, partition),
            )
        else:
            stage = await PartitionStage.open_store(
                self._store,
                path,
                topic=topic,
                partition=partition,
                settings=self._settings,
                ack_hook=self._ack_hook,
                cache_disabled=True,
            )
        self._stages[key] = stage
        try:
            recovery = await stage.recover()
        except BaseException:
            self._stages.pop(key)
            await stage.close()
            raise
        return OpenedPartition(stage=stage, recovery=recovery)

    async def close_partition(self, topic: str, partition: int) -> None:
        """Close one partition's stage (a revocation: flush nothing,
        abandon nothing — staged state stays on the store for the new
        owner)."""
        stage = self._stages.pop((topic, partition))
        await stage.close()

    async def sync_assignment(
        self, claimed: Iterable[tuple[str, int]]
    ) -> AssignmentSync:
        """Reconcile the open set with the consumer assignment: close
        revoked partitions, open (and recover) newly claimed ones."""
        if self._closed:
            raise StageClosedError("stage manager is closed")
        want = set(claimed)
        current = set(self._stages)
        closed: list[tuple[str, int]] = []
        for topic, partition in sorted(current - want):
            await self.close_partition(topic, partition)
            closed.append((topic, partition))
        opened: dict[tuple[str, int], OpenedPartition] = {}
        for topic, partition in sorted(want - current):
            opened[(topic, partition)] = await self.open_partition(topic, partition)
        return AssignmentSync(
            opened=opened, kept=tuple(sorted(current & want)), closed=tuple(closed)
        )

    @staticmethod
    def _aggregate_gauges(
        per_partition: Mapping[tuple[str, int], StageGauges],
        *,
        oldest_eligible_staged_ts: int | None = None,
    ) -> ManagerGauges:
        """The pod-wide fold of per-partition gauges (one code path for
        live scans and the sweep's published snapshot). Eligibility is
        the flush sweep's computation: a live scan passes None."""
        return ManagerGauges(
            partitions=len(per_partition),
            staged_bytes=sum(g.staged_bytes for g in per_partition.values()),
            staged_rows=sum(g.staged_rows for g in per_partition.values()),
            staged_groups=sum(g.staged_groups for g in per_partition.values()),
            oldest_first_staged_ts=min(
                (
                    g.oldest_first_staged_ts
                    for g in per_partition.values()
                    if g.oldest_first_staged_ts is not None
                ),
                default=None,
            ),
            oldest_eligible_staged_ts=oldest_eligible_staged_ts,
            per_partition=per_partition,
        )

    def publish_gauges(
        self,
        per_partition: Mapping[tuple[str, int], StageGauges],
        *,
        oldest_eligible_staged_ts: int | None = None,
    ) -> None:
        """Publish the flush sweep's gauges — folded from the SAME
        ``stats/`` scans the planner decided from — as the pod's shared
        snapshot (M6: one scan per partition per sweep, shared by the
        planner and the metrics scrape).

        ``oldest_eligible_staged_ts`` is the sweep's oldest eligible
        staged key (the alertable backlog signal behind main.py's
        ``millrace_oldest_eligible_staged_age_seconds``); the runner
        computes it with the planner's own knobs and ratio.
        """
        self._published = (
            self._monotonic(),
            self._aggregate_gauges(
                per_partition, oldest_eligible_staged_ts=oldest_eligible_staged_ts
            ),
        )

    async def gauges(self, *, max_staleness_s: float | None = None) -> ManagerGauges:
        """Aggregate the open partitions' gauges.

        With ``max_staleness_s``, serves the sweep-published snapshot
        while it is fresh (age at most ``max_staleness_s``); otherwise
        scans live. The fallback is the safety property: a flush loop
        that stops publishing degrades the readers to their own scans,
        never to frozen numbers. A LIVE scan is deliberately NOT
        published: a reader falling back (e.g. the metrics scrape with
        a halted flusher) needs a fresh answer on every call, and
        re-serving its own scan would freeze exactly the gauges that
        reader exists to keep honest. A live scan carries no
        ``oldest_eligible_staged_ts`` — eligibility is the sweep's
        computation (the planner knobs and the ratio live on the
        runner), so a fallback read reports None rather than guess.
        """
        published = self._published
        if (
            max_staleness_s is not None
            and published is not None
            and self._monotonic() - published[0] <= max_staleness_s
        ):
            return published[1]
        per_partition: dict[tuple[str, int], StageGauges] = {}
        for key in sorted(self._stages):
            per_partition[key] = await self._stages[key].gauges()
        return self._aggregate_gauges(per_partition)

    def metrics_recorders(self) -> Mapping[tuple[str, int], DefaultMetricsRecorder]:
        """The open partitions' SlateDB metrics recorders (wired into
        each DB at open). The metrics bridge snapshots them at scrape
        time and aggregates across instances — never per-partition
        series (docs/kafka-ingestion.md §Deployment)."""
        return {key: stage.metrics_recorder for key, stage in self._stages.items()}

    async def close(self) -> None:
        """Close every open stage. Idempotent."""
        if self._closed:
            return
        self._closed = True
        for key in sorted(self._stages):
            await self._stages[key].close()
        self._stages.clear()
