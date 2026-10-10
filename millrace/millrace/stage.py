"""Per-partition SlateDB staging (docs/kafka-ingestion.md §Staging layout).

ONE SlateDB instance per claimed Kafka partition, at an object-storage
path keyed by the partition (``s3://…/millrace/{topic}/{partition}``):
the consumer assignment *is* the claim, the instance *is* the partition,
and staged state follows the partition to its new owner, never the pod.
This module owns every byte written to or read from that instance — all
keys and values go through :mod:`millrace.keyspace` codecs; nothing here
hand-encodes a key.

Write path — :meth:`PartitionStage.stage_batch`: ONE write batch per
call: the row puts, the per-team ``stats/`` merge, the ``sched_age/``
readiness entry, the ``offsets/`` identity range and any ``poison/``
quarantine entries land atomically, so readiness accounting cannot drift
from the data, not even on a hard kill that lands between them, and the
offset commit that follows covers quarantined records exactly like
staged rows. ``first_staged_ts`` is SET-ONCE: an existing stats entry
keeps its value (the tail's freshness SLA stands on this and the planner
relies on it); it is born again only after the key stood empty — i.e.
after a flush drained the team's last staged row.

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

Flush settlement — :meth:`PartitionStage.commit_flushed`: ONE
transaction: the delete of EXACTLY the flush's offset window
(``keyspace.row_in_flushed_range`` selects within the team's whole
slice — younger and older rows of the same team outside the window
survive), the stats subtraction (or removal, with its ``sched_age/``
entry, when the team drains), the ``offsets/`` pruning and the drop of
the range's persisted ``prepared/`` request. NO ``flushed/`` marker is
written: the settlement is atomic, so "marker landed, deletes didn't" —
the state a marker existed to repair — is unreachable, and the commit's
receipt is durable server-side (and in the caller's hand) before settle
is even called, so a marker could only ever restate it. A crash before
the settle leaves the ``prepared/`` entry, which recovery replays
receipt-first. Deletes and bookkeeping commit atomically, so a crash
lands them together or not at all, and a replayed call is a no-op.

Recovery — :meth:`PartitionStage.recover`: returns every
still-unpublished ``prepared/`` entry (the flusher's replay set) and
collects any LEGACY ``flushed/`` markers — written by builds whose
operators ran them; settled garbage by the argument above — in bounded
pages: one delete batch per page, the write lock taken PER PAGE, so a
partition that accumulated markers for days is collected without one
long lock hold (the consumer keeps staging between pages) and without a
per-marker transaction.

Retention — ``poison/`` is forensic state with a bounded lifetime:
every entry carries the staging-clock stamp of its write (v2 envelope),
and :meth:`PartitionStage.purge_poison_expired` deletes entries older
than a caller-chosen cutoff in bounded batches (the periodic sweeper
lives in main.py; nothing depends on poison presence, so a process
that is down at expiry simply purges later). Envelopes without a stamp
(v1) are kept forever — unknown age is never deleted.

Concurrency: one writer thread of control per instance — an asyncio
lock serializes this process's mutations (the consume loop and the flush
loop share the stage). Across processes SlateDB's single-writer rule is
the guard; the binding-parity suite pins what a contested open actually
does on slatedb 0.17.x (the second OPEN succeeds; the older writer then
fails with ``Error.Closed(FENCED)`` no later than its next write's
durability wait, and nothing it wrote after the contest lands). Read
paths (stats and row scans, gauges) take no lock: scan iterators are
consistent snapshots as of creation (also pinned by the parity suite).

Gauges — :meth:`PartitionStage.gauges`: derived from the ``stats/``
prefix, the same snapshot the flush planner reads, so backpressure
numbers can never drift from flush decisions — across restarts and
owner changes included. At the pod level the sweep's planner scan is
SHARED with the gauges (``StageManager.publish_gauges`` /
``StageManager.gauges(max_staleness_s=...)``): the consumer's
per-poll backpressure read and the ``/metrics`` scrape serve the
sweep's published snapshot while it is fresh and fall back to a live
scan when the sweep stops publishing — one ``stats/`` scan per
partition per sweep, not three independent ones (M6).

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
import logging
import time
from collections.abc import AsyncIterator, Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Final, Self

from slatedb.uniffi import (
    Db,
    DbBuilder,
    DbTransaction,
    DefaultMetricsRecorder,
    IsolationLevel,
    KeyRange,
    ObjectStore,
    Settings,
    WriteBatch,
)

from . import keyspace
from .keyspace import (
    FlushedKey,
    OffsetRange,
    PoisonValue,
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

_RECOVERY_PAGE: Final = 1000
"""Legacy ``flushed/`` markers deleted per write batch in
:meth:`PartitionStage.recover` — bounded pages, one batch (and one
write-lock acquisition) per page, so collecting a partition's markers
never holds the lock across the whole sweep and never opens a
per-marker transaction."""

_POISON_WRITE_CHUNK: Final = 1000
"""Poison entries per durable write batch in :meth:`PartitionStage.poison_records`
(mirrors the consumer's per-poll-cycle bound: a batch is at most a poll's
worth of capped quarantine envelopes)."""

_POISON_PURGE_SCAN_BUDGET: Final = 10_000
"""Poison entries scanned per :meth:`PartitionStage.purge_poison_expired`
call (default) — the bounded unit of forensic-sweep work per partition
per pass. Entries are scanned in offset (key) order; a truncated pass
simply re-scans from the front next time (kept entries are re-read —
cheap at the volumes poison is meant to exist in: the consumer HALTS
past ``MILLRACE_POISON_MAX_RECORDS`` per run, so a prefix that has outgrown
the budget means an incident, not a routine sweep)."""

AckHook = Callable[["StageAck"], None]
"""Callback invoked once per successfully staged batch, after the batch
is durable and before ``stage_batch`` returns (the offset-commit seam —
the consumer's commit callback belongs here or later, never earlier)."""


def _poison_value(entry: PoisonedRecord, *, stamped_at_us: int | None) -> PoisonValue:
    """The envelope for one quarantined record, stamped with the write's
    staging clock when the entry does not already carry a stamp (the
    retention purge's age basis). An entry nobody stamped keeps ``None``
    and encodes as v1 — never purged, the safe direction for forensic
    state."""
    return PoisonValue(
        reason=entry.reason,
        key=entry.key,
        value=entry.value,
        value_bytes_original=entry.value_bytes_original,
        quarantined_at_us=(
            entry.quarantined_at_us
            if entry.quarantined_at_us is not None
            else stamped_at_us
        ),
    )


class StageError(Exception):
    """Base for staging-layer failures."""


class StageClosedError(StageError):
    """An operation was attempted on a closed stage or manager."""


class StageCorruptionError(StageError):
    """Persisted staging state violates a layout invariant (rows whose
    ``stats/`` entry is gone, stats that underflow a settled window).

    Never raised by a state this module can produce — every mutation is
    one atomic batch/transaction — so it means foreign writes or a bug;
    it is deliberately loud rather than clamped.
    """


@dataclass(frozen=True, slots=True)
class StagedRecord:
    """One consumed Kafka record to stage.

    ``event_ts_us`` is the event timestamp in microseconds since the
    Unix epoch (negatives are legal junk and sort correctly);
    ``offset`` is the record's Kafka offset. Integer ranges are
    validated by the keyspace codecs at encode time.
    """

    team_id: int
    event_ts_us: int
    offset: int
    payload: bytes

    def __post_init__(self) -> None:
        if not isinstance(self.payload, bytes):
            raise TypeError(f"payload must be bytes, got {type(self.payload).__name__}")


@dataclass(frozen=True, slots=True)
class PoisonedRecord:
    """One consumed Kafka record that can never become a staged row — an
    undecodable ``team_id`` key, no usable timestamp, a tombstone.

    Quarantined under ``poison/<offset>`` in the SAME write batch as the
    poll cycle's staged rows, so the offset commit that follows covers
    it exactly like a staged row: durably stored aside, never silently
    dropped, never wedging the loop. ``reason`` is the consumer's
    bounded rejection vocabulary; ``value`` may be truncated against the
    consumer's poison byte cap, with ``value_bytes_original`` recording
    the pre-truncation size (0 when the record carried no value).
    ``quarantined_at_us`` is the staging-clock write stamp (v2 envelope)
    — the retention purge's age basis (:meth:`PartitionStage.purge_poison_expired`);
    the stage stamps it at write time when the caller did not.
    """

    offset: int
    reason: str
    key: bytes | None
    value: bytes | None
    value_bytes_original: int
    quarantined_at_us: int | None = None


@dataclass(frozen=True, slots=True)
class StageAck:
    """What a successful :meth:`PartitionStage.stage_batch` stands for.

    The batch is remotely durable when this exists. ``first_offset`` /
    ``last_offset`` are the batch's offset bounds over ALL its records,
    staged rows and poison quarantine entries alike — the consumer
    commits ``last_offset + 1`` for the partition, strictly after
    receiving this. ``rows`` / ``staged_bytes`` count staged rows only;
    ``poisoned`` counts the quarantined records the batch also carried.
    """

    topic: str
    partition: int
    rows: int
    staged_bytes: int
    first_offset: int
    last_offset: int
    poisoned: int = 0


@dataclass(frozen=True, slots=True)
class FlushCommitReport:
    """Accounting of one :meth:`PartitionStage.commit_flushed` call.

    ``team_drained`` is true when the settle removed the team's last
    staged row (its ``stats/`` and ``sched_age/`` entries went with it,
    so the next stage starts the team's ``first_staged_ts`` fresh).
    A replayed call reports zeros and ``team_drained=False``.
    """

    rows_deleted: int
    bytes_deleted: int
    team_drained: bool


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
    only inside the receipt horizon).
    ``legacy_markers_collected``: ``flushed/`` markers deleted. Nothing
    writes that prefix any more — a settlement is one atomic
    transaction, so a marker was always fully-settled state with its
    receipt already in hand — and recovery removes leftover markers in
    bounded pages without decoding them.
    """

    pending_prepared: tuple[RecoveredPrepared, ...]
    legacy_markers_collected: int = 0


@dataclass(frozen=True, slots=True)
class StageGauges:
    """Backpressure gauges for one partition (docs/kafka-ingestion.md
    §Deployment: staged-bytes and oldest-staged-age drive consumer
    pause/resume). ``oldest_first_staged_ts`` is None when nothing is
    staged."""

    staged_bytes: int
    staged_rows: int
    staged_teams: int
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
        staged_teams=len(entries),
        oldest_first_staged_ts=min((s.first_staged_ts for s in entries), default=None),
    )


@dataclass(frozen=True, slots=True)
class PoisonPurgeReport:
    """Accounting of one :meth:`PartitionStage.purge_poison_expired` call.

    ``truncated`` is True when the scan budget ran out before the prefix
    was exhausted — the next pass restarts at the front. ``unreadable``
    counts entries that failed to decode (kept, never deleted — foreign
    writes or a newer build's envelope).
    """

    scanned: int
    deleted: int
    unreadable: int
    truncated: bool


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


@dataclass(frozen=True, slots=True)
class _Settlement:
    """The accounting of one applied flush settlement."""

    rows_deleted: int
    bytes_deleted: int
    team_drained: bool


def _merge_offset_ranges(ranges: Iterable[OffsetRange]) -> list[OffsetRange]:
    """Sort, dedupe and coalesce OVERLAPPING ranges (per topic+partition).

    Overlaps arise from at-least-once redelivery (a range staged twice)
    and must not inflate the list; merely-adjacent ranges stay distinct
    — their boundaries record arrival structure.
    """
    ordered = sorted(
        ranges, key=lambda r: (r.topic, r.partition, r.first_offset, r.last_offset)
    )
    out: list[OffsetRange] = []
    for r in ordered:
        prev = out[-1] if out else None
        if (
            prev is not None
            and prev.topic == r.topic
            and prev.partition == r.partition
            and r.first_offset <= prev.last_offset
        ):
            out[-1] = OffsetRange(
                prev.topic,
                prev.partition,
                prev.first_offset,
                max(prev.last_offset, r.last_offset),
            )
        else:
            out.append(r)
    return out


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
    ) -> PartitionStage:
        """Open (or create) the partition's SlateDB at ``path`` on the
        store ``object_store_url`` resolves to (``memory:///``,
        ``file:///``, ``s3://bucket``).

        NOTE on ``memory:///``: each ``resolve`` is an isolated store —
        reopening "the same" memory path requires a shared store, which
        is what :meth:`open_store` is for.
        """
        return await cls.open_store(
            ObjectStore.resolve(object_store_url),
            path,
            topic=topic,
            partition=partition,
            settings=settings,
            ack_hook=ack_hook,
            metrics_recorder=metrics_recorder,
        )

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
    ) -> PartitionStage:
        """Like :meth:`open` but on a pre-resolved object store.

        ``metrics_recorder`` plugs into SlateDB's ``DbBuilder``: the DB
        records its internals into it, and the operational surface
        snapshots it at scrape time (module docstring). One is created
        per stage when not handed in, so a stage always HAS a recorder —
        the collector never has to know whether wiring happened.
        """
        if not topic:
            raise ValueError("topic must be non-empty")
        if partition < 0:
            raise ValueError(f"partition must be >= 0, got {partition}")
        recorder = (
            metrics_recorder
            if metrics_recorder is not None
            else DefaultMetricsRecorder()
        )
        builder = DbBuilder(path, store)
        builder.with_metrics_recorder(recorder)
        if settings is not None:
            builder.with_settings(settings)
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
        it = await self._db.scan(
            KeyRange(start=start, start_inclusive=True, end=end, end_inclusive=False)
        )
        while True:
            chunk = await it.next_batch(_SCAN_CHUNK)
            if not chunk:
                break
            for kv in chunk:
                yield kv.key, kv.value

    async def _read_stats(self, team_id: int) -> StatsValue | None:
        raw = await self._db.get(keyspace.stats_key(team_id))
        return keyspace.decode_stats_value(raw) if raw is not None else None

    # -- staging --------------------------------------------------------------

    async def stage_batch(
        self,
        records: Iterable[StagedRecord],
        *,
        now_us: int,
        poison: Iterable[PoisonedRecord] = (),
    ) -> StageAck:
        """Stage one consumed microbatch — ONE write batch, remotely durable
        before return.

        Per team in the batch: row puts, the ``stats/`` merge
        (``first_staged_ts`` kept set-once from the entry that already
        exists; created with ``now_us`` when the team stands empty), the
        ``sched_age/`` entry on first stage, and the ``offsets/``
        identity-range merge. ``now_us`` is the injected staging clock —
        never read from the wall here. ``poison`` entries (records that
        can never become rows) land under ``poison/<offset>`` in the
        same batch, so the ack — and the offset commit that follows it —
        covers the quarantine exactly like the staged rows.

        Rejects a batch with neither staged nor poisoned records, a
        batch carrying one row key twice, and a batch quarantining one
        offset twice (both are consumer bugs; redelivery ACROSS batches
        is fine — the row/poison keys are derived from the record, so
        the re-put is idempotent and only the stats counters inflate
        conservatively until the next flush). The ack hook fires after
        durability, never for a rejected or failed batch.
        """
        self._ensure_open()
        keyed: list[tuple[bytes, StagedRecord]] = []
        seen: set[bytes] = set()
        for rec in records:
            row_key = keyspace.encode_row_key(
                RowKey(rec.team_id, rec.event_ts_us, rec.offset)
            )
            if row_key in seen:
                raise ValueError(
                    f"duplicate row key in batch: team {rec.team_id} "
                    f"ts {rec.event_ts_us} offset {rec.offset}"
                )
            seen.add(row_key)
            keyed.append((row_key, rec))

        quarantined = list(poison)
        seen_poison: set[int] = set()
        for entry in quarantined:
            if entry.offset in seen_poison:
                raise ValueError(f"duplicate poison offset in batch: {entry.offset}")
            seen_poison.add(entry.offset)

        if not keyed and not quarantined:
            raise ValueError(
                "stage_batch requires at least one record (staged or poisoned)"
            )

        by_team: dict[int, list[tuple[bytes, StagedRecord]]] = {}
        for row_key, rec in keyed:
            by_team.setdefault(rec.team_id, []).append((row_key, rec))

        total_bytes = 0
        async with self._write_lock:
            batch = WriteBatch()
            for team_id in sorted(by_team):
                rows = by_team[team_id]
                team_bytes = 0
                for row_key, rec in rows:
                    batch.put(row_key, rec.payload)
                    team_bytes += len(rec.payload)
                offsets = [rec.offset for _, rec in rows]

                current = await self._read_stats(team_id)
                if current is None:
                    merged = StatsValue(
                        staged_bytes=team_bytes,
                        first_staged_ts=now_us,
                        last_staged_ts=now_us,
                        row_count=len(rows),
                    )
                    batch.put(
                        keyspace.encode_sched_age_key(SchedAgeKey(now_us, team_id)),
                        b"",
                    )
                else:
                    merged = StatsValue(
                        staged_bytes=current.staged_bytes + team_bytes,
                        # SET-ONCE: later arrivals never advance it.
                        first_staged_ts=current.first_staged_ts,
                        last_staged_ts=now_us,
                        row_count=current.row_count + len(rows),
                    )
                batch.put(
                    keyspace.stats_key(team_id), keyspace.encode_stats_value(merged)
                )

                new_range = OffsetRange(
                    self._topic, self._partition, min(offsets), max(offsets)
                )
                raw_ranges = await self._db.get(keyspace.offsets_key(team_id))
                ranges = (
                    list(keyspace.decode_offsets_value(raw_ranges))
                    if raw_ranges is not None
                    else []
                )
                ranges.append(new_range)
                batch.put(
                    keyspace.offsets_key(team_id),
                    keyspace.encode_offsets_value(_merge_offset_ranges(ranges)),
                )
                total_bytes += team_bytes

            for entry in quarantined:
                batch.put(
                    keyspace.poison_key(entry.offset),
                    # Stamped with this batch's staging clock (the purge's
                    # age basis); the consume side never pre-stamps.
                    keyspace.encode_poison_value(
                        _poison_value(entry, stamped_at_us=now_us)
                    ),
                )

            handle = await self._db.write(batch)
            await handle.await_durable()

        all_offsets = [rec.offset for _, rec in keyed] + [
            entry.offset for entry in quarantined
        ]
        ack = StageAck(
            topic=self._topic,
            partition=self._partition,
            rows=len(keyed),
            staged_bytes=total_bytes,
            first_offset=min(all_offsets),
            last_offset=max(all_offsets),
            poisoned=len(quarantined),
        )
        if self._ack_hook is not None:
            self._ack_hook(ack)
        return ack

    # -- flush inputs -----------------------------------------------------------

    async def iter_key_stats(self) -> list[KeyStats]:
        """The flush planner's input: every team's decoded ``stats/``
        entry as a :class:`millrace.planner.KeyStats`, in team order."""
        self._ensure_open()
        out: list[KeyStats] = []
        for key, value in await self._scan_prefix(keyspace.STATS_PREFIX):
            stats = keyspace.decode_stats_value(value)
            out.append(
                KeyStats(
                    team_id=keyspace.decode_stats_key(key),
                    staged_bytes=stats.staged_bytes,
                    first_staged_ts=stats.first_staged_ts,
                    last_staged_ts=stats.last_staged_ts,
                    row_count=stats.row_count,
                )
            )
        return out

    async def read_team_stats(self, team_id: int) -> KeyStats | None:
        """Point-read one team's ``stats/`` entry as a
        :class:`millrace.planner.KeyStats` (None when the team has none).

        The age/slow readiness path's exact decision input
        (M6): the sweep enumerates age-eligible candidate keys from the
        ``sched_age/`` index and point-reads each candidate's stats,
        rather than trusting a shared scan's older snapshot for the
        deadline call."""
        self._ensure_open()
        stats = await self._read_stats(team_id)
        if stats is None:
            return None
        return KeyStats(
            team_id=team_id,
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

    def scan_team_rows(
        self,
        team_id: int,
        *,
        max_rows: int | None = None,
        max_bytes: int | None = None,
    ) -> AsyncIterator[tuple[RowKey, bytes]]:
        """Stream one team's staged rows in key order — ``(timestamp,
        offset)`` order within the team — a flush's input.

        The optional caps bound the read (a partial prefix of the
        slice): iteration stops before exceeding either. The iterator
        is a consistent snapshot as of creation, so rows staged while
        it streams are not seen.
        """
        if max_rows is not None and max_rows < 1:
            raise ValueError(f"max_rows must be >= 1, got {max_rows}")
        if max_bytes is not None and max_bytes < 1:
            raise ValueError(f"max_bytes must be >= 1, got {max_bytes}")
        return self._scan_team_rows(team_id, max_rows=max_rows, max_bytes=max_bytes)

    async def _scan_team_rows(
        self, team_id: int, *, max_rows: int | None, max_bytes: int | None
    ) -> AsyncIterator[tuple[RowKey, bytes]]:
        self._ensure_open()
        start, end = keyspace.team_rows_range(team_id)
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

    async def scan_team_rows_bounded(
        self, team_id: int, *, max_rows: int, max_bytes: int
    ) -> tuple[list[tuple[RowKey, bytes]], bool]:
        """The flush's bounded scan, collected: one team's staged rows in
        key order (``(timestamp, offset)`` within the team), capped in
        rows and bytes — plus whether a cap cut the scan short.

        ``truncated`` (the second element) is True exactly when the
        team's slice held at least one row past what the caps admitted
        (the row that would exceed a cap is NOT included). It is the
        window-narrowing signal (flush.py §Bounded unit of work): an
        UNtruncated scan saw the team's whole staged slice, so the flush
        window is the scan's own offset span; only a truncated scan must
        narrow to the covered offset prefix against the ``offsets/``
        record.
        """
        if max_rows < 1:
            raise ValueError(f"max_rows must be >= 1, got {max_rows}")
        if max_bytes < 1:
            raise ValueError(f"max_bytes must be >= 1, got {max_bytes}")
        self._ensure_open()
        start, end = keyspace.team_rows_range(team_id)
        rows: list[tuple[RowKey, bytes]] = []
        nbytes = 0
        truncated = False
        async for key, value in self._iter_range(start, end):
            if len(rows) >= max_rows or nbytes + len(value) > max_bytes:
                truncated = True
                break
            rows.append((keyspace.decode_row_key(key), value))
            nbytes += len(value)
        return rows, truncated

    async def read_offsets(self, team_id: int) -> tuple[OffsetRange, ...]:
        """One team's still-staged Kafka offset ranges (the flush's
        identity material), canonical order."""
        self._ensure_open()
        raw = await self._db.get(keyspace.offsets_key(team_id))
        return keyspace.decode_offsets_value(raw) if raw is not None else ()

    async def scan_poison(self) -> list[PoisonedRecord]:
        """Every quarantined record, in offset order.

        A forensic/test reader: nothing on the write, flush or recovery
        path calls this — the ``poison/`` prefix is stored aside, and the
        consumer's poison limits bound how much can accumulate per run.
        """
        self._ensure_open()
        out: list[PoisonedRecord] = []
        for key, raw in await self._scan_prefix(keyspace.POISON_PREFIX):
            value = keyspace.decode_poison_value(raw)
            out.append(
                PoisonedRecord(
                    offset=keyspace.decode_poison_key(key),
                    reason=value.reason,
                    key=value.key,
                    value=value.value,
                    value_bytes_original=value.value_bytes_original,
                    quarantined_at_us=value.quarantined_at_us,
                )
            )
        return out

    async def purge_poison_expired(
        self, cutoff_us: int, *, scan_budget: int = _POISON_PURGE_SCAN_BUDGET
    ) -> PoisonPurgeReport:
        """Delete the quarantine entries older than ``cutoff_us`` (the
        retention policy's edge — the CALLER owns the policy; the stage
        owns the bounded mechanism).

        Poison is forensic: nothing depends on its presence, so the purge
        is safe whenever it runs and a process that is DOWN at expiry
        simply purges later. The pass is bounded: at most ``scan_budget``
        entries are scanned (in offset order), deletes land in
        ``_POISON_WRITE_CHUNK`` durable batches with the write lock taken
        PER BATCH — never one unbounded batch, never one long lock hold,
        so the purge cannot block staging or a flush settle. A truncated
        pass reports ``truncated=True``; the next pass restarts at the
        front of the prefix (kept entries are re-read — see the budget's
        note).

        Two classes are never deleted: entries whose stamp is None (v1
        envelopes — unknown age is keep-forever, the safe direction for
        forensic state) and entries that FAIL to decode (counted as
        ``unreadable`` and logged once per entry per pass — foreign
        writes or a newer build's envelope; a purge must never destroy
        what it cannot read, and a bad entry must not wedge the pass).
        """
        self._ensure_open()
        scanned = deleted = unreadable = 0
        truncated = False
        start, end = keyspace.poison_range()
        pending: list[bytes] = []

        async def flush_deletes() -> None:
            nonlocal deleted
            if not pending:
                return
            batch = WriteBatch()
            for key in pending:
                batch.delete(key)
            async with self._write_lock:
                handle = await self._db.write(batch)
                await handle.await_durable()
            deleted += len(pending)
            pending.clear()

        async for key, raw in self._iter_range(start, end):
            if scanned >= scan_budget:
                truncated = True
                break
            scanned += 1
            try:
                value = keyspace.decode_poison_value(raw)
            except ValueError:
                unreadable += 1
                log.warning(
                    "poison purge: %s[%d] entry at offset %d failed to "
                    "decode — kept (never delete what we cannot read)",
                    self._topic,
                    self._partition,
                    keyspace.decode_poison_key(key),
                )
                continue
            if value.quarantined_at_us is None:
                continue  # unknown age: keep-forever (the v1 rule)
            if value.quarantined_at_us < cutoff_us:
                pending.append(key)
                if len(pending) >= _POISON_WRITE_CHUNK:
                    await flush_deletes()
        await flush_deletes()
        return PoisonPurgeReport(
            scanned=scanned, deleted=deleted, unreadable=unreadable, truncated=truncated
        )

    # -- prepared/ lifecycle ----------------------------------------------------

    async def persist_prepared(
        self, team_id: int, first_offset: int, request: PreparedRequest
    ) -> None:
        """Persist the COMPLETE prepared commit request BEFORE publication
        (durable before return). It is replayed byte-identically after an
        ambiguous response and never regenerated
        (docs/kafka-ingestion.md §The wire contract)."""
        self._ensure_open()
        batch = WriteBatch()
        batch.put(
            keyspace.prepared_key(team_id, first_offset),
            keyspace.encode_prepared_value(request),
        )
        async with self._write_lock:
            handle = await self._db.write(batch)
            await handle.await_durable()

    async def get_prepared(
        self, team_id: int, first_offset: int
    ) -> PreparedRequest | None:
        """Point-read one persisted prepared request."""
        self._ensure_open()
        raw = await self._db.get(keyspace.prepared_key(team_id, first_offset))
        return keyspace.decode_prepared_value(raw) if raw is not None else None

    async def list_prepared(self, team_id: int) -> list[RecoveredPrepared]:
        """Every persisted ``prepared/`` entry of ONE team, in
        ``first_offset`` order (the key sorts numerically).

        The flush path's outstanding-entry gate: at most one entry per
        team exists by construction (a new request is persisted only
        when the team has none outstanding), so this is how the flush
        finds that entry REGARDLESS of where the current scan's window
        starts — a bounded scan's prefix minimum can move under it
        (flush.py §Bounded unit of work), and a missed entry would
        double-publish the window. The prefix holds at most the team's
        in-flight entries, so the scan is tiny."""
        self._ensure_open()
        start, end = keyspace.team_prepared_range(team_id)
        return [
            RecoveredPrepared(
                key=keyspace.decode_prepared_key(key),
                request=keyspace.decode_prepared_value(value),
            )
            async for key, value in self._iter_range(start, end)
        ]

    async def load_prepared(self) -> list[RecoveredPrepared]:
        """Every persisted ``prepared/`` entry, in (team, first_offset)
        order — the flusher's replay set."""
        self._ensure_open()
        return [
            RecoveredPrepared(
                key=keyspace.decode_prepared_key(key),
                request=keyspace.decode_prepared_value(value),
            )
            for key, value in await self._scan_prefix(keyspace.PREPARED_PREFIX)
        ]

    async def drop_prepared(self, team_id: int, first_offset: int) -> None:
        """Delete one ``prepared/`` entry on its own — for abandoning a
        quarantined flush decision. Successful flushes need no call:
        :meth:`commit_flushed` drops the entry in the settlement
        transaction."""
        self._ensure_open()
        batch = WriteBatch()
        batch.delete(keyspace.prepared_key(team_id, first_offset))
        async with self._write_lock:
            handle = await self._db.write(batch)
            await handle.await_durable()

    # -- flush-time quarantine --------------------------------------------------
    #
    # The flush path's own quarantine (docs/kafka-ingestion.md §Staging layout:
    # junk ``event_time`` is clamped or quarantined AT FLUSH TIME): records that
    # staged fine but can never be PUBLISHED — an undecodable payload, an
    # out-of-window ``event_time`` under the quarantine policy, or every row of
    # a decision the server refused — land under ``poison/<offset>`` with the
    # consumer's envelope and durability discipline, mirrored: durable (WAL on
    # the object store) before return, so a later settlement never deletes a
    # staged row whose quarantine entry has not survived it. Re-putting the
    # same offset rewrites the same envelope (the flush's decode is
    # deterministic), so a crash-and-retry between quarantine and settlement
    # is idempotent.

    async def poison_records(
        self, entries: Iterable[PoisonedRecord], *, now_us: int | None = None
    ) -> int:
        """Quarantine flush-time rejections under ``poison/`` — durably, in
        chunks (a refused whale-range decision must not build one unbounded
        ``WriteBatch``). Returns the number of entries written.

        ``now_us`` stamps each entry's ``quarantined_at_us`` (the
        retention purge's age basis) unless the entry carries one.
        Callers without a clock to pass leave the stamp out — those
        envelopes encode as v1 and the purge keeps them forever (never
        delete forensic state whose age is unknown)."""
        self._ensure_open()
        count = 0
        chunk: list[PoisonedRecord] = []
        for entry in entries:
            chunk.append(entry)
            if len(chunk) >= _POISON_WRITE_CHUNK:
                count += await self._write_poison_chunk(chunk, stamped_at_us=now_us)
                chunk = []
        if chunk:
            count += await self._write_poison_chunk(chunk, stamped_at_us=now_us)
        return count

    async def _write_poison_chunk(
        self, chunk: list[PoisonedRecord], *, stamped_at_us: int | None
    ) -> int:
        batch = WriteBatch()
        for entry in chunk:
            batch.put(
                keyspace.poison_key(entry.offset),
                keyspace.encode_poison_value(
                    _poison_value(entry, stamped_at_us=stamped_at_us)
                ),
            )
        async with self._write_lock:
            handle = await self._db.write(batch)
            await handle.await_durable()
        return len(chunk)

    async def settle_quarantined(self, key: FlushedKey) -> FlushCommitReport:
        """Settle a flush decision that will NEVER be committed: every row
        of the window was quarantined (``poison/``) and no publication
        happened, so there is no receipt and nothing to reconcile — the
        settle is a plain delete.

        ONE transaction: delete exactly the window's rows, settle
        ``stats/``/``offsets/``, drop the range's ``prepared/`` entry.
        Idempotent: a replay finds no rows in the window and reports
        zeros."""
        self._ensure_open()
        async with self._write_lock:
            txn = await self._db.begin(IsolationLevel.SERIALIZABLE_SNAPSHOT)
            try:
                settlement = await self._settle_flushed(txn, key)
            except BaseException:
                await txn.rollback()
                raise
            handle = await txn.commit()
            if handle is not None:
                await handle.await_durable()
        return FlushCommitReport(
            rows_deleted=settlement.rows_deleted,
            bytes_deleted=settlement.bytes_deleted,
            team_drained=settlement.team_drained,
        )

    async def reconcile_stats(self, team_id: int) -> bool:
        """Remove a team's ``stats/`` + ``sched_age/`` entries when its rows
        are gone — the redelivery-inflation residue.

        ``stage_batch`` is idempotent at the ROW level but its stats merge
        inflates counters on redelivery (documented there), and a flush
        settles ACTUALS: afterwards a team can hold ``row_count > 0`` with
        zero staged rows. Left alone the residue re-decides the key every
        sweep and — worse — its stale ``first_staged_ts`` pins the
        backpressure age gauge forever. The flush path calls this when a
        decided key scans empty.

        ONE serializable transaction: the stats read and the row scan see
        one snapshot, so a repair either observes a genuinely empty team
        or conflicts with a concurrent ``stage_batch`` on the stats key
        (serializable write-write) — the loser aborts and neither loses
        the new batch's accounting. Returns True when it repaired."""
        self._ensure_open()
        async with self._write_lock:
            txn = await self._db.begin(IsolationLevel.SERIALIZABLE_SNAPSHOT)
            try:
                raw = await txn.get(keyspace.stats_key(team_id))
                if raw is None:
                    await txn.rollback()
                    return False
                start, end = keyspace.team_rows_range(team_id)
                it = await txn.scan(
                    KeyRange(
                        start=start, start_inclusive=True, end=end, end_inclusive=False
                    )
                )
                chunk = await it.next_batch(1)
                if chunk:
                    await txn.rollback()
                    return False
                stats = keyspace.decode_stats_value(raw)
                await txn.delete(keyspace.stats_key(team_id))
                await txn.delete(
                    keyspace.encode_sched_age_key(
                        SchedAgeKey(stats.first_staged_ts, team_id)
                    )
                )
            except BaseException:
                await txn.rollback()
                raise
            handle = await txn.commit()
            if handle is not None:
                await handle.await_durable()
        return True

    # -- flush settlement ---------------------------------------------------------

    async def commit_flushed(self, key: FlushedKey) -> FlushCommitReport:
        """Settle one committed flush — ONE transaction:

        delete EXACTLY the flush's offset window of the team's rows,
        subtract (or remove, with the ``sched_age/`` entry, when
        drained) the team's ``stats/``, prune the committed range out of
        ``offsets/``, and drop the range's ``prepared/`` entry.

        No ``flushed/`` marker is written. The caller holds the commit's
        receipt before calling (the server commits it in the same
        transaction as the snapshot), and this settle is atomic, so the
        two states a marker could distinguish — "delete pending" and
        "receipt unconfirmed" — cannot exist. A crash before this call
        leaves the ``prepared/`` entry; recovery replays it
        receipt-first (flush.py's recovery handoff).

        Idempotent: a replay after an ambiguous failure finds no rows in
        the window and reports zeros.
        """
        self._ensure_open()
        async with self._write_lock:
            txn = await self._db.begin(IsolationLevel.SERIALIZABLE_SNAPSHOT)
            try:
                settlement = await self._settle_flushed(txn, key)
            except BaseException:
                await txn.rollback()
                raise
            handle = await txn.commit()
            if handle is not None:
                await handle.await_durable()
        return FlushCommitReport(
            rows_deleted=settlement.rows_deleted,
            bytes_deleted=settlement.bytes_deleted,
            team_drained=settlement.team_drained,
        )

    async def _settle_flushed(self, txn: DbTransaction, key: FlushedKey) -> _Settlement:
        """The settlement body shared by :meth:`commit_flushed` and
        :meth:`settle_quarantined`."""
        # The exact settlement selection (keyspace.py §The settlement
        # window): scan the team's whole slice, delete the rows the
        # window selects.
        start, end = keyspace.flushed_rows_scan_range(key)
        rows_deleted = 0
        bytes_deleted = 0
        it = await txn.scan(
            KeyRange(start=start, start_inclusive=True, end=end, end_inclusive=False)
        )
        while True:
            chunk = await it.next_batch(_SCAN_CHUNK)
            if not chunk:
                break
            for kv in chunk:
                if keyspace.row_in_flushed_range(keyspace.decode_row_key(kv.key), key):
                    await txn.delete(kv.key)
                    rows_deleted += 1
                    bytes_deleted += len(kv.value)

        drained = await self._settle_stats(txn, key, rows_deleted, bytes_deleted)
        if rows_deleted:
            await self._settle_offsets(txn, key)
        await self._settle_prepared(txn, key)
        return _Settlement(rows_deleted, bytes_deleted, drained)

    async def _settle_stats(
        self, txn: DbTransaction, key: FlushedKey, rows_deleted: int, bytes_deleted: int
    ) -> bool:
        """Subtract the deleted rows from the team's ``stats/`` entry, or
        remove it (with its ``sched_age/`` entry) when the team drained.
        ``first_staged_ts`` is left untouched while rows remain: the
        survivors are younger, so keeping the older timestamp can only
        flush them EARLIER than their own deadline — the freshness SLA's
        safe direction."""
        raw = await txn.get(keyspace.stats_key(key.team_id))
        if raw is None:
            if rows_deleted:
                raise StageCorruptionError(
                    f"team {key.team_id}: {rows_deleted} staged rows in the "
                    "settled window but no stats entry"
                )
            return False
        current = keyspace.decode_stats_value(raw)
        if rows_deleted > current.row_count or bytes_deleted > current.staged_bytes:
            raise StageCorruptionError(
                f"team {key.team_id}: settling {rows_deleted} rows / "
                f"{bytes_deleted} bytes against stats holding "
                f"{current.row_count} rows / {current.staged_bytes} bytes"
            )
        if rows_deleted == 0:
            return False  # idempotent replay: the landing commit already settled
        remaining = current.row_count - rows_deleted
        if remaining == 0:
            await txn.delete(keyspace.stats_key(key.team_id))
            await txn.delete(
                keyspace.encode_sched_age_key(
                    SchedAgeKey(current.first_staged_ts, key.team_id)
                )
            )
            return True
        await txn.put(
            keyspace.stats_key(key.team_id),
            keyspace.encode_stats_value(
                StatsValue(
                    staged_bytes=current.staged_bytes - bytes_deleted,
                    first_staged_ts=current.first_staged_ts,
                    last_staged_ts=current.last_staged_ts,
                    row_count=remaining,
                )
            ),
        )
        return False

    async def _settle_offsets(self, txn: DbTransaction, key: FlushedKey) -> None:
        """Prune the committed window out of the team's ``offsets/``
        ranges: wholly-covered ranges drop, straddlers clip to their
        surviving piece(s) — a range spanning BOTH window edges splits
        into the piece below and the piece above — and the key leaves
        with its last range."""
        raw = await txn.get(keyspace.offsets_key(key.team_id))
        if raw is None:
            return
        kept: list[OffsetRange] = []
        for r in keyspace.decode_offsets_value(raw):
            if r.first_offset < key.first_offset:
                kept.append(
                    OffsetRange(
                        r.topic,
                        r.partition,
                        r.first_offset,
                        min(r.last_offset, key.first_offset - 1),
                    )
                )
            if r.last_offset > key.last_offset:
                kept.append(
                    OffsetRange(
                        r.topic,
                        r.partition,
                        max(r.first_offset, key.last_offset + 1),
                        r.last_offset,
                    )
                )
        if kept:
            await txn.put(
                keyspace.offsets_key(key.team_id), keyspace.encode_offsets_value(kept)
            )
        else:
            await txn.delete(keyspace.offsets_key(key.team_id))

    async def _settle_prepared(self, txn: DbTransaction, key: FlushedKey) -> None:
        """Drop the settled range's ``prepared/`` entry: once the settle
        lands, recovery's replay set is exactly what remains under
        ``prepared/``, so the entry must leave with the window."""
        await txn.delete(keyspace.prepared_key(key.team_id, key.first_offset))

    # -- recovery ---------------------------------------------------------------

    async def recover(self) -> RecoveryReport:
        """Bring a (re)opened instance to its crash-safe boundary.

        Two bounded jobs:

        1. Collect LEGACY ``flushed/`` markers. Nothing writes that
           prefix any more (settlement is one atomic transaction, so a
           marker was always settled state with its receipt already in
           hand — never a repair instruction). Collection is a paged
           delete: one write batch per ``_RECOVERY_PAGE`` keys, the write
           lock taken per page, NO per-marker transaction and no
           team-slice scan — a partition that accumulated markers for
           days is collected without stalling staging.
        2. Return every surviving ``prepared/`` entry — the ambiguity
           set the flusher reconciles receipt-first (flush.py).

        Safe to run any number of times; a clean instance costs one
        empty page-read and the ``prepared/`` scan.
        """
        self._ensure_open()
        collected = 0
        start, end = keyspace.flushed_range()
        it = await self._db.scan(
            KeyRange(start=start, start_inclusive=True, end=end, end_inclusive=False)
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
        """Backpressure gauges derived from the ``stats/`` prefix — the
        same snapshot the planner reads, so gauges cannot drift from
        flush decisions (restart-safe by construction)."""
        return gauges_from_stats(await self.iter_key_stats())

    async def close(self) -> None:
        """Flush and shut the instance down. Idempotent. A fenced
        instance (a newer writer opened the path) surfaces SlateDB's
        ``Error.Closed(FENCED)`` here if nothing surfaced it earlier —
        that is the assignment fence working, not damage."""
        if self._closed:
            return
        self._closed = True
        await self._db.shutdown()


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
    """Backpressure gauges aggregated over the manager's open partitions."""

    partitions: int
    staged_bytes: int
    staged_rows: int
    staged_teams: int
    oldest_first_staged_ts: int | None
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
    silently frozen backpressure numbers. A live fallback scan is NOT
    itself published: a reader in fallback needs a fresh answer on
    every call.
    """

    def __init__(
        self,
        object_store_url: str,
        base_path: str,
        *,
        settings: Settings | None = None,
        ack_hook: AckHook | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if not base_path.strip("/"):
            raise ValueError("base_path must be non-empty")
        self._store = ObjectStore.resolve(object_store_url)
        self._base_path = base_path.strip("/")
        self._settings = settings
        self._ack_hook = ack_hook
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
        stage = await PartitionStage.open_store(
            self._store,
            self.path_for(topic, partition),
            topic=topic,
            partition=partition,
            settings=self._settings,
            ack_hook=self._ack_hook,
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
    ) -> ManagerGauges:
        """The pod-wide fold of per-partition gauges (one code path for
        live scans and the sweep's published snapshot)."""
        return ManagerGauges(
            partitions=len(per_partition),
            staged_bytes=sum(g.staged_bytes for g in per_partition.values()),
            staged_rows=sum(g.staged_rows for g in per_partition.values()),
            staged_teams=sum(g.staged_teams for g in per_partition.values()),
            oldest_first_staged_ts=min(
                (
                    g.oldest_first_staged_ts
                    for g in per_partition.values()
                    if g.oldest_first_staged_ts is not None
                ),
                default=None,
            ),
            per_partition=per_partition,
        )

    def publish_gauges(
        self, per_partition: Mapping[tuple[str, int], StageGauges]
    ) -> None:
        """Publish the flush sweep's gauges — folded from the SAME
        ``stats/`` scans the planner decided from — as the pod's shared
        snapshot (M6: one scan per partition per sweep, shared by the
        planner, the consumer's backpressure and the metrics scrape).
        """
        self._published = (self._monotonic(), self._aggregate_gauges(per_partition))

    async def gauges(self, *, max_staleness_s: float | None = None) -> ManagerGauges:
        """Aggregate the open partitions' gauges.

        With ``max_staleness_s``, serves the sweep-published snapshot
        while it is fresh (age at most ``max_staleness_s``); otherwise
        scans live. The fallback is the safety property: a flush loop
        that stops publishing degrades the readers to their own scans,
        never to frozen numbers. A LIVE scan is deliberately NOT
        published: a reader falling back (e.g. the consumer with a
        halted flusher) needs a fresh answer on every call, and
        re-serving its own scan would freeze exactly the gauges that
        reader exists to keep honest.
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
