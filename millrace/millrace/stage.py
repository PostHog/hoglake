"""Durable staging: one SlateDB instance per claimed Kafka partition.

Each instance lives at ``{base_path}/{topic}/{partition}`` and holds the
partition's consumed records under ``rows/(group, offset)``
(keyspace.py). SlateDB is single-writer per path and fences lazily: a
second open of a path succeeds and the older writer gets
``Error.Closed(FENCED)`` at its next write's durability wait.

Group sizes and ages are kept IN MEMORY only (:class:`GroupState`).
That is sound because staged state never outlives a process: at open,
:meth:`PartitionStage.discover` finds every group that has rows (before
any consumer writes), and the process drains them all before it
consumes (main.py). From then on every row that reaches disk was first
counted in memory under the write lock, so a group with rows on disk is
always in the index; a group leaves the index only when a complete scan
of it was settled and no write touched it since the scan began
(:meth:`PartitionStage.settle`).

The write lock serializes this process's writes to one instance and
covers WRITES ONLY: no scan and no payload lowering ever runs under it
(at object-store latency either would stall staging and offset commits
behind it). Batches are built, and their payloads lowered into the
binding, on a worker thread before the lock is taken.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from collections.abc import Iterable, Mapping, Sequence
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

from . import keyspace, slatedb_patch
from .config import (
    SLATEDB_BLOCK_CACHE_BYTES_DEFAULT,
    SLATEDB_META_CACHE_BYTES_DEFAULT,
    validate_slatedb_cache_bytes,
)
from .keyspace import PartitionTuple, RowKey

slatedb_patch.apply()

log = logging.getLogger(__name__)

_SCAN_CHUNK: Final = 1024
"""Rows pulled per ``DbIterator.next_batch`` round trip."""

_BULK_SCAN_READ_AHEAD_BYTES: Final = 4 * 1024 * 1024
_BULK_SCAN_FETCH_TASKS: Final = 4


def _bulk_scan_options() -> ScanOptions:
    """Options for row scans. SlateDB's defaults fetch one ~4 KiB block
    per object-store GET with one fetch task, which reads a window at
    ~50 KB/s at S3 latencies; read ahead in multi-MiB ranged GETs with a
    few concurrent fetches instead, and keep the one-pass blocks out of
    the shared cache."""
    return ScanOptions(
        durability_filter=DurabilityLevel.MEMORY,
        dirty=False,
        read_ahead_bytes=_BULK_SCAN_READ_AHEAD_BYTES,
        cache_blocks=False,
        max_fetch_tasks=_BULK_SCAN_FETCH_TASKS,
    )


class StageError(Exception):
    """Base for staging-layer failures."""


class StageClosedError(StageError):
    """An operation was attempted on a closed stage or manager."""


class FencedError(StageError):
    """A newer writer opened a claimed partition's SlateDB path. Every
    later write fails the same way, so the process exits 4 rather than
    flap: a restart onto the same assignment would reopen the path and
    fence the new owner."""

    def __init__(self, partitions: Sequence[tuple[str, int]]) -> None:
        self.partitions = tuple(sorted(partitions))
        super().__init__(
            "stage fenced by a newer writer: "
            + ", ".join(f"{t}[{p}]" for t, p in self.partitions)
            + "; do not restart this pod onto the same assignment"
        )


def is_fenced(exc: BaseException) -> bool:
    """SlateDB's lazy fence: ``Error.Closed`` with reason FENCED."""
    return isinstance(exc, SlateError.Closed) and exc.reason == CloseReason.FENCED


@dataclass(frozen=True, slots=True)
class StagedRecord:
    """One consumed record to stage under ``group`` (its partition
    tuple), keyed ``(group, offset)``."""

    group: PartitionTuple
    offset: int
    payload: bytes

    def __post_init__(self) -> None:
        if not isinstance(self.payload, bytes):
            raise TypeError(f"payload must be bytes, got {type(self.payload).__name__}")


@dataclass(frozen=True, slots=True)
class DroppedRecord:
    """One record that can never become a row (consume time: not a JSON
    object, malformed partition source, tombstone; flush time: a value
    no column can hold). Counted and logged, never staged or published.
    ``path`` is the dotted catalog path of the refusing column when the
    decoder knows it."""

    offset: int
    reason: str
    path: str | None = None


@dataclass(frozen=True, slots=True)
class StageAck:
    """A durable :meth:`PartitionStage.stage_batch`."""

    topic: str
    partition: int
    rows: int
    staged_bytes: int
    first_offset: int
    last_offset: int


@dataclass(slots=True)
class GroupState:
    """In-memory accounting of one staged group. ``staged_bytes`` and
    ``rows`` over-count after a Kafka redelivery (a re-put of the same
    key is counted again) and under-count for groups found by
    :meth:`PartitionStage.discover` (zero); neither affects delivery.
    ``gen`` is the stage-wide write generation of the last batch that
    touched the group. ``backlog`` is set when a flush settled only part
    of the group's rows (a capped scan): the rest is due on the next
    sweep, whatever its counted size."""

    first_staged_ts: int
    staged_bytes: int = 0
    rows: int = 0
    gen: int = 0
    backlog: bool = False


@dataclass(frozen=True, slots=True)
class GroupSnapshot:
    group: PartitionTuple
    first_staged_ts: int
    staged_bytes: int
    rows: int
    backlog: bool = False


@dataclass(frozen=True, slots=True)
class Scan:
    """A bounded scan of one group: an exact offset prefix of its staged
    rows. ``complete`` is True when the scan reached the end of the
    group's rows. ``gen`` is the group's write generation read under the
    lock just before the scan began (None when the group was not in the
    index); ``started_us`` the staging clock at that moment."""

    group: PartitionTuple
    rows: list[tuple[RowKey, bytes]]
    complete: bool
    gen: int | None
    started_us: int

    @property
    def nbytes(self) -> int:
        return sum(len(p) for _, p in self.rows)


@dataclass(frozen=True, slots=True)
class StageGauges:
    partitions: int
    staged_bytes: int
    staged_rows: int
    staged_groups: int
    oldest_first_staged_ts: int | None


def build_slatedb_settings(*, max_unflushed_bytes: int, l0_max_ssts: int) -> Settings:
    """The SlateDB settings every partition instance opens with.

    The writer's embedded GC is always off: the maintenance service
    (maintenance.py) owns collection for every pipeline, which keeps
    LIST/DELETE traffic off the ingest pods. The embedded compactor is
    never touched: disabling it stalls memtable flushes at
    ``l0_max_ssts`` with nothing to drain them."""
    if max_unflushed_bytes < 1:
        raise ValueError(f"max_unflushed_bytes must be >= 1, got {max_unflushed_bytes}")
    if l0_max_ssts < 1:
        raise ValueError(f"l0_max_ssts must be >= 1, got {l0_max_ssts}")
    settings = Settings.default()
    settings.set("max_unflushed_bytes", str(max_unflushed_bytes))
    settings.set("l0_max_ssts", str(l0_max_ssts))
    settings.set("garbage_collector_options", "null")
    return settings


def build_slatedb_cache(
    *, block_cache_bytes: int, meta_cache_bytes: int
) -> DbCache | None:
    """The one pod-level SST cache every partition instance shares, or
    None for the 0/0 "disabled" pair. ``max_capacity`` is in bytes. The
    library default is a 512 + 128 MiB split cache PER INSTANCE."""
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
    """A stable, per-instance ``db_cache_id`` in the shared cache: the
    first 8 bytes of SHA-256 over ``"{topic}\\0{partition}"``."""
    if partition < 0:
        raise ValueError(f"partition must be >= 0, got {partition}")
    digest = hashlib.sha256(f"{topic}\0{partition}".encode()).digest()
    return int.from_bytes(digest[:8], "big")


def _build_stage_batch(
    records: Iterable[StagedRecord],
) -> tuple[WriteBatch, dict[PartitionTuple, tuple[int, int]], int, int, int]:
    """Encode and lower one poll's records into a WriteBatch (worker
    thread). Returns the batch, per-group ``(bytes, rows)``, the row
    count and the offset bounds. Refuses an empty batch and a row key
    repeated within the batch (consumer bugs)."""
    batch = WriteBatch()
    seen: set[bytes] = set()
    per_group: dict[PartitionTuple, tuple[int, int]] = {}
    first = last = None
    for rec in records:
        key = keyspace.encode_row_key(RowKey(rec.group, rec.offset))
        if key in seen:
            raise ValueError(
                f"duplicate row key in batch: group {rec.group} offset {rec.offset}"
            )
        seen.add(key)
        batch.put(key, rec.payload)
        nbytes, nrows = per_group.get(rec.group, (0, 0))
        per_group[rec.group] = (nbytes + len(rec.payload), nrows + 1)
        first = rec.offset if first is None else min(first, rec.offset)
        last = rec.offset if last is None else max(last, rec.offset)
    if first is None or last is None:
        raise ValueError("stage_batch requires at least one record")
    return batch, per_group, len(seen), first, last


def _build_delete_batch(rows: Sequence[tuple[RowKey, bytes]]) -> WriteBatch:
    batch = WriteBatch()
    for row_key, _ in rows:
        batch.delete(keyspace.encode_row_key(row_key))
    return batch


class PartitionStage:
    """Owns one SlateDB instance staging one (topic, partition)."""

    def __init__(
        self,
        db: Db,
        *,
        path: str,
        topic: str,
        partition: int,
        metrics_recorder: DefaultMetricsRecorder,
    ) -> None:
        self._db = db
        self._path = path
        self._topic = topic
        self._partition = partition
        self._metrics_recorder = metrics_recorder
        self._write_lock = asyncio.Lock()
        self._groups: dict[PartitionTuple, GroupState] = {}
        self._gen = 0
        self._closed = False

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
        metrics_recorder: DefaultMetricsRecorder | None = None,
        db_cache: DbCache | None = None,
        db_cache_id: int | None = None,
        cache_disabled: bool = False,
    ) -> PartitionStage:
        """Open (or create) the instance at ``path``. The SST cache choice
        is required (the shared pod cache with its id, or
        ``cache_disabled=True``): the binding's per-instance default is
        never taken silently. ``memory:///`` resolves to an isolated
        store per call, so reopening a memory path needs a shared store."""
        if not topic:
            raise ValueError("topic must be non-empty")
        if partition < 0:
            raise ValueError(f"partition must be >= 0, got {partition}")
        if db_cache is not None and cache_disabled:
            raise ValueError("db_cache and cache_disabled are mutually exclusive")
        if (db_cache is None) != (db_cache_id is None):
            raise ValueError("db_cache and db_cache_id go together")
        if db_cache is None and not cache_disabled:
            raise ValueError(
                "an SST cache choice is required: db_cache + db_cache_id, or "
                "cache_disabled=True"
            )
        recorder = metrics_recorder or DefaultMetricsRecorder()
        builder = DbBuilder(path, store)
        builder.with_metrics_recorder(recorder)
        if settings is not None:
            builder.with_settings(settings)
        if db_cache is not None:
            assert db_cache_id is not None
            builder.with_db_cache(db_cache, db_cache_id)
        else:
            builder.with_db_cache_disabled()
        db = await builder.build()
        return cls(
            db, path=path, topic=topic, partition=partition, metrics_recorder=recorder
        )

    @property
    def path(self) -> str:
        return self._path

    @property
    def topic(self) -> str:
        return self._topic

    @property
    def partition(self) -> int:
        return self._partition

    @property
    def metrics_recorder(self) -> DefaultMetricsRecorder:
        return self._metrics_recorder

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    def _ensure_open(self) -> None:
        if self._closed:
            raise StageClosedError(f"stage {self._path} is closed")

    # -- discovery ------------------------------------------------------------

    async def discover(self, *, now_us: int) -> list[PartitionTuple]:
        """Find every group with staged rows and enter it in the index
        (first_staged_ts = ``now_us``, sizes unknown). Called once at
        open, before any consumer write, so it needs no lock.

        A skip-scan: one single-row seek per group, each starting just
        past the previous group's range (prefix-free encoding), so the
        cost is the number of groups, not the staged bytes."""
        self._ensure_open()
        cursor, end = keyspace.rows_range()
        found: list[PartitionTuple] = []
        while True:
            it = await self._db.scan(
                KeyRange(
                    start=cursor, start_inclusive=True, end=end, end_inclusive=False
                )
            )
            chunk = await it.next_batch(1)
            if not chunk:
                break
            group = keyspace.decode_row_key(chunk[0].key).group
            found.append(group)
            self._groups.setdefault(group, GroupState(first_staged_ts=now_us))
            cursor = keyspace.group_rows_range(group)[1]
        await self._delete_foreign_keys()
        return found

    async def _delete_foreign_keys(self) -> None:
        """Delete every key outside ``rows/`` and ``meta/`` (an older
        build's layout: ``stats/``, ``sched_age/``, ``prepared/``,
        ``flushed/``), in bounded batches; scans run outside the lock."""
        rows_start, rows_end = keyspace.rows_range()
        meta_end = keyspace.prefix_successor(keyspace.META_PREFIX)
        ranges = (
            (None, keyspace.META_PREFIX),
            (meta_end, rows_start),
            (rows_end, None),
        )
        deleted = 0
        for start, end in ranges:
            it = await self._db.scan_with_options(
                KeyRange(
                    start=start,
                    start_inclusive=start is not None,
                    end=end,
                    end_inclusive=False,
                ),
                _bulk_scan_options(),
            )
            while True:
                chunk = await it.next_batch(_SCAN_CHUNK)
                if not chunk:
                    break
                batch = WriteBatch()
                for kv in chunk:
                    batch.delete(kv.key)
                async with self._write_lock:
                    handle = await self._db.write(batch)
                    await handle.await_durable()
                deleted += len(chunk)
        if deleted:
            log.info(
                "%s[%d]: deleted %d key(s) of an older staging layout",
                self._topic,
                self._partition,
                deleted,
            )

    async def pinned_table_uuid(self) -> str | None:
        """The incarnation this stage's rows were first committed to."""
        self._ensure_open()
        raw = await self._db.get(keyspace.TABLE_UUID_KEY)
        return None if raw is None else bytes(raw).decode("utf-8")

    async def pin_table_uuid(self, table_uuid: str) -> None:
        """Durably record the incarnation (first commit only)."""
        self._ensure_open()
        batch = WriteBatch()
        batch.put(keyspace.TABLE_UUID_KEY, table_uuid.encode("utf-8"))
        async with self._write_lock:
            handle = await self._db.write(batch)
            await handle.await_durable()

    # -- staging --------------------------------------------------------------

    async def stage_batch(
        self, records: Iterable[StagedRecord], *, now_us: int
    ) -> StageAck:
        """Stage one poll's records for this partition as ONE write
        batch, remotely durable before return. The batch is built (and
        its payloads lowered into the binding) on a worker thread before
        the lock; under the lock only the in-memory index update, the
        write and the durability wait run. The index is updated BEFORE
        the write so memory always covers disk; a failed write leaves
        harmless over-counts (and is process-fatal in the consumer)."""
        self._ensure_open()
        recs = list(records)
        batch, per_group, rows, first, last = await asyncio.to_thread(
            _build_stage_batch, recs
        )
        async with self._write_lock:
            self._ensure_open()
            self._gen += 1
            for group, (nbytes, nrows) in per_group.items():
                state = self._groups.get(group)
                if state is None:
                    state = self._groups[group] = GroupState(first_staged_ts=now_us)
                state.staged_bytes += nbytes
                state.rows += nrows
                state.gen = self._gen
            handle = await self._db.write(batch)
            await handle.await_durable()
        return StageAck(
            topic=self._topic,
            partition=self._partition,
            rows=rows,
            staged_bytes=sum(b for b, _ in per_group.values()),
            first_offset=first,
            last_offset=last,
        )

    async def probe_fence(self, offset: int) -> None:
        """One durable put of ``meta/fence`` for a poll that handled
        records on this partition but staged none (all dropped): a fenced
        writer fails here, before the consumer commits offsets against
        the new owner. An empty batch cannot serve (SlateDB refuses it)."""
        self._ensure_open()
        batch = WriteBatch()
        batch.put(keyspace.FENCE_PROBE_KEY, keyspace.encode_fence_probe_value(offset))
        async with self._write_lock:
            handle = await self._db.write(batch)
            await handle.await_durable()

    # -- the index ------------------------------------------------------------

    def snapshot(self) -> list[GroupSnapshot]:
        """The in-memory index, copied (planner input and gauges)."""
        return [
            GroupSnapshot(g, s.first_staged_ts, s.staged_bytes, s.rows, s.backlog)
            for g, s in list(self._groups.items())
        ]

    def has_group(self, group: PartitionTuple) -> bool:
        return group in self._groups

    # -- flush ----------------------------------------------------------------

    async def scan(self, group: PartitionTuple, *, max_bytes: int, now_us: int) -> Scan:
        """Read an offset prefix of ``group``'s staged rows, at most
        ``max_bytes`` of payload but ALWAYS at least one row when any
        exists (a row larger than the budget must not wedge its group).

        The group's write generation is read under the lock first: every
        write with a generation at or below it has returned, so it is
        visible to the scan iterator created after. The scan itself runs
        outside the lock."""
        if max_bytes < 1:
            raise ValueError(f"max_bytes must be >= 1, got {max_bytes}")
        self._ensure_open()
        async with self._write_lock:
            state = self._groups.get(group)
            gen = None if state is None else state.gen
        start, end = keyspace.group_rows_range(group)
        it = await self._db.scan_with_options(
            KeyRange(start=start, start_inclusive=True, end=end, end_inclusive=False),
            _bulk_scan_options(),
        )
        rows: list[tuple[RowKey, bytes]] = []
        nbytes = 0
        while True:
            chunk = await it.next_batch(_SCAN_CHUNK)
            if not chunk:
                return Scan(group, rows, True, gen, now_us)
            for kv in chunk:
                if rows and nbytes + len(kv.value) > max_bytes:
                    return Scan(group, rows, False, gen, now_us)
                rows.append((keyspace.decode_row_key(kv.key), kv.value))
                nbytes += len(kv.value)

    async def settle(self, scan: Scan) -> bool:
        """Delete exactly the scanned row keys in one durable batch, then
        update the index. Returns whether the group left the index.

        Exact keys, never a range: a row that entered the scanned offset
        range after the scan (a re-key) is not among them and survives.
        The group leaves the index only when the scan was complete and
        no write touched the group since the scan's generation was read;
        a complete scan with later writes lifts ``first_staged_ts`` to
        the scan's start, so a group under steady inflow is due once per
        deadline rather than on every sweep."""
        self._ensure_open()
        batch = (
            await asyncio.to_thread(_build_delete_batch, scan.rows)
            if scan.rows
            else None
        )
        async with self._write_lock:
            if batch is not None:
                handle = await self._db.write(batch)
                await handle.await_durable()
            state = self._groups.get(scan.group)
            if state is None:
                return True
            if scan.complete and state.gen == scan.gen:
                del self._groups[scan.group]
                return True
            state.staged_bytes = max(0, state.staged_bytes - scan.nbytes)
            state.rows = max(0, state.rows - len(scan.rows))
            state.backlog = not scan.complete
            if scan.complete:
                state.first_staged_ts = max(state.first_staged_ts, scan.started_us)
            return False

    async def close(self) -> None:
        """Flush and shut the instance down. Idempotent. A fenced writer
        has already yielded the path; other failures propagate."""
        if self._closed:
            return
        self._closed = True
        try:
            await self._db.shutdown()
        except SlateError.Closed as exc:
            if exc.reason != CloseReason.FENCED:
                raise


class StageManager:
    """Opens and closes the :class:`PartitionStage` of each claimed
    partition on one shared object store, with one pod-level SST cache."""

    def __init__(
        self,
        object_store_url: str,
        base_path: str,
        *,
        settings: Settings | None = None,
        block_cache_bytes: int = SLATEDB_BLOCK_CACHE_BYTES_DEFAULT,
        meta_cache_bytes: int = SLATEDB_META_CACHE_BYTES_DEFAULT,
    ) -> None:
        if not base_path.strip("/"):
            raise ValueError("base_path must be non-empty")
        self._store = ObjectStore.resolve(object_store_url)
        self._base_path = base_path.strip("/")
        self._settings = settings
        self._db_cache = build_slatedb_cache(
            block_cache_bytes=block_cache_bytes, meta_cache_bytes=meta_cache_bytes
        )
        self._cache_capacity = (block_cache_bytes, meta_cache_bytes)
        self._stages: dict[tuple[str, int], PartitionStage] = {}
        self._closed = False

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    @property
    def store(self) -> ObjectStore:
        return self._store

    @property
    def cache_capacity_bytes(self) -> tuple[int, int]:
        """The shared cache's configured ``(block, meta)`` bytes."""
        return self._cache_capacity

    def path_for(self, topic: str, partition: int) -> str:
        return f"{self._base_path}/{topic}/{partition}"

    def partitions(self) -> tuple[tuple[str, int], ...]:
        return tuple(sorted(self._stages))

    def stage(self, topic: str, partition: int) -> PartitionStage:
        return self._stages[(topic, partition)]

    async def open_partition(
        self, topic: str, partition: int, *, now_us: int
    ) -> PartitionStage:
        """Open one partition's stage and discover its staged groups."""
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
                cache_disabled=True,
            )
        try:
            groups = await stage.discover(now_us=now_us)
        except BaseException:
            await stage.close()
            raise
        self._stages[key] = stage
        log.info(
            "opened %s[%d] at %s: %d staged group(s) to drain",
            topic,
            partition,
            path,
            len(groups),
        )
        return stage

    async def close_partition(self, topic: str, partition: int) -> None:
        stage = self._stages.pop((topic, partition))
        await stage.close()

    async def sync_assignment(
        self, claimed: Iterable[tuple[str, int]], *, now_us: int
    ) -> None:
        """Close revoked partitions and open newly claimed ones."""
        if self._closed:
            raise StageClosedError("stage manager is closed")
        want = set(claimed)
        for topic, partition in sorted(set(self._stages) - want):
            await self.close_partition(topic, partition)
        for topic, partition in sorted(want - set(self._stages)):
            await self.open_partition(topic, partition, now_us=now_us)

    def gauges(self) -> StageGauges:
        """Pod totals from the in-memory index (cheap; no I/O)."""
        snaps = [s for stage in list(self._stages.values()) for s in stage.snapshot()]
        return StageGauges(
            partitions=len(self._stages),
            staged_bytes=sum(s.staged_bytes for s in snaps),
            staged_rows=sum(s.rows for s in snaps),
            staged_groups=len(snaps),
            oldest_first_staged_ts=min(
                (s.first_staged_ts for s in snaps), default=None
            ),
        )

    def metrics_recorders(self) -> Mapping[tuple[str, int], DefaultMetricsRecorder]:
        return {
            key: stage.metrics_recorder for key, stage in list(self._stages.items())
        }

    async def close(self) -> None:
        """Close every open stage. Idempotent."""
        if self._closed:
            return
        self._closed = True
        for key in sorted(self._stages):
            await self._stages[key].close()
        self._stages.clear()
