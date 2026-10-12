"""Binding-parity probes for the slatedb Python binding (0.17.x).

PERMANENT tests (docs/kafka-ingestion.md validation item 2): they exist
so a binding upgrade that changes a knob this component depends on fails
loudly HERE, at upgrade time, not in production. Each probe pins the
OBSERVED 0.17.x behavior; if an upgrade changes it, the red test is the
migration guide.

Probes: transactions with isolation (commit / rollback / conflict),
scan ``seek`` and iterator snapshot isolation, the range-delete idiom
this codebase uses, WriteBatch consumption, checkpoint
create/list/delete, the ``await_durable`` ack boundary, and writer
fencing on a contested open.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from slatedb.uniffi import (
    AdminBuilder,
    CheckpointOptions,
    CloseOptions,
    CloseReason,
    CompactionSpec,
    CompactionStatus,
    DbBuilder,
    FlushOptions,
    FlushType,
    GarbageCollectorDirectoryOptions,
    GarbageCollectorOptions,
    GarbageCollectorScheduleOptions,
    IsolationLevel,
    KeyRange,
    ObjectStore,
    Settings,
    SourceId,
    WriteBatch,
)
from slatedb.uniffi import Error as SlateError


def _range(start: bytes | None, end: bytes | None) -> KeyRange:
    return KeyRange(
        start=start,
        start_inclusive=start is not None,
        end=end,
        end_inclusive=False,
    )


async def _collect(it) -> list[tuple[bytes, bytes]]:
    out = []
    while (kv := await it.next()) is not None:
        out.append((kv.key, kv.value))
    return out


# -- transactions ------------------------------------------------------------------


@pytest.mark.component
@pytest.mark.parametrize("level", list(IsolationLevel), ids=lambda l: l.name)
async def test_transaction_commit_rollback_and_read_your_writes(level):
    """Both isolation levels: a commit lands atomically and is visible to
    later reads; a rollback discards; a transaction reads its own writes."""
    db = await DbBuilder("parity/txn", ObjectStore.resolve("memory:///")).build()
    await db.put(b"base", b"committed")

    txn = await db.begin(level)
    assert await txn.get(b"base") == b"committed"  # snapshot reads see prior state
    await txn.put(b"base", b"dirty")
    await txn.put(b"extra", b"dirty")
    assert await txn.get(b"base") == b"dirty"  # read-your-writes
    await txn.rollback()
    assert await db.get(b"base") == b"committed"
    assert await db.get(b"extra") is None

    txn = await db.begin(level)
    await txn.put(b"base", b"new")
    await txn.delete(b"extra-never-existed")  # deleting a missing key is a no-op
    handle = await txn.commit()
    assert handle is not None  # a write-carrying commit returns its WriteHandle
    assert await db.get(b"base") == b"new"
    await db.shutdown()


@pytest.mark.component
@pytest.mark.parametrize("level", list(IsolationLevel), ids=lambda l: l.name)
async def test_transaction_write_write_conflict_is_detected(level):
    """Two concurrent transactions writing the SAME key: the second commit
    fails with Error.Transaction and the first writer's value stands.

    Observed on 0.17.x for BOTH isolation levels (SNAPSHOT included) —
    the binding does last-committer-wins only for disjoint write sets."""
    db = await DbBuilder("parity/conflict", ObjectStore.resolve("memory:///")).build()
    t1 = await db.begin(level)
    t2 = await db.begin(level)
    await t1.put(b"contended", b"t1")
    await t2.put(b"contended", b"t2")
    await t1.commit()
    with pytest.raises(SlateError.Transaction):
        await t2.commit()
    assert await db.get(b"contended") == b"t1"

    # disjoint write sets do not conflict
    t1 = await db.begin(level)
    t2 = await db.begin(level)
    await t1.put(b"a", b"1")
    await t2.put(b"b", b"2")
    await t1.commit()
    await t2.commit()
    assert await db.get(b"a") == b"1" and await db.get(b"b") == b"2"
    await db.shutdown()


# -- scans: seek, snapshot isolation, bounded chunks --------------------------------


@pytest.mark.component
async def test_scan_seek_positions_the_iterator():
    db = await DbBuilder("parity/seek", ObjectStore.resolve("memory:///")).build()
    batch = WriteBatch()
    for i in range(10):
        batch.put(f"s/{i:02}".encode(), f"v{i}".encode())
    await db.write(batch)

    it = await db.scan(_range(b"s/", b"s0"))
    assert (await it.next()).key == b"s/00"
    await it.seek(b"s/05")
    kv = await it.next()
    assert (kv.key, kv.value) == (b"s/05", b"v5")
    # seek is FORWARD-ONLY past returned keys: below the last returned
    # key it fails loudly rather than silently repositioning
    with pytest.raises(SlateError.Invalid, match="cannot seek"):
        await it.seek(b"s/02")
    await it.seek(b"s/99")  # past every key in range: the iterator is done
    assert await it.next() is None

    # next_batch returns up to its bound and signals exhaustion with []
    it = await db.scan(_range(b"s/", b"s0"))
    assert len(await it.next_batch(4)) == 4
    assert len(await it.next_batch(100)) == 6
    assert await it.next_batch(100) == []
    await db.shutdown()


@pytest.mark.component
async def test_scan_iterator_is_a_snapshot_at_creation():
    """A scan iterator does NOT see writes that land after its creation —
    the flush path can scan while the consume loop keeps staging."""
    db = await DbBuilder("parity/snapshot", ObjectStore.resolve("memory:///")).build()
    batch = WriteBatch()
    for i in range(3):
        batch.put(f"k/{i}".encode(), b"v")
    await db.write(batch)

    it = await db.scan(_range(b"k/", b"k0"))
    late = WriteBatch()
    late.put(b"k/1a", b"late")
    await db.write(late)
    assert [k for k, _ in await _collect(it)] == [b"k/0", b"k/1", b"k/2"]
    # a fresh scan sees it
    it = await db.scan(_range(b"k/", b"k0"))
    assert b"k/1a" in [k for k, _ in await _collect(it)]
    await db.shutdown()


# -- the range-delete idiom ----------------------------------------------------------


@pytest.mark.component
async def test_range_delete_idiom_and_write_batch_consumption():
    """The delete shape this codebase uses (there is no native range-delete
    in the binding): scan the half-open range, delete each key in ONE
    WriteBatch. Neighbours outside the range survive; deleting a missing
    key is a silent no-op; a WriteBatch is CONSUMED by db.write and its
    reuse fails loudly."""
    db = await DbBuilder("parity/rangedel", ObjectStore.resolve("memory:///")).build()
    batch = WriteBatch()
    for team in (1, 2):
        for i in range(5):
            batch.put(f"rows/{team}/{i}".encode(), b"v")
    await db.write(batch)

    it = await db.scan(_range(b"rows/1/", b"rows/10"))
    doomed = [k for k, _ in await _collect(it)]
    assert len(doomed) == 5
    delete = WriteBatch()
    for key in doomed:
        delete.delete(key)
    delete.delete(b"rows/1/never-existed")  # no-op, must not fail
    await db.write(delete)

    with pytest.raises(SlateError.Invalid, match="already been consumed"):
        delete.put(b"reused", b"x")

    assert await _collect(await db.scan(_range(b"rows/1/", b"rows/10"))) == []
    assert len(await _collect(await db.scan(_range(b"rows/2/", b"rows/20")))) == 5
    await db.shutdown()


# -- checkpoints ---------------------------------------------------------------------


@pytest.mark.component
async def test_checkpoint_create_list_delete():
    """Checkpoints are metadata handles over the manifest: create one,
    see it listed (by id and name), keep reading the DB, delete it."""
    store = ObjectStore.resolve("memory:///")
    db = await DbBuilder("parity/checkpoint", store).build()
    await db.put(b"k", b"v")
    await db.flush()

    admin = AdminBuilder("parity/checkpoint", store).build()
    created = await admin.create_detached_checkpoint(
        CheckpointOptions(lifetime_ms=None, source=None, name="parity-probe")
    )
    assert created.id and created.manifest_id >= 1
    listed = await admin.list_checkpoints(None)
    assert [(c.id, c.name) for c in listed] == [(created.id, "parity-probe")]
    assert await admin.list_checkpoints("no-such-name") == []
    # the DB is undisturbed by checkpoint bookkeeping
    assert await db.get(b"k") == b"v"
    await admin.delete_checkpoint(created.id)
    assert await admin.list_checkpoints(None) == []
    await db.shutdown()


# -- the durability ack boundary ------------------------------------------------------


@pytest.mark.component
async def test_await_durable_means_survives_reopen():
    """The boundary stage_batch's ack stands on: a write whose handle has
    been awaited durable is replayed by a fresh open on the same store."""
    store = ObjectStore.resolve("memory:///")
    db = await DbBuilder("parity/durable", store).build()
    handle = await db.put(b"acked", b"v")
    assert handle.seqnum() >= 1
    await handle.await_durable()
    await db.shutdown()

    reopened = await DbBuilder("parity/durable", store).build()
    assert await reopened.get(b"acked") == b"v"
    await reopened.shutdown()


# -- writer fencing on a contested open ------------------------------------------------


async def _probe_fencing(store_a: ObjectStore, store_b: ObjectStore, path: str) -> None:
    """Open two writers on one path; pin what the first writer sees."""
    first = await DbBuilder(path, store_a).build()
    await first.put(b"k", b"from-first")
    await first.flush()

    # THE OPEN IS NOT FENCED: a second writer on the same path builds fine.
    second = await DbBuilder(path, store_b).build()

    # The older writer is fenced LAZILY. WHICH call first detects the
    # fence is timing-dependent — the binding's background manifest poll
    # races the caller: under load put() itself raises; in a tight loop
    # put() returns a handle and the fence surfaces at the write's
    # durability wait. The invariant that holds either way: no later
    # than the first durability wait after losing the path, the fenced
    # writer fails with Error.Closed(FENCED).
    try:
        handle = await first.put(b"k2", b"from-first")
        await handle.await_durable()
        raise AssertionError("the fenced writer's write was acknowledged")
    except SlateError.Closed as fenced:
        assert fenced.reason == CloseReason.FENCED
        assert "newer DB client" in str(fenced)
    # Once detected, every shared-state operation keeps failing.
    with pytest.raises(SlateError.Closed):
        await first.flush()
    with pytest.raises(SlateError.Closed):
        await first.get(b"k")
    with pytest.raises(SlateError.Closed):
        await first.scan(_range(None, None))
    # shutdown() closes quietly after a detection (it raises FENCED only
    # when it is itself the detecting call).
    await first.shutdown()

    # The newer writer is unaffected, and the fenced writer's buffered
    # writes never landed.
    await second.put(b"k3", b"from-second")
    await second.flush()
    assert await second.get(b"k") == b"from-first"
    assert await second.get(b"k2") is None
    assert await second.get(b"k3") == b"from-second"
    await second.shutdown()


@pytest.mark.component
async def test_writer_fencing_on_contested_open_memory():
    """Writer fencing on a contested open — observed on slatedb 0.17.x:

    - opening a path that already has a LIVE writer SUCCEEDS — the open
      itself is not fenced;
    - the OLDER writer is fenced lazily: no later than the first
      durability wait after the contested open, it fails with
      ``Error.Closed(reason=FENCED, "detected newer DB client")``. WHICH
      call detects it is timing-dependent (the binding's background
      manifest poll races the caller): under load ``put`` itself raises,
      in a tight loop the handle's ``await_durable`` does; ``flush``,
      ``get`` and ``scan`` raise once detected, and ``shutdown`` raises
      only when it is itself the detecting call;
    - the newer writer proceeds normally; nothing the fenced writer
      writes after the contest ever lands for the new owner.

    Consequence for millrace: the Kafka consumer assignment is the real
    ownership fence, and a partition's stage learns it lost the path at
    its next ``stage_batch`` at the latest (put or the durability wait
    raises FENCED) — it never acknowledges a row that did not land.
    ``memory:///`` variant with one shared store; the ``file:///``
    variant pins the same against a real filesystem-backed store.
    """
    store = ObjectStore.resolve("memory:///")
    await _probe_fencing(store, store, "parity/fencing")


@pytest.mark.integration
async def test_writer_fencing_on_contested_open_file(tmp_path):
    """``file:///`` variant of the contested-open fencing probe — see the
    memory variant's docstring for the pinned behavior. Two separate
    resolves against the same directory: genuinely shared backing."""
    path = str(tmp_path / "db")
    await _probe_fencing(
        ObjectStore.resolve("file:///"), ObjectStore.resolve("file:///"), path
    )


@pytest.mark.component
async def test_memory_resolves_are_isolated_stores():
    """Each ``ObjectStore.resolve("memory:///")`` is a SEPARATE store:
    reopen-through-shared-store is the only coherent memory topology, and
    the manager resolves its store once for exactly this reason."""
    store_a = ObjectStore.resolve("memory:///")
    db = await DbBuilder("parity/isolated", store_a).build()
    await db.put(b"k", b"v")
    await db.flush()
    await db.shutdown()

    store_b = ObjectStore.resolve("memory:///")
    db2 = await DbBuilder("parity/isolated", store_b).build()
    assert await db2.get(b"k") is None
    await db2.shutdown()


# -- external maintenance: compaction submission, GC, checkpoints, fencing ----------
#
# The probes below pin the slatedb 0.17.0 surface the EXTERNAL maintenance
# services (millrace.maintenance — a separate deployment that loops over every
# per-partition staging DB) stand on. All of them need a real listable store,
# so they are ``integration`` (file:///). They exist so that a binding upgrade
# that changes any of these behaviors fails loudly HERE.
#
# Headline findings (0.17.0, each pinned by a test below):
#
# * ``Admin.submit_compaction`` ENQUEUES ONLY: it persists a ``Submitted``
#   record into the DB's ``.compactions`` store. Execution happens in a
#   compactor process — the WRITER's internal compactor picks external
#   submissions up on its poll tick and executes them (verified below:
#   SUBMITTED -> ... -> COMPLETED with L0 drained and no other executor
#   present). The binding exposes NO way to run a compactor process
#   (``Admin::run_compactor`` / ``run_compaction_worker`` exist in the Rust
#   core but are not wrapped — absent from the FFI symbol table), so an
#   external Python service can schedule but can never EXECUTE a compaction.
#   Against a writer started with ``compactor_options`` disabled the call
#   FAILS (``Error.Data("invalid DB state")``): the compactions store is only
#   created by a running compactor.
# * ``Settings.set("compactor_options", "null")`` disables the writer's
#   internal compactor — and then memtable->L0 flushes STALL once L0 holds
#   ``l0_max_ssts`` SSTs (writes to the WAL still land). With no external
#   executor available from this binding, writers must keep their internal
#   compactor enabled.
# * ``Settings.set("garbage_collector_options", "null")`` disables the
#   writer's internal GC cleanly (writes + internal compaction unaffected,
#   no ``gc/`` artifacts) — the knob a deployment flips when the external GC
#   service owns collection.
# * ``Admin.run_gc_once`` executes SYNCHRONOUSLY in the caller's process and
#   is safe against an active writer: flushed WAL SSTs below the replay
#   boundary, old manifests and old compactions files are reclaimed while
#   the writer keeps writing; every read reconciles.
# * GC respects checkpoints: a checkpoint pins its manifest's WAL range
#   ``(replay_after_wal_id, next_wal_sst_id)`` and that manifest's SSTs;
#   ``delete_checkpoint`` + a later GC reclaims them.
# * Compacted-SST reclamation is DEFERRED by design: the compactor writes a
#   self-checkpoint (``compactor_options.checkpoint_lifetime``, default
#   900s) around every compaction commit, and compacted GC's cutoff is the
#   minimum of min_age, the compaction low watermark and the newest L0
#   timestamp — an empty compactions file yields the Unix epoch and disables
#   compacted deletion outright.
# * None of this false-fences the writer: external GC (which rewrites the
#   manifest to drop expired checkpoints), checkpoint create/delete and
#   writer-executed external compactions all leave ``writer_epoch``
#   untouched; the writer never sees ``Closed(FENCED)``.


def _walk(root: str) -> list[str]:
    """Every file under ``root``, relative paths, sorted (the file:/// store's
    object listing)."""
    out = []
    for dirpath, _, files in os.walk(root):
        for name in files:
            out.append(os.path.relpath(os.path.join(dirpath, name), root))
    return sorted(out)


def _gc_options(min_age_ms: int = 0) -> GarbageCollectorOptions:
    """Full-coverage one-shot GC options: every directory task enabled,
    ``min_age`` zeroed (tests want immediate eligibility), no dry run.
    Boundary files stay ENABLED (the list/delete race guard)."""

    def dir_opts() -> GarbageCollectorDirectoryOptions:
        return GarbageCollectorDirectoryOptions(min_age_ms=min_age_ms, dry_run=False)

    return GarbageCollectorOptions(
        manifest_options=dir_opts(),
        wal_options=dir_opts(),
        wal_fence_options=dir_opts(),
        compacted_options=dir_opts(),
        compactions_options=dir_opts(),
        detach_options=GarbageCollectorScheduleOptions(),
    )


def _maintenance_settings(**overrides: str) -> Settings:
    """Default settings with a fast WAL flush ticker, plus dotted-path
    overrides (e.g. ``compactor_options="null"`` disables the internal
    compactor)."""
    s = Settings.default()
    s.set("flush_interval", '"5ms"')
    for key, value_json in overrides.items():
        s.set(key.replace("__", "."), value_json)
    return s


async def _flush_memtable(db: Any) -> None:
    """Flush the memtable to L0 (plain ``db.flush()`` is WAL-only)."""
    await db.flush_with_options(FlushOptions(flush_type=FlushType.MEM_TABLE))


async def _await_until(
    predicate: Callable[[], Awaitable[bool]], *, timeout_s: float = 30.0
) -> None:
    """Poll ``predicate`` until it holds; fail with the deadline, not a
    wall-clock sleep — the outcome is deterministic, only the wait is
    elastic."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while True:
        if await predicate():
            return
        assert loop.time() < deadline, f"condition not reached within {timeout_s}s"
        await asyncio.sleep(0.05)


async def _build(path: str, store: ObjectStore, settings: Settings) -> Any:
    builder = DbBuilder(path, store)
    builder.with_settings(settings)
    return await builder.build()


@pytest.mark.integration
async def test_submit_compaction_requires_a_running_compactor(tmp_path):
    """(0.17.0) ``submit_compaction`` against a DB whose writer runs with
    ``compactor_options`` disabled FAILS with ``Error.Data("invalid DB
    state")``: the call only persists a ``Submitted`` record into the
    ``.compactions`` store, and that store is created by a compactor — no
    compactor ever ran here, so there is nothing to enqueue into.

    The writer is completely unaffected by the failed submission. This is
    the negative half of the external-compaction finding: with the binding
    exposing no compactor runner, disabling the writer's compactor makes
    even SCHEDULING impossible.
    """
    store = ObjectStore.resolve("file:///")
    path = str(tmp_path / "db")
    db = await _build(path, store, _maintenance_settings(compactor_options="null"))
    for i in range(2):
        await db.put(f"k{i}".encode(), b"v")
        await _flush_memtable(db)

    admin = AdminBuilder(path, store).build()
    assert await admin.read_compactions(None) is None  # no compactions store exists
    view = await admin.read_compactor_state_view()
    assert view.compactions is None and len(view.manifest.l0) == 2
    spec = CompactionSpec.TIERED(
        segment=b"",
        sources=[SourceId.SST_VIEW(v.id) for v in view.manifest.l0],
        destination=0,
    )
    with pytest.raises(SlateError.Data, match="invalid DB state"):
        await admin.submit_compaction(spec)

    # the writer is undisturbed by the refused submission
    await db.put(b"after", b"1")
    await _flush_memtable(db)
    assert await db.get(b"k0") == b"v"
    assert await db.get(b"after") == b"1"
    await db.shutdown()


@pytest.mark.integration
async def test_external_submit_compaction_is_executed_by_the_writers_compactor(
    tmp_path,
):
    """(0.17.0) With the writer's internal compactor ENABLED, an external
    ``Admin.submit_compaction`` is picked up on the writer's compactor poll
    and EXECUTED BY THE WRITER: the returned record starts ``SUBMITTED``,
    transitions through ``SCHEDULED`` and reaches a terminal ``COMPLETED``
    state; the manifest's L0 drains into one sorted run.

    Two facts make this the writer's work, not the Admin's: the Admin API
    has no executor at all (the binding wraps no ``run_compactor``), and
    nothing else in this test process touches the DB. Consequence for
    millrace: external submission can only SCHEDULE — the merge's CPU and
    object-store traffic stay on the replica hosting the writer.

    The writer keeps writing across the externally-driven compaction, and
    ``writer_epoch`` is untouched by compactor commits (the compactor
    fences on ``compactor_epoch`` instead) — no false fencing.
    """
    store = ObjectStore.resolve("file:///")
    path = str(tmp_path / "db")
    db = await _build(
        path,
        store,
        _maintenance_settings(
            manifest_poll_interval='"100ms"', compactor_options__poll_interval='"100ms"'
        ),
    )
    admin = AdminBuilder(path, store).build()
    # the writer's compactor initializes the .compactions store at startup
    await _await_until(lambda: _compactions_exist(admin))
    epoch_at_open = (await admin.read_manifest(None)).writer_epoch

    for i in range(4):
        await db.put(f"k{i}".encode(), b"v" * 100)
        await _flush_memtable(db)
    view = await admin.read_compactor_state_view()
    assert len(view.manifest.l0) == 4
    spec = CompactionSpec.TIERED(
        segment=b"",
        sources=[SourceId.SST_VIEW(v.id) for v in view.manifest.l0],
        destination=0,
    )
    submitted = await admin.submit_compaction(spec)
    assert submitted.status is CompactionStatus.SUBMITTED

    async def committed() -> bool:
        # The record's lifecycle is SUBMITTED -> SCHEDULED -> RUNNING ->
        # COMPACTED (worker done, output written) -> COMPLETED (the
        # coordinator committed the manifest update). Wait for the
        # user-visible effect — the manifest commit — not the record.
        manifest = await admin.read_manifest(None)
        return len(manifest.l0) == 0 and len(manifest.compacted) == 1

    await _await_until(committed)
    record = await admin.read_compaction(submitted.id, None)
    # observed terminal state on 0.17.0: COMPLETED (never FAILED)
    assert record.status is CompactionStatus.COMPLETED

    manifest = await admin.read_manifest(None)
    assert len(manifest.l0) == 0
    assert len(manifest.compacted) == 1  # the four L0 SSTs merged into one run
    assert manifest.writer_epoch == epoch_at_open

    # the writer keeps writing cleanly across the externally-driven merge
    for i in range(4, 6):
        await db.put(f"k{i}".encode(), b"w" * 100)
        await _flush_memtable(db)
    for i in range(4):
        assert await db.get(f"k{i}".encode()) == b"v" * 100
    for i in range(4, 6):
        assert await db.get(f"k{i}".encode()) == b"w" * 100
    assert (await admin.read_manifest(None)).writer_epoch == epoch_at_open
    await db.shutdown()


async def _compactions_exist(admin: Any) -> bool:
    return await admin.read_compactions(None) is not None


@pytest.mark.integration
async def test_compactor_disabled_stalls_memtable_flushes_at_l0_max_ssts(tmp_path):
    """(0.17.0) A writer with ``compactor_options`` disabled stalls its
    memtable->L0 flush once L0 holds ``l0_max_ssts`` SSTs — L0 backpressure
    with nothing left to drain it. WAL writes still land (the stall is
    specific to the L0 flush), and ``shutdown_with_options`` with no final
    flush closes cleanly and releases the stalled flush.

    This is why the maintenance design cannot disable the writer's
    compactor even though the setting exists: with this binding there is no
    external executor to take over, so a compactor-less writer wedges at
    ``l0_max_ssts``.
    """
    store = ObjectStore.resolve("file:///")
    path = str(tmp_path / "db")
    db = await _build(
        path, store, _maintenance_settings(compactor_options="null", l0_max_ssts="2")
    )
    for i in range(2):
        await db.put(f"k{i}".encode(), b"v")
        await _flush_memtable(db)

    await db.put(b"k2", b"v")
    stalled = asyncio.create_task(_flush_memtable(db))
    await asyncio.sleep(0.2)  # flush latency baseline here is ~5ms
    assert not stalled.done(), (
        "the third memtable flush should stall on L0 backpressure"
    )

    # WAL writes are unaffected: the backpressure is specific to L0
    handle = await db.put(b"probe", b"x")
    await handle.await_durable()

    await db.shutdown_with_options(CloseOptions(flush_type=None))
    # shutdown releases the stalled flush (it resolves or raises, but the
    # process is never wedged)
    done, _ = await asyncio.wait([stalled], timeout=5)
    assert done, "the stalled flush was not released by shutdown"


@pytest.mark.integration
async def test_external_gc_concurrent_with_active_writer(tmp_path):
    """(0.17.0) ``Admin.run_gc_once`` executes synchronously in the caller's
    process and is safe against an ACTIVE writer. Interleaved rounds of
    writer put+memtable-flush and external GC with ``min_age_ms=0``:

    - every GC run strictly shrinks the WAL directory once garbage exists:
      afterwards exactly the WAL SSTs at/above the latest manifest's
      ``replay_after_wal_id`` remain (the boundary SST is kept by the
      ``Included(replay_after)`` rule);
    - the writer never errors, and every staged key reads back after every
      round;
    - a fresh open after a final GC replays exactly the same data (GC never
      deletes live objects).

    Compaction is disabled to keep the object census deterministic — the
    internal compactor's own commits and self-checkpoints would add moving
    parts; their interaction with GC is covered by
    ``test_external_admin_activity_does_not_fence_the_writer``.
    """
    store = ObjectStore.resolve("file:///")
    path = str(tmp_path / "db")
    db = await _build(
        path, store, _maintenance_settings(compactor_options="null", l0_max_ssts="1000")
    )
    admin = AdminBuilder(path, store).build()

    expected: dict[bytes, bytes] = {}
    for round_ in range(5):
        for i in range(3):
            key = f"k{round_}-{i}".encode()
            value = f"v{round_}-{i}".encode() * 20
            await db.put(key, value)
            expected[key] = value
            await _flush_memtable(db)
        manifest = await admin.read_manifest(None)
        await admin.run_gc_once(_gc_options())
        wal_ids = {
            int(name.removesuffix(".sst"))
            for name in os.listdir(tmp_path / "db" / "wal")
        }
        assert wal_ids
        assert min(wal_ids) >= manifest.replay_after_wal_id
        assert len(wal_ids) <= 2  # the replay boundary SST + the open one
        for key, value in expected.items():
            assert await db.get(key) == value

    # a fresh open after one more GC replays everything
    await db.shutdown()
    await admin.run_gc_once(_gc_options())
    reopened = await _build(
        path, store, _maintenance_settings(compactor_options="null")
    )
    for key, value in expected.items():
        assert await reopened.get(key) == value
    await reopened.shutdown()


@pytest.mark.integration
async def test_gc_respects_checkpoints_and_reclaims_after_release(tmp_path):
    """(0.17.0) A taken checkpoint pins exactly the WAL range
    ``(replay_after_wal_id, next_wal_sst_id)`` of the manifest it
    references: those SSTs survive ``run_gc_once`` while the checkpoint
    lives, and are reclaimed by the next GC after ``delete_checkpoint``.
    WAL SSTs at/below the checkpoint's replay boundary are NOT pinned —
    their content is already in the checkpoint's L0 SSTs.

    This is the disaster-recovery contract (docs/kafka-ingestion.md §Open
    questions: checkpoint/clone as the DR story for staged state) — the
    external GC service never deletes what a checkpoint can still replay.
    """
    store = ObjectStore.resolve("file:///")
    path = str(tmp_path / "db")
    db = await _build(path, store, _maintenance_settings(compactor_options="null"))
    admin = AdminBuilder(path, store).build()

    for i in range(3):
        await db.put(f"a{i}".encode(), b"1" * 40)
        await _flush_memtable(db)
    checkpoint = await admin.create_detached_checkpoint(
        CheckpointOptions(lifetime_ms=None, source=None, name="gc-pin")
    )
    pinned_manifest = await admin.read_manifest(checkpoint.manifest_id)
    lo, hi = pinned_manifest.replay_after_wal_id, pinned_manifest.next_wal_sst_id
    pinned_wal = {f"{i:020}.sst" for i in range(lo + 1, hi)}
    assert pinned_wal, "the checkpoint should pin at least one WAL SST"

    for i in range(3):
        await db.put(f"b{i}".encode(), b"2" * 40)
        await _flush_memtable(db)

    await admin.run_gc_once(_gc_options())
    survivors = set(os.listdir(tmp_path / "db" / "wal"))
    assert pinned_wal <= survivors, "GC deleted a checkpoint-pinned WAL SST"

    # the writer keeps writing while the checkpoint pins history
    await db.put(b"after-gc", b"x")
    await _flush_memtable(db)

    await admin.delete_checkpoint(checkpoint.id)
    await admin.run_gc_once(_gc_options())
    remaining = set(os.listdir(tmp_path / "db" / "wal"))
    assert not (pinned_wal & remaining), "released checkpoint's WAL SSTs not reclaimed"

    for i in range(3):
        assert await db.get(f"a{i}".encode()) == b"1" * 40
        assert await db.get(f"b{i}".encode()) == b"2" * 40
    assert await db.get(b"after-gc") == b"x"
    await db.shutdown()


@pytest.mark.integration
async def test_external_admin_activity_does_not_fence_the_writer(tmp_path):
    """(0.17.0) The CRITICAL fencing interaction: an external maintenance
    process commits manifest/compactions writes of its own — GC's
    expired-checkpoint removal rewrites the manifest, checkpoint
    create/delete rewrite it, and submitted compactions (executed by the
    writer's compactor) commit with a ``compactor_epoch`` bump. None of
    these may be misread by the active writer as a newer writer taking over
    (the lazy ``Closed(FENCED)`` pinned by the contested-open probes above).

    Observed: across a churn loop of external GC runs (with the compactor's
    self-checkpoint lifetime at 1s, so GC manifest rewrites actually fire),
    checkpoint create/delete cycles and external submissions, the writer
    wrote every batch cleanly, ``writer_epoch`` never changed, and every
    key reconciles. One submit failed with ``Error.Invalid("invalid clock
    tick, must be monotonic")`` — a same-millisecond ULID collision between
    independent submitters; transient and retryable, never a fencing event.
    """
    store = ObjectStore.resolve("file:///")
    path = str(tmp_path / "db")
    db = await _build(
        path,
        store,
        _maintenance_settings(
            manifest_poll_interval='"50ms"',
            compactor_options__poll_interval='"100ms"',
            compactor_options__checkpoint_lifetime='"1s"',
        ),
    )
    admin = AdminBuilder(path, store).build()
    epoch_at_open = (await admin.read_manifest(None)).writer_epoch

    expected: dict[bytes, bytes] = {}
    stop = asyncio.Event()
    counts = {"gc": 0, "checkpoints": 0, "submits": 0, "clock_tick_retries": 0}

    async def churn() -> None:
        while not stop.is_set():
            await admin.run_gc_once(_gc_options())
            counts["gc"] += 1
            created = await admin.create_detached_checkpoint(
                CheckpointOptions(lifetime_ms=None, source=None, name="churn")
            )
            await admin.delete_checkpoint(created.id)
            counts["checkpoints"] += 1
            view = await admin.read_compactor_state_view()
            if len(view.manifest.l0) >= 2:
                try:
                    await admin.submit_compaction(
                        CompactionSpec.TIERED(
                            segment=b"",
                            sources=[SourceId.SST_VIEW(v.id) for v in view.manifest.l0],
                            destination=0,
                        )
                    )
                    counts["submits"] += 1
                except SlateError.Invalid as e:
                    # same-ms ULID tick collision between independent
                    # submitters; transient, never fencing
                    assert "clock tick" in str(e)
                    counts["clock_tick_retries"] += 1
            await asyncio.sleep(0.02)

    churn_task = asyncio.create_task(churn())
    try:
        for i in range(20):
            key = f"k{i:03}".encode()
            value = f"w{i}".encode() * 30
            await db.put(key, value)
            expected[key] = value
            if i % 2:
                await _flush_memtable(db)
            await asyncio.sleep(0.02)
        await _flush_memtable(db)
    finally:
        stop.set()
        await churn_task

    assert counts["gc"] >= 3 and counts["checkpoints"] >= 3  # the churn was real
    for key, value in expected.items():
        assert await db.get(key) == value
    handle = await db.put(b"post-churn", b"1")
    await handle.await_durable()
    assert (await admin.read_manifest(None)).writer_epoch == epoch_at_open == 1
    await db.shutdown()


@pytest.mark.integration
async def test_compacted_sst_gc_waits_for_compactor_checkpoint_then_reclaims(tmp_path):
    """(0.17.0) Compacted-SST reclamation is DEFERRED by design: the
    compactor writes a self-checkpoint (default lifetime 900s) on every
    compaction commit so its inputs stay GC-safe during the commit, and
    compacted GC's cutoff is ``min(min_age cutoff, oldest retained
    compaction's start, newest L0 timestamp)`` — with an EMPTY compactions
    file the low watermark is the Unix epoch and nothing in ``compacted/``
    is eligible at all (all conservatisms verified in the same runs).

    Two DBs keep every assertion convergent (no wall-clock windows):

    - DB-A (checkpoint_lifetime 3600s — cannot expire during the test):
      right after a compaction the manifest carries the self-checkpoint,
      and external GC with min_age 0 reclaims NONE of the
      now-unreferenced input SSTs, deterministically.
    - DB-B (checkpoint_lifetime 1s): after expiry, later ``run_gc_once``
      calls remove the expired checkpoint (the GC process rewrites the
      manifest) and reclaim the inputs — all but the SST occupying the
      manifest's ``last_compacted_l0_sst_view_id`` slot: the cutoff filter
      is STRICTLY older than the newest-L0 barrier, a one-SST conservatism
      lag against "an L0 written but not yet visible in the manifest". A
      newer flush moves the barrier and the old one is reclaimed too —
      the lag is bounded, not a leak. The live writer is unaffected
      throughout.
    """
    store = ObjectStore.resolve("file:///")

    # -- DB-A: the pin, with an unexpireable checkpoint --------------------------
    path_a = str(tmp_path / "dba")
    db_a = await _build(
        path_a,
        store,
        _maintenance_settings(
            manifest_poll_interval='"100ms"',
            compactor_options__poll_interval='"100ms"',
            compactor_options__checkpoint_lifetime='"3600s"',
        ),
    )
    admin_a = AdminBuilder(path_a, store).build()
    for i in range(4):
        await db_a.put(f"k{i}".encode(), b"v" * 50)
        await _flush_memtable(db_a)

    async def compacted_a() -> bool:
        manifest = await admin_a.read_manifest(None)
        return bool(manifest.compacted) and not manifest.l0

    await _await_until(compacted_a)
    disk_a = set(os.listdir(tmp_path / "dba" / "compacted"))
    assert len(disk_a) > 1  # inputs + output
    manifest_a = await admin_a.read_manifest(None)
    assert manifest_a.checkpoints, "expected the compactor's self-checkpoint"
    await admin_a.run_gc_once(_gc_options())
    # the self-checkpoint cannot expire during this test (lifetime 3600s),
    # so the pin is total, deterministically
    assert set(os.listdir(tmp_path / "dba" / "compacted")) == disk_a
    for i in range(4):
        assert await db_a.get(f"k{i}".encode()) == b"v" * 50
    await db_a.shutdown()

    # -- DB-B: expiry -> reclamation, and the barrier lag -------------------------
    path = str(tmp_path / "dbb")
    db = await _build(
        path,
        store,
        _maintenance_settings(
            manifest_poll_interval='"100ms"',
            compactor_options__poll_interval='"100ms"',
            compactor_options__checkpoint_lifetime='"1s"',
        ),
    )
    admin = AdminBuilder(path, store).build()
    for i in range(6):
        await db.put(f"k{i}".encode(), b"v" * 50)
        await _flush_memtable(db)

    async def compacted() -> bool:
        manifest = await admin.read_manifest(None)
        return bool(manifest.compacted) and not manifest.l0

    await _await_until(compacted)

    def compacted_ssts() -> set[str]:
        return set(os.listdir(tmp_path / "dbb" / "compacted"))

    before = compacted_ssts()
    assert len(before) > 1  # inputs + output

    # after the 1s self-checkpoint expires, a later GC removes it and
    # reclaims the inputs. Convergence is the fixpoint: on disk == live set
    # ∪ {current barrier SST} (the quiescent writer's compactor may cascade,
    # each generation pinned by its own short-lived self-checkpoint).
    async def converged() -> bool:
        await admin.run_gc_once(_gc_options())
        manifest = await admin.read_manifest(None)
        live = {v.sst.id.value for r in manifest.compacted for v in r.sst_views}
        on_disk = {name.removesuffix(".sst") for name in compacted_ssts()}
        return on_disk == live | {manifest.last_compacted_l0_sst_view_id}

    await _await_until(converged, timeout_s=30.0)
    assert compacted_ssts() < before, "no compacted SST was ever reclaimed"
    first_barrier = (await admin.read_manifest(None)).last_compacted_l0_sst_view_id

    # a newer flush moves the barrier — the size-tiered scheduler needs
    # ``min_compaction_sources`` (4 by default) L0 SSTs, so flush four:
    # once the next compaction's self-checkpoint has also expired, the
    # first barrier SST is reclaimed too
    for i in range(4):
        await db.put(f"k-new-{i}".encode(), b"v" * 50)
        await _flush_memtable(db)

    async def barrier_moved() -> bool:
        manifest = await admin.read_manifest(None)
        return manifest.last_compacted_l0_sst_view_id != first_barrier

    await _await_until(barrier_moved)

    async def old_barrier_reclaimed() -> bool:
        await admin.run_gc_once(_gc_options())
        on_disk = {name.removesuffix(".sst") for name in compacted_ssts()}
        return first_barrier not in on_disk

    await _await_until(old_barrier_reclaimed, timeout_s=30.0)

    for i in range(6):
        assert await db.get(f"k{i}".encode()) == b"v" * 50
    for i in range(4):
        assert await db.get(f"k-new-{i}".encode()) == b"v" * 50
    await db.shutdown()


@pytest.mark.integration
async def test_run_gc_once_on_missing_or_foreign_path_is_a_silent_noop(tmp_path):
    """(0.17.0) The containment contract the maintenance service relies on:

    - ``run_gc_once`` NEVER raises for a path with no manifest (missing or
      foreign): each GC task's error is logged inside the binding and
      swallowed, and the call returns Ok — so an Ok return is NOT evidence
      a DB was GC'd. The service probes ``read_manifest`` first (None = no
      DB here, skip) precisely because GC cannot say it.
    - ``read_manifest(None)`` returns None for a missing path AND for a
      directory holding foreign objects (no manifest among them).
    - ``delete_db(confirm=True)`` REFUSES a manifestless non-empty path
      (``Error.Data("invalid DB state")``) — the fat-finger guard.
    """
    store = ObjectStore.resolve("file:///")

    missing = AdminBuilder(str(tmp_path / "never-existed"), store).build()
    assert await missing.read_manifest(None) is None
    await missing.run_gc_once(_gc_options())  # silent no-op, no error

    junk_path = tmp_path / "foreign"
    (junk_path / "data").mkdir(parents=True)
    (junk_path / "data" / "part-0.parquet").write_bytes(b"PAR1")
    foreign = AdminBuilder(str(junk_path), store).build()
    assert await foreign.read_manifest(None) is None
    await foreign.run_gc_once(_gc_options())  # still a no-op...
    assert (junk_path / "data" / "part-0.parquet").read_bytes() == b"PAR1"  # untouched
    with pytest.raises(SlateError.Data, match="invalid DB state"):
        await foreign.delete_db(True)
