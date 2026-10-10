"""External maintenance for millrace's per-partition SlateDB staging instances.

Deployed BESIDE a millrace fleet (its own process, its own pod): it loops over
ALL per-partition SlateDB instances across topics and replicas — discovered by
prefix listing under the staging root — so the replicas, already hosting many
DBs, shed the collection traffic.

SCOPE: GARBAGE COLLECTION + COMPACTION-DEBT MONITORING. There is deliberately
no compaction EXECUTION loop here. The binding-parity suite
(tests/test_slatedb_parity.py, "external maintenance" section — the
load-bearing probes, each pinning observed 0.17.0 behavior) established:

* ``Admin.submit_compaction`` only ENQUEUES a ``Submitted`` record into the
  DB's ``.compactions`` store; execution needs a compactor process. The Rust
  core has one (``Admin::run_compactor`` / ``run_compaction_worker``) but the
  slatedb 0.17.0 Python binding does not wrap it (verified at the FFI symbol
  level). External processes can therefore SCHEDULE compactions — the writer's
  internal compactor picks them up and executes them — but can never EXECUTE
  them: the merge's CPU and object-store traffic would stay on the replicas,
  which is exactly what this service exists to shed. Worse, against a writer
  running with its compactor disabled the submission FAILS
  (``Error.Data("invalid DB state")``: the compactions store is created by a
  running compactor).
* Disabling the writer's compactor (``compactor_options = null``) is accepted
  by the settings parser but unsafe: memtable→L0 flushes STALL once L0 holds
  ``l0_max_ssts`` SSTs and nothing external can drain them. Writers must keep
  their internal compactor.

If a future binding exposes a compactor runner, the red parity tests are the
migration guide for growing the compaction half of this service.

The DEBT half exists because this service is the only component that sees
every DB: per swept DB it reads the compactor state view
(``Admin.read_compactor_state_view``) and exports the L0 SST count as
``millrace_maintenance_compaction_l0_ssts{topic,partition}`` plus the fleet
max — a writer's embedded compactor falling behind shows HERE first (L0
approaching ``l0_max_ssts`` is the write-stall precursor). Per-DB labels are
fine HERE (PR #331): this service is the fleet authority on debt, so its
cardinality is the fleet's DB count, not a pod's hot-path labels. The read is
one manifest+compactions fetch per DB, ridden on the sweep's existing
``max_dbs_per_sweep`` budget (a DB's series is as fresh as its last rotation
slot — stated, not hidden).

GC is safe to run externally and concurrently with active writers (pinned by
the parity suite): ``Admin.run_gc_once`` executes synchronously in THIS
process, never deletes objects referenced by the latest manifest, any
checkpoint, or in-flight compactions (its cutoffs are conservative: the
compactor's self-checkpoint — ``compactor_options.checkpoint_lifetime``,
default 900s — pins a compaction's inputs after the commit, and an empty
compactions file disables compacted-SST deletion outright).

CONTAINMENT CONTRACT (pinned): ``run_gc_once`` NEVER raises for a missing or
foreign path — per-task errors are logged inside the binding and swallowed —
so an Ok return is no evidence a DB was GC'd. The sweep therefore probes
``read_manifest`` first: ``None`` means "no DB here" (vanished between
discovery and sweep — skipped, counted); an exception (corrupt manifest) is
the contained per-DB failure. (``read_compactor_state_view`` cannot replace
the probe: it raises ``Error.Data`` for a missing DB AND for a corrupt one —
probe-verified — so it cannot tell "vanished" apart from "broken".)

WRITER SETTINGS (owned by config.py / stage.py — the note this docstring used
to carry as a TODO is wired): writers run with
``MILLRACE_SLATEDB_GC_ENABLED=false`` by default — the writer-internal GC
loop is OFF because this service owns collection (doubled GC is wasted
LIST/DELETE traffic on the replicas; GC is idempotent and overlap-safe, so a
misconfigured doubling is wasteful, never corrupt) — and there is no knob at
all for ``compactor_options`` (above). ``MILLRACE_SLATEDB_MAX_UNFLUSHED_BYTES``
/ ``MILLRACE_SLATEDB_L0_MAX_SSTS`` tune the per-instance memory/stall shape.

DISCOVERY IS CACHED: a full recursive listing of the staging root runs at
most once per ``discovery_interval_s`` (default 1 h); sweeps in between work
the CACHED set (a new partition's DB waits at most one discovery cadence for
its first sweep — harmless: a fresh DB has no garbage, and GC's own min_age
exceeds the listing lag; a VANISHED DB's manifest probe answers False and it
is skipped until the next listing drops it). A listing failure with a warm
cache degrades to the cache (counted); with a cold one it fails the
iteration, as before.

Scale doctrine (AGENT.md): sweeps are BUDGETED — at most
``max_dbs_per_sweep`` databases per sweep, paging through the sorted path
set in order (a cursor over successive sweeps, so a fleet larger than the
budget is covered in full every ⌈N/budget⌉ sweeps); a sweep that truncates
says so in its report (the ledger) and in the
``millrace_maintenance_sweep_truncated`` gauge. Per-DB failures are
contained (logged, counted, sweep continues). The cadence is jittered so N
replicas of this service (or N topics' fleets) do not synchronize their
sweeps against the object store.
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import re
import signal
import sys
import time
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Final, Protocol

from slatedb.uniffi import (
    Admin,
    AdminBuilder,
    GarbageCollectorDirectoryOptions,
    GarbageCollectorOptions,
    GarbageCollectorScheduleOptions,
    ObjectStore,
)

from .server import serve

log = logging.getLogger("millrace.maintenance")

EXIT_ERROR = 1
EXIT_CONFIG = 2

_TOPIC_SEGMENT: Final = re.compile(r"^[a-zA-Z0-9._-]{1,249}$")  # Kafka's rules
_DEFAULT_STAGE_BASE: Final = "millrace"


class MaintenanceError(Exception):
    """Base for maintenance-service failures."""


class MaintenanceConfigError(MaintenanceError):
    """Invalid startup configuration. ``problems`` names every bad knob at
    once (config.py's fail-fast idiom)."""

    def __init__(self, problems: list[str]) -> None:
        self.problems = tuple(problems)
        super().__init__(
            f"invalid millrace maintenance configuration ({len(problems)} problem(s)):\n"
            + "\n".join(f"  - {p}" for p in problems)
        )


# -- discovery ----------------------------------------------------------------

#: One listing of every object key under the staging base path. Keys are
#: store-relative: bucket-relative for ``s3://``, absolute filesystem paths
#: for ``file:///`` (matching what ``DbBuilder``/``AdminBuilder`` take).
Lister = Callable[[], Iterable[str]]


@dataclass(frozen=True, slots=True)
class DiscoveredDb:
    """One per-partition SlateDB instance under the staging root.

    ``path`` is exactly what :class:`AdminBuilder` takes; ``topic`` and
    ``partition`` are parsed from the ``{base}/{topic}/{partition}`` layout
    (docs/kafka-ingestion.md §Staging layout).
    """

    path: str
    topic: str
    partition: int


def parse_db_paths(keys: Iterable[str], base_path: str) -> list[DiscoveredDb]:
    """Pure: pick per-partition DB roots out of an object listing.

    A directory is a SlateDB instance iff it holds a manifest object, i.e. a
    key of the shape ``{base}/{topic}/{partition}/manifest/<id>.manifest``
    exists. Everything else (``wal/`` SSTs, ``compacted/`` SSTs, ``gc/``
    boundary files, foreign objects) is ignored — a foreign prefix without a
    manifest never enters the sweep set at all. ``partition`` must be a bare
    non-negative integer (Kafka layout); anything else is not a millrace
    staging DB. The result is sorted by path (the sweep cursor's stable
    order).
    """
    base = base_path.strip("/")
    found: dict[str, DiscoveredDb] = {}
    for key in keys:
        rel = key.strip("/")
        if base:
            if not rel.startswith(base + "/"):
                continue
            rel = rel[len(base) + 1 :]
        parts = rel.split("/")
        # {topic}/{partition}/manifest/{id}.manifest — exactly four segments
        if (
            len(parts) != 4
            or parts[2] != "manifest"
            or not parts[3].endswith(".manifest")
        ):
            continue
        topic, partition_raw = parts[0], parts[1]
        if not _TOPIC_SEGMENT.match(topic):
            continue
        if not partition_raw.isdigit():
            continue
        path = f"{base}/{topic}/{partition_raw}" if base else f"{topic}/{partition_raw}"
        found[path] = DiscoveredDb(path=path, topic=topic, partition=int(partition_raw))
    return [found[path] for path in sorted(found)]


def _walk_filesystem(base_path: str) -> list[str]:
    """The ``file:///`` lister: the object tree IS the local filesystem."""
    out: list[str] = []
    for dirpath, _, files in os.walk(base_path):
        for name in files:
            out.append(os.path.join(dirpath, name))
    return out


def _list_s3(store_url: str, base_path: str) -> list[str]:
    """The ``s3://`` lister, via pyarrow's S3 filesystem (already a millrace
    dependency — no new one). Credentials/endpoint come from the same ambient
    ``AWS_*`` environment SlateDB's own store resolution reads."""
    import pyarrow.fs as pafs

    bucket = store_url.removeprefix("s3://").strip("/")
    fs = pafs.S3FileSystem()  # ambient AWS_* chain, like ObjectStore.resolve
    # pyarrow S3 paths carry the bucket as their first segment
    prefix = f"{bucket}/{base_path.strip('/')}"
    selector = pafs.FileSelector(prefix, recursive=True)
    out: list[str] = []
    for info in fs.get_file_info(selector):
        # strip the bucket back off: slatedb paths are bucket-relative
        if info.is_file:
            out.append(info.path.removeprefix(f"{bucket}/"))
    return out


async def discover_databases(
    store_url: str,
    base_path: str,
    *,
    lister: Lister | None = None,
) -> list[DiscoveredDb]:
    """Enumerate every per-partition SlateDB DB under ``base_path``.

    ``lister`` is the test seam (discovery is a prefix listing; the slatedb
    binding exposes no object-store listing, so production defaults are an
    ``os.walk`` for ``file:///`` and a pyarrow S3 listing for ``s3://``).
    ``memory:///`` staging roots cannot be listed (per-process test stores —
    the parity suite pins that each resolve is isolated) and need the seam.
    The listing runs on a thread: it is blocking object-store I/O.
    """
    if lister is None:
        if store_url.startswith("file://"):
            lister = lambda: _walk_filesystem(base_path)
        elif store_url.startswith("s3://"):
            lister = lambda: _list_s3(store_url, base_path)
        else:
            raise MaintenanceError(
                f"no default lister for {store_url!r}: memory:/// stores are "
                "per-process and unlistable (tests inject a lister); use "
                "file:/// or s3:// staging roots"
            )
    keys = await asyncio.to_thread(lister)
    return parse_db_paths(keys, base_path)


# -- per-DB work seam -----------------------------------------------------------


class DbMaintainer(Protocol):
    """The per-DB maintenance operations one sweep needs. The production
    implementation is :class:`AdminMaintainer` over slatedb's ``Admin``;
    tests substitute fakes (the sweep's budgeting/containment logic is
    store-independent)."""

    async def has_manifest(self, path: str) -> bool:
        """Whether ``path`` currently holds a SlateDB manifest. ``False`` =
        no DB here (vanished between discovery and sweep); an exception = a
        DB that can't be read (contained by the sweep)."""
        ...

    async def read_l0_ssts(self, path: str) -> int:
        """The DB's current L0 SST count — the compaction-debt signal
        (L0 approaching the writer's ``l0_max_ssts`` is the write-stall
        precursor). Only called after :meth:`has_manifest` answered True;
        an exception is the contained per-DB failure."""
        ...

    async def run_gc(self, path: str) -> None:
        """One full GC pass over the DB at ``path``."""
        ...


class AdminMaintainer:
    """``DbMaintainer`` over slatedb's ``Admin`` binding.

    One ``Admin`` handle per DB per operation: ``AdminBuilder.build()`` is a
    pure constructor (no I/O), so there is nothing to cache. GC options are
    fixed for the process (min age applies to every directory task; boundary
    files — the list/delete race guard — stay enabled).
    """

    def __init__(self, store: ObjectStore, gc_options: GarbageCollectorOptions) -> None:
        self._store = store
        self._gc_options = gc_options

    def _admin(self, path: str) -> Admin:
        return AdminBuilder(path, self._store).build()

    async def has_manifest(self, path: str) -> bool:
        return await self._admin(path).read_manifest(None) is not None

    async def read_l0_ssts(self, path: str) -> int:
        # One manifest+compactions fetch; the compactor state view's
        # manifest arm carries the live L0 set. (It cannot replace the
        # has_manifest probe: it raises Error.Data for a missing DB and
        # a corrupt one alike — module docstring §containment contract.)
        view = await self._admin(path).read_compactor_state_view()
        return len(view.manifest.l0)

    async def run_gc(self, path: str) -> None:
        # NOTE: run_gc_once is synchronous-in-caller and NEVER raises for a
        # missing/foreign path (module docstring §containment contract) —
        # the sweep's has_manifest probe is what makes "GC returned Ok"
        # mean "a DB was actually swept".
        await self._admin(path).run_gc_once(self._gc_options)


def gc_options_for(min_age_ms: int) -> GarbageCollectorOptions:
    """Full-coverage one-shot GC options: every directory task enabled with
    the configured minimum age, no dry runs. ``detach_options`` has no age
    threshold (schedule-only). Boundary files stay at SlateDB's default
    (enabled) — they fence the list/delete race against concurrent writers,
    which is the entire operating mode here."""

    def directory() -> GarbageCollectorDirectoryOptions:
        return GarbageCollectorDirectoryOptions(min_age_ms=min_age_ms, dry_run=False)

    return GarbageCollectorOptions(
        manifest_options=directory(),
        wal_options=directory(),
        wal_fence_options=directory(),
        compacted_options=directory(),
        compactions_options=directory(),
        detach_options=GarbageCollectorScheduleOptions(),
    )


# -- the service ---------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class MaintenanceKnobs:
    """Tunables for the maintenance loops (validated at construction)."""

    gc_enabled: bool = True
    gc_interval_s: float = 600.0
    gc_min_age_ms: int = 300_000
    max_dbs_per_sweep: int = 100
    jitter: float = 0.2
    metrics_port: int = 8001
    discovery_interval_s: float = 3600.0

    def __post_init__(self) -> None:
        if self.gc_interval_s <= 0:
            raise ValueError(
                f"MILLRACE_MAINTENANCE_GC_INTERVAL_S must be > 0, "
                f"got {self.gc_interval_s}"
            )
        if self.gc_min_age_ms < 0:
            raise ValueError(
                f"MILLRACE_MAINTENANCE_GC_MIN_AGE_MS must be >= 0, "
                f"got {self.gc_min_age_ms}"
            )
        if self.max_dbs_per_sweep < 1:
            raise ValueError(
                f"MILLRACE_MAINTENANCE_MAX_DBS_PER_SWEEP must be >= 1, "
                f"got {self.max_dbs_per_sweep}"
            )
        if not 0.0 <= self.jitter < 1.0:
            raise ValueError(
                f"MILLRACE_MAINTENANCE_JITTER must be in [0, 1), got {self.jitter}"
            )
        if not 0 <= self.metrics_port <= 65535:
            raise ValueError(
                f"MILLRACE_MAINTENANCE_METRICS_PORT must be in 0-65535, "
                f"got {self.metrics_port}"
            )
        if self.discovery_interval_s <= 0:
            raise ValueError(
                f"MILLRACE_MAINTENANCE_DISCOVERY_INTERVAL_S must be > 0, "
                f"got {self.discovery_interval_s}"
            )


@dataclass(frozen=True, slots=True)
class SweepReport:
    """The ledger of one GC sweep (AGENT.md scale doctrine: bounded work per
    run, and the report records when the bound truncated).

    ``debt_samples`` counts the DBs whose compactor state view was read
    this sweep (the monitoring half — runs even with GC off).
    ``discovery_fresh`` is True when this sweep ran a full listing rather
    than working the cached set (discovery cadence, module docstring).
    """

    discovered: int
    budgeted: int
    gc_ok: int
    gc_failed: int
    skipped_no_manifest: int
    truncated: bool
    gc_enabled: bool
    debt_samples: int = 0
    discovery_fresh: bool = True


@dataclass(frozen=True, slots=True)
class MaintenanceStats:
    """Cumulative counters + the last sweep's report (the metrics
    collector's snapshot; passive observability — nothing feeds control
    flow). ``l0_ssts`` is the debt read's retained per-DB map —
    ``{db path: (topic, partition, l0_sst_count)}``, as of each DB's
    last sweep (pruned to the discovered set at each fresh listing).
    """

    sweeps_total: int
    sweep_failures_total: int
    gc_runs_total: int
    gc_failures_total: int
    gc_skipped_total: int
    last_discovered: int
    last_truncated: bool
    discovery_listings_total: int = 0
    discovery_stale_cache_uses_total: int = 0
    l0_ssts: Mapping[str, tuple[str, int, int]] = MappingProxyType({})


class MaintenanceService:
    """The GC + debt-monitoring loop over every discovered per-partition DB.

    One sweep: discover (a full listing at most once per
    ``discovery_interval_s``; the cached set in between — module
    docstring) → take the next ``max_dbs_per_sweep`` page of the sorted
    path set (a cursor over successive sweeps, so a fleet larger than the
    budget gets full coverage every ⌈N/budget⌉ sweeps) → per DB, probe
    the manifest, read the compactor state view (the debt sample), then
    one ``run_gc_once`` when GC is enabled. Per-DB failures are contained
    (logged, counted, sweep continues); a discovery failure fails the
    ITERATION (logged, counted, loop keeps running) — a store outage never
    kills the process, and with a warm cache the sweep degrades to it.

    Clocks and randomness are injected: ``sleep`` and ``rand`` make cadence
    and jitter deterministic in tests, ``monotonic`` backs the discovery
    cadence; nothing here reads the wall clock. The default ``sleep`` is
    interruptible by :meth:`stop` — with a 10-minute production interval an
    uninterruptible pause would hold a SIGTERM past the orchestrator's
    grace period.
    """

    def __init__(
        self,
        *,
        store_url: str,
        base_path: str,
        knobs: MaintenanceKnobs,
        maintainer: DbMaintainer,
        lister: Lister | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
        rand: random.Random | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._store_url = store_url
        self._base_path = base_path
        self._knobs = knobs
        self._maintainer = maintainer
        self._lister = lister
        self._stop_event = asyncio.Event()
        self._sleep = sleep if sleep is not None else self._interruptible_sleep
        self._rand = rand if rand is not None else random.Random()
        self._monotonic = monotonic
        self._cursor = 0
        self._stopping = False
        self._sweeps_total = 0
        self._sweep_failures_total = 0
        self._gc_runs_total = 0
        self._gc_failures_total = 0
        self._gc_skipped_total = 0
        self._discovery_listings_total = 0
        self._stale_cache_uses = 0
        # The discovery cache (module docstring): None until the first
        # successful listing; ``_listed_at`` is its monotonic stamp.
        self._known_dbs: list[DiscoveredDb] | None = None
        self._listed_at: float | None = None
        # The retained debt read per DB: path -> (topic, partition, l0).
        # Pruned to the discovered set at each fresh listing.
        self._l0_ssts: dict[str, tuple[str, int, int]] = {}
        self._last_report = SweepReport(0, 0, 0, 0, 0, False, knobs.gc_enabled)

    async def _interruptible_sleep(self, seconds: float) -> None:
        """The production pause: ends early when :meth:`stop` lands."""
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
        except TimeoutError:
            pass

    async def _discover(self) -> tuple[list[DiscoveredDb], bool]:
        """The sweep's DB set: a full listing when the cache is cold or
        stale (``discovery_interval_s``), the cached set otherwise.
        Returns ``(dbs, fresh)``. A listing failure with a warm cache
        degrades to the cache (counted); with a cold cache it propagates
        (the loop contains it as an iteration failure)."""
        now = self._monotonic()
        cache = self._known_dbs
        if (
            cache is not None
            and self._listed_at is not None
            and now - self._listed_at < self._knobs.discovery_interval_s
        ):
            return cache, False  # fresh cache: no listing this sweep
        try:
            dbs = await discover_databases(
                self._store_url, self._base_path, lister=self._lister
            )
        except Exception:
            if cache is not None:
                self._stale_cache_uses += 1
                log.warning(
                    "discovery listing failed; sweeping the cached set of %d "
                    "DBs (last listed %.0fs ago)",
                    len(cache),
                    now - (self._listed_at or now),
                )
                return cache, False
            raise
        self._known_dbs = dbs
        self._listed_at = now
        self._discovery_listings_total += 1
        # Retire debt series of DBs no longer discovered.
        live = {db.path for db in dbs}
        self._l0_ssts = {p: v for p, v in self._l0_ssts.items() if p in live}
        return dbs, True

    async def sweep_once(self) -> SweepReport:
        """One budgeted sweep. Discovery errors propagate (the loop
        contains them); per-DB errors never do."""
        dbs, fresh = await self._discover()
        budgeted = self._budget(dbs)
        ok = failed = skipped = debt = 0
        for db in budgeted:
            try:
                if not await self._maintainer.has_manifest(db.path):
                    # vanished between listing and sweep — not an error
                    skipped += 1
                    continue
                # The debt sample rides every swept DB, GC on or off:
                # monitoring is not coupled to collection.
                l0 = await self._maintainer.read_l0_ssts(db.path)
                self._l0_ssts[db.path] = (db.topic, db.partition, l0)
                debt += 1
                if self._knobs.gc_enabled:
                    await self._maintainer.run_gc(db.path)
                    ok += 1
            except Exception:
                # Contained: one corrupt/unreachable DB never wedges the
                # sweep; it is retried on a later rotation.
                failed += 1
                log.exception("maintenance failed for %s", db.path)
        report = SweepReport(
            discovered=len(dbs),
            budgeted=len(budgeted),
            gc_ok=ok,
            gc_failed=failed,
            skipped_no_manifest=skipped,
            truncated=len(budgeted) < len(dbs),
            gc_enabled=self._knobs.gc_enabled,
            debt_samples=debt,
            discovery_fresh=fresh,
        )
        self._sweeps_total += 1
        self._gc_runs_total += ok
        self._gc_failures_total += failed
        self._gc_skipped_total += skipped
        self._last_report = report
        return report

    def _budget(self, dbs: list[DiscoveredDb]) -> list[DiscoveredDb]:
        """The next page of the rotation over the sorted set: contiguous
        pages of ``max_dbs_per_sweep`` (the last one short), restarting at
        the front once the tail is reached. Every DB is visited exactly once
        per pass — the fairest cadence per partition. A membership change
        mid-rotation clamps the cursor rather than skipping a cycle."""
        if self._cursor >= len(dbs):
            self._cursor = 0
        page = dbs[self._cursor : self._cursor + self._knobs.max_dbs_per_sweep]
        self._cursor += len(page)
        if self._cursor >= len(dbs):
            self._cursor = 0
        return page

    def next_delay(self) -> float:
        """The jittered pause before the next sweep: interval × (1 ± jitter),
        uniform. Jitter keeps N deployments' sweeps from synchronizing
        against the object store; ``jitter=0`` is the exact interval."""
        spread = self._knobs.gc_interval_s * self._knobs.jitter
        return self._knobs.gc_interval_s + self._rand.uniform(-spread, spread)

    async def run(self) -> None:
        """Sweep, pause (jittered), repeat until :meth:`stop`. A failed
        iteration is logged + counted and the loop keeps running."""
        while not self._stopping:
            try:
                report = await self.sweep_once()
                log.info(
                    "maintenance sweep: discovered=%d budgeted=%d ok=%d "
                    "failed=%d skipped_no_manifest=%d debt_samples=%d "
                    "truncated=%s discovery=%s",
                    report.discovered,
                    report.budgeted,
                    report.gc_ok,
                    report.gc_failed,
                    report.skipped_no_manifest,
                    report.debt_samples,
                    report.truncated,
                    "listed" if report.discovery_fresh else "cached",
                )
            except Exception:
                self._sweep_failures_total += 1
                log.exception("maintenance sweep failed; continuing")
            await self._sleep(self.next_delay())

    def stop(self) -> None:
        """Ask the loop to stop after the in-flight sweep, waking the
        between-sweep pause. Safe from a signal handler or another thread
        (``Event.set`` via ``add_signal_handler`` runs on the loop's
        thread)."""
        self._stopping = True
        self._stop_event.set()

    def stats(self) -> MaintenanceStats:
        """The cumulative counters + last sweep's truncation state."""
        return MaintenanceStats(
            sweeps_total=self._sweeps_total,
            sweep_failures_total=self._sweep_failures_total,
            gc_runs_total=self._gc_runs_total,
            gc_failures_total=self._gc_failures_total,
            gc_skipped_total=self._gc_skipped_total,
            last_discovered=self._last_report.discovered,
            last_truncated=self._last_report.truncated,
            discovery_listings_total=self._discovery_listings_total,
            discovery_stale_cache_uses_total=self._stale_cache_uses,
            l0_ssts=MappingProxyType(dict(self._l0_ssts)),
        )


# -- metrics --------------------------------------------------------------------


class _MaintenanceCollector:
    """Prometheus view of the service counters (main.py's _PipelineCollector
    idiom; reads only the stats snapshot, so the probe can never block on
    the sweep loop).

    The debt series — ``millrace_maintenance_compaction_l0_ssts
    {topic,partition}`` — is per-DB LABELED on purpose (the flip side of
    the pipeline's no-per-partition rule): this service is the fleet
    authority on compaction debt, one series per discovered DB, and a
    partition racing toward its writer's ``l0_max_ssts`` stall must be
    nameable. Freshness is per-DB last-swept (the rotation paces it);
    ``..._max`` is the fleet's worst.
    """

    def __init__(self, service: MaintenanceService) -> None:
        self._service = service

    def collect(self) -> Any:
        from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily

        s = self._service.stats()
        for name, doc, value in (
            (
                "millrace_maintenance_sweeps_total",
                "GC sweeps completed",
                s.sweeps_total,
            ),
            (
                "millrace_maintenance_sweep_failures_total",
                "Sweeps whose discovery/listing failed",
                s.sweep_failures_total,
            ),
            (
                "millrace_maintenance_gc_runs_total",
                "Per-DB GC passes completed",
                s.gc_runs_total,
            ),
            (
                "millrace_maintenance_gc_failures_total",
                "Per-DB GC passes that failed (contained)",
                s.gc_failures_total,
            ),
            (
                "millrace_maintenance_gc_skipped_total",
                "Discovered paths with no manifest at sweep time",
                s.gc_skipped_total,
            ),
            (
                "millrace_maintenance_discovery_listings_total",
                (
                    "Full staging-root listings run (the rest of the sweeps work "
                    "the cached set — see the discovery cadence knob)"
                ),
                s.discovery_listings_total,
            ),
            (
                "millrace_maintenance_discovery_stale_cache_total",
                (
                    "Sweeps that worked the CACHED set because the fresh "
                    "listing failed (a store outage hides fleet changes here)"
                ),
                s.discovery_stale_cache_uses_total,
            ),
        ):
            yield CounterMetricFamily(name, doc, value=value)
        yield GaugeMetricFamily(
            "millrace_maintenance_dbs_discovered",
            "SlateDB instances found by the last sweep",
            value=s.last_discovered,
        )
        yield GaugeMetricFamily(
            "millrace_maintenance_sweep_truncated",
            "Whether the last sweep's budget truncated the discovered set",
            value=1.0 if s.last_truncated else 0.0,
        )
        per_db = GaugeMetricFamily(
            "millrace_maintenance_compaction_l0_ssts",
            "L0 SST count of the staging instance, as of its last sweep "
            "(the write-stall precursor: the writer stalls when L0 reaches "
            "its l0_max_ssts)",
            labels=["topic", "partition"],
        )
        worst = 0
        for _path, (topic, partition, l0) in sorted(s.l0_ssts.items()):
            per_db.add_metric([topic, str(partition)], float(l0))
            worst = max(worst, l0)
        yield per_db
        yield GaugeMetricFamily(
            "millrace_maintenance_compaction_l0_ssts_max",
            "The fleet's worst per-DB L0 SST count (the write-stall "
            "precursor), as of each DB's last sweep",
            value=float(worst),
        )


# -- configuration / entry point ------------------------------------------------


@dataclass(frozen=True, slots=True)
class MaintenanceConfig:
    """Validated startup configuration for the maintenance process.

    ``store_url`` / ``base_path`` are the parsed halves of
    ``MILLRACE_MAINTENANCE_STAGE_URL`` and must name the SAME staging root
    the writers' ``MILLRACE_STAGE_URL`` does (the deployment wires them from
    one value; this process has its own env prefix because it is its own
    Deployment).
    """

    store_url: str
    base_path: str
    knobs: MaintenanceKnobs


# Mirrors config.py's _parse_stage_url grammar (slatedb's ObjectStore.resolve
# takes no path component, so the split is the caller's job). Kept local
# rather than imported: config.py's helper is private and owned elsewhere in
# the tree; the grammar is 20 lines. Keep in step with it.
def _parse_stage_url(raw: str) -> tuple[str, str] | None:
    if raw.startswith("memory://"):
        rest = raw[len("memory://") :].strip("/")
        return "memory:///", rest or _DEFAULT_STAGE_BASE
    if raw.startswith("file://"):
        rest = raw[len("file://") :].rstrip("/")
        if not rest.strip("/"):
            return None
        return "file:///", rest
    if raw.startswith("s3://"):
        rest = raw[len("s3://") :]
        bucket, _, prefix = rest.partition("/")
        if not bucket or not prefix.strip("/"):
            return None
        return f"s3://{bucket}", prefix.strip("/")
    return None


def _bool(
    env: Mapping[str, str], name: str, default: bool, problems: list[str]
) -> bool:
    raw = env.get(name, "").strip().lower()
    if not raw:
        return default
    if raw in ("true", "1", "yes"):
        return True
    if raw in ("false", "0", "no"):
        return False
    problems.append(f"{name}={raw!r} must be a boolean (true/false)")
    return default


def _float(
    env: Mapping[str, str], name: str, default: float, problems: list[str]
) -> float | None:
    raw = env.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        problems.append(f"{name}={raw!r} must be a number")
        return None


def _int(
    env: Mapping[str, str], name: str, default: int, problems: list[str]
) -> int | None:
    raw = env.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        problems.append(f"{name}={raw!r} must be an integer")
        return None


def load_maintenance_config(env: Mapping[str, str]) -> MaintenanceConfig:
    """Parse and validate the ``MILLRACE_MAINTENANCE_*`` environment.

    Every problem is collected and reported at once (the fail-fast idiom of
    config.py's ``load_config``).
    """
    problems: list[str] = []

    stage_raw = env.get("MILLRACE_MAINTENANCE_STAGE_URL", "").strip()
    stage: tuple[str, str] | None = None
    if not stage_raw:
        problems.append(
            "MILLRACE_MAINTENANCE_STAGE_URL is required and is not set "
            "(the writers' staging root, e.g. s3://bucket/millrace)"
        )
    else:
        stage = _parse_stage_url(stage_raw)
        if stage is None:
            problems.append(
                f"MILLRACE_MAINTENANCE_STAGE_URL {stage_raw!r} must be an "
                "s3://bucket/prefix, file:///path or memory:/// URL with a "
                "non-empty path/prefix"
            )

    gc_enabled = _bool(env, "MILLRACE_MAINTENANCE_GC_ENABLED", True, problems)
    interval = _float(env, "MILLRACE_MAINTENANCE_GC_INTERVAL_S", 600.0, problems)
    min_age = _int(env, "MILLRACE_MAINTENANCE_GC_MIN_AGE_MS", 300_000, problems)
    max_dbs = _int(env, "MILLRACE_MAINTENANCE_MAX_DBS_PER_SWEEP", 100, problems)
    jitter = _float(env, "MILLRACE_MAINTENANCE_JITTER", 0.2, problems)
    port = _int(env, "MILLRACE_MAINTENANCE_METRICS_PORT", 8001, problems)
    discovery_interval = _float(
        env, "MILLRACE_MAINTENANCE_DISCOVERY_INTERVAL_S", 3600.0, problems
    )

    knobs: MaintenanceKnobs | None = None
    if (
        interval is not None
        and min_age is not None
        and max_dbs is not None
        and jitter is not None
        and port is not None
        and discovery_interval is not None
    ):
        try:
            knobs = MaintenanceKnobs(
                gc_enabled=gc_enabled,
                gc_interval_s=interval,
                gc_min_age_ms=min_age,
                max_dbs_per_sweep=max_dbs,
                jitter=jitter,
                metrics_port=port,
                discovery_interval_s=discovery_interval,
            )
        except ValueError as exc:
            problems.append(str(exc))

    if problems:
        raise MaintenanceConfigError(problems)
    assert stage is not None and knobs is not None
    return MaintenanceConfig(store_url=stage[0], base_path=stage[1], knobs=knobs)


async def _run(cfg: MaintenanceConfig) -> int:
    """Assemble the service and run until a signal stops it."""
    store = ObjectStore.resolve(cfg.store_url)
    service = MaintenanceService(
        store_url=cfg.store_url,
        base_path=cfg.base_path,
        knobs=cfg.knobs,
        maintainer=AdminMaintainer(store, gc_options_for(cfg.knobs.gc_min_age_ms)),
    )
    # Ready once one sweep has completed: a pod whose discovery is broken
    # never reports ready (the same "probes never lie" discipline as the
    # pipeline's /readyz).
    ops = serve(
        "0.0.0.0",
        cfg.knobs.metrics_port,
        ready=lambda: (
            None if service.stats().sweeps_total >= 1 else "no sweep has completed yet"
        ),
    )
    from prometheus_client.core import REGISTRY

    REGISTRY.register(_MaintenanceCollector(service))
    try:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, service.stop)
        await service.run()
        return 0
    finally:
        ops.close()


def _main(env: Mapping[str, str]) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )
    try:
        cfg = load_maintenance_config(env)
    except MaintenanceConfigError as e:
        print(str(e), file=sys.stderr)
        return EXIT_CONFIG
    try:
        return asyncio.run(_run(cfg))
    except KeyboardInterrupt:  # pragma: no cover - signal handlers own the stop
        return 0
    except Exception:
        log.exception("millrace maintenance failed")
        return EXIT_ERROR


def main() -> None:
    """The ``python -m millrace.maintenance`` entry point."""
    raise SystemExit(_main(os.environ))


if __name__ == "__main__":
    main()
