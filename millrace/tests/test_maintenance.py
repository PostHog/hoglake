"""Tests for the external maintenance service (millrace.maintenance).

Layers:
- pure unit tests for discovery parsing, sweep budgeting/rotation, jitter
  and config parsing (fakes only — no store);
- ``integration`` tests over ``file:///`` driving the REAL slatedb Admin
  binding (GC actually reclaims; a corrupt DB is contained).

The slatedb behavior this service stands on (run_gc_once executes in the
caller, never raises for missing paths, respects checkpoints, never fences
the writer) is pinned by the "external maintenance" probes in
tests/test_slatedb_parity.py — these tests pin the SERVICE's logic.
"""

from __future__ import annotations

import asyncio
import os
import random
from collections.abc import Awaitable, Callable

import httpx
import pytest
from prometheus_client import CollectorRegistry, generate_latest
from slatedb.uniffi import Db, DbBuilder, FlushOptions, FlushType, ObjectStore, Settings

from millrace.maintenance import (
    AdminMaintainer,
    DiscoveredDb,
    MaintenanceConfigError,
    MaintenanceError,
    MaintenanceKnobs,
    MaintenanceService,
    discover_databases,
    gc_options_for,
    load_maintenance_config,
    parse_db_paths,
)


class FakeMaintainer:
    """Scripted DbMaintainer: records calls; ``broken`` paths raise,
    ``phantom`` paths report no manifest. ``l0`` maps path → the L0 SST
    count the debt read reports (default 0)."""

    def __init__(
        self,
        *,
        broken: set[str] | None = None,
        phantom: set[str] | None = None,
        broken_debt: set[str] | None = None,
        l0: dict[str, int] | None = None,
    ) -> None:
        self.broken = broken or set()
        self.phantom = phantom or set()
        self.broken_debt = broken_debt or set()
        self.l0 = l0 or {}
        self.gc_calls: list[str] = []
        self.probe_calls: list[str] = []
        self.debt_calls: list[str] = []

    async def has_manifest(self, path: str) -> bool:
        self.probe_calls.append(path)
        if path in self.broken:
            raise RuntimeError(f"corrupt manifest at {path}")
        return path not in self.phantom

    async def read_l0_ssts(self, path: str) -> int:
        self.debt_calls.append(path)
        if path in self.broken:
            raise AssertionError("debt read must follow a good probe")
        if path in self.broken_debt:
            raise RuntimeError(f"compactor state unreadable at {path}")
        return self.l0.get(path, 0)

    async def run_gc(self, path: str) -> None:
        if path in self.broken:
            raise AssertionError("run_gc must not be reached for a broken probe")
        self.gc_calls.append(path)


def _keys(*dbs: tuple[str, int], base: str = "millrace") -> list[str]:
    """The object keys a fleet of DBs produces, per (topic, partition)."""
    out: list[str] = []
    for topic, partition in dbs:
        root = f"{base}/{topic}/{partition}"
        out.append(f"{root}/manifest/00000000000000000001.manifest")
        out.append(f"{root}/wal/00000000000000000001.sst")
        out.append(f"{root}/gc/manifest.boundary")
    return out


class FakeClock:
    """Manually advanced monotonic clock for the discovery cadence."""

    def __init__(self, now: float = 10_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, delta_s: float) -> None:
        self.now += delta_s


def _service(
    keys: list[str],
    *,
    knobs: MaintenanceKnobs | None = None,
    maintainer: FakeMaintainer | None = None,
    lister: Callable[[], list[str]] | None = None,
    sleep: Callable[[float], Awaitable[None]] | None = None,
    rand: random.Random | None = None,
    monotonic: FakeClock | None = None,
) -> tuple[MaintenanceService, FakeMaintainer]:
    maintainer = maintainer or FakeMaintainer()

    async def _noop_sleep(_: float) -> None:
        return None

    service = MaintenanceService(
        store_url="memory:///",
        base_path="millrace",
        knobs=knobs or MaintenanceKnobs(),
        maintainer=maintainer,
        lister=lister if lister is not None else lambda: list(keys),
        sleep=sleep if sleep is not None else _noop_sleep,
        rand=rand if rand is not None else random.Random(0),
        monotonic=monotonic if monotonic is not None else FakeClock(),
    )
    return service, maintainer


# -- discovery parsing ----------------------------------------------------------


def test_parse_db_paths_finds_dbs_across_topics_and_partitions():
    keys = _keys(("events", 0), ("events", 1), ("other", 7))
    found = parse_db_paths(keys, "millrace")
    assert found == [
        DiscoveredDb("millrace/events/0", "events", 0),
        DiscoveredDb("millrace/events/1", "events", 1),
        DiscoveredDb("millrace/other/7", "other", 7),
    ]


def test_parse_db_paths_ignores_everything_but_manifest_markers():
    keys = _keys(("events", 0))
    keys += [
        "millrace/events/0/compacted/01M4ABCDEFGHIJKLMNOPQRSTUV.sst",
        "millrace/events/0/manifest/not-a-manifest.txt",  # wrong suffix
        "millrace/events/0/manifest/nested/0001.manifest",  # too deep
        "millrace/events",  # bare prefix entry
        "millrace/events/zero/manifest/0001.manifest",  # non-numeric partition
        "millrace/bad topic/0/manifest/0001.manifest",  # not a Kafka topic name
        "other-root/events/1/manifest/0001.manifest",  # outside the base path
        "foreign/part-0.parquet",
    ]
    found = parse_db_paths(keys, "millrace")
    assert found == [DiscoveredDb("millrace/events/0", "events", 0)]


def test_parse_db_paths_dedupes_and_sorts():
    keys = _keys(("b", 1), ("a", 2)) + _keys(
        ("b", 1),
    )
    found = parse_db_paths(keys, "millrace")
    assert [d.path for d in found] == ["millrace/a/2", "millrace/b/1"]


async def test_discover_databases_uses_the_injected_lister():
    found = await discover_databases(
        "memory:///", "millrace", lister=lambda: _keys(("events", 0), ("events", 1))
    )
    assert [d.path for d in found] == ["millrace/events/0", "millrace/events/1"]


async def test_discover_databases_memory_root_needs_the_seam():
    """memory:/// stores are per-process and unlistable: without an injected
    lister, discovery fails loudly rather than silently finding nothing."""
    with pytest.raises(MaintenanceError, match="no default lister"):
        await discover_databases("memory:///", "millrace")


# -- sweep budgeting / rotation / containment ------------------------------------


async def test_sweep_covers_all_dbs_when_under_budget():
    service, maintainer = _service(_keys(("events", 0), ("events", 1), ("other", 0)))
    report = await service.sweep_once()
    assert report.discovered == 3
    assert report.budgeted == 3
    assert report.gc_ok == 3
    assert not report.truncated
    assert maintainer.gc_calls == [
        "millrace/events/0",
        "millrace/events/1",
        "millrace/other/0",
    ]


async def test_sweep_budget_pages_through_the_fleet():
    """5 DBs, budget 2: sweeps cover [0,1], [2,3], [4], then restart —
    bounded work per run, every DB visited once per pass, truncation
    ledgered on the report (AGENT.md doctrine)."""
    keys = _keys(("t", 0), ("t", 1), ("t", 2), ("t", 3), ("t", 4))
    service, maintainer = _service(keys, knobs=MaintenanceKnobs(max_dbs_per_sweep=2))
    seen: list[list[str]] = []
    reports = []
    for _ in range(4):
        maintainer.gc_calls.clear()
        reports.append(await service.sweep_once())
        seen.append(list(maintainer.gc_calls))
    assert seen == [
        ["millrace/t/0", "millrace/t/1"],
        ["millrace/t/2", "millrace/t/3"],
        ["millrace/t/4"],
        ["millrace/t/0", "millrace/t/1"],
    ]
    assert [r.truncated for r in reports] == [True, True, True, True]
    assert [r.budgeted for r in reports] == [2, 2, 1, 2]
    # every DB was GC'd over the rotation
    assert {p for sweep in seen for p in sweep} == {f"millrace/t/{i}" for i in range(5)}


async def test_sweep_budget_cursor_survives_membership_shrink():
    """The cursor can sit past the end when the discovered set shrinks
    mid-rotation (partitions deleted/recreated): it clamps to the front
    rather than skipping a cycle. (Membership changes are SEEN at the
    next fresh listing — the discovery cache is the cadence mechanism —
    so the clock crosses the discovery interval here.)"""
    clock = FakeClock()
    service, maintainer = _service(
        _keys(("t", 0), ("t", 1), ("t", 2), ("t", 3), ("t", 4)),
        knobs=MaintenanceKnobs(max_dbs_per_sweep=2),
        monotonic=clock,
    )
    await service.sweep_once()  # [0,1]
    await service.sweep_once()  # [2,3]; cursor now 4
    # the fleet shrinks to 2 DBs; the stale cursor (4) must clamp
    service._lister = lambda: _keys(("t", 0), ("t", 1))
    clock.advance(3600.0)  # past the discovery cadence: a fresh listing
    maintainer.gc_calls.clear()
    report = await service.sweep_once()
    assert report.discovery_fresh
    assert report.discovered == 2
    assert report.budgeted == 2
    assert not report.truncated
    assert set(maintainer.gc_calls) == {"millrace/t/0", "millrace/t/1"}


async def test_sweep_contains_per_db_failures():
    """A broken DB (corrupt manifest) and a vanished DB are contained:
    logged/counted, the sweep completes the rest."""
    keys = _keys(("t", 0), ("t", 1), ("t", 2))
    maintainer = FakeMaintainer(broken={"millrace/t/1"}, phantom={"millrace/t/2"})
    service, _ = _service(keys, maintainer=maintainer)
    report = await service.sweep_once()
    assert report.gc_ok == 1
    assert report.gc_failed == 1
    assert report.skipped_no_manifest == 1
    assert maintainer.gc_calls == ["millrace/t/0"]
    stats = service.stats()
    assert stats.gc_failures_total == 1
    assert stats.gc_skipped_total == 1
    # the next sweep retries them (nothing is blacklisted)
    report = await service.sweep_once()
    assert report.gc_failed == 1 and report.skipped_no_manifest == 1


async def test_sweep_with_gc_disabled_monitors_but_collects_nothing():
    """GC off disables COLLECTION, never VISIBILITY: the probe and the
    debt read still ride every swept DB (the service is the fleet's
    debt authority), only ``run_gc`` is skipped."""
    service, maintainer = _service(
        _keys(("t", 0)),
        knobs=MaintenanceKnobs(gc_enabled=False),
        maintainer=FakeMaintainer(l0={"millrace/t/0": 5}),
    )
    report = await service.sweep_once()
    assert report.discovered == 1 and report.gc_ok == 0 and not report.gc_enabled
    assert report.debt_samples == 1
    assert maintainer.gc_calls == []
    assert maintainer.probe_calls == ["millrace/t/0"]
    assert maintainer.debt_calls == ["millrace/t/0"]
    assert service.stats().l0_ssts == {"millrace/t/0": ("t", 0, 5)}


# -- compaction-debt monitoring -----------------------------------------------------


async def test_debt_read_samples_per_swept_db_and_tracks_the_map():
    """Per swept DB the compactor state view is read and retained; the
    retained map is the metrics input (per-DB series + fleet max)."""
    keys = _keys(("t", 0), ("t", 1), ("t", 2))
    service, maintainer = _service(
        keys,
        maintainer=FakeMaintainer(
            l0={"millrace/t/0": 2, "millrace/t/1": 7, "millrace/t/2": 1}
        ),
    )
    report = await service.sweep_once()
    assert report.debt_samples == 3
    assert service.stats().l0_ssts == {
        "millrace/t/0": ("t", 0, 2),
        "millrace/t/1": ("t", 1, 7),
        "millrace/t/2": ("t", 2, 1),
    }
    # a later sweep refreshes a changed reading
    maintainer.l0["millrace/t/1"] = 3
    await service.sweep_once()
    assert service.stats().l0_ssts["millrace/t/1"] == ("t", 1, 3)


async def test_debt_read_failure_is_contained_and_skips_gc():
    """A DB whose debt read fails counts as a contained failure; its GC
    does NOT run (a DB whose state view is unreadable is not GC'd blind),
    and the sweep completes the rest."""
    keys = _keys(("t", 0), ("t", 1))
    service, _ = _service(keys, maintainer=FakeMaintainer(broken_debt={"millrace/t/1"}))
    report = await service.sweep_once()
    assert report.gc_ok == 1
    assert report.gc_failed == 1
    assert report.debt_samples == 1
    assert "millrace/t/1" not in service.stats().l0_ssts


async def test_debt_map_prunes_undiscovered_dbs_at_the_next_listing():
    """A DB that vanishes from a fresh listing loses its retained series
    — the fleet view must not carry ghosts."""
    clock = FakeClock()
    keys = _keys(("t", 0), ("t", 1))
    service, _ = _service(keys, monotonic=clock)
    await service.sweep_once()
    assert set(service.stats().l0_ssts) == {"millrace/t/0", "millrace/t/1"}
    # the fleet shrinks; the next FRESH listing prunes the map
    service._lister = lambda: _keys(("t", 0))
    clock.advance(3700)  # past the default 1 h discovery cadence
    report = await service.sweep_once()
    assert report.discovery_fresh
    assert set(service.stats().l0_ssts) == {"millrace/t/0"}


# -- discovery caching ------------------------------------------------------------


async def test_discovery_lists_once_per_cadence_and_caches_between():
    """The full recursive listing runs on the first sweep and then at
    most once per ``discovery_interval_s``; sweeps in between work the
    cached set (the lister is not even consulted)."""
    clock = FakeClock()
    calls = {"n": 0}

    def counting_lister() -> list[str]:
        calls["n"] += 1
        return _keys(("t", 0))

    service, _ = _service([], lister=counting_lister, monotonic=clock)
    r1 = await service.sweep_once()
    assert r1.discovery_fresh and r1.discovered == 1 and calls["n"] == 1
    for _ in range(3):
        r = await service.sweep_once()
        assert not r.discovery_fresh
    assert calls["n"] == 1  # the cache served them
    assert service.stats().discovery_listings_total == 1
    # cross the cadence: the next sweep re-lists
    clock.advance(3600.0)
    r = await service.sweep_once()
    assert r.discovery_fresh and calls["n"] == 2


async def test_discovery_failure_with_a_warm_cache_degrades_to_it():
    """A listing failure with a warm cache is not an iteration failure:
    the sweep works the cached set and the failure is visible in the
    sweep's ``discovery_fresh`` bit (and the loop's error count stays
    put — the cache is the designed degradation)."""
    clock = FakeClock()
    calls = {"n": 0}

    def flaky() -> list[str]:
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("object store 503")
        return _keys(("t", 0))

    service, maintainer = _service([], lister=flaky, monotonic=clock)
    await service.sweep_once()
    maintainer.gc_calls.clear()
    clock.advance(3600.0)  # stale cache: the next sweep WOULD re-list
    report = await service.sweep_once()
    assert not report.discovery_fresh  # degraded to the cache
    assert report.gc_ok == 1
    assert maintainer.gc_calls == ["millrace/t/0"]
    assert service.stats().sweep_failures_total == 0
    assert service.stats().discovery_stale_cache_uses_total == 1
    # and the service recovers the listing on the next cadence window
    clock.advance(3600.0)
    report = await service.sweep_once()
    assert report.discovery_fresh


async def test_discovery_failure_with_a_cold_cache_fails_the_iteration():
    """No cache, a broken listing: the sweep raises (the run loop counts
    it as an iteration failure and continues) — nothing to sweep is a
    loud condition, never a silent empty fleet."""
    service, _ = _service(
        [], lister=lambda: (_ for _ in ()).throw(RuntimeError("s3 down"))
    )
    with pytest.raises(RuntimeError, match="s3 down"):
        await service.sweep_once()
    assert service.stats().sweeps_total == 0


async def test_run_loop_contains_iteration_failures_and_stops():
    """A discovery failure (object-store outage shape) fails the ITERATION,
    not the process: counted, and the loop keeps sweeping."""
    calls = {"n": 0}

    def flaky_lister() -> list[str]:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("object store 503")
        return _keys(("t", 0))

    delays: list[float] = []
    handle: dict[str, MaintenanceService] = {}

    async def scripted_sleep(d: float) -> None:
        delays.append(d)
        if len(delays) >= 2:
            handle["service"].stop()

    service, maintainer = _service([], lister=flaky_lister, sleep=scripted_sleep)
    handle["service"] = service
    await service.run()
    stats = service.stats()
    assert stats.sweep_failures_total == 1  # the first iteration
    assert stats.sweeps_total == 1  # the second succeeded
    assert stats.gc_runs_total == 1
    assert maintainer.gc_calls == ["millrace/t/0"]


async def test_run_loop_stop_wakes_the_pause_promptly():
    """stop() during the between-sweep pause ends run() promptly even with a
    production-length interval — a SIGTERM must not wait out the cadence
    (the default sleep is interruptible; tests inject theirs instead)."""
    service, _ = _service(
        _keys(("t", 0)), knobs=MaintenanceKnobs(gc_interval_s=3600.0, jitter=0.0)
    )
    service._sleep = service._interruptible_sleep  # the production default
    runner = asyncio.create_task(service.run())
    await asyncio.sleep(0.1)  # let the first sweep complete and the pause begin
    service.stop()
    await asyncio.wait_for(runner, timeout=30)  # must not take the hour
    assert service.stats().sweeps_total == 1


# -- cadence / jitter -------------------------------------------------------------


def test_next_delay_is_jittered_within_bounds_and_deterministic():
    knobs = MaintenanceKnobs(gc_interval_s=100.0, jitter=0.25)
    service, _ = _service([], knobs=knobs, rand=random.Random(42))
    delays = [service.next_delay() for _ in range(50)]
    assert all(75.0 <= d <= 125.0 for d in delays)
    assert len(set(delays)) > 1  # the jitter actually jitters
    # same seed -> same sequence (no wall clock anywhere in the cadence)
    again, _ = _service([], knobs=knobs, rand=random.Random(42))
    assert [again.next_delay() for _ in range(50)] == delays


def test_next_delay_with_zero_jitter_is_the_exact_interval():
    service, _ = _service([], knobs=MaintenanceKnobs(gc_interval_s=30.0, jitter=0.0))
    assert [service.next_delay() for _ in range(3)] == [30.0, 30.0, 30.0]


def test_jitter_keeps_replicas_unsynchronized():
    """Two services with independent randomness must not pick the same delay
    sequence (the anti-synchronization property the jitter exists for)."""
    knobs = MaintenanceKnobs(gc_interval_s=100.0, jitter=0.3)
    a, _ = _service([], knobs=knobs, rand=random.Random(1))
    b, _ = _service([], knobs=knobs, rand=random.Random(2))
    assert [a.next_delay() for _ in range(10)] != [b.next_delay() for _ in range(10)]


# -- config -----------------------------------------------------------------------


def test_load_maintenance_config_defaults():
    cfg = load_maintenance_config(
        {"MILLRACE_MAINTENANCE_STAGE_URL": "s3://bucket/millrace"}
    )
    assert cfg.store_url == "s3://bucket"
    assert cfg.base_path == "millrace"
    assert cfg.knobs == MaintenanceKnobs()


def test_load_maintenance_config_full_override():
    cfg = load_maintenance_config(
        {
            "MILLRACE_MAINTENANCE_STAGE_URL": "file:///var/lib/millrace/",
            "MILLRACE_MAINTENANCE_GC_ENABLED": "false",
            "MILLRACE_MAINTENANCE_GC_INTERVAL_S": "120.5",
            "MILLRACE_MAINTENANCE_GC_MIN_AGE_MS": "0",
            "MILLRACE_MAINTENANCE_MAX_DBS_PER_SWEEP": "7",
            "MILLRACE_MAINTENANCE_JITTER": "0.5",
            "MILLRACE_MAINTENANCE_METRICS_PORT": "0",
        }
    )
    assert cfg.store_url == "file:///"
    assert cfg.base_path == "/var/lib/millrace"
    assert cfg.knobs == MaintenanceKnobs(
        gc_enabled=False,
        gc_interval_s=120.5,
        gc_min_age_ms=0,
        max_dbs_per_sweep=7,
        jitter=0.5,
        metrics_port=0,
    )


def test_load_maintenance_config_reports_all_problems():
    with pytest.raises(MaintenanceConfigError) as exc_info:
        load_maintenance_config(
            {
                "MILLRACE_MAINTENANCE_STAGE_URL": "not-a-url",
                "MILLRACE_MAINTENANCE_GC_ENABLED": "maybe",
                "MILLRACE_MAINTENANCE_GC_INTERVAL_S": "soon",
            }
        )
    problems = exc_info.value.problems
    assert len(problems) == 3
    text = str(exc_info.value)
    for knob in (
        "MILLRACE_MAINTENANCE_STAGE_URL",
        "MILLRACE_MAINTENANCE_GC_ENABLED",
        "MILLRACE_MAINTENANCE_GC_INTERVAL_S",
    ):
        assert knob in text


@pytest.mark.parametrize(
    ("env", "match"),
    [
        ({"MILLRACE_MAINTENANCE_MAX_DBS_PER_SWEEP": "0"}, "MAX_DBS_PER_SWEEP"),
        ({"MILLRACE_MAINTENANCE_JITTER": "1.5"}, "JITTER"),
        ({"MILLRACE_MAINTENANCE_JITTER": "-0.1"}, "JITTER"),
        ({"MILLRACE_MAINTENANCE_GC_MIN_AGE_MS": "-1"}, "GC_MIN_AGE_MS"),
        ({"MILLRACE_MAINTENANCE_GC_INTERVAL_S": "0"}, "GC_INTERVAL_S"),
        ({"MILLRACE_MAINTENANCE_METRICS_PORT": "65536"}, "METRICS_PORT"),
    ],
)
def test_load_maintenance_config_knob_domain_errors(env, match):
    """Out-of-domain knob values are refused, naming the knob."""
    with pytest.raises(MaintenanceConfigError, match=match):
        load_maintenance_config({"MILLRACE_MAINTENANCE_STAGE_URL": "memory:///"} | env)


def test_load_maintenance_config_requires_the_stage_url():
    with pytest.raises(MaintenanceConfigError, match="STAGE_URL is required"):
        load_maintenance_config({})


def test_main_refuses_invalid_config(capsys: pytest.CaptureFixture[str]):
    """The process entry fails fast on bad config, exit code 2 (main.py's
    idiom), with every problem named at once."""
    from millrace import maintenance

    rc = maintenance._main({})
    assert rc == maintenance.EXIT_CONFIG
    err = capsys.readouterr().err
    assert "MILLRACE_MAINTENANCE_STAGE_URL" in err
    assert "problem(s)" in err


# -- integration: real slatedb over file:/// --------------------------------------


def _writer_settings() -> Settings:
    s = Settings.default()
    s.set("flush_interval", '"5ms"')
    # compactor disabled: the WAL census below stays deterministic (the
    # compactor's commits/self-checkpoints are the parity suite's subject)
    s.set("compactor_options", "null")
    s.set("l0_max_ssts", "1000")
    return s


async def _open_writer(store: ObjectStore, path: str) -> Db:
    builder = DbBuilder(path, store)
    builder.with_settings(_writer_settings())
    return await builder.build()


def _wal_count(root: str) -> int:
    wal_dir = os.path.join(root, "wal")
    return len(os.listdir(wal_dir)) if os.path.isdir(wal_dir) else 0


@pytest.mark.integration
async def test_sweep_discovers_and_gcs_real_dbs_with_a_live_writer(tmp_path):
    """End to end: three real DBs across two topics, one with its writer
    still open and writing. One sweep with the REAL Admin binding discovers
    all three and reclaims flushed WAL garbage on each (min_age 0); the live
    writer keeps writing across the sweep and every row reconciles."""
    store = ObjectStore.resolve("file:///")
    base = str(tmp_path / "millrace")
    writers = []
    expected: dict[str, dict[bytes, bytes]] = {}
    for topic, partition in (("events", 0), ("events", 1), ("other", 0)):
        db = await _open_writer(store, f"{base}/{topic}/{partition}")
        writers.append(db)
        expected[f"{topic}/{partition}"] = {}
        for i in range(3):
            key = f"k{i}".encode()
            value = f"{topic}-{partition}-{i}".encode()
            await db.put(key, value)
            expected[f"{topic}/{partition}"][key] = value
            await db.flush_with_options(FlushOptions(flush_type=FlushType.MEM_TABLE))

    wal_before = {str(p): _wal_count(str(p)) for p in tmp_path.glob("millrace/*/*")}
    assert sorted(wal_before) == [
        str(tmp_path / "millrace" / t) for t in ("events/0", "events/1", "other/0")
    ]
    assert all(c >= 3 for c in wal_before.values())

    # keep the first DB's writer OPEN; close the other two (the service must
    # not care either way)
    open_writer = writers[0]
    await writers[1].shutdown()
    await writers[2].shutdown()

    service = MaintenanceService(
        store_url="file:///",
        base_path=base,
        knobs=MaintenanceKnobs(gc_min_age_ms=0),
        maintainer=AdminMaintainer(store, gc_options_for(0)),
    )
    report = await service.sweep_once()
    assert report.discovered == 3
    assert report.gc_ok == 3
    assert report.gc_failed == 0

    for rel, before in wal_before.items():
        assert _wal_count(rel) < before, f"no WAL garbage reclaimed under {rel}"

    # the live writer kept writing across the sweep; everything reconciles
    await open_writer.put(b"after-sweep", b"1")
    await open_writer.flush_with_options(FlushOptions(flush_type=FlushType.MEM_TABLE))
    for key, value in expected["events/0"].items():
        assert await open_writer.get(key) == value
    assert await open_writer.get(b"after-sweep") == b"1"
    await open_writer.shutdown()

    # a second sweep is a clean no-op-ish pass (idempotent)
    report2 = await service.sweep_once()
    assert report2.discovered == 3 and report2.gc_ok == 3


@pytest.mark.integration
async def test_sweep_contains_a_corrupt_db_path(tmp_path):
    """A discovered path whose manifest is garbage fails contained:
    counted in gc_failed, the healthy DBs are still swept."""
    store = ObjectStore.resolve("file:///")
    base = str(tmp_path / "millrace")
    good = await _open_writer(store, f"{base}/events/0")
    await good.put(b"k", b"v")
    await good.flush_with_options(FlushOptions(flush_type=FlushType.MEM_TABLE))

    # a "DB" whose manifest is corrupt (discovery finds the marker, the
    # probe raises): slatedb answers Error.Data(unsupported manifest format)
    corrupt = tmp_path / "millrace" / "events" / "1" / "manifest"
    corrupt.mkdir(parents=True)
    (corrupt / "00000000000000000001.manifest").write_bytes(b"garbage-bytes")

    service = MaintenanceService(
        store_url="file:///",
        base_path=base,
        knobs=MaintenanceKnobs(gc_min_age_ms=0),
        maintainer=AdminMaintainer(store, gc_options_for(0)),
    )
    report = await service.sweep_once()
    assert report.discovered == 2
    assert report.gc_ok == 1
    assert report.gc_failed == 1
    assert service.stats().gc_failures_total == 1

    assert await good.get(b"k") == b"v"
    await good.shutdown()


def test_ops_surface_reports_sweeps():
    """The operational surface (server.py's, reused): /healthz answers,
    /readyz flips once a sweep completed, /metrics exposes the counters.
    (Sync test: blocking httpx calls stay out of the event loop.)"""
    from millrace.maintenance import _MaintenanceCollector
    from millrace.server import serve

    service, _ = _service(
        _keys(("t", 0)), maintainer=FakeMaintainer(l0={"millrace/t/0": 6})
    )
    # a private registry keeps the global prometheus registry untouched
    registry = CollectorRegistry()
    registry.register(_MaintenanceCollector(service))

    ready = {"ok": False}
    with serve(
        "127.0.0.1", 0, ready=lambda: None if ready["ok"] else "no sweep yet"
    ) as server:
        base = f"http://127.0.0.1:{server.port}"
        assert httpx.get(f"{base}/healthz", timeout=5).status_code == 200
        assert httpx.get(f"{base}/readyz", timeout=5).status_code == 503
        ready["ok"] = True
        assert httpx.get(f"{base}/readyz", timeout=5).status_code == 200

    asyncio.run(service.sweep_once())
    body = generate_latest(registry).decode()
    assert "millrace_maintenance_sweeps_total 1.0" in body
    assert "millrace_maintenance_gc_runs_total 1.0" in body
    assert "millrace_maintenance_dbs_discovered 1.0" in body
    assert "millrace_maintenance_sweep_truncated 0.0" in body
    assert "millrace_maintenance_discovery_listings_total 1.0" in body
    # the debt read: per-DB labeled series + the fleet max
    assert (
        'millrace_maintenance_compaction_l0_ssts{partition="0",topic="t"} 6.0' in body
    )
    assert "millrace_maintenance_compaction_l0_ssts_max 6.0" in body
