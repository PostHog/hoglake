"""The at-least-once flush contract, end to end against a real stage and
a scripted hoglake server (flushkit).

Pinned here: every staged row reaches a commit (count ≥ 1, never 0)
before it is deleted; duplicates are allowed; the startup drain flushes
a previous process's rows (and does again after a crash mid-drain); a
fanout over the files cap splits into several commits; a refused payload
retries with backoff, then reports the group stuck while the rest of the
pod keeps flushing; ancient events are written, not dropped; a recreated
destination halts; an ambiguous answer resends the identical bytes under
the same key; a refused read basis rebuilds under a fresh key.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from pathlib import Path
from typing import Any

import httpx
import pytest
from flushkit import (
    RECREATED_UUID,
    TABLE_UUID,
    CommitCapture,
    FakeMonotonic,
    FakeObjectStore,
    commit_ok,
    commit_refusal,
    event_payload,
    make_config,
    month_wire,
    parquet_rows,
    script_options,
    script_table_refresh,
    table_wire,
    team_group,
)
from stagekit import NOW, fast_flush_settings

from millrace.flush import STUCK_AFTER, FlushHalted, FlushRunner, HoglakeFlusher
from millrace.stage import StagedRecord, StageManager

DEC_US = NOW - 20 * 86_400_000_000


def manager(root: Path) -> StageManager:
    return StageManager("file:///", f"{root}/millrace", settings=fast_flush_settings())


async def stage_rows(
    stages: StageManager,
    clock: Any,
    team: int,
    rows: list[tuple[int, bytes]],
    *,
    partition: int = 0,
    group: tuple[str | None, ...] | None = None,
) -> None:
    await stages.stage("events", partition).stage_batch(
        [
            StagedRecord(group=group or team_group(team), offset=o, payload=p)
            for o, p in rows
        ],
        now_us=clock(),
    )


def runner(
    stages: StageManager,
    flusher: HoglakeFlusher,
    cfg: Any,
    clock: Any,
    mono: FakeMonotonic,
    sleeper: Any,
) -> FlushRunner:
    return FlushRunner(
        stages,
        flusher,
        cfg.planner_knobs(),
        now_us=clock,
        monotonic=mono,
        sleep=sleeper,
        concurrency=cfg.flush_concurrency,
    )


def committed_offsets(
    store: FakeObjectStore, capture: CommitCapture, landed: list[int] | None = None
) -> list[str]:
    """Every event name committed (one per published row, duplicates
    included), read back from the parquet the commits register.
    ``landed`` names the commits (by index) the server accepted; default
    all."""
    out: list[str] = []
    payloads = capture.payloads
    for payload in payloads if landed is None else [payloads[i] for i in landed]:
        for f in payload["appends"][0]["files"]:
            out.extend(parquet_rows(store, f["path"]).column("event").to_pylist())
    return out


async def staged_count(stages: StageManager, team: int, partition: int = 0) -> int:
    scan = await stages.stage("events", partition).scan(
        team_group(team), max_bytes=1 << 30, now_us=0
    )
    return len(scan.rows)


@pytest.fixture
def stages(tmp_path: Path) -> Any:
    return manager(tmp_path)


async def test_a_due_group_commits_once_and_its_rows_go(
    httpx_mock: Any,
    flusher_factory: Any,
    store: Any,
    stages: Any,
    fake_clock: Any,
    mono: Any,
    sleeper: Any,
) -> None:
    cfg = make_config()
    flusher = flusher_factory(cfg=cfg, store=store)
    script_options(httpx_mock)
    capture = CommitCapture(httpx_mock, [commit_ok(91)])
    await stages.open_partition("events", 0, now_us=fake_clock())
    await stage_rows(
        stages, fake_clock, 7, [(o, event_payload(7, NOW, f"e{o}")) for o in range(3)]
    )
    report = await runner(stages, flusher, cfg, fake_clock, mono, sleeper).run_once(
        drain=True
    )
    assert report.committed == 1
    (payload,) = capture.payloads
    assert uuid.UUID(payload["idempotency_key"]).version == 4
    assert payload["read_snapshot"] == 88
    assert payload["appends"][0]["expected_table_uuid"] == TABLE_UUID
    assert sorted(committed_offsets(store, capture)) == ["e0", "e1", "e2"]
    assert await staged_count(stages, 7) == 0
    assert not stages.stage("events", 0).has_group(team_group(7))
    await stages.close()


async def test_each_build_mints_a_fresh_key(
    httpx_mock: Any,
    flusher_factory: Any,
    store: Any,
    stages: Any,
    fake_clock: Any,
    mono: Any,
    sleeper: Any,
) -> None:
    cfg = make_config()
    flusher = flusher_factory(cfg=cfg, store=store)
    script_options(httpx_mock)
    capture = CommitCapture(httpx_mock, [commit_ok(91), commit_ok(92)])
    await stages.open_partition("events", 0, now_us=fake_clock())
    r = runner(stages, flusher, cfg, fake_clock, mono, sleeper)
    await stage_rows(stages, fake_clock, 7, [(0, event_payload(7, NOW))])
    await r.run_once(drain=True)
    await stage_rows(stages, fake_clock, 7, [(0, event_payload(7, NOW))])  # a rewind
    await r.run_once(drain=True)
    keys = [p["idempotency_key"] for p in capture.payloads]
    assert len(set(keys)) == 2  # the rewind is a duplicate, by contract
    assert committed_offsets(store, capture).count("pageview") == 2
    await stages.close()


async def test_an_ambiguous_answer_resends_the_identical_bytes(
    httpx_mock: Any,
    flusher_factory: Any,
    store: Any,
    stages: Any,
    fake_clock: Any,
    mono: Any,
    sleeper: Any,
) -> None:
    cfg = make_config()
    flusher = flusher_factory(cfg=cfg, store=store)
    script_options(httpx_mock)
    capture = CommitCapture(
        httpx_mock,
        [
            httpx.ReadTimeout("lost"),
            commit_refusal(503, "commit_queue_timeout", "busy"),
            commit_ok(91),
        ],
    )
    await stages.open_partition("events", 0, now_us=fake_clock())
    await stage_rows(stages, fake_clock, 7, [(0, event_payload(7, NOW))])
    report = await runner(stages, flusher, cfg, fake_clock, mono, sleeper).run_once(
        drain=True
    )
    assert report.committed == 1
    assert len(capture.bodies) == 3
    assert capture.bodies[0] == capture.bodies[1] == capture.bodies[2]
    await stages.close()


async def test_a_refused_read_basis_rebuilds_under_a_fresh_key(
    httpx_mock: Any,
    flusher_factory: Any,
    store: Any,
    stages: Any,
    fake_clock: Any,
    mono: Any,
    sleeper: Any,
) -> None:
    cfg = make_config()
    flusher = flusher_factory(cfg=cfg, store=store)
    script_options(httpx_mock)
    script_table_refresh(httpx_mock, table=table_wire(read_snapshot_id=95))
    capture = CommitCapture(
        httpx_mock,
        [commit_refusal(409, "ddl_since_read_snapshot", "spec moved"), commit_ok(96)],
    )
    await stages.open_partition("events", 0, now_us=fake_clock())
    await stage_rows(stages, fake_clock, 7, [(0, event_payload(7, NOW))])
    report = await runner(stages, flusher, cfg, fake_clock, mono, sleeper).run_once(
        drain=True
    )
    assert report.committed == 1
    first, second = capture.payloads
    assert first["idempotency_key"] != second["idempotency_key"]
    assert (first["read_snapshot"], second["read_snapshot"]) == (88, 95)
    assert await staged_count(stages, 7) == 0
    await stages.close()


async def test_a_fanout_over_the_files_cap_splits_into_several_commits(
    httpx_mock: Any,
    flusher_factory: Any,
    store: Any,
    stages: Any,
    fake_clock: Any,
    mono: Any,
    sleeper: Any,
) -> None:
    cfg = make_config(max_files_per_commit=1)
    flusher = flusher_factory(cfg=cfg, store=store)
    script_options(httpx_mock)
    capture = CommitCapture(httpx_mock, [commit_ok(91), commit_ok(92)])
    await stages.open_partition("events", 0, now_us=fake_clock())
    # One staged group whose rows the live spec files under two months.
    await stage_rows(
        stages,
        fake_clock,
        7,
        [(0, event_payload(7, NOW, "jan")), (1, event_payload(7, DEC_US, "dec"))],
    )
    report = await runner(stages, flusher, cfg, fake_clock, mono, sleeper).run_once(
        drain=True
    )
    assert report.committed == 1
    assert len(capture.payloads) == 2
    assert len({p["idempotency_key"] for p in capture.payloads}) == 2
    assert all(len(p["appends"][0]["files"]) == 1 for p in capture.payloads)
    assert sorted(committed_offsets(store, capture)) == ["dec", "jan"]
    assert {p["message"].split("commit=")[1].split()[0] for p in capture.payloads} == {
        "1/2",
        "2/2",
    }
    assert await staged_count(stages, 7) == 0
    await stages.close()


def reusable_table_reads(httpx_mock: Any) -> None:
    """Every refusal drops the shape cache; each retry re-reads it."""
    from flushkit import TABLE_URL

    httpx_mock.add_response(
        method="GET",
        url=f"{TABLE_URL}?totals=false",
        json=table_wire(),
        is_reusable=True,
        is_optional=True,
    )


async def test_a_split_commit_settles_each_part_as_it_lands(
    httpx_mock: Any,
    flusher_factory: Any,
    store: Any,
    stages: Any,
    fake_clock: Any,
    mono: Any,
    sleeper: Any,
) -> None:
    """B1: part 1 of 2 lands and its rows are deleted at once; part 2 is
    refused three times (the group goes stuck), then lands. Every row is
    published exactly once: retries never republish a landed part."""
    cfg = make_config(max_files_per_commit=1)
    flusher = flusher_factory(cfg=cfg, store=store)
    script_options(httpx_mock)
    reusable_table_reads(httpx_mock)
    refused = commit_refusal(422, "record_validation", "nope")
    capture = CommitCapture(
        httpx_mock, [commit_ok(91), refused, refused, refused, commit_ok(92)]
    )
    await stages.open_partition("events", 0, now_us=fake_clock())
    await stage_rows(
        stages,
        fake_clock,
        7,
        [(0, event_payload(7, NOW, "jan")), (1, event_payload(7, DEC_US, "dec"))],
    )
    r = runner(stages, flusher, cfg, fake_clock, mono, sleeper)
    first = await r.run_once(drain=True)
    assert first.failed == 1
    (landed_first,) = committed_offsets(store, capture, landed=[0])
    # The landed part's row is gone already; the refused part's row stays.
    scan = await stages.stage("events", 0).scan(
        team_group(7), max_bytes=1 << 30, now_us=0
    )
    assert len(scan.rows) == 1
    assert landed_first.encode() not in scan.rows[0][1]
    for _ in range(2):
        mono.advance(3600)
        assert (await r.run_once()).failed == 1
    assert r.stuck_groups == 1
    assert await staged_count(stages, 7) == 1
    mono.advance(3600)
    assert (await r.run_once()).committed == 1
    assert r.stuck_groups == 0
    assert await staged_count(stages, 7) == 0
    assert not stages.stage("events", 0).has_group(team_group(7))
    # Every retry carried only the unpublished part.
    assert all(len(p["appends"][0]["files"]) == 1 for p in capture.payloads)
    retried = {
        tuple(committed_offsets(store, capture, landed=[i])) for i in range(1, 5)
    }
    assert len(retried) == 1
    published = committed_offsets(store, capture, landed=[0, 4])
    assert sorted(published) == ["dec", "jan"]  # exactly once each
    await stages.close()


async def test_a_refused_payload_retries_then_reports_the_group_stuck_and_the_pod_goes_on(
    httpx_mock: Any,
    flusher_factory: Any,
    store: Any,
    stages: Any,
    fake_clock: Any,
    mono: Any,
    sleeper: Any,
    caplog: pytest.LogCaptureFixture,
) -> None:
    cfg = make_config(flush_concurrency=1)
    flusher = flusher_factory(cfg=cfg, store=store)
    script_options(httpx_mock)
    poisoned = {"count": 0, "healed": False}

    def answer(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        if "group=team_id=7/" in body["message"] and not poisoned["healed"]:
            poisoned["count"] += 1
            return commit_refusal(422, "record_validation", "bad value")
        return commit_ok(100 + poisoned["count"])

    from flushkit import PREPARED_URL, TABLE_URL

    # Every refusal drops the shape cache; each retry re-reads it.
    httpx_mock.add_response(
        method="GET",
        url=f"{TABLE_URL}?totals=false",
        json=table_wire(),
        is_reusable=True,
        is_optional=True,
    )
    httpx_mock.add_callback(
        answer, method="POST", url=PREPARED_URL, is_reusable=True, is_optional=True
    )
    await stages.open_partition("events", 0, now_us=fake_clock())
    await stage_rows(stages, fake_clock, 7, [(0, event_payload(7, NOW))])
    r = runner(stages, flusher, cfg, fake_clock, mono, sleeper)
    for attempt in range(STUCK_AFTER):
        await stage_rows(
            stages,
            fake_clock,
            8 + attempt,
            [(10 + attempt, event_payload(8 + attempt, NOW))],
        )
        with caplog.at_level(logging.WARNING, logger="millrace.flush"):
            report = await r.run_once(drain=True)
        assert report.failed == 1  # team 7, again
        assert report.committed == 1  # the other group still flushed
        mono.advance(3600)
    assert poisoned["count"] == STUCK_AFTER
    assert r.stuck_groups == 1
    assert r.stuck_total == 1
    assert flusher.stats().commit_refusals == {"422": STUCK_AFTER}
    assert any(
        rec.levelno == logging.ERROR and "STUCK" in rec.getMessage()
        for rec in caplog.records
    )
    # The refused rows are never dropped.
    assert await staged_count(stages, 7) == 1
    # ... and the drain treats a stuck group as done rather than wedging.
    await r.drain()
    # The stuck group stays indexed, so it keeps retrying ...
    assert stages.stage("events", 0).has_group(team_group(7))
    # ... and lands once the server accepts it.
    poisoned["healed"] = True
    mono.advance(3600)
    report = await r.run_once()
    assert report.committed == 1
    assert await staged_count(stages, 7) == 0
    assert r.stuck_groups == 0
    assert not stages.stage("events", 0).has_group(team_group(7))
    await stages.close()


async def test_ancient_and_far_future_events_are_written_not_dropped(
    httpx_mock: Any,
    flusher_factory: Any,
    store: Any,
    stages: Any,
    fake_clock: Any,
    mono: Any,
    sleeper: Any,
) -> None:
    cfg = make_config()
    flusher = flusher_factory(cfg=cfg, store=store)
    script_options(httpx_mock)
    capture = CommitCapture(httpx_mock, [commit_ok(91)])
    ancient = 31_536_000_000_000  # 1971-01-01
    future = NOW + 400 * 86_400_000_000
    await stages.open_partition("events", 0, now_us=fake_clock())
    await stage_rows(
        stages,
        fake_clock,
        7,
        [(0, event_payload(7, ancient, "old")), (1, event_payload(7, future, "new"))],
    )
    report = await runner(stages, flusher, cfg, fake_clock, mono, sleeper).run_once(
        drain=True
    )
    assert report.committed == 1
    assert report.dropped == 0
    files = capture.payloads[0]["appends"][0]["files"]
    assert sorted(f["partition_values"][1] for f in files) == sorted(
        [month_wire(ancient), month_wire(future)]
    )
    assert sorted(committed_offsets(store, capture)) == ["new", "old"]
    await stages.close()


async def test_unrepresentable_records_are_dropped_and_counted_never_batch_fatal(
    httpx_mock: Any,
    flusher_factory: Any,
    store: Any,
    stages: Any,
    fake_clock: Any,
    mono: Any,
    sleeper: Any,
) -> None:
    cfg = make_config()
    flusher = flusher_factory(cfg=cfg, store=store)
    script_options(httpx_mock)
    capture = CommitCapture(httpx_mock, [commit_ok(91)])
    await stages.open_partition("events", 0, now_us=fake_clock())
    await stage_rows(
        stages,
        fake_clock,
        7,
        [
            (0, b"not json"),
            (1, json.dumps({"team_id": "x", "timestamp": NOW}).encode()),
            (2, event_payload(7, NOW, "ok")),
        ],
    )
    report = await runner(stages, flusher, cfg, fake_clock, mono, sleeper).run_once(
        drain=True
    )
    assert report.committed == 1
    assert committed_offsets(store, capture) == ["ok"]
    assert sum(flusher.stats().records_dropped.values()) == 2
    assert await staged_count(stages, 7) == 0
    await stages.close()


async def test_a_window_of_only_unrepresentable_records_settles_without_a_commit(
    httpx_mock: Any,
    flusher_factory: Any,
    store: Any,
    stages: Any,
    fake_clock: Any,
    mono: Any,
    sleeper: Any,
) -> None:
    cfg = make_config()
    flusher = flusher_factory(cfg=cfg, store=store)
    script_options(httpx_mock)
    await stages.open_partition("events", 0, now_us=fake_clock())
    await stage_rows(stages, fake_clock, 7, [(0, b"[1, 2]")])
    report = await runner(stages, flusher, cfg, fake_clock, mono, sleeper).run_once(
        drain=True
    )
    assert report.dropped == 1
    assert await staged_count(stages, 7) == 0
    await stages.close()


async def test_a_row_larger_than_the_scan_budget_still_flushes(
    httpx_mock: Any,
    flusher_factory: Any,
    store: Any,
    stages: Any,
    fake_clock: Any,
    mono: Any,
    sleeper: Any,
) -> None:
    cfg = make_config(flush_bytes=8)
    flusher = flusher_factory(cfg=cfg, store=store)
    script_options(httpx_mock)
    capture = CommitCapture(httpx_mock, [commit_ok(91), commit_ok(92)])
    await stages.open_partition("events", 0, now_us=fake_clock())
    await stage_rows(
        stages,
        fake_clock,
        7,
        [(0, event_payload(7, NOW, "a")), (1, event_payload(7, NOW, "b"))],
    )
    await runner(stages, flusher, cfg, fake_clock, mono, sleeper).drain()
    # One row per decision (each larger than the budget), both published.
    assert len(capture.payloads) == 2
    assert sorted(committed_offsets(store, capture)) == ["a", "b"]
    assert await staged_count(stages, 7) == 0
    await stages.close()


async def test_a_recreated_destination_halts(
    httpx_mock: Any,
    flusher_factory: Any,
    store: Any,
    stages: Any,
    fake_clock: Any,
    mono: Any,
    sleeper: Any,
) -> None:
    cfg = make_config()
    flusher = flusher_factory(cfg=cfg, store=store)
    script_options(httpx_mock)
    script_table_refresh(httpx_mock, table=table_wire(uuid=RECREATED_UUID))
    await stages.open_partition("events", 0, now_us=fake_clock())
    await stage_rows(stages, fake_clock, 7, [(0, event_payload(7, NOW))])
    mono.advance(10 * 3600)  # past the shape TTL
    r = runner(stages, flusher, cfg, fake_clock, mono, sleeper)
    with pytest.raises(FlushHalted) as halted:
        await r.run_once(drain=True)
    assert halted.value.reason == "table_recreated"
    assert r.halted is halted.value
    assert await staged_count(stages, 7) == 1
    await stages.close()


async def test_a_commit_refused_as_table_recreated_halts(
    httpx_mock: Any,
    flusher_factory: Any,
    store: Any,
    stages: Any,
    fake_clock: Any,
    mono: Any,
    sleeper: Any,
) -> None:
    cfg = make_config()
    flusher = flusher_factory(cfg=cfg, store=store)
    script_options(httpx_mock)
    CommitCapture(
        httpx_mock, [commit_refusal(409, "table_recreated", "new incarnation")]
    )
    await stages.open_partition("events", 0, now_us=fake_clock())
    await stage_rows(stages, fake_clock, 7, [(0, event_payload(7, NOW))])
    with pytest.raises(FlushHalted, match="table_recreated"):
        await runner(stages, flusher, cfg, fake_clock, mono, sleeper).run_once(
            drain=True
        )
    assert await staged_count(stages, 7) == 1
    await stages.close()


async def test_the_startup_drain_flushes_a_previous_process_rows(
    tmp_path: Path,
    httpx_mock: Any,
    flusher_factory: Any,
    store: Any,
    fake_clock: Any,
    mono: Any,
    sleeper: Any,
) -> None:
    previous = manager(tmp_path)
    await previous.open_partition("events", 0, now_us=fake_clock())
    await previous.open_partition("events", 1, now_us=fake_clock())
    await stage_rows(previous, fake_clock, 7, [(0, event_payload(7, NOW, "p0"))])
    await stage_rows(
        previous, fake_clock, 8, [(5, event_payload(8, NOW, "p1"))], partition=1
    )
    await previous.close()

    cfg = make_config()
    flusher = flusher_factory(cfg=cfg, store=store)
    script_options(httpx_mock)
    capture = CommitCapture(httpx_mock, [commit_ok(91), commit_ok(92)])
    stages = manager(tmp_path)
    await stages.sync_assignment({("events", 0), ("events", 1)}, now_us=fake_clock())
    # Discovery put both groups in the index, sizes unknown.
    assert stages.gauges().staged_groups == 2
    await runner(stages, flusher, cfg, fake_clock, mono, sleeper).drain()
    assert sorted(committed_offsets(store, capture)) == ["p0", "p1"]
    assert stages.gauges().staged_groups == 0
    assert await staged_count(stages, 7) == 0
    assert await staged_count(stages, 8, partition=1) == 0
    await stages.close()


async def test_a_crash_mid_drain_drains_again_duplicates_allowed(
    tmp_path: Path,
    httpx_mock: Any,
    flusher_factory: Any,
    store: Any,
    fake_clock: Any,
    mono: Any,
    sleeper: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    previous = manager(tmp_path)
    await previous.open_partition("events", 0, now_us=fake_clock())
    await stage_rows(
        previous, fake_clock, 7, [(o, event_payload(7, NOW, f"e{o}")) for o in range(3)]
    )
    await previous.close()

    cfg = make_config()
    flusher = flusher_factory(cfg=cfg, store=store)
    script_options(httpx_mock)
    capture = CommitCapture(httpx_mock, [commit_ok(91), commit_ok(92)])

    # The first drain's commit lands, then the process dies before the
    # delete batch.
    crashing = manager(tmp_path)
    await crashing.open_partition("events", 0, now_us=fake_clock())
    from millrace.stage import PartitionStage

    async def crash(self: Any, scan: Any) -> bool:
        raise asyncio.CancelledError("killed before the settle")

    with monkeypatch.context() as patch:
        patch.setattr(PartitionStage, "settle", crash)
        with pytest.raises(asyncio.CancelledError):
            await flusher.flush_group(
                crashing.stage("events", 0), team_group(7), "drain"
            )
    await crashing.close()

    restarted = manager(tmp_path)
    await restarted.open_partition("events", 0, now_us=fake_clock())
    await runner(restarted, flusher, cfg, fake_clock, mono, sleeper).drain()
    published = committed_offsets(store, capture)
    assert sorted(set(published)) == ["e0", "e1", "e2"]
    assert len(published) == 6  # every row twice: a duplicate, never a loss
    assert await staged_count(restarted, 7) == 0
    await restarted.close()


async def test_one_flush_in_flight_per_group(
    httpx_mock: Any,
    flusher_factory: Any,
    store: Any,
    stages: Any,
    fake_clock: Any,
    mono: Any,
    sleeper: Any,
) -> None:
    cfg = make_config()
    flusher = flusher_factory(cfg=cfg, store=store)
    await stages.open_partition("events", 0, now_us=fake_clock())
    await stage_rows(stages, fake_clock, 7, [(0, event_payload(7, NOW))])
    release = asyncio.Event()
    calls: list[Any] = []

    async def slow(stage: Any, group: Any, trigger: str) -> Any:
        calls.append(group)
        await release.wait()
        return None

    flusher.flush_group = slow  # type: ignore[method-assign]
    r = runner(stages, flusher, cfg, fake_clock, mono, sleeper)
    first = asyncio.create_task(r.run_once(drain=True))
    await asyncio.sleep(0)
    second = await r.run_once(drain=True)
    assert second.decisions == 0
    release.set()
    await first
    assert calls == [team_group(7)]
    await stages.close()


# -- review round: clamping, pinned incarnation, encoder bugs, backlog,
# -- shutdown, drain retries, steady groups, partitions -------------------


def keyed(payloads: list[tuple[int, bytes]]) -> list[StagedRecord]:
    """Classify payloads exactly as the consumer does (the live keyer)."""
    from pyhoglake import TableInfo

    from millrace.consumer import classify_message
    from millrace.grouping import GroupKeyer

    keyer = GroupKeyer(TableInfo.from_wire(table_wire()))
    out = []
    for offset, payload in payloads:
        rec = classify_message(keyer, offset=offset, value=payload)
        assert isinstance(rec, StagedRecord), rec
        out.append(rec)
    return out


async def test_out_of_range_timestamps_are_keyed_and_land_clamped(
    httpx_mock: Any,
    flusher_factory: Any,
    store: Any,
    stages: Any,
    fake_clock: Any,
    mono: Any,
    sleeper: Any,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """B2: a year-10000 and a year-0 event time, as epoch ints and as ISO
    strings, are staged (never malformed_partition_source), keyed under
    the clamped month, and land as 9999-12-31T23:59:59.999999 /
    0001-01-01T00:00:00: counted once per value, warned once per column."""
    from millrace.coerce import TIMESTAMP_MAX_US, TIMESTAMP_MIN_US

    year_10000 = TIMESTAMP_MAX_US + 86_400_000_000
    year_0 = TIMESTAMP_MIN_US - 86_400_000_000

    def doc(ts: Any, name: str) -> bytes:
        return json.dumps({"team_id": 7, "timestamp": ts, "event": name}).encode()

    records = keyed(
        [
            (0, doc(year_10000, "int-high")),
            (1, doc(year_0, "int-low")),
            (2, doc("10000-01-01T00:00:00", "iso-high")),
            (3, doc("0000-06-01T00:00:00", "iso-low")),
        ]
    )
    high, low = ("7", month_wire(TIMESTAMP_MAX_US)), ("7", month_wire(TIMESTAMP_MIN_US))
    assert [r.group for r in records] == [high, low, high, low]

    cfg = make_config()
    flusher = flusher_factory(cfg=cfg, store=store)
    script_options(httpx_mock)
    capture = CommitCapture(httpx_mock, [commit_ok(91), commit_ok(92)])
    await stages.open_partition("events", 0, now_us=fake_clock())
    await stages.stage("events", 0).stage_batch(records, now_us=fake_clock())
    with caplog.at_level(logging.WARNING, logger="millrace.flush"):
        report = await runner(stages, flusher, cfg, fake_clock, mono, sleeper).run_once(
            drain=True
        )
    assert (report.committed, report.dropped) == (2, 0)
    landed: dict[str, int] = {}
    for payload in capture.payloads:
        (f,) = payload["appends"][0]["files"]
        rows = parquet_rows(store, f["path"]).to_pylist()
        for row in rows:
            from datetime import datetime, timedelta

            landed[row["event"]] = (
                row["timestamp"] - datetime.fromisoformat("1970-01-01T00:00:00")
            ) // timedelta(microseconds=1)
            assert f["partition_values"][1] == (
                high[1] if row["event"].endswith("high") else low[1]
            )
    assert landed == {
        "int-high": TIMESTAMP_MAX_US,
        "iso-high": TIMESTAMP_MAX_US,
        "int-low": TIMESTAMP_MIN_US,
        "iso-low": TIMESTAMP_MIN_US,
    }
    assert flusher.stats().values_clamped == {"timestamp": 4}
    warnings = [r for r in caplog.records if "clamped" in r.getMessage()]
    assert len(warnings) == 1  # rate-limited per column
    await stages.close()


async def test_the_first_commit_pins_the_incarnation_and_a_recreate_halts_the_drain(
    tmp_path: Path,
    httpx_mock: Any,
    flusher_factory: Any,
    store: Any,
    fake_clock: Any,
    mono: Any,
    sleeper: Any,
) -> None:
    """S1: the halt survives a restart. A stage whose rows were committed
    to one incarnation refuses to drain into another: the drain raises
    ``table_recreated`` (exit 3) before any commit."""
    from millrace.main import EXIT_HALTED, _exit_code_for

    cfg = make_config()
    first = manager(tmp_path)
    await first.open_partition("events", 0, now_us=fake_clock())
    flusher = flusher_factory(cfg=cfg, store=store)
    script_options(httpx_mock)
    capture = CommitCapture(httpx_mock, [commit_ok(91)])
    await stage_rows(first, fake_clock, 7, [(0, event_payload(7, NOW))])
    assert await first.stage("events", 0).pinned_table_uuid() is None
    await runner(first, flusher, cfg, fake_clock, mono, sleeper).run_once(drain=True)
    assert await first.stage("events", 0).pinned_table_uuid() == TABLE_UUID
    await stage_rows(first, fake_clock, 7, [(1, event_payload(7, NOW))])
    await first.close()

    recreated = flusher_factory(
        cfg=cfg, store=store, table=table_wire(uuid=RECREATED_UUID)
    )
    restarted = manager(tmp_path)
    await restarted.open_partition("events", 0, now_us=fake_clock())
    r = runner(restarted, recreated, cfg, fake_clock, mono, sleeper)
    with pytest.raises(FlushHalted) as halted:
        await r.drain()
    assert halted.value.reason == "table_recreated"
    assert r.halted is halted.value
    assert _exit_code_for([halted.value]) == EXIT_HALTED
    assert len(capture.bodies) == 1  # nothing was committed after the restart
    assert await staged_count(restarted, 7) == 1
    await restarted.close()


async def test_an_encoder_bug_fails_the_decision_and_keeps_every_row(
    httpx_mock: Any,
    flusher_factory: Any,
    store: Any,
    stages: Any,
    fake_clock: Any,
    mono: Any,
    sleeper: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """S2: a record that defeats the Arrow build is a millrace bug: the
    decision fails, nothing is dropped, nothing committed."""
    import millrace.flush as flush_mod

    real = flush_mod.JsonObjectDecoder.decode_record

    def drifting(self: Any, payload: bytes) -> Any:
        if b'"drift"' in payload:
            return flush_mod.DecodedRecord(
                values={"team_id": 2**63 + 5, "timestamp": NOW, "event": None},
                dropped_keys=(),
            )
        return real(self, payload)

    monkeypatch.setattr(flush_mod.JsonObjectDecoder, "decode_record", drifting)
    cfg = make_config()
    flusher = flusher_factory(cfg=cfg, store=store)
    script_options(httpx_mock)
    capture = CommitCapture(httpx_mock, [])
    await stages.open_partition("events", 0, now_us=fake_clock())
    await stage_rows(
        stages,
        fake_clock,
        7,
        [(0, event_payload(7, NOW)), (1, event_payload(7, NOW, "drift"))],
    )
    report = await runner(stages, flusher, cfg, fake_clock, mono, sleeper).run_once(
        drain=True
    )
    assert report.failed == 1
    assert capture.bodies == []
    assert flusher.stats().records_dropped == {}
    assert await staged_count(stages, 7) == 2
    await stages.close()


async def test_an_incomplete_scan_is_due_on_the_next_sweep(
    tmp_path: Path,
    httpx_mock: Any,
    flusher_factory: Any,
    store: Any,
    fake_clock: Any,
    mono: Any,
    sleeper: Any,
) -> None:
    """S4: a discovered group has no counted size, so neither the size
    nor the age lane would take it; once a capped scan left rows behind
    it keeps draining on every sweep instead of waiting for the slow
    lane."""
    previous = manager(tmp_path)
    await previous.open_partition("events", 0, now_us=fake_clock())
    await stage_rows(
        previous, fake_clock, 7, [(o, event_payload(7, NOW, f"e{o}")) for o in range(3)]
    )
    await previous.close()

    cfg = make_config(flush_bytes=8)  # one row per decision
    flusher = flusher_factory(cfg=cfg, store=store)
    script_options(httpx_mock)
    capture = CommitCapture(httpx_mock, [commit_ok(91), commit_ok(92), commit_ok(93)])
    stages = manager(tmp_path)
    await stages.open_partition("events", 0, now_us=fake_clock())
    r = runner(stages, flusher, cfg, fake_clock, mono, sleeper)
    assert (await r.run_once()).decisions == 0  # not due by the planner
    assert (await r.run_once(drain=True)).committed == 1  # capped: 1 of 3
    snap = stages.stage("events", 0).snapshot()
    assert [s.backlog for s in snap] == [True]
    assert snap[0].staged_bytes == 0
    assert (await r.run_once()).committed == 1  # due: backlog
    assert (await r.run_once()).committed == 1  # the last row, complete
    assert (await r.run_once()).decisions == 0
    assert len(capture.payloads) == 3
    assert await staged_count(stages, 7) == 0
    await stages.close()


async def test_shutdown_mid_flush_keeps_the_rows(
    tmp_path: Path,
    httpx_mock: Any,
    flusher_factory: Any,
    store: Any,
    fake_clock: Any,
    mono: Any,
    sleeper: Any,
) -> None:
    """T1: a flush cancelled (or a stage closed) while its commit is in
    flight deletes nothing; the next process discovers the group. A
    runner stopped while a flush is in flight lets it finish its own
    error handling and keeps the rows of a commit that did not land."""
    import threading

    from flushkit import PREPARED_URL

    entered = threading.Event()
    release = threading.Event()
    answers = [commit_ok(91), commit_refusal(422, "record_validation", "no")]

    def blocked(request: httpx.Request) -> httpx.Response:
        entered.set()
        release.wait(10)
        return answers.pop(0)

    httpx_mock.add_callback(
        blocked, method="POST", url=PREPARED_URL, is_reusable=True, is_optional=True
    )
    cfg = make_config()
    flusher = flusher_factory(cfg=cfg, store=store)
    script_options(httpx_mock)
    reusable_table_reads(httpx_mock)

    async def wait_entered() -> None:
        for _ in range(500):
            if entered.is_set():
                return
            await asyncio.sleep(0.01)
        raise AssertionError("the commit never started")

    # (a) cancel the flush task, then close the stages, while waiting.
    stages = manager(tmp_path)
    await stages.open_partition("events", 0, now_us=fake_clock())
    rows = [(o, event_payload(7, NOW, f"e{o}")) for o in range(3)]
    await stage_rows(stages, fake_clock, 7, rows)
    task = asyncio.create_task(
        flusher.flush_group(stages.stage("events", 0), team_group(7), "size")
    )
    await wait_entered()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await staged_count(stages, 7) == 3
    await stages.close()
    release.set()  # the commit lands after the process "died": a duplicate later
    reopened = manager(tmp_path)
    await reopened.open_partition("events", 0, now_us=fake_clock())
    assert reopened.stage("events", 0).has_group(team_group(7))
    assert await staged_count(reopened, 7) == 3

    # (b) runner.stop() during run(): the in-flight flush finishes (its
    # commit is refused), run() returns, the rows stay.
    entered.clear()
    release.clear()
    r = FlushRunner(
        reopened,
        flusher,
        cfg.planner_knobs(),
        now_us=fake_clock,
        monotonic=mono,
        sleep=asyncio.sleep,
        sweep_s=0.01,
    )
    fake_clock.advance(cfg.slow_lane_deadline_s * 1_000_000)
    running = asyncio.create_task(r.run())
    await wait_entered()
    r.stop()
    release.set()
    await asyncio.wait_for(running, timeout=10)
    assert await staged_count(reopened, 7) == 3
    await reopened.close()
    again = manager(tmp_path)
    await again.open_partition("events", 0, now_us=fake_clock())
    assert again.stage("events", 0).has_group(team_group(7))
    await again.close()


async def test_the_drain_retries_a_transiently_failing_group(
    httpx_mock: Any,
    flusher_factory: Any,
    store: Any,
    stages: Any,
    fake_clock: Any,
    mono: Any,
    sleeper: Any,
) -> None:
    """T3: the first attempt exhausts the ladder on 500s; the drain backs
    off and retries rather than returning with the group staged."""
    from millrace.flush import _PUBLISH_MAX_ATTEMPTS

    cfg = make_config()
    flusher = flusher_factory(cfg=cfg, store=store)
    script_options(httpx_mock)
    script_table_refresh(httpx_mock)
    capture = CommitCapture(
        httpx_mock,
        [commit_refusal(500, "internal", "boom")] * _PUBLISH_MAX_ATTEMPTS
        + [commit_ok(91)],
    )
    await stages.open_partition("events", 0, now_us=fake_clock())
    await stage_rows(stages, fake_clock, 7, [(0, event_payload(7, NOW))])

    async def advancing(seconds: float) -> None:
        mono.advance(seconds)

    r = FlushRunner(
        stages,
        flusher,
        cfg.planner_knobs(),
        now_us=fake_clock,
        monotonic=mono,
        sleep=advancing,
    )
    await asyncio.wait_for(r.drain(), timeout=10)
    assert len(capture.bodies) == _PUBLISH_MAX_ATTEMPTS + 1
    assert await staged_count(stages, 7) == 0
    assert r.stuck_groups == 0
    await stages.close()


async def test_a_steady_group_flushes_once_per_deadline_not_per_sweep(
    httpx_mock: Any,
    flusher_factory: Any,
    store: Any,
    stages: Any,
    fake_clock: Any,
    mono: Any,
    sleeper: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T7: rows arriving during a complete scan keep the group indexed
    with its first staged time lifted to the scan's start, so the age
    lane takes it once per deadline, not on every sweep."""
    from millrace.stage import PartitionStage

    cfg = make_config(min_flush_bytes=1)
    flusher = flusher_factory(cfg=cfg, store=store)
    script_options(httpx_mock)
    capture = CommitCapture(httpx_mock, [commit_ok(91), commit_ok(92)])
    await stages.open_partition("events", 0, now_us=fake_clock())
    stage = stages.stage("events", 0)
    await stage_rows(stages, fake_clock, 7, [(0, event_payload(7, NOW))])
    real_scan = PartitionStage.scan
    arrivals = iter(range(100, 200))

    async def scan_then_arrive(self: Any, group: Any, **kw: Any) -> Any:
        result = await real_scan(self, group, **kw)
        await stage_rows(
            stages, fake_clock, 7, [(next(arrivals), event_payload(7, NOW))]
        )
        return result

    monkeypatch.setattr(PartitionStage, "scan", scan_then_arrive)
    r = runner(stages, flusher, cfg, fake_clock, mono, sleeper)
    deadline_us = cfg.flush_deadline_s * 1_000_000
    fake_clock.advance(deadline_us)
    assert (await r.run_once()).committed == 1
    for _ in range(5):  # sweeps inside the next deadline: nothing due
        fake_clock.advance(deadline_us // 10)
        assert (await r.run_once()).decisions == 0
    assert stage.has_group(team_group(7))
    fake_clock.advance(deadline_us)
    assert (await r.run_once()).committed == 1
    assert len(capture.payloads) == 2
    await stages.close()


async def test_two_partitions_with_the_same_group_and_offsets_write_distinct_objects(
    httpx_mock: Any,
    flusher_factory: Any,
    store: Any,
    stages: Any,
    fake_clock: Any,
    mono: Any,
    sleeper: Any,
) -> None:
    cfg = make_config()
    flusher = flusher_factory(cfg=cfg, store=store)
    script_options(httpx_mock)
    capture = CommitCapture(httpx_mock, [commit_ok(91), commit_ok(92)])
    for partition in (0, 1):
        await stages.open_partition("events", partition, now_us=fake_clock())
        await stage_rows(
            stages,
            fake_clock,
            7,
            [(0, event_payload(7, NOW, f"p{partition}"))],
            partition=partition,
        )
    await runner(stages, flusher, cfg, fake_clock, mono, sleeper).run_once(drain=True)
    paths = [p["appends"][0]["files"][0]["path"] for p in capture.payloads]
    assert len(set(paths)) == 2
    assert len({p["idempotency_key"] for p in capture.payloads}) == 2
    assert sorted(committed_offsets(store, capture)) == ["p0", "p1"]
    await stages.close()


async def test_failure_state_is_pruned_when_a_partition_closes(
    httpx_mock: Any,
    flusher_factory: Any,
    store: Any,
    stages: Any,
    fake_clock: Any,
    mono: Any,
    sleeper: Any,
) -> None:
    cfg = make_config()
    flusher = flusher_factory(cfg=cfg, store=store)
    script_options(httpx_mock)
    CommitCapture(httpx_mock, [commit_refusal(422, "record_validation", "no")])
    await stages.open_partition("events", 1, now_us=fake_clock())
    await stage_rows(stages, fake_clock, 7, [(0, event_payload(7, NOW))], partition=1)
    r = runner(stages, flusher, cfg, fake_clock, mono, sleeper)
    assert (await r.run_once(drain=True)).failed == 1
    assert [k[:2] for k in r._failures] == [("events", 1)]
    await stages.close_partition("events", 1)
    assert (await r.run_once()).decisions == 0
    assert r._failures == {}
    await stages.close()


async def test_a_stop_during_the_drain_returns_without_flushing(
    httpx_mock: Any,
    flusher_factory: Any,
    store: Any,
    stages: Any,
    fake_clock: Any,
    mono: Any,
    sleeper: Any,
) -> None:
    """N1: a stopped runner's drain returns at once; nothing is flushed
    and the rows stay for the next process."""
    cfg = make_config()
    flusher = flusher_factory(cfg=cfg, store=store)
    capture = CommitCapture(httpx_mock, [])
    await stages.open_partition("events", 0, now_us=fake_clock())
    await stage_rows(stages, fake_clock, 7, [(0, event_payload(7, NOW))])
    r = runner(stages, flusher, cfg, fake_clock, mono, sleeper)
    r.stop()
    assert r.stopping
    await asyncio.wait_for(r.drain(), timeout=5)
    assert capture.bodies == []
    assert await staged_count(stages, 7) == 1
    await stages.close()
