"""Integration tests for the staging layer: real SlateDB on ``file:///``.

What the memory-backed component layer cannot see: persistence across a
process boundary. The hard-kill test stages through a subprocess that is
SIGKILLed mid-stream: every ACKNOWLEDGED row must be present and
byte-exact on reopen, batch-atomic, and read by the drain's row stream. The
reassignment test reopens the same path from a fresh manager; the
fencing test contests a live writer's path.
"""

from __future__ import annotations

import asyncio
import select
import signal
import subprocess
import sys
from pathlib import Path

import pytest
from slatedb.uniffi import ObjectStore
from stagekit import (
    NOW,
    fast_flush_settings,
    g,
    payload_for,
    rec,
    shared_cache,
    staged_groups,
)

from millrace.stage import (
    FencedError,
    PartitionStage,
    StagedRecord,
    StageManager,
    is_fenced,
)

pytestmark = pytest.mark.integration

KILL_CHILD = Path(__file__).with_name("stage_kill_child.py")

BATCH_SIZE = 10
PAYLOAD_SIZE = 1024
TEAMS = 4
KILL_AFTER_ACKS = 12
BIG = 1 << 40


def _read_line(proc: subprocess.Popen[str], timeout_s: float) -> str:
    ready, _, _ = select.select([proc.stdout], [], [], timeout_s)
    if not ready:
        proc.kill()
        raise AssertionError(
            f"child produced no line within {timeout_s}s; stderr:\n{proc.stderr.read()}"
        )
    line = proc.stdout.readline().strip()
    if not line:
        raise AssertionError(
            f"child exited early (rc={proc.poll()}); stderr:\n{proc.stderr.read()}"
        )
    return line


async def _open(path: str) -> PartitionStage:
    return await PartitionStage.open_store(
        ObjectStore.resolve("file:///"),
        path,
        topic="events",
        partition=0,
        settings=fast_flush_settings(),
        **shared_cache(),
    )


def test_durability_across_hard_kill(tmp_path: Path) -> None:
    """A subprocess stages batches and is SIGKILLed mid-stream. On reopen:
    every acknowledged row is present and byte-exact, present rows come
    in whole batches (the write batch is the atomic unit), and the stats
    prefix reconciles exactly with the row scans."""
    path = str(tmp_path / "millrace" / "events" / "0")
    proc = subprocess.Popen(
        [
            sys.executable,
            str(KILL_CHILD),
            path,
            "100000",
            str(BATCH_SIZE),
            str(PAYLOAD_SIZE),
            str(TEAMS),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    acked: list[tuple[int, int, int]] = []  # (batch_index, first_offset, last_offset)
    try:
        assert _read_line(proc, 60) == "READY"
        while len(acked) < KILL_AFTER_ACKS:
            line = _read_line(proc, 60)
            batch_index, first, last = map(int, line.split()[1:])
            assert batch_index == len(acked)
            assert first == (acked[-1][2] + 1 if acked else 0)  # contiguous stream
            assert last == first + BATCH_SIZE - 1
            acked.append((batch_index, first, last))
        proc.kill()  # SIGKILL, mid-stream
        proc.wait(timeout=30)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=30)
    assert proc.returncode == -signal.SIGKILL

    asyncio.run(_verify_after_kill(path, acked))


async def _verify_after_kill(path: str, acked: list[tuple[int, int, int]]) -> None:
    stage = await _open(path)
    try:
        # 1. the row stream finds exactly the groups that hold rows
        found = await staged_groups(stage)
        rows_by_team: dict[int, dict[int, bytes]] = {t: {} for t in range(TEAMS)}
        for team in range(TEAMS):
            scan = await stage.scan((str(team),), max_bytes=BIG, now_us=NOW)
            assert scan.complete
            for rk, value in scan.rows:
                rows_by_team[team][rk.offset] = value
        assert set(found) == {(str(t),) for t in range(TEAMS) if rows_by_team[t]}

        # 2. every acknowledged row survived, byte-exact
        for _, first, last in acked:
            for offset in range(first, last + 1):
                value = rows_by_team[offset % TEAMS][offset]
                assert value == payload_for(offset, PAYLOAD_SIZE)

        # 3. batch atomicity: an un-acked batch is wholly present or absent
        present = {o for rows in rows_by_team.values() for o in rows}
        for batch_index in range(max(present) // BATCH_SIZE + 1):
            batch_offsets = set(
                range(batch_index * BATCH_SIZE, (batch_index + 1) * BATCH_SIZE)
            )
            landed = len(batch_offsets & present)
            assert landed in (0, BATCH_SIZE), f"batch {batch_index} split: {landed}"

        # 4. the reopened instance keeps accepting writes
        ack = await stage.stage_batch(
            [StagedRecord(group=("0",), offset=10**9, payload=b"after")],
            now_us=NOW + 10**6,
        )
        assert ack.rows == 1
    finally:
        await stage.close()


async def test_assignment_change_between_deploys(tmp_path: Path):
    """A fresh manager opens the same partition path, reads the
    previous owner's group and rows exactly, and keeps staging on top."""
    base = str(tmp_path / "millrace")
    first_owner = StageManager("file:///", base, settings=fast_flush_settings())
    stage = await first_owner.open_partition("events", 3)
    assert stage.snapshot() == []
    await stage.stage_batch([rec(11, 0, i) for i in range(10)], now_us=NOW)
    await stage.stage_batch([rec(11, 0, 10 + i) for i in range(10)], now_us=NOW + 1)
    await first_owner.close()

    second_owner = StageManager("file:///", base, settings=fast_flush_settings())
    assert second_owner.path_for("events", 3) == first_owner.path_for("events", 3)
    stage = await second_owner.open_partition("events", 3)
    assert await staged_groups(stage) == [g(11)]
    scan = await stage.scan(g(11), max_bytes=BIG, now_us=NOW)
    assert [rk.offset for rk, _ in scan.rows] == list(range(20))
    await stage.stage_batch([rec(11, 0, 20 + i) for i in range(5)], now_us=NOW + 6)
    scan = await stage.scan(g(11), max_bytes=BIG, now_us=NOW)
    assert [rk.offset for rk, _ in scan.rows] == list(range(25))
    await second_owner.close()


async def test_a_settle_survives_a_clean_reopen(tmp_path: Path):
    path = str(tmp_path / "db")
    stage = await _open(path)
    await stage.stage_batch([rec(2, 0, i, b"x" * 10) for i in range(6)], now_us=NOW)
    scan = await stage.scan(g(2), max_bytes=40, now_us=NOW)
    assert not await stage.settle(scan)
    await stage.close()
    reopened = await _open(path)
    assert await staged_groups(reopened) == [g(2)]
    rest = await reopened.scan(g(2), max_bytes=BIG, now_us=NOW)
    assert [rk.offset for rk, _ in rest.rows] == [4, 5]
    assert await reopened.settle(rest)
    await reopened.close()
    empty = await _open(path)
    assert await staged_groups(empty) == []
    await empty.close()


async def test_a_contested_open_fences_the_older_stage(tmp_path: Path):
    """A second open of a live writer's path succeeds; the older stage's
    next durable write fails FENCED (``is_fenced``), and nothing it
    writes after the contest lands for the new owner."""
    path = str(tmp_path / "db")
    old = await _open(path)
    await old.stage_batch([rec(1, 0, 0)], now_us=NOW)
    new = await _open(path)
    try:
        with pytest.raises(Exception) as excinfo:
            for offset in range(1, 200):
                await old.stage_batch([rec(1, 0, offset)], now_us=NOW)
        assert is_fenced(excinfo.value)
        with pytest.raises(Exception) as probe:
            await old.probe_fence(5)
        assert is_fenced(probe.value)
        assert str(FencedError([("events", 0)])).startswith("stage fenced")
        assert await staged_groups(new) == [g(1)]
        scan = await new.scan(g(1), max_bytes=BIG, now_us=NOW)
        assert [rk.offset for rk, _ in scan.rows] == [0]
    finally:
        await new.close()
        await old.close()  # a fenced shutdown is swallowed
