"""Shared helpers for the staging test layers (not a test module — the
name does not match ``test_*`` and pytest never collects it).

Lives beside the tests so the component suite, the file:///
integration suite, the parity probes and the kill-child subprocess all
build records and payloads the same way (hedgerow's fakes.py
pattern).
"""

from __future__ import annotations

import asyncio
from typing import Any, Literal

from millrace.stage import StagedRecord

NOW = 1_800_000_000_000_000  # an arbitrary fixed staging instant, µs (≈ 2027-01-15)


def fast_flush_settings() -> Any:
    """SlateDB settings with a 5 ms WAL flush interval (default: 100 ms).

    ``stage_batch`` awaits remote durability, whose latency tracks the
    WAL flush ticker; the default would put ~100 ms on every staged
    batch and dominate suite time without exercising anything different.
    """
    from slatedb.uniffi import Settings

    s = Settings.default()
    s.set("flush_interval", '"5ms"')
    return s


def shared_cache(topic: str = "events", partition: int = 0) -> dict[str, Any]:
    """The cache keywords ``PartitionStage.open``/``open_store`` now
    REQUIRE, at the production shape: a split cache at the config
    defaults (``build_slatedb_cache``) scoped by ``db_cache_id_for``.
    A fresh cache per call. Spread it:
    ``open_store(..., **shared_cache(topic, partition))``."""
    from millrace.config import (
        SLATEDB_BLOCK_CACHE_BYTES_DEFAULT,
        SLATEDB_META_CACHE_BYTES_DEFAULT,
    )
    from millrace.stage import build_slatedb_cache, db_cache_id_for

    return {
        "db_cache": build_slatedb_cache(
            block_cache_bytes=SLATEDB_BLOCK_CACHE_BYTES_DEFAULT,
            meta_cache_bytes=SLATEDB_META_CACHE_BYTES_DEFAULT,
        ),
        "db_cache_id": db_cache_id_for(topic, partition),
    }


#: The field name tests label one-field tuples with (an identity(team_id)
#: spec's display name).
FIELDS = ("team_id",)


def g(team: int) -> tuple[str | None, ...]:
    """The one-field partition tuple an ``identity(team_id)`` spec gives
    ``team`` — the staging group the stage tests key rows under."""
    return (str(team),)


def rec(team: int, ts: int, offset: int, payload: bytes | None = None) -> StagedRecord:
    """A staged record of tuple ``g(team)`` at ``offset``. ``ts`` only
    shapes the default payload text (records carry no timestamp)."""
    return StagedRecord(
        group=g(team),
        offset=offset,
        payload=payload
        if payload is not None
        else f"payload-{team}-{ts}-{offset}".encode(),
    )


async def stream_windows(
    stage: Any, *, max_bytes: int = 1 << 40, now_us: int = NOW
) -> list[Any]:
    """Every window of one :class:`~millrace.stage.RowStream` pass (the
    startup drain's read), in stream order."""
    stream = stage.row_stream(max_bytes=max_bytes, now_us=now_us)
    out = []
    while (window := await stream.next_window()) is not None:
        out.append(window)
    return out


async def staged_groups(stage: Any) -> list[tuple[str | None, ...]]:
    """The groups with staged rows, in key order, each once (one
    :func:`stream_windows` pass)."""
    groups: list[tuple[str | None, ...]] = []
    for window in await stream_windows(stage):
        if not groups or groups[-1] != window.group:
            groups.append(window.group)
    return groups


def payload_for(offset: int, size: int) -> bytes:
    """The deterministic payload the kill-child writes for ``offset``;
    the parent re-derives it to verify byte-exact survival."""
    body = f"rec-{offset:012d}|".encode()
    return (body * (size // len(body) + 1))[:size]


class LockedScanSpy:
    """Records, for every range scan a stage's ``Db`` opens — directly,
    in a transaction (``begin``) or on a snapshot (``snapshot``) — and
    every iterator call that reads (``next_batch``, ``next``, ``seek``),
    whether the stage's write lock was held at that moment — then
    delegates, so the scans really run.

    The pin for "no scan under the write lock": at object-store latency
    a scan page is one or more GETs, and a lock holder that waits on one
    stalls staging and the consumer's offset commits behind it (the
    hoglake-millrace dev hang). Install with
    ``LockedScanSpy(stage, monkeypatch)``; ``under_lock`` lists the
    offending calls, ``calls`` all of them. Iterator calls are checked
    per call, so a scan opened before the lock but paged inside it is
    caught too. "Held" means held by the CALLING task (:class:`_OwnedLock`):
    another task reading while a write holds the lock stalls nobody. The
    blind spot: a holder that offloads a scan to another task or thread
    and waits for it under the lock is not seen."""

    _SCANS = ("scan", "scan_with_options", "scan_prefix", "scan_prefix_with_options")

    def __init__(self, stage: Any, monkeypatch: Any) -> None:
        self.calls: list[tuple[str, bool]] = []
        self._stage = stage
        assert not stage._write_lock.locked()
        self._lock = _OwnedLock()
        monkeypatch.setattr(stage, "_write_lock", self._lock)
        db = stage._db
        self._wrap_scans(db, "db", monkeypatch.setattr)
        for opener, who in (("begin", "txn"), ("snapshot", "snapshot")):
            real = getattr(db, opener)

            async def spied_open(*args: Any, _real: Any = real, _who: str = who) -> Any:
                handle = await _real(*args)
                self._wrap_scans(handle, _who, setattr)
                return handle

            monkeypatch.setattr(db, opener, spied_open)

    def _wrap_scans(self, target: Any, who: str, set_attr: Any) -> None:
        for name in self._SCANS:
            real = getattr(target, name)

            async def spied(*args: Any, _real: Any = real, _name: str = name) -> Any:
                self._note(f"{who}.{_name}")
                return _Iter(await _real(*args), self)

            set_attr(target, name, spied)

    def _note(self, what: str) -> None:
        self.calls.append((what, self._lock.held_by_current_task()))

    @property
    def under_lock(self) -> list[str]:
        return [what for what, locked in self.calls if locked]


class _OwnedLock(asyncio.Lock):
    """An asyncio.Lock that knows its holder: a read by ANOTHER task
    while the lock is held (the startup drain streams while a window's
    settle writes) stalls nobody; a read by the holder does."""

    def __init__(self) -> None:
        super().__init__()
        self._owner: asyncio.Task[Any] | None = None

    async def acquire(self) -> Literal[True]:
        await super().acquire()
        self._owner = asyncio.current_task()
        return True

    def release(self) -> None:
        self._owner = None
        super().release()

    def held_by_current_task(self) -> bool:
        return self.locked() and self._owner is asyncio.current_task()


class _Iter:
    def __init__(self, real: Any, spy: LockedScanSpy) -> None:
        self._real = real
        self._spy = spy

    async def next_batch(self, n: int) -> Any:
        self._spy._note("next_batch")
        return await self._real.next_batch(n)

    async def next(self) -> Any:
        self._spy._note("next")
        return await self._real.next()

    async def seek(self, key: bytes) -> Any:
        self._spy._note("seek")
        return await self._real.seek(key)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)
