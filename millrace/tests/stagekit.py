"""Shared helpers for the staging test layers (not a test module — the
name does not match ``test_*`` and pytest never collects it).

Lives beside the tests so the component suite, the file:///
integration suite, the parity probes and the kill-child subprocess all
build records, windows and payloads the same way (hedgerow's fakes.py
pattern).
"""

from __future__ import annotations

from typing import Any

from millrace.keyspace import FlushedKey, PreparedRequest
from millrace.stage import StagedRecord

NOW = 1_800_000_000_000_000  # an arbitrary fixed staging instant, µs (≈ 2027-01-15)
TABLE = "00000000-0000-0000-0000-0000000000aa"  # the "table" idempotency scope


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
    A fresh cache per call — correctness never depends on cache warmth,
    and moka allocates by occupancy, not capacity. Spread it:
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


def window(team: int, first: int, last: int) -> FlushedKey:
    """The settlement window for the committed range of tuple ``g(team)``
    over ``first..last`` — what ``commit_flushed``
    deletes."""
    return FlushedKey(g(team), first, last, group_fields=FIELDS)


def prepared(
    key: str, *, team: int, first: int = 0, at: int = NOW, rows: int = 0
) -> PreparedRequest:
    """A persisted prepared request for tuple ``g(team)`` on events[0]."""
    return PreparedRequest(
        idempotency_key=key,
        body=b"{}",
        persisted_at=at,
        topic="events",
        partition=0,
        group=g(team),
        group_fields=FIELDS,
        row_count=rows,
        digest="0" * 64,
        scanned_at=None,
    )


def legacy_marker_kv(team: int, first: int, last: int) -> tuple[bytes, bytes]:
    """A ``flushed/`` (key, value) pair as pre-lifecycle builds wrote it.

    Only tests seed these: no current code writes ``flushed/`` keys, and
    ``recover()`` collects them WITHOUT decoding — the value here is
    deliberately not a real envelope, to pin that. The key keeps the
    legacy byte shape (u64 team + two offset-binary int64s) so the
    collection is exercised on realistic bytes.
    """

    def ordered(v: int) -> bytes:
        return (v + (1 << 63)).to_bytes(8, "big")

    key = b"flushed/" + team.to_bytes(8, "big") + ordered(first) + ordered(last)
    return key, b"\x01not-a-real-flushed-value"


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
    caught too."""

    _SCANS = ("scan", "scan_with_options", "scan_prefix", "scan_prefix_with_options")

    def __init__(self, stage: Any, monkeypatch: Any) -> None:
        self.calls: list[tuple[str, bool]] = []
        self._stage = stage
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
        self.calls.append((what, self._stage._write_lock.locked()))

    @property
    def under_lock(self) -> list[str]:
        return [what for what, locked in self.calls if locked]


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
