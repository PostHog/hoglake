"""Shared helpers for the staging test layers (not a test module — the
name does not match ``test_*`` and pytest never collects it).

Lives beside the tests so the component suite, the file:///
integration suite, the parity probes and the kill-child subprocess all
build records, windows and payloads the same way (hedgerow's fakes.py
pattern).
"""

from __future__ import annotations

from typing import Any

from millrace.keyspace import FlushedKey
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


def rec(team: int, ts: int, offset: int, payload: bytes | None = None) -> StagedRecord:
    return StagedRecord(
        team_id=team,
        event_ts_us=ts,
        offset=offset,
        payload=payload
        if payload is not None
        else f"payload-{team}-{ts}-{offset}".encode(),
    )


def window(team: int, first: int, last: int) -> FlushedKey:
    """The settlement window for the committed range (team, first..last)
    — what ``commit_flushed`` / ``settle_quarantined`` delete."""
    return FlushedKey(team, first, last)


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
