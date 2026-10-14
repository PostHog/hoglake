"""Crash recovery with the real entry point, at least once.

Millrace runs as a SUBPROCESS (``python -m millrace.main``) with the
hoglake API behind the test's CommitProxy, so failures land ON THE WIRE:

- kill -9 mid-flush: the proxy HOLDS a commit's response (the server
  committed; the child never learns it) and the child is SIGKILLed in
  that window. The restart's startup drain re-flushes everything still
  staged, that window included: a duplicate, never a loss.
- lost commit response: the proxy DROPS every commit response after the
  server commits. The child resends the identical request under the same
  key and the server's receipt dedupes it (the receipt never moves).
  SIGKILL inside the retry storm; the restart re-flushes.
- startup drain: a child stages a wave without flushing it and is
  SIGKILLed; the restarted child reports not-ready while it flushes that
  wave, and only then is assigned and consumes the next wave.

Every scenario ends with the at-least-once reconciliation: every
produced seq queryable at least once, nothing foreign, and the group
committed to the log ends.
"""

from __future__ import annotations

import asyncio
import re
import signal
import time

import livekit
import pytest
from livekit import now_us

pytestmark = pytest.mark.live

TENANTS = 4
FLUSH_DEADLINE_S = 4
WAVE_EVENTS = 600


def _cfg(lake_home, topic, *, deadline_s: int = FLUSH_DEADLINE_S) -> object:
    # Children tune only through the environment: slow == age deadline
    # makes every group due at the deadline whatever its size.
    return livekit.live_config(
        topic=topic.name,
        group=topic.group,
        stage_base_path=f"stage/{livekit.RUN_ID}/{lake_home.slug}",
        catalog=lake_home.catalog.name,
        namespace="ns1",
        table="events",
        flush_deadline_s=deadline_s,
        slow_lane_deadline_s=deadline_s,
    )


def _produce(producer, *, count: int, seq_start: int) -> int:
    skew = livekit.Skew(TENANTS, seed=99)
    teams = skew.sample(count)
    base = now_us()
    livekit.produce_batch(
        producer,
        [(team, base + i) for i, team in enumerate(teams)],
        seq_start=seq_start,
    )
    return seq_start + count


def _receipt_snapshot(lake_home, key: str) -> int | None:
    """The server-side receipt for an idempotency key (direct read)."""
    from pyhoglake import NotFoundError

    try:
        body = lake_home.client._request(
            "GET",
            f"/catalogs/{lake_home.catalog.name}/commit/receipts/{key}",
        )
    except NotFoundError:
        return None
    return int(body["snapshot_id"])


async def _reconcile_at_least_once(
    lake_home, table, producer, admin, topic, total: int
) -> None:
    ends = livekit.end_offsets(admin, topic.name, range(topic.partitions))
    assert sum(ends.values()) == total
    produced = [e.seq for e in producer.produced]
    assert len(produced) == total
    await livekit.wait_until_async(
        lambda: livekit.all_landed(table, lake_home.catalog, lake_home.fs, produced),
        240,
        desc=f"all {total} produced seqs queryable",
    )
    await livekit.wait_until_async(
        lambda: livekit.group_offsets_reached(
            livekit.committed_offsets(admin, topic.group, topic.name), ends
        ),
        120,
        desc=f"group {topic.group} committed to the log ends {ends}",
    )
    counts = await asyncio.to_thread(
        livekit.lake_seq_counts, table, lake_home.head(), lake_home.fs
    )
    livekit.assert_at_least_once(counts, produced)


def _stop(*children: livekit.Child | None) -> None:
    for child in children:
        if child is not None and child.proc is not None and child.proc.poll() is None:
            child.terminate()
            child.wait_exit(timeout_s=30)


async def test_kill_minus_9_mid_flush(
    lake_home, topic_factory, producer_factory, admin
):
    table = lake_home.create_events_table()
    topic = topic_factory(partitions=2)
    producer = producer_factory(topic.name)
    cfg = _cfg(lake_home, topic)

    proxy = livekit.CommitProxy(
        upstream_port=int(livekit.HOGLAKE_URL.rsplit(":", 1)[1])
    )
    child_a: livekit.Child | None = None
    child_b: livekit.Child | None = None
    try:
        proxy.arm_hold_next()
        port_a = livekit.free_port()
        child_a = livekit.Child(
            livekit.child_env(cfg, hoglake_url=proxy.url, metrics_port=port_a),
            name="kill-a",
        )
        child_a.start()
        child_a.wait_ready(port_a, timeout_s=120)
        next_seq = _produce(producer, count=WAVE_EVENTS, seq_start=0)

        # A commit's response arrives held: the server committed, the
        # child never saw it.
        proxy.wait_held(timeout_s=120)
        key = proxy.held_key
        assert key is not None
        assert [s for k, s in proxy.responses if k == key] == [200]
        assert _receipt_snapshot(lake_home, key) is not None

        child_a.kill()
        assert child_a.wait_exit() == -signal.SIGKILL

        # Restart on the direct url: the startup drain re-flushes the held
        # window's rows (still staged) under a fresh key.
        port_b = livekit.free_port()
        child_b = livekit.Child(
            livekit.child_env(cfg, metrics_port=port_b), name="kill-b"
        )
        child_b.start()
        child_b.wait_ready(port_b, timeout_s=180)
        assert "startup drain finished" in child_b.output()

        next_seq = _produce(producer, count=WAVE_EVENTS, seq_start=next_seq)
        await _reconcile_at_least_once(
            lake_home, table, producer, admin, topic, next_seq
        )
    finally:
        proxy.close()
        _stop(child_a, child_b)


async def test_lost_commit_response(lake_home, topic_factory, producer_factory, admin):
    table = lake_home.create_events_table()
    topic = topic_factory(partitions=2)
    producer = producer_factory(topic.name)
    cfg = _cfg(lake_home, topic)

    proxy = livekit.CommitProxy(
        upstream_port=int(livekit.HOGLAKE_URL.rsplit(":", 1)[1])
    )
    child_a: livekit.Child | None = None
    child_b: livekit.Child | None = None
    try:
        # EVERY commit response is dropped after landing on the server.
        proxy.arm_drop()
        port_a = livekit.free_port()
        child_a = livekit.Child(
            livekit.child_env(cfg, hoglake_url=proxy.url, metrics_port=port_a),
            name="lost-a",
        )
        child_a.start()
        child_a.wait_ready(port_a, timeout_s=120)
        next_seq = _produce(producer, count=WAVE_EVENTS, seq_start=0)

        livekit.wait_until(
            lambda: len(proxy.dropped) >= 1,
            120,
            desc="the first dropped commit response",
        )
        key = proxy.dropped[0]
        assert (key, 200) in proxy.responses
        receipt_snapshot = _receipt_snapshot(lake_home, key)
        assert receipt_snapshot is not None

        # The retry resends under the SAME key; the receipt absorbs it.
        livekit.wait_until(
            lambda: proxy.dropped.count(key) >= 2,
            60,
            desc="the identical retry being dropped too",
        )
        assert _receipt_snapshot(lake_home, key) == receipt_snapshot

        child_a.kill()
        assert child_a.wait_exit() == -signal.SIGKILL

        port_b = livekit.free_port()
        child_b = livekit.Child(
            livekit.child_env(cfg, metrics_port=port_b), name="lost-b"
        )
        child_b.start()
        child_b.wait_ready(port_b, timeout_s=180)
        assert _receipt_snapshot(lake_home, key) == receipt_snapshot

        next_seq = _produce(producer, count=WAVE_EVENTS, seq_start=next_seq)
        await _reconcile_at_least_once(
            lake_home, table, producer, admin, topic, next_seq
        )
    finally:
        proxy.close()
        _stop(child_a, child_b)


async def test_startup_drain_flushes_before_consuming(
    lake_home, topic_factory, producer_factory, admin
):
    table = lake_home.create_events_table()
    topic = topic_factory(partitions=2)
    producer = producer_factory(topic.name)
    child_a: livekit.Child | None = None
    child_b: livekit.Child | None = None
    try:
        # Child A never flushes (hour-long deadlines): wave 1 stays staged.
        port_a = livekit.free_port()
        child_a = livekit.Child(
            livekit.child_env(
                _cfg(lake_home, topic, deadline_s=3600), metrics_port=port_a
            ),
            name="drain-a",
        )
        child_a.start()
        child_a.wait_ready(port_a, timeout_s=120)
        next_seq = _produce(producer, count=WAVE_EVENTS, seq_start=0)
        ends = livekit.end_offsets(admin, topic.name, range(topic.partitions))
        await livekit.wait_until_async(
            lambda: livekit.group_offsets_reached(
                livekit.committed_offsets(admin, topic.group, topic.name), ends
            ),
            120,
            desc="wave 1 durably staged (offsets committed)",
        )
        assert not livekit.lake_files(table, lake_home.head())
        child_a.kill()
        assert child_a.wait_exit() == -signal.SIGKILL

        # Wave 2 waits in Kafka while the restarted child drains wave 1.
        next_seq = _produce(producer, count=WAVE_EVENTS, seq_start=next_seq)
        port_b = livekit.free_port()
        child_b = livekit.Child(
            livekit.child_env(_cfg(lake_home, topic), metrics_port=port_b),
            name="drain-b",
        )
        child_b.start()
        statuses: list[int | None] = []
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            status = livekit.probe_status(port_b, "/readyz")
            statuses.append(status)
            if status == 200:
                break
            assert child_b.proc is not None and child_b.proc.poll() is None, (
                child_b.output()[-4000:]
            )
            await asyncio.sleep(0.05)
        assert statuses[-1] == 200, child_b.output()[-4000:]
        out = child_b.output()
        drained = out.index("startup drain finished")
        # The streaming drain of one wave can finish before the first
        # probe lands, so the order is read from the log: the partitions'
        # streams started, the drain finished, and only then the assign
        # (readiness turns 200 only after start() returns, test_main).
        streaming = out.index("streaming its staged rows")
        assert streaming < drained < out.index("assigned "), out[-4000:]
        streamed = re.findall(
            r"startup drain of \S+\[\d+\] streamed: \d+ window\(s\), (\d+) row\(s\)",
            out[:drained],
        )
        assert len(streamed) == topic.partitions, out[-4000:]
        assert sum(int(rows) for rows in streamed) >= WAVE_EVENTS
        samples = livekit.scrape_metrics(port_b)
        drain_rows = livekit.metric(samples, "millrace_drain_rows_total")
        assert drain_rows is not None and drain_rows >= WAVE_EVENTS
        assert out.index("assigned ") > drained, "assigned before the drain finished"
        # Every wave-1 row was published by the drain itself.
        wave1 = list(range(WAVE_EVENTS))
        assert livekit.all_landed(table, lake_home.catalog, lake_home.fs, wave1)

        await _reconcile_at_least_once(
            lake_home, table, producer, admin, topic, next_seq
        )
    finally:
        _stop(child_a, child_b)
