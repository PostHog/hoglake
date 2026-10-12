"""Scenarios 2 and 3 (Phase 5): crash recovery with the real entry point.

Millrace runs as a SUBPROCESS (``python -m millrace.main`` — the shipped
wiring, not a test double) with the hoglake API behind the test's
CommitProxy, so the failure lands ON THE WIRE:

- scenario 2 (kill -9 mid-flush): the proxy HOLDS the first commit's
  response — the server has committed (the proxy read the 200 off the
  wire), the child never learns it. SIGKILL in that window. The restart
  finds the persisted ``prepared/`` entry and the receipt settles it
  (zero republication); everything still staged reflushes.
- scenario 3 (lost commit response): the proxy DROPS every commit
  response after the server commits. The child's retry ladder republishes
  the IDENTICAL persisted bytes; the server's receipt dedupes every
  attempt — the catalog head never moves. SIGKILL inside the retry
  storm; the restart resolves the entry from the receipt.

Both then produce a second wave through the restarted child and
reconcile EXACTLY: per-tenant counts, no loss, no double-count.
"""

from __future__ import annotations

import signal

import livekit
import pytest
from livekit import now_us

pytestmark = pytest.mark.live

TENANTS = 4
FLUSH_DEADLINE_S = 4
WAVE_EVENTS = 600


def _cfg(lake_home, topic) -> object:
    return livekit.live_config(
        topic=topic.name,
        group=topic.group,
        partitions=tuple(range(topic.partitions)),
        stage_base_path=f"stage/{livekit.RUN_ID}/{lake_home.slug}",
        catalog=lake_home.catalog.name,
        namespace="ns1",
        table="events",
        flush_deadline_s=FLUSH_DEADLINE_S,
        flush_sweep_s=1,
    )


# These scenarios script the commit wire EXACTLY (the proxy holds/drops
# "the next" response, the assertions name "the" in-flight commit and the
# catalog head's stability): that needs ONE commit in flight, so the
# child's flush sweep runs serial. M5's bounded parallelism (the default
# 4, exercised everywhere else in this suite) interleaves four teams'
# commits through the proxy and the exact-sequence scripting no longer
# knows which one the proxy parked.
_SERIAL_FLUSH = {"MILLRACE_FLUSH_CONCURRENCY": "1"}


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
    """The server-side receipt for an idempotency key (direct read, no
    proxy)."""
    from pyhoglake import NotFoundError

    try:
        body = lake_home.client._request(
            "GET",
            f"/catalogs/{lake_home.catalog.name}/commit/receipts/{key}",
        )
    except NotFoundError:
        return None
    return int(body["snapshot_id"])


def _commit_keys(proxy: livekit.CommitProxy) -> list[str]:
    """Every commit's idempotency key the proxy forwarded, in order."""
    return [
        key
        for method, path, key in proxy.requests
        if method == "POST" and path.endswith("/commit/prepared") and key is not None
    ]


async def _reconcile_exactly(
    lake_home, table, producer, admin, topic, total: int
) -> None:
    """The shared tail of both scenarios: everything produced is
    queryable, per tenant, exactly once, and the group committed to the
    log ends."""
    ends = livekit.end_offsets(admin, topic.name, range(topic.partitions))
    assert sum(ends.values()) == total
    await livekit.wait_until_async(
        lambda: (
            sum(livekit.lake_team_counts(table, lake_home.head()).values()) == total
        ),
        240,
        desc=f"all {total} events queryable",
    )
    await livekit.wait_until_async(
        lambda: livekit.group_offsets_reached(
            livekit.committed_offsets(admin, topic.group, topic.name), ends
        ),
        120,
        desc=f"group {topic.group} committed to the log ends {ends}",
    )
    produced_by_team: dict[int, int] = {}
    for e in producer.produced:
        produced_by_team[e.team_id] = produced_by_team.get(e.team_id, 0) + 1
    counts = livekit.lake_team_counts(table, lake_home.head())
    assert counts == produced_by_team, "per-tenant mismatch after recovery: " + str(
        [
            (t, produced_by_team.get(t, 0), counts.get(t, 0))
            for t in sorted(set(produced_by_team) | set(counts))
            if produced_by_team.get(t, 0) != counts.get(t, 0)
        ]
    )


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
            livekit.child_env(
                cfg, hoglake_url=proxy.url, metrics_port=port_a, extra=_SERIAL_FLUSH
            ),
            name="kill-a",
        )
        child_a.start()
        child_a.wait_ready(port_a, timeout_s=120)

        # Wave 1: flushes start at the first deadline crossing.
        next_seq = _produce(producer, count=WAVE_EVENTS, seq_start=0)

        # The first commit's response arrives held: the server committed
        # (the proxy read the 200), the child never saw it.
        proxy.wait_held(timeout_s=120)
        key = _commit_keys(proxy)[-1]
        held = [s for k, s in proxy.responses if k == key]
        assert held == [200], f"the held commit was answered {held}, not 200"
        receipt_snapshot = _receipt_snapshot(lake_home, key)
        assert receipt_snapshot is not None, (
            "the server holds no receipt for a commit it answered 200"
        )

        child_a.kill()
        assert child_a.wait_exit() == -signal.SIGKILL

        # Restart: same group, same stage paths, DIRECT url. Recovery must
        # settle the persisted entry from the receipt — zero republication.
        port_b = livekit.free_port()
        child_b = livekit.Child(
            livekit.child_env(cfg, metrics_port=port_b, extra=_SERIAL_FLUSH),
            name="kill-b",
        )
        child_b.start()
        child_b.wait_ready(port_b, timeout_s=120)
        # The receipt settles the persisted entry — the line names the
        # ORIGINAL commit's snapshot, proving zero republication.
        child_b.wait_for_log(
            f"from the commit receipt (snapshot {receipt_snapshot})", timeout_s=120
        )
        # No replay of the persisted request happened anywhere.
        assert "replayed the persisted commit" not in child_b.output()
        # The receipt is stable: the kill between persist and settle
        # produced exactly one logical commit.
        assert _receipt_snapshot(lake_home, key) == receipt_snapshot

        # Wave 2 through the restarted child; then exact reconciliation.
        next_seq = _produce(producer, count=WAVE_EVENTS, seq_start=next_seq)
        await _reconcile_exactly(lake_home, table, producer, admin, topic, next_seq)
    finally:
        proxy.close()
        for child in (child_a, child_b):
            if (
                child is not None
                and child.proc is not None
                and child.proc.poll() is None
            ):
                child.terminate()
                child.wait_exit(timeout_s=30)


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
            livekit.child_env(
                cfg, hoglake_url=proxy.url, metrics_port=port_a, extra=_SERIAL_FLUSH
            ),
            name="lost-a",
        )
        child_a.start()
        child_a.wait_ready(port_a, timeout_s=120)

        next_seq = _produce(producer, count=WAVE_EVENTS, seq_start=0)

        # The first commit: answered 200 on the wire, never delivered.
        livekit.wait_until(
            lambda: len(proxy.dropped) >= 1,
            120,
            desc="the first dropped commit response",
        )
        key = proxy.dropped[0]
        assert (key, 200) in proxy.responses, (
            f"the dropped commit was answered {proxy.responses}, not 200"
        )
        receipt_snapshot = _receipt_snapshot(lake_home, key)
        assert receipt_snapshot is not None
        assert lake_home.head() == receipt_snapshot

        # The retry ladder republishes the IDENTICAL persisted bytes; the
        # server's receipt dedupes every attempt — the head never moves.
        livekit.wait_until(
            lambda: proxy.dropped.count(key) >= 2,
            60,
            desc="the identical retry being dropped too",
        )
        # The proxy logs a drop only AFTER reading the server's 200 off
        # the wire, so at this point attempt 2 has landed and been
        # deduped: the head must not have moved.
        assert lake_home.head() == receipt_snapshot, (
            "a deduped retry advanced the catalog head — the receipt did not absorb it"
        )

        child_a.kill()
        assert child_a.wait_exit() == -signal.SIGKILL

        # Restart: recovery finds the persisted prepared entry and the
        # RECEIPT resolves it — zero republication, zero new snapshots.
        port_b = livekit.free_port()
        child_b = livekit.Child(
            livekit.child_env(cfg, metrics_port=port_b, extra=_SERIAL_FLUSH),
            name="lost-b",
        )
        child_b.start()
        child_b.wait_ready(port_b, timeout_s=120)
        child_b.wait_for_log(
            f"from the commit receipt (snapshot {receipt_snapshot})", timeout_s=120
        )
        assert "replayed the persisted commit" not in child_b.output()
        assert _receipt_snapshot(lake_home, key) == receipt_snapshot

        next_seq = _produce(producer, count=WAVE_EVENTS, seq_start=next_seq)
        await _reconcile_exactly(lake_home, table, producer, admin, topic, next_seq)
    finally:
        proxy.close()
        for child in (child_a, child_b):
            if (
                child is not None
                and child.proc is not None
                and child.proc.poll() is None
            ):
                child.terminate()
                child.wait_exit(timeout_s=30)
