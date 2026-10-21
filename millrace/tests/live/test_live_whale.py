"""Scenario 6 (Phase 5): whale overload.

One tenant at ~1000× the median rate crosses the size trigger
repeatedly (full-size flushes, footer stats); the tail tenants in the
SAME run still flush inside the age deadline. Evidence:

- the commit log (``catalog.snapshots()`` messages — the deterministic
  commit text is the audit trail) shows ``trigger=size`` commits for the
  whale and ``trigger=age`` commits for tail teams;
- whale files carry ~a scan budget's worth of rows each; tail files stay
  tiny;
- every row lands at least once, whale and tail alike;
- the tail's worst produce→queryable latency stays within deadline +
  margin while the whale is flushing — the SLA the whole design stands
  on (docs/kafka-ingestion.md §The problem).
"""

from __future__ import annotations

import asyncio
import re
import statistics
import time
from collections import Counter

import livekit
import pytest
from livekit import now_us

pytestmark = pytest.mark.live

WHALE = 1
TAIL_TEAMS = list(range(2, 22))  # 20 tail tenants
WHALE_EVENTS = 4000
TAIL_EVENTS_PER_TEAM = 3
PAD_BYTES = 1000  # ~1.1 KB payloads
# The size lane fires at MILLRACE_FLUSH_BYTES of STAGED bytes and a scan
# reads at most that much: at ~1.06 KB payloads a size flush carries
# ~190 rows.
FLUSH_BYTES = 200 * 1024
FLUSH_DEADLINE_S = 12
TAIL_MARGIN_S = 25.0

# A single-group commit names the flushed partition tuple in its human
# form (``group=team_id=<n>/timestamp_month=<m>``); a batched commit
# (several groups' files) says how many groups it carries.
_COMMIT = re.compile(r"group=team_id=(\d+)/\S* trigger=(\w+)")
_BATCH = re.compile(r"groups=(\d+) trigger=(\w+)")


def _commit_triggers(catalog) -> list[tuple[int | None, str]]:
    """(team_id, trigger) for every millrace commit in the catalog; the
    team is None for a batched commit."""
    out: list[tuple[int | None, str]] = []
    for snap in catalog.snapshots():
        if snap.message and snap.message.startswith("millrace=v2"):
            m = _COMMIT.search(snap.message)
            if m is not None:
                out.append((int(m.group(1)), m.group(2)))
                continue
            b = _BATCH.search(snap.message)
            assert b is not None, snap.message
            out.append((None, b.group(2)))
    return out


async def test_whale_overload(lake_home, topic_factory, producer_factory, admin):
    table = lake_home.create_events_table()
    topic = topic_factory(partitions=4)
    producer = producer_factory(topic.name)

    cfg = livekit.live_config(
        topic=topic.name,
        group=topic.group,
        stage_base_path=f"stage/{livekit.RUN_ID}/{lake_home.slug}",
        catalog=lake_home.catalog.name,
        namespace="ns1",
        table=table.name,
        flush_bytes=FLUSH_BYTES,
        flush_deadline_s=FLUSH_DEADLINE_S,
    )
    async with livekit.Pipeline(cfg, sweep_s=0.5):
        base = now_us()
        seq = 0
        # The tail first: 3 events per tenant, all inside the deadline.
        tail_produced_at = time.monotonic()
        for team in TAIL_TEAMS:
            for i in range(TAIL_EVENTS_PER_TEAM):
                await asyncio.to_thread(producer.send, team, base + i, seq)
                seq += 1
        # The whale: ~1000× the median tail rate, padded to size.
        whale_assignments = [(WHALE, base + 1000 + i) for i in range(WHALE_EVENTS)]
        await asyncio.to_thread(
            livekit.produce_batch,
            producer,
            whale_assignments,
            seq_start=seq,
            pad_bytes=PAD_BYTES,
        )

        # The tail lands inside deadline + margin, whale storm in flight.
        def tail_visible() -> bool:
            counts = livekit.lake_team_counts(table, lake_home.head())
            return all(counts.get(t, 0) >= TAIL_EVENTS_PER_TEAM for t in TAIL_TEAMS)

        await livekit.wait_until_async(
            tail_visible, FLUSH_DEADLINE_S + TAIL_MARGIN_S, desc="the tail flushed"
        )
        tail_latency_s = time.monotonic() - tail_produced_at
        assert tail_latency_s <= FLUSH_DEADLINE_S + TAIL_MARGIN_S

        # Then the whole whale.
        await livekit.wait_until_async(
            lambda: (
                livekit.lake_team_counts(table, lake_home.head()).get(WHALE, 0)
                >= WHALE_EVENTS
            ),
            240,
            desc=f"all {WHALE_EVENTS} whale events queryable",
        )

    head = lake_home.head()

    # At least once, whale and tail alike.
    seq_counts = await asyncio.to_thread(
        livekit.lake_seq_counts, table, head, lake_home.fs
    )
    livekit.assert_at_least_once(seq_counts, [e.seq for e in producer.produced])

    # The trigger audit: whale commits are size-triggered, then drained
    # as backlog (a capped scan marks the rest due on the next sweep);
    # tail commits are age-triggered.
    triggers = _commit_triggers(lake_home.catalog)
    whale_triggers = Counter(t for team, t in triggers if team == WHALE)
    # The tail tenants are due on the age lane together: they flush in
    # batched commits (or alone, if only one was due in a sweep).
    tail_triggers = [t for team, t in triggers if team is None or team in TAIL_TEAMS]
    assert whale_triggers["size"] >= 1, f"whale triggers: {whale_triggers}"
    assert whale_triggers["size"] + whale_triggers["backlog"] >= 2, (
        f"whale triggers: {whale_triggers}"
    )
    assert tail_triggers and set(tail_triggers) == {"age"}, (
        f"tail triggers: {Counter(tail_triggers)}"
    )

    # File sizing: a size-triggered whale commit lands ONE file holding
    # about a scan budget's worth of rows (200 KiB of ~1.06 KB payloads
    # ≈ 190 rows). The whale's age remnant — the
    # rows left under the threshold when production stopped, at most one
    # file — is the only small whale file; tail files stay at their
    # handful of rows.
    whale_rows = []
    tail_rows = []
    for f in livekit.lake_files(table, head):
        assert f.partition_values is not None
        if int(f.partition_values[0]) == WHALE:
            whale_rows.append(f.record_count)
        else:
            tail_rows.append(f.record_count)
    big_whale_files = [r for r in whale_rows if r >= 150]
    assert len(whale_rows) - len(big_whale_files) <= 1, (
        f"more than the remnant came out small: {sorted(whale_rows)}"
    )
    assert len(big_whale_files) >= whale_triggers["size"], (
        f"whale size commits {whale_triggers['size']} vs big files "
        f"{sorted(big_whale_files)} (all whale files: {sorted(whale_rows)})"
    )
    assert max(tail_rows) <= TAIL_EVENTS_PER_TEAM * 2, (
        f"tail file row counts: {sorted(tail_rows)}"
    )
    assert min(big_whale_files) >= 50 * statistics.median(tail_rows)

    print(
        f"\n[whale] files={len(whale_rows)} rows/file "
        f"min={min(whale_rows)} max={max(whale_rows)}; "
        f"tail files={len(tail_rows)} median rows/file={statistics.median(tail_rows)}; "
        f"tail worst produce->queryable {tail_latency_s:.1f}s "
        f"(deadline {FLUSH_DEADLINE_S}s)"
    )
