"""Scenario 6 (Phase 5): whale overload.

One tenant at ~1000× the median rate crosses the size trigger
repeatedly (full-size flushes, footer stats); the tail tenants in the
SAME run still flush inside the age deadline. Evidence:

- the commit log (``catalog.snapshots()`` messages — the deterministic
  commit text is the audit trail) shows ``trigger=size`` commits for the
  whale and ``trigger=age`` commits for tail teams;
- whale files carry ~a target's worth of rows each; tail files stay tiny;
- every row lands exactly once, whale and tail alike;
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
# The size lane sizes the estimated parquet OUTPUT: the trigger is
# `staged_bytes x estimated_ratio >= target_output_bytes`, and the ratio
# is OBSERVED (the flusher's EWMA replaces the 0.2 fallback with the
# first completed flush — docs/kafka-ingestion.md §The flush planner).
# The pads here are INCOMPRESSIBLE by design (livekit's pad fill), so the
# observed ratio sits at ~1 from the first observation on: the trigger
# then fires at ~a target's worth of staged bytes (~190 rows) for the
# whole run — the arithmetic holds under the observed ratio, not just
# the fallback. (A compressible pad taught the EWMA a tiny ratio after
# the first flush and the size threshold moved mid-scenario: the whale
# then staged past it only once, and the remnant rode the age lane.)
TARGET_OUTPUT_BYTES = 200 * 1024
FLUSH_DEADLINE_S = 12
TAIL_MARGIN_S = 25.0

_COMMIT = re.compile(r"team=(\d+) trigger=(\w+)")


def _commit_triggers(catalog) -> list[tuple[int, str]]:
    """(team_id, trigger) for every millrace commit in the catalog."""
    out = []
    for snap in catalog.snapshots():
        if snap.message and snap.message.startswith("millrace=v1"):
            m = _COMMIT.search(snap.message)
            assert m is not None, snap.message
            out.append((int(m.group(1)), m.group(2)))
    return out


async def test_whale_overload(lake_home, topic_factory, producer_factory, admin):
    table = lake_home.create_events_table()
    topic = topic_factory(partitions=4)
    producer = producer_factory(topic.name)

    cfg = livekit.live_config(
        topic=topic.name,
        group=topic.group,
        partitions=tuple(range(topic.partitions)),
        stage_base_path=f"stage/{livekit.RUN_ID}/{lake_home.slug}",
        catalog=lake_home.catalog.name,
        namespace="ns1",
        table=table.name,
        target_output_bytes=TARGET_OUTPUT_BYTES,
        flush_deadline_s=FLUSH_DEADLINE_S,
        flush_sweep_s=1,
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
            return all(counts.get(t, 0) == TAIL_EVENTS_PER_TEAM for t in TAIL_TEAMS)

        await livekit.wait_until_async(
            tail_visible, FLUSH_DEADLINE_S + TAIL_MARGIN_S, desc="the tail flushed"
        )
        tail_latency_s = time.monotonic() - tail_produced_at
        assert tail_latency_s <= FLUSH_DEADLINE_S + TAIL_MARGIN_S

        # Then the whole whale.
        await livekit.wait_until_async(
            lambda: (
                livekit.lake_team_counts(table, lake_home.head()).get(WHALE, 0)
                == WHALE_EVENTS
            ),
            240,
            desc=f"all {WHALE_EVENTS} whale events queryable",
        )

    head = lake_home.head()

    # Exactness, whale and tail alike.
    counts = livekit.lake_team_counts(table, head)
    expected = Counter({WHALE: WHALE_EVENTS})
    expected.update({t: TAIL_EVENTS_PER_TEAM for t in TAIL_TEAMS})
    assert counts == expected

    # The trigger audit: whale commits are size-triggered, tail commits
    # age-triggered. (The whale's trailing remnant — the rows left under
    # the target when production stops — flushes by AGE by design; the
    # planner's precedence is per key per pass.)
    triggers = _commit_triggers(lake_home.catalog)
    whale_triggers = Counter(t for team, t in triggers if team == WHALE)
    tail_triggers = [t for team, t in triggers if team in TAIL_TEAMS]
    assert whale_triggers["size"] >= 2, f"whale triggers: {whale_triggers}"
    assert tail_triggers and set(tail_triggers) == {"age"}, (
        f"tail triggers: {Counter(tail_triggers)}"
    )

    # File sizing: a size-triggered whale commit lands ONE file holding
    # at least a trigger's worth of rows — the trigger fires at
    # `staged >= target ÷ ratio` and the observed ratio of these
    # incompressible pads is clamped at <= 1, so >= 200 KiB of ~1.06 KB
    # payloads ≈ 190 rows, every time. The whale's age remnant — the
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
