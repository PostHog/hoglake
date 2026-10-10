"""Scenario 5 (Phase 5): junk ``event_time`` is quarantined per policy.

Events whose PAYLOAD ``timestamp`` lies outside the configured window
(here: 2027+, more than ``event_time_max_future_s`` ahead of now) are
quarantined at flush time — the staged row is never mutated, the
original payload lands in the partition's ``poison/`` prefix, and no
future-dated partition value is ever registered with the server. Good
rows interleaved with the junk (same teams, same partitions) flush
normally. The Kafka RECORD timestamp stays "now" throughout — the
broker itself rejects records dated too far out, and the policy under
test is the flush's, not the broker's.

One team is pure junk: its decision settles WITHOUT a commit (the
no-file case of the same rule).
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import Counter

import livekit
import pytest
from livekit import now_us

pytestmark = pytest.mark.live

# 2027-06-01T00:00:00Z in micros — 235 days out, far past now+1d.
JUNK_TS_US = 1_814_803_200_000_000


async def test_junk_event_time_quarantined(
    lake_home, topic_factory, producer_factory, admin
):
    table = lake_home.create_events_table()
    topic = topic_factory(partitions=2)
    producer = producer_factory(topic.name)

    good_teams = [11, 22, 33]
    junk_teams = [11, 22, 77]  # 77 is PURE junk
    base = now_us()
    seq = 0

    def send(team: int, ts_us: int) -> None:
        nonlocal seq
        # The record timestamp stays "now" (see module docstring).
        producer.send(team, ts_us, seq, record_ts_us=base + seq)
        seq += 1

    for i in range(60):
        send(good_teams[i % len(good_teams)], base + i)
    for i in range(15):
        send(junk_teams[i % len(junk_teams)], JUNK_TS_US + i)
    await asyncio.to_thread(producer.flush)

    cfg = livekit.live_config(
        topic=topic.name,
        group=topic.group,
        partitions=tuple(range(topic.partitions)),
        stage_base_path=f"stage/{livekit.RUN_ID}/{lake_home.slug}",
        catalog=lake_home.catalog.name,
        namespace="ns1",
        table=table.name,
        flush_deadline_s=3,
        flush_sweep_s=1,
    )
    async with livekit.Pipeline(cfg, sweep_s=0.5) as pipe:
        # The good rows land; the junk NEVER does.
        await livekit.wait_until_async(
            lambda: (
                sum(livekit.lake_team_counts(table, lake_home.head()).values()) == 60
            ),
            90,
            desc="the 60 good events queryable",
        )
        # Everything consumed and committed (junk included — the offset
        # commit covers the quarantine exactly like a staged row).
        ends = await asyncio.to_thread(
            livekit.end_offsets, admin, topic.name, range(topic.partitions)
        )
        assert sum(ends.values()) == 75
        assert pipe.consumer is not None
        await livekit.wait_until_async(
            lambda: livekit.offsets_reached(
                pipe.consumer.committed_offsets(), topic.name, ends
            ),
            60,
            desc="committed offsets covering the junk",
        )

        # The junk is settled out of staging into the quarantine —
        # including the pure-junk team's commit-less settlement, which no
        # lake-side condition can see. Wait for the quarantine itself.
        async def poisoned() -> int:
            total = 0
            for p in range(topic.partitions):
                total += len(await pipe.stages.stage(topic.name, p).scan_poison())
            return total

        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if await poisoned() == 15:
                break
            await asyncio.sleep(0.25)
        else:
            raise AssertionError(
                f"timed out waiting for 15 quarantined rows; got {await poisoned()}"
            )

        # -- the assertions ------------------------------------------------
        counts = livekit.lake_team_counts(table, lake_home.head())
        assert counts == Counter({11: 20, 22: 20, 33: 20})
        assert 77 not in counts, "the pure-junk team committed rows"

        # No future-dated partition value was ever registered.
        months = livekit.lake_month_partitions(table, lake_home.head())
        assert months == {livekit.expected_month(base)}, (
            f"future-dated partitions registered: {months}"
        )

        # The junk rows survive in the quarantine, byte-exact, with the
        # flush-time reason.
        junk_seqs = set()
        reasons = Counter()
        for p in range(topic.partitions):
            stage = pipe.stages.stage(topic.name, p)
            for entry in await stage.scan_poison():
                reasons[entry.reason] += 1
                if entry.reason == "event_time_out_of_window":
                    assert entry.value is not None
                    junk_seqs.add(json.loads(entry.value)["seq"])
        assert reasons == Counter({"event_time_out_of_window": 15})
        assert junk_seqs == set(range(60, 75))

        # And the flusher counted them as quarantined.
        assert pipe.flusher is not None
        assert pipe.flusher.stats().records_quarantined == 15
