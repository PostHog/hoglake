"""Scenario 5 (Phase 5): junk ``event_time`` is dropped per policy.

Events whose PAYLOAD ``timestamp`` lies outside the configured window
(here: 2027+, more than ``event_time_max_future_s`` ahead of now) are
dropped at flush time — the staged row is never mutated, the drop is
counted under ``event_time_out_of_window`` and the row is deleted by its
window's settlement, and no future-dated partition value is ever
registered with the server. Good
rows interleaved with the junk (same teams, same partitions) flush
normally. The Kafka RECORD timestamp stays "now" throughout — the
broker itself rejects records dated too far out, and the policy under
test is the flush's, not the broker's.

One team is pure junk: its decision settles WITHOUT a commit (the
no-file case of the same rule).
"""

from __future__ import annotations

import asyncio
import time
from collections import Counter

import livekit
import pytest
from livekit import now_us

from millrace.coerce import load_json_object

pytestmark = pytest.mark.live

# 2027-06-01T00:00:00Z in micros — 235 days out, far past now+1d.
JUNK_TS_US = 1_814_803_200_000_000


async def test_junk_event_time_dropped(
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
        # Everything consumed and committed (junk included — it staged
        # like any row; only the flush drops it).
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

        # The junk is counted and settled out of staging — including the
        # pure-junk team's commit-less settlement, which no lake-side
        # condition can see. Wait for the counter and an empty stage.
        assert pipe.flusher is not None
        flusher = pipe.flusher

        def dropped() -> int:
            return flusher.stats().records_dropped.get("event_time_out_of_window", 0)

        async def staged_rows() -> int:
            total = 0
            for p in range(topic.partitions):
                stats = await pipe.stages.stage(topic.name, p).iter_key_stats()
                total += sum(s.row_count for s in stats)
            return total

        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if dropped() == 15 and await staged_rows() == 0:
                break
            await asyncio.sleep(0.25)
        else:
            raise AssertionError(
                f"timed out waiting for 15 dropped rows and a drained stage; "
                f"got {dropped()} dropped, {await staged_rows()} staged"
            )

        # The junk tuples themselves hold no rows on any partition: the
        # dropped rows were settled out with their windows.
        keyer = pipe.consumer.keyer
        junk_groups = {
            keyer.group_for(
                load_json_object(
                    livekit.event_payload(
                        junk_teams[i % len(junk_teams)], JUNK_TS_US + i, 60 + i
                    )
                )
            )
            for i in range(15)
        }
        assert len(junk_groups) == len(junk_teams)
        for p in range(topic.partitions):
            stage = pipe.stages.stage(topic.name, p)
            for group in junk_groups:
                rows = [rk async for rk, _ in stage.scan_group_rows(group)]
                assert rows == [], f"junk group {group} still staged on partition {p}"

        # -- the assertions ------------------------------------------------
        counts = livekit.lake_team_counts(table, lake_home.head())
        assert counts == Counter({11: 20, 22: 20, 33: 20})
        assert 77 not in counts, "the pure-junk team committed rows"

        # No future-dated partition value was ever registered.
        months = livekit.lake_month_partitions(table, lake_home.head())
        assert months == {livekit.expected_month(base)}, (
            f"future-dated partitions registered: {months}"
        )

        # Every drop is the junk window's, and nothing else dropped.
        assert flusher.stats().records_dropped == {"event_time_out_of_window": 15}
