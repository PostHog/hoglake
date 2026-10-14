"""Pathological event times are WRITTEN, never dropped; records that
cannot be represented at all are dropped and counted.

Events whose PAYLOAD ``timestamp`` is ancient (1971) or far in the future
(2027+) land in the lake under their own month partitions, beside the
good rows of the same teams. The Kafka RECORD timestamp stays "now"
(the broker rejects records dated too far out). A value that is not a
JSON object is dropped at consume time and counted
(``malformed_value``); its offset is still committed.
"""

from __future__ import annotations

import asyncio

import livekit
import pytest
from livekit import now_us

pytestmark = pytest.mark.live

# 2027-06-01T00:00:00Z and 1971-01-01T00:00:00Z in micros.
FUTURE_TS_US = 1_814_803_200_000_000
ANCIENT_TS_US = 31_536_000_000_000
MALFORMED = 5


async def test_pathological_event_times_are_written(
    lake_home, topic_factory, producer_factory, admin
):
    table = lake_home.create_events_table()
    topic = topic_factory(partitions=2)
    producer = producer_factory(topic.name)

    teams = [11, 22, 33]
    base = now_us()
    seq = 0
    produced: list[int] = []

    def send(team: int, ts_us: int) -> None:
        nonlocal seq
        producer.send(team, ts_us, seq, record_ts_us=base + seq)
        produced.append(seq)
        seq += 1

    for i in range(60):
        send(teams[i % len(teams)], base + i)
    for i in range(15):
        send(teams[i % len(teams)], FUTURE_TS_US + i)
    for i in range(15):
        send(teams[i % len(teams)], ANCIENT_TS_US + i)
    for i in range(MALFORMED):
        producer.send(77, base, 1000 + i, value=b"not json {")
    await asyncio.to_thread(producer.flush)

    cfg = livekit.live_config(
        topic=topic.name,
        group=topic.group,
        stage_base_path=f"stage/{livekit.RUN_ID}/{lake_home.slug}",
        catalog=lake_home.catalog.name,
        namespace="ns1",
        table=table.name,
        flush_deadline_s=3,
    )
    async with livekit.Pipeline(cfg, sweep_s=0.5) as pipe:
        await livekit.wait_until_async(
            lambda: livekit.all_landed(
                table, lake_home.catalog, lake_home.fs, produced
            ),
            90,
            desc="every good, ancient and future event queryable",
        )
        ends = await asyncio.to_thread(
            livekit.end_offsets, admin, topic.name, range(topic.partitions)
        )
        assert sum(ends.values()) == 90 + MALFORMED
        assert pipe.consumer is not None
        await livekit.wait_until_async(
            lambda: livekit.offsets_reached(
                pipe.consumer.committed_offsets(), topic.name, ends
            ),
            60,
            desc="committed offsets covering the malformed records",
        )
        assert pipe.consumer.stats().records_dropped == {"malformed_value": MALFORMED}
        assert pipe.flusher is not None
        assert pipe.flusher.stats().records_dropped == {}

    head = lake_home.head()
    counts = await asyncio.to_thread(livekit.lake_seq_counts, table, head, lake_home.fs)
    livekit.assert_at_least_once(counts, produced)
    months = livekit.lake_month_partitions(table, head)
    assert {
        livekit.expected_month(base),
        livekit.expected_month(FUTURE_TS_US),
        livekit.expected_month(ANCIENT_TS_US),
    } <= months, months
