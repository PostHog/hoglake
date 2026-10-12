"""Live smoke: the stack pieces the ten scenarios stand on, each proven
in isolation before the pipeline scenarios lean on them.

- s3:// SlateDB staging against MinIO (Phase 5 is where s3:// staging is
  first exercised — every scenario's stage lives there): stage, close,
  REOPEN, every acknowledged row back byte-exact.
- The broker: produce, and the log ends where the producer says it does.
- The server (built from this checkout): create the events-like table
  with the live partition spec and read it back.
"""

from __future__ import annotations

import livekit
import pytest
from livekit import now_us

pytestmark = pytest.mark.live


async def test_s3_staging_smoke(lake_home, topic_factory):
    """Stage on s3:// (MinIO), close, reopen: durable, byte-exact."""
    from millrace.stage import PartitionStage, StagedRecord

    topic = topic_factory(partitions=1)
    path = f"stage/{livekit.RUN_ID}/{topic.name}-smoke/{topic.name}/0"
    base = now_us()
    records = [
        StagedRecord(
            team_id=7,
            event_ts_us=base + i,
            offset=i,
            payload=livekit.event_payload(7, base + i, i),
        )
        for i in range(50)
    ]
    stage = await PartitionStage.open(
        f"s3://{livekit.BUCKET}",
        path,
        topic=topic.name,
        partition=0,
        settings=livekit.live_slate_settings(),
    )
    ack = await stage.stage_batch(records, now_us=base)
    assert ack.rows == 50 and ack.first_offset == 0 and ack.last_offset == 49
    await stage.close()

    reopened = await PartitionStage.open(
        f"s3://{livekit.BUCKET}",
        path,
        topic=topic.name,
        partition=0,
        settings=livekit.live_slate_settings(),
    )
    try:
        rows = [kv async for kv in reopened.scan_team_rows(7)]
        assert len(rows) == 50
        for i, (row_key, payload) in enumerate(rows):
            assert row_key.offset == i
            assert payload == livekit.event_payload(7, base + i, i)
        stats = await reopened.iter_key_stats()
        assert len(stats) == 1
        assert stats[0].row_count == 50
        recovery = await reopened.recover()
        assert (
            recovery.pending_prepared == () and recovery.legacy_markers_collected == 0
        )
    finally:
        await reopened.close()


def test_kafka_roundtrip_smoke(topic_factory, producer_factory, admin):
    topic = topic_factory(partitions=2)
    producer = producer_factory(topic.name)
    base = now_us()
    livekit.produce_batch(producer, [(1 + i, base + i) for i in range(20)])
    ends = livekit.end_offsets(admin, topic.name, range(2))
    assert sum(ends.values()) == 20
    assert len(producer.produced) == 20


def test_events_table_create_and_spec(lake_home):
    table = lake_home.create_events_table()
    assert [c.name for c in table.columns] == [
        "team_id",
        "timestamp",
        "seq",
        "event",
        "value",
    ]
    spec = table.info().partition_spec
    assert spec is not None
    assert [f.transform for f in spec.fields] == ["identity", "month"]
    assert table.files() == []
