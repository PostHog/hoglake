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
    from slatedb.uniffi import ObjectStore

    from millrace.config import (
        SLATEDB_BLOCK_CACHE_BYTES_DEFAULT,
        SLATEDB_META_CACHE_BYTES_DEFAULT,
    )
    from millrace.stage import (
        PartitionStage,
        StagedRecord,
        build_slatedb_cache,
        db_cache_id_for,
    )

    # The production cache shape: one pod-level cache, reused across
    # the close/reopen exactly as a StageManager would.
    cache = build_slatedb_cache(
        block_cache_bytes=SLATEDB_BLOCK_CACHE_BYTES_DEFAULT,
        meta_cache_bytes=SLATEDB_META_CACHE_BYTES_DEFAULT,
    )
    topic = topic_factory(partitions=1)
    path = f"stage/{livekit.RUN_ID}/{topic.name}-smoke/{topic.name}/0"
    base = now_us()
    records = [
        StagedRecord(
            group=("7",),
            offset=i,
            payload=livekit.event_payload(7, base + i, i),
        )
        for i in range(50)
    ]
    store = ObjectStore.resolve(f"s3://{livekit.BUCKET}")
    stage = await PartitionStage.open_store(
        store,
        path,
        topic=topic.name,
        partition=0,
        settings=livekit.live_slate_settings(),
        db_cache=cache,
        db_cache_id=db_cache_id_for(topic.name, 0),
    )
    ack = await stage.stage_batch(records, now_us=base)
    assert ack.rows == 50 and ack.first_offset == 0 and ack.last_offset == 49
    await stage.close()

    reopened = await PartitionStage.open_store(
        store,
        path,
        topic=topic.name,
        partition=0,
        settings=livekit.live_slate_settings(),
        db_cache=cache,
        db_cache_id=db_cache_id_for(topic.name, 0),
    )
    try:
        # Discovery finds the group a previous process left staged.
        assert await reopened.discover(now_us=base) == [("7",)]
        scan = await reopened.scan(("7",), max_bytes=1 << 30, now_us=base)
        assert scan.complete and len(scan.rows) == 50
        for i, (row_key, payload) in enumerate(scan.rows):
            assert row_key.offset == i
            assert payload == livekit.event_payload(7, base + i, i)
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
