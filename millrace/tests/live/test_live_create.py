"""Destination creation from configuration (destination.py).

The catalog, namespace and table do NOT pre-exist: the real
``millrace.main`` subprocess creates all three from its environment
(``MILLRACE_CATALOG_DATA_PATH`` on the LAKE bucket — a different bucket
from the staging root, as config.py requires — ``MILLRACE_TABLE_COLUMNS``,
the recommended dev shape ``MILLRACE_PARTITION_BY=identity(team_id),day(timestamp)``
and ``MILLRACE_SORT_BY=timestamp``), then consumes and flushes waves
that land at least once per tenant into files under the created data
path. The live table carries the configured spec and sort order,
installed in one atomic creation.

Teardown drops the table and the namespace. The CATALOG leaks: the API
has no catalog delete (tests/live/conftest.py, AGENT.md's known gap);
it dies with the stack.
"""

from __future__ import annotations

import asyncio
from typing import Any

import livekit
import pytest
from livekit import now_us
from pyhoglake import NotFoundError

pytestmark = pytest.mark.live

TABLE_COLUMNS = "team_id:long,timestamp:timestamptz,seq:long,event:string,value:double"


def _slug(request: pytest.FixtureRequest) -> str:
    return "".join(
        c if c.isalnum() or c in "-_" else "-"
        for c in request.node.name.removeprefix("test_")[:40]
    )


def _creation_env(data_path: str) -> dict[str, str]:
    return {
        "MILLRACE_CATALOG_DATA_PATH": data_path,
        "MILLRACE_TABLE_COLUMNS": TABLE_COLUMNS,
        "MILLRACE_PARTITION_BY": "identity(team_id),day(timestamp)",
        "MILLRACE_SORT_BY": "timestamp",
    }


def _assert_created_shape(client: Any, catalog_name: str, data_path: str) -> Any:
    """The created table, after asserting its columns, spec and sort."""
    catalog = client.catalog(catalog_name)
    assert catalog.data_path == data_path
    table = catalog.namespace("ns1").table("events")
    info = table.info()
    fid = {c.name: c.field_id for c in info.columns}
    assert {c.name: (c.type, c.nullable) for c in info.columns} == {
        "team_id": ("long", True),
        "timestamp": ("timestamptz", True),
        "seq": ("long", True),
        "event": ("string", True),
        "value": ("double", True),
    }
    assert info.partition_spec is not None
    assert [(f.source_field_id, f.transform) for f in info.partition_spec.fields] == [
        (fid["team_id"], "identity"),
        (fid["timestamp"], "day"),
    ]
    assert info.sort_spec is not None
    assert [
        (f.source_field_id, f.direction, f.null_order) for f in info.sort_spec.fields
    ] == [(fid["timestamp"], "asc", "nulls_last")]
    return table


def _drop(client: Any, catalog_name: str, table: Any) -> None:
    """Best-effort teardown: the table, then the (now empty) namespace.
    No catalog delete exists; the catalog leaks."""
    try:
        if table is not None:
            table.drop(author="millrace-tests", message="drop test fixture")
        client.catalog(catalog_name).namespace("ns1").drop(
            author="millrace-tests", message="drop test fixture"
        )
    except Exception as e:  # noqa: BLE001 - teardown is best-effort
        print(f"teardown: dropping {catalog_name}/ns1 failed: {e}")


def _stop(child: Any) -> None:
    if child.proc is not None and child.proc.poll() is None:
        child.terminate()
        child.wait_exit(timeout_s=60)


async def test_pipeline_creates_its_destination_from_config(
    client, topic_factory, producer_factory, request
):
    slug = _slug(request)
    catalog_name = f"mr-{livekit.RUN_ID}-{slug}".lower()[:60]
    data_path = f"s3://{livekit.LAKE_BUCKET}/{livekit.RUN_ID}/lake/{slug}/"
    with pytest.raises(NotFoundError):
        client.catalog(catalog_name)

    topic = topic_factory(partitions=2)
    producer = producer_factory(topic.name)
    cfg = livekit.live_config(
        topic=topic.name,
        group=topic.group,
        stage_base_path=f"stage/{livekit.RUN_ID}/{slug}",
        catalog=catalog_name,
        namespace="ns1",
        table="events",
        flush_deadline_s=2,
        slow_lane_deadline_s=2,
    )
    port = livekit.free_port()
    child = livekit.Child(
        livekit.child_env(cfg, metrics_port=port, extra=_creation_env(data_path)),
        name="create",
    )
    child.start()
    table = None
    try:
        child.wait_ready(port, timeout_s=120)
        out = child.output()
        assert f"millrace created catalog {catalog_name}" in out, out[-3000:]
        assert f"millrace created namespace {catalog_name}.ns1" in out
        assert f"millrace created table {catalog_name}/ns1/events" in out
        assert "differs from the configured shape" not in out, out[-3000:]

        table = await asyncio.to_thread(
            _assert_created_shape, client, catalog_name, data_path
        )
        catalog = client.catalog(catalog_name)

        base = now_us()
        rows = [(7, base + i) for i in range(60)] + [(9, base + i) for i in range(40)]
        events = await asyncio.to_thread(livekit.produce_batch, producer, rows)
        seqs = [e.seq for e in events]
        fs = livekit.s3_config().filesystem()
        await livekit.wait_until_async(
            lambda: livekit.all_landed(table, catalog, fs, seqs),
            120,
            desc="the wave queryable in the created table",
        )
        livekit.assert_at_least_once(
            livekit.lake_seq_counts(table, livekit.head_snapshot(catalog), fs), seqs
        )
        # Every data file of the head snapshot lives under the created
        # catalog's data path — the lake bucket, never the staging one.
        files = livekit.lake_files(table, livekit.head_snapshot(catalog))
        assert files
        for f in files:
            assert f.path.startswith(data_path), f.path
    finally:
        _stop(child)
        _drop(client, catalog_name, table)


async def test_two_replicas_race_to_create_exactly_one_table(
    client, topic_factory, producer_factory, admin, request
):
    """Two ``millrace.main`` children (replica_count=2, ordinals 0/1)
    boot together against a destination that does not exist, with the
    same creation env. Exactly ONE of them creates the table (the other
    re-reads the winner's — never two creates), neither warns about a
    disagreement, 400 rows land, and each replica claims (and so
    commits offsets for) only its own partition."""
    slug = _slug(request)
    catalog_name = f"mr-{livekit.RUN_ID}-{slug}".lower()[:60]
    data_path = f"s3://{livekit.LAKE_BUCKET}/{livekit.RUN_ID}/lake/{slug}/"
    with pytest.raises(NotFoundError):
        client.catalog(catalog_name)

    topic = topic_factory(partitions=2)
    producer = producer_factory(topic.name)
    children = []
    ports = []
    for ordinal in (0, 1):
        cfg = livekit.live_config(
            topic=topic.name,
            group=topic.group,
            stage_base_path=f"stage/{livekit.RUN_ID}/{slug}",
            catalog=catalog_name,
            namespace="ns1",
            table="events",
            replica_count=2,
            ordinal=ordinal,
            flush_deadline_s=2,
            slow_lane_deadline_s=2,
        )
        port = livekit.free_port()
        ports.append(port)
        children.append(
            livekit.Child(
                livekit.child_env(
                    cfg, metrics_port=port, extra=_creation_env(data_path)
                ),
                name=f"race-{ordinal}",
            )
        )
    for child in children:
        child.start()
    table = None
    try:
        for child, port in zip(children, ports, strict=True):
            child.wait_ready(port, timeout_s=120)
        outputs = [child.output() for child in children]
        created = sum(
            out.count(f"millrace created table {catalog_name}/ns1/events")
            for out in outputs
        )
        assert created == 1, "\n---\n".join(o[-2000:] for o in outputs)
        # The loser's path is visible too: it either lost the create race
        # or found the table already there and agreeing. A replica that
        # logs neither took a path this test does not know about.
        loser = next(
            o for o in outputs if f"millrace created table {catalog_name}" not in o
        )
        assert (
            "was created by another replica" in loser
            or "exists and agrees with the configured shape" in loser
        ), loser[-3000:]
        for out in outputs:
            # The destination module's warnings only: the boot warning
            # about embedded GC being off is expected on every child.
            assert "WARNING millrace.destination" not in out, out[-3000:]
            assert "differs from the configured shape" not in out

        table = await asyncio.to_thread(
            _assert_created_shape, client, catalog_name, data_path
        )
        catalog = client.catalog(catalog_name)

        base = now_us()
        rows = [(team, base + i) for team in (1, 2, 3, 4) for i in range(100)]
        events = await asyncio.to_thread(livekit.produce_batch, producer, rows)
        seqs = [e.seq for e in events]
        fs = livekit.s3_config().filesystem()
        await livekit.wait_until_async(
            lambda: livekit.all_landed(table, catalog, fs, seqs),
            180,
            desc="400 rows queryable in the created table",
        )
        livekit.assert_at_least_once(
            livekit.lake_seq_counts(table, livekit.head_snapshot(catalog), fs), seqs
        )

        # Each replica claimed only its own partition (ordinal == p % 2)
        # and the group's offsets reached the end on both.
        for ordinal, child in enumerate(children):
            out = child.output()
            assert f"assigned {topic.name}: [{ordinal}]" in out, out[-3000:]
            assert f"assigned {topic.name}: [{1 - ordinal}]" not in out
        ends = await asyncio.to_thread(
            livekit.end_offsets, admin, topic.name, range(topic.partitions)
        )
        committed = await asyncio.to_thread(
            livekit.committed_offsets, admin, topic.group, topic.name
        )
        assert livekit.group_offsets_reached(committed, ends), (committed, ends)
    finally:
        for child in children:
            _stop(child)
        _drop(client, catalog_name, table)
