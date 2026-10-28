"""A VARIANT companion column end to end against the real server.

``properties`` (a string) and ``properties_sv`` (a nullable variant with
the all-string declaration of ``ingest.events``) are added to the table
by an operator alter, as in production. With
``MILLRACE_VARIANT_SOURCES=properties_sv=properties`` every event lands,
each ``properties_sv`` reads back as ``json.loads(properties)`` (SQL NULL
for an absent or invalid one, a Variant null for the text ``null``), the
files carry ``VARIANT(1)`` in the declared layout, the server accepts
millrace's footer stats with no row for the variant, and server
compaction (``HOGLAKE_COMPACTION_VARIANT``, set by ci/live-millrace.sh)
rolls the files up with every value unchanged. Unmapped, the column is
written as SQL NULL and the pipeline is not stuck.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
from typing import Any

import livekit
import pyarrow.parquet as pq
import pytest
from livekit import now_us
from pyhoglake import ops
from pyhoglake.variant import VARIANT_NULL, to_python

import millrace.flush as flush_mod

pytestmark = pytest.mark.live

TEAM = 9
WAVES = 5  # the server's default minimum compaction group
FLUSH_DEADLINE_S = 2

#: properties_sv's declaration on ingest.events: every field a string.
DECLARATION: dict[str, Any] = {
    "type": "object",
    "fields": [
        {"name": name, "type": "string"}
        for name in (
            "$session_id",
            "$window_id",
            *(f"$group_{i}" for i in range(5)),
            "$current_url",
            "$pathname",
            "$host",
            "$browser",
            "$os",
            "$device_type",
            "$lib",
            "$referring_domain",
            "$device_id",
        )
    ],
}

_VALID = {"$browser": "Chrome", "$session_id": "s-1", "$current_url": "https://x"}
#: (properties as produced, properties_sv as read back).
CASES: list[tuple[str | None, Any]] = [
    (json.dumps(_VALID), _VALID),
    (json.dumps({"$os": "Mac", "free": [1, None]}), {"$os": "Mac", "free": [1, None]}),
    (json.dumps({"$browser": 5}), {"$browser": 5}),  # a type miss: kept as value
    ("null", VARIANT_NULL),
    (None, None),  # absent properties
    ('{"$browser":"a","$browser":"b"}', None),  # duplicate key: invalid
    ('{"$browser":"\\ud800"}', None),  # lone surrogate escape: invalid
]


def _variant_table(lake_home: Any, name: str) -> tuple[Any, Any]:
    """The events table plus the operator's alter; returns it with the
    live ``properties_sv`` column."""
    table = lake_home.create_events_table(name)
    info = table.alter(
        [
            ops.add_column("properties", "string"),
            ops.add_column("properties_sv", "variant", shredding=DECLARATION),
        ]
    )
    (column,) = [c for c in info.columns if c.name == "properties_sv"]
    return table, column


def _produce(producer: Any, seqs: range) -> None:
    base = now_us()
    for seq in seqs:
        text = CASES[seq % len(CASES)][0]
        producer.send(
            TEAM,
            base + seq,
            seq,
            extra=None if text is None else {"properties": text},
        )
    producer.flush()


def _read(lake_home: Any, path: str) -> pq.ParquetFile:
    return pq.ParquetFile(lake_home.fs.open_input_file(path[len("s3://") :]))


def _values(lake_home: Any, table: Any, column: Any) -> dict[int, list[Any]]:
    """Every live row's properties_sv, by seq (a list: duplicates allowed)."""
    out: dict[int, list[Any]] = {}
    for f in livekit.lake_files(table, lake_home.head()):
        data = _read(lake_home, f.path).read()
        values = to_python(data.column("properties_sv"), column)
        for seq, value in zip(data.column("seq").to_pylist(), values, strict=True):
            out.setdefault(seq, []).append(value)
    return out


def _assert_values(values: dict[int, list[Any]], seqs: range) -> None:
    assert set(values) == set(seqs)
    for seq, got in values.items():
        want = CASES[seq % len(CASES)][1]
        assert all(v == want for v in got), (seq, got, want)


async def test_a_mapped_variant_lands_every_event_and_compacts(
    lake_home, topic_factory, producer_factory, monkeypatch
):
    # Every file registers millrace's own footer stats, so the server's
    # answer shows what millrace sent (and compaction needs no hydrator).
    monkeypatch.setattr(flush_mod, "FOOTER_STATS_MIN_BYTES", 1)
    table, column = _variant_table(lake_home, "events_sv")
    topic = topic_factory(partitions=2)
    producer = producer_factory(topic.name)
    cfg = dataclasses.replace(
        livekit.live_config(
            topic=topic.name,
            group=topic.group,
            stage_base_path=f"stage/{livekit.RUN_ID}/{lake_home.slug}",
            catalog=lake_home.catalog.name,
            namespace="ns1",
            table=table.name,
            flush_deadline_s=FLUSH_DEADLINE_S,
            flush_sweep_s=1,
        ),
        variant_sources=(("properties_sv", "properties"),),
    )
    async with livekit.Pipeline(cfg, sweep_s=0.5) as pipe:
        assert pipe.runner is not None and pipe.flusher is not None
        # One wave per flush: WAVES small files in one (team, month) bucket.
        for wave in range(WAVES):
            seqs = range(wave * len(CASES), (wave + 1) * len(CASES))
            await asyncio.to_thread(_produce, producer, seqs)
            await livekit.wait_until_async(
                lambda w=wave: (
                    pipe.runner.halted is not None
                    or len(livekit.lake_files(table, lake_home.head())) == w + 1
                ),
                60,
                desc=lambda w=wave: f"wave {w} flushed (halt: {pipe.runner.halted})",
            )
        assert pipe.runner.halted is None
        await livekit.wait_until_async(
            lambda: pipe.stages.gauges().staged_groups == 0,
            30,
            desc=lambda: f"staging residue: {pipe.stages.gauges()}",
        )
        stats = pipe.flusher.stats()
        assert stats.records_dropped == {}
        assert pipe.runner.stuck_groups == 0
        assert {r for _, r in stats.variant_invalid} == {
            "duplicate_key",
            "invalid_unicode",
        }

    produced = range(WAVES * len(CASES))
    _assert_values(await asyncio.to_thread(_values, lake_home, table, column), produced)
    files = livekit.lake_files(table, lake_home.head())
    assert len(files) == WAVES
    for f in files:
        schema = str(_read(lake_home, f.path).schema)
        assert "properties_sv (Variant(1))" in schema
        assert "$referring_domain" in schema  # the declared layout
        stats = lake_home.client._request(
            "GET",
            f"/catalogs/{lake_home.catalog.name}/namespaces/ns1/tables/"
            f"{table.name}/files/{f.data_file_id}/stats",
        )
        assert stats["stats_state"] == "provided", stats
        paths = {c["path"] for c in stats["columns"]}
        assert "team_id" in paths and "properties" in paths
        assert not any(p.startswith("properties_sv") for p in paths), paths

    result = await asyncio.to_thread(
        lake_home.client._request,
        "POST",
        f"/catalogs/{lake_home.catalog.name}/maintenance/compact",
    )
    assert result["groups_compacted"] == 1, result
    assert (result["files_in"], result["files_out"]) == (WAVES, 1), result
    assert result["failed_groups"] == 0, result
    (compacted,) = livekit.lake_files(table, lake_home.head())
    assert "properties_sv (Variant(1))" in str(_read(lake_home, compacted.path).schema)
    _assert_values(await asyncio.to_thread(_values, lake_home, table, column), produced)


async def test_an_unmapped_variant_is_written_as_sql_null(
    lake_home, topic_factory, producer_factory
):
    table, column = _variant_table(lake_home, "events_unmapped")
    topic = topic_factory(partitions=2)
    producer = producer_factory(topic.name)
    cfg = livekit.live_config(
        topic=topic.name,
        group=topic.group,
        stage_base_path=f"stage/{livekit.RUN_ID}/{lake_home.slug}",
        catalog=lake_home.catalog.name,
        namespace="ns1",
        table=table.name,
        flush_deadline_s=FLUSH_DEADLINE_S,
    )
    assert cfg.variant_sources == ()
    seqs = range(len(CASES))
    async with livekit.Pipeline(cfg, sweep_s=0.5) as pipe:
        assert pipe.runner is not None and pipe.flusher is not None
        await asyncio.to_thread(_produce, producer, seqs)
        await livekit.wait_until_async(
            lambda: (
                pipe.runner.halted is not None
                or sum(livekit.lake_team_counts(table, lake_home.head()).values())
                >= len(CASES)
            ),
            FLUSH_DEADLINE_S + 60,
            desc=lambda: f"every event landed (halt: {pipe.runner.halted})",
        )
        assert pipe.runner.halted is None
        assert pipe.runner.stuck_groups == 0
        assert pipe.flusher.stats().records_dropped == {}
        assert pipe.flusher.stats().variant_rows == {}

    values = await asyncio.to_thread(_values, lake_home, table, column)
    assert set(values) == set(seqs)
    assert all(v is None for got in values.values() for v in got), values
    # The text column beside it is filled as before.
    rows = await asyncio.to_thread(
        livekit.lake_rows, table, lake_home.head(), lake_home.fs
    )
    by_seq = dict(
        zip(rows.column("seq").to_pylist(), rows.column("properties").to_pylist())
    )
    assert by_seq == {seq: CASES[seq][0] for seq in seqs}
