"""SlateDB metrics bridge tests (millrace/slatedb_metrics.py).

Layers: the fold is pure over :class:`MetricSnapshot` (fabricated
snapshots, no SlateDB); the recorder wiring is component-tested against
real ``PartitionStage``\\ s on ``memory:///`` (the recorder the stage
exposes is the one its DB writes into); the collector arm degrades
per-recorder, never failing a scrape.
"""

from __future__ import annotations

from typing import Any

import pytest
from stagekit import NOW, fast_flush_settings, rec

from millrace.slatedb_metrics import (
    MetricSnapshot,
    slatedb_families,
    snapshot_recorder,
)
from millrace.stage import StageManager

component = pytest.mark.component


def _counter(name: str, value: int, labels: tuple[tuple[str, str], ...] = ()):
    return MetricSnapshot(name=name, labels=labels, kind="counter", value=value)


def _gauge(name: str, value: int, labels: tuple[tuple[str, str], ...] = ()):
    return MetricSnapshot(name=name, labels=labels, kind="gauge", value=value)


def _samples(families: Any) -> dict[tuple[str, tuple[tuple[str, str], ...]], float]:
    """{ (family, sorted label pairs): value } over every family."""
    out: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
    for family in families:
        for sample in family.samples:
            out[(sample.name, tuple(sorted(sample.labels.items())))] = sample.value
    return out


class TestPureFold:
    def test_counters_sum_across_instances(self) -> None:
        families = slatedb_families(
            [
                [_counter("slatedb.wal.wal_buffer_flushes", 3)],
                [_counter("slatedb.wal.wal_buffer_flushes", 4)],
            ]
        )
        samples = _samples(families)
        assert samples[("millrace_slatedb_wal_buffer_flushes_total", ())] == 7

    def test_the_depth_gauge_takes_the_worst_instance(self) -> None:
        families = slatedb_families(
            [
                [_gauge("slatedb.db.segment_max_l0_sst_count", 2)],
                [_gauge("slatedb.db.segment_max_l0_sst_count", 5)],
            ]
        )
        samples = _samples(families)
        assert samples[("millrace_slatedb_l0_ssts_per_segment_max", ())] == 5

    def test_labeled_families_fold_per_label_values(self) -> None:
        families = slatedb_families(
            [
                [
                    _counter(
                        "slatedb.db_cache.access_count",
                        6,
                        (("entry_kind", "data_block"), ("result", "hit")),
                    ),
                    _counter(
                        "slatedb.db_cache.access_count",
                        2,
                        (("entry_kind", "data_block"), ("result", "miss")),
                    ),
                ],
                [
                    _counter(
                        "slatedb.db_cache.access_count",
                        4,
                        (("entry_kind", "data_block"), ("result", "hit")),
                    ),
                ],
            ]
        )
        samples = _samples(families)
        assert (
            samples[
                (
                    "millrace_slatedb_block_cache_accesses_total",
                    (("entry_kind", "data_block"), ("result", "hit")),
                )
            ]
            == 10
        )
        assert (
            samples[
                (
                    "millrace_slatedb_block_cache_accesses_total",
                    (("entry_kind", "data_block"), ("result", "miss")),
                )
            ]
            == 2
        )

    def test_unknown_names_and_histograms_are_ignored(self) -> None:
        families = slatedb_families(
            [
                [
                    _counter("slatedb.future.brand_new_metric", 1),
                    MetricSnapshot(
                        name="slatedb.db.l0_sst_count",  # whitelisted name...
                        labels=(),
                        kind="histogram",  # ...but a shape we never bridge
                        value=None,
                    ),
                    MetricSnapshot(
                        name="slatedb.object_store.request_duration_seconds",
                        labels=(("api", "put"),),
                        kind="histogram",
                        value=None,
                    ),
                ]
            ]
        )
        assert families == []

    def test_unobserved_families_are_not_emitted(self) -> None:
        assert slatedb_families([[], []]) == []
        # Only the observed whitelisted names appear.
        families = slatedb_families([[_gauge("slatedb.db.total_mem_size_bytes", 9)]])
        assert [f.name for f in families] == ["millrace_slatedb_memtable_bytes"]


@component
class TestRecorderWiring:
    async def test_the_stages_recorder_is_the_one_its_db_writes_to(
        self, stage_factory: Any
    ) -> None:
        stage = await stage_factory()
        await stage.stage_batch([rec(1, 100, 0)], now_us=NOW)
        names = {m.name for m in snapshot_recorder(stage.metrics_recorder)}
        assert "slatedb.db.write_batch_count" in names
        by_name = {m.name: m for m in snapshot_recorder(stage.metrics_recorder)}
        assert by_name["slatedb.db.write_batch_count"].value == 1
        await stage.close()

    async def test_two_instances_aggregate_into_one_series(
        self, memory_store: Any
    ) -> None:
        manager = StageManager("memory:///", "millrace", settings=fast_flush_settings())
        one = await manager.open_partition("events", 0)
        two = await manager.open_partition("events", 1)
        await one.stage.stage_batch([rec(1, 100, 0)], now_us=NOW)

        def wal_flush_bytes(recorder: Any) -> int:
            return sum(
                m.value or 0
                for m in snapshot_recorder(recorder)
                if m.name == "slatedb.wal.wal_flush_bytes"
            )

        v1 = wal_flush_bytes(one.stage.metrics_recorder)
        assert v1 > 0  # the durability await flushed a WAL buffer
        await two.stage.stage_batch([rec(2, 100, 0)], now_us=NOW)
        await two.stage.stage_batch([rec(2, 101, 1)], now_us=NOW)
        v2 = wal_flush_bytes(two.stage.metrics_recorder)
        assert v2 > 0

        snapshots = [
            snapshot_recorder(recorder)
            for recorder in manager.metrics_recorders().values()
        ]
        samples = _samples(slatedb_families(snapshots))
        # ONE pod-level series, exactly the sum of the two instances —
        # never a per-partition label (cardinality).
        assert samples[("millrace_slatedb_wal_flush_bytes_total", ())] == v1 + v2
        assert not any(
            "partition" in label for name, labels in samples for label in labels
        )
        await manager.close()

    def test_snapshot_recorder_reads_the_real_recorder_shape(self) -> None:
        """The uniffi seam: a hand-driven DefaultMetricsRecorder's
        counters/gauges (with labels) land in MetricSnapshots with the
        kinds this module's whitelist mapping relies on."""
        from slatedb.uniffi import DefaultMetricsRecorder, MetricLabel

        recorder = DefaultMetricsRecorder()
        counter = recorder.register_counter("slatedb.wal.wal_buffer_flushes", "", [])
        gauge = recorder.register_gauge(
            "slatedb.db.l0_stall_count", "", [MetricLabel(key="type", value="num_ssts")]
        )
        counter.increment(3)
        gauge.set(7)
        snapshots = snapshot_recorder(recorder)
        by_name = {m.name: m for m in snapshots}
        assert by_name["slatedb.wal.wal_buffer_flushes"] == MetricSnapshot(
            name="slatedb.wal.wal_buffer_flushes",
            labels=(),
            kind="counter",
            value=3,
        )
        assert by_name["slatedb.db.l0_stall_count"] == MetricSnapshot(
            name="slatedb.db.l0_stall_count",
            labels=(("type", "num_ssts"),),
            kind="gauge",
            value=7,
        )
        families = slatedb_families([snapshots])
        samples = _samples(families)
        assert samples[("millrace_slatedb_wal_buffer_flushes_total", ())] == 3
        assert (
            samples[("millrace_slatedb_l0_stalls_total", (("type", "num_ssts"),))] == 7
        )
