"""SlateDB → Prometheus metrics bridge (docs/kafka-ingestion.md
§Deployment and operations).

Every partition's SlateDB instance writes its internals into a
``DefaultMetricsRecorder`` (wired at open — ``PartitionStage``); this
module snapshots those recorders AT SCRAPE TIME and folds them into
the whitelisted operational families, **aggregated across the pod's
partition instances**: per-partition label series at K instances per
pod is a cardinality trap, so every family here is a pod-level sum (or
max, where the metric is a depth).

The whitelist is introspected from slatedb 0.17.0's actual emission
(verified live against the binding, not recalled):

- L0 shape: ``slatedb.db.l0_sst_count`` (gauge),
  ``slatedb.db.segment_max_l0_sst_count`` (gauge) — L0 file count and
  the worst per-segment L0 depth (the write-stall driver:
  ``l0_max_ssts`` backpressure trips on depth).
- WAL: ``slatedb.wal.wal_buffer_flushes``,
  ``slatedb.wal.wal_buffer_flush_requests``,
  ``slatedb.wal.wal_flush_bytes`` (counters) and
  ``slatedb.wal.wal_buffer_estimated_bytes`` (gauge). NOTE: 0.17.0
  emits NO WAL flush-latency histogram (the design's "ack-latency
  driver" is observable only through the flush rate/bytes here and the
  consumer-visible stage latency; the one histogram SlateDB emits is
  ``slatedb.object_store.request_duration_seconds``, which cannot
  separate WAL puts from SST uploads by label, so it is deliberately
  NOT bridged).
- Write-stall state: ``slatedb.db.backpressure_count`` (counter of
  write-stall events) and ``slatedb.db.l0_stall_count`` (counter with a
  bounded ``type`` label — kept; labels are only ever from SlateDB's
  own fixed vocabulary, never a partition).
- Memtable bytes: ``slatedb.db.total_mem_size_bytes`` (gauge).
- Block cache: ``slatedb.db_cache.access_count`` (counter with
  ``entry_kind`` × ``result`` labels; the hit rate is
  ``result="hit"`` over the sum, PromQL-side).

Anything SlateDB emits that is NOT whitelisted is ignored — a newer
SlateDB adding or renaming metrics can never break a scrape
(forward-compat is the default path, not a special case).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final, Literal

from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily


#: The uniffi ``Metric``'s shape as this module reads it (slatedb ships
#: no type stubs; the protocol pins the slice we rely on and keeps the
#: converter testable with plain fakes).
@dataclass(frozen=True, slots=True)
class MetricSnapshot:
    """One SlateDB metric observation: name, label pairs, kind and the
    scalar value (histograms carry ``None`` — none are whitelisted, see
    the module docstring). Values keep their emitted numeric type."""

    name: str
    labels: tuple[tuple[str, str], ...]
    kind: Literal["counter", "gauge", "histogram"]
    value: int | float | None


def snapshot_recorder(recorder: Any) -> list[MetricSnapshot]:
    """One recorder's ``snapshot()`` as plain :class:`MetricSnapshot`\\ s.

    Tolerant by construction: a metric whose shape the binding changes
    under us (a new value kind, an unparseable label, a non-numeric
    value) is skipped, never a scrape failure. Raises only if the
    recorder itself fails — the caller (the collector) degrades per
    recorder.
    """
    out: list[MetricSnapshot] = []
    for metric in recorder.snapshot():
        value = metric.value
        if value.is_counter() or value.is_gauge() or value.is_up_down_counter():
            kind: Literal["counter", "gauge", "histogram"] = (
                "gauge" if value.is_gauge() else "counter"
            )
            raw = value[0]
            if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                continue  # a shape this binding did not have at pin time
            scalar: int | float | None = raw
        elif value.is_histogram():
            kind = "histogram"
            scalar = None
        else:  # a value kind this binding did not have at pin time
            continue
        out.append(
            MetricSnapshot(
                name=str(metric.name),
                labels=tuple(
                    (str(label.key), str(label.value)) for label in metric.labels
                ),
                kind=kind,
                value=scalar,
            )
        )
    return out


@dataclass(frozen=True, slots=True)
class _Rule:
    """One whitelisted SlateDB metric → its Prometheus family."""

    family: str
    doc: str
    # Labels retained from SlateDB's own (bounded) vocabulary. A label
    # never carries a partition/instance identity (cardinality).
    keep_labels: tuple[str, ...] = ()
    # How per-instance values fold into the pod's single series.
    aggregate: Literal["sum", "max"] = "sum"


_WHITELIST: Final[dict[str, _Rule]] = {
    "slatedb.db.l0_sst_count": _Rule(
        "millrace_slatedb_l0_sst_files",
        "L0 SST files across the pod's staging instances",
    ),
    "slatedb.db.segment_max_l0_sst_count": _Rule(
        "millrace_slatedb_l0_ssts_per_segment_max",
        "Worst per-segment L0 depth across the pod's staging instances "
        "(the l0_max_ssts write-stall driver)",
        aggregate="max",
    ),
    "slatedb.db.total_mem_size_bytes": _Rule(
        "millrace_slatedb_memtable_bytes",
        "Memtable bytes across the pod's staging instances",
    ),
    "slatedb.db.backpressure_count": _Rule(
        "millrace_slatedb_write_stalls_total",
        "Write-stall (backpressure) events across the pod's staging instances",
    ),
    "slatedb.db.l0_stall_count": _Rule(
        "millrace_slatedb_l0_stalls_total",
        "L0 write stalls across the pod's staging instances, by SlateDB stall type",
        keep_labels=("type",),
    ),
    "slatedb.wal.wal_buffer_flushes": _Rule(
        "millrace_slatedb_wal_buffer_flushes_total",
        "WAL buffer flushes across the pod's staging instances (the "
        "stage-ack latency driver's rate arm)",
    ),
    "slatedb.wal.wal_buffer_flush_requests": _Rule(
        "millrace_slatedb_wal_buffer_flush_requests_total",
        "WAL buffer flush requests across the pod's staging instances",
    ),
    "slatedb.wal.wal_flush_bytes": _Rule(
        "millrace_slatedb_wal_flush_bytes_total",
        "WAL bytes flushed across the pod's staging instances",
    ),
    "slatedb.wal.wal_buffer_estimated_bytes": _Rule(
        "millrace_slatedb_wal_buffer_bytes",
        "Estimated WAL buffer bytes across the pod's staging instances",
    ),
    "slatedb.db_cache.access_count": _Rule(
        "millrace_slatedb_block_cache_accesses_total",
        "Block-cache accesses across the pod's staging instances, by "
        "entry kind and result (the hit rate is hit/(hit+miss) in PromQL)",
        keep_labels=("entry_kind", "result"),
    ),
}


def slatedb_families(
    snapshots: list[list[MetricSnapshot]],
) -> list[CounterMetricFamily | GaugeMetricFamily]:
    """Fold per-instance snapshots into the whitelisted Prometheus
    families — aggregated across instances (sum, or max for the depth
    gauge). Unknown or unparseable SlateDB names are ignored; a family
    with no observations in this snapshot set is NOT emitted (a quiet
    pod exposes no zero series for metrics its SlateDB never touched)."""
    collected: dict[str, dict[tuple[str, ...], list[int | float]]] = {}
    kinds: dict[str, str] = {}
    for snapshot in snapshots:
        for metric in snapshot:
            rule = _WHITELIST.get(metric.name)
            if rule is None or metric.value is None:
                continue  # forward-compat: unknown names and histograms
            label_values = tuple(
                dict(metric.labels).get(kept, "") for kept in rule.keep_labels
            )
            collected.setdefault(metric.name, {}).setdefault(label_values, []).append(
                metric.value
            )
            kinds[metric.name] = metric.kind

    families: list[CounterMetricFamily | GaugeMetricFamily] = []
    for name, rule in _WHITELIST.items():
        per_labels = collected.get(name)
        if not per_labels:
            continue
        # The family kind is the metric's SlateDB kind; the fold across
        # instances is a sum, except depth-style gauges whose pod signal
        # is the worst instance (max).
        family_type: Any = (
            CounterMetricFamily if kinds[name] == "counter" else GaugeMetricFamily
        )
        family = family_type(rule.family, rule.doc, labels=list(rule.keep_labels))
        for label_values, values in sorted(per_labels.items()):
            folded = max(values) if rule.aggregate == "max" else sum(values)
            if rule.keep_labels:
                family.add_metric(list(label_values), folded)
            else:
                family.add_metric([], folded)
        families.append(family)
    return families
