"""main.py's startup-validation surface and the metrics collector.

The wiring itself (config → StageManager → consumer + flusher + ops
server, signal stops, halt exit codes) is exercised by the live suite's
subprocess scenarios; these tests pin the parts that fail BEFORE any
service is needed — plus the pure decisions (exit-code classification,
probe status).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest
from flushkit import make_config, table_wire
from kafkakit import FakeConsumer
from pyhoglake import TableInfo

from millrace import main
from millrace.consumer import AssignmentError, ConsumerFencedError, MillraceConsumer
from millrace.flush import FlushHalted
from millrace.grouping import GroupKeyer
from millrace.stage import StageGauges, StageManager


def _valid_env() -> dict[str, str]:
    return {
        "MILLRACE_KAFKA_BOOTSTRAP_SERVERS": "broker:9092",
        "MILLRACE_KAFKA_TOPIC": "events",
        "MILLRACE_REPLICA_COUNT": "1",
        "POD_NAME": "millrace-events-0",
        "MILLRACE_CATALOG": "cat",
        "MILLRACE_NAMESPACE": "ns1",
        "MILLRACE_TABLE": "events",
        "MILLRACE_STAGE_URL": "memory:///",
    }


def test_main_refuses_invalid_config(capsys: pytest.CaptureFixture[str]):
    rc = main._main({})
    assert rc == main.EXIT_CONFIG
    err = capsys.readouterr().err
    assert "MILLRACE_KAFKA_BOOTSTRAP_SERVERS" in err
    assert "MILLRACE_STAGE_URL" in err
    # Every problem is reported at once — fail-fast, not one per restart.
    assert "problem(s)" in err


def test_main_requires_hoglake_url(capsys: pytest.CaptureFixture[str]):
    rc = main._main(_valid_env())
    assert rc == main.EXIT_CONFIG
    assert "HOGLAKE_URL" in capsys.readouterr().err


def test_main_maps_a_destination_refusal_to_the_config_exit(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    """A destination the configuration cannot create (destination.py:
    the catalog is missing and MILLRACE_CATALOG_DATA_PATH is unset) is a
    ConfigError raised from inside the pipeline's startup: exit 2 with
    the knob named on stderr, like any other config refusal — not the
    generic exit 1 a server failure gets."""
    from millrace.destination import DestinationConfigError

    def refuse(client: Any, cfg: Any) -> None:
        raise DestinationConfigError(
            ["catalog 'cat' does not exist and MILLRACE_CATALOG_DATA_PATH is not set"]
        )

    monkeypatch.setattr(main, "ensure_destination", refuse)
    rc = main._main(_valid_env() | {"HOGLAKE_URL": "http://hog.test"})
    assert rc == main.EXIT_CONFIG
    err = capsys.readouterr().err
    assert "MILLRACE_CATALOG_DATA_PATH" in err and "problem(s)" in err


def test_main_maps_a_destination_server_failure_to_exit_1_in_one_line(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
):
    """Transport / 5xx / 403 while ensuring the destination: exit 1 and
    ONE error line carrying the cause — no traceback."""
    from millrace.destination import DestinationError

    def fail(client: Any, cfg: Any) -> None:
        raise DestinationError("cannot ensure the hoglake destination: HTTP 503")

    monkeypatch.setattr(main, "ensure_destination", fail)
    with caplog.at_level(logging.ERROR, logger="millrace"):
        rc = main._main(_valid_env() | {"HOGLAKE_URL": "http://hog.test"})
    assert rc == main.EXIT_ERROR
    (record,) = [r for r in caplog.records if r.name == "millrace"]
    assert record.exc_info is None
    assert "HTTP 503" in record.message and "startup failed" in record.message


def test_main_ensures_the_destination_before_the_flusher_resolves(
    monkeypatch: pytest.MonkeyPatch,
):
    """The order is the point: the partition tuple is the staging key,
    so the destination exists (created or verified) before anything
    resolves and pins it."""
    calls: list[str] = []

    def ensure(client: Any, cfg: Any) -> None:
        calls.append("ensure")

    def resolve(cls: Any, client: Any, cfg: Any, **kwargs: Any) -> Any:
        calls.append("resolve")
        raise RuntimeError("stop here")

    monkeypatch.setattr(main, "ensure_destination", ensure)
    monkeypatch.setattr(main.HoglakeFlusher, "resolve", classmethod(resolve))
    rc = main._main(_valid_env() | {"HOGLAKE_URL": "http://hog.test"})
    assert rc == main.EXIT_ERROR
    assert calls == ["ensure", "resolve"]


def test_s3_kwargs_maps_only_present_values():
    assert main._s3_kwargs({}) == {}
    assert main._s3_kwargs(
        {
            "HOGLAKE_S3_ENDPOINT": "http://minio:9000",
            "HOGLAKE_S3_ACCESS_KEY": "a",
            "HOGLAKE_S3_SECRET_KEY": "s",
            "HOGLAKE_S3_REGION": "us-east-1",
        }
    ) == {
        "access_key": "a",
        "secret_key": "s",
        "endpoint_override": "http://minio:9000",
        "region": "us-east-1",
    }


def _collector():
    """A _PipelineCollector over real-but-unstarted components."""
    cfg = make_config()
    stages = StageManager("memory:///", "millrace")
    consumer = MillraceConsumer(
        FakeConsumer(),
        stages,
        cfg,
        keyer=GroupKeyer(TableInfo.from_wire(table_wire())),
        now_us=lambda: 0,
    )
    from pyhoglake import HoglakeClient

    from millrace.flush import HoglakeFlusher

    flusher = HoglakeFlusher(
        HoglakeClient("http://localhost:1"),
        cfg,
        now_us=lambda: 0,
        sleep=_no_sleep,
        monotonic=lambda: 0.0,
        executor=None,
    )
    return consumer, stages, flusher


async def _no_sleep(_seconds: float) -> None:
    return None


def _samples(collector: Any) -> dict[str, float]:
    """The collector's output as {sample_name: value}."""
    return {s.name: s.value for m in collector.collect() for s in m.samples}


def test_pipeline_collector_counters_without_a_loop():
    """Sync context (no running loop): the gauge arm degrades, the
    counters always collect."""
    import asyncio

    consumer, stages, flusher = _collector()
    loop = asyncio.new_event_loop()
    try:
        collector = main._PipelineCollector(consumer, flusher, stages, loop)
        samples = _samples(collector)
        assert samples["millrace_messages_consumed_total"] == 0
        # No running loop: the staging gauges degrade to absent.
        assert "millrace_staged_bytes" not in samples
    finally:
        flusher.close()
        # Drain the scheduled-but-never-run gauges() hop before closing,
        # so no coroutine dies unawaited.
        loop.run_until_complete(asyncio.sleep(0.05))
        loop.close()


async def test_pipeline_collector_gauges_with_a_loop():
    import asyncio

    consumer, stages, flusher = _collector()
    try:
        collector = main._PipelineCollector(
            consumer, flusher, stages, asyncio.get_running_loop()
        )
        # collect() runs on the ops-server thread in production — calling
        # it on the loop's own thread would deadlock the gauge arm's
        # run_coroutine_threadsafe into its timeout.
        samples = await asyncio.to_thread(_samples, collector)
        # Zero open partitions: staged bytes collect as 0; the
        # eligible-age gauge is absent (a live scan carries no policy
        # input, and nothing is staged anyway).
        assert samples["millrace_staged_bytes"] == 0
        assert "millrace_oldest_eligible_staged_age_seconds" not in samples
        assert "millrace_oldest_staged_age_seconds" not in samples  # renamed away
    finally:
        flusher.close()


async def test_pipeline_collector_bridges_slatedb_metrics():
    """The SlateDB arm: a real stage's recorder flows into the
    whitelisted pod-level families, aggregated — and a broken recorder
    degrades to its families absent, never a scrape failure."""
    import asyncio

    consumer, stages, flusher = _collector()
    try:
        opened = await stages.open_partition("events", 0)
        from stagekit import rec

        await opened.stage.stage_batch([rec(1, 100, 0)], now_us=0)
        collector = main._PipelineCollector(
            consumer, flusher, stages, asyncio.get_running_loop()
        )
        samples = await asyncio.to_thread(_samples, collector)
        # A whitelisted counter the one write batch provably bumped.
        assert samples["millrace_slatedb_wal_flush_bytes_total"] > 0
        # Counters/gauges of the pipeline itself still collect.
        assert samples["millrace_messages_consumed_total"] == 0

        class BrokenRecorder:
            def snapshot(self) -> Any:
                raise RuntimeError("recorder lost")

        opened.stage._metrics_recorder = BrokenRecorder()
        samples = await asyncio.to_thread(_samples, collector)
        assert "millrace_slatedb_wal_flush_bytes_total" not in samples
        assert samples["millrace_messages_consumed_total"] == 0
        assert "millrace_staged_bytes" in samples  # the gauge arm survives
    finally:
        flusher.close()


# -- exit-code classification (pure) ---------------------------------------------


def _halted() -> FlushHalted:
    return FlushHalted("destination_missing", "the table is gone")


def test_exit_code_classification():
    """The exit-code map (README §Rollouts and fencing): FENCED gets its
    own code so a supervisor can refuse the restart-into-the-same-path —
    and it outranks a simultaneous halt (the flap accrues immediately;
    the halt's reconciliation waits either way)."""
    assert main._exit_code_for([None]) == 0
    assert main._exit_code_for([]) == 0
    assert main._exit_code_for([RuntimeError("boom")]) == main.EXIT_ERROR
    # A topology refusal at start shares the config exit: the operator
    # changes the replica count or the topic, never "restart and hope".
    assert main._exit_code_for([AssignmentError("no share")]) == main.EXIT_CONFIG
    assert main._exit_code_for([_halted()]) == main.EXIT_HALTED
    fenced = ConsumerFencedError([("events", 3)])
    assert main._exit_code_for([fenced]) == main.EXIT_FENCED
    # both at once: the fence wins
    assert main._exit_code_for([_halted(), fenced]) == main.EXIT_FENCED
    assert main.EXIT_FENCED not in (main.EXIT_ERROR, main.EXIT_CONFIG, main.EXIT_HALTED)


# -- the probe decisions (pure; millpond's _liveness_status pattern) ---------------


def test_liveness_status_latches_only():
    assert (
        main.liveness_status(halted=None, consumer_fenced=False, fenced_partitions=())
        is None
    )
    # a halted flush pipeline is not live
    problem = main.liveness_status(
        halted=_halted(), consumer_fenced=False, fenced_partitions=()
    )
    assert problem is not None and "HALTED" in problem
    # a fenced consumer is not live (and the probe says why)
    problem = main.liveness_status(
        halted=None, consumer_fenced=True, fenced_partitions=()
    )
    assert problem is not None and "fenced" in problem
    # a partition fenced out of flush scheduling is a liveness failure
    # even while the consumer idles (nothing else ever says it)
    problem = main.liveness_status(
        halted=None, consumer_fenced=False, fenced_partitions=[("events", 7)]
    )
    assert problem is not None and "events[7]" in problem
    # precedence: the halt is named first when several hold
    problem = main.liveness_status(
        halted=_halted(), consumer_fenced=True, fenced_partitions=[("events", 7)]
    )
    assert problem is not None and "HALTED" in problem


def test_readiness_status():
    # never started: not ready, whatever liveness says
    assert main.readiness_status(started=False, liveness=None) is not None
    # started and healthy: ready
    assert main.readiness_status(started=True, liveness=None) is None
    # started but halted/fenced: NOT ready — a pod that lost a
    # partition's ownership (static assignment: the fence means the
    # ordinal double-assigned) must not pass readiness
    problem = main.readiness_status(started=True, liveness="HALTED: x")
    assert problem == "HALTED: x"


def test_probe_read_retries_the_gil_race_then_degrades():
    """The ops thread reads loop-thread sets; a raced iteration is a
    transient RuntimeError — retried, and only persistently raced reads
    degrade to the fallback (never a 500 on the probe thread)."""
    calls = {"n": 0}

    def flaky() -> int:
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("set changed size during iteration")
        return 7

    assert main._probe_read(flaky, -1) == 7 and calls["n"] == 3

    def always_raced() -> int:
        raise RuntimeError("set changed size during iteration")

    assert main._probe_read(always_raced, -1) == -1  # degraded, not raised


# -- collector additions: per-partition series, fenced set, the eligible-age gauge ------


def _collector_with_runner():
    consumer, stages, flusher = _collector()
    from millrace.flush import FlushRunner

    cfg = make_config()
    runner = FlushRunner(stages, flusher, cfg.planner_knobs(), now_us=lambda: 0)
    return consumer, stages, flusher, runner


async def test_pipeline_collector_per_partition_and_fenced_series():
    """A stuck partition must be FINDABLE: per-partition staged bytes and
    oldest-age series (cardinality bounded by assigned partitions), plus
    the flush runner's fenced set as a labeled series."""
    consumer, stages, flusher, runner = _collector_with_runner()
    try:
        opened = await stages.open_partition("events", 0)
        from stagekit import NOW, rec

        await opened.stage.stage_batch([rec(1, 100, 0, payload=b"abcd")], now_us=NOW)
        runner._note_fenced(("events", 1), RuntimeError("Closed(FENCED)"), where="test")

        collector = main._PipelineCollector(
            consumer,
            flusher,
            stages,
            asyncio.get_running_loop(),
            runner=runner,
        )
        raw: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
        for family in await asyncio.to_thread(lambda: list(collector.collect())):
            for s in family.samples:
                raw[(s.name, tuple(sorted(s.labels.items())))] = s.value
        labels = (("partition", "0"), ("topic", "events"))
        assert raw[("millrace_partition_staged_bytes", labels)] == 4
        assert ("millrace_partition_oldest_staged_age_seconds", labels) in raw
        assert (
            raw[
                (
                    "millrace_flush_partition_fenced",
                    (("partition", "1"), ("topic", "events")),
                )
            ]
            == 1.0
        )
    finally:
        flusher.close()


async def test_pipeline_collector_serves_the_sweep_published_eligible_age():
    """The alertable backlog series: the pod-level oldest-age gauge is
    the oldest ELIGIBLE staged key, computed by the flush sweep with the
    planner's own knobs and published with the fold — here the sweep is
    stood in for by a direct ``publish_gauges`` (the runner-side
    computation is pinned in test_flush.py). A live scan (no published
    snapshot) carries no policy input and leaves the series absent;
    the raw per-partition oldest stays regardless."""
    from stagekit import NOW

    consumer, stages, flusher = _collector()
    try:
        opened = await stages.open_partition("events", 0)
        from stagekit import rec

        await opened.stage.stage_batch([rec(1, 100, 0, payload=b"abcd")], now_us=NOW)
        collector = main._PipelineCollector(
            consumer,
            flusher,
            stages,
            asyncio.get_running_loop(),
            gauge_max_staleness_s=1000.0,
        )

        async def raw_samples() -> dict[tuple[str, tuple[tuple[str, str], ...]], float]:
            out: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
            for family in await asyncio.to_thread(lambda: list(collector.collect())):
                for s in family.samples:
                    out[(s.name, tuple(sorted(s.labels.items())))] = s.value
            return out

        # No sweep has published: the live fallback serves the raw
        # per-partition gauges but NO eligible-age series.
        raw = await raw_samples()
        assert ("millrace_oldest_eligible_staged_age_seconds", ()) not in raw
        assert (
            "millrace_partition_oldest_staged_age_seconds",
            (("partition", "0"), ("topic", "events")),
        ) in raw

        # The sweep publishes its fold with an eligible oldest: the
        # series appears, and the retired raw pod-level series is gone.
        stages.publish_gauges(
            {
                ("events", 0): StageGauges(
                    staged_bytes=4,
                    staged_rows=1,
                    staged_groups=1,
                    oldest_first_staged_ts=NOW,
                )
            },
            oldest_eligible_staged_ts=NOW,
        )
        raw = await raw_samples()
        assert ("millrace_oldest_eligible_staged_age_seconds", ()) in raw
        assert ("millrace_oldest_staged_age_seconds", ()) not in raw
        # The pod's staging-group count (partition tuples holding rows).
        assert raw[("millrace_staged_groups", ())] == 1

        # A publish with nothing eligible drops the series (absent, not
        # zero — "no backlog" is not "backlog age zero").
        stages.publish_gauges(
            {
                ("events", 0): StageGauges(
                    staged_bytes=4,
                    staged_rows=1,
                    staged_groups=1,
                    oldest_first_staged_ts=NOW,
                )
            },
            oldest_eligible_staged_ts=None,
        )
        raw = await raw_samples()
        assert ("millrace_oldest_eligible_staged_age_seconds", ()) not in raw
    finally:
        flusher.close()
        await stages.close()


# -- the dropped-records counter ---------------------------------------------------


def test_records_dropped_total_merges_consume_and_flush_reasons():
    """One labeled counter for both halves: the consumer's and the
    flusher's per-reason tallies sum per label; no other drop series
    exists."""
    consumer, stages, flusher = _collector()
    loop = asyncio.new_event_loop()
    try:
        consumer._records_dropped.update({"malformed_value": 2, "uncastable_column": 1})
        flusher._records_dropped.update(
            {"uncastable_column": 3, "event_time_out_of_window": 4}
        )
        collector = main._PipelineCollector(consumer, flusher, stages, loop)
        raw = {
            (s.name, tuple(sorted(s.labels.items()))): s.value
            for m in collector.collect()
            for s in m.samples
        }
        dropped = {
            labels[0][1]: value
            for (name, labels), value in raw.items()
            if name == "millrace_records_dropped_total"
        }
        assert dropped == {
            "malformed_value": 2,
            "uncastable_column": 4,
            "event_time_out_of_window": 4,
        }
        assert not any("poison" in name or "quarantin" in name for name, _ in raw)
    finally:
        flusher.close()
        loop.run_until_complete(asyncio.sleep(0.05))
        loop.close()


# -- the flush counters, by exact name, labels and value ---------------------------


def _raw(collector: Any) -> dict[tuple[str, tuple[tuple[str, str], ...]], float]:
    return {
        (s.name, tuple(sorted(s.labels.items()))): s.value
        for m in collector.collect()
        for s in m.samples
    }


def test_flush_counters_are_exposed_by_exact_name_labels_and_value():
    """Each flusher counter reaches its own series: seeded with distinct
    values, a swapped pair of counters (unresolved vs orphaned, say)
    reads back wrong. ``millrace_prepared_reprepared_total`` carries
    every reason in REPREPARE_REASONS, a never-incremented one at 0."""
    from millrace.flush import REPREPARE_REASONS

    consumer, stages, flusher = _collector()
    loop = asyncio.new_event_loop()
    try:
        collector = main._PipelineCollector(consumer, flusher, stages, loop)
        zero = _raw(collector)
        assert {
            labels: value
            for (name, labels), value in zero.items()
            if name == "millrace_prepared_reprepared_total"
        } == {(("reason", reason),): 0 for reason in REPREPARE_REASONS}
        assert set(REPREPARE_REASONS) == {
            "ddl_since_read_snapshot",
            "read_snapshot_expired",
        }

        flusher._prepared_replays = 3
        flusher._reprepared["read_snapshot_expired"] = 5
        flusher._unresolved_uploads = 7
        flusher._receipt_settlements = 11
        flusher._orphaned_uploads = 13
        flusher._flush_deduped = 17
        raw = _raw(collector)
        assert raw[("millrace_prepared_replays_total", ())] == 3
        assert (
            raw[
                (
                    "millrace_prepared_reprepared_total",
                    (("reason", "read_snapshot_expired"),),
                )
            ]
            == 5
        )
        assert (
            raw[
                (
                    "millrace_prepared_reprepared_total",
                    (("reason", "ddl_since_read_snapshot"),),
                )
            ]
            == 0
        )
        assert raw[("millrace_flush_unresolved_uploads_total", ())] == 7
        assert raw[("millrace_receipt_settlements_total", ())] == 11
        assert raw[("millrace_flush_orphaned_uploads_total", ())] == 13
        assert raw[("millrace_flush_deduped_total", ())] == 17
        assert len(
            [k for k in raw if k[0] == "millrace_prepared_reprepared_total"]
        ) == len(REPREPARE_REASONS)
    finally:
        flusher.close()
        loop.run_until_complete(asyncio.sleep(0.05))
        loop.close()


# -- the pod-level SlateDB cache ------------------------------------------------------


@pytest.mark.parametrize(
    ("env", "expected"),
    [
        (
            {
                "MILLRACE_SLATEDB_BLOCK_CACHE_BYTES": "12345678",
                "MILLRACE_SLATEDB_META_CACHE_BYTES": "87654321",
            },
            (12345678, 87654321),
        ),
        (
            {
                "MILLRACE_SLATEDB_BLOCK_CACHE_BYTES": "0",
                "MILLRACE_SLATEDB_META_CACHE_BYTES": "0",
            },
            (0, 0),
        ),
    ],
    ids=["non-default", "disabled"],
)
def test_main_passes_the_cache_pair_to_the_stage_manager(
    monkeypatch: pytest.MonkeyPatch, env: dict[str, str], expected: tuple[int, int]
):
    """The env pair reaches the ONE StageManager the pipeline builds —
    non-default values (so a dropped kwarg falling back to the
    StageManager default cannot pass) and the 0/0 disabled pair."""
    seen: list[dict[str, Any]] = []

    def stub(*args: Any, **kwargs: Any) -> Any:
        seen.append(kwargs)
        raise RuntimeError("stop here")

    monkeypatch.setattr(main, "StageManager", stub)
    rc = main._main(_valid_env() | env | {"HOGLAKE_URL": "http://hog.test"})
    assert rc == main.EXIT_ERROR
    (kwargs,) = seen
    assert (kwargs["block_cache_bytes"], kwargs["meta_cache_bytes"]) == expected


def _labelled(collector: Any, family: str) -> dict[tuple[tuple[str, str], ...], float]:
    return {
        tuple(sorted(s.labels.items())): s.value
        for m in collector.collect()
        if m.name == family
        for s in m.samples
    }


@pytest.mark.parametrize(
    ("block", "meta"), [(8192, 16384), (0, 0)], ids=["sized", "disabled"]
)
async def test_pipeline_collector_exports_the_cache_capacity(block: int, meta: int):
    """``millrace_slatedb_cache_capacity_bytes{half}``: the configured
    pair, static, 0/0 when disabled — the binding exposes no occupancy."""
    import asyncio

    consumer, _, flusher = _collector()
    stages = StageManager(
        "memory:///", "millrace", block_cache_bytes=block, meta_cache_bytes=meta
    )
    try:
        collector = main._PipelineCollector(
            consumer, flusher, stages, asyncio.get_running_loop()
        )
        got = await asyncio.to_thread(
            _labelled, collector, "millrace_slatedb_cache_capacity_bytes"
        )
        assert got == {(("half", "block"),): block, (("half", "meta"),): meta}
    finally:
        flusher.close()
        await stages.close()
