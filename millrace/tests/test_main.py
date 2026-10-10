"""main.py's startup-validation surface and the metrics collector.

The wiring itself (config → StageManager → consumer + flusher + ops
server, signal stops, halt exit codes) is exercised by the live suite's
subprocess scenarios; these tests pin the parts that fail BEFORE any
service is needed — plus the pure decisions (exit-code classification,
probe status) and the poison sweeper's loop discipline.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from flushkit import make_config
from kafkakit import FakeConsumer

from millrace import main
from millrace.consumer import ConsumerFencedError, MillraceConsumer
from millrace.flush import FlushHalted
from millrace.stage import StageManager


def _valid_env() -> dict[str, str]:
    return {
        "MILLRACE_KAFKA_BOOTSTRAP_SERVERS": "broker:9092",
        "MILLRACE_KAFKA_TOPIC": "events",
        "MILLRACE_KAFKA_PARTITIONS": "0",
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
    consumer = MillraceConsumer(FakeConsumer(), stages, cfg, now_us=lambda: 0)
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
        assert samples["millrace_backpressure_paused"] == 0.0
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
        # Zero open partitions: staged bytes collect as 0, the age gauge
        # is absent (nothing staged).
        assert samples["millrace_staged_bytes"] == 0
        assert "millrace_oldest_staged_age_seconds" not in samples
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


# -- collector additions: per-partition series, fenced set, paused count ----------


def _collector_with_runner():
    consumer, stages, flusher = _collector()
    from millrace.flush import FlushRunner

    cfg = make_config()
    runner = FlushRunner(stages, flusher, cfg.planner_knobs(), now_us=lambda: 0)
    sweeper = main.PoisonSweeper(
        stages, retention_s=7 * 86400, sweep_s=3600, now_us=lambda: 0
    )
    return consumer, stages, flusher, runner, sweeper


async def test_pipeline_collector_per_partition_and_fenced_series():
    """A stuck partition must be FINDABLE: per-partition staged bytes and
    oldest-age series (cardinality bounded by assigned partitions), plus
    the flush runner's fenced set as a labeled series."""
    consumer, stages, flusher, runner, sweeper = _collector_with_runner()
    try:
        opened = await stages.open_partition("events", 0)
        from stagekit import NOW, rec

        await opened.stage.stage_batch([rec(1, 100, 0, payload=b"abcd")], now_us=NOW)
        runner._note_fenced(("events", 1), RuntimeError("Closed(FENCED)"), where="test")
        consumer._applied_pause.add(("events", 0))

        collector = main._PipelineCollector(
            consumer,
            flusher,
            stages,
            asyncio.get_running_loop(),
            runner=runner,
            sweeper=sweeper,
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
        # the two-level backpressure surface: any-paused + the count
        assert raw[("millrace_backpressure_paused", ())] == 1.0
        assert raw[("millrace_backpressure_paused_partitions", ())] == 1.0
        assert raw[("millrace_poison_entries_purged_total", ())] == 0
    finally:
        flusher.close()


# -- the poison sweeper -----------------------------------------------------------


async def test_poison_sweeper_purges_expired_entries_per_partition():
    """End to end over memory:/// stages: entries past the retention go,
    fresh ones stay, and the counter tracks the total (the loop's own
    discipline — bounded purges per partition, failures contained)."""
    from stagekit import NOW, rec

    from millrace.stage import PoisonedRecord

    stages = StageManager("memory:///", "millrace")
    try:
        opened = await stages.open_partition("events", 0)
        stage = opened.stage
        await stage.stage_batch(
            [rec(1, 100, 0)],
            poison=[
                PoisonedRecord(7, "malformed_key", None, b"x", 1),  # stamped NOW
            ],
            now_us=NOW,
        )
        clock = {"now": NOW}

        sweeper = main.PoisonSweeper(
            stages,
            retention_s=3600,
            sweep_s=60,
            now_us=lambda: clock["now"],
        )
        # nothing is old yet
        assert await sweeper.sweep_once() == 0
        assert [p.offset for p in await stage.scan_poison()] == [7]
        # age the world past the retention
        clock["now"] += 3601 * 1_000_000
        assert await sweeper.sweep_once() == 1
        assert await stage.scan_poison() == []
        assert sweeper.purged_total == 1
        assert sweeper.sweeps_total == 2
    finally:
        await stages.close()


async def test_poison_sweeper_contains_partition_failures_and_stops_promptly():
    """A partition whose purge raises is contained (counted, logged, the
    sweep continues); stop() wakes the between-sweep pause."""
    from stagekit import NOW

    stages = StageManager("memory:///", "millrace")
    try:
        opened = await stages.open_partition("events", 0)

        class Boom:
            async def purge_poison_expired(self, cutoff: int) -> Any:
                raise RuntimeError("store sad")

        sweeper = main.PoisonSweeper(
            stages, retention_s=3600, sweep_s=60, now_us=lambda: NOW
        )
        # break the one partition's purge
        monkey_target = opened.stage
        real = monkey_target.purge_poison_expired
        monkey_target.purge_poison_expired = Boom().purge_poison_expired  # type: ignore[method-assign]
        assert await sweeper.sweep_once() == 0
        assert sweeper.partition_failures_total == 1
        monkey_target.purge_poison_expired = real  # type: ignore[method-assign]
        assert await sweeper.sweep_once() == 0
        assert sweeper.partition_failures_total == 1

        # run() loop: stops promptly rather than riding out a long sweep_s
        sweeper2 = main.PoisonSweeper(
            stages, retention_s=3600, sweep_s=3600, now_us=lambda: NOW
        )
        runner = asyncio.create_task(sweeper2.run())
        await asyncio.sleep(0.05)  # first sweep + pause begins
        sweeper2.stop()
        await asyncio.wait_for(runner, timeout=10)
        assert sweeper2.sweeps_total == 1
    finally:
        await stages.close()
