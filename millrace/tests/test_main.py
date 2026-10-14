"""main.py's startup-validation surface and the metrics collector.

The wiring itself (config → StageManager → drain → consumer + flusher
+ ops server, signal stops, halt exit codes) is exercised by the live suite's
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
from millrace.consumer import AssignmentError, MillraceConsumer
from millrace.flush import FlushHalted
from millrace.grouping import GroupKeyer
from millrace.stage import FencedError, StageManager


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


def _components(**stage_kwargs: Any) -> tuple[Any, StageManager, Any, Any]:
    """Real-but-unstarted components for the collector."""
    from pyhoglake import HoglakeClient

    from millrace.flush import FlushRunner, HoglakeFlusher

    cfg = make_config()
    stages = StageManager("memory:///", "millrace", **stage_kwargs)
    consumer = MillraceConsumer(
        FakeConsumer(),
        stages,
        cfg,
        keyer=GroupKeyer(TableInfo.from_wire(table_wire())),
        now_us=lambda: 0,
    )
    flusher = HoglakeFlusher(
        HoglakeClient("http://localhost:1"),
        cfg,
        now_us=lambda: 0,
        sleep=_no_sleep,
        monotonic=lambda: 0.0,
        executor=None,
    )
    runner = FlushRunner(stages, flusher, cfg.planner_knobs(), now_us=lambda: 0)
    return consumer, stages, flusher, runner


async def _no_sleep(_seconds: float) -> None:
    return None


def _collector(
    consumer: Any, stages: Any, flusher: Any, runner: Any, *, draining: bool = False
) -> Any:
    return main._PipelineCollector(
        consumer, flusher, stages, runner, draining=lambda: draining
    )


def _raw(collector: Any) -> dict[tuple[str, tuple[tuple[str, str], ...]], float]:
    return {
        (s.name, tuple(sorted(s.labels.items()))): s.value
        for m in collector.collect()
        for s in m.samples
    }


def test_pipeline_collector_reads_in_memory_state_without_a_loop():
    """No event loop anywhere: every series is read from in-memory state
    on the scrape thread."""
    consumer, stages, flusher, runner = _components()
    try:
        raw = _raw(_collector(consumer, stages, flusher, runner, draining=True))
        assert raw[("millrace_messages_consumed_total", ())] == 0
        assert raw[("millrace_staged_bytes", ())] == 0
        assert raw[("millrace_staged_groups", ())] == 0
        assert raw[("millrace_draining", ())] == 1.0
        assert raw[("millrace_flush_groups_stuck", ())] == 0
        # Nothing staged, nothing due: the age series are absent.
        assert ("millrace_oldest_staged_age_seconds", ()) not in raw
        assert ("millrace_oldest_eligible_staged_age_seconds", ()) not in raw
    finally:
        flusher.close()


async def test_pipeline_collector_staging_gauges_and_slatedb_bridge():
    """Staged rows show in the pod gauges straight from the index; a real
    stage's SlateDB recorder flows into the pod-level families, and a
    broken recorder degrades to its families absent."""
    from stagekit import NOW, rec

    consumer, stages, flusher, runner = _components()
    try:
        stage = await stages.open_partition("events", 0, now_us=NOW)
        await stage.stage_batch([rec(1, 100, 0, payload=b"abcd")], now_us=NOW)
        collector = _collector(consumer, stages, flusher, runner)
        raw = await asyncio.to_thread(_raw, collector)
        assert raw[("millrace_staged_bytes", ())] == 4
        assert raw[("millrace_staged_rows", ())] == 1
        assert raw[("millrace_staged_groups", ())] == 1
        assert raw[("millrace_draining", ())] == 0.0
        assert ("millrace_oldest_staged_age_seconds", ()) in raw
        assert raw[("millrace_slatedb_wal_flush_bytes_total", ())] > 0

        class BrokenRecorder:
            def snapshot(self) -> Any:
                raise RuntimeError("recorder lost")

        stage._metrics_recorder = BrokenRecorder()
        raw = await asyncio.to_thread(_raw, collector)
        assert ("millrace_slatedb_wal_flush_bytes_total", ()) not in raw
        assert raw[("millrace_staged_bytes", ())] == 4
    finally:
        flusher.close()
        await stages.close()


def test_pipeline_collector_serves_the_runners_oldest_due_age():
    consumer, stages, flusher, runner = _components()
    try:
        collector = _collector(consumer, stages, flusher, runner)
        assert ("millrace_oldest_eligible_staged_age_seconds", ()) not in _raw(
            collector
        )
        runner._oldest_due_ts = 0
        raw = _raw(collector)
        assert raw[("millrace_oldest_eligible_staged_age_seconds", ())] > 0
    finally:
        flusher.close()


def test_records_dropped_total_merges_consume_and_flush_reasons():
    consumer, stages, flusher, runner = _components()
    try:
        consumer._records_dropped.update({"malformed_value": 2, "uncastable_column": 1})
        flusher._records_dropped.update(
            {"uncastable_column": 3, "undecodable_payload": 4}
        )
        raw = _raw(_collector(consumer, stages, flusher, runner))
        dropped = {
            labels[0][1]: value
            for (name, labels), value in raw.items()
            if name == "millrace_records_dropped_total"
        }
        assert dropped == {
            "malformed_value": 2,
            "uncastable_column": 4,
            "undecodable_payload": 4,
        }
    finally:
        flusher.close()


def test_flush_counters_are_exposed_by_exact_name_labels_and_value():
    """Seeded with distinct values, so a swapped pair reads back wrong."""
    from millrace.flush import STUCK_AFTER, _Failures

    consumer, stages, flusher, runner = _components()
    try:
        flusher._decisions_committed = 3
        flusher._decisions_failed = 5
        flusher._commits = 7
        flusher._rows_published = 11
        flusher._files_written = 13
        flusher._bytes_written = 17
        flusher._orphaned_uploads = 19
        flusher._commit_refusals.update({"422": 23, "400": 29})
        runner._stuck_total = 31
        flusher._ambiguous_uploads = 37
        flusher._values_clamped.update({"timestamp": 41, "props.ts": 43})
        runner._failures[("events", 0, ("1",))] = _Failures(STUCK_AFTER, 0.0)
        runner._failures[("events", 0, ("2",))] = _Failures(1, 0.0)
        raw = _raw(_collector(consumer, stages, flusher, runner))
        assert raw[("millrace_flushes_committed_total", ())] == 3
        assert raw[("millrace_flush_failures_total", ())] == 5
        assert raw[("millrace_flush_commits_total", ())] == 7
        assert raw[("millrace_flush_rows_published_total", ())] == 11
        assert raw[("millrace_flush_files_written_total", ())] == 13
        assert raw[("millrace_flush_bytes_written_total", ())] == 17
        assert raw[("millrace_flush_orphaned_uploads_total", ())] == 19
        assert raw[("millrace_flush_commit_refusals_total", (("status", "422"),))] == 23
        assert raw[("millrace_flush_commit_refusals_total", (("status", "400"),))] == 29
        assert raw[("millrace_flush_groups_stuck_total", ())] == 31
        assert raw[("millrace_flush_groups_stuck", ())] == 1
        assert raw[("millrace_flush_ambiguous_uploads_total", ())] == 37
        assert raw[("millrace_values_clamped_total", (("column", "timestamp"),))] == 41
        assert raw[("millrace_values_clamped_total", (("column", "props.ts"),))] == 43
        # The removed exactly-once machinery exports nothing.
        assert not any(
            word in name
            for name, _ in raw
            for word in ("prepared", "receipt", "deduped", "unresolved", "fenced")
        )
    finally:
        flusher.close()


@pytest.mark.parametrize(
    ("block", "meta"), [(8192, 16384), (0, 0)], ids=["sized", "disabled"]
)
def test_pipeline_collector_exports_the_cache_capacity(block: int, meta: int):
    consumer, stages, flusher, runner = _components(
        block_cache_bytes=block, meta_cache_bytes=meta
    )
    try:
        raw = _raw(_collector(consumer, stages, flusher, runner))
        assert (
            raw[("millrace_slatedb_cache_capacity_bytes", (("half", "block"),))]
            == block
        )
        assert (
            raw[("millrace_slatedb_cache_capacity_bytes", (("half", "meta"),))] == meta
        )
    finally:
        flusher.close()


# -- exit-code classification and probes (pure) ----------------------------------


def _halted() -> FlushHalted:
    return FlushHalted("table_recreated", "the table now resolves elsewhere")


def test_exit_code_classification():
    """Fenced (4) outranks halted (3) outranks an assignment refusal (2)
    outranks anything else (1)."""
    assert main._exit_code_for([None]) == 0
    assert main._exit_code_for([]) == 0
    assert main._exit_code_for([RuntimeError("boom")]) == main.EXIT_ERROR
    assert main._exit_code_for([AssignmentError("no share")]) == main.EXIT_CONFIG
    assert main._exit_code_for([_halted()]) == main.EXIT_HALTED
    fenced = FencedError([("events", 3)])
    assert main._exit_code_for([fenced]) == main.EXIT_FENCED
    assert main._exit_code_for([_halted(), fenced]) == main.EXIT_FENCED
    assert main._exit_code_for([AssignmentError("x"), _halted()]) == main.EXIT_HALTED
    assert main._exit_code_for([RuntimeError("x"), AssignmentError("y")]) == (
        main.EXIT_CONFIG
    )
    assert (main.EXIT_ERROR, main.EXIT_CONFIG, main.EXIT_HALTED, main.EXIT_FENCED) == (
        1,
        2,
        3,
        4,
    )


def test_liveness_status_latches_only_on_a_terminal_condition():
    assert main.liveness_status(terminal=None) is None
    problem = main.liveness_status(terminal=_halted())
    assert problem is not None and "HALTED" in problem
    problem = main.liveness_status(terminal=FencedError([("events", 7)]))
    assert problem is not None and "fenced" in problem and "events[7]" in problem


def test_readiness_status_is_unready_through_the_drain():
    # Before the drain finished and the consumer was assigned: not ready.
    problem = main.readiness_status(started=False, liveness=None)
    assert problem is not None and "drain" in problem
    assert main.readiness_status(started=True, liveness=None) is None
    assert main.readiness_status(started=True, liveness="HALTED: x") == "HALTED: x"


def test_probe_read_retries_the_gil_race_then_degrades():
    calls = {"n": 0}

    def flaky() -> int:
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("dict changed size during iteration")
        return 7

    assert main._probe_read(flaky, -1) == 7 and calls["n"] == 3

    def always_raced() -> int:
        raise RuntimeError("dict changed size during iteration")

    assert main._probe_read(always_raced, -1) == -1
