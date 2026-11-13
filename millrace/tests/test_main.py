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
from collections import Counter
from typing import Any

import httpx
import pytest
from flushkit import make_config, table_wire
from kafkakit import FakeConsumer
from pyhoglake import TableInfo, _variant_backend
from pyhoglake.variant import VariantReport

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


def _components(
    variant_sources: tuple[tuple[str, str], ...] = (), **stage_kwargs: Any
) -> tuple[Any, StageManager, Any, Any]:
    """Real-but-unstarted components for the collector."""
    from pyhoglake import HoglakeClient

    from millrace.flush import FlushRunner, HoglakeFlusher

    cfg = make_config(variant_sources=variant_sources)
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
        stage = await stages.open_partition("events", 0)
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


def test_flush_counters_are_exposed_by_exact_name_labels_and_value(
    monkeypatch: pytest.MonkeyPatch,
):
    """Seeded with distinct values, so a swapped pair reads back wrong."""
    from millrace.flush import BUILD_STAGES, STUCK_AFTER, _Failures

    monkeypatch.setattr(_variant_backend, "ENCODE_FALLBACKS", Counter())
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
        runner._drain_rows = 47
        runner._drain_bytes = 53
        flusher._ambiguous_uploads = 37
        flusher._values_clamped.update({"timestamp": 41, "props.ts": 43})
        runner._failures[("events", 0, ("1",))] = _Failures(STUCK_AFTER, 0.0)
        runner._failures[("events", 0, ("2",))] = _Failures(1, 0.0)
        raw = _raw(_collector(consumer, stages, flusher, runner))
        assert raw[("millrace_flushes_committed_total", ())] == 3
        assert raw[("millrace_flush_failures_total", ())] == 5
        assert (
            raw[
                (
                    "millrace_flush_commits_total",
                    (("catalog", "cat"), ("table", "ns1.events")),
                )
            ]
            == 7
        )
        assert ("millrace_flush_commits_total", ()) not in raw
        assert raw[("millrace_flush_rows_published_total", ())] == 11
        assert raw[("millrace_flush_files_written_total", ())] == 13
        assert raw[("millrace_flush_bytes_written_total", ())] == 17
        assert raw[("millrace_flush_orphaned_uploads_total", ())] == 19
        assert raw[("millrace_flush_commit_refusals_total", (("status", "422"),))] == 23
        assert raw[("millrace_flush_commit_refusals_total", (("status", "400"),))] == 29
        assert raw[("millrace_flush_groups_stuck_total", ())] == 31
        assert raw[("millrace_drain_rows_total", ())] == 47
        assert raw[("millrace_drain_bytes_total", ())] == 53
        assert raw[("millrace_flush_groups_stuck", ())] == 1
        assert raw[("millrace_flush_ambiguous_uploads_total", ())] == 37
        assert raw[("millrace_values_clamped_total", (("column", "timestamp"),))] == 41
        assert raw[("millrace_values_clamped_total", (("column", "props.ts"),))] == 43
        # Every stage is a series from the start, a zero until a build.
        built = {
            labels: value
            for (name, labels), value in raw.items()
            if name == "millrace_flush_build_seconds_total"
        }
        assert built == {(("stage", s),): 0.0 for s in BUILD_STAGES}
        assert raw[("millrace_flush_build_queue_seconds_total", ())] == 0.0
        for i, stage in enumerate(BUILD_STAGES):
            flusher._build_seconds[stage] = 59.0 + i
        flusher._build_queue_seconds = 67.5
        raw = _raw(_collector(consumer, stages, flusher, runner))
        for i, stage in enumerate(BUILD_STAGES):
            assert raw[("millrace_flush_build_seconds_total", (("stage", stage),))] == (
                59.0 + i
            )
        assert raw[("millrace_flush_build_queue_seconds_total", ())] == 67.5
        # Variant families: present with no series until a mapped column
        # is built, then one series per label tuple.
        variant = ("millrace_variant_rows_total", "millrace_variant_invalid_total")
        assert not any(name in variant for name, _ in raw)
        flusher._variant_rows.update({("sv", "encoded"): 71, ("sv", "invalid"): 73})
        flusher._variant_invalid[("sv", "duplicate_key")] = 79
        flusher._variant_values[("sv", "$.$os", "fallback")] = 83
        raw = _raw(_collector(consumer, stages, flusher, runner))
        assert (
            raw[
                (
                    "millrace_variant_rows_total",
                    (("column", "sv"), ("outcome", "encoded")),
                )
            ]
            == 71
        )
        assert (
            raw[
                (
                    "millrace_variant_rows_total",
                    (("column", "sv"), ("outcome", "invalid")),
                )
            ]
            == 73
        )
        assert (
            raw[
                (
                    "millrace_variant_invalid_total",
                    (("column", "sv"), ("reason", "duplicate_key")),
                )
            ]
            == 79
        )
        assert (
            raw[
                (
                    "millrace_variant_values_total",
                    (("column", "sv"), ("outcome", "fallback"), ("path", "$.$os")),
                )
            ]
            == 83
        )
        # The Rust codec's transitional series per build: the rows a
        # recursion bound took Python's verdict for and the shadow's
        # seconds. A report's fallbacks are pyhoglake's per-process count
        # already, so not counted again.
        fallback = "millrace_variant_codec_fallback_total"
        assert raw[("millrace_variant_codec_recursion_bound_rows_total", ())] == 0
        assert raw[("millrace_variant_shadow_seconds_total", ())] == 0.0
        report = VariantReport(
            codec_fallbacks={"mismatch": 2, "shadow_mismatch": 1},
            recursion_bound_rows=3,
            shadow_seconds=0.25,
        )
        flusher._note_variant("sv", report)
        flusher._note_variant("sv", report)
        raw = _raw(_collector(consumer, stages, flusher, runner))
        assert set(_by_reason(raw, fallback).values()) == {0}
        assert raw[("millrace_variant_codec_recursion_bound_rows_total", ())] == 6
        assert raw[("millrace_variant_shadow_seconds_total", ())] == 0.5
        # The removed exactly-once machinery exports nothing.
        assert not any(
            word in name
            for name, _ in raw
            for word in ("prepared", "receipt", "deduped", "unresolved", "fenced")
        )
    finally:
        flusher.close()


def _by_reason(raw: dict[Any, float], name: str) -> dict[str, float]:
    return {dict(labels)["reason"]: v for (n, labels), v in raw.items() if n == name}


def test_pipeline_collector_exports_the_codec_counts_per_process(
    monkeypatch: pytest.MonkeyPatch,
):
    """pyhoglake counts the codec's fallbacks, the calls auto gives Python
    and what the shadow compared per process (a read has no report, and an
    encode whose build raises later returns none); each scrape reads those
    counts, every reason from 0."""
    for counter in ("ENCODE_FALLBACKS", "DECODE_FALLBACKS", "ROUTED", "SHADOWED"):
        monkeypatch.setattr(_variant_backend, counter, Counter())
    consumer, stages, flusher, runner = _components()
    encode = "millrace_variant_codec_fallback_total"
    decode = "millrace_variant_codec_decode_fallback_total"
    shadow = (
        "millrace_variant_shadow_rows_total",
        "millrace_variant_shadow_calls_total",
    )

    def routed(raw: dict[Any, float]) -> dict[tuple[str, str], float]:
        name = "millrace_variant_codec_routed_to_python_total"
        return {
            (dict(k)["op"], dict(k)["reason"]): v
            for (n, k), v in raw.items()
            if n == name
        }

    try:
        collector = _collector(consumer, stages, flusher, runner)
        raw = _raw(collector)
        assert _by_reason(raw, encode) == dict.fromkeys(
            ["mismatch", "type", "panic", "shadow_mismatch"], 0
        )
        assert _by_reason(raw, decode) == {"mismatch": 0, "panic": 0}
        assert routed(raw) == {
            ("encode", "unavailable"): 0,
            ("encode", "plan"): 0,
            ("encode", "not_utf8"): 0,
            ("decode", "unavailable"): 0,
            ("decode", "storage"): 0,
        }
        assert [raw[(name, ())] for name in shadow] == [0, 0]
        report = VariantReport()
        _variant_backend._fall_back("mismatch", 1, "sv", report)
        _variant_backend._fall_back("shadow_mismatch", None, "sv", VariantReport())
        _variant_backend._fall_back("mismatch", None, None, VariantReport())
        _variant_backend._decode_fall_back("mismatch", 7, "sv")
        _variant_backend._decode_fall_back("mismatch", None, None)
        _variant_backend._decode_fall_back("panic", None, "sv")
        _variant_backend._count(_variant_backend.ROUTED, ("encode", "not_utf8"))
        _variant_backend._count(_variant_backend.ROUTED, ("decode", "storage"))
        _variant_backend.SHADOWED.update(calls=3, rows=1000)
        # Exported once, whether or not a build returns the report.
        flusher._note_variant("sv", report)
        raw = _raw(collector)
        assert _by_reason(raw, encode) == {
            "mismatch": 2,
            "type": 0,
            "panic": 0,
            "shadow_mismatch": 1,
        }
        assert _by_reason(raw, decode) == {"mismatch": 2, "panic": 1}
        assert {k: v for k, v in routed(raw).items() if v} == {
            ("encode", "not_utf8"): 1,
            ("decode", "storage"): 1,
        }
        assert [raw[(name, ())] for name in shadow] == [1000, 3]
    finally:
        flusher.close()


@pytest.mark.parametrize(
    ("mode", "module", "backend"),
    [("python", object(), "python"), ("auto", None, "python"),
     ("auto", object(), "native"), ("native", object(), "native")],
)  # fmt: skip
def test_pipeline_collector_names_the_variant_codec(
    monkeypatch: pytest.MonkeyPatch, mode: str, module: object, backend: str
):
    monkeypatch.setattr(_variant_backend, "MODE", mode)
    monkeypatch.setattr(_variant_backend, "native", lambda: (module, "why"))
    consumer, stages, flusher, runner = _components()
    try:
        raw = _raw(_collector(consumer, stages, flusher, runner))
        info = {k: v for k, v in raw.items() if k[0] == "millrace_variant_codec_info"}
        assert info == {("millrace_variant_codec_info", (("backend", backend),)): 1}
    finally:
        flusher.close()


@pytest.mark.parametrize("backend", ["native", "python"])
def test_pipeline_collector_exports_encode_seconds_per_column_and_codec(
    monkeypatch: pytest.MonkeyPatch, backend: str
):
    """Every mapped column under both codecs from the first scrape; a
    build's encode seconds go under the process's codec."""
    monkeypatch.setattr(_variant_backend, "MODE", backend)
    monkeypatch.setattr(_variant_backend, "native", lambda: (object(), ""))
    consumer, stages, flusher, runner = _components((("sv", "properties"),))
    name = "millrace_variant_encode_seconds_total"

    def exported() -> dict[tuple[str, str], float]:
        raw = _raw(_collector(consumer, stages, flusher, runner))
        return {
            (dict(labels)["column"], dict(labels)["backend"]): value
            for (n, labels), value in raw.items()
            if n == name
        }

    try:
        expected = {("sv", "native"): 0.0, ("sv", "python"): 0.0}
        assert exported() == expected
        flusher._note_variant("sv", VariantReport(encode_seconds=0.25))
        flusher._note_variant("sv", VariantReport(encode_seconds=0.5))
        assert exported() == expected | {("sv", backend): 0.75}
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


# -- the credential millrace presents to hoglake (HOGLAKE_AUTH_*) ------------


def test_hoglake_auth_is_none_when_nothing_is_set():
    """No HOGLAKE_AUTH_* means no Authorization header: what an
    HOGLAKE_AUTH_MODE=off server expects and what millrace always did."""
    assert main.hoglake_auth({}) is None
    assert main.hoglake_auth({"HOGLAKE_AUTH_TOKEN": "  "}) is None


def test_hoglake_auth_fixed_token_sends_it_as_a_bearer():
    from pyhoglake.auth import BearerToken

    auth = main.hoglake_auth({"HOGLAKE_AUTH_TOKEN": "eyJ.fixed"})
    assert isinstance(auth, BearerToken)
    (req,) = list(auth.auth_flow(httpx.Request("GET", "http://hog.test/v1/catalogs")))
    assert req.headers["Authorization"] == "Bearer eyJ.fixed"


def test_hoglake_auth_token_file_is_read_once_and_trimmed(tmp_path: Any):
    from pyhoglake.auth import BearerToken

    path = tmp_path / "token"
    path.write_text("eyJ.from-file\n")
    auth = main.hoglake_auth({"HOGLAKE_AUTH_TOKEN_FILE": str(path)})
    assert isinstance(auth, BearerToken)
    (req,) = list(auth.auth_flow(httpx.Request("GET", "http://hog.test/v1/catalogs")))
    assert req.headers["Authorization"] == "Bearer eyJ.from-file"


def test_hoglake_auth_token_file_problems_are_config_errors(tmp_path: Any):
    from millrace.config import ConfigError

    with pytest.raises(ConfigError, match="HOGLAKE_AUTH_TOKEN_FILE"):
        main.hoglake_auth({"HOGLAKE_AUTH_TOKEN_FILE": str(tmp_path / "missing")})
    empty = tmp_path / "empty"
    empty.write_text(" \n")
    with pytest.raises(ConfigError, match="is empty"):
        main.hoglake_auth({"HOGLAKE_AUTH_TOKEN_FILE": str(empty)})


def test_hoglake_auth_client_credentials_mint_against_the_token_endpoint(
    monkeypatch: pytest.MonkeyPatch,
):
    """The three variables build pyhoglake's ClientCredentials: one mint at
    the configured token URL with the client id and secret as HTTP Basic
    and the audience in the form, then the bearer on every request until
    it is refreshed — the contract in hoglake's docs/auth.md."""
    from pyhoglake.auth import ClientCredentials

    mints: list[dict[str, Any]] = []

    def fake_post(url: str, **kwargs: Any) -> httpx.Response:
        mints.append({"url": url, **kwargs})
        return httpx.Response(
            200, json={"access_token": "eyJ.minted", "expires_in": 900}
        )

    monkeypatch.setattr(httpx, "post", fake_post)
    auth = main.hoglake_auth(
        {
            "HOGLAKE_AUTH_TOKEN_URL": "http://issuer.test/oauth/token",
            "HOGLAKE_AUTH_CLIENT_ID": "hlc_abc",
            "HOGLAKE_AUTH_CLIENT_SECRET": "s3cret",
        }
    )
    assert isinstance(auth, ClientCredentials)
    flow = auth.auth_flow(httpx.Request("GET", "http://hog.test/v1/catalogs"))
    req = next(flow)
    assert req.headers["Authorization"] == "Bearer eyJ.minted"
    (mint,) = mints
    assert mint["url"] == "http://issuer.test/oauth/token"
    assert mint["auth"] == ("hlc_abc", "s3cret")
    assert mint["data"] == {"grant_type": "client_credentials", "audience": "hoglake"}
    # Cached: a second request mints nothing.
    next(auth.auth_flow(httpx.Request("GET", "http://hog.test/v1/catalogs")))
    assert len(mints) == 1


def test_hoglake_auth_audience_is_configurable(monkeypatch: pytest.MonkeyPatch):
    mints: list[dict[str, Any]] = []

    def fake_post(url: str, **kwargs: Any) -> httpx.Response:
        mints.append(kwargs)
        return httpx.Response(200, json={"access_token": "t", "expires_in": 60})

    monkeypatch.setattr(httpx, "post", fake_post)
    auth = main.hoglake_auth(
        {
            "HOGLAKE_AUTH_TOKEN_URL": "http://issuer.test/oauth/token",
            "HOGLAKE_AUTH_CLIENT_ID": "id",
            "HOGLAKE_AUTH_CLIENT_SECRET": "secret",
            "HOGLAKE_AUTH_AUDIENCE": "hoglake-staging",
        }
    )
    assert auth is not None
    next(auth.auth_flow(httpx.Request("GET", "http://hog.test/v1/catalogs")))
    assert mints[0]["data"]["audience"] == "hoglake-staging"


def test_hoglake_auth_refuses_partial_client_credentials():
    from millrace.config import ConfigError

    with pytest.raises(ConfigError, match="missing HOGLAKE_AUTH_CLIENT_SECRET"):
        main.hoglake_auth(
            {
                "HOGLAKE_AUTH_TOKEN_URL": "http://issuer.test/oauth/token",
                "HOGLAKE_AUTH_CLIENT_ID": "id",
            }
        )


def test_hoglake_auth_refuses_two_sources():
    """Two credentials is a mistake to surface, not a precedence to apply:
    a leftover variable must never silently choose what millrace sends."""
    from millrace.config import ConfigError

    with pytest.raises(ConfigError, match="exactly one of"):
        main.hoglake_auth(
            {"HOGLAKE_AUTH_TOKEN": "t", "HOGLAKE_AUTH_TOKEN_FILE": "/nonexistent"}
        )


def test_main_maps_a_bad_credential_configuration_to_the_config_exit(
    capsys: pytest.CaptureFixture[str],
):
    rc = main._main(
        _valid_env()
        | {
            "HOGLAKE_URL": "http://hog.test",
            "HOGLAKE_AUTH_TOKEN": "t",
            "HOGLAKE_AUTH_TOKEN_FILE": "/x",
        }
    )
    assert rc == main.EXIT_CONFIG
    assert "exactly one of" in capsys.readouterr().err


def test_main_hands_the_credential_to_the_hoglake_client(
    monkeypatch: pytest.MonkeyPatch,
):
    """The credential built from the environment is the one the client
    sends: _run threads it into HoglakeClient(auth=...)."""
    import pyhoglake

    seen: dict[str, Any] = {}
    real = pyhoglake.HoglakeClient

    class Capturing(real):  # type: ignore[misc,valid-type]
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            seen["auth"] = kwargs.get("auth")
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(pyhoglake, "HoglakeClient", Capturing)

    def stop(client: Any, cfg: Any) -> None:
        raise RuntimeError("stop here")

    monkeypatch.setattr(main, "ensure_destination", stop)
    rc = main._main(
        _valid_env() | {"HOGLAKE_URL": "http://hog.test", "HOGLAKE_AUTH_TOKEN": "eyJ.t"}
    )
    assert rc == main.EXIT_ERROR
    from pyhoglake.auth import BearerToken

    assert isinstance(seen["auth"], BearerToken)
