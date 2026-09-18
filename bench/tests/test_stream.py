"""Unit tests for the continuous event-stream task (no server needed)."""

from __future__ import annotations

import itertools
import signal
from datetime import UTC, datetime

import httpx
import numpy as np
import pyarrow as pa
import pytest
from pyhoglake import transforms
from pyhoglake.errors import (
    CommitConflictError,
    HoglakeError,
    IncarnationChangedError,
    ValidationError,
)

from hoglake_bench.seed import tables as T
from hoglake_bench.seed.vocab import build_vocabulary
from hoglake_bench.stream import events, pacing, streamer
from hoglake_bench.stream.distribution import (
    DEFAULT_WHALE_SHARE,
    TeamDistribution,
)

# -- distribution ------------------------------------------------------------


def test_whale_share_is_honoured_within_tolerance() -> None:
    dist = TeamDistribution.build(teams=200, whale_share=0.45, zipf_s=1.1)
    rng = np.random.default_rng(7)
    idx = dist.sample_indices(rng, 200_000)
    whale = float((idx == 0).mean())
    assert whale == pytest.approx(0.45, abs=0.01)


def test_weights_sum_to_one_and_tail_is_monotone() -> None:
    dist = TeamDistribution.build(teams=50, whale_share=0.4, zipf_s=1.2)
    assert sum(dist.weights) == pytest.approx(1.0)
    assert dist.weights[0] == pytest.approx(0.4)
    tail = dist.weights[1:]
    assert all(a >= b for a, b in itertools.pairwise(tail))
    # the whale outweighs the biggest ordinary team
    assert dist.weights[0] > dist.weights[1] * 2


def test_the_default_shape_is_dominated_by_one_team() -> None:
    dist = TeamDistribution.build()
    assert dist.whale_share == DEFAULT_WHALE_SHARE
    # the whale carries several times the runner-up and hundreds of times
    # an ordinary tail team
    assert dist.weights[0] > dist.weights[1] * 3
    assert dist.weights[0] > dist.weights[-1] * 100
    assert sum(dist.weights[1:]) == pytest.approx(1.0 - DEFAULT_WHALE_SHARE)


def test_sampling_is_deterministic_under_a_seed() -> None:
    dist = TeamDistribution.build(teams=64)
    a = dist.sample(np.random.default_rng(1234), 5_000)
    b = dist.sample(np.random.default_rng(1234), 5_000)
    assert np.array_equal(a, b)
    assert not np.array_equal(a, dist.sample(np.random.default_rng(1235), 5_000))


def test_a_whale_is_a_whale_across_windows() -> None:
    """Per-team rates are stable: the same team dominates every window."""
    dist = TeamDistribution.build(teams=120, whale_share=0.5)
    rng = np.random.default_rng(99)
    for _ in range(6):
        idx = dist.sample_indices(rng, 40_000)
        counts = np.bincount(idx, minlength=len(dist.team_ids))
        assert int(counts.argmax()) == 0
        assert dist.ids(np.array([0]))[0] == dist.whale_id


def test_single_team_distribution_is_degenerate_but_legal() -> None:
    dist = TeamDistribution.build(teams=1)
    assert dist.weights == (1.0,)
    assert set(dist.sample(np.random.default_rng(0), 100)) == {dist.whale_id}


@pytest.mark.parametrize(
    "kwargs",
    [
        {"teams": 0},
        {"whale_share": 1.0},
        {"whale_share": -0.1},
        {"zipf_s": 0.0},
    ],
)
def test_bad_distribution_parameters_are_refused(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        TeamDistribution.build(**kwargs)  # type: ignore[arg-type]


# -- batch shape / sorting ---------------------------------------------------


def test_schema_reuses_the_seed_pageviews_columns_plus_properties() -> None:
    assert events.STREAM_SCHEMA.names == [
        *T.PAGEVIEWS_SCHEMA.names,
        "properties",
    ]
    assert events.STREAM_SCHEMA.field("properties").type == pa.string()
    # properties-style blobs stay TEXT; variant is deliberately out of scope
    assert all(
        events.STREAM_SCHEMA.field(i).type != "variant"
        for i in range(len(events.STREAM_SCHEMA.names))
    )


def test_batches_are_sorted_to_match_the_declared_sort_order() -> None:
    dist = TeamDistribution.build(teams=40)
    rng = np.random.default_rng(3)
    batch, _ = events.make_batch(
        rng,
        build_vocabulary(11),
        4_000,
        teams=dist,
        lo_us=1_700_000_000_000_000,
        hi_us=1_700_003_600_000_000,
    )
    sorted_batch = events.sort_batch(batch)
    assert events.SORT_COLUMNS == ("team_id", "ts")
    keys = list(
        zip(
            sorted_batch.column("team_id").to_pylist(),
            sorted_batch.column("ts").to_pylist(),
        )
    )
    assert keys == sorted(keys)
    assert sorted_batch.num_rows == batch.num_rows


def test_batch_carries_the_skew_and_the_requested_time_window() -> None:
    dist = TeamDistribution.build(teams=30, whale_share=0.6)
    lo, hi = 1_700_000_000_000_000, 1_700_003_600_000_000
    batch, idx = events.make_batch(
        np.random.default_rng(5),
        build_vocabulary(11),
        6_000,
        teams=dist,
        lo_us=lo,
        hi_us=hi,
    )
    assert len(idx) == batch.num_rows
    assert float((idx == 0).mean()) == pytest.approx(0.6, abs=0.03)
    micros = [int(v.value) for v in batch.column("ts")]
    assert min(micros) >= lo
    assert max(micros) < hi
    assert set(batch.column("team_id").to_pylist()) <= set(dist.team_ids)


# -- hour partitioning -------------------------------------------------------


@pytest.mark.parametrize(
    "iso",
    [
        "2026-09-18T14:31:07+00:00",
        "2026-09-18T00:00:00+00:00",
        "1970-01-01T00:00:00+00:00",
        "1969-12-31T23:59:59+00:00",  # pre-epoch: floors, never truncates
    ],
)
def test_epoch_hour_agrees_with_the_client_transform(iso: str) -> None:
    """Parity against pyhoglake's own `hour`, not a restated constant."""
    when = datetime.fromisoformat(iso)
    micros = int(when.timestamp() * 1_000_000)
    assert pacing.epoch_hour(micros) == transforms.hour(when)


def test_hour_label_is_the_partition_cell_a_human_can_read() -> None:
    micros = int(datetime(2026, 9, 18, 14, 31, 7, tzinfo=UTC).timestamp() * 1e6)
    assert pacing.hour_label(micros) == "2026-09-18T14Z"


# -- the clock ---------------------------------------------------------------


def test_clock_tracks_wall_time_by_default() -> None:
    fake = [100.0]
    clock = pacing.StreamClock(1_000_000_000_000_000, now=lambda: fake[0])
    fake[0] = 130.0
    assert clock.now_us() == 1_000_000_000_000_000 + 30_000_000


def test_time_compression_rolls_hours_over_fast() -> None:
    fake = [0.0]
    # 6 simulated hours per real minute
    clock = pacing.StreamClock(
        0, speedup=pacing.speedup_for_hours_per_minute(6.0), now=lambda: fake[0]
    )
    fake[0] = 60.0
    assert pacing.epoch_hour(clock.now_us()) == 6


def test_advance_window_is_monotonic_and_never_empty() -> None:
    fake = [0.0]
    clock = pacing.StreamClock(500, now=lambda: fake[0])
    lo, hi = clock.advance_window()
    assert lo == 500 and hi > lo
    fake[0] = 1.0
    lo2, hi2 = clock.advance_window()
    assert lo2 == hi and hi2 > lo2


# -- flush policy ------------------------------------------------------------


def test_flush_triggers_on_bytes_first() -> None:
    policy = pacing.FlushPolicy(max_bytes=1_000, max_seconds=60.0)
    assert policy.trigger(pending_bytes=999, pending_rows=10, age_s=1.0) is None
    assert policy.trigger(pending_bytes=1_000, pending_rows=10, age_s=1.0) == "bytes"


def test_flush_triggers_on_time_when_bytes_are_short() -> None:
    policy = pacing.FlushPolicy(max_bytes=10**9, max_seconds=5.0)
    assert policy.trigger(pending_bytes=10, pending_rows=1, age_s=4.9) is None
    assert policy.trigger(pending_bytes=10, pending_rows=1, age_s=5.0) == "time"


def test_an_empty_buffer_never_flushes() -> None:
    """A partitioned append of 0 rows is a client-side 422; never issue one."""
    policy = pacing.FlushPolicy(max_bytes=1, max_seconds=0.0)
    assert policy.trigger(pending_bytes=0, pending_rows=0, age_s=999.0) is None


# -- rate limiter ------------------------------------------------------------


class _FakeClock:
    def __init__(self) -> None:
        self.t = 0.0
        self.sleeps: list[float] = []

    def now(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.t += seconds


def test_rate_limiter_paces_to_its_target_without_busy_waiting() -> None:
    clock = _FakeClock()
    limiter = pacing.RateLimiter(1_000.0, now=clock.now, sleep=clock.sleep)
    for _ in range(10):
        limiter.acquire(100)
    # 1000 events at 1000/s == 1.0s of simulated time
    assert clock.t == pytest.approx(1.0, abs=1e-9)
    # every wait was a real sleep, never a zero-length spin
    assert clock.sleeps
    assert all(s > 0 for s in clock.sleeps)
    # and each sleep is chunked, not a spin: bounded iteration count
    assert len(clock.sleeps) <= 10 * (0.1 / limiter.max_sleep_s + 1)


def test_rate_limiter_is_a_no_op_when_unlimited() -> None:
    clock = _FakeClock()
    limiter = pacing.RateLimiter(0.0, now=clock.now, sleep=clock.sleep)
    assert limiter.acquire(10_000) == 0.0
    assert clock.sleeps == []


def test_rate_limiter_gives_up_its_wait_when_asked_to_stop() -> None:
    clock = _FakeClock()
    limiter = pacing.RateLimiter(1.0, now=clock.now, sleep=clock.sleep)
    slept = limiter.acquire(100, should_stop=lambda: True)
    assert slept == 0.0
    assert clock.sleeps == []


def test_rate_limiter_does_not_bank_unused_credit() -> None:
    """A slow producer must not earn a burst allowance it can spend later."""
    clock = _FakeClock()
    limiter = pacing.RateLimiter(100.0, now=clock.now, sleep=clock.sleep)
    limiter.acquire(10)
    clock.t += 60.0  # a long stall (a slow flush, say)
    clock.sleeps.clear()
    limiter.acquire(10)
    assert clock.sleeps == []  # no wait owed...
    limiter.acquire(10)
    assert clock.sleeps  # ...but the next batch is paced again


# -- signal handling ---------------------------------------------------------


def test_first_signal_asks_for_a_clean_stop() -> None:
    lines: list[str] = []
    exits: list[int] = []
    stop = streamer.StopController(emit=lines.append, immediate=exits.append)
    assert not stop.stopping
    stop.handle(signal.SIGINT)
    assert stop.stopping
    assert stop.first_signal == signal.SIGINT
    assert exits == []
    assert any("flush" in line for line in lines)


def test_second_signal_exits_immediately() -> None:
    exits: list[int] = []
    stop = streamer.StopController(emit=lambda _: None, immediate=exits.append)
    stop.handle(signal.SIGINT)
    stop.handle(signal.SIGINT)
    assert exits == [streamer.IMMEDIATE_EXIT_CODE]


def test_sigterm_is_a_clean_stop_too() -> None:
    stop = streamer.StopController(emit=lambda _: None, immediate=lambda _: None)
    stop.handle(signal.SIGTERM)
    assert stop.stopping and stop.first_signal == signal.SIGTERM


def test_install_and_restore_round_trip() -> None:
    stop = streamer.StopController(emit=lambda _: None, immediate=lambda _: None)
    before = signal.getsignal(signal.SIGINT)
    stop.install()
    try:
        assert signal.getsignal(signal.SIGINT) == stop.handle
    finally:
        stop.restore()
    assert signal.getsignal(signal.SIGINT) is before


def test_a_signalled_stop_discards_the_buffer_and_summarises_cleanly() -> None:
    """SIGINT mid-run: the in-flight flush finishes, the remainder is
    dropped, the summary is printed, and the exit path is the clean one."""
    totals = streamer.StreamTotals(team_count=3)
    totals.record_flush(
        reason="bytes",
        rows=1_000,
        arrow_bytes=2_000,
        files=1,
        team_counts=[600, 300, 100],
    )
    lines: list[str] = []
    stop = streamer.StopController(emit=lines.append, immediate=lambda _: None)
    stop.handle(signal.SIGINT)
    totals.dropped_events = 250
    text = "\n".join(
        streamer.summary_lines(
            totals,
            TeamDistribution.build(teams=3),
            elapsed_s=10.0,
            stopped_by="SIGINT",
        )
    )
    assert "1,000" in text
    assert "SIGINT" in text
    assert "250" in text and "dropped" in text


# -- failure classification --------------------------------------------------


@pytest.mark.parametrize(
    "exc",
    [
        HoglakeError("admission timeout", status_code=503),
        HoglakeError("bad gateway", status_code=502),
        HoglakeError("transport", status_code=None),
        CommitConflictError("occ", status_code=409),
        httpx.ReadTimeout("timed out"),
        httpx.ConnectError("refused"),
    ],
)
def test_transient_failures_are_retryable(exc: BaseException) -> None:
    assert streamer.classify_failure(exc) == "retry"


@pytest.mark.parametrize(
    "exc",
    [
        ValidationError("bad stats", status_code=422),
        IncarnationChangedError("recreated", status_code=409),
        ValueError("harness bug"),
    ],
)
def test_permanent_failures_stop_the_run(exc: BaseException) -> None:
    assert streamer.classify_failure(exc) == "fatal"


def test_retrying_flush_backs_off_and_eventually_succeeds() -> None:
    attempts: list[int] = []
    sleeps: list[float] = []

    def flush() -> str:
        attempts.append(len(attempts))
        if len(attempts) < 3:
            raise HoglakeError("admission", status_code=503)
        return "ok"

    totals = streamer.StreamTotals(team_count=1)
    out = streamer.with_retries(
        flush, totals=totals, max_retries=5, sleep=sleeps.append, jitter=lambda: 0.0
    )
    assert out == "ok"
    assert totals.retries == 2
    assert sleeps == sorted(sleeps) and len(sleeps) == 2
    assert sleeps[1] > sleeps[0]  # exponential


def test_a_422_stops_the_run_with_a_clear_message() -> None:
    def flush() -> str:
        raise ValidationError("column_stats for an unknown field id", status_code=422)

    totals = streamer.StreamTotals(team_count=1)
    with pytest.raises(streamer.BenchAbort) as excinfo:
        streamer.with_retries(flush, totals=totals, max_retries=5, sleep=lambda _: None)
    assert "422" in str(excinfo.value) or "permanent" in str(excinfo.value)
    assert totals.retries == 0


def test_retries_are_bounded() -> None:
    def flush() -> str:
        raise HoglakeError("still 503", status_code=503)

    totals = streamer.StreamTotals(team_count=1)
    with pytest.raises(streamer.BenchAbort):
        streamer.with_retries(
            flush,
            totals=totals,
            max_retries=3,
            sleep=lambda _: None,
            jitter=lambda: 0.0,
        )
    assert totals.retries == 3
    assert totals.flush_failures == 1


# -- totals / progress -------------------------------------------------------


def test_totals_accumulate_per_team_and_by_flush_reason() -> None:
    totals = streamer.StreamTotals(team_count=3)
    totals.record_flush(
        reason="bytes", rows=100, arrow_bytes=400, files=1, team_counts=[70, 20, 10]
    )
    totals.record_flush(
        reason="time", rows=50, arrow_bytes=200, files=2, team_counts=[30, 15, 5]
    )
    assert totals.events == 150
    assert totals.arrow_bytes == 600
    assert totals.files == 3
    assert totals.flushes == 2
    assert totals.reasons == {"bytes": 1, "time": 1}
    assert list(totals.per_team) == [100, 35, 15]


def test_progress_line_is_parseable_and_names_the_open_hour() -> None:
    totals = streamer.StreamTotals(team_count=2)
    totals.record_flush(
        reason="bytes", rows=1_000, arrow_bytes=5_000, files=1, team_counts=[900, 100]
    )
    line = streamer.progress_line(
        totals,
        elapsed_s=10.0,
        hour=pacing.hour_label(1_700_000_000_000_000),
        buffered_rows=17,
        buffered_bytes=99,
    )
    tokens = dict(
        token.split("=", 1)
        for token in line.split()
        if "=" in token and not token.startswith("(")
    )
    assert tokens["events"] == "1,000"
    assert tokens["files"] == "1"
    assert tokens["retries"] == "0"
    assert tokens["hour"] == "2023-11-14T22Z"
    assert "17" in line


# -- CLI wiring --------------------------------------------------------------


def test_stream_is_a_subcommand_but_never_part_of_all() -> None:
    from hoglake_bench.cli import SCENARIOS, build_parser

    assert "stream" not in SCENARIOS  # `all` must never launch a forever-loop
    args = build_parser().parse_args(["stream", "--rate", "500", "--max-events", "10"])
    assert args.scenario == "stream"
    assert args.rate == 500.0
    assert args.max_events == 10
    assert args.flush_mb == streamer.DEFAULT_FLUSH_MB
    assert args.flush_seconds == streamer.DEFAULT_FLUSH_SECONDS


def test_rate_and_rate_mb_s_are_mutually_exclusive() -> None:
    from hoglake_bench.cli import build_parser

    with pytest.raises(SystemExit):
        build_parser().parse_args(["stream", "--rate", "10", "--rate-mb-s", "1"])


def test_stream_declares_an_end_to_end_io_mode() -> None:
    assert streamer.IO_MODE == "end-to-end"
