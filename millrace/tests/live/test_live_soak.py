"""The soak: two real millrace processes under steady load for
``MILLRACE_SOAK_S`` seconds, one of them ``kill -9``'d and restarted
halfway.

DESELECTED unless ``MILLRACE_SOAK_S`` names a duration (tests/live/
conftest.py — deselected, not skipped: the harness fails on a skip, and
the CI suite never runs a soak). Run it through the live harness with
the soak selected (README §live suite)::

    MILLRACE_SOAK_S=1800 MILLRACE_LIVE_PYTEST_ARGS="-k soak -s" just millrace live

Shape: the flat ``events`` table and the nested table of
test_live_nested.py (spec ``identity(properties.team_id),
month(properties.ts)``, sort ``properties.ts``), each fed by its own
4-partition topic through its own ``python -m millrace.main`` child (one
process = one topic → one table), static assignment of every partition.
A sleep-paced producer per topic sends ~``RATE`` events/s across six
teams and two months — payload time running backwards within a team
part of the time — with ~1 % malformed records (non-JSON values and
tombstones) and a monotone ``seq``. Halfway, the events child is
SIGKILLed and restarted on the same group and stage paths.

Every ``SCRAPE_EVERY_S`` the children's ``/metrics`` (+ probes + RSS)
land as a row in ``soak-metrics.csv``; every WARNING/ERROR line the
children print lands in ``soak-warnings.log``; the end-of-run tables in
``soak-summary.md`` (all under ``$HOGLAKE_LIVE_MILLRACE_REPORT_DIR``,
default ``.artifacts/live-millrace/``).

Drop accounting across the kill (the counters are per process and the
restart replays from the group's committed offsets, which INCLUDE
dropped records — consumer.py commits the last handled offset):

- the survivor (nested) child drops EXACTLY the malformed records, per
  reason;
- the restarted child drops EXACTLY the malformed records at an offset
  ≥ the group's committed offset at the kill (read after the SIGKILL, so
  stable), per reason;
- the killed child's last scrape (taken immediately before the SIGKILL)
  cannot exceed the malformed records produced before that scrape;
- ``killed + restarted − produced`` is the replay double-count: the
  malformed records in [committed offset, killed child's position) the
  first process had already counted. It is reported, and is a LOWER
  bound — drops the killed child made between its last scrape and the
  SIGKILL are unobservable.
"""

from __future__ import annotations

import csv
import os
import random
import re
import signal
import threading
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import livekit
import pytest
from livekit import MetricSamples, metric, metric_by_label, now_us

pytestmark = pytest.mark.live

SOAK_ENV = "MILLRACE_SOAK_S"
PARTITIONS = 4
RATE = 200.0  # events/s per topic
TEAMS = (3, 7, 11, 19, 42, 64)
DAY_US = 86_400 * livekit.MICROS_PER_SECOND
# The second month: 20 days back is, from any date, in a different month
# half the time — so the run spans
# two months whatever day it runs (≥ 2 distinct months either way).
MONTH_SHIFT_US = 20 * DAY_US
LATE_SHARE = 0.3  # payload time steps BACK within a team this often
LATE_MAX_S = 900
MALFORMED_SHARE = 0.01  # half non-JSON values, half tombstones
SCRAPE_EVERY_S = 15.0
FLUSH_DEADLINE_S = 30
DRAIN_TIMEOUT_S = 4 * FLUSH_DEADLINE_S + 240

DROP_REASONS = (
    "malformed_value",
    "missing_value",
    "malformed_partition_source",
    "invalid_offset",
)
_LOG_LEVEL = re.compile(r"\b(WARNING|WARN|ERROR|CRITICAL)\b")
_LOG_NORMALIZE = re.compile(r"\d+")

C_CONSUMED = "millrace_messages_consumed_total"
C_DROPPED = "millrace_records_dropped_total"
C_COMMITS = "millrace_flushes_committed_total"
C_FAILURES = "millrace_flush_failures_total"
C_ORPHANED = "millrace_flush_orphaned_uploads_total"
G_STUCK = "millrace_flush_groups_stuck"
G_BYTES = "millrace_staged_bytes"
G_ROWS = "millrace_staged_rows"
G_GROUPS = "millrace_staged_groups"
G_OLDEST = "millrace_oldest_eligible_staged_age_seconds"

CSV_COLUMNS = [
    "timestamp",
    "phase",
    "child",
    "consumed",
    *(f"dropped_{r}" for r in DROP_REASONS),
    "dropped_other",
    "staged_bytes",
    "staged_rows",
    "staged_groups",
    "oldest_eligible_age_s",
    "flushes_committed",
    "flush_failures",
    "groups_stuck",
    "orphaned_uploads",
    "rss_bytes",
    "healthz",
    "readyz",
    "scrape_error",
]


def _report_dir() -> Path:
    raw = os.environ.get("HOGLAKE_LIVE_MILLRACE_REPORT_DIR", "").strip()
    path = (
        Path(raw)
        if raw
        else Path(__file__).resolve().parents[3] / ".artifacts" / "live-millrace"
    )
    path.mkdir(parents=True, exist_ok=True)
    return path


# ---------------------------------------------------------------------------
# The paced producer (one thread per topic: EventProducer is not
# thread-safe, so each thread owns its own)
# ---------------------------------------------------------------------------


class Pacer(threading.Thread):
    """Sends ``RATE`` events/s on a sleep-based schedule: event ``n`` is
    due at ``start + n / RATE``; each wake sends what is due. ``max_lag``
    records the worst backlog a wake found (a stalled producer shows up
    there, not as a silently lower rate)."""

    def __init__(self, topic: str, *, nested: bool, seed: int) -> None:
        super().__init__(daemon=True, name=f"soak-pacer-{topic}")
        self.producer = livekit.EventProducer(topic)
        self.nested = nested
        self.rng = random.Random(seed)
        self.malformed: dict[int, str] = {}  # seq → expected drop reason
        self.sent = 0
        self.max_lag = 0
        self.error: BaseException | None = None
        self.started_at = 0.0
        self.stopped_at = 0.0
        self._halt = threading.Event()

    def run(self) -> None:
        try:
            self.started_at = time.monotonic()
            while not self._halt.is_set():
                due = int((time.monotonic() - self.started_at) * RATE)
                self.max_lag = max(self.max_lag, due - self.sent)
                while self.sent < due:
                    self._send(self.sent)
                    self.sent += 1
                time.sleep(0.005)
        except BaseException as e:  # noqa: BLE001 - surfaced by the test loop
            self.error = e
        finally:
            self.stopped_at = time.monotonic()

    def _send(self, seq: int) -> None:
        now = now_us()
        team = TEAMS[self.rng.randrange(len(TEAMS))]
        roll = self.rng.random()
        if roll < MALFORMED_SHARE / 2:
            self.malformed[seq] = "malformed_value"
            self.producer.send(
                team, now, seq, record_ts_us=now, value=f"not-json:{seq}".encode()
            )
            return
        if roll < MALFORMED_SHARE:
            self.malformed[seq] = "missing_value"
            self.producer.send(team, now, seq, record_ts_us=now, tombstone=True)
            return
        ts = now
        if self.rng.random() < 0.5:
            ts -= MONTH_SHIFT_US
        if self.rng.random() < LATE_SHARE:
            ts -= int(self.rng.uniform(1, LATE_MAX_S) * livekit.MICROS_PER_SECOND)
        # The RECORD timestamp stays "now" (the nested test's rule: the
        # broker's retention reads it); the PAYLOAD carries the event time.
        if self.nested:
            self.producer.send(
                team,
                ts,
                seq,
                record_ts_us=now,
                value=livekit.nested_payload(team, ts, seq),
            )
        else:
            self.producer.send(team, ts, seq, record_ts_us=now)

    def stop(self) -> list[livekit.ProducedEvent]:
        self._halt.set()
        self.join(timeout=60)
        assert not self.is_alive(), f"{self.name} did not stop"
        if self.error is not None:
            raise self.error
        return self.producer.flush(timeout_s=120)


# ---------------------------------------------------------------------------
# One child incarnation and its observations
# ---------------------------------------------------------------------------


@dataclass
class Incarnation:
    name: str
    child: livekit.Child
    port: int
    ready_at: float = 0.0
    last: MetricSamples | None = None
    prekill: MetricSamples | None = None
    max_staged_bytes: float = 0.0
    max_oldest_s: float = 0.0
    rss_min: int | None = None
    rss_max: int | None = None
    scrapes: int = 0
    scrape_errors: int = 0
    gauges_absent: int = 0
    tail: int = 0

    def counter(self, samples: MetricSamples | None, name: str) -> float:
        if samples is None:
            return 0.0
        return metric(samples, name) or 0.0

    def drops(self, samples: MetricSamples | None) -> Counter[str]:
        if samples is None:
            return Counter()
        return Counter(
            {
                r: int(v)
                for r, v in metric_by_label(samples, C_DROPPED, "reason").items()
            }
        )


@dataclass
class Stream:
    kind: str  # "events" | "nested"
    topic: Any
    table: Any
    cfg: Any
    pacer: Pacer
    incarnations: list[Incarnation] = field(default_factory=list)
    produced: list[livekit.ProducedEvent] = field(default_factory=list)
    committed_at_kill: dict[int, int] | None = None
    sent_before_kill_scrape: int | None = None

    @property
    def current(self) -> Incarnation:
        return self.incarnations[-1]


def _start(stream: Stream, name: str) -> Incarnation:
    port = livekit.free_port()
    child = livekit.Child(livekit.child_env(stream.cfg, metrics_port=port), name=name)
    child.start()
    inc = Incarnation(name=name, child=child, port=port)
    stream.incarnations.append(inc)
    child.wait_ready(port, timeout_s=120)
    inc.ready_at = time.monotonic()
    return inc


# ---------------------------------------------------------------------------
# The report files
# ---------------------------------------------------------------------------


class Report:
    def __init__(self, directory: Path) -> None:
        self.dir = directory
        self.csv_path = directory / "soak-metrics.csv"
        self.warnings_path = directory / "soak-warnings.log"
        self.summary_path = directory / "soak-summary.md"
        with self.csv_path.open("w", newline="") as f:
            csv.writer(f).writerow(CSV_COLUMNS)
        self.warnings_path.write_text("")
        self.warning_classes: Counter[tuple[str, str]] = Counter()

    def row(self, values: dict[str, Any]) -> None:
        with self.csv_path.open("a", newline="") as f:
            csv.writer(f).writerow(
                ["" if values.get(c) is None else values[c] for c in CSV_COLUMNS]
            )

    def tail_warnings(self, inc: Incarnation) -> None:
        lines, inc.tail = inc.child.lines_since(inc.tail)
        hits = [line for line in lines if _LOG_LEVEL.search(line)]
        if not hits:
            return
        stamp = time.strftime("%Y-%m-%dT%H:%M:%S%z")
        with self.warnings_path.open("a") as f:
            for line in hits:
                f.write(f"{stamp} [{inc.name}] {line}\n")
                self.warning_classes[
                    (inc.name.split("-")[0], _warning_class(line))
                ] += 1


def _warning_class(line: str) -> str:
    """A log line's class: the level and message with the timestamp
    prefix stripped and every number masked (dedupe key)."""
    match = _LOG_LEVEL.search(line)
    body = line[match.start() :] if match else line
    return _LOG_NORMALIZE.sub("N", body)[:200]


def _scrape(report: Report, inc: Incarnation, phase: str) -> MetricSamples | None:
    samples: MetricSamples | None
    error = ""
    try:
        samples = livekit.scrape_metrics(inc.port)
    except Exception as e:  # noqa: BLE001 - recorded in the row
        samples = None
        error = repr(e)[:120]
    healthz = livekit.probe_status(inc.port, "/healthz")
    readyz = livekit.probe_status(inc.port, "/readyz")
    rss = (
        livekit.process_rss_bytes(inc.child.proc.pid)
        if inc.child.proc is not None and inc.child.proc.poll() is None
        else None
    )
    inc.scrapes += 1
    if rss is not None:
        inc.rss_min = rss if inc.rss_min is None else min(inc.rss_min, rss)
        inc.rss_max = rss if inc.rss_max is None else max(inc.rss_max, rss)
    values: dict[str, Any] = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "phase": phase,
        "child": inc.name,
        "rss_bytes": rss,
        "healthz": healthz,
        "readyz": readyz,
        "scrape_error": error,
    }
    if samples is None:
        inc.scrape_errors += 1
    else:
        inc.last = samples
        drops = inc.drops(samples)
        staged_bytes = metric(samples, G_BYTES)
        oldest = metric(samples, G_OLDEST)
        if staged_bytes is None:
            inc.gauges_absent += 1
        inc.max_staged_bytes = max(inc.max_staged_bytes, staged_bytes or 0.0)
        inc.max_oldest_s = max(inc.max_oldest_s, oldest or 0.0)
        values.update(
            {
                "consumed": int(inc.counter(samples, C_CONSUMED)),
                **{f"dropped_{r}": drops.get(r, 0) for r in DROP_REASONS},
                "dropped_other": sum(
                    v for r, v in drops.items() if r not in DROP_REASONS
                ),
                "staged_bytes": staged_bytes,
                "staged_rows": metric(samples, G_ROWS),
                "staged_groups": metric(samples, G_GROUPS),
                "oldest_eligible_age_s": oldest,
                "flushes_committed": int(inc.counter(samples, C_COMMITS)),
                "flush_failures": int(inc.counter(samples, C_FAILURES)),
                "groups_stuck": metric(samples, G_STUCK),
                "orphaned_uploads": int(inc.counter(samples, C_ORPHANED)),
            }
        )
    report.row(values)
    return samples


def _drained(samples: MetricSamples | None) -> bool:
    return (
        samples is not None
        and metric(samples, G_ROWS) == 0
        and metric(samples, G_OLDEST) is None
    )


# ---------------------------------------------------------------------------
# The soak
# ---------------------------------------------------------------------------


def test_soak(lake_home, topic_factory, admin):
    soak_s = float(os.environ.get(SOAK_ENV, "0") or 0)
    if soak_s <= 0:  # the conftest deselects this case; never a skip
        raise AssertionError(f"{SOAK_ENV} must be a positive number of seconds")
    report = Report(_report_dir())

    streams: list[Stream] = []
    for kind, seed in (("events", 1), ("nested", 2)):
        table = (
            lake_home.create_events_table()
            if kind == "events"
            else lake_home.create_nested_events_table()
        )
        topic = topic_factory(partitions=PARTITIONS)
        cfg = livekit.live_config(
            topic=topic.name,
            group=topic.group,
            stage_base_path=f"stage/{livekit.RUN_ID}/{lake_home.slug}",
            catalog=lake_home.catalog.name,
            namespace="ns1",
            table=table.name,
            flush_deadline_s=FLUSH_DEADLINE_S,
            # Children tune only through env and the age lane's 1 MiB
            # floor is a constant: slow == age makes every group due at
            # the deadline.
            slow_lane_deadline_s=FLUSH_DEADLINE_S,
        )
        streams.append(
            Stream(
                kind=kind,
                topic=topic,
                table=table,
                cfg=cfg,
                pacer=Pacer(topic.name, nested=kind == "nested", seed=seed),
            )
        )
    events = streams[0]
    slow_lane_s = float(events.cfg.slow_lane_deadline_s)

    failures: list[str] = []
    try:
        for stream in streams:
            _start(stream, f"{stream.kind}-a")
        for stream in streams:
            stream.pacer.start()

        t0 = time.monotonic()
        end = t0 + soak_s
        kill_at = t0 + soak_s / 2
        next_scrape = t0
        killed = False
        while time.monotonic() < end:
            for stream in streams:
                if stream.pacer.error is not None:
                    raise stream.pacer.error
                report.tail_warnings(stream.current)
                rc = stream.current.child.proc.poll()  # type: ignore[union-attr]
                if rc is not None:
                    raise AssertionError(
                        f"child {stream.current.name} exited {rc} mid-soak; tail:\n"
                        + stream.current.child.output()[-4000:]
                    )
            if not killed and time.monotonic() >= kill_at:
                killed = True
                victim = events.current
                # The last observation of the doomed process, then SIGKILL
                # at once (the window between them is the unobservable tail).
                events.sent_before_kill_scrape = events.pacer.sent
                victim.prekill = _scrape(report, victim, "prekill")
                victim.child.kill()
                assert victim.child.wait_exit() == -signal.SIGKILL
                report.tail_warnings(victim)
                # Dead process → the group's committed offsets are stable:
                # the restart resumes exactly here.
                events.committed_at_kill = livekit.committed_offsets(
                    admin, events.topic.group, events.topic.name
                )
                print(
                    f"[soak] t={time.monotonic() - t0:.0f}s SIGKILL {victim.name}; "
                    f"committed at kill {events.committed_at_kill}",
                    flush=True,
                )
                _start(events, "events-b")
                print(
                    f"[soak] t={time.monotonic() - t0:.0f}s events-b ready",
                    flush=True,
                )
            if time.monotonic() >= next_scrape:
                next_scrape += SCRAPE_EVERY_S
                parts = []
                for stream in streams:
                    s = _scrape(report, stream.current, "run")
                    parts.append(
                        f"{stream.current.name} consumed="
                        f"{int(stream.current.counter(s, C_CONSUMED))} "
                        f"staged_bytes={metric(s, G_BYTES) if s else None} "
                        f"oldest={metric(s, G_OLDEST) if s else None}"
                    )
                print(
                    f"[soak] t={time.monotonic() - t0:.0f}s "
                    f"produced={[st.pacer.sent for st in streams]} "
                    + " | ".join(parts),
                    flush=True,
                )
            time.sleep(0.5)

        # Stop producing; every send delivered (a delivery error fails).
        for stream in streams:
            stream.produced = stream.pacer.stop()

        # Drain: consumed to the log ends, nothing staged, nothing eligible.
        ends = {
            s.kind: livekit.end_offsets(admin, s.topic.name, range(PARTITIONS))
            for s in streams
        }
        drain_deadline = time.monotonic() + DRAIN_TIMEOUT_S
        drained = False
        while time.monotonic() < drain_deadline:
            states = []
            for stream in streams:
                report.tail_warnings(stream.current)
                samples = _scrape(report, stream.current, "drain")
                committed = livekit.committed_offsets(
                    admin, stream.topic.group, stream.topic.name
                )
                states.append(
                    _drained(samples)
                    and livekit.group_offsets_reached(committed, ends[stream.kind])
                )
            if all(states):
                drained = True
                break
            time.sleep(5)
        if not drained:
            failures.append(
                f"staging did not drain within {DRAIN_TIMEOUT_S}s: "
                + "; ".join(
                    f"{s.current.name} rows={metric(s.current.last, G_ROWS) if s.current.last else None} "
                    f"oldest={metric(s.current.last, G_OLDEST) if s.current.last else None}"
                    for s in streams
                )
            )

        # ---- lake readback ------------------------------------------------
        lake: dict[str, Counter[int]] = {}
        for stream in streams:
            valid = len(stream.produced) - len(stream.pacer.malformed)
            try:
                livekit.wait_until(
                    lambda st=stream, n=valid: (
                        sum(
                            livekit.lake_tuple_counts(
                                st.table, lake_home.head()
                            ).values()
                        )
                        >= n
                    ),
                    120,
                    interval_s=2,
                    desc=f"{valid} {stream.kind} rows queryable",
                )
            except AssertionError as e:
                failures.append(str(e))
            lake[stream.kind] = livekit.lake_seq_counts(
                stream.table, lake_home.head(), lake_home.fs
            )

        # ---- assertions (collected; the summary is written regardless) ---
        rows: dict[str, dict[str, Any]] = {}
        for stream in streams:
            produced_seqs = {e.seq for e in stream.produced}
            malformed = stream.pacer.malformed
            valid_seqs = produced_seqs - set(malformed)
            counts = lake[stream.kind]
            missing = valid_seqs - set(counts)
            unexpected = set(counts) - valid_seqs
            duplicates = sum(c - 1 for c in counts.values() if c > 1)
            if missing:
                failures.append(
                    f"{stream.kind}: {len(missing)} produced seqs never landed "
                    f"(first {sorted(missing)[:10]})"
                )
            if unexpected:
                failures.append(
                    f"{stream.kind}: {len(unexpected)} lake seqs never produced as "
                    f"valid events (first {sorted(unexpected)[:10]})"
                )
            expected_drops = Counter(malformed.values())
            by_seq = {e.seq: e for e in stream.produced}
            final_drops: Counter[str] = Counter()
            consumed = 0
            for inc in stream.incarnations:
                samples = inc.prekill if inc.prekill is not None else inc.last
                final_drops += inc.drops(samples)
                consumed += int(inc.counter(samples, C_CONSUMED))
            double_counted: int | None = None
            if stream.committed_at_kill is None:
                if final_drops != expected_drops:
                    failures.append(
                        f"{stream.kind}: drops {dict(final_drops)} != malformed "
                        f"{dict(expected_drops)}"
                    )
            else:
                a, b = stream.incarnations[0], stream.incarnations[-1]
                committed = {p: max(0, o) for p, o in stream.committed_at_kill.items()}
                replay_expected = Counter(
                    reason
                    for seq, reason in malformed.items()
                    if by_seq[seq].offset >= committed.get(by_seq[seq].partition, 0)
                )
                b_drops = b.drops(b.last)
                if b_drops != replay_expected:
                    failures.append(
                        f"{stream.kind}: restarted child dropped {dict(b_drops)}, "
                        f"expected exactly the malformed records at/after the "
                        f"committed offsets {dict(replay_expected)}"
                    )
                a_bound = Counter(
                    reason
                    for seq, reason in malformed.items()
                    if seq < (stream.sent_before_kill_scrape or 0)
                )
                a_drops = a.drops(a.prekill)
                over = {
                    r: (v, a_bound.get(r, 0))
                    for r, v in a_drops.items()
                    if v > a_bound.get(r, 0)
                }
                if over:
                    failures.append(
                        f"{stream.kind}: killed child counted more drops than "
                        f"malformed records produced before its last scrape: {over}"
                    )
                double_counted = sum(final_drops.values()) - sum(
                    expected_drops.values()
                )
            # No orphans (a killed process counts none of its own) and no
            # stuck groups anywhere.
            for inc in stream.incarnations:
                samples = inc.prekill if inc.prekill is not None else inc.last
                orphaned = inc.counter(samples, C_ORPHANED)
                if orphaned:
                    failures.append(f"{inc.name}: orphaned={orphaned}")
                if samples is not None and (metric(samples, G_STUCK) or 0) > 0:
                    failures.append(f"{inc.name}: stuck flush groups at the end")
            for inc in stream.incarnations:
                if (
                    "HALTED" in inc.child.output()
                    or "FlushHalted" in inc.child.output()
                ):
                    failures.append(f"{inc.name}: FlushHalted in its output")
                # A due group's age keeps growing for the sweep and the
                # flush that serve it: one more deadline of slack.
                if inc.max_oldest_s > slow_lane_s + FLUSH_DEADLINE_S:
                    failures.append(
                        f"{inc.name}: oldest eligible age {inc.max_oldest_s:.0f}s "
                        f"exceeded MILLRACE_SLOW_LANE_DEADLINE_S={slow_lane_s:.0f} "
                        f"+ {FLUSH_DEADLINE_S}s"
                    )
            cur = stream.current
            alive = cur.child.proc is not None and cur.child.proc.poll() is None
            health = livekit.probe_status(cur.port, "/healthz")
            ready = livekit.probe_status(cur.port, "/readyz")
            if not (alive and health == 200 and ready == 200):
                failures.append(
                    f"{cur.name}: alive={alive} healthz={health} readyz={ready} at the end"
                )
            rows[stream.kind] = {
                "produced": len(stream.produced),
                "valid": len(valid_seqs),
                "malformed": dict(expected_drops),
                "consumed": consumed,
                "landed_distinct": len(set(counts) & valid_seqs),
                "landed_rows": sum(counts.values()),
                "duplicates": duplicates,
                "missing": len(missing),
                "dropped": dict(final_drops),
                "double_counted": double_counted,
                "max_lag": stream.pacer.max_lag,
                "rate": len(stream.produced)
                / max(1e-9, stream.pacer.stopped_at - stream.pacer.started_at),
            }

        summary = _summary(streams, rows, report, soak_s, failures)
        report.summary_path.write_text(summary)
        print(summary, flush=True)
        assert not failures, "soak failures:\n" + "\n".join(failures)
    finally:
        for stream in streams:
            if stream.pacer.is_alive():
                stream.pacer._halt.set()
                stream.pacer.join(timeout=30)
            for inc in stream.incarnations:
                proc = inc.child.proc
                if proc is not None and proc.poll() is None:
                    inc.child.terminate()
                    try:
                        inc.child.wait_exit(timeout_s=60)
                    except Exception:  # noqa: BLE001 - teardown is best-effort
                        inc.child.kill()
                report.tail_warnings(inc)
            stream.pacer.producer.close()


def _summary(
    streams: list[Stream],
    rows: dict[str, dict[str, Any]],
    report: Report,
    soak_s: float,
    failures: list[str],
) -> str:
    def mib(v: float | None) -> str:
        return "-" if v is None else f"{v / (1 << 20):.1f}"

    out = [
        (
            f"# millrace soak — {soak_s:.0f}s, {RATE:.0f} ev/s/topic, "
            f"{PARTITIONS} partitions, flush deadline {FLUSH_DEADLINE_S}s"
        ),
        "",
        "## Events per topic",
        "",
        (
            "| topic | produced | valid | rate ev/s | max pacer lag "
            "| consumed (Σ incarnations) | landed distinct | landed rows "
            "| duplicates | missing | malformed produced "
            "| dropped (Σ incarnations) | replay double-count |"
        ),
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for stream in streams:
        r = rows[stream.kind]
        out.append(
            f"| {stream.kind} | {r['produced']} | {r['valid']} | {r['rate']:.1f} "
            f"| {r['max_lag']} | {r['consumed']} | {r['landed_distinct']} "
            f"| {r['landed_rows']} | {r['duplicates']} | {r['missing']} "
            f"| {r['malformed']} | {r['dropped']} "
            f"| {'-' if r['double_counted'] is None else r['double_counted']} |"
        )
    out += [
        "",
        "## Per child incarnation (final scrape; the killed one: its last pre-kill scrape)",
        "",
        (
            "| child | commits | flush failures | orphaned | max staged MiB "
            "| max oldest eligible s | RSS min MiB | RSS max MiB "
            "| scrapes (errors / gauges absent) |"
        ),
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for stream in streams:
        for inc in stream.incarnations:
            s = inc.prekill if inc.prekill is not None else inc.last
            out.append(
                f"| {inc.name} | {int(inc.counter(s, C_COMMITS))} "
                f"| {int(inc.counter(s, C_FAILURES))} "
                f"| {int(inc.counter(s, C_ORPHANED))} | {mib(inc.max_staged_bytes)} "
                f"| {inc.max_oldest_s:.1f} | {mib(inc.rss_min)} | {mib(inc.rss_max)} "
                f"| {inc.scrapes} ({inc.scrape_errors} / {inc.gauges_absent}) |"
            )
    events = streams[0]
    if events.committed_at_kill is not None:
        out += [
            "",
            (
                f"Kill: group committed offsets at the SIGKILL "
                f"{events.committed_at_kill}; seqs sent before the last "
                f"pre-kill scrape: {events.sent_before_kill_scrape}."
            ),
        ]
    out += ["", "## WARNING/ERROR line classes (per stream)", ""]
    if report.warning_classes:
        out += ["| stream | count | class |", "|---|---|---|"]
        for (who, cls), n in report.warning_classes.most_common():
            out.append(f"| {who} | {n} | `{cls.replace('|', '/')}` |")
    else:
        out.append("none")
    out += ["", "## Result", ""]
    out += [f"- FAIL: {f}" for f in failures] or ["- PASS"]
    out += [
        "",
        f"Files: {report.csv_path}, {report.warnings_path}, {report.summary_path}",
        "",
    ]
    return "\n".join(out)
