"""Planner replay: a synthetic skewed arrival stream over a simulated day,
run through the OLD (two-lane, size-on-raw-bytes) and the NEW (three-lane,
compression-aware) policy on the same seeded stream.

Design validation item 4 (docs/kafka-ingestion.md §Validation plan),
landed as a unit suite. Only ``random.Random.random()`` feeds the model —
the one method whose sequence Python guarantees stable across versions
for a fixed seed — so the stream is reproducible in any process.

Model
-----

- Zipf cohort: team k (0-based) arrives at ``C/(k+1)`` bytes/s (Zipf
  α=1), in lumps of ``clamp(2 * rate, 1 MB, 4 MB)`` bytes spaced
  lump/rate seconds apart, jittered x[0.5, 1.5). One lump is always
  >= 0.5 MB, which clears the age lane's floor — so lumpy teams, even
  slow ones, flush one deadline after each lump under BOTH policies.
  They are the control cohort: the lane change must not move them.
- Trickle cohort: N_TRICKLE teams at a flat TRICKLE_RATE bytes/s, one
  arrival per tick — the design doc's "median tenant contributes
  sub-row counts per flush window" shape (~9 rows per 15-minute
  window). The rate is chosen so even 6 HOURS of accumulation stays
  below the age lane's floor: these are the pathological keys the slow
  lane exists for, and they are where the headline files/day reduction
  is measured. Half the cohort goes silent at SILENCE_S — deliberately
  off-phase with the 6 h slow cadence, so their below-floor residue
  can only be collected by a LATER slow-lane flush.
- The planner sweeps every TICK_S seconds. A sweep-based trigger lands
  its decision in [deadline, deadline + TICK_S) — the assertions are
  written against those sweep bounds, the honest granularity of the
  design.
- BEFORE vs AFTER: the identical arrival stream drives two independent
  keyspaces. BEFORE models the pre-three-lane policy: age flushes
  EVERY nonempty key at the 15-minute deadline (min_flush_bytes=1) and
  the size trigger reads raw staged bytes (ratio 1.0). AFTER is the
  three-lane policy with the compression-aware size lane.

Scale note: the slow lane's 6 h hold keeps every team whose lump
spacing is under 6 h active in EVERY sweep, so the replay's cost is
~min(N_TEAMS, 21600) x ticks per pass; N_TEAMS is 10^4 (not the
design's 10^5) to keep the suite inside seconds — the per-tier shape
the assertions read is rank-local and unchanged by the truncation.
"""

import random
from dataclasses import dataclass, field

import pytest

from millrace.planner import (
    FlushDecision,
    KeyStats,
    PlannerKnobs,
    plan_flush,
)

US = 1_000_000
SEED = 20261009
N_TEAMS = 10_000
DAY_S = 86_400
TICK_S = 60
TICKS = DAY_S // TICK_S
BASE_US = 1_800_000_000 * US  # simulated epoch ≈ 2027-01-15

C_RATE = 1_000_000.0  # rank-1 team's bytes/s; team k gets C/(k+1)
LUMP_MIN = 1_000_000.0
LUMP_MAX = 4_000_000.0

# Knobs (replay scale: the byte sizes are scaled down, the STRUCTURE —
# ratios between lanes — is the design's). With RATIO 0.2 the size
# lane's staged threshold is 5 x TARGET = 80 MiB: ranks 1..6 cross it
# in 80..560 s, comfortably ahead of the 15-minute deadline.
TARGET_BYTES = 16 * 1024 * 1024
RATIO = 0.2
FLUSH_DEADLINE_S = 900
SLOW_LANE_DEADLINE_S = 21_600  # 6 h
MIN_FLUSH_BYTES = 256 * 1024
MAX_FILES_PER_COMMIT = 250

AFTER_KNOBS = PlannerKnobs(
    target_output_bytes=TARGET_BYTES,
    flush_deadline_s=FLUSH_DEADLINE_S,
    slow_lane_deadline_s=SLOW_LANE_DEADLINE_S,
    min_flush_bytes=MIN_FLUSH_BYTES,
    max_files_per_commit=MAX_FILES_PER_COMMIT,
)
# The pre-three-lane policy, expressed in the new knobs: every nonempty
# key flushes at the age deadline (floor = 1 byte) and the size trigger
# sizes the RAW staged bytes (ratio 1.0).
BEFORE_KNOBS = PlannerKnobs(
    target_output_bytes=TARGET_BYTES,
    flush_deadline_s=FLUSH_DEADLINE_S,
    slow_lane_deadline_s=SLOW_LANE_DEADLINE_S,
    min_flush_bytes=1,
    max_files_per_commit=MAX_FILES_PER_COMMIT,
)
BEFORE_RATIO = 1.0

TOP_TIER = range(6)  # ranks 1..6 — every one size-flushes well under 900 s
MEDIUM_TIER = range(500, 1501)  # a lump every 501..1501 s: the age lane
MEDIAN_TIER = range(4_500, 5_501)  # the lumpy control cohort (above)
CHURN_COHORT = range(1_501, 2_501)  # silent after noon; residue >= floor
NOON_S = DAY_S // 2

N_TRICKLE = 2_000
TRICKLE_RATE = 10  # bytes/s — 216 KB even at the 6 h mark: never floor-eligible
TRICKLE_BYTES_PER_TICK = TRICKLE_RATE * TICK_S
TRICKLE_TEAMS = range(N_TEAMS, N_TEAMS + N_TRICKLE)
SILENCED_TRICKLE = range(N_TEAMS + N_TRICKLE // 2, N_TEAMS + N_TRICKLE)
SILENCE_S = 46_800  # 13 h — NOT a multiple of the 6 h slow cadence

# Pinned totals of the seeded run (the fixture prints them; a change to
# the model or the planner that moves them must be explained in review).
# BEFORE slow == 0 pins the old policy's blind spot: with every key
# age-flushed at 15 minutes, the deadline lane had nothing to collect.
EXPECTED_BEFORE: dict[str, int] = {"size": 16574, "age": 381001, "slow": 0}
EXPECTED_AFTER: dict[str, int] = {"size": 2639, "age": 247275, "slow": 6000}


@dataclass
class PassResult:
    decisions: dict[str, int] = field(
        default_factory=lambda: {"size": 0, "age": 0, "slow": 0}
    )
    commits: int = 0
    peak_active: int = 0
    lane_ages_s: dict[str, list[float]] = field(
        default_factory=lambda: {"size": [], "age": [], "slow": []}
    )
    trickle_decisions: dict[int, list[FlushDecision]] = field(default_factory=dict)


@dataclass
class ReplayResult:
    before: PassResult = field(default_factory=PassResult)
    after: PassResult = field(default_factory=PassResult)
    top_tier: dict[int, list[FlushDecision]] = field(default_factory=dict)
    top_tier_before_files: int = 0
    medium_ages_s: list[float] = field(default_factory=list)
    medium_teams_flushed: set[int] = field(default_factory=set)
    median_decisions: dict[int, list[FlushDecision]] = field(default_factory=dict)
    median_before_counts: dict[int, int] = field(default_factory=dict)
    churn_flushed: set[int] = field(default_factory=set)
    churn_flushed_post_noon: set[int] = field(default_factory=set)
    churn_last_decision_s: float = 0.0
    silenced_collected: dict[int, FlushDecision] = field(default_factory=dict)
    max_active_age_s: float = 0.0  # sampled post-sweep, after pass


def _simulate() -> ReplayResult:
    rng = random.Random(SEED)
    rate = [C_RATE / (k + 1) for k in range(N_TEAMS)]
    lump = [min(max(2.0 * rate[k], LUMP_MIN), LUMP_MAX) for k in range(N_TEAMS)]

    # Arrival stream, bucketed by tick: (team, bytes, arrival µs). The
    # Zipf cohort's draws come first, then the trickle cohort's — the
    # stream is a deterministic function of SEED either way.
    buckets: list[list[tuple[int, int, int]]] = [[] for _ in range(TICKS)]
    for k in range(N_TEAMS):
        d = lump[k] / rate[k]
        horizon = NOON_S if k in CHURN_COHORT else DAY_S
        t = rng.random() * d  # decorrelating phase
        while t < horizon:
            size = int(lump[k] * (0.5 + rng.random()))
            if size > 0:
                buckets[int(t // TICK_S)].append((k, size, BASE_US + int(t * US)))
            t += d
    for team in TRICKLE_TEAMS:
        start_tick = int(rng.random() * (FLUSH_DEADLINE_S / TICK_S))
        horizon = SILENCE_S if team in SILENCED_TRICKLE else DAY_S
        for tick in range(start_tick, horizon // TICK_S):
            buckets[tick].append(
                (team, TRICKLE_BYTES_PER_TICK, BASE_US + (tick + 1) * TICK_S * US)
            )

    # Two keyspaces over the same stream: before = the old policy,
    # after = the three lanes.
    staged = {
        "before": [0] * (N_TEAMS + N_TRICKLE),
        "after": [0] * (N_TEAMS + N_TRICKLE),
    }
    first = {
        "before": [0] * (N_TEAMS + N_TRICKLE),
        "after": [0] * (N_TEAMS + N_TRICKLE),
    }
    last = {"before": [0] * (N_TEAMS + N_TRICKLE), "after": [0] * (N_TEAMS + N_TRICKLE)}
    rows = {"before": [0] * (N_TEAMS + N_TRICKLE), "after": [0] * (N_TEAMS + N_TRICKLE)}
    active: dict[str, set[int]] = {"before": set(), "after": set()}
    result = ReplayResult()
    result.top_tier = {k: [] for k in TOP_TIER}
    result.median_decisions = {k: [] for k in MEDIAN_TIER}
    result.median_before_counts = {k: 0 for k in MEDIAN_TIER}
    result.before.trickle_decisions = {t: [] for t in TRICKLE_TEAMS}
    result.after.trickle_decisions = {t: [] for t in TRICKLE_TEAMS}

    def run_pass(name: str, knobs: PlannerKnobs, ratio: float, now_us: int) -> None:
        res = result.before if name == "before" else result.after
        act = active[name]
        res.peak_active = max(res.peak_active, len(act))
        st_, fi, la, ro = staged[name], first[name], last[name], rows[name]
        snapshot = [
            KeyStats(
                team_id=k,
                staged_bytes=st_[k],
                first_staged_ts=fi[k],
                last_staged_ts=la[k],
                row_count=ro[k],
            )
            for k in act
        ]
        decisions = plan_flush(
            snapshot, now_us, knobs, estimated_compression_ratio=ratio
        )
        # one commit per decision (the runner's shape; the fanout guard
        # in flush.py bounds files per commit, not a planner-side chunker)
        res.commits += len(decisions)
        for d in decisions:
            res.decisions[d.trigger] += 1
            assert 0 <= d.age_us < (SLOW_LANE_DEADLINE_S + TICK_S) * US
            res.lane_ages_s[d.trigger].append(d.age_us / US)
            if d.trigger == "age":
                assert (
                    FLUSH_DEADLINE_S * US <= d.age_us < (FLUSH_DEADLINE_S + TICK_S) * US
                )
            elif d.trigger == "slow":
                assert (
                    SLOW_LANE_DEADLINE_S * US
                    <= d.age_us
                    < (SLOW_LANE_DEADLINE_S + TICK_S) * US
                )
            team = d.team_id
            if name == "after":
                if team in result.top_tier:
                    result.top_tier[team].append(d)
                elif team in MEDIUM_TIER:
                    result.medium_teams_flushed.add(team)
                    result.medium_ages_s.append(d.age_us / US)
                elif team in result.median_decisions:
                    result.median_decisions[team].append(d)
                if team in CHURN_COHORT:
                    result.churn_flushed.add(team)
                    if now_us >= BASE_US + NOON_S * US:
                        result.churn_flushed_post_noon.add(team)
                    result.churn_last_decision_s = max(
                        result.churn_last_decision_s, (now_us - BASE_US) / US
                    )
                if team in SILENCED_TRICKLE and now_us >= BASE_US + SILENCE_S * US:
                    result.silenced_collected[team] = d
            else:
                if team in TOP_TIER:
                    result.top_tier_before_files += 1
                if team in result.median_before_counts:
                    result.median_before_counts[team] += 1
            if team in TRICKLE_TEAMS:
                res.trickle_decisions[team].append(d)
            st_[team] = 0
            ro[team] = 0
            act.discard(team)

    for i in range(TICKS):
        now_s = (i + 1) * TICK_S
        now_us = BASE_US + now_s * US
        for k, size, arrived_us in buckets[i]:
            for name in ("before", "after"):
                if staged[name][k] == 0:
                    first[name][k] = arrived_us  # set at first stage, never advanced
                    active[name].add(k)
                staged[name][k] += size
                last[name][k] = arrived_us
                rows[name][k] += max(1, size // 1000)
        run_pass("before", BEFORE_KNOBS, BEFORE_RATIO, now_us)
        run_pass("after", AFTER_KNOBS, RATIO, now_us)
        if i == TICKS // 2 or i == TICKS - 1:
            # The slow lane's guarantee, sampled post-sweep: no key is
            # left sitting at or past the slow deadline.
            for k in active["after"]:
                result.max_active_age_s = max(
                    result.max_active_age_s, (now_us - first["after"][k]) / US
                )
            assert result.max_active_age_s < SLOW_LANE_DEADLINE_S

    return result


def _pct(ages: list[float], q: float) -> float:
    ordered = sorted(ages)
    return ordered[int(len(ordered) * q)]


@pytest.fixture(scope="module")
def replay() -> ReplayResult:
    result = _simulate()
    print(
        "\n=== planner replay: one seeded day, "
        f"{N_TEAMS} Zipf teams + {N_TRICKLE} trickle, {TICK_S}s sweeps ==="
    )
    for name, res in (("before", result.before), ("after", result.after)):
        print(
            f"{name}: {res.decisions}  commits: {res.commits}  "
            f"peak active keys: {res.peak_active}"
        )
    continuing = range(N_TEAMS, N_TEAMS + N_TRICKLE // 2)
    before_trickle = sum(len(result.before.trickle_decisions[t]) for t in continuing)
    after_trickle = sum(len(result.after.trickle_decisions[t]) for t in continuing)
    print(
        f"HEADLINE tail files/day per trickle key: "
        f"{before_trickle / len(continuing):.1f} -> "
        f"{after_trickle / len(continuing):.1f} "
        f"in-day ({before_trickle / after_trickle:.0f}x); steady-state cadence "
        f"{DAY_S / FLUSH_DEADLINE_S:.0f}/day -> {DAY_S / SLOW_LANE_DEADLINE_S:.0f}/day "
        f"({SLOW_LANE_DEADLINE_S // FLUSH_DEADLINE_S}x); "
        f"total decisions {sum(result.before.decisions.values())} -> "
        f"{sum(result.after.decisions.values())}"
    )
    for team, ds in sorted(result.top_tier.items()):
        sizes = [d.staged_bytes for d in ds]
        print(
            f"top tier rank {team + 1}: {len(ds)} flushes, "
            f"all {sorted({d.trigger for d in ds})}, "
            f"staged bytes min/mean/max "
            f"{min(sizes):,}/{sum(sizes) // len(sizes):,}/{max(sizes):,} "
            f"(size threshold {int(TARGET_BYTES / RATIO):,})"
        )
    print(
        f"whale (top-6) files/day: {result.top_tier_before_files} -> "
        f"{sum(len(ds) for ds in result.top_tier.values())}"
    )
    for lane, ages in result.after.lane_ages_s.items():
        if ages:
            print(
                f"after {lane} lane: n={len(ages)}  "
                f"p50 {_pct(ages, 0.5):.1f}s p99 {_pct(ages, 0.99):.1f}s "
                f"max {max(ages):.1f}s"
            )
    print(
        f"churn cohort: {len(result.churn_flushed)}/{len(CHURN_COHORT)} flushed, "
        f"{len(result.churn_flushed_post_noon)} collected post-noon, last at "
        f"{result.churn_last_decision_s}s; silenced trickle: "
        f"{len(result.silenced_collected)}/{len(SILENCED_TRICKLE)} collected "
        f"post-silence (silence at {SILENCE_S}s)"
    )
    return result


pytestmark = pytest.mark.replay


def test_top_tier_flushes_on_size_at_the_target_output_size(replay: ReplayResult):
    for team, ds in replay.top_tier.items():
        assert len(ds) >= 100, f"rank {team + 1} flushed {len(ds)}x in a day"
        # One sweep of this key's arrivals plus one lump is the most a
        # size-triggered file may overshoot the staged threshold by.
        overshoot = C_RATE / (team + 1) * TICK_S + 1.5 * LUMP_MAX + 1
        for d in ds:
            assert d.trigger == "size"
            # The trigger's own condition (float-identical to the
            # planner's evaluation), and the sweep-honesty upper bound.
            assert d.staged_bytes * RATIO >= TARGET_BYTES
            assert d.staged_bytes < TARGET_BYTES / RATIO + overshoot


def test_medium_tier_flushes_on_age_at_the_deadline(replay: ReplayResult):
    assert replay.medium_teams_flushed == set(MEDIUM_TIER)
    # Every medium team rides the age lane regularly (one flush per
    # lump, one deadline after it staged — tens per day each).
    assert len(replay.medium_ages_s) >= 10 * len(MEDIUM_TIER)
    for age_s in replay.medium_ages_s:
        assert FLUSH_DEADLINE_S <= age_s < FLUSH_DEADLINE_S + TICK_S


def test_median_tier_is_unmoved_by_the_lane_change(replay: ReplayResult):
    # The control cohort: lumpy teams whose every lump clears the age
    # lane's floor flush one deadline after each lump under BOTH
    # policies — identical decisions, all age, at the deadline. The
    # three-lane change must not disturb tenants that were never
    # pathological.
    for team, ds in replay.median_decisions.items():
        assert ds, f"median team {team} never flushed"
        assert all(d.trigger == "age" for d in ds)
        for d in ds:
            assert FLUSH_DEADLINE_S <= d.age_us / US < FLUSH_DEADLINE_S + TICK_S
        assert len(ds) == replay.median_before_counts[team]


def test_tail_files_per_day_before_vs_after(replay: ReplayResult):
    # The point of the slow lane. A continuous-trickle key flushes
    # ~96 times/day under the old every-key-at-15-min policy and ~4
    # under the slow lane (3 within this in-day window: the first
    # flush lands one slow deadline after first stage). Measured on
    # the CONTINUING half of the cohort — the silenced half's job is
    # residue collection, not rate.
    continuing = range(N_TEAMS, N_TEAMS + N_TRICKLE // 2)
    before = [len(replay.before.trickle_decisions[t]) for t in continuing]
    after = [len(replay.after.trickle_decisions[t]) for t in continuing]
    assert min(before) >= 85  # ~86400/960; phase decorrelation shaves a few
    assert max(after) <= DAY_S // SLOW_LANE_DEADLINE_S
    assert sum(before) / sum(after) >= 20


def test_every_trickle_decision_is_age_before_and_slow_after(replay: ReplayResult):
    for ds in replay.before.trickle_decisions.values():
        assert ds, "every trickle key flushes under the old policy"
        assert all(d.trigger == "age" for d in ds)
    for ds in replay.after.trickle_decisions.values():
        assert ds, "the slow lane collects every trickle key"
        assert all(d.trigger == "slow" for d in ds)


def test_no_key_sits_past_the_slow_deadline(replay: ReplayResult):
    # Sampled post-sweep inside the fixture (noon and day end); the
    # per-decision inline asserts bound every flush's age to within one
    # sweep of the slow deadline.
    assert replay.max_active_age_s < SLOW_LANE_DEADLINE_S
    slow_ages = replay.after.lane_ages_s["slow"]
    assert _pct(slow_ages, 0.99) < SLOW_LANE_DEADLINE_S + TICK_S


def test_age_lane_p99_stays_within_one_sweep_of_the_deadline(replay: ReplayResult):
    age_ages = replay.after.lane_ages_s["age"]
    assert _pct(age_ages, 0.99) < FLUSH_DEADLINE_S + TICK_S


def test_churned_teams_leave_no_residue(replay: ReplayResult):
    # Zipf churn teams (residue >= the floor) are collected by the age
    # lane within deadline + one sweep of going silent.
    assert len(replay.churn_flushed) >= 0.9 * len(CHURN_COHORT)
    assert len(replay.churn_flushed_post_noon) >= 100
    assert replay.churn_last_decision_s < NOON_S + FLUSH_DEADLINE_S + TICK_S


def test_silenced_trickle_teams_are_collected_by_the_slow_lane(replay: ReplayResult):
    # Below-floor residue of a gone-silent key is the slow lane's job:
    # every silenced trickle key is collected exactly once post-silence,
    # by a slow flush, inside one slow deadline of going quiet.
    assert set(replay.silenced_collected) == set(SILENCED_TRICKLE)
    for d in replay.silenced_collected.values():
        assert d.trigger == "slow"
        assert SILENCE_S < (d.first_staged_ts - BASE_US) / US + d.age_us / US
        assert d.age_us / US <= SLOW_LANE_DEADLINE_S + TICK_S
        # The residue it carries is honestly below the age lane's floor.
        assert d.staged_bytes < MIN_FLUSH_BYTES


def test_one_commit_per_decision(replay: ReplayResult):
    # The runner commits per (team, window): commits track decisions
    # exactly, and the planner never invents an empty one.
    for res in (replay.before, replay.after):
        assert res.commits == sum(res.decisions.values()) > 0


def test_seeded_totals_are_stable(replay: ReplayResult):
    assert replay.before.decisions == EXPECTED_BEFORE
    assert replay.after.decisions == EXPECTED_AFTER
