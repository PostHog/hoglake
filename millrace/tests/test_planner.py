"""The flush planner: three lanes, precedence, ordering.

Unit tests pin the exact boundaries (a flipped ``>=`` must red here);
the hypothesis properties sweep the input space for the spec bullets —
per-key trigger exclusivity, lane precedence, the age lane's size
floor, determinism. Boundary values are chosen so the
deadline arithmetic (deadline seconds x 1_000_000) and the size
arithmetic (staged bytes x ratio vs. target) are exercised exactly:
the pinned ratio 0.5 is a power of two, so the float products in the
size lane are exact and "one byte each side" means one byte.
"""

from hypothesis import assume, given
from hypothesis import strategies as st

from millrace.planner import (
    DEFAULT_COMPRESSION_RATIO,
    DEFAULT_POLICY,
    FLUSH_DEADLINE_S_DEFAULT,
    MAX_FILES_PER_COMMIT_DEFAULT,
    MIN_FLUSH_BYTES_DEFAULT,
    SLOW_LANE_DEADLINE_S_DEFAULT,
    TARGET_OUTPUT_BYTES_DEFAULT,
    FlushDecision,
    FlushPolicy,
    KeyStats,
    PlannerKnobs,
    ThreeLanePolicy,
    decide_key,
    plan_flush,
)

US = 1_000_000
NOW_US = 1_800_000_000 * US  # an arbitrary fixed instant (≈ 2027-01-15)
TARGET = 128 * 1024 * 1024
FLUSH_S = 900
SLOW_S = 21_600  # 6 h
MIN_BYTES = 256 * 1024
RATIO = 0.5  # exact in binary: the size lane's boundary pins are byte-exact
KNOBS = PlannerKnobs(
    target_output_bytes=TARGET,
    flush_deadline_s=FLUSH_S,
    slow_lane_deadline_s=SLOW_S,
    min_flush_bytes=MIN_BYTES,
    max_files_per_commit=250,
)


def key(team_id: int, *, staged: int, age_us: int, rows: int = 1) -> KeyStats:
    """A key whose oldest staged byte is exactly ``age_us`` old at NOW_US."""
    return KeyStats(
        team_id=team_id,
        staged_bytes=staged,
        first_staged_ts=NOW_US - age_us,
        last_staged_ts=NOW_US,
        row_count=rows,
    )


# -- size lane: staged_bytes x ratio vs. target_output_bytes --------------------
# (RATIO = 0.5, so the staged-bytes threshold is exactly 2 x TARGET.)


def test_nothing_decided_below_every_trigger():
    assert (
        decide_key(
            key(1, staged=2 * TARGET - 1, age_us=(FLUSH_S - 1) * US),
            NOW_US,
            KNOBS,
            estimated_compression_ratio=RATIO,
        )
        is None
    )


def test_size_trigger_fires_when_the_estimate_equals_the_target():
    d = decide_key(
        key(1, staged=2 * TARGET, age_us=0),
        NOW_US,
        KNOBS,
        estimated_compression_ratio=RATIO,
    )
    assert d is not None and d.trigger == "size"


def test_size_trigger_silent_one_staged_byte_below_the_threshold():
    assert (
        decide_key(
            key(1, staged=2 * TARGET - 1, age_us=0),
            NOW_US,
            KNOBS,
            estimated_compression_ratio=RATIO,
        )
        is None
    )


def test_size_trigger_threshold_moves_inversely_with_the_ratio():
    # Doubling the ratio halves the staged bytes needed: at ratio 0.25
    # the threshold is 4 x TARGET, byte-exact (0.25 is a power of two).
    k_at = key(1, staged=4 * TARGET, age_us=0)
    d = decide_key(k_at, NOW_US, KNOBS, estimated_compression_ratio=0.25)
    assert d is not None and d.trigger == "size"
    assert (
        decide_key(
            key(1, staged=4 * TARGET - 1, age_us=0),
            NOW_US,
            KNOBS,
            estimated_compression_ratio=0.25,
        )
        is None
    )
    # …and at ratio 1.0 the threshold IS the target, byte-exact.
    d = decide_key(
        key(1, staged=TARGET, age_us=0),
        NOW_US,
        KNOBS,
        estimated_compression_ratio=1.0,
    )
    assert d is not None and d.trigger == "size"
    assert (
        decide_key(
            key(1, staged=TARGET - 1, age_us=0),
            NOW_US,
            KNOBS,
            estimated_compression_ratio=1.0,
        )
        is None
    )


def test_default_compression_ratio_is_the_fallback_and_is_used():
    assert DEFAULT_COMPRESSION_RATIO == 0.2
    # With the default ratio (no keyword passed), the staged threshold
    # sits at 5 x TARGET; the ±1 byte pin fails loudly if the default
    # drifts. (0.2 is not binary-exact, but at ~5e8 the gap to the
    # boundary is ~2e-7 x staged >> one ulp, so the pins are stable.)
    d = decide_key(key(1, staged=5 * TARGET, age_us=0), NOW_US, KNOBS)
    assert d is not None and d.trigger == "size"
    assert decide_key(key(1, staged=5 * TARGET - 1, age_us=0), NOW_US, KNOBS) is None


# -- age lane: deadline AND the min_flush_bytes floor ----------------------------


def test_age_trigger_fires_at_exactly_the_deadline_with_exactly_min_bytes():
    d = decide_key(
        key(1, staged=MIN_BYTES, age_us=FLUSH_S * US),
        NOW_US,
        KNOBS,
        estimated_compression_ratio=RATIO,
    )
    assert d is not None and d.trigger == "age"


def test_age_trigger_silent_one_microsecond_below_the_deadline():
    assert (
        decide_key(
            key(1, staged=MIN_BYTES, age_us=FLUSH_S * US - 1),
            NOW_US,
            KNOBS,
            estimated_compression_ratio=RATIO,
        )
        is None
    )


def test_age_trigger_silent_one_byte_below_the_size_floor():
    # The pathological-key property: a tiny key at the 15-minute mark
    # earns NOTHING.
    assert (
        decide_key(
            key(1, staged=MIN_BYTES - 1, age_us=FLUSH_S * US),
            NOW_US,
            KNOBS,
            estimated_compression_ratio=RATIO,
        )
        is None
    )


def test_age_lane_ends_at_the_slow_deadline():
    # Age-eligible size, but past the slow deadline: the slow lane owns
    # it — the age lane's window is [flush_deadline, slow_lane_deadline).
    d = decide_key(
        key(1, staged=MIN_BYTES, age_us=SLOW_S * US),
        NOW_US,
        KNOBS,
        estimated_compression_ratio=RATIO,
    )
    assert d is not None and d.trigger == "slow"


# -- slow lane: the deadline, regardless of size --------------------------------


def test_slow_trigger_fires_at_exactly_the_slow_deadline():
    d = decide_key(
        key(1, staged=1, age_us=SLOW_S * US),
        NOW_US,
        KNOBS,
        estimated_compression_ratio=RATIO,
    )
    assert d is not None and d.trigger == "slow"


def test_slow_trigger_flushes_an_empty_key_at_the_deadline():
    # Churned-tenant residue: the slow lane is the only lane that
    # claims a key still below min_flush_bytes — including one with
    # nothing but a stats entry left.
    d = decide_key(
        key(1, staged=0, age_us=SLOW_S * US),
        NOW_US,
        KNOBS,
        estimated_compression_ratio=RATIO,
    )
    assert d is not None and d.trigger == "slow"


def test_a_tiny_key_one_microsecond_below_the_slow_deadline_earns_nothing():
    # The sharpened pathological-key property: below the floor, only
    # the slow lane flushes a key — one microsecond short of it, the
    # key sits.
    assert (
        decide_key(
            key(1, staged=MIN_BYTES - 1, age_us=SLOW_S * US - 1),
            NOW_US,
            KNOBS,
            estimated_compression_ratio=RATIO,
        )
        is None
    )


# -- precedence: exactly one trigger per key, size > slow > age ------------------


def test_a_full_key_flushes_on_size_even_past_the_slow_deadline():
    # Over the size target AND past both deadlines: a full file is
    # never wrong to write — size outranks everything (and the file
    # ships full footer stats, not deferred).
    d = decide_key(
        key(1, staged=20 * TARGET, age_us=(SLOW_S + 1) * US),
        NOW_US,
        KNOBS,
        estimated_compression_ratio=RATIO,
    )
    assert d is not None and d.trigger == "size"


def test_a_key_past_the_slow_deadline_flushes_slow_regardless_of_medium_size():
    # Age-eligible size (>= min_flush_bytes) AND past the slow deadline:
    # the slow lane claims it — a key that sat unflushed this long is
    # not a freshness-SLA flush.
    d = decide_key(
        key(1, staged=10 * MIN_BYTES, age_us=(SLOW_S + 1) * US),
        NOW_US,
        KNOBS,
        estimated_compression_ratio=RATIO,
    )
    assert d is not None and d.trigger == "slow"


def test_a_young_full_key_is_size_decided():
    d = decide_key(
        key(1, staged=10 * TARGET, age_us=(FLUSH_S - 1) * US),
        NOW_US,
        KNOBS,
        estimated_compression_ratio=RATIO,
    )
    assert d is not None and d.trigger == "size"


def test_negative_age_never_trips_a_deadline_trigger():
    # Clock skew in the snapshot (first_staged_ts ahead of now): no
    # deadline trigger by construction; the size trigger is age-blind.
    future = key(1, staged=2 * TARGET - 1, age_us=-US)
    assert decide_key(future, NOW_US, KNOBS, estimated_compression_ratio=RATIO) is None
    d = decide_key(
        key(1, staged=2 * TARGET, age_us=-US),
        NOW_US,
        KNOBS,
        estimated_compression_ratio=RATIO,
    )
    assert d is not None and d.trigger == "size"


def test_age_trigger_reads_first_staged_ts_never_last_staged_ts():
    # Continuous arrivals must not defer the freshness SLA: the newest
    # byte is one second old, the oldest is past the deadline, and the
    # key is decided by the oldest — first_staged_ts is never advanced
    # by later arrivals (KeyStats docstring; enforced by the stage
    # writer in Phase 2, relied on here).
    k = KeyStats(
        team_id=1,
        staged_bytes=MIN_BYTES,
        first_staged_ts=NOW_US - (FLUSH_S + 1) * US,
        last_staged_ts=NOW_US - US,
        row_count=100,
    )
    d = decide_key(k, NOW_US, KNOBS, estimated_compression_ratio=RATIO)
    assert d is not None and d.trigger == "age"


def test_decision_carries_age_bytes_and_team():
    d = decide_key(
        key(7, staged=2 * TARGET, age_us=42 * US),
        NOW_US,
        KNOBS,
        estimated_compression_ratio=RATIO,
    )
    assert d == FlushDecision(
        team_id=7,
        trigger="size",
        staged_bytes=2 * TARGET,
        first_staged_ts=NOW_US - 42 * US,
        age_us=42 * US,
    )


# -- the compression-ratio input -------------------------------------------------


def test_ratio_validation_refuses_zero_negative_above_one_and_nan():
    for bad in (0.0, -0.0, -0.5, 1.0000000001, float("inf"), float("nan")):
        try:
            decide_key(
                key(1, staged=2 * TARGET, age_us=0),
                NOW_US,
                KNOBS,
                estimated_compression_ratio=bad,
            )
        except ValueError as e:
            assert "estimated_compression_ratio" in str(e)
        else:
            raise AssertionError(f"ratio {bad} accepted")


def test_ratio_validation_accepts_one_and_tiny_positive():
    for good in (1.0, 0.001):
        d = decide_key(
            key(1, staged=2**48, age_us=0),
            NOW_US,
            KNOBS,
            estimated_compression_ratio=good,
        )
        assert d is not None and d.trigger == "size"


def test_plan_flush_validates_the_ratio_even_on_an_empty_snapshot():
    try:
        plan_flush([], NOW_US, KNOBS, estimated_compression_ratio=0.0)
    except ValueError as e:
        assert "estimated_compression_ratio" in str(e)
    else:
        raise AssertionError("plan_flush accepted ratio 0.0")


# -- the policy seam --------------------------------------------------------------


class _RecordingPolicy:
    """A policy test-double: flushes every key it is asked about, as
    "size", and records the arguments it was handed."""

    def __init__(self) -> None:
        self.seen: list[tuple[int, int, PlannerKnobs, float]] = []

    def decide(
        self,
        key: KeyStats,
        now_us: int,
        knobs: PlannerKnobs,
        estimated_compression_ratio: float,
    ) -> FlushDecision | None:
        self.seen.append((key.team_id, now_us, knobs, estimated_compression_ratio))
        return FlushDecision(
            team_id=key.team_id,
            trigger="size",
            staged_bytes=key.staged_bytes,
            first_staged_ts=key.first_staged_ts,
            age_us=now_us - key.first_staged_ts,
        )


def test_plan_flush_dispatches_through_the_given_policy():
    policy = _RecordingPolicy()
    stats = [key(2, staged=1, age_us=US), key(1, staged=1, age_us=2 * US)]
    decisions = plan_flush(
        stats,
        NOW_US,
        KNOBS,
        estimated_compression_ratio=0.125,
        policy=policy,
    )
    # Every key was routed through the policy with the planner's
    # arguments, and the policy's decisions came back ordered.
    assert policy.seen == [
        (2, NOW_US, KNOBS, 0.125),
        (1, NOW_US, KNOBS, 0.125),
    ]
    assert [d.team_id for d in decisions] == [1, 2]
    assert all(d.trigger == "size" for d in decisions)


def test_decide_key_dispatches_through_the_given_policy():
    policy = _RecordingPolicy()
    k = key(5, staged=1, age_us=US)
    d = decide_key(k, NOW_US, KNOBS, estimated_compression_ratio=0.75, policy=policy)
    assert d is not None and d.trigger == "size"
    assert policy.seen == [(5, NOW_US, KNOBS, 0.75)]


def test_the_default_policy_is_the_three_lane_policy():
    assert isinstance(DEFAULT_POLICY, ThreeLanePolicy)
    assert isinstance(DEFAULT_POLICY, FlushPolicy)
    # …and decide_key's default path is exactly the policy's decision.
    k = key(3, staged=2 * TARGET, age_us=US)
    assert decide_key(
        k, NOW_US, KNOBS, estimated_compression_ratio=RATIO
    ) == DEFAULT_POLICY.decide(k, NOW_US, KNOBS, RATIO)


# -- plan_flush over a snapshot ----------------------------------------------------


def test_empty_snapshot_decides_nothing():
    assert plan_flush([], NOW_US, KNOBS) == []


def test_a_snapshot_with_nothing_ready_decides_nothing():
    stats = [key(1, staged=1, age_us=US), key(2, staged=2, age_us=2 * US)]
    assert plan_flush(stats, NOW_US, KNOBS) == []


def test_duplicate_team_ids_are_refused():
    stats = [key(1, staged=1, age_us=US), key(1, staged=2, age_us=2 * US)]
    try:
        plan_flush(stats, NOW_US, KNOBS)
    except ValueError as e:
        assert "team_id 1" in str(e)
    else:  # pragma: no cover - the raise is the assertion
        raise AssertionError("duplicate team_id accepted")


def test_decisions_are_ordered_oldest_first_staged_ts_first():
    young = key(1, staged=2 * TARGET, age_us=10 * US)  # size
    old = key(2, staged=1, age_us=(SLOW_S + 100) * US)  # slow
    mid = key(3, staged=MIN_BYTES, age_us=(FLUSH_S + 50) * US)  # age
    for order in ([young, old, mid], [mid, young, old], [old, mid, young]):
        decisions = plan_flush(order, NOW_US, KNOBS, estimated_compression_ratio=RATIO)
        assert [d.team_id for d in decisions] == [2, 3, 1]
        assert [d.trigger for d in decisions] == ["slow", "age", "size"]


def test_decision_ordering_ties_break_by_team_id():
    same_age = (FLUSH_S + 1) * US
    stats = [
        key(9, staged=MIN_BYTES, age_us=same_age),
        key(3, staged=MIN_BYTES, age_us=same_age),
    ]
    assert [d.team_id for d in plan_flush(stats, NOW_US, KNOBS)] == [3, 9]


# -- knob validation and defaults -------------------------------------------------------


def test_knob_defaults_are_the_documented_design_values():
    knobs = PlannerKnobs()
    assert knobs.target_output_bytes == TARGET_OUTPUT_BYTES_DEFAULT == 500 * 1024 * 1024
    assert knobs.flush_deadline_s == FLUSH_DEADLINE_S_DEFAULT == 900
    assert knobs.slow_lane_deadline_s == SLOW_LANE_DEADLINE_S_DEFAULT == 6 * 3600
    assert knobs.min_flush_bytes == MIN_FLUSH_BYTES_DEFAULT == 1024 * 1024
    assert knobs.max_files_per_commit == MAX_FILES_PER_COMMIT_DEFAULT == 512


def test_knob_validation():
    PlannerKnobs(
        target_output_bytes=1,
        flush_deadline_s=1,
        slow_lane_deadline_s=1,
        min_flush_bytes=1,
        max_files_per_commit=1,
    )
    for kwargs, field in (
        ({"target_output_bytes": 0}, "target_output_bytes"),
        ({"flush_deadline_s": 0}, "flush_deadline_s"),
        ({"slow_lane_deadline_s": 0}, "slow_lane_deadline_s"),
        ({"min_flush_bytes": 0}, "min_flush_bytes"),
        ({"max_files_per_commit": 0}, "max_files_per_commit"),
        ({"flush_deadline_s": -1}, "flush_deadline_s"),
    ):
        fields: dict[str, int] = {
            "target_output_bytes": TARGET,
            "flush_deadline_s": FLUSH_S,
            "slow_lane_deadline_s": SLOW_S,
            "min_flush_bytes": MIN_BYTES,
            "max_files_per_commit": 250,
        }
        fields.update(kwargs)
        try:
            PlannerKnobs(**fields)  # type: ignore[arg-type]
        except ValueError as e:
            assert field in str(e)
        else:
            raise AssertionError(f"accepted {kwargs}")


# -- properties ---------------------------------------------------------------------

key_stats = st.builds(
    KeyStats,
    team_id=st.integers(min_value=0, max_value=2**64 - 1),
    staged_bytes=st.integers(min_value=0, max_value=2**48),
    first_staged_ts=st.integers(min_value=-(2**62), max_value=2**62),
    last_staged_ts=st.integers(min_value=-(2**62), max_value=2**62),
    row_count=st.integers(min_value=0, max_value=2**40),
)
nows = st.integers(min_value=-(2**62), max_value=2**62)
ratios = st.floats(
    min_value=1e-6,
    max_value=1.0,
    allow_nan=False,
    allow_infinity=False,
    allow_subnormal=False,
)
knobs_any = st.builds(
    PlannerKnobs,
    target_output_bytes=st.integers(min_value=1, max_value=2**40),
    flush_deadline_s=st.integers(min_value=1, max_value=2**20),
    slow_lane_deadline_s=st.integers(min_value=1, max_value=2**20),
    min_flush_bytes=st.integers(min_value=1, max_value=2**40),
    max_files_per_commit=st.integers(min_value=1, max_value=10_000),
)
# (flush, slow) with flush <= slow — the expected, unenforced configuration.
deadline_pairs = st.tuples(
    st.integers(min_value=1, max_value=2**20), st.integers(min_value=1, max_value=2**20)
).map(lambda p: (min(p), max(p)))


@given(k=key_stats, now=nows, knobs=knobs_any, ratio=ratios)
def test_decide_key_matches_the_documented_policy(
    k: KeyStats, now: int, knobs: PlannerKnobs, ratio: float
):
    d = decide_key(k, now, knobs, estimated_compression_ratio=ratio)
    age = now - k.first_staged_ts
    expected: str | None
    if k.staged_bytes * ratio >= knobs.target_output_bytes:
        expected = "size"
    elif age >= knobs.slow_lane_deadline_s * US:
        expected = "slow"
    elif age >= knobs.flush_deadline_s * US and k.staged_bytes >= knobs.min_flush_bytes:
        expected = "age"
    else:
        expected = None
    assert (d.trigger if d is not None else None) == expected
    if d is not None:
        assert d.age_us == age
        assert d.team_id == k.team_id
        assert d.staged_bytes == k.staged_bytes
        assert d.first_staged_ts == k.first_staged_ts


@given(pair=deadline_pairs, staged=st.integers(0, 2**48), data=st.data())
def test_past_slow_deadline_always_flushes_slow_regardless_of_size(
    pair: tuple[int, int], staged: int, data: st.DataObject
):
    # The slow lane's promise: a key this old ALWAYS flushes (the churn
    # reaper's job, now a lane). Target 2**62 with ratio 1.0 keeps the
    # size lane out of reach so "slow" is exact, not "size or slow".
    _, slow_s = pair
    age = data.draw(st.integers(min_value=slow_s * US, max_value=slow_s * US + 2**40))
    knobs = PlannerKnobs(
        target_output_bytes=2**62,
        flush_deadline_s=pair[0],
        slow_lane_deadline_s=slow_s,
        min_flush_bytes=MIN_BYTES,
        max_files_per_commit=250,
    )
    d = decide_key(
        key(1, staged=staged, age_us=age),
        NOW_US,
        knobs,
        estimated_compression_ratio=1.0,
    )
    assert d is not None and d.trigger == "slow"


@given(pair=deadline_pairs, staged=st.integers(0, 2**48), data=st.data())
def test_age_decisions_stay_inside_their_window(
    pair: tuple[int, int], staged: int, data: st.DataObject
):
    # The lanes partition (age x size): an "age" decision can only be
    # drawn with age in [flush_deadline, slow_lane_deadline).
    flush_s, slow_s = pair
    assume(flush_s < slow_s)
    age = data.draw(st.integers(min_value=-(2**40), max_value=slow_s * US + 2**40))
    knobs = PlannerKnobs(
        target_output_bytes=2**62,
        flush_deadline_s=flush_s,
        slow_lane_deadline_s=slow_s,
        min_flush_bytes=MIN_BYTES,
        max_files_per_commit=250,
    )
    d = decide_key(
        key(1, staged=staged, age_us=age),
        NOW_US,
        knobs,
        estimated_compression_ratio=1.0,
    )
    if d is not None and d.trigger == "age":
        assert flush_s * US <= age < slow_s * US
        assert staged >= MIN_BYTES


@given(pair=deadline_pairs, data=st.data())
def test_a_tiny_key_earns_nothing_until_the_slow_deadline(
    pair: tuple[int, int], data: st.DataObject
):
    # The pathological-key property, swept: below min_flush_bytes the
    # age lane never fires, however old the key — and past the slow
    # deadline the slow lane always claims it.
    flush_s, slow_s = pair
    assume(flush_s < slow_s)
    staged = data.draw(st.integers(min_value=0, max_value=MIN_BYTES - 1))
    age = data.draw(st.integers(min_value=0, max_value=slow_s * US - 1))
    knobs = PlannerKnobs(
        target_output_bytes=2**62,
        flush_deadline_s=flush_s,
        slow_lane_deadline_s=slow_s,
        min_flush_bytes=MIN_BYTES,
        max_files_per_commit=250,
    )
    k = key(1, staged=staged, age_us=age)
    assert decide_key(k, NOW_US, knobs, estimated_compression_ratio=1.0) is None
    old = key(1, staged=staged, age_us=slow_s * US)
    d = decide_key(old, NOW_US, knobs, estimated_compression_ratio=1.0)
    assert d is not None and d.trigger == "slow"


@given(
    pair=deadline_pairs,
    target=st.integers(1, 2**40),
    ratio=st.sampled_from([1.0, 0.5, 0.25, 0.125]),
    data=st.data(),
)
def test_a_young_key_is_size_decided_iff_the_estimate_reaches_target(
    pair: tuple[int, int], target: int, ratio: float, data: st.DataObject
):
    # Powers-of-two ratios keep the float math exact, so the boundary
    # pin is byte-exact at every scale: the staged threshold is
    # target / ratio, and one staged byte below it flushes nothing.
    flush_s, _ = pair
    assume(flush_s > 1)
    age = data.draw(st.integers(min_value=0, max_value=flush_s * US - 1))
    knobs = PlannerKnobs(
        target_output_bytes=target,
        flush_deadline_s=flush_s,
        slow_lane_deadline_s=pair[1],
        min_flush_bytes=MIN_BYTES,
        max_files_per_commit=250,
    )
    threshold = int(target / ratio)  # exact: ratio is a power of two
    d = decide_key(
        key(1, staged=threshold, age_us=age),
        NOW_US,
        knobs,
        estimated_compression_ratio=ratio,
    )
    assert d is not None and d.trigger == "size"
    assert (
        decide_key(
            key(1, staged=threshold - 1, age_us=age),
            NOW_US,
            knobs,
            estimated_compression_ratio=ratio,
        )
        is None
    )


@given(
    k=key_stats,
    now=nows,
    knobs=knobs_any,
    r1=ratios,
    data=st.data(),
)
def test_size_decisions_are_monotone_in_the_ratio(
    k: KeyStats, now: int, knobs: PlannerKnobs, r1: float, data: st.DataObject
):
    # A bigger ratio can only make the size lane fire sooner: whatever
    # size-flushes at r1 still size-flushes at r2 >= r1 (precedence is
    # stable — size is checked first either way).
    r2 = data.draw(
        st.floats(
            min_value=r1,
            max_value=1.0,
            allow_nan=False,
            allow_infinity=False,
            allow_subnormal=False,
        )
    )
    d1 = decide_key(k, now, knobs, estimated_compression_ratio=r1)
    d2 = decide_key(k, now, knobs, estimated_compression_ratio=r2)
    if d1 is not None and d1.trigger == "size":
        assert d2 is not None and d2.trigger == "size"


@given(keys=st.lists(key_stats, unique_by=lambda k: k.team_id, max_size=40), now=nows)
def test_plan_flush_is_deterministic_under_input_order(keys: list[KeyStats], now: int):
    forward = plan_flush(keys, now, KNOBS)
    assert plan_flush(list(reversed(keys)), now, KNOBS) == forward
    assert plan_flush(list(keys), now, KNOBS) == forward


@given(keys=st.lists(key_stats, unique_by=lambda k: k.team_id, max_size=40), now=nows)
def test_at_most_one_decision_per_key_and_output_sorted(keys: list[KeyStats], now: int):
    decisions = plan_flush(keys, now, KNOBS)
    decided = [d.team_id for d in decisions]
    assert len(set(decided)) == len(decided)  # exactly one trigger per key
    assert set(decided) <= {k.team_id for k in keys}
    order = [(d.first_staged_ts, d.team_id) for d in decisions]
    assert order == sorted(order)
