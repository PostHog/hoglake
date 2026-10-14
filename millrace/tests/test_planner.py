"""The flush predicate (planner.decide): lane order size > slow > age,
inclusive boundaries, the minimum size gating only the age lane, and
knob validation. Pure; the mutation gate runs this file."""

import pytest
from hypothesis import given
from hypothesis import strategies as st

from millrace.planner import (
    FLUSH_BYTES_DEFAULT,
    FLUSH_DEADLINE_S_DEFAULT,
    MIN_FLUSH_BYTES_DEFAULT,
    SLOW_LANE_DEADLINE_S_DEFAULT,
    PlannerKnobs,
    decide,
)

S = 1_000_000  # µs per second
NOW = 1_800_000_000 * S
KNOBS = PlannerKnobs(
    flush_bytes=1000, flush_deadline_s=60, slow_lane_deadline_s=600, min_flush_bytes=100
)


def lane(staged: int, age_us: int, knobs: PlannerKnobs = KNOBS) -> str | None:
    return decide(
        staged_bytes=staged, first_staged_ts=NOW - age_us, now_us=NOW, knobs=knobs
    )


def test_defaults():
    k = PlannerKnobs()
    assert (k.flush_bytes, k.flush_deadline_s, k.slow_lane_deadline_s) == (
        256 * 1024 * 1024,
        900,
        6 * 3600,
    )
    assert k.min_flush_bytes == 1024 * 1024
    assert FLUSH_BYTES_DEFAULT == 256 * 1024 * 1024
    assert FLUSH_DEADLINE_S_DEFAULT == 900
    assert SLOW_LANE_DEADLINE_S_DEFAULT == 21600
    assert MIN_FLUSH_BYTES_DEFAULT == 1024 * 1024


def test_size_lane_is_inclusive_and_outranks_everything():
    assert lane(999, 0) is None
    assert lane(1000, 0) == "size"
    assert lane(1001, 0) == "size"
    assert lane(1000, 10_000 * S) == "size"
    assert lane(1000, -5 * S) == "size"


def test_slow_lane_is_inclusive_takes_any_size_and_outranks_age():
    assert lane(0, 600 * S - 1) is None
    assert lane(0, 600 * S) == "slow"
    assert lane(1, 600 * S) == "slow"
    assert lane(500, 600 * S) == "slow"  # also age-eligible: slow wins
    assert lane(999, 600 * S + 1) == "slow"


def test_age_lane_is_inclusive_on_both_deadline_and_minimum():
    assert lane(100, 60 * S - 1) is None
    assert lane(100, 60 * S) == "age"
    assert lane(99, 60 * S) is None  # below the minimum: waits for slow
    assert lane(99, 599 * S) is None
    assert lane(999, 599 * S) == "age"


def test_negative_ages_trip_no_deadline():
    assert lane(500, -1) is None
    assert lane(0, -(10**12)) is None


def test_deadlines_are_seconds():
    knobs = PlannerKnobs(
        flush_bytes=10**9, flush_deadline_s=2, slow_lane_deadline_s=3, min_flush_bytes=1
    )
    assert lane(1, 2 * S - 1, knobs) is None
    assert lane(1, 2 * S, knobs) == "age"
    assert lane(1, 3 * S - 1, knobs) == "age"
    assert lane(1, 3 * S, knobs) == "slow"


@given(
    staged=st.integers(min_value=0, max_value=2000),
    age=st.integers(min_value=-700 * S, max_value=700 * S),
)
def test_the_lanes_partition_the_space(staged: int, age: int):
    expected = (
        "size"
        if staged >= 1000
        else "slow"
        if age >= 600 * S
        else "age"
        if age >= 60 * S and staged >= 100
        else None
    )
    assert lane(staged, age) == expected


@pytest.mark.parametrize(
    "field",
    ["flush_bytes", "flush_deadline_s", "slow_lane_deadline_s", "min_flush_bytes"],
)
def test_knobs_must_be_positive(field: str):
    PlannerKnobs(**{field: 1})
    for bad in (0, -1):
        with pytest.raises(ValueError, match=rf"^{field} must be >= 1, got {bad}$"):
            PlannerKnobs(**{field: bad})
