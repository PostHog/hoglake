"""The flush predicate: when one staged group is due. Pure: no I/O, no
clocks.

Checked in this order, first match wins:

- **size**: ``staged_bytes >= flush_bytes``. One scan budget's worth is
  staged; flushing now produces a full-budget file. Compaction owns
  final file sizing.
- **slow**: ``age >= slow_lane_deadline``, any size. The only lane that
  takes a group below ``min_flush_bytes``, so every group is due within
  the slow deadline of its first staged row.
- **age**: ``age >= flush_deadline`` and ``staged_bytes >=
  min_flush_bytes``. The freshness SLA for medium groups; a tiny group
  waits for the slow lane instead of producing a tiny file per
  deadline.

Slow outranks age so an age-lane decision's age always lies in
``[flush_deadline, slow_lane_deadline)``. Negative ages (clock skew)
trip no deadline.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final, Literal

FLUSH_BYTES_DEFAULT: Final = 256 * 1024 * 1024
FLUSH_DEADLINE_S_DEFAULT: Final = 900
SLOW_LANE_DEADLINE_S_DEFAULT: Final = 6 * 3600
MIN_FLUSH_BYTES_DEFAULT: Final = 1024 * 1024

_MICROS_PER_SECOND: Final = 1_000_000

FlushTrigger = Literal["size", "slow", "age"]


@dataclass(frozen=True, slots=True)
class PlannerKnobs:
    flush_bytes: int = FLUSH_BYTES_DEFAULT
    flush_deadline_s: int = FLUSH_DEADLINE_S_DEFAULT
    slow_lane_deadline_s: int = SLOW_LANE_DEADLINE_S_DEFAULT
    min_flush_bytes: int = MIN_FLUSH_BYTES_DEFAULT

    def __post_init__(self) -> None:
        for name in (
            "flush_bytes",
            "flush_deadline_s",
            "slow_lane_deadline_s",
            "min_flush_bytes",
        ):
            value = getattr(self, name)
            if value < 1:
                raise ValueError(f"{name} must be >= 1, got {value}")


def decide(
    *, staged_bytes: int, first_staged_ts: int, now_us: int, knobs: PlannerKnobs
) -> FlushTrigger | None:
    """The lane that makes a group due now, or None. ``first_staged_ts``
    and ``now_us`` are microseconds since the epoch."""
    if staged_bytes >= knobs.flush_bytes:
        return "size"
    age_us = now_us - first_staged_ts
    if age_us >= knobs.slow_lane_deadline_s * _MICROS_PER_SECOND:
        return "slow"
    if (
        age_us >= knobs.flush_deadline_s * _MICROS_PER_SECOND
        and staged_bytes >= knobs.min_flush_bytes
    ):
        return "age"
    return None
