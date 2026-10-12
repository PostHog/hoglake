"""Pure per-key flush decisions (docs/kafka-ingestion.md §The flush planner).

Three lanes, evaluated per key; arrivals never reset a key's
``first_staged_ts``:

- **size** — ``staged_bytes × estimated_compression_ratio >=
  target_output_bytes``: the whales, early and often, full-size files.
  The lane sizes the OUTPUT, not the input: the trigger estimates the
  parquet file size, taking compression roughly into account. The ratio
  (parquet bytes ÷ staged bytes) is a pure planner *input* — the
  flusher feeds it an observed EWMA of completed flushes
  (``flush.CompressionRatioEwma``; in-memory, reset on a table
  shape/incarnation change); before the first observation lands,
  :data:`DEFAULT_COMPRESSION_RATIO` applies.
- **age** — ``age >= flush_deadline`` AND ``staged_bytes >=
  min_flush_bytes``: medium tenants — the freshness SLA. The minimum
  size is what keeps the lane "medium": a genuinely tiny key at the
  15-minute mark earns NOTHING here, so the tail stops producing a
  small file per key per window.
- **slow** — ``age >= slow_lane_deadline`` regardless of size: the
  pathological low-volume keys, one flush per 6–24 h instead of ~96
  tiny files a day. The slow lane subsumes the old churn reaper: any
  key this old flushes, so churned tenants leave no residue and the
  stats keyspace reflects live tenants.

Units: timestamps (``first_staged_ts``, ``last_staged_ts``, ``now_us``)
are microseconds since the Unix epoch; the deadline knobs are seconds
and are converted once per evaluation. Negative ages (a clock-skewed
snapshot with ``first_staged_ts`` ahead of ``now``) trip no deadline
trigger by construction.

Precedence — one trigger per key
--------------------------------

A key is decided by exactly one lane per planning pass. The checks run
**size, then slow, then age**, which makes the three lanes a PARTITION
of the (age × size) space — every decided key maps to exactly one lane
and each lane is a contiguous region:

- **size outranks everything**, because a full file is never wrong to
  write: once the staged bytes estimate to a whole target output file,
  no freshness argument improves on flushing now. (The commit's stats
  mode no longer keys off this choice — footer stats follow the
  REALIZED file size in flush.py, so a big catch-up flush on a slow
  lane ships full stats too.)
- **slow outranks age**: a key old enough for the slow lane flushes
  there regardless of medium size. The age lane's floor
  (``min_flush_bytes``) is deliberately low, so a key past the slow
  deadline is almost always age-eligible too — but letting the age lane
  claim it would report a 15-minute-SLA flush for a key that sat
  unflushed for hours (downtime catch-up or a pathological trickle
  crossing the floor late). The slow lane is the honest accounting of
  that case, and it is also the ONLY lane that can claim a key still
  below ``min_flush_bytes``.

Consequence, deliberate: an age-lane decision's age always sits in
``[flush_deadline, slow_lane_deadline)`` — the freshness SLA owns
exactly that window.

Ordering
--------

Decisions are ordered oldest ``first_staged_ts`` first, tie-broken by
``team_id``. The sort is total (a snapshot holds at most one entry per
team; a duplicate is refused), so the output is a deterministic
function of the snapshot regardless of its iteration order.

The policy seam
---------------

``plan_flush`` owns snapshot iteration, input validation, duplicate
refusal and ordering; the per-key trigger choice is delegated to a
``policy`` object (:class:`FlushPolicy`) — :class:`ThreeLanePolicy` is
the default. The planner is pure, so a pluggable policy is a small step
from here: another object with the same single method. No registries,
no entry points.

Everything here is a pure function of a stats snapshot and an injected
clock — no I/O, no SlateDB, no Kafka, no ``time.monotonic()`` or
``datetime.now()`` — so hypothesis can replay skewed tenant
distributions against it (design validation item 4).
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Final, Literal, Protocol, runtime_checkable

FlushTrigger = Literal["size", "age", "slow"]

TRIGGER_SIZE: Final[FlushTrigger] = "size"
TRIGGER_AGE: Final[FlushTrigger] = "age"
TRIGGER_SLOW: Final[FlushTrigger] = "slow"

_MICROS_PER_SECOND: Final = 1_000_000

#: Fallback compression ratio (parquet bytes ÷ staged bytes) used until
#: the flusher feeds back an observed EWMA. 0.2 (5× compression) is the
#: right order for JSON-ish event payloads into snappy parquet; it is
#: deliberately on the small side of plausible so the fallback
#: OVER-stages before size-flushing (a bigger file than target is a
#: sizing miss; a smaller one is a new small-file regime).
DEFAULT_COMPRESSION_RATIO: Final = 0.2

#: Knob defaults (docs/kafka-ingestion.md §The flush planner). The
#: target is the estimated OUTPUT file size: ~500 MB parquet, the
#: lakehouse-sized file the server-side compaction story is built
#: around. The flush deadline is the tail's freshness SLA. The slow
#: lane deadline is the pathological-key cadence — 6 h by default
#: (config accepts 6–24 h), trading those keys' freshness from ~96
#: files/day to ~4. The age lane's floor is deliberately low — ~1 row/s
#: sustained (~1 MiB of staged bytes per 15-minute window at ~1
#: KB/row): below it a 15-minute file is the small-file regime the lane
#: exists to suppress; "medium" tenants sit comfortably above it and
#: keep their SLA.
TARGET_OUTPUT_BYTES_DEFAULT: Final = 500 * 1024 * 1024
FLUSH_DEADLINE_S_DEFAULT: Final = 900
SLOW_LANE_DEADLINE_S_DEFAULT: Final = 6 * 3600
MIN_FLUSH_BYTES_DEFAULT: Final = 1024 * 1024
MAX_FILES_PER_COMMIT_DEFAULT: Final = 512


@dataclass(frozen=True, slots=True)
class KeyStats:
    """One team's staged-state snapshot (a decoded ``stats/`` value plus
    the team_id from its key).

    ``first_staged_ts`` / ``last_staged_ts`` are microseconds since the
    Unix epoch. ``first_staged_ts`` is set when the key's first row is
    staged after the key stood empty and is NEVER advanced by later
    arrivals — the stage writer (Phase 2) enforces that, the age
    trigger and the tail's freshness SLA rely on it, and the replay
    suite pins it. ``last_staged_ts`` is informational; no trigger
    reads it.

    The planner trusts the snapshot: these values come from the
    ``stats/`` prefix, whose byte-level decoding already validated them
    (``keyspace.decode_stats_value``).
    """

    team_id: int
    staged_bytes: int
    first_staged_ts: int
    last_staged_ts: int
    row_count: int


@dataclass(frozen=True, slots=True)
class PlannerKnobs:
    """The planner's tuning knobs. Deadlines are seconds; the slow-lane
    deadline is expected to sit above the flush deadline (steady state
    then flushes by size or age and only pathological keys ride the
    slow lane), but the planner does not require it — the lane
    precedence makes any combination well-defined.
    """

    target_output_bytes: int = TARGET_OUTPUT_BYTES_DEFAULT
    flush_deadline_s: int = FLUSH_DEADLINE_S_DEFAULT
    slow_lane_deadline_s: int = SLOW_LANE_DEADLINE_S_DEFAULT
    min_flush_bytes: int = MIN_FLUSH_BYTES_DEFAULT
    max_files_per_commit: int = MAX_FILES_PER_COMMIT_DEFAULT

    def __post_init__(self) -> None:
        if self.target_output_bytes < 1:
            raise ValueError(
                f"target_output_bytes must be >= 1, got {self.target_output_bytes}"
            )
        if self.flush_deadline_s < 1:
            raise ValueError(
                f"flush_deadline_s must be >= 1, got {self.flush_deadline_s}"
            )
        if self.slow_lane_deadline_s < 1:
            raise ValueError(
                f"slow_lane_deadline_s must be >= 1, got {self.slow_lane_deadline_s}"
            )
        if self.min_flush_bytes < 1:
            raise ValueError(
                f"min_flush_bytes must be >= 1, got {self.min_flush_bytes}"
            )
        if self.max_files_per_commit < 1:
            raise ValueError(
                f"max_files_per_commit must be >= 1, got {self.max_files_per_commit}"
            )


@dataclass(frozen=True, slots=True)
class FlushDecision:
    """One key chosen for flush this pass.

    ``age_us`` is ``now_us - first_staged_ts`` at plan time — how long
    the key's oldest staged byte had waited when the sweep decided it;
    negative only under clock skew in the snapshot.
    """

    team_id: int
    trigger: FlushTrigger
    staged_bytes: int
    first_staged_ts: int
    age_us: int


@runtime_checkable
class FlushPolicy(Protocol):
    """The policy seam: the per-key trigger choice, nothing else.

    A policy answers "which lane, if any, decides this key in this
    pass" as a pure function of the key's stats, the injected clock,
    the knobs and the compression-ratio input. ``plan_flush`` owns
    everything else (iteration, validation, duplicates, ordering), so
    an alternate policy is one small object with this one method.
    Implementations must be stateless and deterministic.
    """

    def decide(
        self,
        key: KeyStats,
        now_us: int,
        knobs: PlannerKnobs,
        estimated_compression_ratio: float,
    ) -> FlushDecision | None: ...


class ThreeLanePolicy(FlushPolicy):
    """The default policy: three lanes, precedence size > slow > age
    (module docstring). Stateless — :data:`DEFAULT_POLICY` is the one
    instance a caller needs."""

    def decide(
        self,
        key: KeyStats,
        now_us: int,
        knobs: PlannerKnobs,
        estimated_compression_ratio: float,
    ) -> FlushDecision | None:
        age_us = now_us - key.first_staged_ts
        trigger: FlushTrigger
        if key.staged_bytes * estimated_compression_ratio >= knobs.target_output_bytes:
            trigger = TRIGGER_SIZE
        elif age_us >= knobs.slow_lane_deadline_s * _MICROS_PER_SECOND:
            trigger = TRIGGER_SLOW
        elif (
            age_us >= knobs.flush_deadline_s * _MICROS_PER_SECOND
            and key.staged_bytes >= knobs.min_flush_bytes
        ):
            trigger = TRIGGER_AGE
        else:
            return None
        return FlushDecision(
            team_id=key.team_id,
            trigger=trigger,
            staged_bytes=key.staged_bytes,
            first_staged_ts=key.first_staged_ts,
            age_us=age_us,
        )


DEFAULT_POLICY: Final[ThreeLanePolicy] = ThreeLanePolicy()


def _check_ratio(estimated_compression_ratio: float) -> None:
    """The ratio is the fraction of staged bytes expected to survive
    into parquet: ``0 < r <= 1``. Zero/negative is a broken estimate;
    above 1 claims the parquet would be LARGER than the raw staged
    bytes — for the compressible payloads this pipeline stages that
    means the observer is broken, and refusing loudly beats silently
    flushing early on a poisoned estimate. NaN fails the comparison and
    is refused with the rest."""
    if not (0.0 < estimated_compression_ratio <= 1.0):
        raise ValueError(
            f"estimated_compression_ratio must be > 0 and <= 1, "
            f"got {estimated_compression_ratio}"
        )


def decide_key(
    key: KeyStats,
    now_us: int,
    knobs: PlannerKnobs,
    *,
    estimated_compression_ratio: float = DEFAULT_COMPRESSION_RATIO,
    policy: FlushPolicy = DEFAULT_POLICY,
) -> FlushDecision | None:
    """The per-key predicate: which lane, if any, decides this key in
    this pass (precedence size > slow > age — module docstring). A
    one-key convenience wrapper around the policy; ``plan_flush`` is
    the sweep path."""
    _check_ratio(estimated_compression_ratio)
    return policy.decide(key, now_us, knobs, estimated_compression_ratio)


def plan_flush(
    stats: Iterable[KeyStats],
    now_us: int,
    knobs: PlannerKnobs,
    *,
    estimated_compression_ratio: float = DEFAULT_COMPRESSION_RATIO,
    policy: FlushPolicy = DEFAULT_POLICY,
) -> list[FlushDecision]:
    """Choose which keys flush this sweep, ordered oldest
    ``first_staged_ts`` first (tie-break: ``team_id``).

    ``estimated_compression_ratio`` is the observed parquet-bytes ÷
    staged-bytes fraction (an EWMA over this table's completed flushes
    once the flusher feeds it back; :data:`DEFAULT_COMPRESSION_RATIO`
    until then). It is validated on every call — a broken estimate must
    fail the sweep loudly, not skew a day of sizing decisions.

    Deterministic: the output depends only on the snapshot's contents,
    not its iteration order. A snapshot naming one team twice is
    refused — two entries for one key would decide the key twice,
    breaking the one-trigger-per-key rule downstream.
    """
    _check_ratio(estimated_compression_ratio)
    decisions: list[FlushDecision] = []
    seen: set[int] = set()
    for key in stats:
        if key.team_id in seen:
            raise ValueError(f"duplicate stats entry for team_id {key.team_id}")
        seen.add(key.team_id)
        decision = policy.decide(key, now_us, knobs, estimated_compression_ratio)
        if decision is not None:
            decisions.append(decision)
    decisions.sort(key=lambda d: (d.first_staged_ts, d.team_id))
    return decisions
