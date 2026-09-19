"""Tenant-volume skew for the synthetic event stream.

The shape this models, and why it is not a single power law:

- **Cardinality is high.** Six figures of tenants in a day, not hundreds.
- **The head is nearly flat in log-rank.** Between the largest tenant and
  the 99th percentile the volume falls about as fast as rank rises — a
  power law with an exponent near 1.
- **The body falls much faster.** Between the 99th and the 50th
  percentile the same fit gives an exponent near 2. A single exponent
  cannot hold both ends: fit the head and the median comes out orders of
  magnitude too busy; fit the median and the largest tenant vanishes.
- **The tail is enormous and nearly idle.** Most tenants send a trickle.
  The mean is a couple of orders of magnitude above the median, so the
  mean is useless for sizing anything.

So the model is a **ladder**, not a formula: a handful of
(rank fraction, relative volume) anchors with log-log interpolation
between them. Anchors are the parameters; everything else is derived.
That reproduces both ends by construction and makes the shape legible
and editable, which a fitted exponent is not.

``LADDER`` is calibrated to the shape of a large multi-tenant event
stream. It is a SHAPE, deliberately carrying no provenance, no absolute
volumes and no tenant identity: volumes are expressed as multiples of
the median tenant, so only the ratios between anchors mean anything.
Absolute throughput is the streamer's ``--rate``, not this module's
business.

Two properties this gets right that the previous whale-plus-Zipf model
got wrong, both of which matter for layout work:

- **A flush covers the active head.** The upper ladder appears in every
  flush window, so a file's ``team_id`` bounds span essentially the whole
  domain no matter how the rows inside are ordered. That is a property of
  flush-by-time-and-size, and it is what makes file-level pruning on a
  tenant key worthless until compaction produces range-disjoint files.
- **Team id is independent of volume.** Ids are assigned by a seeded
  shuffle. Real ids come from signup order and have nothing to do with
  how busy a tenant is. Dense ascending ids paired with descending
  weights — the previous behaviour — made sorting by ``team_id`` almost
  the same thing as sorting by volume, which flatters any layout scheme
  measured against it.

Sampling is vectorized: one cumulative-distribution ``searchsorted`` per
batch (the same technique as ``seed.tables.weighted_idx``), never a
per-row Python draw.
"""

from __future__ import annotations

import functools
from dataclasses import dataclass

import numpy as np

#: Tenants active in the stream. Six figures, because the size of the
#: idle tail is the thing that decides how many tenants have to share a
#: compacted object, and a few hundred tenants cannot show that.
DEFAULT_TEAMS = 150_000

#: The volume ladder: ``(rank_fraction, volume_relative_to_the_median)``,
#: largest tenant first. ``rank_fraction`` is position in the volume
#: ranking — 0.01 is the 99th percentile, 0.5 the median. Volumes are
#: multiples of the median tenant, so the ladder is scale-free.
#:
#: Read it as: the largest tenant is ~6 orders of magnitude above the
#: median, the 99th percentile ~3, the 95th ~2, and the bottom of the
#: tail sits two orders BELOW the median. The steepening between the
#: 95th percentile and the median is the part a single exponent cannot
#: reproduce.
#:
#: The head anchor is tuned to the busiest tenant's SHARE of all events,
#: not to its volume ratio against the median. The two cannot both hold:
#: the total is emergent from the whole ladder, so fixing the ratio
#: moves the share and vice versa. Share is the property a bench about
#: flush head-heaviness needs, so it wins — which is why this number
#: reads lower than the p50/p95/p99 ratios would lead you to expect.
LADDER: tuple[tuple[float, float], ...] = (
    (0.00, 1_200_000.0),
    (0.01, 1_600.0),
    (0.05, 193.0),
    (0.50, 1.0),
    (1.00, 0.01),
)

#: Team ids are synthetic and dense from this base. They identify
#: nothing: no real tenant, no customer, no org.
TEAM_ID_BASE = 10_000

#: Seed for the id-to-rank shuffle. Fixed so a run is reproducible;
#: separate from the event rng so changing one does not move the other.
DEFAULT_ID_SEED = 20260919


def ladder_weights(
    teams: int, ladder: tuple[tuple[float, float], ...] = LADDER
) -> np.ndarray:
    """Normalized weights for ``teams`` tenants, rank 0 busiest.

    Log-log interpolation between the anchors: linear in
    ``log(volume)`` against ``log(rank)``, which is what makes each
    segment a power law and the joins continuous. Ranks are 1-based so
    the top anchor has a finite log.
    """
    if teams == 1:
        return np.ones(1, dtype=np.float64)
    ranks = np.arange(1, teams + 1, dtype=np.float64)
    # A rank fraction of 0 means "the single busiest tenant" — rank 1,
    # not rank 0, which has no logarithm.
    anchor_ranks = np.array([max(1.0, f * teams) for f, _ in ladder], dtype=np.float64)
    anchor_volumes = np.array([v for _, v in ladder], dtype=np.float64)
    weights = np.exp(
        np.interp(np.log(ranks), np.log(anchor_ranks), np.log(anchor_volumes)),
    )
    return weights / weights.sum()


@dataclass(frozen=True)
class TeamDistribution:
    """A fixed team weight vector, ordered by volume: slot 0 is the
    busiest tenant. ``team_ids[slot]`` is that tenant's id, and the two
    are deliberately uncorrelated."""

    team_ids: tuple[int, ...]
    weights: tuple[float, ...]

    @classmethod
    def build(
        cls,
        *,
        teams: int = DEFAULT_TEAMS,
        base: int = TEAM_ID_BASE,
        ladder: tuple[tuple[float, float], ...] = LADDER,
        id_seed: int = DEFAULT_ID_SEED,
    ) -> TeamDistribution:
        if teams < 1:
            raise ValueError(f"--teams must be at least 1, got {teams}")
        if len(ladder) < 2:
            raise ValueError("ladder needs at least two anchors to interpolate between")
        fractions = [f for f, _ in ladder]
        if fractions != sorted(fractions) or len(set(fractions)) != len(fractions):
            raise ValueError("ladder rank fractions must be strictly ascending")
        if any(not 0.0 <= f <= 1.0 for f in fractions):
            raise ValueError("ladder rank fractions must be in [0, 1]")
        # np.interp CLAMPS outside the anchor range rather than raising,
        # so a ladder that does not reach both ends produces a silently
        # flat head or tail instead of an error. Refuse it here: a bench
        # that quietly models something other than what its anchors say
        # is worse than one that will not start.
        if fractions[0] != 0.0 or fractions[-1] != 1.0:
            raise ValueError(
                "ladder must span the whole ranking: first anchor at rank "
                "fraction 0.0 (the busiest tenant) and last at 1.0 (the "
                f"quietest), got {fractions[0]} and {fractions[-1]}"
            )
        if any(v <= 0.0 for _, v in ladder):
            raise ValueError(
                "ladder volumes must be positive — log-log needs a finite log"
            )
        weights = ladder_weights(teams, ladder)
        # Ids shuffled against rank: slot 0 stays the busiest tenant, but
        # its id is arbitrary. See the module docstring.
        ids = np.arange(base, base + teams, dtype=np.int64)
        np.random.default_rng(id_seed).shuffle(ids)
        return cls(
            team_ids=tuple(int(i) for i in ids),
            weights=tuple(float(w) for w in weights),
        )

    @property
    def whale_id(self) -> int:
        """The busiest tenant's id. Not the lowest id — see the shuffle."""
        return self.team_ids[0]

    @property
    def whale_share(self) -> float:
        return self.weights[0]

    # Both of these are rebuilt from the frozen tuples on every call
    # otherwise, and every call is once per flush at six-figure tenant
    # counts. cached_property writes straight into the instance __dict__,
    # which is why it works on a frozen dataclass (nothing here is
    # slotted) and why it costs nothing after the first flush.
    @functools.cached_property
    def _cdf(self) -> np.ndarray:
        cdf = np.cumsum(np.asarray(self.weights, dtype=np.float64))
        return cdf / cdf[-1]

    @functools.cached_property
    def _ids(self) -> np.ndarray:
        return np.asarray(self.team_ids, dtype=np.int64)

    def sample_indices(self, rng: np.random.Generator, n: int) -> np.ndarray:
        """``n`` draws as SLOT indices into ``team_ids`` — the form the
        per-team counters want (``np.bincount``), no id lookup needed."""
        idx = np.searchsorted(self._cdf, rng.random(n)).astype(np.int64)
        # searchsorted can land one past the end on a float-rounding edge
        return np.clip(idx, 0, len(self.team_ids) - 1)

    def ids(self, indices: np.ndarray) -> np.ndarray:
        return self._ids[indices]

    def sample(self, rng: np.random.Generator, n: int) -> np.ndarray:
        return self.ids(self.sample_indices(rng, n))
