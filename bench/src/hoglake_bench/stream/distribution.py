"""Team-volume skew for the synthetic event stream.

**The default is a plausible shape, not a measured production
distribution.** Nothing in this repo or in millpond records how the real
firehose divides across tenants, and this module does not pretend
otherwise. What the trees actually contain:

- ``hoglake_bench.seed.tables`` (TEAM_IDS / TEAM_WEIGHTS) is the closest
  prior art and is self-labelled as a deliberate synthetic skew — its own
  comment says only that "real tenants are not uniform", and its tests
  assert arity and ``sum == 1``, never an observed share.
- millpond's include/exclude machinery is an ALLOWLIST: membership with
  no volume attached, and the live list is fetched from a control-plane
  endpoint the repo deliberately knows nothing about
  (``millpond/include_values.py``). Its own load generator draws
  ``team_id`` UNIFORMLY (``test/producer.py``) and applies a Zipf law to
  ``distinct_id`` instead — the power-law shape used here, one level up.
- The nearest real measurement that could exist is millpond's
  ``millpond_filter_matched_total{value=...}`` series at run time. No
  recorded output of it lives in source. If someone reads it, replace the
  default and say so here.

So: the model is a whale plus a Zipf tail, both configurable.

- one **whale** team takes ``whale_share`` of all events outright;
- the remaining ``1 - whale_share`` spreads over the tail by a power law
  ``w_i is proportional to i**-zipf_s``, so ordinary teams are themselves
  unevenly sized rather than a flat block.

Weights are FIXED for the life of a run, so a whale stays a whale across
hours — which is what makes the per-file ``team_id`` bounds of an
hour-partitioned, sort-ordered table worth looking at.

Sampling is vectorized: one cumulative-distribution ``searchsorted`` per
batch (the same technique as ``seed.tables.weighted_idx``), never a
per-row Python draw.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

#: Enough teams for the tail to be a tail, few enough that per-team
#: bookkeeping and the summary stay cheap.
DEFAULT_TEAMS = 250

#: The whale's share of all events. A plausible dominant-tenant shape,
#: NOT a measured number — see the module docstring.
DEFAULT_WHALE_SHARE = 0.45

#: Power-law exponent over the non-whale tail. ~1 is the classic Zipf
#: value and the one millpond's own generator uses for its distinct_id
#: pool; above 1 concentrates the tail further.
DEFAULT_ZIPF_S = 1.1

#: Team ids are synthetic and dense from this base. They identify
#: nothing: no real tenant, no customer, no org.
TEAM_ID_BASE = 10_000


@dataclass(frozen=True)
class TeamDistribution:
    """A fixed, ordered team weight vector. ``team_ids[0]`` is the whale."""

    team_ids: tuple[int, ...]
    weights: tuple[float, ...]

    @classmethod
    def build(
        cls,
        *,
        teams: int = DEFAULT_TEAMS,
        whale_share: float = DEFAULT_WHALE_SHARE,
        zipf_s: float = DEFAULT_ZIPF_S,
        base: int = TEAM_ID_BASE,
    ) -> TeamDistribution:
        if teams < 1:
            raise ValueError(f"--teams must be at least 1, got {teams}")
        if not 0.0 <= whale_share < 1.0:
            raise ValueError(
                f"--whale-share must be in [0, 1), got {whale_share} — at 1.0 "
                "the tail vanishes and there is no skew left to model"
            )
        if zipf_s <= 0.0:
            raise ValueError(f"--zipf must be positive, got {zipf_s}")
        ids = tuple(base + i for i in range(teams))
        if teams == 1:
            return cls(team_ids=ids, weights=(1.0,))
        ranks = np.arange(1, teams, dtype=np.float64)
        tail = ranks**-zipf_s
        tail *= (1.0 - whale_share) / tail.sum()
        return cls(team_ids=ids, weights=(whale_share, *(float(w) for w in tail)))

    @property
    def whale_id(self) -> int:
        return self.team_ids[0]

    @property
    def whale_share(self) -> float:
        return self.weights[0]

    def share(self, team_id: int) -> float:
        return self.weights[self.team_ids.index(team_id)]

    def sample_indices(self, rng: np.random.Generator, n: int) -> np.ndarray:
        """``n`` draws as SLOT indices into ``team_ids`` — the form the
        per-team counters want (``np.bincount``), no id lookup needed."""
        cdf = np.cumsum(np.asarray(self.weights, dtype=np.float64))
        cdf /= cdf[-1]
        idx = np.searchsorted(cdf, rng.random(n)).astype(np.int64)
        # searchsorted can land one past the end on a float-rounding edge
        return np.clip(idx, 0, len(self.team_ids) - 1)

    def ids(self, indices: np.ndarray) -> np.ndarray:
        return np.asarray(self.team_ids, dtype=np.int64)[indices]

    def sample(self, rng: np.random.Generator, n: int) -> np.ndarray:
        return self.ids(self.sample_indices(rng, n))
