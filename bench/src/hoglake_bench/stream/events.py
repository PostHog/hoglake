"""The streamed events table: schema, fabrication, sort.

The column set is the seeded warehouse's ``events.pageviews``
(``hoglake_bench.seed.tables.PAGEVIEWS_SCHEMA``) reused wholesale, plus
one ``properties`` blob — millpond's property payload, which is what
gives a row its bytes. The blob is **text**: pyarrow cannot write native
Parquet VARIANT(1), so variant is deliberately out of scope here exactly
as it is for the seed task (``datagen.VARIANT_EXCLUSION_REASON``).

Two things the streamer relies on:

``sort_batch``
    Sorts by the declared sort order (``team_id``, then event time)
    before the batch is handed to the writer — millpond does the same
    upstream of its sink (``MILLPOND_SORT_BY=team_id,timestamp``,
    millpond README §Configuration). Sort order is advisory for writers
    and binding for compaction, so declaring it AND honouring it is what
    lets compaction's sorted path keep team locality in a table that is
    not partitioned by team.

``make_batch``
    Draws ``team_id`` from the skewed distribution rather than pinning a
    batch to one team, so the skew shows up as WITHIN-file distribution:
    most rows in every file belong to the whale, the tail scattered
    through. That is what makes a file's ``team_id`` bounds worth
    reading — they are wide unless the sort actually held.
"""

from __future__ import annotations

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from ..seed import tables as T
from ..seed.vocab import Vocabulary
from .distribution import TeamDistribution

#: The streamed shape: every pageviews column, plus the property blob.
STREAM_SCHEMA = pa.schema([*T.PAGEVIEWS_SCHEMA, pa.field("properties", pa.string())])

#: The declared sort order, in tuple order.
SORT_COLUMNS = ("team_id", "ts")

#: The partition spec: the event timestamp, to the hour. Deliberately
#: NOT team-partitioned — the production events table is not partitioned
#: per tenant, so every team's events land in the same hourly cell.
PARTITION_COLUMN = "ts"
PARTITION_TRANSFORM = "hour"

#: Default property-blob size, in bytes of payload per row.
DEFAULT_PROPERTIES_BYTES = 192

#: Property-blob prefixes; the payload after them is per-row random, so
#: the column does not dictionary-encode away to nothing.
_PROPERTY_HEADS = tuple(
    f'{{"$lib":"{lib}","$lib_version":"{major}.{minor}.0",'
    f'"$screen":"{width}x{height}","$payload":"'
    for lib in ("web", "posthog-js", "posthog-python", "posthog-node")
    for major, minor in ((1, 4), (2, 11), (3, 0))
    for width, height in ((1920, 1080), (1440, 900), (390, 844))
)


def _properties(rng: np.random.Generator, n: int, nbytes: int) -> pa.Array:
    pad = T.hex_strings(rng, n, max(1, nbytes // 2))
    heads = T.take(_PROPERTY_HEADS, T.uniform_idx(rng, len(_PROPERTY_HEADS), n))
    return pc.binary_join_element_wise(heads, pa.array(pad, pa.string()), '"}', "")


def make_batch(
    rng: np.random.Generator,
    vocab: Vocabulary,
    n: int,
    *,
    teams: TeamDistribution,
    lo_us: int,
    hi_us: int,
    properties_bytes: int = DEFAULT_PROPERTIES_BYTES,
) -> tuple[pa.Table, np.ndarray]:
    """``n`` events in ``[lo_us, hi_us)``, teams drawn from ``teams``.

    Returns the (unsorted) batch and the team SLOT indices that produced
    it, so the caller can ``bincount`` without a second pass.
    """
    idx = teams.sample_indices(rng, n)
    # Reuse the seeded pageviews fabricator wholesale, then overwrite the
    # single-team column it fills with the skewed draw.
    batch = T.pageviews(rng, vocab, n, team_id=0, lo_us=lo_us, hi_us=hi_us)
    batch = batch.set_column(
        batch.schema.get_field_index("team_id"),
        T.PAGEVIEWS_SCHEMA.field("team_id"),
        pa.array(teams.ids(idx), pa.int64()),
    )
    batch = batch.append_column(
        STREAM_SCHEMA.field("properties"),
        _properties(rng, n, properties_bytes),
    )
    return batch.cast(STREAM_SCHEMA), idx


def sort_batch(batch: pa.Table) -> pa.Table:
    """Sort to the declared order before the write, as millpond does."""
    return batch.sort_by([(column, "ascending") for column in SORT_COLUMNS])
