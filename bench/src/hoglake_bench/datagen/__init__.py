"""Synthetic data generation spanning the full hoglake 1.1.0 type system.

Two halves:

- :mod:`.columns` — column/schema builders: every writable scalar type
  (all of the server's scalars except ``variant`` — see
  :data:`VARIANT_EXCLUSION_REASON`), nested list/struct/map showcases,
  a seeded random-schema generator, and the server caps they respect.
- :mod:`.arrays` — deterministic, mostly-vectorized value fabrication
  for any generated schema, with realistic null rates and
  bounds-stressing values (uint64 beyond int64, multi-KB strings,
  shared-prefix string pairs, NaN floats).

Everything is a pure function of the caller's ``numpy`` Generator: same
seed, same schema, same bytes.
"""

from .arrays import BOUND_PREFIX, make_table
from .columns import (
    MAX_COLUMN_NODES,
    VARIANT_EXCLUSION_REASON,
    WRITABLE_SCALARS,
    ColSpec,
    deep_list_chain,
    full_coverage_columns,
    nested_showcase,
    node_count,
    random_columns,
    scalar_showcase,
    to_arrow_schema,
)

__all__ = [
    "BOUND_PREFIX",
    "MAX_COLUMN_NODES",
    "VARIANT_EXCLUSION_REASON",
    "WRITABLE_SCALARS",
    "ColSpec",
    "deep_list_chain",
    "full_coverage_columns",
    "make_table",
    "nested_showcase",
    "node_count",
    "random_columns",
    "scalar_showcase",
    "to_arrow_schema",
]
