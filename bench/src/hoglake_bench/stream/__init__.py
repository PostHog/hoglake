"""The ``stream`` task: a continuous synthetic event stream.

Not a benchmark scenario — it runs until it is interrupted, so ``all``
must never launch it and it flags no regressions. It does journal a
results line and declare an IO mode, because every byte it writes is
real parquet in the object store.
"""

from __future__ import annotations

from .distribution import TeamDistribution
from .streamer import (
    DEFAULT_CATALOG,
    DEFAULT_FLUSH_MB,
    DEFAULT_FLUSH_SECONDS,
    IO_MODE,
    add_args,
    run,
)

__all__ = [
    "DEFAULT_CATALOG",
    "DEFAULT_FLUSH_MB",
    "DEFAULT_FLUSH_SECONDS",
    "IO_MODE",
    "TeamDistribution",
    "add_args",
    "run",
]
