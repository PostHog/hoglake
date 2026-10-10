"""millrace — Kafka → hoglake ingestion daemon with SlateDB staging.

Staging decouples arrival from flush: rows accumulate in an embedded,
object-storage-native LSM keyed by partition value, and per-key
readiness (size for whales, age for the tail) decides when each key
flushes to parquet + prepared commit. See docs/kafka-ingestion.md for
the design and docs/kafka-ingestion-plan.md for the build-and-test
sequence.
"""

import importlib.metadata

# Read from the installed metadata so it cannot drift from pyproject.toml.
__version__ = importlib.metadata.version("millrace")

__all__ = ["__version__"]
