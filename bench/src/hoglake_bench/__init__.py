"""hoglake-bench: stress-test/benchmark harness for the hoglake control plane."""

import importlib.metadata

# Read from the installed metadata so it cannot drift from pyproject.toml.
__version__ = importlib.metadata.version("hoglake-bench")
