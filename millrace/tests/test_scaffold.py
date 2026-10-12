"""Phase 0 smoke: the package imports and carries the dev version."""

import millrace
from millrace import config, consumer, flush, keyspace, main, planner, server, stage


def test_package_imports():
    assert millrace.__version__ == "0.1.0.dev0"


def test_version_carries_the_dev_suffix():
    # AGENT.md versioning: an unreleased build never claims a released number.
    assert millrace.__version__.endswith("dev0")


def test_every_phase_0_module_is_present():
    for module in (config, consumer, flush, keyspace, main, planner, server, stage):
        assert module.__doc__
