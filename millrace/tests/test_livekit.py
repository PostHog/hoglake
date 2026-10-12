"""Non-live pins for the live harness's env rendering (tests/live/livekit.py).

The live suite only ever runs against the docker stack, so the parts of
``livekit`` that are pure (the child env dump) get their unit coverage
here — a dump bug would otherwise surface only as a boot-refused child
on the stack.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

from millrace.config import AutoOffsetReset


def _livekit() -> Any:
    """Import tests/live/livekit.py by path (the directory is not a
    package and lives off the non-live sys.path)."""
    if "livekit" in sys.modules:
        return sys.modules["livekit"]
    path = Path(__file__).parent / "live" / "livekit.py"
    spec = importlib.util.spec_from_file_location("livekit", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["livekit"] = module
    spec.loader.exec_module(module)
    return module


def test_livekit_pins_earliest_offset_reset_for_test_determinism():
    """The suite produces-then-consumes with a fresh group per test, so
    livekit must pin EARLIEST — production's LATEST default (fleet
    policy, config.py) would starve every backlog assertion. Pins both
    livekit surfaces: the in-process config and the child env dump."""
    livekit = _livekit()

    cfg = livekit.live_config(
        topic="t",
        group="g",
        partitions=(0,),
        stage_base_path="bench",
        catalog="c",
        namespace="ns",
        table="tbl",
    )

    assert cfg.kafka_auto_offset_reset is AutoOffsetReset.EARLIEST

    env = livekit.child_env(cfg, metrics_port=0)
    assert env["MILLRACE_KAFKA_AUTO_OFFSET_RESET"] == "earliest"
