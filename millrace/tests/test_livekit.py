"""Non-live pins for the live harness's env rendering (tests/live/livekit.py).

The live suite only ever runs against the docker stack, so the parts of
``livekit`` that are pure (the child env dump) get their unit coverage
here — a dump bug would otherwise surface only as a boot-refused child
on the stack (the consumer batch's flag: a ``VALUE_JSON`` config dumped
as bare ``value-json``, which config refuses at boot for naming no
payload field).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest
from flushkit import make_config

from millrace.config import AutoOffsetReset, TeamKeyCodec, load_config


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


@pytest.mark.parametrize(
    ("codec", "field", "expected"),
    [
        (TeamKeyCodec.UTF8_DECIMAL, None, "utf8-decimal"),
        (TeamKeyCodec.BE64, None, "be64"),
        (TeamKeyCodec.VALUE_JSON, "team_id", "value-json:team_id"),
    ],
)
def test_child_env_dumps_the_full_codec_spec(codec, field, expected):
    livekit = _livekit()
    cfg = make_config(team_key_codec=codec, team_id_field=field)
    env = livekit.child_env(cfg, hoglake_url="http://hog.test", metrics_port=0)
    assert env["MILLRACE_TEAM_KEY_CODEC"] == expected
    # The proof, not a restatement: the rendered knob must parse back
    # through the real loader to the same codec configuration (a bare
    # "value-json" is REFUSED there). (The rest of the child's env is
    # live-stack-shaped — its s3 stage URL only exists on the stack —
    # so the round-trip is scoped to the codec knob.)
    from test_config import MINIMAL_ENV

    reparsed = load_config(MINIMAL_ENV | {"MILLRACE_TEAM_KEY_CODEC": expected})
    assert reparsed.team_key_codec is codec
    assert reparsed.team_id_field == field


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
