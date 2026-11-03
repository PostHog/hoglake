"""Pin millrace's wiring into the ci-python.yml gate.

Phase 0's meta-test (docs/kafka-ingestion-plan.md): a workflow that
path-filters itself out of a PR leaves its required check "expected"
forever (AGENT.md, #299), and a gate that does not need a leaf job
cannot see that job fail (AGENT.md, #304). Both failure modes are
pinned here, and so are the image's hand-pinned codec build tools.
"""

import re
import tomllib
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = ROOT / ".github" / "workflows"
WORKFLOW = WORKFLOWS / "ci-python.yml"


def _jobs():
    return yaml.safe_load(WORKFLOW.read_text())["jobs"]


def test_millrace_tree_triggers_the_workflow():
    steps = _jobs()["changes"]["steps"]
    filter_step = next(step for step in steps if step.get("id") == "filter")
    filters = yaml.safe_load(filter_step["with"]["filters"])
    assert "millrace/**" in filters["python"]


def test_python_checks_gate_needs_the_millrace_job():
    jobs = _jobs()
    assert "millrace" in jobs
    assert "millrace" in jobs["python-checks"]["needs"]


def test_python_checks_gate_needs_the_codec_wheels_when_the_codec_changes():
    """The Rust codec's wheels (variant-codec-wheels.yml) run only when the
    codec changed, which runs the gate too, and the gate must see them fail."""
    jobs = _jobs()
    steps = jobs["changes"]["steps"]
    filter_step = next(step for step in steps if step.get("id") == "filter")
    filters = yaml.safe_load(filter_step["with"]["filters"])
    both = set(filters["python"]) & set(filters["codec"])
    assert {
        "pyhoglake-variant-codec/**",
        ".github/workflows/variant-codec-wheels.yml",
    } <= both
    assert "uv.lock" in filters["codec"]
    wheels = jobs["variant-codec-wheels"]
    assert wheels["if"] == "needs.changes.outputs.codec == 'true'"
    assert wheels["uses"] == "./.github/workflows/variant-codec-wheels.yml"
    assert wheels["with"] == {"all-platforms": False}
    assert "variant-codec-wheels" in jobs["python-checks"]["needs"]
    # A release builds every platform.
    publish = WORKFLOWS / "publish-pyhoglake-variant-codec.yml"
    release = yaml.safe_load(publish.read_text())["jobs"]["wheels"]
    assert release["with"] == {"all-platforms": True}


def test_the_image_builds_the_codec_with_the_release_wheels_tools():
    """millrace/Dockerfile's codec stage pins Rust by its image tag, and
    maturin and zig by version, by hand: they must be rust-toolchain.toml's
    and variant-codec-wheels.yml's, which build the published wheels."""
    dockerfile = (ROOT / "millrace" / "Dockerfile").read_text()
    toolchain = ROOT / "pyhoglake-variant-codec" / "rust-toolchain.toml"
    channel = tomllib.loads(toolchain.read_text())["toolchain"]["channel"]
    stage = r"^FROM --platform=\$BUILDPLATFORM rust:(\S+)-slim-\S+ AS codec$"
    assert re.findall(stage, dockerfile, re.MULTILINE) == [channel]
    env = yaml.safe_load((WORKFLOWS / "variant-codec-wheels.yml").read_text())["env"]
    for pin in (env["MATURIN"], env["ZIG"]):
        assert f" {pin} " in dockerfile
